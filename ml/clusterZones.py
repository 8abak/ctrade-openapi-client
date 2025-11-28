#!/usr/bin/env python3
"""
ml/clusterZones.py

Cluster all zone personalities into behavioral regimes using:

  1) StandardScaler
  2) PCA (linear dimension reduction to 3D)
  3) HDBSCAN (density-based clustering)

Input:
  - zone_personality (must already be populated)

Output:
  - zone_cluster table:
        id           -- zone id (same as zones.id, zone_personality.id)
        cluster_id   -- cluster label (-1 = noise / outlier)
        cluster_prob -- membership strength [0, 1]

Usage (from repo root):
    source venv/bin/activate
    python ml/clusterZones.py
"""

import psycopg2
import numpy as np
import hdbscan
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from collections import Counter

# ------------------------------------------------------------------------------
# Database configuration
# ------------------------------------------------------------------------------

DB_CONFIG = {
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
    "host": "localhost",
    "port": 5432,
}


def get_conn():
    """Open a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def dict_cur(conn):
    """Context manager that yields a RealDictCursor (rows as dicts)."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()


# ------------------------------------------------------------------------------
# Features used to describe each zone's personality
# ------------------------------------------------------------------------------

FEATURES = [
    "dir_zone",
    "net_move",
    "abs_move",
    "full_range",
    "body_range_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "duration_sec",
    "n_ticks",
    "speed",
    "noise_ratio",
    "pos_of_extreme",
    "delay_frac_0_2",
    "delay_frac_0_4",
    "n_swings",
    "swing_dir_changes",
    "avg_swing_range",
    "max_swing_range",
]


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def load_zone_personality(conn):
    """Load zone_personality rows and return (zone_ids, feature_matrix)."""
    print("Loading zone_personality...")
    with dict_cur(conn) as cur:
        cur.execute(
            f"""
            SELECT id, {", ".join(FEATURES)}
            FROM zone_personality
            ORDER BY id
            """
        )
        rows = cur.fetchall()

    if not rows:
        raise RuntimeError("zone_personality is empty. Build personalities first.")

    zone_ids = [int(r["id"]) for r in rows]

    # Build feature matrix [n_zones x n_features]
    M = np.array(
        [
            [float(r[f]) if r[f] is not None else 0.0 for f in FEATURES]
            for r in rows
        ],
        dtype=float,
    )

    print(f"Loaded {len(zone_ids)} zones, feature matrix shape = {M.shape}")
    return zone_ids, M


def run_pca(M_scaled, n_components=3):
    """Run PCA to reduce feature space to n_components."""
    print(f"Running PCA ({n_components} components)...")
    pca = PCA(n_components=n_components, random_state=42)
    embedding = pca.fit_transform(M_scaled)
    explained = pca.explained_variance_ratio_.sum()
    print(f"PCA done, embedding shape = {embedding.shape}, "
          f"explained variance = {explained:.3f}")
    return embedding


def run_hdbscan(embedding):
    """Run HDBSCAN clustering on PCA embedding."""
    print("Running HDBSCAN clustering...")
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=300,   # tune later if needed
        min_samples=50,
        prediction_data=True,
    )
    labels = clusterer.fit_predict(embedding)
    probs = clusterer.probabilities_

    print("HDBSCAN done.")
    print("Cluster label counts:", Counter(labels))
    return labels, probs


def write_zone_cluster(conn, zone_ids, labels, probs):
    """Write clustering results into zone_cluster table."""
    print("Writing results into zone_cluster...")
    with conn, conn.cursor() as cur:
        # Clear existing rows to keep one set of clusters at a time
        cur.execute("TRUNCATE zone_cluster")

        for zid, cid, p in zip(zone_ids, labels, probs):
            cur.execute(
                """
                INSERT INTO zone_cluster (id, cluster_id, cluster_prob)
                VALUES (%s, %s, %s)
                """,
                (int(zid), int(cid), float(p)),
            )
    print("zone_cluster updated.")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    conn = get_conn()

    # 1) Load personalities
    zone_ids, M = load_zone_personality(conn)

    # 2) Scale features
    print("Scaling features with StandardScaler...")
    scaler = StandardScaler()
    M_scaled = scaler.fit_transform(M)

    # 3) PCA
    embedding = run_pca(M_scaled, n_components=3)

    # 4) HDBSCAN
    labels, probs = run_hdbscan(embedding)

    # 5) Save to DB
    write_zone_cluster(conn, zone_ids, labels, probs)

    print("All done.")
    conn.close()


if __name__ == "__main__":
    main()
