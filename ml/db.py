# ml/db.py
import os, json, base64, time
from contextlib import contextmanager
from typing import Iterable, List, Dict, Any, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

def get_engine() -> Engine:
    return create_engine(_DATABASE_URL, pool_pre_ping=True, pool_recycle=1800)

@contextmanager
def db_conn(engine: Optional[Engine] = None):
    eng = engine or get_engine()
    with eng.begin() as conn:
        yield conn

def fetch_ticks(start: int, end: int) -> List[Dict[str, Any]]:
    """
    Assumes table 'ticks' with columns: id BIGINT, timestamp TIMESTAMPTZ, mid DOUBLE PRECISION (fallback to (bid+ask)/2).
    """
    sql = text("""
        SELECT id AS tickid, timestamp, 
               COALESCE(mid, (bid+ask)/2.0) AS price
        FROM ticks
        WHERE id BETWEEN :s AND :e
        ORDER BY id ASC
    """)
    with db_conn() as conn:
        rows = conn.execute(sql, {"s": int(start), "e": int(end)}).mappings().all()
        return [dict(r) for r in rows]

def upsert_many(table: str, rows: List[Dict[str, Any]], conflict_key: str = "tickid") -> int:
    if not rows:
        return 0
    cols = sorted(rows[0].keys())
    cols_sql = ", ".join(cols)
    params_sql = ", ".join([f":{c}" for c in cols])
    updates_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != conflict_key])
    sql = text(f"""
        INSERT INTO {table} ({cols_sql})
        VALUES ({params_sql})
        ON CONFLICT ({conflict_key}) DO UPDATE SET {updates_sql}
    """)
    with db_conn() as conn:
        conn.execute(sql, rows)
    return len(rows)

def upsert_walk_run(run: Dict[str, Any]):
    cols = sorted(run.keys())
    cols_sql = ", ".join(cols)
    params_sql = ", ".join([f":{c}" for c in cols])
    updates_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "run_id"])
    sql = text(f"""
        INSERT INTO walk_runs ({cols_sql}) VALUES ({params_sql})
        ON CONFLICT (run_id) DO UPDATE SET {updates_sql}
    """)
    with db_conn() as conn:
        conn.execute(sql, run)

def mark_run_confirmed(run_id: str):
    with db_conn() as conn:
        conn.execute(text("UPDATE walk_runs SET confirmed=TRUE WHERE run_id=:r"), {"r": run_id})

def save_model_blob(model_id: str, algo: str, blob_bytes: bytes, notes: str, extra_params: Optional[dict]=None):
    params = extra_params.copy() if extra_params else {}
    params["blob_b64"] = base64.b64encode(blob_bytes).decode("ascii")
    row = {
        "model_id": model_id,
        "algo": algo,
        "params": json.dumps(params),
        "calib": json.dumps({}),
        "notes": notes
    }
    cols = ",".join(row.keys())
    placeholders = ",".join([f":{k}" for k in row.keys()])
    updates = "algo=EXCLUDED.algo, params=EXCLUDED.params, calib=EXCLUDED.calib, notes=EXCLUDED.notes"
    with db_conn() as conn:
        conn.execute(text(f"""
            INSERT INTO models ({cols}) VALUES ({placeholders})
            ON CONFLICT (model_id) DO UPDATE SET {updates}
        """), row)

def load_model_blob(model_id: Optional[str]=None, algo: Optional[str]=None) -> Tuple[str, bytes]:
    if model_id:
        sql = text("SELECT model_id, params FROM models WHERE model_id=:m LIMIT 1")
        args = {"m": model_id}
    else:
        sql = text("SELECT model_id, params FROM models WHERE algo=:a ORDER BY created_at DESC LIMIT 1")
        args = {"a": algo}
    with db_conn() as conn:
        row = conn.execute(sql, args).mappings().first()
        if not row:
            raise RuntimeError("Model not found.")
        params = json.loads(row["params"]) if isinstance(row["params"], str) else row["params"]
        b64 = params.get("blob_b64")
        if not b64:
            raise RuntimeError("Model blob missing in params.blob_b64")
        return row["model_id"], base64.b64decode(b64)

def latest_prediction() -> Optional[Dict[str, Any]]:
    with db_conn() as conn:
        row = conn.execute(text("""
            SELECT tickid, model_id, p_up, p_neu, p_dn, s_curve, decided_label
            FROM predictions ORDER BY tickid DESC LIMIT 1
        """)).mappings().first()
        return dict(row) if row else None

def review_slice(start: int, total: int, offset: int, limit: int) -> Dict[str, Any]:
    s = start + offset
    e = min(start + total - 1, s + limit - 1)
    with db_conn() as conn:
        ticks = conn.execute(text("""
            SELECT id AS tickid, extract(epoch from timestamp) AS ts, 
                   COALESCE(mid,(bid+ask)/2.0) AS price
            FROM ticks WHERE id BETWEEN :s AND :e ORDER BY id
        """), {"s": s, "e": e}).mappings().all()

        kal = conn.execute(text("""
            SELECT tickid, level, slope, var
            FROM kalman_states WHERE tickid BETWEEN :s AND :e ORDER BY tickid
        """), {"s": s, "e": e}).mappings().all()

        feats = conn.execute(text("""
            SELECT tickid, level, slope, residual, vol_ewstd, vol_ewstd_long, r50, r200, r1000, rsi, stoch_k, stoch_d, vwap_dist, r2_lin, tod_bucket
            FROM ml_features WHERE tickid BETWEEN :s AND :e ORDER BY tickid
        """), {"s": s, "e": e}).mappings().all()

        labels = conn.execute(text("""
            SELECT tickid, direction, is_segment_start
            FROM trend_labels WHERE tickid BETWEEN :s AND :e ORDER BY tickid
        """), {"s": s, "e": e}).mappings().all()

        preds = conn.execute(text("""
            SELECT tickid, p_up, p_neu, p_dn, s_curve, decided_label
            FROM predictions WHERE tickid BETWEEN :s AND :e ORDER BY tickid
        """), {"s": s, "e": e}).mappings().all()

    return {
        "offset": offset, "limit": limit,
        "ticks": [dict(r) for r in ticks],
        "kalman": [dict(r) for r in kal],
        "features": [dict(r) for r in feats],
        "labels": [dict(r) for r in labels],
        "predictions": [dict(r) for r in preds],
        "range": [s, e]
    }
