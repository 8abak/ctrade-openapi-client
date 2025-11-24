# PATH: jobs/build_kalseg_outcome.py
"""
Compute kalseg_outcome labels for the first N segments.

Usage (from project root):
    python -m jobs.build_kalseg_outcome --limit 5000

This script implements the "Hybrid" labeling scheme (Option C):

1) Early commitment in first EARLY_WINDOW_TICKS:
      - If price reaches +EARLY_TARGET without going below -EARLY_STOP: early_label = +1
      - If price reaches -EARLY_TARGET without going above +EARLY_STOP: early_label = -1
      - Else: early_label = 0

2) Final winner up to FINAL_MAX_TICKS (if early_label == 0):
      - First hit of +FINAL_TARGET → final_label = +1
      - First hit of -FINAL_TARGET → final_label = -1
      - None within window       → final_label = 0

Everything is measured relative to base_price at kalseg.start_id,
using kal column if present, otherwise mid.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, List

from backend.db import get_conn, dict_cur, detect_mid_expr, detect_ts_col


# --- Labeling hyperparameters (can be tuned later) ---

EARLY_WINDOW_TICKS = 200
EARLY_TARGET       = 0.40
EARLY_STOP         = 0.20

FINAL_TARGET       = 1.00
FINAL_MAX_TICKS    = 3000


@dataclass
class SegmentOutcome:
    seg_id: int
    start_id: int
    end_id: int
    dir_kalseg: int

    base_price: float

    early_window_ticks: int
    early_target: float
    early_stop: float
    early_mfe_up: float
    early_mfe_down: float
    early_label: int

    final_target: float
    final_max_ticks: int
    final_label: int
    final_hit_delta: Optional[float]
    final_hit_tick_id: Optional[int]
    final_hit_ts: Optional[datetime]
    ticks_to_hit: Optional[int]
    secs_to_hit: Optional[float]


def map_direction(raw) -> int:
    if raw is None:
        return 0
    s = str(raw).strip().lower()
    if s in ("up", "u", "1", "+1"):
        return 1
    if s in ("down", "dn", "d", "-1"):
        return -1
    try:
        v = int(s)
        return 1 if v > 0 else -1 if v < 0 else 0
    except ValueError:
        return 0


def ensure_outcome_table(conn):
    """Just in case, but we already created it via SQL console."""
    ddl = """
    CREATE TABLE IF NOT EXISTS kalseg_outcome (
        seg_id              BIGINT PRIMARY KEY REFERENCES kalseg(id),
        start_id            BIGINT NOT NULL,
        end_id              BIGINT NOT NULL,
        dir_kalseg          SMALLINT NOT NULL,
        base_price          DOUBLE PRECISION NOT NULL,
        early_window_ticks  INTEGER NOT NULL,
        early_target        DOUBLE PRECISION NOT NULL,
        early_stop          DOUBLE PRECISION NOT NULL,
        early_mfe_up        DOUBLE PRECISION NOT NULL,
        early_mfe_down      DOUBLE PRECISION NOT NULL,
        early_label         SMALLINT NOT NULL,
        final_target        DOUBLE PRECISION NOT NULL,
        final_max_ticks     INTEGER NOT NULL,
        final_label         SMALLINT NOT NULL,
        final_hit_delta     DOUBLE PRECISION,
        final_hit_tick_id   BIGINT,
        final_hit_ts        TIMESTAMPTZ,
        ticks_to_hit        INTEGER,
        secs_to_hit         DOUBLE PRECISION,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with dict_cur(conn) as cur:
        cur.execute(ddl)


def ticks_has_kal(conn) -> bool:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='ticks'
              AND column_name='kal'
            """
        )
        return cur.fetchone() is not None


def fetch_first_n_kalseg(conn, limit: int) -> List[dict]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, start_id, end_id, direction
            FROM kalseg
            ORDER BY id ASC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def fetch_price_path(
    conn,
    ts_col: str,
    mid_expr: str,
    has_kal: bool,
    start_id: int,
    max_ticks: int,
) -> List[dict]:
    """
    Get up to max_ticks rows from ticks starting at start_id (inclusive),
    ordered ascending by id. Returns list of dicts with id, ts, mid, kal?
    """
    with dict_cur(conn) as cur:
        kal_sel = ", kal" if has_kal else ""
        cur.execute(
            f"""
            SELECT id,
                   {ts_col}   AS ts,
                   {mid_expr} AS mid
                   {kal_sel}
            FROM ticks
            WHERE id >= %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (start_id, max_ticks),
        )
        return cur.fetchall()


def compute_outcome_for_segment(
    seg: dict,
    price_rows: List[dict],
) -> Optional[SegmentOutcome]:
    """
    Core labeling logic for a single segment.
    price_rows must be sorted by id ASC, starting at >= seg.start_id.
    """

    seg_id = int(seg["id"])
    start_id = int(seg["start_id"])
    end_id = int(seg["end_id"])
    dir_kalseg = map_direction(seg.get("direction"))

    if not price_rows:
        # no data available; skip
        return None

    # Ensure base row is the first available >= start_id
    base_row = price_rows[0]
    base_price = float(base_row.get("kal") or base_row["mid"])
    base_ts = base_row["ts"]

    # ---------------------------------------------
    # Sweep forward and compute deltas
    # ---------------------------------------------
    early_len = min(EARLY_WINDOW_TICKS, len(price_rows))

    early_mfe_up = 0.0
    early_mfe_down = 0.0

    mfe_up = 0.0
    mfe_down = 0.0

    first_up_hit_tick = None
    first_down_hit_tick = None
    first_up_hit_ts = None
    first_down_hit_ts = None

    first_up_hit_delta = None
    first_down_hit_delta = None

    early_label = 0

    for idx, row in enumerate(price_rows):
        price = float(row.get("kal") or row["mid"])
        delta = price - base_price

        # global MFE / MAE
        if delta > mfe_up:
            mfe_up = delta
        if delta < mfe_down:
            mfe_down = delta

        # early window stats
        if idx < early_len:
            if delta > early_mfe_up:
                early_mfe_up = delta
            if delta < early_mfe_down:
                early_mfe_down = delta

        # first FINAL_TARGET hits
        if first_up_hit_tick is None and delta >= FINAL_TARGET:
            first_up_hit_tick = int(row["id"])
            first_up_hit_ts = row["ts"]
            first_up_hit_delta = delta

        if first_down_hit_tick is None and delta <= -FINAL_TARGET:
            first_down_hit_tick = int(row["id"])
            first_down_hit_ts = row["ts"]
            first_down_hit_delta = delta

    # ---------------------------------------------
    # Decide early_label
    # ---------------------------------------------
    if (
        early_mfe_up >= EARLY_TARGET
        and early_mfe_down > -EARLY_STOP
    ):
        early_label = 1
    elif (
        early_mfe_down <= -EARLY_TARGET
        and early_mfe_up < EARLY_STOP
    ):
        early_label = -1
    else:
        early_label = 0

    # ---------------------------------------------
    # Decide final_label using fallback
    # ---------------------------------------------
    if early_label != 0:
        final_label = early_label
        final_hit_delta = None
        final_hit_tick_id = None
        final_hit_ts = None
        ticks_to_hit = None
        secs_to_hit = None
    else:
        # Check which target was hit first, if any
        final_label = 0
        final_hit_delta = None
        final_hit_tick_id = None
        final_hit_ts = None
        ticks_to_hit = None
        secs_to_hit = None

        if first_up_hit_tick is not None or first_down_hit_tick is not None:
            # we have at least one hit, find which is earlier in id
            if first_up_hit_tick is not None and (
                first_down_hit_tick is None or first_up_hit_tick < first_down_hit_tick
            ):
                final_label = 1
                final_hit_delta = first_up_hit_delta
                final_hit_tick_id = first_up_hit_tick
                final_hit_ts = first_up_hit_ts
            else:
                final_label = -1
                final_hit_delta = first_down_hit_delta
                final_hit_tick_id = first_down_hit_tick
                final_hit_ts = first_down_hit_ts

            ticks_to_hit = (
                final_hit_tick_id - start_id if final_hit_tick_id is not None else None
            )

            if final_hit_ts is not None and base_ts is not None:
                secs_to_hit = (final_hit_ts - base_ts).total_seconds()
        else:
            final_label = 0

    return SegmentOutcome(
        seg_id=seg_id,
        start_id=start_id,
        end_id=end_id,
        dir_kalseg=dir_kalseg,
        base_price=base_price,
        early_window_ticks=EARLY_WINDOW_TICKS,
        early_target=EARLY_TARGET,
        early_stop=EARLY_STOP,
        early_mfe_up=early_mfe_up,
        early_mfe_down=early_mfe_down,
        early_label=early_label,
        final_target=FINAL_TARGET,
        final_max_ticks=FINAL_MAX_TICKS,
        final_label=final_label,
        final_hit_delta=final_hit_delta,
        final_hit_tick_id=final_hit_tick_id,
        final_hit_ts=final_hit_ts,
        ticks_to_hit=ticks_to_hit,
        secs_to_hit=secs_to_hit,
    )


def upsert_outcome(conn, outcome: SegmentOutcome):
    sql = """
    INSERT INTO kalseg_outcome (
        seg_id, start_id, end_id, dir_kalseg,
        base_price,
        early_window_ticks, early_target, early_stop,
        early_mfe_up, early_mfe_down, early_label,
        final_target, final_max_ticks, final_label,
        final_hit_delta, final_hit_tick_id, final_hit_ts,
        ticks_to_hit, secs_to_hit,
        created_at, updated_at
    ) VALUES (
        %(seg_id)s, %(start_id)s, %(end_id)s, %(dir_kalseg)s,
        %(base_price)s,
        %(early_window_ticks)s, %(early_target)s, %(early_stop)s,
        %(early_mfe_up)s, %(early_mfe_down)s, %(early_label)s,
        %(final_target)s, %(final_max_ticks)s, %(final_label)s,
        %(final_hit_delta)s, %(final_hit_tick_id)s, %(final_hit_ts)s,
        %(ticks_to_hit)s, %(secs_to_hit)s,
        now(), now()
    )
    ON CONFLICT (seg_id) DO UPDATE
       SET start_id           = EXCLUDED.start_id,
           end_id             = EXCLUDED.end_id,
           dir_kalseg         = EXCLUDED.dir_kalseg,
           base_price         = EXCLUDED.base_price,
           early_window_ticks = EXCLUDED.early_window_ticks,
           early_target       = EXCLUDED.early_target,
           early_stop         = EXCLUDED.early_stop,
           early_mfe_up       = EXCLUDED.early_mfe_up,
           early_mfe_down     = EXCLUDED.early_mfe_down,
           early_label        = EXCLUDED.early_label,
           final_target       = EXCLUDED.final_target,
           final_max_ticks    = EXCLUDED.final_max_ticks,
           final_label        = EXCLUDED.final_label,
           final_hit_delta    = EXCLUDED.final_hit_delta,
           final_hit_tick_id  = EXCLUDED.final_hit_tick_id,
           final_hit_ts       = EXCLUDED.final_hit_ts,
           ticks_to_hit       = EXCLUDED.ticks_to_hit,
           secs_to_hit        = EXCLUDED.secs_to_hit,
           updated_at         = now();
    """
    with dict_cur(conn) as cur:
        cur.execute(sql, outcome.__dict__)


def main(limit: int):
    conn = get_conn()
    ensure_outcome_table(conn)

    ts_col = detect_ts_col(conn)
    mid_expr = detect_mid_expr(conn)
    has_kal = ticks_has_kal(conn)

    segs = fetch_first_n_kalseg(conn, limit)
    print(f"Processing {len(segs)} kalseg rows (limit={limit})")

    processed = 0
    for seg in segs:
        start_id = int(seg["start_id"])
        prices = fetch_price_path(
            conn,
            ts_col=ts_col,
            mid_expr=mid_expr,
            has_kal=has_kal,
            start_id=start_id,
            max_ticks=FINAL_MAX_TICKS,
        )
        outcome = compute_outcome_for_segment(seg, prices)
        if outcome is None:
            print(f"Seg {seg['id']}: no price data, skipping")
            continue

        upsert_outcome(conn, outcome)
        processed += 1
        if processed % 100 == 0:
            print(f"... {processed} segments labeled")

    conn.commit()
    print(f"Done. Labeled {processed} segments.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    main(args.limit)
