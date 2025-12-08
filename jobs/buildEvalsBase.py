# PATH: jobs/buildEvalsBase.py
#
# Phase A job: compute base_sign / level / signed_importance for all ticks
# and populate evals (one row per tick).
#
# - No DB job_journal table: journalling is file-based in ../journal.txt
# - Uses backend.db helpers to avoid assumptions about ticks schema.
#
# Run as:
#   python -m jobs.buildEvalsBase

import datetime
from pathlib import Path
from typing import List, Tuple

import psycopg2.extras

from backend.db import (
    get_conn,
    dict_cur,
    detect_mid_expr,
    detect_ts_col,
)

# -------- Config --------

BATCH_SIZE = 100_000  # ticks per batch; adjust if needed

# cTrade root: parent of jobs/
ROOT_DIR = Path(__file__).resolve().parents[1]
JOURNAL_PATH = ROOT_DIR / "journal.txt"


# -------- Journal helper (file-based) --------

def write_journal_line(message: str) -> None:
    """
    Append a single line to journal.txt with UTC ISO timestamp.

    Example:
      2025-12-09T01:02:03 buildEvalsBase phase_A batch last_tick_id=123456 count=100000
    """
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds")
    line = f"{ts} {message}\n"
    with JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(line)


# -------- Phase A core --------

def phase_a_run(conn) -> None:
    """
    Populate / refresh evals for all ticks, in ascending id.

    - Uses detect_mid_expr() and detect_ts_col() to derive price/time.
    - For first tick overall: base_sign = 0.
    - For subsequent ticks: sign(mid[i] - mid[i-1]).
    - level = 1 if base_sign != 0, else 0.
    - signed_importance = base_sign * level.
    - Upsert into evals.
    """
    mid_expr = detect_mid_expr(conn)   # e.g. "price", "mid", "(bid+ask)/2.0"
    ts_col = detect_ts_col(conn)       # e.g. "ts", "timestamp", ...

    last_id = 0
    prev_mid = None

    while True:
        # Stream ticks in ascending id, limited by BATCH_SIZE
        with dict_cur(conn) as cur:
            cur.execute(
                f"""
                SELECT id,
                       {mid_expr} AS mid,
                       {ts_col} AS ts
                FROM ticks
                WHERE id > %s
                ORDER BY id
                LIMIT %s
                """,
                (last_id, BATCH_SIZE),
            )
            rows = cur.fetchall()

        if not rows:
            break

        records: List[Tuple] = []

        for row in rows:
            tick_id = row["id"]
            mid = row["mid"]
            ts = row["ts"]

            if prev_mid is None:
                # Very first tick in entire history
                base_sign = 0
            else:
                if mid > prev_mid:
                    base_sign = 1
                elif mid < prev_mid:
                    base_sign = -1
                else:
                    base_sign = 0

            if base_sign != 0:
                level = 1
                signed_importance = base_sign * 1
            else:
                level = 0
                signed_importance = 0

            prev_mid = mid
            last_id = tick_id

            # tick_id, mid, timestamp, base_sign, level, signed_importance
            records.append((tick_id, mid, ts, base_sign, level, signed_importance))

        if records:
            # Upsert into evals
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO evals
                        (tick_id, mid, "timestamp",
                         base_sign, level, signed_importance, computed_at)
                    VALUES %s
                    ON CONFLICT (tick_id)
                    DO UPDATE SET
                        mid = EXCLUDED.mid,
                        "timestamp" = EXCLUDED."timestamp",
                        base_sign = EXCLUDED.base_sign,
                        level = EXCLUDED.level,
                        signed_importance = EXCLUDED.signed_importance,
                        computed_at = now()
                    """,
                    records,
                    template="(%s,%s,%s,%s,%s,%s,now())",
                )

        write_journal_line(
            f"buildEvalsBase phase_A batch last_tick_id={last_id} count={len(records)}"
        )

    write_journal_line("buildEvalsBase phase_A finished")


# -------- Entrypoint --------

def main() -> None:
    conn = get_conn()
    try:
        phase_a_run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
