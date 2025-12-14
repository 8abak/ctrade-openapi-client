# PATH: backend/jobs/breakLinesFull.py

"""
Break segLines repeatedly until max_abs_dist < THRESHOLD.
Run manually on server.

Usage:
  python backend/jobs/breakLinesFull.py --segm 118 --threshold 3.0
"""

import argparse
from backend.db import get_conn, dict_cur
from backend.jobs.breakLine import break_single_line  # reuse logic

THRESHOLD = 3.0


def run(segm_id: int, threshold: float):
    conn = get_conn()

    while True:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, max_abs_dist
                FROM seglines
                WHERE segm_id = %s
                  AND is_active = true
                ORDER BY max_abs_dist DESC
                LIMIT 1
                """,
                (segm_id,),
            )
            row = cur.fetchone()

        if not row:
            print("No active lines left.")
            break

        line_id = row["id"]
        max_dist = row["max_abs_dist"]

        print(f"Top line {line_id}, max |dist| = {max_dist:.4f}")

        if max_dist is None or max_dist < threshold:
            print("✓ All lines below threshold. Done.")
            break

        print(f"→ Breaking line {line_id}")
        break_single_line(
            segm_id=segm_id,
            segLine_id=line_id,
            conn=conn,
            journal=True,
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm", type=int, required=True)
    ap.add_argument("--threshold", type=float, default=THRESHOLD)
    args = ap.parse_args()

    run(args.segm, args.threshold)
