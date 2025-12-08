# PATH: jobs/buildEvals.py
#
# Stage 2 job: build / maintain the importance ladder (Phase B promotions)
# - Assumes Phase A has already populated evals (base_sign, level, signed_importance, mid).
# - No DB job_journal table; journalling is done to ../journal.txt (cTrade root).
#
# Run as:
#   python -m jobs.buildEvals

import datetime
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from backend.db import get_conn, dict_cur  # reuse existing DB helpers


# ------------ Config ------------

# Must be an odd integer
WINDOW_SIZE = 5

# Number of candidate rows per DB batch for scanning level/sign
PHASE_B_BATCH_SIZE = 100_000

# Small pause between promotion passes (can be 0)
SLEEP_SECONDS = 0.05

# cTrade root: parent of jobs/
ROOT_DIR = Path(__file__).resolve().parents[1]
JOURNAL_PATH = ROOT_DIR / "journal.txt"


# ------------ Types ------------

@dataclass
class EvalsRow:
    tick_id: int
    mid: float
    base_sign: int
    level: int


# ------------ Journal helper (file-based) ------------

def write_journal_line(message: str) -> None:
    """
    Append a single line to journal.txt with UTC ISO timestamp.

    Example line:
      2025-12-09T01:02:03 buildEvals phase_B sign=+1 level=2 promotions=137
    """
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds")
    line = f"{ts} {message}\n"
    # Append mode, avoid read + rewrite to keep it simple and safe
    with JOURNAL_PATH.open("a", encoding="utf-8") as f:
        f.write(line)


# ------------ Phase B core ------------

def iter_candidates(conn, sign: int, level: int):
    """
    Yield candidate rows (base_sign=sign, level=level) ordered by tick_id,
    in batches of PHASE_B_BATCH_SIZE.

    We do NOT assume any symbol column; we operate purely on evals as given.
    """
    last_tick_id = 0

    while True:
        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT tick_id, mid, base_sign, level
                FROM evals
                WHERE base_sign = %s
                  AND level = %s
                  AND tick_id > %s
                ORDER BY tick_id
                LIMIT %s
                """,
                (sign, level, last_tick_id, PHASE_B_BATCH_SIZE),
            )
            rows = cur.fetchall()

        if not rows:
            break

        yield [EvalsRow(**row) for row in rows]
        last_tick_id = rows[-1]["tick_id"]


def phase_b_pass(conn, sign: int, level: int) -> int:
    """
    Run a single promotion pass for a given (sign, level).

    - Sliding window of size WINDOW_SIZE (must be odd).
    - Only the middle tick of each full window can be promoted.
    - sign = +1: middle.mid must be a strict maximum within the window.
    - sign = -1: middle.mid must be a strict minimum within the window.
    - Ties (non-unique max/min) -> no promotion.

    Returns:
        int: number of promoted rows.
    """
    assert WINDOW_SIZE % 2 == 1, "WINDOW_SIZE must be an odd integer"

    window: List[EvalsRow] = []
    to_promote: List[int] = []

    # Overlap between DB batches: last (WINDOW_SIZE - 1) candidates of previous block
    overlap: List[EvalsRow] = []

    for batch in iter_candidates(conn, sign, level):
        # Combine previous overlap with current batch
        candidates = overlap + batch

        for row in candidates:
            window.append(row)
            if len(window) > WINDOW_SIZE:
                window.pop(0)

            if len(window) == WINDOW_SIZE:
                mid_idx = WINDOW_SIZE // 2   # 0-based index for the middle
                middle = window[mid_idx]

                mids = [r.mid for r in window]
                mid_val = mids[mid_idx]

                if sign == 1:
                    # middle must be a strict maximum
                    if mid_val == max(mids) and mids.count(mid_val) == 1:
                        to_promote.append(middle.tick_id)
                else:
                    # sign == -1, middle must be a strict minimum
                    if mid_val == min(mids) and mids.count(mid_val) == 1:
                        to_promote.append(middle.tick_id)

        # Save overlap for the next DB batch: last (WINDOW_SIZE - 1) candidates
        if len(candidates) >= WINDOW_SIZE - 1:
            overlap = candidates[-(WINDOW_SIZE - 1):]
        else:
            overlap = candidates

    if not to_promote:
        write_journal_line(
            f"buildEvals phase_B sign={sign:+d} level={level} promotions=0"
        )
        return 0

    # Deduplicate to be safe (one tick can be middle of overlapping windows)
    unique_ids = sorted(set(to_promote))

    # Apply promotions: level -> level + 1, signed_importance = base_sign * (level + 1)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE evals
            SET
                level = level + 1,
                signed_importance = base_sign * (level + 1),
                computed_at = now()
            WHERE tick_id = ANY(%s)
            """,
            (unique_ids,),
        )

    write_journal_line(
        f"buildEvals phase_B sign={sign:+d} level={level} promotions={len(unique_ids)}"
    )
    return len(unique_ids)


def phase_b_run(conn) -> None:
    """
    Run promotions for both signs (+1, -1):

    For each sign:
        k = 1, 2, 3, ...
        - Run a promotion pass for (sign, k).
        - If promotions == 0 for level k, stop for that sign.
    """
    write_journal_line("buildEvals start_phase_B")

    for sign in (1, -1):
        level = 1
        while True:
            promotions = phase_b_pass(conn, sign, level)
            # autocommit is enabled on this connection, but if that changes,
            # this code still behaves correctly.
            if promotions == 0:
                # No promotions at this level -> no higher levels
                break
            level += 1
            if SLEEP_SECONDS > 0:
                time.sleep(SLEEP_SECONDS)

    write_journal_line("buildEvals finished_phase_B")


# ------------ Main entrypoint ------------

def main() -> None:
    conn = get_conn()
    try:
        # Stage 2: assume Phase A already done, so we just run promotions.
        phase_b_run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
