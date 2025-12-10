# PATH: jobs/buildKalman.py
#
# Fill ticks.kal using a 1D Kalman filter, walking forward in id order.
#
# Usage examples (from project root):
#
#   # Fill all XAUUSD ticks where kal IS NULL, in chunks of 20k
#   python -m jobs.buildKalman --symbol XAUUSD --chunk-size 20000
#
#   # Dry-run (no updates), just print how many would be set:
#   python -m jobs.buildKalman --symbol XAUUSD --dry-run
#
# Behaviour:
#   - Detects mid expression via backend.db.detect_mid_expr()
#   - Scans ticks for the given symbol in id order.
#   - For each tick:
#       * step the Kalman filter using mid as observation
#       * if kal IS NULL, write the Kalman state into kal
#       * if kal is already non-NULL, we keep it but still feed mid
#         into the filter for continuity.
#   - Works chunk-by-chunk to avoid loading whole table into memory.

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional, List, Tuple

from backend.db import get_conn, dict_cur, detect_mid_expr  # type: ignore
from ml.kalman import ScalarKalmanConfig, ScalarKalmanFilter


@dataclass
class JobConfig:
  symbol: str
  chunk_size: int = 20000
  process_var: float = 1e-4
  meas_var: float = 1e-2
  init_var: float = 1.0
  start_id: Optional[int] = None   # if None, start from very first tick for symbol
  dry_run: bool = False


def get_id_range_for_symbol(conn, symbol: str) -> Optional[Tuple[int, int]]:
  """
  Return (min_id, max_id) for ticks of this symbol, or None if none exist.
  """
  with dict_cur(conn) as cur:
    cur.execute(
      """
      SELECT MIN(id) AS min_id,
             MAX(id) AS max_id
      FROM ticks
      WHERE symbol = %s
      """,
      (symbol,),
    )
    row = cur.fetchone()
    if not row or row["min_id"] is None or row["max_id"] is None:
      return None
    return int(row["min_id"]), int(row["max_id"])


def iterate_ticks_chunk(
  conn,
  symbol: str,
  mid_expr: str,
  from_id: int,
  to_id: int,
):
  """
  Yield rows in [from_id, to_id] for a symbol, ordered by id.
  Each row: {"id", "mid", "kal"}.
  """
  with dict_cur(conn) as cur:
    cur.execute(
      f"""
      SELECT id,
             {mid_expr} AS mid,
             kal
      FROM ticks
      WHERE symbol = %s
        AND id BETWEEN %s AND %s
      ORDER BY id ASC
      """,
      (symbol, from_id, to_id),
    )
    for row in cur.fetchall():
      yield {
        "id": int(row["id"]),
        "mid": float(row["mid"]),
        "kal": float(row["kal"]) if row["kal"] is not None else None,
      }


def update_kal_values(
  conn,
  updates: List[Tuple[float, int]],
  dry_run: bool,
) -> None:
  """
  Perform batch UPDATE ticks SET kal = %s WHERE id = %s for all updates.
  """
  if not updates:
    return

  if dry_run:
    return  # don't actually write

  with conn, dict_cur(conn) as cur:
    cur.executemany(
      """
      UPDATE ticks
      SET kal = %s
      WHERE id = %s
      """,
      updates,
    )


def run_job(cfg: JobConfig) -> None:
  conn = get_conn()
  mid_expr = detect_mid_expr(conn)

  id_range = get_id_range_for_symbol(conn, cfg.symbol)
  if id_range is None:
    print(f"No ticks found for symbol={cfg.symbol}.")
    return

  min_id, max_id = id_range
  start_id = cfg.start_id if cfg.start_id is not None else min_id

  if start_id < min_id:
    start_id = min_id

  print(
    f"Building Kalman for symbol={cfg.symbol}, "
    f"id range [{start_id}, {max_id}], "
    f"chunk_size={cfg.chunk_size}, dry_run={cfg.dry_run}"
  )

  # Set up Kalman filter
  kcfg = ScalarKalmanConfig(
    process_var=cfg.process_var,
    meas_var=cfg.meas_var,
    init_var=cfg.init_var,
  )
  kf = ScalarKalmanFilter(kcfg)

  # Optional: warm-start from last non-NULL kal before start_id
  with dict_cur(conn) as cur:
    cur.execute(
      """
      SELECT id, kal
      FROM ticks
      WHERE symbol = %s
        AND id < %s
        AND kal IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
      """,
      (cfg.symbol, start_id),
    )
    row = cur.fetchone()

  if row and row["kal"] is not None:
    x0 = float(row["kal"])
    print(f"Warm-starting Kalman from existing kal at id={row['id']}, kal={x0:.5f}")
    kf.reset(x0=x0, P0=kcfg.init_var)
  else:
    # We'll initialise on the first observation we see.
    print("No existing kal before start_id; Kalman will initialise on first tick.")

  current_id = start_id
  total_seen = 0
  total_updated = 0

  while current_id <= max_id:
    chunk_to_id = min(max_id, current_id + cfg.chunk_size - 1)
    rows = list(iterate_ticks_chunk(conn, cfg.symbol, mid_expr, current_id, chunk_to_id))
    if not rows:
      break

    updates: List[Tuple[float, int]] = []

    for r in rows:
      z = r["mid"]
      x_kal = kf.step(z)
      total_seen += 1

      if r["kal"] is None:
        updates.append((x_kal, r["id"]))
        total_updated += 1

    update_kal_values(conn, updates, cfg.dry_run)

    print(
      f"Processed ids [{current_id}, {chunk_to_id}], "
      f"rows={len(rows)}, updated={len(updates)}, "
      f"total_seen={total_seen}, total_updated={total_updated}"
    )

    current_id = chunk_to_id + 1

  print(f"Done. total_seen={total_seen}, total_updated={total_updated}, dry_run={cfg.dry_run}")


def main() -> None:
  parser = argparse.ArgumentParser()
  parser.add_argument("--symbol", required=True, help="Symbol to process (e.g. XAUUSD).")
  parser.add_argument("--chunk-size", type=int, default=20000,
                      help="How many ticks to process per DB chunk.")
  parser.add_argument("--start-id", type=int, default=None,
                      help="Optional starting tick id (default: earliest id for symbol).")
  parser.add_argument("--process-var", type=float, default=1e-4,
                      help="Kalman process variance Q (higher = more responsive).")
  parser.add_argument("--meas-var", type=float, default=1e-2,
                      help="Kalman measurement variance R (higher = smoother).")
  parser.add_argument("--init-var", type=float, default=1.0,
                      help="Initial variance P0.")
  parser.add_argument("--dry-run", action="store_true",
                      help="If set, do not write updates, just report counts.")

  args = parser.parse_args()

  cfg = JobConfig(
    symbol=args.symbol,
    chunk_size=args.chunk_size,
    process_var=args.process_var,
    meas_var=args.meas_var,
    init_var=args.init_var,
    start_id=args.start_id,
    dry_run=args.dry_run,
  )

  run_job(cfg)


if __name__ == "__main__":
  main()
