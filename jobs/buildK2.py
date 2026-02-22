# jobs/buildK2.py
#
# Build ticks.k2 as a "Kalman of kalman" (second-pass denoising):
#   measurement z := ticks.kal
#   output      x := ticks.k2
#
# Design goals:
# - Very light DB load (small batches + sleep)
# - Resume-safe (stores filter state per symbol)
# - Logs progress to logs/buildK2.log
#
# Run:
#   mkdir -p logs
#   nohup python -m jobs.buildK2 >/dev/null 2>&1 &
#   tail -f logs/buildK2.log
#
from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn, dict_cur  # backend-standard DB access


# ------------------------------ CONFIG ---------------------------------

# Keep this SMALL to avoid pressure on Postgres / disk.
FETCH_BATCH_ROWS = 25_000

# Sleep between batches to keep it "slow and steady".
SLEEP_SECONDS = 0.20

# Kalman params (random-walk model):
# - Higher R => smoother (trust measurements less)
# - Higher Q => more responsive (allow estimate to move more)
#
# Since kal is already smoothed, we typically make k2 even smoother:
Q = 1e-5
R = 5e-3

# Log every N processed ticks
LOG_EVERY_TICKS = 1_000_000

LOG_PATH = "logs/buildK2.log"


# ------------------------------ DATA -----------------------------------

@dataclass
class TickRow:
    id: int
    ts: datetime
    kal: float


@dataclass
class K2State:
    symbol: str
    last_ts: datetime
    last_id: int
    x: float
    p: float
    processed: int


# ---------------------------- KALMAN -----------------------------------

def kalman_step(x: float, p: float, z: float, q: float, r: float) -> Tuple[float, float]:
    """
    1D Kalman filter, random-walk state model:
      x_k = x_{k-1} + w,  w ~ N(0, q)
      z_k = x_k + v,      v ~ N(0, r)
    """
    # Predict
    p = p + q

    # Update
    k = p / (p + r)  # gain
    x = x + k * (z - x)
    p = (1.0 - k) * p
    return x, p


# ----------------------------- DB --------------------------------------

def ensure_state_table(conn) -> None:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS k2_state (
                symbol      text PRIMARY KEY,
                last_ts     timestamptz NOT NULL,
                last_id     bigint      NOT NULL,
                x           double precision NOT NULL,
                p           double precision NOT NULL,
                processed   bigint      NOT NULL DEFAULT 0,
                updated_at  timestamptz NOT NULL DEFAULT now()
            );
            """
        )
    conn.commit()


def list_symbols(conn) -> List[str]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM ticks
            WHERE symbol IS NOT NULL
            ORDER BY symbol
            """
        )
        return [r["symbol"] for r in cur.fetchall()]


def get_first_tick_key(conn, symbol: str) -> Optional[Tuple[datetime, int, float]]:
    """Return (timestamp, id, kal) for earliest tick with kal not null."""
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT timestamp, id, kal
            FROM ticks
            WHERE symbol = %s
              AND kal IS NOT NULL
            ORDER BY timestamp ASC, id ASC
            LIMIT 1
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["timestamp"], int(r["id"]), float(r["kal"]))


def get_last_tick_key(conn, symbol: str) -> Optional[Tuple[datetime, int]]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT timestamp, id
            FROM ticks
            WHERE symbol = %s
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return (r["timestamp"], int(r["id"]))


def load_state(conn, symbol: str) -> Optional[K2State]:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT symbol, last_ts, last_id, x, p, processed
            FROM k2_state
            WHERE symbol = %s
            """,
            (symbol,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return K2State(
            symbol=r["symbol"],
            last_ts=r["last_ts"],
            last_id=int(r["last_id"]),
            x=float(r["x"]),
            p=float(r["p"]),
            processed=int(r["processed"]),
        )


def save_state(conn, st: K2State) -> None:
    with dict_cur(conn) as cur:
        cur.execute(
            """
            INSERT INTO k2_state (symbol, last_ts, last_id, x, p, processed, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (symbol) DO UPDATE SET
                last_ts   = EXCLUDED.last_ts,
                last_id   = EXCLUDED.last_id,
                x         = EXCLUDED.x,
                p         = EXCLUDED.p,
                processed = EXCLUDED.processed,
                updated_at= now()
            """,
            (st.symbol, st.last_ts, st.last_id, st.x, st.p, st.processed),
        )


def fetch_next_batch(conn, symbol: str, after_key: Tuple[datetime, int], limit: int) -> List[TickRow]:
    """
    Fetch next ticks strictly after (ts,id), ordered (ts,id).
    We compute k2 from kal, but we only update rows where k2 IS NULL (so we can safely resume).
    """
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, timestamp, kal
            FROM ticks
            WHERE symbol = %s
              AND (timestamp, id) > (%s, %s)
              AND kal IS NOT NULL
              AND k2 IS NULL
            ORDER BY timestamp ASC, id ASC
            LIMIT %s
            """,
            (symbol, after_key[0], after_key[1], limit),
        )
        rows = cur.fetchall()

    out: List[TickRow] = []
    for r in rows:
        out.append(TickRow(id=int(r["id"]), ts=r["timestamp"], kal=float(r["kal"])))
    return out


def bulk_update_k2(conn, pairs: List[Tuple[int, float]]) -> None:
    """
    pairs: [(tick_id, k2_value), ...]
    """
    if not pairs:
        return
    with dict_cur(conn) as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE ticks t
            SET k2 = v.k2
            FROM (VALUES %s) AS v(id, k2)
            WHERE t.id = v.id
            """,
            pairs,
            page_size=10_000,
        )


# ---------------------------- MAIN -------------------------------------

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("buildK2")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if re-imported.
    if not logger.handlers:
        fh = logging.FileHandler(LOG_PATH)
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def main() -> None:
    logger = setup_logging()
    logger.info("START buildK2 | batch=%s sleep=%.2fs Q=%g R=%g", FETCH_BATCH_ROWS, SLEEP_SECONDS, Q, R)

    conn = get_conn()
    conn.autocommit = False

    ensure_state_table(conn)

    symbols = list_symbols(conn)
    if not symbols:
        logger.info("No symbols found in ticks. Exit.")
        return

    logger.info("Symbols: %s", symbols)

    for symbol in symbols:
        try:
            last_key = get_last_tick_key(conn, symbol)
            if last_key is None:
                logger.info("symbol=%s no ticks -> skip", symbol)
                continue

            st = load_state(conn, symbol)
            if st is None:
                first = get_first_tick_key(conn, symbol)
                if first is None:
                    logger.info("symbol=%s no ticks with kal -> skip", symbol)
                    continue

                # Initialize filter with first kal as x, and a modest initial uncertainty.
                first_ts, first_id, first_kal = first
                st = K2State(
                    symbol=symbol,
                    last_ts=first_ts,
                    last_id=first_id,
                    x=first_kal,
                    p=1.0,
                    processed=0,
                )

                # Also write k2 for the first tick (if still NULL)
                try:
                    with dict_cur(conn) as cur:
                        cur.execute(
                            "UPDATE ticks SET k2=%s WHERE id=%s AND k2 IS NULL",
                            (st.x, st.last_id),
                        )
                    save_state(conn, st)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                logger.info(
                    "symbol=%s initialized at first tick id=%s ts=%s x=%.6f",
                    symbol, st.last_id, st.last_ts.isoformat(), st.x,
                )

            logger.info(
                "symbol=%s resume from id=%s ts=%s processed=%s",
                symbol, st.last_id, st.last_ts.isoformat(), st.processed,
            )

            next_milestone = (st.processed // LOG_EVERY_TICKS) * LOG_EVERY_TICKS + LOG_EVERY_TICKS

            while (st.last_ts, st.last_id) < last_key:
                batch = fetch_next_batch(conn, symbol, (st.last_ts, st.last_id), FETCH_BATCH_ROWS)
                if not batch:
                    # Either up-to-date, or there are rows but already filled with k2.
                    # Refresh last_key in case new ticks arrived.
                    last_key = get_last_tick_key(conn, symbol) or last_key
                    if (st.last_ts, st.last_id) >= last_key:
                        break
                    time.sleep(1.0)
                    continue

                updates: List[Tuple[int, float]] = []

                for row in batch:
                    st.x, st.p = kalman_step(st.x, st.p, row.kal, Q, R)
                    updates.append((row.id, float(st.x)))
                    st.last_ts = row.ts
                    st.last_id = row.id
                    st.processed += 1

                    if st.processed >= next_milestone:
                        logger.info(
                            "symbol=%s processed=%s last_id=%s last_ts=%s x=%.6f",
                            symbol, st.processed, st.last_id, st.last_ts.isoformat(), st.x
                        )
                        next_milestone += LOG_EVERY_TICKS

                # Commit one batch = one transaction (light + safe)
                try:
                    bulk_update_k2(conn, updates)
                    save_state(conn, st)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

                # Slow down intentionally
                time.sleep(SLEEP_SECONDS)

                # Refresh tail occasionally
                if st.processed % (FETCH_BATCH_ROWS * 10) == 0:
                    last_key = get_last_tick_key(conn, symbol) or last_key

            logger.info(
                "symbol=%s DONE processed=%s last_id=%s last_ts=%s",
                symbol, st.processed, st.last_id, st.last_ts.isoformat()
            )

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.exception("symbol=%s ERROR: %s", symbol, str(e))

    logger.info("END buildK2")


if __name__ == "__main__":
    main()