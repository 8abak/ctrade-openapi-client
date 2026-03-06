#!/usr/bin/env python3
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL
from ml.kalman import ScalarKalmanConfig, ScalarKalmanFilter


SYMBOL = "XAUUSD"
BATCH_SIZE = 1000
POLL_IDLE_SECONDS = 0.2

KAL_CFG = ScalarKalmanConfig(process_var=1e-4, meas_var=1e-2, init_var=1.0)
K2_CFG = ScalarKalmanConfig(process_var=1e-5, meas_var=5e-3, init_var=1.0)

STOP = False


@dataclass
class CalcState:
    symbol: str
    last_processed_id: int
    kal_x: Optional[float]
    kal_p: float
    k2_x: Optional[float]
    k2_p: float


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tickcalc_state (
                symbol text PRIMARY KEY,
                last_processed_id bigint NOT NULL,
                kal_x double precision NULL,
                kal_p double precision NOT NULL DEFAULT 1.0,
                k2_x double precision NULL,
                k2_p double precision NOT NULL DEFAULT 1.0,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def load_or_init_state(conn, symbol):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT symbol, last_processed_id, kal_x, kal_p, k2_x, k2_p
            FROM tickcalc_state
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if row:
            return CalcState(
                symbol=row["symbol"],
                last_processed_id=int(row["last_processed_id"]),
                kal_x=float(row["kal_x"]) if row["kal_x"] is not None else None,
                kal_p=float(row["kal_p"]) if row["kal_p"] is not None else 1.0,
                k2_x=float(row["k2_x"]) if row["k2_x"] is not None else None,
                k2_p=float(row["k2_p"]) if row["k2_p"] is not None else 1.0,
            )

        cur.execute(
            """
            SELECT id, kal, k2
            FROM ticks
            WHERE symbol = %s
              AND kal IS NOT NULL
              AND k2 IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        seed = cur.fetchone()

    if seed:
        st = CalcState(
            symbol=symbol,
            last_processed_id=int(seed["id"]),
            kal_x=float(seed["kal"]),
            kal_p=1.0,
            k2_x=float(seed["k2"]),
            k2_p=1.0,
        )
    else:
        st = CalcState(
            symbol=symbol,
            last_processed_id=0,
            kal_x=None,
            kal_p=1.0,
            k2_x=None,
            k2_p=1.0,
        )
    save_state(conn, st)
    conn.commit()
    return st


def save_state(conn, st):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tickcalc_state (
                symbol, last_processed_id, kal_x, kal_p, k2_x, k2_p, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (symbol) DO UPDATE SET
                last_processed_id = EXCLUDED.last_processed_id,
                kal_x = EXCLUDED.kal_x,
                kal_p = EXCLUDED.kal_p,
                k2_x = EXCLUDED.k2_x,
                k2_p = EXCLUDED.k2_p,
                updated_at = now()
            """,
            (st.symbol, st.last_processed_id, st.kal_x, st.kal_p, st.k2_x, st.k2_p),
        )


def fetch_batch(conn, symbol, after_id, limit):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp, bid, ask
            FROM ticks
            WHERE symbol = %s
              AND id > %s
              AND (
                  mid IS NULL
               OR spread IS NULL
               OR kal IS NULL
               OR k2 IS NULL
              )
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, after_id, limit),
        )
        return cur.fetchall()


def get_head_id(conn, symbol):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(id), 0)
            FROM ticks
            WHERE symbol = %s
            """,
            (symbol,),
        )
        return int(cur.fetchone()[0] or 0)


def apply_updates(conn, updates):
    if not updates:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE ticks t
            SET mid = v.mid,
                spread = v.spread,
                kal = v.kal,
                k2 = v.k2
            FROM (VALUES %s) AS v(id, mid, spread, kal, k2)
            WHERE t.id = v.id
              AND (
                  t.mid IS DISTINCT FROM v.mid
               OR t.spread IS DISTINCT FROM v.spread
               OR t.kal IS DISTINCT FROM v.kal
               OR t.k2 IS DISTINCT FROM v.k2
              )
            """,
            updates,
            page_size=min(1000, len(updates)),
        )


def handle_signal(_sig, _frame):
    global STOP
    STOP = True


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    conn = db_connect()
    ensure_state_table(conn)
    state = load_or_init_state(conn, SYMBOL)

    kal_filter = ScalarKalmanFilter(KAL_CFG)
    k2_filter = ScalarKalmanFilter(K2_CFG)
    if state.kal_x is not None:
        kal_filter.reset(state.kal_x, state.kal_p)
    if state.k2_x is not None:
        k2_filter.reset(state.k2_x, state.k2_p)

    processed_total = 0
    processed_since = 0
    updated_since = 0
    stats_at = time.time()
    last_ts = None
    last_batch_ms = 0.0
    last_batch_updated = 0

    print(
        f"tickcalc start symbol={SYMBOL} from_id={state.last_processed_id}",
        flush=True,
    )

    while not STOP:
        try:
            rows = fetch_batch(conn, SYMBOL, state.last_processed_id, BATCH_SIZE)
            if not rows:
                time.sleep(POLL_IDLE_SECONDS)
                now = time.time()
                if now - stats_at >= 5.0:
                    rate = processed_since / (now - stats_at) if now > stats_at else 0.0
                    head_id = get_head_id(conn, SYMBOL)
                    behind = max(0, head_id - state.last_processed_id)
                    lag = None
                    if last_ts is not None:
                        lag = (datetime.now(timezone.utc) - last_ts.astimezone(timezone.utc)).total_seconds()
                    if lag is None:
                        print(
                            f"tickcalc stats rate={rate:.1f}/s updated={updated_since} behind={behind} last_id={state.last_processed_id} batch_updated={last_batch_updated} batch_ms={last_batch_ms:.2f}",
                            flush=True,
                        )
                    else:
                        print(
                            f"tickcalc stats rate={rate:.1f}/s updated={updated_since} behind={behind} lag={lag:.3f}s last_id={state.last_processed_id} batch_updated={last_batch_updated} batch_ms={last_batch_ms:.2f}",
                            flush=True,
                        )
                    processed_since = 0
                    updated_since = 0
                    stats_at = now
                continue

            batch_started = time.time()
            updates = []
            for row in rows:
                bid = float(row["bid"]) if row["bid"] is not None else None
                ask = float(row["ask"]) if row["ask"] is not None else None
                if bid is None or ask is None:
                    continue

                mid = round((bid + ask) / 2.0, 2)
                spread = round(ask - bid, 2)
                kal = round(kal_filter.step(mid), 2)
                k2 = round(k2_filter.step(kal), 2)
                updates.append((int(row["id"]), mid, spread, kal, k2))

            if updates:
                apply_updates(conn, updates)
            last_batch_updated = len(updates)

            last_row = rows[-1]
            state.last_processed_id = int(last_row["id"])
            state.kal_x = kal_filter.x
            state.kal_p = kal_filter.P if kal_filter.P is not None else state.kal_p
            state.k2_x = k2_filter.x
            state.k2_p = k2_filter.P if k2_filter.P is not None else state.k2_p
            save_state(conn, state)
            conn.commit()

            processed_total += len(rows)
            processed_since += len(rows)
            updated_since += len(updates)
            last_ts = last_row["timestamp"]
            last_batch_ms = (time.time() - batch_started) * 1000.0

            now = time.time()
            if now - stats_at >= 5.0:
                rate = processed_since / (now - stats_at) if now > stats_at else 0.0
                head_id = get_head_id(conn, SYMBOL)
                behind = max(0, head_id - state.last_processed_id)
                lag = None
                if last_ts is not None:
                    lag = (datetime.now(timezone.utc) - last_ts.astimezone(timezone.utc)).total_seconds()
                if lag is None:
                    print(
                        f"tickcalc stats processed={processed_total} rate={rate:.1f}/s updated={updated_since} behind={behind} last_id={state.last_processed_id} batch_updated={last_batch_updated} batch_ms={last_batch_ms:.2f}",
                        flush=True,
                    )
                else:
                    print(
                        f"tickcalc stats processed={processed_total} rate={rate:.1f}/s updated={updated_since} behind={behind} lag={lag:.3f}s last_id={state.last_processed_id} batch_updated={last_batch_updated} batch_ms={last_batch_ms:.2f}",
                        flush=True,
                    )
                processed_since = 0
                updated_since = 0
                stats_at = now

        except Exception as e:
            print(f"tickcalc error: {e}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(1.0)

    try:
        conn.close()
    except Exception:
        pass
    print("tickcalc stopped", flush=True)


if __name__ == "__main__":
    main()
