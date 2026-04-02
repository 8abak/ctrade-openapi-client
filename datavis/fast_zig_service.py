#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

import psycopg2.extras

from datavis.db import db_connect


TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
POLL_SECONDS = max(0.02, float(os.getenv("FAST_ZIG_POLL_SECONDS", "0.05")))
IDLE_POLL_SECONDS = max(POLL_SECONDS, float(os.getenv("FAST_ZIG_IDLE_POLL_SECONDS", "0.10")))
BATCH_SIZE = max(1, int(os.getenv("FAST_ZIG_BATCH_SIZE", "200")))
WINDOW_SIZE = 15
CENTER_INDEX = 7
RECENT_TICK_COUNT = WINDOW_SIZE - 1

STOP = False


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.fast_zig_state (
    symbol text PRIMARY KEY,
    last_processed_tick_id bigint NOT NULL DEFAULT 0,
    last_pivot_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.fast_zig_pivots (
    version_id bigserial PRIMARY KEY,
    pivot_id bigint NOT NULL,
    symbol text NOT NULL,
    source_tick_id bigint NOT NULL,
    source_timestamp timestamptz NOT NULL,
    direction text NOT NULL CHECK (direction IN ('high', 'low')),
    pivot_price double precision NOT NULL,
    visible_from_tick_id bigint NOT NULL,
    visible_to_tick_id bigint NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS fast_zig_state_symbol_idx
    ON public.fast_zig_state (symbol);

CREATE UNIQUE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_idx
    ON public.fast_zig_pivots (symbol, pivot_id)
    WHERE visible_to_tick_id IS NULL;

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_source_tick_idx
    ON public.fast_zig_pivots (symbol, source_tick_id, pivot_id, version_id);

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_visible_from_idx
    ON public.fast_zig_pivots (symbol, visible_from_tick_id, pivot_id, version_id);

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_order_idx
    ON public.fast_zig_pivots (symbol, pivot_id, version_id)
    WHERE visible_to_tick_id IS NULL;
"""


def shutdown(*_: Any) -> None:
    global STOP
    STOP = True


def ensure_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def load_state(cur: Any) -> Dict[str, int]:
    cur.execute(
        """
        SELECT last_processed_tick_id, last_pivot_id
        FROM public.fast_zig_state
        WHERE symbol = %s
        """,
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    if row:
        return {
            "last_processed_tick_id": int(row["last_processed_tick_id"] or 0),
            "last_pivot_id": int(row["last_pivot_id"] or 0),
        }

    cur.execute(
        """
        INSERT INTO public.fast_zig_state (symbol, last_processed_tick_id, last_pivot_id)
        VALUES (%s, 0, 0)
        ON CONFLICT (symbol) DO NOTHING
        """,
        (TICK_SYMBOL,),
    )
    return {"last_processed_tick_id": 0, "last_pivot_id": 0}


def store_state(cur: Any, *, last_processed_tick_id: int, last_pivot_id: int) -> None:
    cur.execute(
        """
        INSERT INTO public.fast_zig_state (symbol, last_processed_tick_id, last_pivot_id, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE
        SET last_processed_tick_id = EXCLUDED.last_processed_tick_id,
            last_pivot_id = EXCLUDED.last_pivot_id,
            updated_at = NOW()
        """,
        (TICK_SYMBOL, last_processed_tick_id, last_pivot_id),
    )


def fetch_recent_ticks(cur: Any, last_processed_tick_id: int) -> List[Dict[str, Any]]:
    if last_processed_tick_id <= 0:
        return []
    cur.execute(
        """
        SELECT id, timestamp, bid, ask, mid
        FROM (
            SELECT id, timestamp, bid, ask, mid
            FROM public.ticks
            WHERE symbol = %s AND id <= %s
            ORDER BY id DESC
            LIMIT %s
        ) recent
        ORDER BY id ASC
        """,
        (TICK_SYMBOL, last_processed_tick_id, RECENT_TICK_COUNT),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_ticks_after(cur: Any, after_id: int, limit: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, timestamp, bid, ask, mid
        FROM public.ticks
        WHERE symbol = %s AND id > %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (TICK_SYMBOL, after_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def load_last_pivot(cur: Any) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT version_id, pivot_id, direction, pivot_price, source_tick_id
        FROM public.fast_zig_pivots
        WHERE symbol = %s AND visible_to_tick_id IS NULL
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def detect_pivot(window_ticks: Deque[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(window_ticks) != WINDOW_SIZE:
        return None

    center = window_ticks[CENTER_INDEX]
    asks = [row["ask"] for row in window_ticks]
    bids = [row["bid"] for row in window_ticks]
    if any(value is None for value in asks) or any(value is None for value in bids):
        return None

    is_high = center["ask"] == max(asks)
    is_low = center["bid"] == min(bids)
    if is_high and is_low:
        return None
    if is_high:
        return {
            "direction": "high",
            "price": float(center["ask"]),
            "source_tick_id": int(center["id"]),
            "source_timestamp": center["timestamp"],
        }
    if is_low:
        return {
            "direction": "low",
            "price": float(center["bid"]),
            "source_tick_id": int(center["id"]),
            "source_timestamp": center["timestamp"],
        }
    return None


def is_more_extreme(previous: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    if previous["direction"] == "high":
        return float(candidate["price"]) > float(previous["pivot_price"])
    return float(candidate["price"]) < float(previous["pivot_price"])


def insert_pivot_version(
    cur: Any,
    *,
    pivot_id: int,
    candidate: Dict[str, Any],
    decision_tick_id: int,
) -> Dict[str, Any]:
    cur.execute(
        """
        INSERT INTO public.fast_zig_pivots (
            pivot_id,
            symbol,
            source_tick_id,
            source_timestamp,
            direction,
            pivot_price,
            visible_from_tick_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING version_id, pivot_id, direction, pivot_price, source_tick_id
        """,
        (
            pivot_id,
            TICK_SYMBOL,
            candidate["source_tick_id"],
            candidate["source_timestamp"],
            candidate["direction"],
            candidate["price"],
            decision_tick_id,
        ),
    )
    return dict(cur.fetchone())


def replace_current_pivot_version(cur: Any, current: Dict[str, Any], *, decision_tick_id: int) -> None:
    cur.execute(
        """
        UPDATE public.fast_zig_pivots
        SET visible_to_tick_id = %s
        WHERE version_id = %s
        """,
        (decision_tick_id - 1, current["version_id"]),
    )


def apply_pivot(
    cur: Any,
    current: Optional[Dict[str, Any]],
    candidate: Dict[str, Any],
    *,
    last_pivot_id: int,
    decision_tick_id: int,
) -> tuple[Optional[Dict[str, Any]], int]:
    if current is None:
        next_pivot_id = max(1, last_pivot_id + 1)
        return insert_pivot_version(cur, pivot_id=next_pivot_id, candidate=candidate, decision_tick_id=decision_tick_id), next_pivot_id

    if candidate["direction"] == current["direction"]:
        if not is_more_extreme(current, candidate):
            return current, max(last_pivot_id, int(current["pivot_id"]))
        replace_current_pivot_version(cur, current, decision_tick_id=decision_tick_id)
        return (
            insert_pivot_version(cur, pivot_id=int(current["pivot_id"]), candidate=candidate, decision_tick_id=decision_tick_id),
            max(last_pivot_id, int(current["pivot_id"])),
        )

    next_pivot_id = max(last_pivot_id, int(current["pivot_id"])) + 1
    return insert_pivot_version(cur, pivot_id=next_pivot_id, candidate=candidate, decision_tick_id=decision_tick_id), next_pivot_id


def log_progress(*, last_processed_tick_id: int, last_pivot_id: int, batch_count: int, batch_ms: float) -> None:
    print(
        "fast-zig stats symbol={0} tick={1} pivots={2} batch={3} batch_ms={4:.2f}".format(
            TICK_SYMBOL,
            last_processed_tick_id,
            last_pivot_id,
            batch_count,
            batch_ms,
        ),
        flush=True,
    )


def run_loop() -> None:
    last_log = time.time()
    idle_sleep = POLL_SECONDS

    while not STOP:
        conn = None
        try:
            conn = db_connect()
            conn.autocommit = False
            ensure_schema(conn)

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                state = load_state(cur)
                conn.commit()

                last_processed_tick_id = int(state["last_processed_tick_id"])
                last_pivot_id = int(state["last_pivot_id"])
                current_pivot = load_last_pivot(cur)
                if current_pivot:
                    last_pivot_id = max(last_pivot_id, int(current_pivot["pivot_id"]))
                window_ticks: Deque[Dict[str, Any]] = deque(
                    fetch_recent_ticks(cur, last_processed_tick_id),
                    maxlen=WINDOW_SIZE,
                )

                while not STOP:
                    batch_started = time.perf_counter()
                    rows = fetch_ticks_after(cur, last_processed_tick_id, BATCH_SIZE)
                    if not rows:
                        conn.rollback()
                        time.sleep(idle_sleep)
                        idle_sleep = IDLE_POLL_SECONDS
                        continue

                    idle_sleep = POLL_SECONDS
                    for row in rows:
                        window_ticks.append(row)
                        if len(window_ticks) == WINDOW_SIZE:
                            candidate = detect_pivot(window_ticks)
                            if candidate:
                                current_pivot, last_pivot_id = apply_pivot(
                                    cur,
                                    current_pivot,
                                    candidate,
                                    last_pivot_id=last_pivot_id,
                                    decision_tick_id=int(row["id"]),
                                )
                        last_processed_tick_id = int(row["id"])

                    store_state(
                        cur,
                        last_processed_tick_id=last_processed_tick_id,
                        last_pivot_id=last_pivot_id,
                    )
                    conn.commit()

                    now = time.time()
                    if now - last_log >= 5.0:
                        log_progress(
                            last_processed_tick_id=last_processed_tick_id,
                            last_pivot_id=last_pivot_id,
                            batch_count=len(rows),
                            batch_ms=(time.perf_counter() - batch_started) * 1000.0,
                        )
                        last_log = now

        except Exception as exc:
            print("fast-zig error: {0}".format(exc), flush=True)
            try:
                if conn and not conn.closed:
                    conn.rollback()
            except Exception:
                pass
            time.sleep(1.0)
        finally:
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass


def main() -> None:
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    run_loop()


if __name__ == "__main__":
    main()
