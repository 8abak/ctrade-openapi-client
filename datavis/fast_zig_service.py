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
PIVOT_WINDOW_SIZE = 9
PIVOT_CENTER_INDEX = 4
MAX_LEVEL = 3

STOP = False


def shutdown(*_: Any) -> None:
    global STOP
    STOP = True


def ensure_storage_ready(conn: Any) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                to_regclass('public.fast_zig_state') AS state_table,
                to_regclass('public.fast_zig_pivots') AS pivots_table
            """
        )
        row = dict(cur.fetchone() or {})
    if not row.get("state_table") or not row.get("pivots_table"):
        raise RuntimeError("fast zig tables are missing; apply deploy/sql/20260403_fast_zig.sql first")


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
        SELECT
            version_id,
            pivot_id,
            direction,
            pivot_price,
            source_tick_id,
            source_timestamp,
            level
        FROM public.fast_zig_pivots
        WHERE symbol = %s AND visible_to_tick_id IS NULL
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_recent_current_pivots(cur: Any, *, minimum_level: int, limit: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            version_id,
            pivot_id,
            direction,
            pivot_price,
            source_tick_id,
            source_timestamp,
            level
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND visible_to_tick_id IS NULL
          AND level >= %s
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT %s
        """,
        (TICK_SYMBOL, minimum_level, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    rows.reverse()
    return rows


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
    previous_price = float(previous["pivot_price"])
    candidate_price = float(candidate["price"])
    if previous["direction"] == "high":
        return candidate_price > previous_price
    return candidate_price < previous_price


def close_pivot_version(cur: Any, *, version_id: int, decision_tick_id: int) -> None:
    cur.execute(
        """
        UPDATE public.fast_zig_pivots
        SET visible_to_tick_id = %s,
            updated_at = NOW()
        WHERE version_id = %s
        """,
        (decision_tick_id - 1, version_id),
    )


def insert_pivot_version(
    cur: Any,
    *,
    pivot_id: int,
    direction: str,
    pivot_price: float,
    source_tick_id: int,
    source_timestamp: Any,
    level: int,
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
            level,
            visible_from_tick_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING
            version_id,
            pivot_id,
            direction,
            pivot_price,
            source_tick_id,
            source_timestamp,
            level
        """,
        (
            pivot_id,
            TICK_SYMBOL,
            source_tick_id,
            source_timestamp,
            direction,
            pivot_price,
            level,
            decision_tick_id,
        ),
    )
    return dict(cur.fetchone())


def replace_with_candidate(
    cur: Any,
    current: Dict[str, Any],
    *,
    candidate: Dict[str, Any],
    decision_tick_id: int,
) -> Dict[str, Any]:
    close_pivot_version(cur, version_id=int(current["version_id"]), decision_tick_id=decision_tick_id)
    return insert_pivot_version(
        cur,
        pivot_id=int(current["pivot_id"]),
        direction=str(candidate["direction"]),
        pivot_price=float(candidate["price"]),
        source_tick_id=int(candidate["source_tick_id"]),
        source_timestamp=candidate["source_timestamp"],
        level=int(current["level"] or 1),
        decision_tick_id=decision_tick_id,
    )


def promote_pivot(cur: Any, current: Dict[str, Any], *, target_level: int, decision_tick_id: int) -> Dict[str, Any]:
    close_pivot_version(cur, version_id=int(current["version_id"]), decision_tick_id=decision_tick_id)
    return insert_pivot_version(
        cur,
        pivot_id=int(current["pivot_id"]),
        direction=str(current["direction"]),
        pivot_price=float(current["pivot_price"]),
        source_tick_id=int(current["source_tick_id"]),
        source_timestamp=current["source_timestamp"],
        level=target_level,
        decision_tick_id=decision_tick_id,
    )


def apply_level_one(
    cur: Any,
    current: Optional[Dict[str, Any]],
    candidate: Dict[str, Any],
    *,
    last_pivot_id: int,
    decision_tick_id: int,
) -> tuple[Optional[Dict[str, Any]], int, bool]:
    if current is None:
        next_pivot_id = max(1, last_pivot_id + 1)
        inserted = insert_pivot_version(
            cur,
            pivot_id=next_pivot_id,
            direction=str(candidate["direction"]),
            pivot_price=float(candidate["price"]),
            source_tick_id=int(candidate["source_tick_id"]),
            source_timestamp=candidate["source_timestamp"],
            level=1,
            decision_tick_id=decision_tick_id,
        )
        return inserted, next_pivot_id, True

    if candidate["direction"] == current["direction"]:
        if not is_more_extreme(current, candidate):
            return current, max(last_pivot_id, int(current["pivot_id"])), False
        replaced = replace_with_candidate(cur, current, candidate=candidate, decision_tick_id=decision_tick_id)
        return replaced, max(last_pivot_id, int(current["pivot_id"])), True

    next_pivot_id = max(last_pivot_id, int(current["pivot_id"])) + 1
    inserted = insert_pivot_version(
        cur,
        pivot_id=next_pivot_id,
        direction=str(candidate["direction"]),
        pivot_price=float(candidate["price"]),
        source_tick_id=int(candidate["source_tick_id"]),
        source_timestamp=candidate["source_timestamp"],
        level=1,
        decision_tick_id=decision_tick_id,
    )
    return inserted, next_pivot_id, True


def qualifies_for_promotion(pivots: List[Dict[str, Any]], *, target_level: int) -> Optional[Dict[str, Any]]:
    if len(pivots) != PIVOT_WINDOW_SIZE:
        return None

    center = pivots[PIVOT_CENTER_INDEX]
    if int(center["level"] or 1) >= target_level:
        return None

    peers = [row for row in pivots if row["direction"] == center["direction"]]
    center_price = float(center["pivot_price"])
    other_prices = [float(row["pivot_price"]) for row in peers if int(row["pivot_id"]) != int(center["pivot_id"])]
    if not other_prices:
        return None

    if center["direction"] == "high":
        return center if all(center_price > price for price in other_prices) else None
    return center if all(center_price < price for price in other_prices) else None


def apply_promotions(cur: Any, *, decision_tick_id: int) -> None:
    while True:
        promoted_any = False
        for target_level in range(2, MAX_LEVEL + 1):
            pivots = load_recent_current_pivots(cur, minimum_level=target_level - 1, limit=PIVOT_WINDOW_SIZE)
            candidate = qualifies_for_promotion(pivots, target_level=target_level)
            if not candidate:
                continue
            promote_pivot(cur, candidate, target_level=target_level, decision_tick_id=decision_tick_id)
            promoted_any = True
            break
        if not promoted_any:
            return


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
            ensure_storage_ready(conn)

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
                                current_pivot, last_pivot_id, changed = apply_level_one(
                                    cur,
                                    current_pivot,
                                    candidate,
                                    last_pivot_id=last_pivot_id,
                                    decision_tick_id=int(row["id"]),
                                )
                                if changed:
                                    apply_promotions(cur, decision_tick_id=int(row["id"]))
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
