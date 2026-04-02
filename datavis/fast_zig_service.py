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
BATCH_SIZE = max(1, int(os.getenv("FAST_ZIG_BATCH_SIZE", "500")))
WINDOW_SIZE = 9
CENTER_INDEX = 4
RECENT_TICK_COUNT = WINDOW_SIZE - 1
BASE_LEVEL = 0
MAX_LEVEL = 3
LOOKBACK_SAME_DIRECTION = 4
RECALC_TAIL_PIVOTS = max(32, int(os.getenv("FAST_ZIG_RECALC_TAIL_PIVOTS", "256")))
STATE_FINAL = "final"
STATE_CANDIDATE = "candidate"
REQUIRED_PIVOT_COLUMNS = {
    "version_id",
    "pivot_id",
    "symbol",
    "source_tick_id",
    "source_timestamp",
    "direction",
    "pivot_price",
    "level",
    "state",
    "visible_from_tick_id",
    "visible_to_tick_id",
    "created_at",
    "updated_at",
}
REQUIRED_STATE_COLUMNS = {
    "symbol",
    "last_processed_tick_id",
    "last_pivot_id",
    "updated_at",
}

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
            raise RuntimeError(
                "fast zig tables are missing; apply deploy/sql/20260403_fast_zig.sql and deploy/sql/20260404_fast_zig_levels.sql first"
            )
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('fast_zig_pivots', 'fast_zig_state')
            """
        )
        columns: Dict[str, set[str]] = {
            "fast_zig_pivots": set(),
            "fast_zig_state": set(),
        }
        for info in cur.fetchall():
            columns.setdefault(info["table_name"], set()).add(info["column_name"])
    if not REQUIRED_PIVOT_COLUMNS.issubset(columns["fast_zig_pivots"]):
        missing = sorted(REQUIRED_PIVOT_COLUMNS - columns["fast_zig_pivots"])
        raise RuntimeError("fast zig pivots schema is incomplete; missing columns: {0}".format(", ".join(missing)))
    if not REQUIRED_STATE_COLUMNS.issubset(columns["fast_zig_state"]):
        missing = sorted(REQUIRED_STATE_COLUMNS - columns["fast_zig_state"])
        raise RuntimeError("fast zig state schema is incomplete; missing columns: {0}".format(", ".join(missing)))


def current_pivot_columns_sql() -> str:
    return """
        version_id,
        pivot_id,
        direction,
        pivot_price,
        source_tick_id,
        source_timestamp,
        level,
        state
    """


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
        SELECT {columns}
        FROM public.fast_zig_pivots
        WHERE symbol = %s AND visible_to_tick_id IS NULL
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT 1
        """.format(columns=current_pivot_columns_sql()),
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def load_current_pivot_tail(cur: Any, *, limit: int) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {columns}
        FROM public.fast_zig_pivots
        WHERE symbol = %s
          AND visible_to_tick_id IS NULL
        ORDER BY pivot_id DESC, version_id DESC
        LIMIT %s
        """.format(columns=current_pivot_columns_sql()),
        (TICK_SYMBOL, limit),
    )
    rows = [dict(row) for row in cur.fetchall()]
    rows.reverse()
    return rows


def detect_level_zero_pivot(window_ticks: Deque[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
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


def is_more_extreme(direction: str, previous_price: float, candidate_price: float) -> bool:
    if direction == "high":
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
    state: str,
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
            state,
            visible_from_tick_id,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        RETURNING {columns}
        """.format(columns=current_pivot_columns_sql()),
        (
            pivot_id,
            TICK_SYMBOL,
            source_tick_id,
            source_timestamp,
            direction,
            pivot_price,
            level,
            state,
            decision_tick_id,
        ),
    )
    return dict(cur.fetchone())


def replace_level_zero_pivot(cur: Any, current: Dict[str, Any], *, candidate: Dict[str, Any], decision_tick_id: int) -> Dict[str, Any]:
    close_pivot_version(cur, version_id=int(current["version_id"]), decision_tick_id=decision_tick_id)
    return insert_pivot_version(
        cur,
        pivot_id=int(current["pivot_id"]),
        direction=str(candidate["direction"]),
        pivot_price=float(candidate["price"]),
        source_tick_id=int(candidate["source_tick_id"]),
        source_timestamp=candidate["source_timestamp"],
        level=int(current.get("level") or BASE_LEVEL),
        state=str(current.get("state") or STATE_FINAL),
        decision_tick_id=decision_tick_id,
    )


def apply_level_zero(
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
            level=BASE_LEVEL,
            state=STATE_FINAL,
            decision_tick_id=decision_tick_id,
        )
        return inserted, next_pivot_id, True

    current_price = float(current["pivot_price"])
    candidate_price = float(candidate["price"])
    if candidate["direction"] == current["direction"]:
        if not is_more_extreme(str(current["direction"]), current_price, candidate_price):
            return current, max(last_pivot_id, int(current["pivot_id"])), False
        replaced = replace_level_zero_pivot(cur, current, candidate=candidate, decision_tick_id=decision_tick_id)
        return replaced, max(last_pivot_id, int(current["pivot_id"])), True

    next_pivot_id = max(last_pivot_id, int(current["pivot_id"])) + 1
    inserted = insert_pivot_version(
        cur,
        pivot_id=next_pivot_id,
        direction=str(candidate["direction"]),
        pivot_price=float(candidate["price"]),
        source_tick_id=int(candidate["source_tick_id"]),
        source_timestamp=candidate["source_timestamp"],
        level=BASE_LEVEL,
        state=STATE_FINAL,
        decision_tick_id=decision_tick_id,
    )
    return inserted, next_pivot_id, True


def target_from_lower_level(pivots: List[Dict[str, Any]]) -> None:
    for pivot in pivots:
        pivot["_target_level"] = BASE_LEVEL
        pivot["_target_state"] = STATE_FINAL


def qualifies_upper_candidate(pivot: Dict[str, Any], history: List[Dict[str, Any]]) -> bool:
    if len(history) < LOOKBACK_SAME_DIRECTION:
        return False
    pivot_price = float(pivot["pivot_price"])
    direction = str(pivot["direction"])
    if direction == "high":
        return all(pivot_price > float(previous["pivot_price"]) for previous in history[-LOOKBACK_SAME_DIRECTION:])
    return all(pivot_price < float(previous["pivot_price"]) for previous in history[-LOOKBACK_SAME_DIRECTION:])


def restore_level_assignment(pivot: Dict[str, Any], baseline: Dict[int, tuple[int, str]]) -> None:
    prior_level, prior_state = baseline[int(pivot["pivot_id"])]
    pivot["_target_level"] = prior_level
    pivot["_target_state"] = prior_state


def assign_upper_level(pivots: List[Dict[str, Any]], *, target_level: int) -> None:
    eligible = [pivot for pivot in pivots if int(pivot["_target_level"]) >= target_level - 1]
    if not eligible:
        return

    baseline = {
        int(pivot["pivot_id"]): (int(pivot["_target_level"]), str(pivot["_target_state"]))
        for pivot in pivots
    }
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    current_candidate: Optional[Dict[str, Any]] = None

    for pivot in eligible:
        direction = str(pivot["direction"])
        history = highs if direction == "high" else lows
        if qualifies_upper_candidate(pivot, history):
            if current_candidate is None:
                pivot["_target_level"] = target_level
                pivot["_target_state"] = STATE_CANDIDATE
                current_candidate = pivot
            elif direction == str(current_candidate["direction"]):
                if is_more_extreme(
                    direction,
                    float(current_candidate["pivot_price"]),
                    float(pivot["pivot_price"]),
                ):
                    restore_level_assignment(current_candidate, baseline)
                    pivot["_target_level"] = target_level
                    pivot["_target_state"] = STATE_CANDIDATE
                    current_candidate = pivot
            else:
                current_candidate["_target_level"] = target_level
                current_candidate["_target_state"] = STATE_FINAL
                pivot["_target_level"] = target_level
                pivot["_target_state"] = STATE_CANDIDATE
                current_candidate = pivot
        history.append(pivot)


def recalculate_target_levels(pivots: List[Dict[str, Any]]) -> None:
    target_from_lower_level(pivots)
    for target_level in range(1, MAX_LEVEL + 1):
        assign_upper_level(pivots, target_level=target_level)


def reconcile_tail_levels(cur: Any, *, decision_tick_id: int) -> int:
    pivots = load_current_pivot_tail(cur, limit=RECALC_TAIL_PIVOTS)
    if not pivots:
        return 0
    recalculate_target_levels(pivots)
    changed = 0
    for pivot in pivots:
        current_level = int(pivot.get("level") or BASE_LEVEL)
        current_state = str(pivot.get("state") or STATE_FINAL)
        target_level = int(pivot.get("_target_level") or BASE_LEVEL)
        target_state = str(pivot.get("_target_state") or STATE_FINAL)
        if current_level == target_level and current_state == target_state:
            continue
        close_pivot_version(cur, version_id=int(pivot["version_id"]), decision_tick_id=decision_tick_id)
        updated = insert_pivot_version(
            cur,
            pivot_id=int(pivot["pivot_id"]),
            direction=str(pivot["direction"]),
            pivot_price=float(pivot["pivot_price"]),
            source_tick_id=int(pivot["source_tick_id"]),
            source_timestamp=pivot["source_timestamp"],
            level=target_level,
            state=target_state,
            decision_tick_id=decision_tick_id,
        )
        pivot.update(updated)
        changed += 1
    return changed


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
                            candidate = detect_level_zero_pivot(window_ticks)
                            if candidate:
                                current_pivot, last_pivot_id, changed = apply_level_zero(
                                    cur,
                                    current_pivot,
                                    candidate,
                                    last_pivot_id=last_pivot_id,
                                    decision_tick_id=int(row["id"]),
                                )
                                if changed:
                                    reconcile_tail_levels(cur, decision_tick_id=int(row["id"]))
                                    current_pivot = load_last_pivot(cur)
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
