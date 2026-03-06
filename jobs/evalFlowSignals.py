#!/usr/bin/env python3
import argparse
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL


DEFAULT_SYMBOL = os.getenv("EVAL_FLOW_SYMBOL", "XAUUSD")
DEFAULT_DIRECTION = os.getenv("EVAL_FLOW_DIRECTION", "desc").lower()
DEFAULT_TP_USD = float(os.getenv("EVAL_FLOW_TP_USD", "1.0"))
DEFAULT_SL_USD = float(os.getenv("EVAL_FLOW_SL_USD", "1.0"))
DEFAULT_MAX_HOLD_SECONDS = int(os.getenv("EVAL_FLOW_MAX_HOLD_SECONDS", "21600"))
DEFAULT_BATCH_SIZE = int(os.getenv("EVAL_FLOW_BATCH_SIZE", "100"))
DEFAULT_PROGRESS_EVERY = int(os.getenv("EVAL_FLOW_PROGRESS_EVERY", "100"))


@dataclass
class EvalResult:
    symbol: str
    signal_id: int
    entry_tick_id: int
    entry_ts: datetime
    side: str
    entry_px: float
    exit_tick_id: Optional[int]
    exit_ts: Optional[datetime]
    exit_px: Optional[float]
    outcome: str
    pnl: int
    seconds_to_close: Optional[float]
    tp_px: float
    sl_px: float
    max_hold_seconds: int


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def ensure_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name='flow_signal_outcomes'
            LIMIT 1
            """
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                "Missing public.flow_signal_outcomes. Apply sql/2026-03-06-create-flow-signal-outcomes.sql first."
            )


def get_signal_id_bounds(conn, symbol: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(id), MAX(id)
            FROM flow_signals
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
    min_id = int(row[0]) if row and row[0] is not None else None
    max_id = int(row[1]) if row and row[1] is not None else None
    return min_id, max_id


def fetch_signals_batch(
    conn,
    *,
    symbol: str,
    start_signal_id: int,
    end_signal_id: int,
    direction: str,
    cursor_signal_id: Optional[int],
    limit: int,
    force: bool,
):
    direction = direction.lower()
    if direction not in ("desc", "asc"):
        raise ValueError(f"Unsupported direction: {direction}")

    where = [
        "s.symbol = %s",
        "s.id >= %s",
        "s.id <= %s",
    ]
    params: list = [symbol, int(end_signal_id), int(start_signal_id)]

    if cursor_signal_id is not None:
        if direction == "desc":
            where.append("s.id < %s")
        else:
            where.append("s.id > %s")
        params.append(int(cursor_signal_id))

    if not force:
        where.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM flow_signal_outcomes o
                WHERE o.symbol = s.symbol
                  AND o.signal_id = s.id
            )
            """
        )

    order_sql = "ORDER BY s.id DESC" if direction == "desc" else "ORDER BY s.id ASC"

    sql = f"""
    SELECT s.id, s.tick_id, s.timestamp, s.side
    FROM flow_signals s
    WHERE {" AND ".join(where)}
    {order_sql}
    LIMIT %s
    """
    params.append(int(limit))

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def fetch_entry_tick(conn, symbol: str, tick_id: int):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp, bid, ask
            FROM ticks
            WHERE id = %s
              AND symbol = %s
            LIMIT 1
            """,
            (int(tick_id), symbol),
        )
        return cur.fetchone()


def find_first_hit(
    conn,
    *,
    symbol: str,
    entry_tick_id: int,
    entry_ts: datetime,
    deadline_ts: datetime,
    comparator_sql: str,
    threshold: float,
):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, timestamp, bid, ask
            FROM ticks
            WHERE symbol = %s
              AND id > %s
              AND timestamp <= %s
              AND ({comparator_sql})
            ORDER BY id ASC
            LIMIT 1
            """,
            (symbol, int(entry_tick_id), deadline_ts, float(threshold)),
        )
        return cur.fetchone()


def evaluate_signal(
    conn,
    *,
    symbol: str,
    signal_id: int,
    signal_tick_id: int,
    side: str,
    tp_usd: float,
    sl_usd: float,
    max_hold_seconds: int,
) -> Optional[EvalResult]:
    entry = fetch_entry_tick(conn, symbol, signal_tick_id)
    if not entry:
        return None
    if entry["bid"] is None or entry["ask"] is None:
        return None

    entry_tick_id = int(entry["id"])
    entry_ts = entry["timestamp"]
    bid = float(entry["bid"])
    ask = float(entry["ask"])
    side = (side or "").lower()
    if side not in ("buy", "sell"):
        return None

    deadline_ts = entry_ts + timedelta(seconds=max_hold_seconds)

    if side == "buy":
        entry_px = ask
        tp_px = entry_px + tp_usd
        sl_px = entry_px - sl_usd
        tp_row = find_first_hit(
            conn,
            symbol=symbol,
            entry_tick_id=entry_tick_id,
            entry_ts=entry_ts,
            deadline_ts=deadline_ts,
            comparator_sql="bid >= %s",
            threshold=tp_px,
        )
        sl_row = find_first_hit(
            conn,
            symbol=symbol,
            entry_tick_id=entry_tick_id,
            entry_ts=entry_ts,
            deadline_ts=deadline_ts,
            comparator_sql="bid <= %s",
            threshold=sl_px,
        )
        tp_exit_px = float(tp_row["bid"]) if tp_row and tp_row["bid"] is not None else None
        sl_exit_px = float(sl_row["bid"]) if sl_row and sl_row["bid"] is not None else None
    else:
        entry_px = bid
        tp_px = entry_px - tp_usd
        sl_px = entry_px + sl_usd
        tp_row = find_first_hit(
            conn,
            symbol=symbol,
            entry_tick_id=entry_tick_id,
            entry_ts=entry_ts,
            deadline_ts=deadline_ts,
            comparator_sql="ask <= %s",
            threshold=tp_px,
        )
        sl_row = find_first_hit(
            conn,
            symbol=symbol,
            entry_tick_id=entry_tick_id,
            entry_ts=entry_ts,
            deadline_ts=deadline_ts,
            comparator_sql="ask >= %s",
            threshold=sl_px,
        )
        tp_exit_px = float(tp_row["ask"]) if tp_row and tp_row["ask"] is not None else None
        sl_exit_px = float(sl_row["ask"]) if sl_row and sl_row["ask"] is not None else None

    chosen = None
    chosen_outcome = "no_hit"
    chosen_pnl = 0
    chosen_exit_px = None

    if tp_row is not None and sl_row is not None:
        if int(tp_row["id"]) <= int(sl_row["id"]):
            chosen = tp_row
            chosen_outcome = "tp"
            chosen_pnl = 1
            chosen_exit_px = tp_exit_px
        else:
            chosen = sl_row
            chosen_outcome = "sl"
            chosen_pnl = -1
            chosen_exit_px = sl_exit_px
    elif tp_row is not None:
        chosen = tp_row
        chosen_outcome = "tp"
        chosen_pnl = 1
        chosen_exit_px = tp_exit_px
    elif sl_row is not None:
        chosen = sl_row
        chosen_outcome = "sl"
        chosen_pnl = -1
        chosen_exit_px = sl_exit_px

    exit_tick_id = int(chosen["id"]) if chosen is not None else None
    exit_ts = chosen["timestamp"] if chosen is not None else None
    seconds_to_close = (
        max(0.0, (exit_ts - entry_ts).total_seconds()) if exit_ts is not None else None
    )

    return EvalResult(
        symbol=symbol,
        signal_id=int(signal_id),
        entry_tick_id=entry_tick_id,
        entry_ts=entry_ts,
        side=side,
        entry_px=float(entry_px),
        exit_tick_id=exit_tick_id,
        exit_ts=exit_ts,
        exit_px=chosen_exit_px,
        outcome=chosen_outcome,
        pnl=chosen_pnl,
        seconds_to_close=seconds_to_close,
        tp_px=float(tp_px),
        sl_px=float(sl_px),
        max_hold_seconds=int(max_hold_seconds),
    )


def upsert_outcome(conn, r: EvalResult):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flow_signal_outcomes (
                symbol, signal_id, entry_tick_id, entry_ts, side, entry_px,
                exit_tick_id, exit_ts, exit_px, outcome, pnl, seconds_to_close,
                tp_px, sl_px, max_hold_seconds, created_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            ON CONFLICT (symbol, signal_id) DO UPDATE SET
                entry_tick_id = EXCLUDED.entry_tick_id,
                entry_ts = EXCLUDED.entry_ts,
                side = EXCLUDED.side,
                entry_px = EXCLUDED.entry_px,
                exit_tick_id = EXCLUDED.exit_tick_id,
                exit_ts = EXCLUDED.exit_ts,
                exit_px = EXCLUDED.exit_px,
                outcome = EXCLUDED.outcome,
                pnl = EXCLUDED.pnl,
                seconds_to_close = EXCLUDED.seconds_to_close,
                tp_px = EXCLUDED.tp_px,
                sl_px = EXCLUDED.sl_px,
                max_hold_seconds = EXCLUDED.max_hold_seconds,
                created_at = now()
            """,
            (
                r.symbol,
                r.signal_id,
                r.entry_tick_id,
                r.entry_ts,
                r.side,
                r.entry_px,
                r.exit_tick_id,
                r.exit_ts,
                r.exit_px,
                r.outcome,
                r.pnl,
                r.seconds_to_close,
                r.tp_px,
                r.sl_px,
                r.max_hold_seconds,
            ),
        )


def parse_args():
    ap = argparse.ArgumentParser(
        description="Evaluate flow_signals against ticks with first-hit TP/SL logic."
    )
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ap.add_argument("--start-signal-id", type=int, default=None)
    ap.add_argument("--end-signal-id", type=int, default=1)
    ap.add_argument("--direction", choices=["desc", "asc"], default=DEFAULT_DIRECTION)
    ap.add_argument("--tp-usd", type=float, default=DEFAULT_TP_USD)
    ap.add_argument("--sl-usd", type=float, default=DEFAULT_SL_USD)
    ap.add_argument("--max-hold-seconds", type=int, default=DEFAULT_MAX_HOLD_SECONDS)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    ap.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    return ap.parse_args()


def main():
    args = parse_args()
    symbol = (args.symbol or "").strip() or DEFAULT_SYMBOL
    direction = args.direction.lower()
    batch_size = max(1, int(args.batch_size))
    progress_every = max(1, int(args.progress_every))
    tp_usd = float(args.tp_usd)
    sl_usd = float(args.sl_usd)
    max_hold_seconds = int(args.max_hold_seconds)

    conn = db_connect()
    ensure_table_exists(conn)

    min_signal_id, max_signal_id = get_signal_id_bounds(conn, symbol)
    if max_signal_id is None:
        print(f"evalFlowSignals: no flow_signals for symbol={symbol}", flush=True)
        return

    start_signal_id = int(args.start_signal_id) if args.start_signal_id is not None else int(max_signal_id)
    end_signal_id = int(args.end_signal_id)
    if start_signal_id < end_signal_id:
        start_signal_id, end_signal_id = end_signal_id, start_signal_id

    print(
        (
            f"evalFlowSignals start symbol={symbol} direction={direction} "
            f"start_id={start_signal_id} end_id={end_signal_id} "
            f"tp={tp_usd} sl={sl_usd} max_hold={max_hold_seconds}s force={args.force}"
        ),
        flush=True,
    )

    cursor_signal_id: Optional[int] = None
    processed = 0
    tp_count = 0
    sl_count = 0
    no_hit_count = 0
    closed_seconds_sum = 0.0
    closed_count = 0
    started_at = time.time()

    try:
        while True:
            signals = fetch_signals_batch(
                conn,
                symbol=symbol,
                start_signal_id=start_signal_id,
                end_signal_id=end_signal_id,
                direction=direction,
                cursor_signal_id=cursor_signal_id,
                limit=batch_size,
                force=bool(args.force),
            )
            if not signals:
                conn.commit()
                break

            for s in signals:
                signal_id = int(s["id"])
                signal_tick_id = int(s["tick_id"])
                side = (s["side"] or "").lower()

                result = evaluate_signal(
                    conn,
                    symbol=symbol,
                    signal_id=signal_id,
                    signal_tick_id=signal_tick_id,
                    side=side,
                    tp_usd=tp_usd,
                    sl_usd=sl_usd,
                    max_hold_seconds=max_hold_seconds,
                )
                if result is None:
                    cursor_signal_id = signal_id
                    continue

                upsert_outcome(conn, result)

                processed += 1
                if result.outcome == "tp":
                    tp_count += 1
                elif result.outcome == "sl":
                    sl_count += 1
                else:
                    no_hit_count += 1

                if result.seconds_to_close is not None and result.outcome in ("tp", "sl"):
                    closed_seconds_sum += float(result.seconds_to_close)
                    closed_count += 1

                cursor_signal_id = signal_id

                if processed > 0 and processed % progress_every == 0:
                    conn.commit()
                    decided = tp_count + sl_count
                    winrate = (100.0 * tp_count / decided) if decided > 0 else 0.0
                    avg_secs = (closed_seconds_sum / closed_count) if closed_count > 0 else math.nan
                    avg_secs_txt = f"{avg_secs:.2f}" if closed_count > 0 else "n/a"
                    elapsed = max(1e-6, time.time() - started_at)
                    print(
                        (
                            f"evalFlowSignals progress processed={processed} tp={tp_count} sl={sl_count} "
                            f"no_hit={no_hit_count} winrate={winrate:.2f}% avg_secs={avg_secs_txt} "
                            f"rate={processed/elapsed:.1f}/s"
                        ),
                        flush=True,
                    )

            conn.commit()

    except KeyboardInterrupt:
        print("evalFlowSignals interrupted", flush=True)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    decided = tp_count + sl_count
    winrate = (100.0 * tp_count / decided) if decided > 0 else 0.0
    avg_secs = (closed_seconds_sum / closed_count) if closed_count > 0 else math.nan
    avg_secs_txt = f"{avg_secs:.2f}" if closed_count > 0 else "n/a"
    print(
        (
            f"evalFlowSignals done processed={processed} tp={tp_count} sl={sl_count} "
            f"no_hit={no_hit_count} winrate={winrate:.2f}% avg_secs={avg_secs_txt}"
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
