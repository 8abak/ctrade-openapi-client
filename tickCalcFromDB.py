#!/usr/bin/env python3
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL
from ml.kalman import ScalarKalmanConfig, ScalarKalmanFilter


SYMBOL = "XAUUSD"
BATCH_SIZE = 1000
POLL_IDLE_SECONDS = 0.2

KAL_CFG = ScalarKalmanConfig(process_var=1e-4, meas_var=1e-2, init_var=1.0)
K2_CFG = ScalarKalmanConfig(process_var=1e-5, meas_var=5e-3, init_var=1.0)
PIVOT_LAYERS: Tuple[Tuple[str, float], ...] = (
    ("nano", 0.70),
    ("micro", 2.43),
    ("macro", 6.08),
)

STOP = False


@dataclass
class CalcState:
    symbol: str
    last_processed_id: int
    kal_x: Optional[float]
    kal_p: float
    k2_x: Optional[float]
    k2_p: float


@dataclass
class PivotState:
    symbol: str
    dayid: int
    layer: str
    rev: float
    last_tick_id: int
    pivotno: int
    anchor_tickid: Optional[int]
    anchor_ts: Optional[datetime]
    anchor_px: Optional[float]
    anchor_dayrow: Optional[int]
    cand_dir: Optional[int]
    cand_tickid: Optional[int]
    cand_ts: Optional[datetime]
    cand_px: Optional[float]
    cand_dayrow: Optional[int]


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


def ensure_pivot_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.pivots (
                id BIGSERIAL PRIMARY KEY,
                dayid BIGINT NOT NULL,
                layer TEXT NOT NULL,
                rev DOUBLE PRECISION NOT NULL,
                tickid BIGINT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                px DOUBLE PRECISION NOT NULL,
                ptype CHAR(1) NOT NULL,
                pivotno INTEGER NOT NULL,
                dayrow INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pivotcalc_state (
                symbol text NOT NULL,
                dayid bigint NOT NULL,
                layer text NOT NULL,
                rev double precision NOT NULL,
                last_tick_id bigint NOT NULL DEFAULT 0,
                pivotno integer NOT NULL DEFAULT 0,
                anchor_tickid bigint NULL,
                anchor_ts timestamptz NULL,
                anchor_px double precision NULL,
                anchor_dayrow integer NULL,
                cand_dir integer NULL,
                cand_tickid bigint NULL,
                cand_ts timestamptz NULL,
                cand_px double precision NULL,
                cand_dayrow integer NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (symbol, dayid, layer)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_day_layer_tick_idx
            ON public.pivots (dayid, layer, tickid)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS pivots_tickid_idx
            ON public.pivots (tickid)
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


def load_pivot_states(conn, symbol: str) -> Dict[Tuple[int, str], PivotState]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                symbol, dayid, layer, rev, last_tick_id, pivotno,
                anchor_tickid, anchor_ts, anchor_px, anchor_dayrow,
                cand_dir, cand_tickid, cand_ts, cand_px, cand_dayrow
            FROM pivotcalc_state
            WHERE symbol = %s
            """,
            (symbol,),
        )
        rows = cur.fetchall()

    out: Dict[Tuple[int, str], PivotState] = {}
    for row in rows:
        st = PivotState(
            symbol=row["symbol"],
            dayid=int(row["dayid"]),
            layer=row["layer"],
            rev=float(row["rev"]),
            last_tick_id=int(row["last_tick_id"] or 0),
            pivotno=int(row["pivotno"] or 0),
            anchor_tickid=int(row["anchor_tickid"]) if row["anchor_tickid"] is not None else None,
            anchor_ts=row["anchor_ts"],
            anchor_px=float(row["anchor_px"]) if row["anchor_px"] is not None else None,
            anchor_dayrow=int(row["anchor_dayrow"]) if row["anchor_dayrow"] is not None else None,
            cand_dir=int(row["cand_dir"]) if row["cand_dir"] is not None else None,
            cand_tickid=int(row["cand_tickid"]) if row["cand_tickid"] is not None else None,
            cand_ts=row["cand_ts"],
            cand_px=float(row["cand_px"]) if row["cand_px"] is not None else None,
            cand_dayrow=int(row["cand_dayrow"]) if row["cand_dayrow"] is not None else None,
        )
        out[(st.dayid, st.layer)] = st
    return out


def save_pivot_states(conn, states: Dict[Tuple[int, str], PivotState]):
    if not states:
        return
    rows = []
    for st in states.values():
        rows.append(
            (
                st.symbol,
                st.dayid,
                st.layer,
                st.rev,
                st.last_tick_id,
                st.pivotno,
                st.anchor_tickid,
                st.anchor_ts,
                st.anchor_px,
                st.anchor_dayrow,
                st.cand_dir,
                st.cand_tickid,
                st.cand_ts,
                st.cand_px,
                st.cand_dayrow,
            )
        )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO pivotcalc_state (
                symbol, dayid, layer, rev, last_tick_id, pivotno,
                anchor_tickid, anchor_ts, anchor_px, anchor_dayrow,
                cand_dir, cand_tickid, cand_ts, cand_px, cand_dayrow, updated_at
            )
            VALUES %s
            ON CONFLICT (symbol, dayid, layer) DO UPDATE SET
                rev = EXCLUDED.rev,
                last_tick_id = EXCLUDED.last_tick_id,
                pivotno = EXCLUDED.pivotno,
                anchor_tickid = EXCLUDED.anchor_tickid,
                anchor_ts = EXCLUDED.anchor_ts,
                anchor_px = EXCLUDED.anchor_px,
                anchor_dayrow = EXCLUDED.anchor_dayrow,
                cand_dir = EXCLUDED.cand_dir,
                cand_tickid = EXCLUDED.cand_tickid,
                cand_ts = EXCLUDED.cand_ts,
                cand_px = EXCLUDED.cand_px,
                cand_dayrow = EXCLUDED.cand_dayrow,
                updated_at = now()
            """,
            [row + (datetime.now(timezone.utc),) for row in rows],
            page_size=min(1000, len(rows)),
        )


def fetch_day_ranges(conn, start_id: int, end_id: int) -> List[Tuple[int, int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, startid, endid
            FROM public.days
            WHERE endid >= %s
              AND startid <= %s
            ORDER BY startid ASC, id ASC
            """,
            (int(start_id), int(end_id)),
        )
        rows = cur.fetchall()
    return [(int(dayid), int(startid), int(endid)) for dayid, startid, endid in rows]


def find_day_for_tick(day_ranges: List[Tuple[int, int, int]], tick_id: int) -> Optional[Tuple[int, int]]:
    if not day_ranges:
        return None
    for idx, (dayid, startid, endid) in enumerate(day_ranges):
        next_start = day_ranges[idx + 1][1] if idx + 1 < len(day_ranges) else None
        if startid <= tick_id <= endid:
            return dayid, tick_id - startid + 1
        if tick_id >= startid and next_start is None:
            return dayid, tick_id - startid + 1
        if tick_id >= startid and next_start is not None and tick_id < next_start:
            return dayid, tick_id - startid + 1
    return None


def get_or_create_pivot_state(
    states: Dict[Tuple[int, str], PivotState],
    *,
    symbol: str,
    dayid: int,
    layer: str,
    rev: float,
    tick_id: int,
    ts: datetime,
    px: float,
    dayrow: int,
) -> PivotState:
    key = (int(dayid), str(layer))
    st = states.get(key)
    if st is not None:
        return st
    st = PivotState(
        symbol=symbol,
        dayid=int(dayid),
        layer=str(layer),
        rev=float(rev),
        last_tick_id=int(tick_id),
        pivotno=0,
        anchor_tickid=int(tick_id),
        anchor_ts=ts,
        anchor_px=float(px),
        anchor_dayrow=int(dayrow),
        cand_dir=None,
        cand_tickid=None,
        cand_ts=None,
        cand_px=None,
        cand_dayrow=None,
    )
    states[key] = st
    return st


def apply_pivot_tick(
    st: PivotState,
    *,
    tick_id: int,
    ts: datetime,
    px: float,
    dayrow: int,
) -> Optional[Tuple[int, str, float, int, datetime, float, str, int, int]]:
    if st.anchor_px is None:
        st.anchor_tickid = int(tick_id)
        st.anchor_ts = ts
        st.anchor_px = float(px)
        st.anchor_dayrow = int(dayrow)
        st.last_tick_id = int(tick_id)
        return None

    if st.cand_dir is None:
        delta = float(px) - float(st.anchor_px)
        if abs(delta) >= float(st.rev):
            st.cand_dir = 1 if delta > 0.0 else -1
            st.cand_tickid = int(tick_id)
            st.cand_ts = ts
            st.cand_px = float(px)
            st.cand_dayrow = int(dayrow)
        st.last_tick_id = int(tick_id)
        return None

    if st.cand_tickid is None or st.cand_px is None:
        st.cand_tickid = int(tick_id)
        st.cand_ts = ts
        st.cand_px = float(px)
        st.cand_dayrow = int(dayrow)
        st.last_tick_id = int(tick_id)
        return None

    pivot_row = None
    if st.cand_dir == 1:
        if float(px) >= float(st.cand_px):
            st.cand_tickid = int(tick_id)
            st.cand_ts = ts
            st.cand_px = float(px)
            st.cand_dayrow = int(dayrow)
        elif (float(st.cand_px) - float(px)) >= float(st.rev):
            st.pivotno += 1
            pivot_row = (
                st.dayid,
                st.layer,
                st.rev,
                int(st.cand_tickid),
                st.cand_ts,
                float(st.cand_px),
                "h",
                st.pivotno,
                int(st.cand_dayrow if st.cand_dayrow is not None else dayrow),
            )
            st.anchor_tickid = int(st.cand_tickid)
            st.anchor_ts = st.cand_ts
            st.anchor_px = float(st.cand_px)
            st.anchor_dayrow = st.cand_dayrow
            st.cand_dir = -1
            st.cand_tickid = int(tick_id)
            st.cand_ts = ts
            st.cand_px = float(px)
            st.cand_dayrow = int(dayrow)
    else:
        if float(px) <= float(st.cand_px):
            st.cand_tickid = int(tick_id)
            st.cand_ts = ts
            st.cand_px = float(px)
            st.cand_dayrow = int(dayrow)
        elif (float(px) - float(st.cand_px)) >= float(st.rev):
            st.pivotno += 1
            pivot_row = (
                st.dayid,
                st.layer,
                st.rev,
                int(st.cand_tickid),
                st.cand_ts,
                float(st.cand_px),
                "l",
                st.pivotno,
                int(st.cand_dayrow if st.cand_dayrow is not None else dayrow),
            )
            st.anchor_tickid = int(st.cand_tickid)
            st.anchor_ts = st.cand_ts
            st.anchor_px = float(st.cand_px)
            st.anchor_dayrow = st.cand_dayrow
            st.cand_dir = 1
            st.cand_tickid = int(tick_id)
            st.cand_ts = ts
            st.cand_px = float(px)
            st.cand_dayrow = int(dayrow)

    st.last_tick_id = int(tick_id)
    return pivot_row


def insert_pivots(conn, rows: List[Tuple[int, str, float, int, datetime, float, str, int, int]]):
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.pivots (
                dayid, layer, rev, tickid, ts, px, ptype, pivotno, dayrow
            )
            SELECT v.dayid, v.layer, v.rev, v.tickid, v.ts, v.px, v.ptype, v.pivotno, v.dayrow
            FROM (VALUES %s) AS v(dayid, layer, rev, tickid, ts, px, ptype, pivotno, dayrow)
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.pivots p
                WHERE p.dayid = v.dayid
                  AND p.layer = v.layer
                  AND p.tickid = v.tickid
                  AND p.ptype = v.ptype
            )
            """,
            rows,
            page_size=min(1000, len(rows)),
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
    ensure_pivot_tables(conn)
    state = load_or_init_state(conn, SYMBOL)
    pivot_states = load_pivot_states(conn, SYMBOL)

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
            pivot_rows = []
            day_ranges = fetch_day_ranges(conn, int(rows[0]["id"]), int(rows[-1]["id"]))
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

                day_info = find_day_for_tick(day_ranges, int(row["id"]))
                if day_info is None:
                    continue
                dayid, dayrow = day_info
                for layer, rev in PIVOT_LAYERS:
                    pst = get_or_create_pivot_state(
                        pivot_states,
                        symbol=SYMBOL,
                        dayid=dayid,
                        layer=layer,
                        rev=rev,
                        tick_id=int(row["id"]),
                        ts=row["timestamp"],
                        px=kal,
                        dayrow=dayrow,
                    )
                    pivot_row = apply_pivot_tick(
                        pst,
                        tick_id=int(row["id"]),
                        ts=row["timestamp"],
                        px=kal,
                        dayrow=dayrow,
                    )
                    if pivot_row is not None:
                        pivot_rows.append(pivot_row)

            if updates:
                apply_updates(conn, updates)
            if pivot_rows:
                insert_pivots(conn, pivot_rows)
            last_batch_updated = len(updates)

            last_row = rows[-1]
            state.last_processed_id = int(last_row["id"])
            state.kal_x = kal_filter.x
            state.kal_p = kal_filter.P if kal_filter.P is not None else state.kal_p
            state.k2_x = k2_filter.x
            state.k2_p = k2_filter.P if k2_filter.P is not None else state.k2_p
            save_state(conn, state)
            save_pivot_states(conn, pivot_states)
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
