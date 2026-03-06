#!/usr/bin/env python3
import json
import math
import os
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL


SYMBOL = os.getenv("FLOW_SYMBOL", "XAUUSD")
BATCH_SIZE = int(os.getenv("FLOW_BATCH_SIZE", "2000"))
POLL_IDLE_SECONDS = float(os.getenv("FLOW_IDLE_SECONDS", "0.2"))

GAP_RESET_SECONDS = float(os.getenv("FLOW_GAP_RESET_SECONDS", "1800"))
WINDOW_SECONDS = float(os.getenv("FLOW_WINDOW_SECONDS", "60"))
RANGE_Z_THRESH = float(os.getenv("FLOW_RANGE_Z_THRESH", "2.0"))
TICKRATE_Z_THRESH = float(os.getenv("FLOW_TICKRATE_Z_THRESH", "1.0"))
COOLDOWN_SECONDS = float(os.getenv("FLOW_COOLDOWN_SECONDS", "10"))

EMA_FAST_HL = float(os.getenv("FLOW_EMA_FAST_HALFLIFE", "10"))
EMA_SLOW_HL = float(os.getenv("FLOW_EMA_SLOW_HALFLIFE", "30"))
EMA_15M_HL = float(os.getenv("FLOW_EMA_15M_HALFLIFE", "900"))
EMA_1H_HL = float(os.getenv("FLOW_EMA_1H_HALFLIFE", "3600"))
ZSCORE_HL = float(os.getenv("FLOW_ZSCORE_HALFLIFE", "900"))

VAR_EPS = 1e-12
STOP = False


@dataclass
class FlowState:
    symbol: str
    last_tick_id: int = 0
    prev_ts: Optional[datetime] = None
    session_id: int = 0
    session_start_ts: Optional[datetime] = None
    session_sum_mid: float = 0.0
    session_n: int = 0
    atvwap: Optional[float] = None
    win60_hi: Optional[float] = None
    win60_lo: Optional[float] = None
    range60: Optional[float] = None
    tick_count60: int = 0
    tick_rate60: Optional[float] = None
    ret60: Optional[float] = None
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    ema15m: Optional[float] = None
    ema1h: Optional[float] = None
    ema1h_slope: Optional[float] = None
    range_ewm_mean: Optional[float] = None
    range_ewm_var: Optional[float] = None
    tickrate_ewm_mean: Optional[float] = None
    tickrate_ewm_var: Optional[float] = None
    range_z: Optional[float] = None
    tickrate_z: Optional[float] = None
    state: str = "idle"
    side: Optional[str] = None
    impulse_high: Optional[float] = None
    impulse_low: Optional[float] = None
    cooldown_until: Optional[datetime] = None


class RollingWindow60:
    def __init__(self, window_seconds: float):
        self.window_seconds = float(window_seconds)
        self.rows = deque()
        self.maxdq = deque()
        self.mindq = deque()
        self.seq = 0

    def clear(self):
        self.rows.clear()
        self.maxdq.clear()
        self.mindq.clear()

    def push(self, ts: datetime, mid: float, spread: Optional[float]):
        self.seq += 1
        sid = self.seq
        self.rows.append((sid, ts, float(mid), float(spread) if spread is not None else None))

        while self.maxdq and self.maxdq[-1][1] <= mid:
            self.maxdq.pop()
        self.maxdq.append((sid, float(mid)))

        while self.mindq and self.mindq[-1][1] >= mid:
            self.mindq.pop()
        self.mindq.append((sid, float(mid)))

        self._evict(ts)

    def _evict(self, now_ts: datetime):
        cutoff = now_ts - timedelta(seconds=self.window_seconds)
        while self.rows and self.rows[0][1] < cutoff:
            sid, _ts, _mid, _spread = self.rows.popleft()
            if self.maxdq and self.maxdq[0][0] == sid:
                self.maxdq.popleft()
            if self.mindq and self.mindq[0][0] == sid:
                self.mindq.popleft()

    def metrics(self, now_ts: datetime):
        self._evict(now_ts)
        if not self.rows:
            return None
        hi = self.maxdq[0][1] if self.maxdq else None
        lo = self.mindq[0][1] if self.mindq else None
        if hi is None or lo is None:
            return None
        first_mid = self.rows[0][2]
        count = len(self.rows)
        elapsed = max(1e-6, (now_ts - self.rows[0][1]).total_seconds())
        return {
            "hi": float(hi),
            "lo": float(lo),
            "range": float(hi - lo),
            "count": int(count),
            "rate": float(count / elapsed),
            "ret": float(self.rows[-1][2] - first_mid),
        }


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def alpha_from_dt(dt_seconds: float, halflife_seconds: float) -> float:
    if dt_seconds <= 0:
        return 1.0
    return 1.0 - math.exp(-math.log(2.0) * dt_seconds / max(1e-6, halflife_seconds))


def ewma(prev: Optional[float], x: float, dt_seconds: float, halflife_seconds: float) -> float:
    if prev is None:
        return float(x)
    a = alpha_from_dt(dt_seconds, halflife_seconds)
    return float(prev + a * (x - prev))


def ewm_mean_var(
    prev_mean: Optional[float],
    prev_var: Optional[float],
    x: float,
    dt_seconds: float,
    halflife_seconds: float,
):
    if prev_mean is None or prev_var is None:
        return float(x), 0.0
    a = alpha_from_dt(dt_seconds, halflife_seconds)
    delta = x - prev_mean
    mean = prev_mean + a * delta
    var = (1.0 - a) * (prev_var + a * delta * delta)
    return float(mean), float(max(0.0, var))


def zscore(x: Optional[float], mean: Optional[float], var: Optional[float]) -> Optional[float]:
    if x is None or mean is None or var is None or var <= VAR_EPS:
        return None
    return float((x - mean) / math.sqrt(var))


def ensure_tables_exist(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name IN ('flow_state', 'flow_signals')
            """
        )
        have = {r[0] for r in cur.fetchall()}
    missing = [t for t in ("flow_state", "flow_signals") if t not in have]
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}. Apply sql/2026-03-06-create-flow60s.sql first.")


def load_or_init_state(conn, symbol: str) -> FlowState:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM flow_state
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()

    if not row:
        return FlowState(symbol=symbol)

    return FlowState(
        symbol=row["symbol"],
        last_tick_id=int(row["last_tick_id"] or 0),
        prev_ts=row["prev_ts"],
        session_id=int(row["session_id"] or 0),
        session_start_ts=row["session_start_ts"],
        session_sum_mid=float(row["session_sum_mid"] or 0.0),
        session_n=int(row["session_n"] or 0),
        atvwap=float(row["atvwap"]) if row["atvwap"] is not None else None,
        win60_hi=float(row["win60_hi"]) if row["win60_hi"] is not None else None,
        win60_lo=float(row["win60_lo"]) if row["win60_lo"] is not None else None,
        range60=float(row["range60"]) if row["range60"] is not None else None,
        tick_count60=int(row["tick_count60"] or 0),
        tick_rate60=float(row["tick_rate60"]) if row["tick_rate60"] is not None else None,
        ret60=float(row["ret60"]) if row["ret60"] is not None else None,
        ema_fast=float(row["ema_fast"]) if row["ema_fast"] is not None else None,
        ema_slow=float(row["ema_slow"]) if row["ema_slow"] is not None else None,
        ema15m=float(row["ema15m"]) if row["ema15m"] is not None else None,
        ema1h=float(row["ema1h"]) if row["ema1h"] is not None else None,
        ema1h_slope=float(row["ema1h_slope"]) if row["ema1h_slope"] is not None else None,
        range_ewm_mean=float(row["range_ewm_mean"]) if row["range_ewm_mean"] is not None else None,
        range_ewm_var=float(row["range_ewm_var"]) if row["range_ewm_var"] is not None else None,
        tickrate_ewm_mean=float(row["tickrate_ewm_mean"]) if row["tickrate_ewm_mean"] is not None else None,
        tickrate_ewm_var=float(row["tickrate_ewm_var"]) if row["tickrate_ewm_var"] is not None else None,
        range_z=float(row["range_z"]) if row["range_z"] is not None else None,
        tickrate_z=float(row["tickrate_z"]) if row["tickrate_z"] is not None else None,
        state=row["state"] or "idle",
        side=row["side"],
        impulse_high=float(row["impulse_high"]) if row["impulse_high"] is not None else None,
        impulse_low=float(row["impulse_low"]) if row["impulse_low"] is not None else None,
        cooldown_until=row["cooldown_until"],
    )


def save_state(conn, st: FlowState):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flow_state (
                symbol, last_tick_id, updated_at, prev_ts, session_id, session_start_ts,
                session_sum_mid, session_n, atvwap, win60_hi, win60_lo, range60,
                tick_count60, tick_rate60, ret60, ema_fast, ema_slow, ema15m, ema1h,
                ema1h_slope, range_ewm_mean, range_ewm_var, tickrate_ewm_mean,
                tickrate_ewm_var, range_z, tickrate_z, state, side, impulse_high,
                impulse_low, cooldown_until
            )
            VALUES (
                %s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (symbol) DO UPDATE SET
                last_tick_id = EXCLUDED.last_tick_id,
                updated_at = now(),
                prev_ts = EXCLUDED.prev_ts,
                session_id = EXCLUDED.session_id,
                session_start_ts = EXCLUDED.session_start_ts,
                session_sum_mid = EXCLUDED.session_sum_mid,
                session_n = EXCLUDED.session_n,
                atvwap = EXCLUDED.atvwap,
                win60_hi = EXCLUDED.win60_hi,
                win60_lo = EXCLUDED.win60_lo,
                range60 = EXCLUDED.range60,
                tick_count60 = EXCLUDED.tick_count60,
                tick_rate60 = EXCLUDED.tick_rate60,
                ret60 = EXCLUDED.ret60,
                ema_fast = EXCLUDED.ema_fast,
                ema_slow = EXCLUDED.ema_slow,
                ema15m = EXCLUDED.ema15m,
                ema1h = EXCLUDED.ema1h,
                ema1h_slope = EXCLUDED.ema1h_slope,
                range_ewm_mean = EXCLUDED.range_ewm_mean,
                range_ewm_var = EXCLUDED.range_ewm_var,
                tickrate_ewm_mean = EXCLUDED.tickrate_ewm_mean,
                tickrate_ewm_var = EXCLUDED.tickrate_ewm_var,
                range_z = EXCLUDED.range_z,
                tickrate_z = EXCLUDED.tickrate_z,
                state = EXCLUDED.state,
                side = EXCLUDED.side,
                impulse_high = EXCLUDED.impulse_high,
                impulse_low = EXCLUDED.impulse_low,
                cooldown_until = EXCLUDED.cooldown_until
            """,
            (
                st.symbol,
                st.last_tick_id,
                st.prev_ts,
                st.session_id,
                st.session_start_ts,
                st.session_sum_mid,
                st.session_n,
                st.atvwap,
                st.win60_hi,
                st.win60_lo,
                st.range60,
                st.tick_count60,
                st.tick_rate60,
                st.ret60,
                st.ema_fast,
                st.ema_slow,
                st.ema15m,
                st.ema1h,
                st.ema1h_slope,
                st.range_ewm_mean,
                st.range_ewm_var,
                st.tickrate_ewm_mean,
                st.tickrate_ewm_var,
                st.range_z,
                st.tickrate_z,
                st.state,
                st.side,
                st.impulse_high,
                st.impulse_low,
                st.cooldown_until,
            ),
        )


def fetch_batch(conn, symbol: str, after_id: int, limit: int):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp, mid, spread, kal, k2
            FROM ticks
            WHERE symbol = %s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, int(after_id), int(limit)),
        )
        return cur.fetchall()


def insert_signal(conn, symbol: str, tick_id: int, ts: datetime, side: str, price: float, st: FlowState, reason: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flow_signals (
                symbol, tick_id, timestamp, side, price, atvwap, range60,
                tick_rate60, range_z, tickrate_z, reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                symbol,
                int(tick_id),
                ts,
                side,
                float(price),
                st.atvwap,
                st.range60,
                st.tick_rate60,
                st.range_z,
                st.tickrate_z,
                json.dumps(reason, separators=(",", ":")),
            ),
        )


def handle_signal(_sig, _frame):
    global STOP
    STOP = True


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    conn = db_connect()
    ensure_tables_exist(conn)
    st = load_or_init_state(conn, SYMBOL)

    window = RollingWindow60(WINDOW_SECONDS)
    last_mid: Optional[float] = None
    processed_since = 0
    signals_since = 0
    stats_at = time.time()

    print(f"flow60s start symbol={SYMBOL} from_id={st.last_tick_id}", flush=True)

    while not STOP:
        try:
            rows = fetch_batch(conn, SYMBOL, st.last_tick_id, BATCH_SIZE)
            if not rows:
                time.sleep(POLL_IDLE_SECONDS)
                now = time.time()
                if now - stats_at >= 5.0:
                    rate = processed_since / (now - stats_at) if now > stats_at else 0.0
                    print(
                        f"flow60s stats rate={rate:.1f}/s signals={signals_since} last_id={st.last_tick_id} state={st.state}",
                        flush=True,
                    )
                    processed_since = 0
                    signals_since = 0
                    stats_at = now
                continue

            batch_signals = 0
            for row in rows:
                tick_id = int(row["id"])
                ts = row["timestamp"]
                mid = float(row["mid"]) if row["mid"] is not None else None
                spread = float(row["spread"]) if row["spread"] is not None else None
                kal = float(row["kal"]) if row["kal"] is not None else None
                k2 = float(row["k2"]) if row["k2"] is not None else None

                dt = 0.0
                if st.prev_ts is not None and ts is not None:
                    dt = max(0.0, (ts - st.prev_ts).total_seconds())

                if st.prev_ts is not None and dt >= GAP_RESET_SECONDS:
                    st.session_id += 1
                    st.session_start_ts = ts
                    st.session_sum_mid = 0.0
                    st.session_n = 0
                    st.atvwap = None
                    st.state = "idle"
                    st.side = None
                    st.impulse_high = None
                    st.impulse_low = None
                    window.clear()

                st.prev_ts = ts
                st.last_tick_id = tick_id
                if mid is None:
                    continue

                prev_win_hi = st.win60_hi
                prev_win_lo = st.win60_lo
                prev_ema_fast = st.ema_fast
                prev_ema_slow = st.ema_slow
                prev_ema1h = st.ema1h

                if st.session_n == 0:
                    st.session_start_ts = ts
                st.session_sum_mid += mid
                st.session_n += 1
                st.atvwap = st.session_sum_mid / st.session_n

                window.push(ts, mid, spread)
                m = window.metrics(ts)
                if m is not None:
                    st.win60_hi = m["hi"]
                    st.win60_lo = m["lo"]
                    st.range60 = m["range"]
                    st.tick_count60 = m["count"]
                    st.tick_rate60 = m["rate"]
                    st.ret60 = m["ret"]

                ema_dt = max(0.001, dt)
                st.ema_fast = ewma(st.ema_fast, mid, ema_dt, EMA_FAST_HL)
                st.ema_slow = ewma(st.ema_slow, mid, ema_dt, EMA_SLOW_HL)
                st.ema15m = ewma(st.ema15m, mid, ema_dt, EMA_15M_HL)
                st.ema1h = ewma(st.ema1h, mid, ema_dt, EMA_1H_HL)
                if prev_ema1h is not None and dt > 0:
                    st.ema1h_slope = (st.ema1h - prev_ema1h) / dt
                else:
                    st.ema1h_slope = 0.0

                if st.range60 is not None:
                    st.range_ewm_mean, st.range_ewm_var = ewm_mean_var(
                        st.range_ewm_mean,
                        st.range_ewm_var,
                        st.range60,
                        ema_dt,
                        ZSCORE_HL,
                    )
                if st.tick_rate60 is not None:
                    st.tickrate_ewm_mean, st.tickrate_ewm_var = ewm_mean_var(
                        st.tickrate_ewm_mean,
                        st.tickrate_ewm_var,
                        st.tick_rate60,
                        ema_dt,
                        ZSCORE_HL,
                    )

                st.range_z = zscore(st.range60, st.range_ewm_mean, st.range_ewm_var)
                st.tickrate_z = zscore(st.tick_rate60, st.tickrate_ewm_mean, st.tickrate_ewm_var)

                bull = (
                    st.ema1h_slope is not None
                    and st.atvwap is not None
                    and st.ema15m is not None
                    and st.ema1h_slope > 0
                    and mid > st.ema15m
                    and mid > st.atvwap
                )
                bear = (
                    st.ema1h_slope is not None
                    and st.atvwap is not None
                    and st.ema15m is not None
                    and st.ema1h_slope < 0
                    and mid < st.ema15m
                    and mid < st.atvwap
                )
                bias = "buy" if bull else ("sell" if bear else None)

                ret_ok = (
                    st.ret60 is not None
                    and ((bias == "buy" and st.ret60 > 0) or (bias == "sell" and st.ret60 < 0))
                )
                big_move = (
                    bias is not None
                    and st.range_z is not None
                    and st.tickrate_z is not None
                    and st.range_z >= RANGE_Z_THRESH
                    and st.tickrate_z >= TICKRATE_Z_THRESH
                    and ret_ok
                )

                cooldown_active = st.cooldown_until is not None and ts < st.cooldown_until
                if cooldown_active:
                    st.state = "cooldown"
                elif st.state == "cooldown":
                    st.state = "idle"

                if not cooldown_active:
                    if st.state in ("idle", "", None):
                        if big_move:
                            st.state = "impulse"
                            st.side = bias
                            st.impulse_high = mid
                            st.impulse_low = mid

                    elif st.state == "impulse":
                        if big_move and bias == st.side:
                            st.impulse_high = mid if st.impulse_high is None else max(st.impulse_high, mid)
                            st.impulse_low = mid if st.impulse_low is None else min(st.impulse_low, mid)
                        else:
                            st.state = "pullback"

                    elif st.state == "pullback":
                        if st.impulse_high is None or st.impulse_low is None or st.side not in ("buy", "sell"):
                            st.state = "idle"
                            st.side = None
                        else:
                            span = max(VAR_EPS, st.impulse_high - st.impulse_low)
                            if st.side == "buy":
                                retrace = max(0.0, st.impulse_high - mid) / span
                                holds = st.atvwap is not None and mid >= st.atvwap and retrace <= 0.5
                                reaccel = (
                                    last_mid is not None
                                    and st.ema_fast is not None
                                    and st.ema_slow is not None
                                    and mid > last_mid
                                    and st.ema_fast >= st.ema_slow
                                )
                            else:
                                retrace = max(0.0, mid - st.impulse_low) / span
                                holds = st.atvwap is not None and mid <= st.atvwap and retrace <= 0.5
                                reaccel = (
                                    last_mid is not None
                                    and st.ema_fast is not None
                                    and st.ema_slow is not None
                                    and mid < last_mid
                                    and st.ema_fast <= st.ema_slow
                                )
                            if holds and reaccel:
                                st.state = "armed"

                    if st.state == "armed" and st.side in ("buy", "sell"):
                        use_k = kal is not None and k2 is not None
                        if use_k:
                            trigger = (st.side == "buy" and k2 > kal) or (st.side == "sell" and k2 < kal)
                        else:
                            cross_up = (
                                prev_ema_fast is not None
                                and prev_ema_slow is not None
                                and st.ema_fast is not None
                                and st.ema_slow is not None
                                and prev_ema_fast <= prev_ema_slow
                                and st.ema_fast > st.ema_slow
                            )
                            cross_dn = (
                                prev_ema_fast is not None
                                and prev_ema_slow is not None
                                and st.ema_fast is not None
                                and st.ema_slow is not None
                                and prev_ema_fast >= prev_ema_slow
                                and st.ema_fast < st.ema_slow
                            )
                            micro_up = prev_win_hi is not None and mid >= prev_win_hi
                            micro_dn = prev_win_lo is not None and mid <= prev_win_lo
                            trigger = (st.side == "buy" and cross_up and micro_up) or (
                                st.side == "sell" and cross_dn and micro_dn
                            )

                        if trigger:
                            reason = {
                                "state": st.state,
                                "bias": bias,
                                "use_kal_k2": use_k,
                                "range_z": st.range_z,
                                "tickrate_z": st.tickrate_z,
                                "range_z_thresh": RANGE_Z_THRESH,
                                "tickrate_z_thresh": TICKRATE_Z_THRESH,
                                "ret60": st.ret60,
                                "atvwap": st.atvwap,
                                "ema1h_slope": st.ema1h_slope,
                                "big_move": bool(big_move),
                            }
                            insert_signal(conn, SYMBOL, tick_id, ts, st.side, mid, st, reason)
                            batch_signals += 1
                            st.cooldown_until = ts + timedelta(seconds=COOLDOWN_SECONDS)
                            st.state = "cooldown"
                            st.side = None
                            st.impulse_high = None
                            st.impulse_low = None

                last_mid = mid

            save_state(conn, st)
            conn.commit()

            processed_since += len(rows)
            signals_since += batch_signals
            now = time.time()
            if now - stats_at >= 5.0:
                rate = processed_since / (now - stats_at) if now > stats_at else 0.0
                print(
                    f"flow60s stats processed={processed_since} rate={rate:.1f}/s signals={signals_since} last_id={st.last_tick_id} state={st.state}",
                    flush=True,
                )
                processed_since = 0
                signals_since = 0
                stats_at = now

        except Exception as e:
            print(f"flow60s error: {e}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(1.0)

    try:
        conn.close()
    except Exception:
        pass
    print("flow60s stopped", flush=True)


if __name__ == "__main__":
    main()
