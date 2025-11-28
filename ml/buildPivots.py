# ml/buildPivots.py
#
# Regime-Adaptive Directional Change (RADC) pivot & swing builder
#
# Assumptions about DB schema:
#   ticks(id BIGINT PK, symbol TEXT, timestamp TIMESTAMPTZ,
#         bid DOUBLE PRECISION, ask DOUBLE PRECISION,
#         kal DOUBLE PRECISION, mid DOUBLE PRECISION)
#
#   piv / swg tables are already created using the DDL we defined earlier.
#
# NOTE: For the first run, assume piv and swg are EMPTY.
#       We can later add "rebuild range" logic.

import math
import sys
from collections import deque
from datetime import timedelta

import numpy as np
import psycopg2
from psycopg2.extras import Json

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "ctrade",
    "user": "babak",
    "password": "babak33044",
}

SYMBOL = "XAUUSD"

# Range selection (by tick id).
# If END_TICK_ID is None, the script will go until the latest tick.
START_TICK_ID = None   # e.g. 1 or 1_000_000; None = min id
END_TICK_ID = None     # e.g. 10_000_000;   None = max id

# Streaming / performance
TICK_BATCH_SIZE = 10_000

# Volatility estimation (EWMA)
EWMA_ALPHA = 0.05          # higher = more reactive, 0.05–0.1 reasonable
VOL_EPS = 1e-8

# RADC thresholds
SWING_MEMORY = 300         # how many past swings to remember for quantile
MIN_SWINGS_FOR_QUANT = 20  # before this, fall back to DEFAULT_QSIZE
QUANTILE_LEVEL = 0.7       # e.g. 0.7 -> 70th percentile of swing sizes
DEFAULT_QSIZE = 1.0        # fallback swing size (in vol units)

COUNTER_RATIO = 0.4        # counter move must be at least 40% of swing size
MIN_SWING_VOL = 0.5        # require swing itself to be at least 0.5 vol units
MIN_COUNTER_VOL = 0.5      # and counter move at least 0.5 vol units

# Additional small filters
MIN_TICKS_PER_SWING = 10         # avoid pivots after ultra-short swings
MIN_DURATION_SEC = 1.0           # avoid pivots in < 1 second (optional)

# Commit behaviour
COMMIT_EVERY_N_SWINGS = 100

# ----------------------------------------------------------------------
# DB helpers
# ----------------------------------------------------------------------


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def insert_pivot(cur, tick_id, ts, price, ptype,
                 vol_local=None, swing_zscore=None,
                 energy=None, reg_code=None,
                 prev_piv_id=None, next_piv_id=None,
                 meta=None):
    sql = """
        INSERT INTO piv
        (tick_id, ts, price, ptype,
         vol_local, swing_zscore, energy, reg_code,
         prev_piv_id, next_piv_id, meta)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """
    cur.execute(
        sql,
        (
            tick_id,
            ts,
            price,
            ptype,
            vol_local,
            swing_zscore,
            energy,
            reg_code,
            prev_piv_id,
            next_piv_id,
            Json(meta) if meta is not None else None,
        ),
    )
    return cur.fetchone()[0]


def update_pivot_links(cur, pivot_id, prev_piv_id=None, next_piv_id=None):
    sets = []
    params = []
    if prev_piv_id is not None:
        sets.append("prev_piv_id = %s")
        params.append(prev_piv_id)
    if next_piv_id is not None:
        sets.append("next_piv_id = %s")
        params.append(next_piv_id)

    if not sets:
        return

    params.append(pivot_id)
    sql = f"UPDATE piv SET {', '.join(sets)} WHERE id = %s"
    cur.execute(sql, params)


def update_pivot_swing_zscore(cur, pivot_id, swing_zscore):
    sql = "UPDATE piv SET swing_zscore = %s WHERE id = %s"
    cur.execute(sql, (swing_zscore, pivot_id))


def insert_swing(cur,
                 start_piv_id, end_piv_id,
                 start_tick_id, end_tick_id,
                 start_ts, end_ts,
                 p_start, p_end,
                 dir_, ret, ret_abs,
                 dur_sec, tick_count,
                 vol_mean, vol_max,
                 rv, vel, lin_r2,
                 imp, meta=None):
    sql = """
        INSERT INTO swg
        (start_piv_id, end_piv_id,
         start_tick_id, end_tick_id,
         start_ts, end_ts,
         dir, p_start, p_end,
         ret, ret_abs,
         dur_sec, tick_count,
         vol_mean, vol_max,
         rv, vel, lin_r2,
         imp, meta)
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.execute(
        sql,
        (
            start_piv_id,
            end_piv_id,
            start_tick_id,
            end_tick_id,
            start_ts,
            end_ts,
            dir_,
            p_start,
            p_end,
            ret,
            ret_abs,
            dur_sec,
            tick_count,
            vol_mean,
            vol_max,
            rv,
            vel,
            lin_r2,
            imp,
            Json(meta) if meta is not None else None,
        ),
    )


# ----------------------------------------------------------------------
# Core RADC logic
# ----------------------------------------------------------------------


class RADCState:
    """Holds the evolving state while we stream ticks."""

    def __init__(self):
        # Volatility (EWMA of returns^2)
        self.var_ewma = None
        self.sigma = None

        # Previous tick data
        self.prev_price = None
        self.prev_ts = None
        self.prev_tick_id = None

        # Current direction (+1 up, -1 down)
        self.dir = None

        # Current pivot
        self.curr_piv_id = None
        self.curr_piv_tick_id = None
        self.curr_piv_ts = None
        self.curr_piv_price = None
        self.curr_piv_ptype = None

        # Current extreme inside swing
        self.ext_price = None
        self.ext_tick_id = None
        self.ext_ts = None
        self.ext_sigma = None

        # Per-swing aggregation
        self.swing_start_piv_id = None
        self.swing_start_tick_id = None
        self.swing_start_ts = None
        self.swing_start_price = None

        self.swing_tick_count = 0
        self.swing_rv = 0.0             # sum of returns^2
        self.swing_sigma_sum = 0.0
        self.swing_sigma_max = 0.0

        # Completed swings (z-scores) history
        self.recent_swings = deque(maxlen=SWING_MEMORY)

        # Counters
        self.num_swings_inserted = 0

    def update_vol(self, r):
        r2 = r * r
        if self.var_ewma is None:
            self.var_ewma = r2
        else:
            self.var_ewma = EWMA_ALPHA * r2 + (1.0 - EWMA_ALPHA) * self.var_ewma
        self.sigma = math.sqrt(max(self.var_ewma, VOL_EPS))

    def get_qsize(self):
        if len(self.recent_swings) >= MIN_SWINGS_FOR_QUANT:
            return float(np.quantile(np.array(self.recent_swings), QUANTILE_LEVEL))
        else:
            return DEFAULT_QSIZE


# ----------------------------------------------------------------------
# Utility
# ----------------------------------------------------------------------


def compute_lin_reg_r2(prices, times_sec):
    """
    Simple linear regression R^2 of price ~ time.
    times_sec: list of seconds since swing start.
    """
    n = len(prices)
    if n < 2:
        return None

    x = np.array(times_sec, dtype=float)
    y = np.array(prices, dtype=float)
    x_mean = x.mean()
    y_mean = y.mean()
    cov_xy = ((x - x_mean) * (y - y_mean)).sum()
    var_x = ((x - x_mean) ** 2).sum()
    var_y = ((y - y_mean) ** 2).sum()

    if var_x <= 0 or var_y <= 0:
        return None

    beta = cov_xy / var_x
    alpha = y_mean - beta * x_mean
    y_hat = alpha + beta * x
    ss_tot = ((y - y_mean) ** 2).sum()
    ss_res = ((y - y_hat) ** 2).sum()
    if ss_tot <= 0:
        return None
    r2 = 1.0 - ss_res / ss_tot
    return float(r2)


# ----------------------------------------------------------------------
# Main processing
# ----------------------------------------------------------------------


def process_ticks():
    conn = get_conn()
    conn.autocommit = False

    read_cur = conn.cursor(name="tick_cursor")
    write_cur = conn.cursor()

    # Determine ID range if None
    def fetch_scalar(sql, params=None):
        c = conn.cursor()
        c.execute(sql, params or [])
        v = c.fetchone()[0]
        c.close()
        return v

    if START_TICK_ID is None:
        START_TICK_ID = fetch_scalar(
            "SELECT MIN(id) FROM ticks WHERE symbol = %s", (SYMBOL,)
        )
    if END_TICK_ID is None:
        END_TICK_ID = fetch_scalar(
            "SELECT MAX(id) FROM ticks WHERE symbol = %s", (SYMBOL,)
        )

    if START_TICK_ID is None or END_TICK_ID is None:
        print("No ticks found for symbol", SYMBOL)
        return

    print(f"Processing ticks {START_TICK_ID} .. {END_TICK_ID} for {SYMBOL}")

    read_cur.execute(
        """
        SELECT id, timestamp, mid
        FROM ticks
        WHERE symbol = %s
          AND id >= %s
          AND id <= %s
        ORDER BY id ASC
        """,
        (SYMBOL, START_TICK_ID, END_TICK_ID),
    )

    state = RADCState()

    # For computing R^2 cheaply, we keep prices and times only within current swing
    swing_prices = []
    swing_times = []  # seconds since swing start_ts

    total_ticks = 0

    while True:
        rows = read_cur.fetchmany(TICK_BATCH_SIZE)
        if not rows:
            break

        for tick_id, ts, price in rows:
            total_ticks += 1

            if state.prev_price is None:
                # First tick, just store and continue.
                state.prev_price = price
                state.prev_ts = ts
                state.prev_tick_id = tick_id
                continue

            # Compute return and volatility
            r = price - state.prev_price
            state.update_vol(r)

            # Initialise pivot and direction after we have 2 ticks
            if state.curr_piv_id is None:
                # Determine initial direction from first non-zero return
                move = price - state.prev_price
                if move >= 0:
                    state.dir = +1
                    ptype = -1  # starting from a low
                else:
                    state.dir = -1
                    ptype = +1  # starting from a high

                # Insert first pivot at previous tick
                state.curr_piv_id = insert_pivot(
                    write_cur,
                    tick_id=state.prev_tick_id,
                    ts=state.prev_ts,
                    price=state.prev_price,
                    ptype=ptype,
                    vol_local=state.sigma,
                    swing_zscore=None,
                    energy=None,
                    reg_code=None,
                    prev_piv_id=None,
                    next_piv_id=None,
                    meta={"note": "initial_pivot"},
                )
                state.curr_piv_tick_id = state.prev_tick_id
                state.curr_piv_ts = state.prev_ts
                state.curr_piv_price = state.prev_price
                state.curr_piv_ptype = ptype

                # Initialise extreme to pivot
                state.ext_price = state.curr_piv_price
                state.ext_tick_id = state.curr_piv_tick_id
                state.ext_ts = state.curr_piv_ts
                state.ext_sigma = state.sigma

                # Initialise swing aggregation
                state.swing_start_piv_id = state.curr_piv_id
                state.swing_start_tick_id = state.curr_piv_tick_id
                state.swing_start_ts = state.curr_piv_ts
                state.swing_start_price = state.curr_piv_price

                state.swing_tick_count = 1
                state.swing_rv = 0.0
                state.swing_sigma_sum = state.sigma
                state.swing_sigma_max = state.sigma

                swing_prices = [state.curr_piv_price]
                swing_times = [0.0]

                # Now continue processing as usual for this tick (falls through)

            # Update per-swing aggregation with current tick
            state.swing_tick_count += 1
            state.swing_rv += r * r
            state.swing_sigma_sum += state.sigma
            if state.sigma > state.swing_sigma_max:
                state.swing_sigma_max = state.sigma

            # Time since swing start
            dur_since_start = (ts - state.swing_start_ts).total_seconds()
            swing_prices.append(price)
            swing_times.append(max(dur_since_start, 0.0))

            # Update extreme and compute counter-move
            if state.dir == +1:
                # We are in an upswing: extreme is the highest price since pivot.
                if price >= state.ext_price:
                    state.ext_price = price
                    state.ext_tick_id = tick_id
                    state.ext_ts = ts
                    state.ext_sigma = state.sigma
                    counter_vol = 0.0
                else:
                    swing_abs = state.ext_price - state.curr_piv_price
                    swing_vol = swing_abs / max(state.ext_sigma, VOL_EPS)

                    counter_abs = state.ext_price - price
                    counter_vol = counter_abs / max(state.sigma, VOL_EPS)

            else:
                # dir == -1 : downswing, extreme is the lowest price since pivot.
                if price <= state.ext_price:
                    state.ext_price = price
                    state.ext_tick_id = tick_id
                    state.ext_ts = ts
                    state.ext_sigma = state.sigma
                    counter_vol = 0.0
                else:
                    swing_abs = state.curr_piv_price - state.ext_price
                    swing_vol = swing_abs / max(state.ext_sigma, VOL_EPS)

                    counter_abs = price - state.ext_price
                    counter_vol = counter_abs / max(state.sigma, VOL_EPS)

            # Decide whether to mark a new pivot at the extreme
            do_pivot = False
            if state.ext_tick_id is not None and state.ext_tick_id != state.curr_piv_tick_id:
                # Ensure we defined swing_vol in both branches
                if state.dir == +1:
                    swing_abs = state.ext_price - state.curr_piv_price
                else:
                    swing_abs = state.curr_piv_price - state.ext_price

                swing_vol = swing_abs / max(state.ext_sigma, VOL_EPS)

                qsize = state.get_qsize()
                # basic filters
                enough_swing = swing_vol >= max(MIN_SWING_VOL, 0.0)
                enough_counter = counter_vol >= max(qsize, MIN_COUNTER_VOL)
                ratio_ok = counter_vol >= COUNTER_RATIO * swing_vol
                enough_ticks = state.swing_tick_count >= MIN_TICKS_PER_SWING
                enough_time = dur_since_start >= MIN_DURATION_SEC

                if enough_swing and enough_counter and ratio_ok and enough_ticks and enough_time:
                    do_pivot = True

            if do_pivot:
                # 1) Insert new pivot at the extreme point
                new_ptype = +1 if state.dir == +1 else -1
                new_piv_id = insert_pivot(
                    write_cur,
                    tick_id=state.ext_tick_id,
                    ts=state.ext_ts,
                    price=state.ext_price,
                    ptype=new_ptype,
                    vol_local=state.ext_sigma,
                    swing_zscore=None,  # will be updated after swing insertion
                    energy=None,
                    reg_code=None,
                    prev_piv_id=state.curr_piv_id,
                    next_piv_id=None,
                    meta=None,
                )

                # Link previous pivot to this one
                update_pivot_links(
                    write_cur,
                    pivot_id=state.curr_piv_id,
                    next_piv_id=new_piv_id,
                )

                # 2) Close current swing and insert into swg
                p_start = state.swing_start_price
                p_end = state.ext_price
                dir_ = +1 if p_end > p_start else -1

                ret = p_end - p_start
                ret_abs = abs(ret)
                dur_sec = (state.ext_ts - state.swing_start_ts).total_seconds()
                tick_count = state.swing_tick_count

                if tick_count > 0:
                    vol_mean = state.swing_sigma_sum / tick_count
                else:
                    vol_mean = state.sigma

                vol_max = state.swing_sigma_max
                rv = state.swing_rv
                vel = ret_abs / max(dur_sec, 1e-6)

                # Linear fit R^2
                lin_r2 = compute_lin_reg_r2(swing_prices, swing_times)

                # Importance: |ret| divided by mean vol
                imp = ret_abs / max(vol_mean, VOL_EPS)

                insert_swing(
                    write_cur,
                    start_piv_id=state.swing_start_piv_id,
                    end_piv_id=new_piv_id,
                    start_tick_id=state.swing_start_tick_id,
                    end_tick_id=state.ext_tick_id,
                    start_ts=state.swing_start_ts,
                    end_ts=state.ext_ts,
                    p_start=p_start,
                    p_end=p_end,
                    dir_=dir_,
                    ret=ret,
                    ret_abs=ret_abs,
                    dur_sec=dur_sec,
                    tick_count=tick_count,
                    vol_mean=vol_mean,
                    vol_max=vol_max,
                    rv=rv,
                    vel=vel,
                    lin_r2=lin_r2,
                    imp=imp,
                    meta=None,
                )

                # 3) Compute swing z-score and update pivot row we just inserted
                swing_z = ret_abs / max(vol_mean, VOL_EPS)
                update_pivot_swing_zscore(write_cur, new_piv_id, swing_z)

                # 4) Update swing history distribution
                state.recent_swings.append(swing_z)
                state.num_swings_inserted += 1

                if state.num_swings_inserted % COMMIT_EVERY_N_SWINGS == 0:
                    conn.commit()
                    print(
                        f"Committed after {state.num_swings_inserted} swings, "
                        f"{total_ticks} ticks processed..."
                    )

                # 5) Prepare for next swing: new pivot becomes current
                state.curr_piv_id = new_piv_id
                state.curr_piv_tick_id = state.ext_tick_id
                state.curr_piv_ts = state.ext_ts
                state.curr_piv_price = state.ext_price
                state.curr_piv_ptype = new_ptype

                # Direction flips
                state.dir = -state.dir

                # Extreme reset to new pivot (will move with new swing)
                state.ext_price = state.curr_piv_price
                state.ext_tick_id = state.curr_piv_tick_id
                state.ext_ts = state.curr_piv_ts
                state.ext_sigma = state.sigma

                # Reset swing aggregations for next swing
                state.swing_start_piv_id = state.curr_piv_id
                state.swing_start_tick_id = state.curr_piv_tick_id
                state.swing_start_ts = state.curr_piv_ts
                state.swing_start_price = state.curr_piv_price

                state.swing_tick_count = 1
                state.swing_rv = 0.0
                state.swing_sigma_sum = state.sigma
                state.swing_sigma_max = state.sigma

                swing_prices = [state.curr_piv_price]
                swing_times = [0.0]

            # Finally, move prev_* forward
            state.prev_price = price
            state.prev_ts = ts
            state.prev_tick_id = tick_id

        # End of batch loop

    # After all ticks, we do NOT force-closing the last partial swing yet.
    # We can add optional logic later if needed.

    conn.commit()
    read_cur.close()
    write_cur.close()
    conn.close()

    print(
        f"Done. Processed {total_ticks} ticks, "
        f"inserted {state.num_swings_inserted} swings and their pivots."
    )


if __name__ == "__main__":
    try:
        process_ticks()
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
