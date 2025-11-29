# ml/buildPivots.py
#
# Regime-Adaptive Directional Change (RADC) pivot & swing builder
# INCREMENTAL + CHUNKED VERSION
#
# - Each run continues from the last pivot in DB (if any)
# - Reads ticks in small chunks using simple LIMIT queries
# - Builds at most MAX_SWINGS_PER_RUN swings OR MAX_TICKS_PER_RUN ticks, then exits
# - Safe to run repeatedly until full history is covered
#
# Tables assumed:
#
# ticks(
#   id BIGINT PRIMARY KEY,
#   symbol TEXT,
#   timestamp TIMESTAMPTZ,
#   bid DOUBLE PRECISION,
#   ask DOUBLE PRECISION,
#   kal DOUBLE PRECISION,
#   mid DOUBLE PRECISION
# )
#
# piv(
#   id BIGSERIAL PRIMARY KEY,
#   tick_id BIGINT,
#   ts TIMESTAMPTZ,
#   price DOUBLE PRECISION,
#   ptype SMALLINT,
#   vol_local DOUBLE PRECISION,
#   swing_zscore DOUBLE PRECISION,
#   energy DOUBLE PRECISION,
#   reg_code INTEGER,
#   prev_piv_id BIGINT,
#   next_piv_id BIGINT,
#   meta JSONB,
#   created_at TIMESTAMPTZ DEFAULT now()
# )
#
# swg(
#   id BIGSERIAL PRIMARY KEY,
#   start_piv_id BIGINT,
#   end_piv_id BIGINT,
#   start_tick_id BIGINT,
#   end_tick_id BIGINT,
#   start_ts TIMESTAMPTZ,
#   end_ts TIMESTAMPTZ,
#   dir SMALLINT,
#   p_start DOUBLE PRECISION,
#   p_end DOUBLE PRECISION,
#   ret DOUBLE PRECISION,
#   ret_abs DOUBLE PRECISION,
#   dur_sec DOUBLE PRECISION,
#   tick_count INTEGER,
#   vol_mean DOUBLE PRECISION,
#   vol_max DOUBLE PRECISION,
#   rv DOUBLE PRECISION,
#   vel DOUBLE PRECISION,
#   lin_r2 DOUBLE PRECISION,
#   imp DOUBLE PRECISION,
#   meta JSONB,
#   created_at TIMESTAMPTZ DEFAULT now()
# )

import math
import sys
from collections import deque

import numpy as np
import psycopg2
from psycopg2.extras import Json

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
}

SYMBOL = "XAUUSD"

# Tick loading
TICK_CHUNK_SIZE = 5000          # how many ticks per DB query
MAX_TICKS_PER_RUN = 200000      # safety limit per run (can raise later)

# How many new swings to build per run
MAX_SWINGS_PER_RUN = 250

# Volatility (EWMA) params
EWMA_ALPHA = 0.05
VOL_EPS = 0.00000001

# RADC thresholds
SWING_MEMORY = 300
MIN_SWINGS_FOR_QUANT = 20
QUANTILE_LEVEL = 0.7
DEFAULT_QSIZE = 1.0

COUNTER_RATIO = 0.4
MIN_SWING_VOL = 0.5
MIN_COUNTER_VOL = 0.5

MIN_TICKS_PER_SWING = 10
MIN_DURATION_SEC = 1.0

COMMIT_EVERY_N_SWINGS = 50
REPORT_EVERY_N_TICKS = 20000


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
    sql = "UPDATE piv SET " + ", ".join(sets) + " WHERE id = %s"
    cur.execute(sql, params)


def update_pivot_swing_zscore(cur, pivot_id, swing_zscore):
    cur.execute(
        "UPDATE piv SET swing_zscore = %s WHERE id = %s",
        (swing_zscore, pivot_id),
    )


def insert_swing(cur,
                 start_piv_id, end_piv_id,
                 start_tick_id, end_tick_id,
                 start_ts, end_ts,
                 dir_, p_start, p_end,
                 ret, ret_abs,
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
            start_piv_id, end_piv_id,
            start_tick_id, end_tick_id,
            start_ts, end_ts,
            dir_, p_start, p_end,
            ret, ret_abs,
            dur_sec, tick_count,
            vol_mean, vol_max,
            rv, vel, lin_r2,
            imp, Json(meta) if meta is not None else None,
        ),
    )


# ----------------------------------------------------------------------
# Core RADC state
# ----------------------------------------------------------------------

class RADCState:
    def __init__(self):
        # volatility EWMA
        self.var_ewma = None
        self.sigma = None

        # last tick
        self.prev_price = None
        self.prev_ts = None
        self.prev_tick_id = None

        # direction: +1 up, -1 down
        self.dir = None

        # current pivot
        self.curr_piv_id = None
        self.curr_piv_tick_id = None
        self.curr_piv_ts = None
        self.curr_piv_price = None
        self.curr_piv_ptype = None

        # extreme within current swing
        self.ext_price = None
        self.ext_tick_id = None
        self.ext_ts = None
        self.ext_sigma = None

        # swing start (pivot)
        self.swing_start_piv_id = None
        self.swing_start_tick_id = None
        self.swing_start_ts = None
        self.swing_start_price = None

        # swing aggregates
        self.swing_tick_count = 0
        self.swing_rv = 0.0
        self.swing_sigma_sum = 0.0
        self.swing_sigma_max = 0.0

        # recent swings for quantile-based threshold
        self.recent_swings = deque(maxlen=SWING_MEMORY)

        self.total_swings_inserted = 0

    def update_vol(self, r):
        r2 = r * r
        if self.var_ewma is None:
            self.var_ewma = r2
        else:
            self.var_ewma = EWMA_ALPHA * r2 + (1.0 - EWMA_ALPHA) * self.var_ewma
        if self.var_ewma < VOL_EPS:
            self.var_ewma = VOL_EPS
        self.sigma = math.sqrt(self.var_ewma)

    def get_qsize(self):
        if len(self.recent_swings) >= MIN_SWINGS_FOR_QUANT:
            arr = np.array(self.recent_swings, dtype=float)
            return float(np.quantile(arr, QUANTILE_LEVEL))
        return DEFAULT_QSIZE


def compute_lin_reg_r2(prices, times_sec):
    n = len(prices)
    if n < 2:
        return None
    x = np.array(times_sec, dtype=float)
    y = np.array(prices, dtype=float)
    x_mean = x.mean()
    y_mean = y.mean()
    var_x = ((x - x_mean) ** 2).sum()
    var_y = ((y - y_mean) ** 2).sum()
    if var_x <= 0.0 or var_y <= 0.0:
        return None
    cov_xy = ((x - x_mean) * (y - y_mean)).sum()
    beta = cov_xy / var_x
    alpha = y_mean - beta * x_mean
    y_hat = alpha + beta * x
    ss_tot = ((y - y_mean) ** 2).sum()
    ss_res = ((y - y_hat) ** 2).sum()
    if ss_tot <= 0.0:
        return None
    r2 = 1.0 - ss_res / ss_tot
    return float(r2)


# ----------------------------------------------------------------------
# Main incremental builder
# ----------------------------------------------------------------------

def process_ticks_incremental():
    conn = get_conn()
    conn.autocommit = False

    meta_cur = conn.cursor()

    # Any ticks at all?
    meta_cur.execute(
        "SELECT MIN(id), MAX(id) FROM ticks WHERE symbol = %s",
        (SYMBOL,),
    )
    min_id, max_id = meta_cur.fetchone()
    if min_id is None or max_id is None:
        print("No ticks for symbol", SYMBOL)
        conn.close()
        return

    # Last pivot if exists
    meta_cur.execute(
        """
        SELECT id, tick_id, ts, price, ptype
        FROM piv
        ORDER BY tick_id DESC
        LIMIT 1
        """
    )
    row = meta_cur.fetchone()
    have_prev_piv = row is not None

    state = RADCState()

    if have_prev_piv:
        last_piv_id, last_piv_tick_id, last_piv_ts, last_piv_price, last_piv_ptype = row
        start_from_tick_id = last_piv_tick_id
        print(
            "Continuing from last pivot id=%d at tick %d price=%.2f ptype=%d"
            % (last_piv_id, last_piv_tick_id, last_piv_price, last_piv_ptype)
        )

        # Seed state from last pivot
        state.curr_piv_id = last_piv_id
        state.curr_piv_tick_id = last_piv_tick_id
        state.curr_piv_ts = last_piv_ts
        state.curr_piv_price = last_piv_price
        state.curr_piv_ptype = last_piv_ptype

        state.prev_price = last_piv_price
        state.prev_ts = last_piv_ts
        state.prev_tick_id = last_piv_tick_id

        state.dir = +1 if last_piv_ptype == -1 else -1

        state.ext_price = last_piv_price
        state.ext_tick_id = last_piv_tick_id
        state.ext_ts = last_piv_ts
        state.ext_sigma = 0.0

        state.swing_start_piv_id = last_piv_id
        state.swing_start_tick_id = last_piv_tick_id
        state.swing_start_ts = last_piv_ts
        state.swing_start_price = last_piv_price

        state.swing_tick_count = 1

        # rebuild recent swing_zscore memory
        meta_cur.execute(
            """
            SELECT swing_zscore
            FROM piv
            WHERE swing_zscore IS NOT NULL
            ORDER BY tick_id DESC
            LIMIT %s
            """,
            (SWING_MEMORY,),
        )
        z_rows = meta_cur.fetchall()
        for (sz,) in reversed(z_rows):
            state.recent_swings.append(float(sz))

    else:
        start_from_tick_id = min_id
        print("No previous pivots, starting from first tick", start_from_tick_id)

    meta_cur.close()

    write_cur = conn.cursor()

    last_processed_tick_id = start_from_tick_id - 1
    total_ticks = 0
    swings_this_run = 0
    stop = False

    swing_prices = []
    swing_times = []

    print(
        "Incremental RADC run: symbol=%s, start_from_tick_id=%d"
        % (SYMBOL, start_from_tick_id)
    )
    sys.stdout.flush()

    while not stop and total_ticks < MAX_TICKS_PER_RUN:
        # Load next chunk of ticks
        read_cur = conn.cursor()
        read_cur.execute(
            """
            SELECT id, timestamp, mid
            FROM ticks
            WHERE symbol = %s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (SYMBOL, last_processed_tick_id, TICK_CHUNK_SIZE),
        )
        rows = read_cur.fetchall()
        read_cur.close()

        if not rows:
            break

        batch_size = len(rows)
        batch_first_id = rows[0][0]
        batch_last_id = rows[-1][0]
        print(
            "[batch] loaded %d ticks (id %d .. %d)"
            % (batch_size, batch_first_id, batch_last_id)
        )
        sys.stdout.flush()

        for tick_id, ts, price in rows:
            last_processed_tick_id = tick_id
            total_ticks += 1

            # first tick seen by the state
            if state.prev_price is None:
                state.prev_price = price
                state.prev_ts = ts
                state.prev_tick_id = tick_id
                continue

            r = price - state.prev_price
            state.update_vol(r)

            # establish first pivot if none yet
            if state.curr_piv_id is None:
                move = price - state.prev_price
                if move >= 0.0:
                    state.dir = +1
                    ptype = -1
                else:
                    state.dir = -1
                    ptype = +1

                pivot_id = insert_pivot(
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

                state.curr_piv_id = pivot_id
                state.curr_piv_tick_id = state.prev_tick_id
                state.curr_piv_ts = state.prev_ts
                state.curr_piv_price = state.prev_price
                state.curr_piv_ptype = ptype

                state.ext_price = state.curr_piv_price
                state.ext_tick_id = state.curr_piv_tick_id
                state.ext_ts = state.curr_piv_ts
                state.ext_sigma = state.sigma

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

            # Only accumulate swing stats once we have a pivot
            if state.curr_piv_id is not None:
                state.swing_tick_count += 1
                state.swing_rv += r * r
                state.swing_sigma_sum += state.sigma
                if state.sigma > state.swing_sigma_max:
                    state.swing_sigma_max = state.sigma

                dur_since_start = (ts - state.swing_start_ts).total_seconds()
                if dur_since_start < 0.0:
                    dur_since_start = 0.0
                swing_prices.append(price)
                swing_times.append(dur_since_start)

                # update extreme and counter move
                if state.dir == +1:
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

                do_pivot = False

                if state.ext_tick_id is not None and state.ext_tick_id != state.curr_piv_tick_id:
                    if state.dir == +1:
                        swing_abs = state.ext_price - state.curr_piv_price
                    else:
                        swing_abs = state.curr_piv_price - state.ext_price

                    swing_vol = swing_abs / max(state.ext_sigma, VOL_EPS)
                    qsize = state.get_qsize()

                    enough_swing = swing_vol >= MIN_SWING_VOL
                    enough_counter = counter_vol >= max(qsize, MIN_COUNTER_VOL)
                    ratio_ok = counter_vol >= COUNTER_RATIO * swing_vol
                    enough_ticks = state.swing_tick_count >= MIN_TICKS_PER_SWING
                    enough_time = dur_since_start >= MIN_DURATION_SEC

                    if (
                        enough_swing
                        and enough_counter
                        and ratio_ok
                        and enough_ticks
                        and enough_time
                    ):
                        do_pivot = True

                if do_pivot:
                    new_ptype = +1 if state.dir == +1 else -1
                    new_piv_id = insert_pivot(
                        write_cur,
                        tick_id=state.ext_tick_id,
                        ts=state.ext_ts,
                        price=state.ext_price,
                        ptype=new_ptype,
                        vol_local=state.ext_sigma,
                        swing_zscore=None,
                        energy=None,
                        reg_code=None,
                        prev_piv_id=state.curr_piv_id,
                        next_piv_id=None,
                        meta=None,
                    )

                    update_pivot_links(
                        write_cur,
                        pivot_id=state.curr_piv_id,
                        next_piv_id=new_piv_id,
                    )

                    p_start = state.swing_start_price
                    p_end = state.ext_price
                    dir_ = +1 if p_end > p_start else -1

                    ret = p_end - p_start
                    ret_abs = abs(ret)
                    dur_sec = (state.ext_ts - state.swing_start_ts).total_seconds()
                    tick_count = state.swing_tick_count
                    if tick_count > 0:
                        vol_mean = state.swing_sigma_sum / float(tick_count)
                    else:
                        vol_mean = state.sigma
                    vol_max = state.swing_sigma_max
                    rv = state.swing_rv
                    if dur_sec <= 0.0:
                        vel = ret_abs
                    else:
                        vel = ret_abs / dur_sec
                    lin_r2 = compute_lin_reg_r2(swing_prices, swing_times)
                    imp = ret_abs / max(vol_mean, VOL_EPS)

                    insert_swing(
                        write_cur,
                        start_piv_id=state.swing_start_piv_id,
                        end_piv_id=new_piv_id,
                        start_tick_id=state.swing_start_tick_id,
                        end_tick_id=state.ext_tick_id,
                        start_ts=state.swing_start_ts,
                        end_ts=state.ext_ts,
                        dir_=dir_,
                        p_start=p_start,
                        p_end=p_end,
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

                    swing_z = ret_abs / max(vol_mean, VOL_EPS)
                    update_pivot_swing_zscore(write_cur, new_piv_id, swing_z)
                    state.recent_swings.append(swing_z)

                    state.total_swings_inserted += 1
                    swings_this_run += 1

                    if swings_this_run % COMMIT_EVERY_N_SWINGS == 0:
                        conn.commit()
                        print(
                            "[commit] swings_this_run=%d, last_tick_id=%d"
                            % (swings_this_run, tick_id)
                        )
                        sys.stdout.flush()

                    # prepare next swing
                    state.curr_piv_id = new_piv_id
                    state.curr_piv_tick_id = state.ext_tick_id
                    state.curr_piv_ts = state.ext_ts
                    state.curr_piv_price = state.ext_price
                    state.curr_piv_ptype = new_ptype
                    state.dir = -state.dir

                    state.ext_price = state.curr_piv_price
                    state.ext_tick_id = state.curr_piv_tick_id
                    state.ext_ts = state.curr_piv_ts
                    state.ext_sigma = state.sigma

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

                    if swings_this_run >= MAX_SWINGS_PER_RUN:
                        print(
                            "[stop] reached MAX_SWINGS_PER_RUN=%d, last_tick_id=%d"
                            % (MAX_SWINGS_PER_RUN, tick_id)
                        )
                        sys.stdout.flush()
                        stop = True
                        break

            # update previous tick for next step
            state.prev_price = price
            state.prev_ts = ts
            state.prev_tick_id = tick_id

            if total_ticks % REPORT_EVERY_N_TICKS == 0:
                print(
                    "[progress] ticks_this_run=%d, swings_this_run=%d, last_tick_id=%d"
                    % (total_ticks, swings_this_run, tick_id)
                )
                sys.stdout.flush()

        if stop:
            break

        if total_ticks >= MAX_TICKS_PER_RUN:
            print(
                "[stop] reached MAX_TICKS_PER_RUN=%d at tick_id=%d"
                % (MAX_TICKS_PER_RUN, last_processed_tick_id)
            )
            sys.stdout.flush()
            break

    conn.commit()
    write_cur.close()
    conn.close()

    print(
        "Run finished. ticks_this_run=%d, swings_this_run=%d"
        % (total_ticks, swings_this_run)
    )
    sys.stdout.flush()


if __name__ == "__main__":
    try:
        process_ticks_incremental()
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
