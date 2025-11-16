import math
from collections import deque
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

# ============================
# CONFIG
# ============================

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <-- put your real password
DB_HOST = "localhost"
DB_PORT = 5432

BATCH_SIZE = 10_000

# Velocity thresholds (for vel_cat) in $/second
VEL_SLOW = 0.05
VEL_FAST = 0.30

# Kalman filter parameters (simple 1D filter)
KAL_Q = 0.01  # process noise
KAL_R = 1.0   # measurement noise

# Volatility window (for vol_val: rolling std of mic_dm)
VOL_WINDOW = 50

# Momentum window (mom_val = mid - mid_N_ticks_ago)
MOM_WINDOW = 20


# ============================
# Helpers
# ============================

def sign(x: float, eps: float = 1e-9) -> int:
    if x is None:
        return 0
    if x > eps:
        return 1
    if x < -eps:
        return -1
    return 0


def vel_category(v: float) -> int:
    if v is None:
        return 0
    if v >= VEL_FAST:
        return 2
    if v >= VEL_SLOW:
        return 1
    if v <= -VEL_FAST:
        return -2
    if v <= -VEL_SLOW:
        return -1
    return 0


def slope_category(d: float, slow: float, fast: float) -> int:
    """Generic 5-level binning for kal_chg or mom_val."""
    if d >= fast:
        return 2
    if d >= slow:
        return 1
    if d <= -fast:
        return -2
    if d <= -slow:
        return -1
    return 0


class RollingStd:
    """Simple rolling std for mic_dm using fixed window."""
    def __init__(self, window: int):
        self.window = window
        self.buf = deque()
        self.sum = 0.0
        self.sumsq = 0.0

    def push(self, x: float):
        self.buf.append(x)
        self.sum += x
        self.sumsq += x * x
        if len(self.buf) > self.window:
            old = self.buf.popleft()
            self.sum -= old
            self.sumsq -= old * old

    def std(self) -> Optional[float]:
        n = len(self.buf)
        if n < 2:
            return None
        mean = self.sum / n
        var = (self.sumsq / n) - mean * mean
        if var < 0:
            var = 0.0
        return math.sqrt(var)


# ============================
# Main builder
# ============================

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = False

    cur = conn.cursor()

    # Server-side cursor to stream ticks in order
    stream_cur = conn.cursor(name="tick_stream")
    stream_cur.itersize = BATCH_SIZE

    stream_cur.execute("""
        SELECT id, "timestamp", mid
        FROM ticks
        ORDER BY "timestamp", id
    """)

    prev_mid = None
    prev_ts = None

    # gap state
    current_date = None
    last_mid_of_day = None

    # vel state
    prev_vel_cat = None
    vel_grp = 0
    vel_pos = 0

    # kalman state
    kal_x = None  # state mean
    kal_p = None  # state covariance
    prev_kal_val = None
    kal_grp = 0
    kal_pos = 0
    prev_kal_cat = None

    # momentum state
    mom_window = deque()
    prev_mom_cat = None
    mom_grp = 0
    mom_pos = 0

    # volatility state
    vol_calc = RollingStd(VOL_WINDOW)

    insert_rows = []
    ticks_kal_updates = []
    total = 0

    print("Starting Phase 1 segments build...")

    while True:
        rows = stream_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for tick_id, ts, mid in rows:
            # ----------------------
            # mic: micro movement
            # ----------------------
            if prev_mid is None:
                mic_dm = 0.0
                mic_dt = 0.0
                mic_v = 0.0
            else:
                mic_dm = mid - prev_mid
                dt_seconds = (ts - prev_ts).total_seconds() if prev_ts is not None else 0.0
                mic_dt = dt_seconds if dt_seconds > 0 else 0.0
                mic_v = (mic_dm / mic_dt) if mic_dt > 0 else 0.0

            # ----------------------
            # gap: daily gaps
            # ----------------------
            tick_date = ts.date()
            if current_date is None:
                gap_flag = False
                gap_prev = None
                gap_sz = None
                gap_dir = None

                current_date = tick_date
                last_mid_of_day = mid
            else:
                if tick_date == current_date:
                    gap_flag = False
                    gap_prev = None
                    gap_sz = None
                    gap_dir = None

                    last_mid_of_day = mid
                else:
                    gap_flag = True
                    gap_prev = last_mid_of_day
                    if gap_prev is not None:
                        gap_sz = mid - gap_prev
                        gap_dir = sign(gap_sz, eps=0.01)
                    else:
                        gap_sz = None
                        gap_dir = None

                    current_date = tick_date
                    last_mid_of_day = mid

            # ----------------------
            # vel: velocity regimes
            # ----------------------
            vel_cat_val = vel_category(mic_v)

            if prev_vel_cat is None:
                vel_prev_val = None
                vel_grp = 1
                vel_pos = 0
            else:
                vel_prev_val = prev_vel_cat
                if vel_cat_val != prev_vel_cat:
                    vel_grp += 1
                    vel_pos = 0
                else:
                    vel_pos += 1

            vel_len = None  # later

            prev_vel_cat = vel_cat_val

            # ----------------------
            # kal: Kalman filter
            # ----------------------
            z = mid

            if kal_x is None:
                kal_x = z
                kal_p = 1.0
                kal_val = z
            else:
                x_pred = kal_x
                p_pred = kal_p + KAL_Q

                k_gain = p_pred / (p_pred + KAL_R)
                kal_x = x_pred + k_gain * (z - x_pred)
                kal_p = (1 - k_gain) * p_pred
                kal_val = kal_x

            if prev_kal_val is None:
                kal_chg = 0.0
            else:
                kal_chg = kal_val - prev_kal_val

            kal_cat_val = slope_category(kal_chg, slow=0.05, fast=0.30)

            if prev_kal_cat is None:
                kal_grp = 1
                kal_pos = 0
            else:
                if kal_cat_val != prev_kal_cat:
                    kal_grp += 1
                    kal_pos = 0
                else:
                    kal_pos += 1

            kal_len = None  # later

            prev_kal_val = kal_val
            prev_kal_cat = kal_cat_val

            ticks_kal_updates.append((tick_id, kal_val))

            # ----------------------
            # mom: momentum
            # ----------------------
            mom_window.append(mid)
            if len(mom_window) > MOM_WINDOW:
                mom_window.popleft()

            if len(mom_window) < MOM_WINDOW:
                mom_val = 0.0
            else:
                mom_val = mid - mom_window[0]

            mom_cat_val = slope_category(mom_val, slow=0.5, fast=1.5)

            if prev_mom_cat is None:
                mom_grp = 1
                mom_pos = 0
            else:
                if mom_cat_val != prev_mom_cat:
                    mom_grp += 1
                    mom_pos = 0
                else:
                    mom_pos += 1

            mom_len = None  # later
            prev_mom_cat = mom_cat_val

            # ----------------------
            # vol: local volatility
            # ----------------------
            vol_calc.push(mic_dm)
            vol_val = vol_calc.std()

            vol_cat = None
            vol_grp = None
            vol_pos = None
            vol_len = None

            # ----------------------
            # Collect row
            # ----------------------
            insert_rows.append((
                tick_id,

                mic_dm, mic_dt, mic_v,

                gap_flag, gap_prev, gap_sz, gap_dir,

                vel_cat_val, vel_prev_val, vel_grp, vel_pos, vel_len,

                None, None, None, None, None, None,  # str_*

                vol_val, vol_cat, vol_grp, vol_pos, vol_len,

                kal_val, kal_chg, kal_cat_val, kal_grp, kal_pos, kal_len,

                mom_val, mom_cat_val, mom_grp, mom_pos, mom_len,
            ))

            total += 1
            prev_mid = mid
            prev_ts = ts

        # =====================
        # FLUSH TO DATABASE
        # =====================
        if insert_rows:
            execute_values(
                cur,
                """
                INSERT INTO segments (
                    id,
                    mic_dm, mic_dt, mic_v,
                    gap_flag, gap_prev, gap_sz, gap_dir,
                    vel_cat, vel_prev, vel_grp, vel_pos, vel_len,
                    str_dir, str_grp, str_pos, str_len, str_amp, str_dur,
                    vol_val, vol_cat, vol_grp, vol_pos, vol_len,
                    kal_val, kal_chg, kal_cat, kal_grp, kal_pos, kal_len,
                    mom_val, mom_cat, mom_grp, mom_pos, mom_len
                ) VALUES %s
                """,
                insert_rows,
                page_size=BATCH_SIZE,
            )
            insert_rows.clear()

        if ticks_kal_updates:
            execute_values(
                cur,
                """
                UPDATE ticks AS t
                SET kal = v.kal_val
                FROM (VALUES %s) AS v(id, kal_val)
                WHERE t.id = v.id
                """,
                ticks_kal_updates,
                page_size=BATCH_SIZE,
            )
            ticks_kal_updates.clear()

        conn.commit()
        print(f"Processed {total} ticks...")

    stream_cur.close()
    cur.close()
    conn.close()

    print(f"Done. segments built for {total} ticks.")


if __name__ == "__main__":
    main()
