# ml/buildPatternWindows.py
#
# Layer 3: pattern windows over swings.
#
# For each sliding window of consecutive swings (size W), build
# an aggregated pattern row in pwin.

import sys
import psycopg2
from psycopg2.extras import Json

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
}

SYMBOL = "XAUUSD"  # currently swg is XAU-only; kept for future use.

WINDOW_SIZE = 4           # number of swings per window
BATCH_INSERT = 2000       # number of windows per INSERT batch
TRUNCATE_PWIN_FIRST = True


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def safe_ratio(num, den):
    if den is None:
        return None
    if den == 0:
        return None
    return float(num) / float(den)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def build_pwin():
    conn = get_conn()
    conn.autocommit = False

    cur = conn.cursor()

    if TRUNCATE_PWIN_FIRST:
        print("Truncating pwin...")
        cur.execute("TRUNCATE TABLE pwin;")
        conn.commit()

    # Load all swings ordered in time. For now we assume swg only contains XAUUSD.
    # If later swg gets a symbol column, filter by it here.
    print("Loading swings from swg...")
    cur.execute(
        """
        SELECT
            id,
            start_piv_id,
            end_piv_id,
            start_tick_id,
            end_tick_id,
            start_ts,
            end_ts,
            dir,
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
            imp
        FROM swg
        ORDER BY start_ts ASC
        """
    )

    swings = cur.fetchall()
    n_swings = len(swings)
    print(f"Loaded {n_swings} swings.")

    if n_swings < WINDOW_SIZE:
        print("Not enough swings to build any windows.")
        cur.close()
        conn.close()
        return

    insert_sql = """
        INSERT INTO pwin (
            start_swg_id, end_swg_id,
            start_piv_id, end_piv_id,
            start_ts, end_ts,
            swg_ids, num_swgs,
            dir_pattern, up_count, dn_count,
            net_ret, abs_ret_sum, dur_sec,
            mean_vel, max_vel, max_imp,
            vol_mean, vol_max,
            net_ret_norm, vel_contrast, imp_contrast,
            meta
        ) VALUES (
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,
            %s
        )
    """

    def row_to_dict(row):
        (
            sid,
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
        ) = row
        return {
            "id": sid,
            "start_piv_id": start_piv_id,
            "end_piv_id": end_piv_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "dir": dir_,
            "ret": ret,
            "ret_abs": ret_abs,
            "dur_sec": dur_sec,
            "vol_mean": vol_mean,
            "vol_max": vol_max,
            "vel": vel,
            "imp": imp,
        }

    swings_dict = [row_to_dict(r) for r in swings]

    total_windows = 0
    batch = []

    print("Building windows...")
    for i in range(0, n_swings - WINDOW_SIZE + 1):
        window = swings_dict[i : i + WINDOW_SIZE]

        start_swg = window[0]
        end_swg = window[-1]

        swg_ids = [w["id"] for w in window]
        dirs = [w["dir"] for w in window]
        up_count = sum(1 for d in dirs if d > 0)
        dn_count = sum(1 for d in dirs if d < 0)

        net_ret = sum(w["ret"] for w in window)
        abs_ret_sum = sum(abs(w["ret"]) for w in window)
        dur_sec = (end_swg["end_ts"] - start_swg["start_ts"]).total_seconds()

        vel_list = [w["vel"] for w in window if w["vel"] is not None]
        mean_vel = sum(vel_list) / len(vel_list) if vel_list else None
        max_vel = max(vel_list) if vel_list else None

        imp_list = [w["imp"] for w in window if w["imp"] is not None]
        max_imp = max(imp_list) if imp_list else None

        vol_mean_list = [w["vol_mean"] for w in window if w["vol_mean"] is not None]
        vol_mean = sum(vol_mean_list) / len(vol_mean_list) if vol_mean_list else None

        vol_max_list = [w["vol_max"] for w in window if w["vol_max"] is not None]
        vol_max = max(vol_max_list) if vol_max_list else None

        net_ret_norm = safe_ratio(net_ret, abs_ret_sum)
        vel_contrast = safe_ratio(max_vel, mean_vel) if mean_vel is not None else None

        # A simple "importance contrast": max_imp / (abs_ret_sum / dur_sec)
        base_rate = safe_ratio(abs_ret_sum, dur_sec) if dur_sec is not None else None
        imp_contrast = safe_ratio(max_imp, base_rate) if base_rate is not None else None

        meta = None  # placeholder for later pattern labels / clustering tags

        batch.append(
            (
                start_swg["id"],
                end_swg["id"],
                start_swg["start_piv_id"],
                end_swg["end_piv_id"],
                start_swg["start_ts"],
                end_swg["end_ts"],
                swg_ids,
                WINDOW_SIZE,
                dirs,
                up_count,
                dn_count,
                net_ret,
                abs_ret_sum,
                dur_sec,
                mean_vel,
                max_vel,
                max_imp,
                vol_mean,
                vol_max,
                net_ret_norm,
                vel_contrast,
                imp_contrast,
                Json(meta),
            )
        )

        total_windows += 1

        if len(batch) >= BATCH_INSERT:
            with conn.cursor() as wcur:
                wcur.executemany(insert_sql, batch)
            conn.commit()
            print(f"Inserted {total_windows} windows so far...")
            batch = []

    # Insert any remaining windows
    if batch:
        with conn.cursor() as wcur:
            wcur.executemany(insert_sql, batch)
        conn.commit()
        print(f"Inserted {total_windows} windows in total.")

    cur.close()
    conn.close()
    print("Done building pwin.")


if __name__ == "__main__":
    try:
        build_pwin()
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
