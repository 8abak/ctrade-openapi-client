# ml/buildSwingBehavior.py
#
# Layer 2: swing–pivot behavior builder.
#
# For each pivot that has both a previous and next swing, we build
# a behavior row describing how those two swings compare.

import sys
import psycopg2
from psycopg2.extras import Json

# --------------------------------------------------------
# CONFIG
# --------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
}

SYMBOL = "XAUUSD"

BATCH_SIZE = 5000          # how many rows to fetch from join at a time
TRUNCATE_SPB_FIRST = True  # set False if you will append for ranges later


# --------------------------------------------------------
# DB helpers
# --------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def safe_ratio(num, den):
    if den is None:
        return None
    if den == 0:
        return None
    return float(num) / float(den)


# --------------------------------------------------------
# Main builder
# --------------------------------------------------------

def build_spb():
    conn = get_conn()
    conn.autocommit = False

    cur = conn.cursor()

    if TRUNCATE_SPB_FIRST:
        print("Truncating spb...")
        cur.execute("TRUNCATE TABLE spb;")
        conn.commit()

    # We join piv with swg before/after for this symbol.
    # Note: piv itself doesn't store symbol, but ticks & piv are per symbol;
    # we filter by the ticks range through swg joins (assuming swg built for this symbol only).
    # If you ever mix symbols in piv/swg, add a symbol column there and filter directly.

    join_sql = """
        SELECT
            p.id           AS pivot_id,
            p.ts           AS piv_ts,
            p.price        AS piv_price,
            p.ptype        AS ptype,

            s_prev.id      AS prev_swg_id,
            s_prev.ret     AS prev_ret,
            s_prev.ret_abs AS prev_ret_abs,
            s_prev.dur_sec AS prev_dur_sec,
            s_prev.tick_count AS prev_tick_count,
            s_prev.vol_mean   AS prev_vol_mean,
            s_prev.vol_max    AS prev_vol_max,
            s_prev.vel        AS prev_vel,
            s_prev.imp        AS prev_imp,

            s_next.id      AS next_swg_id,
            s_next.ret     AS next_ret,
            s_next.ret_abs AS next_ret_abs,
            s_next.dur_sec AS next_dur_sec,
            s_next.tick_count AS next_tick_count,
            s_next.vol_mean   AS next_vol_mean,
            s_next.vol_max    AS next_vol_max,
            s_next.vel        AS next_vel,
            s_next.imp        AS next_imp

        FROM piv p
        JOIN swg s_prev ON s_prev.end_piv_id = p.id
        JOIN swg s_next ON s_next.start_piv_id = p.id
        ORDER BY p.id ASC
    """

    print("Selecting pivot + swings join...")
    cur.execute(join_sql)

    insert_sql = """
        INSERT INTO spb (
            pivot_id, ts, price, ptype,
            prev_swg_id, next_swg_id,
            prev_ret, prev_ret_abs, prev_dur_sec, prev_tick_count,
            prev_vol_mean, prev_vol_max, prev_vel, prev_imp,
            next_ret, next_ret_abs, next_dur_sec, next_tick_count,
            next_vol_mean, next_vol_max, next_vel, next_imp,
            size_ratio, dur_ratio, vel_ratio, imp_ratio,
            meta
        ) VALUES (
            %s,%s,%s,%s,
            %s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,
            %s
        )
    """

    total_rows = 0

    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        out_rows = []
        for row in rows:
            (
                pivot_id,
                piv_ts,
                piv_price,
                ptype,
                prev_swg_id,
                prev_ret,
                prev_ret_abs,
                prev_dur_sec,
                prev_tick_count,
                prev_vol_mean,
                prev_vol_max,
                prev_vel,
                prev_imp,
                next_swg_id,
                next_ret,
                next_ret_abs,
                next_dur_sec,
                next_tick_count,
                next_vol_mean,
                next_vol_max,
                next_vel,
                next_imp,
            ) = row

            size_ratio = safe_ratio(next_ret_abs, prev_ret_abs)
            dur_ratio = safe_ratio(next_dur_sec, prev_dur_sec)
            vel_ratio = safe_ratio(next_vel, prev_vel)
            imp_ratio = safe_ratio(next_imp, prev_imp)

            meta = None  # keep for future clustering labels etc.

            out_rows.append(
                (
                    pivot_id,
                    piv_ts,
                    piv_price,
                    ptype,
                    prev_swg_id,
                    next_swg_id,
                    prev_ret,
                    prev_ret_abs,
                    prev_dur_sec,
                    prev_tick_count,
                    prev_vol_mean,
                    prev_vol_max,
                    prev_vel,
                    prev_imp,
                    next_ret,
                    next_ret_abs,
                    next_dur_sec,
                    next_tick_count,
                    next_vol_mean,
                    next_vol_max,
                    next_vel,
                    next_imp,
                    size_ratio,
                    dur_ratio,
                    vel_ratio,
                    imp_ratio,
                    Json(meta),
                )
            )

        with conn.cursor() as wcur:
            wcur.executemany(insert_sql, out_rows)

        total_rows += len(out_rows)
        conn.commit()
        print(f"Inserted {total_rows} spb rows so far...")

    cur.close()
    conn.close()
    print(f"Done. Total spb rows inserted: {total_rows}")


if __name__ == "__main__":
    try:
        build_spb()
    except KeyboardInterrupt:
        print("Interrupted by user")
        sys.exit(1)
