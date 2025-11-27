#!/usr/bin/env python3
import sys
import math
from datetime import datetime
from statistics import mean
import psycopg2


DB_CONFIG = {
    "dbname": "trading",
    "user": "babak",
    "password": "babak33044",
    "host": "localhost",
    "port": 5432,
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_zone(cursor, zone_id: int):
    """
    Assumes there is a 'zones' table with:
        id, start_id, end_id, dir_zone
    Adjust column names if your table is slightly different.
    """
    cursor.execute(
        """
        SELECT id, start_id, end_id, dir_zone
        FROM zones
        WHERE id = %s
        """,
        (zone_id,),
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Zone {zone_id} not found in zones table")

    zid, start_id, end_id, dir_zone = row
    return {
        "id": zid,
        "start_id": start_id,
        "end_id": end_id,
        "dir_zone": dir_zone,
    }


def fetch_ticks(cursor, start_id: int, end_id: int):
    """
    Assumes ticks table has:
        id, ts, price
    If your column names differ (e.g. time, mid), adjust here.
    """
    cursor.execute(
        """
        SELECT id, ts, price
        FROM ticks
        WHERE id BETWEEN %s AND %s
        ORDER BY id
        """,
        (start_id, end_id),
    )
    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f"No ticks between {start_id} and {end_id}")
    return rows  # list of (id, ts, price)


def compute_swings(prices):
    """
    Very simple swing logic:
    - Look at consecutive price deltas.
    - Every time the sign of the delta changes, we close a swing.
    - Swing range = max - min within that run.
    """
    if len(prices) < 3:
        return 0, 0, 0.0, 0.0

    swings = []
    current_sign = None
    current_min = prices[0]
    current_max = prices[0]

    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        sign = 0
        if delta > 0:
            sign = 1
        elif delta < 0:
            sign = -1

        # extend current swing if same direction or flat
        if current_sign is None:
            current_sign = sign

        if sign == 0 or sign == current_sign:
            current_min = min(current_min, prices[i])
            current_max = max(current_max, prices[i])
        else:
            # direction changed: close previous swing
            swings.append(current_max - current_min)
            current_sign = sign
            current_min = min(prices[i - 1], prices[i])
            current_max = max(prices[i - 1], prices[i])

    # close last swing
    if current_sign is not None:
        swings.append(current_max - current_min)

    swings = [abs(s) for s in swings if s is not None]

    if not swings:
        return 0, 0, 0.0, 0.0

    n_swings = len(swings)
    swing_dir_changes = max(0, n_swings - 1)
    avg_swing_range = float(mean(swings))
    max_swing_range = float(max(swings))

    return n_swings, swing_dir_changes, avg_swing_range, max_swing_range


def compute_delay_fraction(
    prices, times, full_range, dir_zone, frac
) -> float:
    """
    Rough definition:
    - For up zones: look for first time price reaches low + frac * full_range.
    - For down zones: look for first time price reaches high - frac * full_range.
    - For flat/0: use low + frac * full_range.
    Return fraction of total duration [0,1].
    """
    if full_range <= 0 or len(prices) < 2:
        return 0.0

    first_ts = times[0]
    last_ts = times[-1]
    total_sec = (last_ts - first_ts).total_seconds()
    if total_sec <= 0:
        return 0.0

    low = min(prices)
    high = max(prices)

    if dir_zone is None or dir_zone == 0:
        threshold = low + frac * full_range
        comparator = lambda p: p >= threshold
    elif dir_zone > 0:
        threshold = low + frac * full_range
        comparator = lambda p: p >= threshold
    else:
        threshold = high - frac * full_range
        comparator = lambda p: p <= threshold

    hit_time = None
    for p, t in zip(prices, times):
        if comparator(p):
            hit_time = t
            break

    if hit_time is None:
        return 1.0

    delay_sec = (hit_time - first_ts).total_seconds()
    return max(0.0, min(1.0, delay_sec / total_sec))


def build_zone_personality(zone_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1) Get zone basic info
            zone = fetch_zone(cur, zone_id)
            start_id = zone["start_id"]
            end_id = zone["end_id"]
            dir_zone = zone["dir_zone"]

            # 2) Get ticks for this zone
            ticks = fetch_ticks(cur, start_id, end_id)
            tick_ids = [r[0] for r in ticks]
            times = [r[1] for r in ticks]  # ts
            prices = [float(r[2]) for r in ticks]

            n_ticks = len(ticks)

            open_price = prices[0]
            close_price = prices[-1]
            high_price = max(prices)
            low_price = min(prices)

            net_move = close_price - open_price
            abs_move = abs(net_move)
            full_range = high_price - low_price

            if full_range > 0:
                body_range_ratio = abs_move / full_range
                upper_wick_ratio = (high_price - max(open_price, close_price)) / full_range
                lower_wick_ratio = (min(open_price, close_price) - low_price) / full_range
            else:
                body_range_ratio = 0.0
                upper_wick_ratio = 0.0
                lower_wick_ratio = 0.0

            # time / speed
            first_ts = times[0]
            last_ts = times[-1]
            duration_sec = (last_ts - first_ts).total_seconds()
            if duration_sec <= 0:
                duration_sec = 0.0
                speed = 0.0
            else:
                # speed as "net movement magnitude per second"
                speed = abs_move / duration_sec

            # noise ratio: how much the range is larger than the body
            if full_range > 0:
                noise_ratio = (full_range - abs_move) / full_range
            else:
                noise_ratio = 0.0

            # position of main extreme (in fraction of ticks)
            if n_ticks > 1:
                if dir_zone is None or dir_zone == 0:
                    # choose the more "dominant" extreme relative to open
                    up_dist = abs(high_price - open_price)
                    dn_dist = abs(low_price - open_price)
                    target_price = high_price if up_dist >= dn_dist else low_price
                elif dir_zone > 0:
                    target_price = high_price
                else:
                    target_price = low_price

                extreme_index = 0
                for i, p in enumerate(prices):
                    if p == target_price:
                        extreme_index = i
                        break
                pos_of_extreme = extreme_index / (n_ticks - 1)
            else:
                pos_of_extreme = 0.0

            delay_frac_0_2 = compute_delay_fraction(
                prices, times, full_range, dir_zone, 0.2
            )
            delay_frac_0_4 = compute_delay_fraction(
                prices, times, full_range, dir_zone, 0.4
            )

            n_swings, swing_dir_changes, avg_swing_range, max_swing_range = compute_swings(
                prices
            )

            # Insert / upsert into zone_personality
            cur.execute(
                """
                INSERT INTO zone_personality (
                    id,
                    start_id, end_id,
                    dir_zone,
                    open_price, close_price, high_price, low_price,
                    net_move, abs_move, full_range,
                    body_range_ratio, upper_wick_ratio, lower_wick_ratio,
                    duration_sec, n_ticks, speed, noise_ratio,
                    pos_of_extreme, delay_frac_0_2, delay_frac_0_4,
                    n_swings, swing_dir_changes, avg_swing_range, max_swing_range
                )
                VALUES (
                    %(id)s,
                    %(start_id)s, %(end_id)s,
                    %(dir_zone)s,
                    %(open_price)s, %(close_price)s, %(high_price)s, %(low_price)s,
                    %(net_move)s, %(abs_move)s, %(full_range)s,
                    %(body_range_ratio)s, %(upper_wick_ratio)s, %(lower_wick_ratio)s,
                    %(duration_sec)s, %(n_ticks)s, %(speed)s, %(noise_ratio)s,
                    %(pos_of_extreme)s, %(delay_frac_0_2)s, %(delay_frac_0_4)s,
                    %(n_swings)s, %(swing_dir_changes)s, %(avg_swing_range)s, %(max_swing_range)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    start_id = EXCLUDED.start_id,
                    end_id = EXCLUDED.end_id,
                    dir_zone = EXCLUDED.dir_zone,
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    net_move = EXCLUDED.net_move,
                    abs_move = EXCLUDED.abs_move,
                    full_range = EXCLUDED.full_range,
                    body_range_ratio = EXCLUDED.body_range_ratio,
                    upper_wick_ratio = EXCLUDED.upper_wick_ratio,
                    lower_wick_ratio = EXCLUDED.lower_wick_ratio,
                    duration_sec = EXCLUDED.duration_sec,
                    n_ticks = EXCLUDED.n_ticks,
                    speed = EXCLUDED.speed,
                    noise_ratio = EXCLUDED.noise_ratio,
                    pos_of_extreme = EXCLUDED.pos_of_extreme,
                    delay_frac_0_2 = EXCLUDED.delay_frac_0_2,
                    delay_frac_0_4 = EXCLUDED.delay_frac_0_4,
                    n_swings = EXCLUDED.n_swings,
                    swing_dir_changes = EXCLUDED.swing_dir_changes,
                    avg_swing_range = EXCLUDED.avg_swing_range,
                    max_swing_range = EXCLUDED.max_swing_range
                """,
                {
                    "id": zone_id,
                    "start_id": start_id,
                    "end_id": end_id,
                    "dir_zone": dir_zone,
                    "open_price": open_price,
                    "close_price": close_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "net_move": net_move,
                    "abs_move": abs_move,
                    "full_range": full_range,
                    "body_range_ratio": body_range_ratio,
                    "upper_wick_ratio": upper_wick_ratio,
                    "lower_wick_ratio": lower_wick_ratio,
                    "duration_sec": duration_sec,
                    "n_ticks": n_ticks,
                    "speed": speed,
                    "noise_ratio": noise_ratio,
                    "pos_of_extreme": pos_of_extreme,
                    "delay_frac_0_2": delay_frac_0_2,
                    "delay_frac_0_4": delay_frac_0_4,
                    "n_swings": n_swings,
                    "swing_dir_changes": swing_dir_changes,
                    "avg_swing_range": avg_swing_range,
                    "max_swing_range": max_swing_range,
                },
            )

    print(f"Zone personality built for zone {zone_id}")
    

def main():
    if len(sys.argv) != 2:
        print("Usage: python buildZonePersonality.py <zone_id>")
        sys.exit(1)

    try:
        zone_id = int(sys.argv[1])
    except ValueError:
        print("zone_id must be an integer")
        sys.exit(1)

    try:
        build_zone_personality(zone_id)
    except Exception as e:
        print(f"Error building zone personality for {zone_id}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
