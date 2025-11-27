# PATH: jobs/buildZonePersonality.py
#
# Build "zone personality" features for a single zone and store them
# in the zone_personality table.
#
# Usage:
#   python -m jobs.buildZonePersonality 5
#
# This will compute features for zone_id = 5.

import argparse
import math
from typing import List, Dict, Any, Optional

from db import get_conn  # assumes existing helper as in other jobs

EPS = 1e-9
DELAY_LEVELS = (0.2, 0.4)  # "inner" $ distances for delay metrics
SWING_EPS = 0.01           # minimal price move to consider as a swing step


def sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def fetch_zone(conn, zone_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, start_id, end_id, direction
            FROM zones
            WHERE id = %s
            """,
            (zone_id,),
        )
        row = cur.fetchone()

    if not row:
        return None

    return {
        "zone_id": row[0],
        "start_id": row[1],
        "end_id": row[2],
        "dir_zone": int(row[3]) if row[3] is not None else 0,
    }


def fetch_ticks(conn, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, "timestamp", mid
            FROM ticks
            WHERE id BETWEEN %s AND %s
            ORDER BY id
            """,
            (start_id, end_id),
        )
        rows = cur.fetchall()

    ticks = []
    for r in rows:
        ticks.append(
            {
                "id": r[0],
                "ts": r[1],
                "mid": float(r[2]),
            }
        )
    return ticks


def fetch_prev_context(conn, zone_id: int) -> Dict[str, Any]:
    """
    Look at earlier zone_personality rows (if any) to fill
    prev_zone_id, prev_dir, ratios, and rolling stats.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                zone_id,
                dir_zone,
                full_range,
                duration_sec,
                speed
            FROM zone_personality
            WHERE zone_id < %s
            ORDER BY zone_id DESC
            LIMIT 5
            """,
            (zone_id,),
        )
        rows = cur.fetchall()

    if not rows:
        # No previous zones recorded
        return {
            "prev_zone_id": None,
            "prev_dir": 0,
            "prev_range": 0.0,
            "prev_duration": 0.0,
            "ratio_range": 0.0,
            "ratio_duration": 0.0,
            "ratio_speed": 0.0,
            "prev2_dir": 0,
            "rolling_range3": 0.0,
            "rolling_speed3": 0.0,
            "rolling_up5": 0.0,
        }

    # rows are [ (zone_id, dir_zone, full_range, duration_sec, speed), ... ]
    prev = rows[0]
    prev_zone_id = prev[0]
    prev_dir = int(prev[1]) if prev[1] is not None else 0
    prev_range = float(prev[2]) if prev[2] is not None else 0.0
    prev_duration = float(prev[3]) if prev[3] is not None else 0.0
    prev_speed = float(prev[4]) if prev[4] is not None else 0.0

    prev2_dir = 0
    if len(rows) > 1 and rows[1][1] is not None:
        prev2_dir = int(rows[1][1])

    # Rolling last 3 ranges & speeds
    last3 = rows[:3]
    if last3:
        rolling_range3 = sum(float(r[2] or 0.0) for r in last3) / len(last3)
        rolling_speed3 = sum(float(r[4] or 0.0) for r in last3) / len(last3)
    else:
        rolling_range3 = 0.0
        rolling_speed3 = 0.0

    # Rolling last 5 up directions
    ups = 0
    for r in rows:
        if int(r[1] or 0) == 1:
            ups += 1
    rolling_up5 = ups / len(rows)

    # Ratios will be filled later when we know current range/speed/duration
    return {
        "prev_zone_id": prev_zone_id,
        "prev_dir": prev_dir,
        "prev_range": prev_range,
        "prev_duration": prev_duration,
        "prev_speed": prev_speed,
        "prev2_dir": prev2_dir,
        "rolling_range3": rolling_range3,
        "rolling_speed3": rolling_speed3,
        "rolling_up5": rolling_up5,
    }


def classify_zone_type(
    body_range_ratio: float,
    noise_ratio: float,
    pos_of_extreme: float,
    full_range: float,
) -> int:
    """
    Very rough PA-style classification:
      0 = unknown / normal
      1 = impulse / breakout
      2 = exhaustion
      3 = consolidation
    """
    # Consolidation: tiny range or very wick-y & noisy
    if full_range < 0.3 or (body_range_ratio < 0.3 and noise_ratio > 0.5):
        return 3

    # Impulse: strong body, low noise
    if body_range_ratio > 0.6 and noise_ratio < 0.3:
        return 1

    # Exhaustion: big bar, noisy, extreme at the end
    if body_range_ratio > 0.5 and noise_ratio > 0.4 and pos_of_extreme > 0.7:
        return 2

    return 0


def compute_zone_personality_for_ticks(
    ticks: List[Dict[str, Any]],
    dir_zone: int,
) -> Dict[str, Any]:
    if not ticks:
        # Safeguard: empty zone (shouldn't happen)
        return {
            "open_price": 0.0,
            "close_price": 0.0,
            "high_price": 0.0,
            "low_price": 0.0,
            "net_move": 0.0,
            "abs_move": 0.0,
            "full_range": 0.0,
            "body_range_ratio": 0.0,
            "upper_wick_ratio": 0.0,
            "lower_wick_ratio": 0.0,
            "duration_sec": 0.0,
            "n_ticks": 0,
            "speed": 0.0,
            "noise_ratio": 0.0,
            "pos_of_extreme": 0.0,
            "delay_frac_0_2": 1.0,
            "delay_frac_0_4": 1.0,
            "n_swings": 0,
            "swing_dir_changes": 0,
            "avg_swing_range": 0.0,
            "max_swing_range": 0.0,
            "zone_type_code": 0,
        }

    mids = [t["mid"] for t in ticks]
    ts_list = [t["ts"] for t in ticks]
    n = len(mids)

    open_price = mids[0]
    close_price = mids[-1]
    high_price = max(mids)
    low_price = min(mids)

    net_move = close_price - open_price
    abs_move = abs(net_move)
    full_range = high_price - low_price

    body_range_ratio = abs_move / (full_range + EPS)
    upper_wick = high_price - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low_price
    upper_wick_ratio = upper_wick / (full_range + EPS)
    lower_wick_ratio = lower_wick / (full_range + EPS)

    # Duration
    duration_sec = (ts_list[-1] - ts_list[0]).total_seconds() if n > 1 else 0.0
    n_ticks = n

    dir_sgn = int(dir_zone or 0)
    if dir_sgn == 0:
        # fall back to net move if direction not set
        dir_sgn = sign(net_move)

    speed = 0.0
    if duration_sec > 0 and dir_sgn != 0:
        speed = (dir_sgn * net_move) / duration_sec

    # Noise and direction consistency
    total_step = 0.0
    noise_ratio = 0.0
    if n > 1:
        last_mid = mids[0]
        for mid in mids[1:]:
            total_step += abs(mid - last_mid)
            last_mid = mid
        if total_step > 0:
            noise_ratio = (total_step - abs_move) / (total_step + EPS)

    # Position of extreme (in zone direction)
    if dir_sgn >= 0:
        # bullish or neutral -> look at high
        extreme_index = max(range(n), key=lambda i: mids[i])
    else:
        extreme_index = min(range(n), key=lambda i: mids[i])
    pos_of_extreme = extreme_index / (n - 1) if n > 1 else 0.0

    # Delay metrics for two inner distances (0.2 and 0.4)
    def delay_to(threshold: float) -> float:
        if dir_sgn == 0 or duration_sec <= 0:
            return 1.0
        target = open_price + dir_sgn * threshold
        hit_ts = None
        for i in range(n):
            mid = mids[i]
            if (dir_sgn == 1 and mid >= target) or (dir_sgn == -1 and mid <= target):
                hit_ts = ts_list[i]
                break
        if hit_ts is None:
            return 1.0
        return (hit_ts - ts_list[0]).total_seconds() / duration_sec

    delay_frac_0_2 = delay_to(DELAY_LEVELS[0])
    delay_frac_0_4 = delay_to(DELAY_LEVELS[1])

    # Micro swings inside the zone
    n_swings = 0
    swing_dir_changes = 0
    swing_ranges: List[float] = []

    run_dir = 0
    run_min = mids[0]
    run_max = mids[0]

    for i in range(1, n):
        delta = mids[i] - mids[i - 1]
        if abs(delta) < SWING_EPS:
            # ignore tiny moves, just extend min/max
            run_min = min(run_min, mids[i])
            run_max = max(run_max, mids[i])
            continue

        step_dir = sign(delta)

        if run_dir == 0:
            # start first swing
            run_dir = step_dir
            run_min = min(run_min, mids[i])
            run_max = max(run_max, mids[i])
        elif step_dir == run_dir:
            # continue same swing
            run_min = min(run_min, mids[i])
            run_max = max(run_max, mids[i])
        else:
            # direction change -> close previous swing
            swing_ranges.append(run_max - run_min)
            n_swings += 1
            swing_dir_changes += 1

            # start new swing
            run_dir = step_dir
            run_min = mids[i - 1]
            run_max = mids[i]
            if run_min > run_max:
                run_min, run_max = run_max, run_min

    # close last swing (if any)
    if run_dir != 0:
        swing_ranges.append(run_max - run_min)
        n_swings += 1

    if swing_ranges:
        avg_swing_range = sum(swing_ranges) / len(swing_ranges)
        max_swing_range = max(swing_ranges)
    else:
        avg_swing_range = 0.0
        max_swing_range = 0.0

    # PA-style type
    zone_type_code = classify_zone_type(
        body_range_ratio=body_range_ratio,
        noise_ratio=noise_ratio,
        pos_of_extreme=pos_of_extreme,
        full_range=full_range,
    )

    return {
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
        "zone_type_code": zone_type_code,
    }


def upsert_zone_personality(conn, zone_row, features, ctx):
    """
    Insert or update zone_personality row for given zone.
    ctx is prev-context; we finalize ratios here.
    """
    zone_id = zone_row["zone_id"]
    start_id = zone_row["start_id"]
    end_id = zone_row["end_id"]
    dir_zone = zone_row["dir_zone"]

    full_range = features["full_range"]
    duration_sec = features["duration_sec"]
    speed = features["speed"]

    prev_range = ctx["prev_range"]
    prev_duration = ctx["prev_duration"]
    prev_speed = ctx["prev_speed"]

    ratio_range = full_range / (prev_range + EPS) if prev_range > 0 else 0.0
    ratio_duration = (
        duration_sec / (prev_duration + EPS) if prev_duration > 0 else 0.0
    )
    ratio_speed = speed / (prev_speed + EPS) if prev_speed != 0 else 0.0

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO zone_personality (
                zone_id,
                start_id,
                end_id,
                dir_zone,
                open_price,
                close_price,
                high_price,
                low_price,
                net_move,
                abs_move,
                full_range,
                body_range_ratio,
                upper_wick_ratio,
                lower_wick_ratio,
                duration_sec,
                n_ticks,
                speed,
                noise_ratio,
                pos_of_extreme,
                delay_frac_0_2,
                delay_frac_0_4,
                n_swings,
                swing_dir_changes,
                avg_swing_range,
                max_swing_range,
                prev_zone_id,
                prev_dir,
                prev_range,
                prev_duration,
                ratio_range,
                ratio_duration,
                ratio_speed,
                prev2_dir,
                rolling_range3,
                rolling_speed3,
                rolling_up5,
                zone_type_code
            )
            VALUES (
                %(zone_id)s,
                %(start_id)s,
                %(end_id)s,
                %(dir_zone)s,
                %(open_price)s,
                %(close_price)s,
                %(high_price)s,
                %(low_price)s,
                %(net_move)s,
                %(abs_move)s,
                %(full_range)s,
                %(body_range_ratio)s,
                %(upper_wick_ratio)s,
                %(lower_wick_ratio)s,
                %(duration_sec)s,
                %(n_ticks)s,
                %(speed)s,
                %(noise_ratio)s,
                %(pos_of_extreme)s,
                %(delay_frac_0_2)s,
                %(delay_frac_0_4)s,
                %(n_swings)s,
                %(swing_dir_changes)s,
                %(avg_swing_range)s,
                %(max_swing_range)s,
                %(prev_zone_id)s,
                %(prev_dir)s,
                %(prev_range)s,
                %(prev_duration)s,
                %(ratio_range)s,
                %(ratio_duration)s,
                %(ratio_speed)s,
                %(prev2_dir)s,
                %(rolling_range3)s,
                %(rolling_speed3)s,
                %(rolling_up5)s,
                %(zone_type_code)s
            )
            ON CONFLICT (zone_id) DO UPDATE
            SET
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
                max_swing_range = EXCLUDED.max_swing_range,
                prev_zone_id = EXCLUDED.prev_zone_id,
                prev_dir = EXCLUDED.prev_dir,
                prev_range = EXCLUDED.prev_range,
                prev_duration = EXCLUDED.prev_duration,
                ratio_range = EXCLUDED.ratio_range,
                ratio_duration = EXCLUDED.ratio_duration,
                ratio_speed = EXCLUDED.ratio_speed,
                prev2_dir = EXCLUDED.prev2_dir,
                rolling_range3 = EXCLUDED.rolling_range3,
                rolling_speed3 = EXCLUDED.rolling_speed3,
                rolling_up5 = EXCLUDED.rolling_up5,
                zone_type_code = EXCLUDED.zone_type_code
            """,
            {
                "zone_id": zone_id,
                "start_id": start_id,
                "end_id": end_id,
                "dir_zone": dir_zone,
                **features,
                "prev_zone_id": ctx["prev_zone_id"],
                "prev_dir": ctx["prev_dir"],
                "prev_range": prev_range,
                "prev_duration": prev_duration,
                "ratio_range": ratio_range,
                "ratio_duration": ratio_duration,
                "ratio_speed": ratio_speed,
                "prev2_dir": ctx["prev2_dir"],
                "rolling_range3": ctx["rolling_range3"],
                "rolling_speed3": ctx["rolling_speed3"],
                "rolling_up5": ctx["rolling_up5"],
            },
        )


def main():
    parser = argparse.ArgumentParser(
        description="Build personality features for a single zone."
    )
    parser.add_argument(
        "zone_id",
        type=int,
        help="Zone id to process (e.g. 5)",
    )
    args = parser.parse_args()
    zone_id = args.zone_id

    conn = get_conn()
    try:
        zone = fetch_zone(conn, zone_id)
        if not zone:
            print(f"No zone with id {zone_id}")
            return

        ticks = fetch_ticks(conn, zone["start_id"], zone["end_id"])
        if not ticks:
            print(f"No ticks for zone {zone_id} (ids {zone['start_id']}–{zone['end_id']})")
            return

        ctx = fetch_prev_context(conn, zone_id)
        features = compute_zone_personality_for_ticks(ticks, zone["dir_zone"])

        upsert_zone_personality(conn, zone, features, ctx)
        conn.commit()
        print(f"Zone {zone_id} personality stored.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
