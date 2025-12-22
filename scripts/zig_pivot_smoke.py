import argparse

from backend.db import get_conn, dict_cur
from jobs.buildZigPivots import recompute_zig_pivots_for_segment


def validate_pivots(rows):
    if not rows:
        return "no pivots found"

    last_ts = None
    last_dir = None
    last_tick = None
    for r in rows:
        if r["ts"] is None or r["price"] is None or r["tick_id"] is None:
            return "missing ts/price/tick_id in pivot rows"

        if last_ts is not None and r["ts"] < last_ts:
            return "timestamps not in chronological order"
        last_ts = r["ts"]

        if last_tick is not None and int(r["tick_id"]) < last_tick:
            return "tick_id not in chronological order"
        last_tick = int(r["tick_id"])

        direction = r.get("direction")
        if last_dir is not None and direction == last_dir:
            return "directions do not alternate"
        last_dir = direction

    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--segm-id", type=int, default=119)
    args = ap.parse_args()

    conn = get_conn()
    try:
        n = recompute_zig_pivots_for_segment(conn, args.segm_id)
        print(f"[zig_smoke] segm_id={args.segm_id} pivots={n}")

        with dict_cur(conn) as cur:
            cur.execute(
                """
                SELECT id, segm_id, tick_id, ts, price, direction, pivot_index
                FROM public.zig_pivots
                WHERE segm_id=%s
                ORDER BY pivot_index ASC, ts ASC, id ASC
                """,
                (int(args.segm_id),),
            )
            rows = cur.fetchall()

        err = validate_pivots(rows)
        if err:
            raise SystemExit(f"[zig_smoke] validation failed: {err}")

        for r in rows:
            if r["ts"] is None:
                raise SystemExit("[zig_smoke] missing ts for overlay series")
            if r["tick_id"] is None:
                raise SystemExit("[zig_smoke] missing tick_id for overlay series")
            if r["price"] is None:
                raise SystemExit("[zig_smoke] missing price for overlay series")

        print("[zig_smoke] ok: ordering, alternation, overlay inputs")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
