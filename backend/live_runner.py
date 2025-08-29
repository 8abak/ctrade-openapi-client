# PATH: backend/live_runner.py
"""
Live runner service: keeps labeling up to "now" continuously.

Usage options:
1) Ad-hoc (screen/tmux):    python -m backend.live_runner
2) As a simple background process:  nohup python -m backend.live_runner >/var/log/live_runner.log 2>&1 &

Behavior:
- Reads stat.last_done_tick_id and the last segm.
- If new ticks arrive WITHOUT a >3m gap: re-compute and EXTEND the last segment (delete & rebuild last segm).
- If a >3m gap is observed: closes the previous segment and starts a new one.
- Sleeps a short interval between iterations (default 10s; override with env LIVE_RUNNER_INTERVAL).
- Safe, leak-proof: one DB transaction per segment build (implemented inside runner.Runner).

This module reuses backend/runner.py (same labeling strategy as /api/run).
"""
import os
import time
from backend.runner import Runner
from backend.db import get_conn, dict_cur

POLL_S = float(os.getenv("LIVE_RUNNER_INTERVAL", "10"))  # seconds

def _get_last_segment(conn):
    with dict_cur(conn) as cur:
        cur.execute("SELECT id, start_id, end_id FROM segm ORDER BY id DESC LIMIT 1;")
        return cur.fetchone()

def _get_ptr(conn) -> int:
    with dict_cur(conn) as cur:
        cur.execute("SELECT val FROM stat WHERE key='last_done_tick_id'")
        r = cur.fetchone()
        return int(r["val"] if r else 0)

def _set_ptr(conn, v: int):
    with dict_cur(conn) as cur:
        cur.execute("UPDATE stat SET val=%s WHERE key='last_done_tick_id'", (int(v),))

def _max_tick_id(conn) -> int:
    with dict_cur(conn) as cur:
        cur.execute("SELECT COALESCE(MAX(id),0) AS m FROM ticks")
        return int(cur.fetchone()["m"])

def main():
    r = Runner()
    conn = get_conn()
    print(f"[live_runner] started, polling every {POLL_S}s")

    while True:
        try:
            ptr = _get_ptr(conn)
            last_tick = _max_tick_id(conn)
            if last_tick <= ptr:
                time.sleep(POLL_S)
                continue

            last_seg = _get_last_segment(conn)

            # If we have an open segment whose end equals the pointer, extend it.
            if last_seg and int(last_seg["end_id"]) == ptr:
                nxt = r._next_segment_bounds(ptr + 1, last_tick)
                if not nxt:
                    # shouldn't happen; wait and retry
                    time.sleep(POLL_S)
                    continue
                _, new_end = nxt
                if new_end <= ptr:
                    time.sleep(POLL_S)
                    continue

                print(f"[live_runner] extending segm #{last_seg['id']} -> end_id={new_end}")
                # Delete and rebuild from its start to new_end
                with dict_cur(conn) as cur:
                    cur.execute("DELETE FROM segm WHERE id=%s", (int(last_seg["id"]),))
                r._process_segment(int(last_seg["start_id"]), int(new_end))
                _set_ptr(conn, int(new_end))
                continue

            # Otherwise, start a new segment at ptr+1
            nxt = r._next_segment_bounds(ptr + 1, last_tick)
            if not nxt:
                time.sleep(POLL_S)
                continue
            seg_start, seg_end = nxt
            print(f"[live_runner] new segment {seg_start}..{seg_end}")
            r._process_segment(seg_start, seg_end)
            _set_ptr(conn, seg_end)

        except Exception as e:
            print(f"[live_runner] error: {e}")
            # brief backoff to avoid tight loop on repeated errors
            time.sleep(max(POLL_S, 5.0))

        time.sleep(POLL_S)

if __name__ == "__main__":
    main()
