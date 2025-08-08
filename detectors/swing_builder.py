# detectors/swing_builder.py
import os
from sqlalchemy import create_engine, text

ENGINE = create_engine(os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading"))

def _dir(a, b):  # +1 up if b > a else -1
    return +1 if b > a else -1

def build_swings(scale: int, min_magnitude: float = 0.0) -> int:
    """
    Reads peaks for a given scale, stitches alternating highs/lows into approved swings.
    Idempotent: (scale, start_tickid, end_tickid) is unique.
    """
    built = 0
    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
          SELECT id, ts, tickid, price, kind
          FROM peaks
          WHERE scale=:scale
          ORDER BY ts
        """), {"scale": scale}).mappings().all()
        if not rows:
            return 0

        pending = None  # (ts, tickid, price, kind)
        for r in rows:
            ts, tid, pr, kd = r["ts"], r["tickid"], r["price"], r["kind"]
            if pending is None:
                pending = (ts, tid, pr, kd)
                continue

            # same kind → upgrade to more extreme
            if kd == pending[3]:
                if (kd == +1 and pr > pending[2]) or (kd == -1 and pr < pending[2]):
                    pending = (ts, tid, pr, kd)
                continue

            # opposite kind → approve pending->current swing
            s_ts, s_tid, s_p, s_k = pending
            e_ts, e_tid, e_p, e_k = ts, tid, pr, kd
            mag = abs(e_p - s_p)
            dur = int((e_ts - s_ts).total_seconds())
            if dur <= 0:
                pending = (ts, tid, pr, kd)
                continue

            if mag >= min_magnitude:
                direction = _dir(s_p, e_p)
                vel = mag / max(dur, 1)

                conn.execute(text("""
                  INSERT INTO swings (scale, direction, start_ts, end_ts,
                    start_tickid, end_tickid, start_price, end_price,
                    magnitude, duration_sec, velocity, status)
                  VALUES (:scale,:direction,:s_ts,:e_ts,:s_tid,:e_tid,
                          :s_p,:e_p,:mag,:dur,:vel,1)
                  ON CONFLICT DO NOTHING
                """), {"scale": scale, "direction": direction,
                       "s_ts": s_ts, "e_ts": e_ts, "s_tid": s_tid, "e_tid": e_tid,
                       "s_p": s_p, "e_p": e_p, "mag": mag, "dur": dur, "vel": vel})
                built += 1

            pending = (ts, tid, pr, kd)  # shift window
    return built
