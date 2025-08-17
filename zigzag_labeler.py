from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from ml_config import DATABASE_URL, ZZ_ABS_THRESHOLD

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def _mid(row):
    if row["mid"] is not None:
        return float(row["mid"])
    b, a = row["bid"], row["ask"]
    return None if b is None or a is None else (float(b) + float(a)) / 2.0

def _ticks_day(day):
    q = text("""
        SELECT id, timestamp, bid, ask, mid
        FROM ticks
        WHERE timestamp >= :s AND timestamp < :e
        ORDER BY id
    """)
    s = datetime.combine(day, datetime.min.time()).astimezone(timezone.utc)
    e = s + timedelta(days=1)
    with engine.begin() as c:
        for r in c.execute(q, {"s": s, "e": e}):
            d = dict(r._mapping)
            d["midv"] = _mid(d)
            if d["midv"] is not None:
                yield d

def _insert_zig(day, start_row, end_row, direction, threshold, hi, lo, count):
    sql = text("""
        INSERT INTO zigzags(day,start_tickid,end_tickid,start_time,end_time,
                            direction,start_price,end_price,high_price,low_price,
                            threshold,tick_count,duration_sec)
        VALUES(:day,:st,:et,:stt,:ett,:dir,:sp,:ep,:hp,:lp,:th,:tc,:dur)
        RETURNING id
    """)
    dur = int((end_row["timestamp"] - start_row["timestamp"]).total_seconds())
    with engine.begin() as c:
        return c.execute(sql, {
            "day": day,
            "st": start_row["id"], "et": end_row["id"],
            "stt": start_row["timestamp"], "ett": end_row["timestamp"],
            "dir": direction,
            "sp": start_row["midv"], "ep": end_row["midv"],
            "hp": hi, "lp": lo, "th": threshold, "tc": count, "dur": dur
        }).scalar_one()

def process_day(day, threshold=ZZ_ABS_THRESHOLD):
    """
    Candidate forms when |price - base| >= threshold.
    Candidate confirms when reversal from candidate extreme >= threshold.
    """
    rows = list(_ticks_day(day))
    if not rows:
        return []

    confirmed = []
    base = rows[0]
    base_p = base["midv"]
    hi = lo = base_p
    candidate = None
    count = 1

    for r in rows[1:]:
        p = r["midv"]
        count += 1
        if p > hi: hi = p
        if p < lo: lo = p

        if candidate is None:
            d = p - base_p
            if d >= threshold:
                candidate = {"dir": "up", "start": base, "ext": r, "extp": p, "count": count}
            elif d <= -threshold:
                candidate = {"dir": "dn", "start": base, "ext": r, "extp": p, "count": count}
            continue

        if candidate["dir"] == "up":
            if p > candidate["extp"]:
                candidate["extp"] = p
                candidate["ext"] = r
            if candidate["extp"] - p >= threshold:
                zid = _insert_zig(day, candidate["start"], candidate["ext"], "up", threshold, hi, lo, candidate["count"])
                confirmed.append(zid)
                base = candidate["ext"]; base_p = base["midv"]
                candidate = {"dir":"dn","start":base,"ext":r,"extp":p,"count":1}
                hi = max(base_p, p); lo = min(base_p, p); count = 1
        else:
            if p < candidate["extp"]:
                candidate["extp"] = p
                candidate["ext"] = r
            if p - candidate["extp"] >= threshold:
                zid = _insert_zig(day, candidate["start"], candidate["ext"], "dn", threshold, hi, lo, candidate["count"])
                confirmed.append(zid)
                base = candidate["ext"]; base_p = base["midv"]
                candidate = {"dir":"up","start":base,"ext":r,"extp":p,"count":1}
                hi = max(base_p, p); lo = min(base_p, p); count = 1

    return confirmed
