from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import create_engine, text
from datetime import date
from ml_config import DATABASE_URL

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
router = APIRouter(tags=["lview"])

@router.get("/zigs")
def zigs(day: str = Query(..., description="YYYY-MM-DD (UTC)")):
    d = date.fromisoformat(day)
    q = text("""
        SELECT id, start_time, end_time, start_price, end_price, direction
        FROM zigzags WHERE day=:d ORDER BY start_time
    """)
    with engine.begin() as c:
        return [dict(r._mapping) for r in c.execute(q, {"d": d})]

@router.get("/active")
def active(day: str = Query(...)):
    d = date.fromisoformat(day)
    last_q = text("SELECT end_tickid FROM zigzags WHERE day=:d ORDER BY end_time DESC LIMIT 1")
    with engine.begin() as c:
        last = c.execute(last_q, {"d": d}).first()
        if last:
            q = text("""
                SELECT id AS tickid, timestamp AS ts, COALESCE(mid,(bid+ask)/2.0) AS mid
                FROM ticks WHERE id > :sid AND timestamp::date=:d ORDER BY id
            """)
            ticks = [dict(r._mapping) for r in c.execute(q, {"sid": last[0], "d": d})]
        else:
            q = text("""
                SELECT id AS tickid, timestamp AS ts, COALESCE(mid,(bid+ask)/2.0) AS mid
                FROM ticks WHERE timestamp::date=:d ORDER BY id
            """)
            ticks = [dict(r._mapping) for r in c.execute(q, {"d": d})]
    return {"ticks": ticks}

@router.get("/no_return")
def no_return(zig_id: int):
    q = text("""
        SELECT n.tickid, n.ts,
               (SELECT tickid FROM tick_features
                WHERE zig_id=:z AND pred_is_earliest IS TRUE
                ORDER BY tickid LIMIT 1) AS pred_tickid
        FROM no_return_points n WHERE n.zig_id=:z
    """)
    with engine.begin() as c:
        row = c.execute(q, {"z": zig_id}).first()
        if not row:
            raise HTTPException(404, "Not found")
        return dict(row._mapping)
