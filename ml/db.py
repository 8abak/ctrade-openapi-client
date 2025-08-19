# ml/db.py — PATCH: replace upsert_many with this version
import os, json, base64, time
from contextlib import contextmanager
from typing import Iterable, List, Dict, Any, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

def get_engine() -> Engine:
    return create_engine(_DATABASE_URL, pool_pre_ping=True, pool_recycle=1800)

@contextmanager
def db_conn(engine: Optional[Engine] = None):
    eng = engine or get_engine()
    with eng.begin() as conn:
        yield conn

def _serialize_jsonish(v):
    # Convert Python dict/list to JSON string so Postgres casts text -> jsonb
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return v

def upsert_many(table: str, rows: List[Dict[str, Any]], conflict_key: str = "tickid") -> int:
    if not rows:
        return 0
    # Normalize/serialize values
    cleaned = []
    cols = sorted(rows[0].keys())
    for r in rows:
        cleaned.append({c: _serialize_jsonish(r.get(c)) for c in cols})

    cols_sql = ", ".join(cols)
    params_sql = ", ".join([f":{c}" for c in cols])
    updates_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != conflict_key])
    sql = text(f"""
        INSERT INTO {table} ({cols_sql})
        VALUES ({params_sql})
        ON CONFLICT ({conflict_key}) DO UPDATE SET {updates_sql}
    """)
    with db_conn() as conn:
        conn.execute(sql, cleaned)
    return len(cleaned)
