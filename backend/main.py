#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
import datetime as dt
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# ====== CONFIG ======
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or \
               "postgresql://postgres:postgres@localhost:5432/postgres"

# Jobs we call without modifying your existing code
JOB_RUN_DAY = ["python", "-m", "make_dataset"]         # expects: --date YYYY-MM-DD
JOB_WALK_FWD = ["python", "-m", "jobs.backfill"]       # expects: --start N --end M

# Fallback table names used by the chart / pipeline
TABLE_TICKS = os.getenv("TABLE_TICKS", "ticks")
TABLE_MACRO = os.getenv("TABLE_MACRO", "macro_trends")
TABLE_MICRO = os.getenv("TABLE_MICRO", "micro_trends")
TABLE_PRED  = os.getenv("TABLE_PRED",  "predictions")

# ====== APP ======
app = Flask(__name__)
CORS(app)

# ====== DB UTILS ======
def _db() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def _one(sql: str, args: Tuple = ()) -> Optional[Tuple]:
    with _db() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()

def _all(sql: str, args: Tuple = ()) -> List[Tuple]:
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, args)
        return [dict(r) for r in cur.fetchall()]

def _exists_table(schema: str, table: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema=%s AND table_name=%s
    """
    return _one(q, (schema, table)) is not None

def _has_column(schema: str, table: str, column: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s AND column_name=%s
    """
    return _one(q, (schema, table, column)) is not None

def _max_tick_id_from_table(schema: str, table: str, col: str) -> Optional[int]:
    if not _exists_table(schema, table) or not _has_column(schema, table, col):
        return None
    q = f"SELECT MAX({col}) FROM {schema}.{table}"
    row = _one(q)
    return row[0] if row and row[0] is not None else None

def _min_tick_id() -> Optional[int]:
    row = _one(f"SELECT MIN(id) FROM {TABLE_TICKS}")
    return row[0] if row and row[0] is not None else None

def _max_tick_id() -> Optional[int]:
    row = _one(f"SELECT MAX(id) FROM {TABLE_TICKS}")
    return row[0] if row and row[0] is not None else None

# ====== SAFE SQL (READ-ONLY) ======
_SQL_ALLOWED_START = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE | re.DOTALL)
_SQL_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|ALTER|DROP|TRUNCATE|CREATE|GRANT|REVOKE|VACUUM|ANALYZE)\b",
    re.IGNORECASE,
)

def _validate_readonly_sql(sql: str) -> Optional[str]:
    if ";" in sql:
        return "Multiple statements are not allowed."
    if not _SQL_ALLOWED_START.search(sql):
        return "Only SELECT/WITH queries are allowed."
    if _SQL_FORBIDDEN.search(sql):
        return "DDL/DML statements are not allowed."
    return None

# ====== JOB RUNNERS ======
def _run_single_day(date_str: str) -> Tuple[int, str]:
    # Ex: make_dataset --date 2025-06-17
    cmd = JOB_RUN_DAY + ["--date", date_str]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return proc.returncode, proc.stdout

def _run_walk_forward(start_id: int, end_id: int) -> Tuple[int, str]:
    # Ex: jobs.backfill --start 1 --end 200000
    cmd = JOB_WALK_FWD + ["--start", str(start_id), "--end", str(end_id)]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return proc.returncode, proc.stdout

# ====== ROUTES ======
@app.get("/api/health")
def health():
    try:
        _ = _one("SELECT 1")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/api/sql")
def sql_console():
    payload = request.get_json(silent=True) or {}
    sql = payload.get("sql", "")
    limit = int(payload.get("limit", 1000))
    err = _validate_readonly_sql(sql)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    sql_limited = f"WITH q AS ({sql}) SELECT * FROM q LIMIT %s"
    try:
        rows = _all(sql_limited, (limit,))
        return jsonify({"ok": True, "rows": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

@app.post("/api/run-day")
def run_day():
    payload = request.get_json(silent=True) or {}
    date_str = payload.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "Missing 'date' (YYYY-MM-DD)"}), 400

    try:
        # Validate date
        _ = dt.date.fromisoformat(date_str)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid date format. Use YYYY-MM-DD"}), 400

    code, out = _run_single_day(date_str)
    return jsonify({"ok": code == 0, "code": code, "log": out})

@app.post("/api/run")
def run_until_now():
    """
    Walk forward from the last analyzed macro segment tick to the latest tick id.
    If no macro table yet, start from MIN(ticks.id).
    """
    schema = request.args.get("schema", "public")

    start_from = _max_tick_id_from_table(schema, TABLE_MACRO, "end_tickid")
    if start_from is None:
        start_from = _min_tick_id()
    latest = _max_tick_id()

    if start_from is None or latest is None:
        return jsonify({"ok": False, "error": "ticks table is empty"}), 400
    if start_from >= latest:
        return jsonify({"ok": True, "message": "Already up-to-date", "start": start_from, "end": latest})

    # Make inclusive ranges friendly to your job
    start_id = int(start_from)
    end_id = int(latest)

    code, out = _run_walk_forward(start_id, end_id)
    return jsonify({"ok": code == 0, "code": code, "start": start_id, "end": end_id, "log": out})

@app.get("/api/label-tables")
def label_tables():
    """
    Return tables in 'public' schema that contain a 'tickid' column.
    The frontend can use this list to render toggleable layers.
    """
    schema = request.args.get("schema", "public")
    q = """
    SELECT table_name
    FROM information_schema.columns
    WHERE table_schema=%s AND column_name='tickid'
    ORDER BY table_name
    """
    tables = [r["table_name"] for r in _all(q, (schema,))]
    return jsonify({"ok": True, "tables": tables})

@app.get("/api/labels/<table>")
def labels_for_table(table: str):
    """
    Fetch labels windowed by tickid range. Expects the table to have a 'tickid' column.
    Query args:
      from (inclusive), to (inclusive), limit (default 5000)
    """
    schema = request.args.get("schema", "public")
    if not _has_column(schema, table, "tickid"):
        return jsonify({"ok": False, "error": f"{schema}.{table} has no column 'tickid'"}), 400

    t_from = request.args.get("from", type=int)
    t_to   = request.args.get("to", type=int)
    limit  = request.args.get("limit", default=5000, type=int)

    where, args = ["1=1"], []
    if t_from is not None:
        where.append("tickid >= %s"); args.append(t_from)
    if t_to is not None:
        where.append("tickid <= %s"); args.append(t_to)

    sql = f"SELECT * FROM {schema}.{table} WHERE {' AND '.join(where)} ORDER BY tickid LIMIT %s"
    args.append(limit)
    rows = _all(sql, tuple(args))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})

@app.get("/api/macro-segments")
def macro_segments():
    schema = request.args.get("schema", "public")
    limit = request.args.get("limit", type=int, default=1000)
    if not _exists_table(schema, TABLE_MACRO):
        return jsonify({"ok": True, "rows": [], "count": 0})
    sql = f"SELECT * FROM {schema}.{TABLE_MACRO} ORDER BY start_time DESC LIMIT %s"
    rows = _all(sql, (limit,))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})

@app.get("/api/micro-events")
def micro_events():
    schema = request.args.get("schema", "public")
    seg_id = request.args.get("macro_id", type=int)
    limit = request.args.get("limit", type=int, default=5000)

    if not _exists_table(schema, TABLE_MICRO):
        return jsonify({"ok": True, "rows": [], "count": 0})

    where, args = [], []
    if seg_id is not None and _has_column(schema, TABLE_MICRO, "macro_id"):
        where.append("macro_id=%s"); args.append(seg_id)

    sql = f"SELECT * FROM {schema}.{TABLE_MICRO}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY start_tickid LIMIT %s"
    args.append(limit)
    rows = _all(sql, tuple(args))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})

@app.get("/api/predictions")
def predictions():
    schema = request.args.get("schema", "public")
    limit = request.args.get("limit", type=int, default=5000)
    if not _exists_table(schema, TABLE_PRED):
        return jsonify({"ok": True, "rows": [], "count": 0})
    sql = f"SELECT * FROM {schema}.{TABLE_PRED} ORDER BY tickid DESC LIMIT %s"
    rows = _all(sql, (limit,))
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})

# ====== MAIN ======
if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8501"))
    debug = os.getenv("DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)
