#!/usr/bin/env python3
import psycopg2, json
from flask import Flask, request, jsonify
from datetime import datetime
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
DB = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

@app.get("/api/zigzag")
def zigzag():
    level = request.args.get("level","medium")
    day   = request.args.get("day")  # YYYY-MM-DD
    table = {"micro":"micro_trends","medium":"medium_trends","maxi":"maxi_trends"}.get(level, "medium_trends")
    with psycopg2.connect(**DB) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
          SELECT start_ts, end_ts, start_price, end_price
          FROM {table}
          WHERE run_day = %s
          ORDER BY start_ts
        """, (day,))
        segs = cur.fetchall()
        cur.execute("""
          SELECT ts, price, kind FROM zigzag_points
          WHERE level=%s AND run_day=%s
          ORDER BY ts
        """, (level, day))
        pts = cur.fetchall()
    return jsonify({"segments":segs, "points":pts})
