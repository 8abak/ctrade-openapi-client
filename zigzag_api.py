#!/usr/bin/env python3
import psycopg2, json, math
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
DB = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)

LEVEL_TABLE = {
    "micro": "micro_trends",
    "medium":"medium_trends",
    "maxi":  "maxi_trends",
}

def q(conn, sql, params=()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

@app.get("/api/zigzag")
def zigzag():
    """
    Params:
      mode = 'date' | 'id'            (default 'date')
      levels = comma list             (default 'micro,medium,maxi')
      # mode=date
      day = YYYY-MM-DD                (use run_day)
      # mode=id
      start_id = bigint               (from ticks.id)
      span_minutes = int              (default 60) -> returns segments with start_ts within [start_ts, start_ts+span]
      # paging:
      cursor_ts = ISO8601             (exclusive lower bound: fetch strictly after this ts)
      limit = int                     (hard cap per level, default 2000)
    """
    mode   = (request.args.get("mode") or "date").lower()
    levels = (request.args.get("levels") or "micro,medium,maxi").split(",")
    levels = [l for l in levels if l in LEVEL_TABLE]
    limit  = int(request.args.get("limit") or 2000)

    with psycopg2.connect(**DB) as conn:
        if mode == "date":
            day = request.args.get("day")
            if not day: return jsonify({"error":"missing day"}), 400
            # optional paging by timestamp:
            cursor_ts = request.args.get("cursor_ts")
            where_more = "AND start_ts > %s" if cursor_ts else ""
            params_more = [day] + ([cursor_ts] if cursor_ts else [])
            segs = {}
            pts  = {}
            min_ts, max_ts = None, None
            for lvl in levels:
                tbl = LEVEL_TABLE[lvl]
                segs[lvl] = q(conn, f"""
                  SELECT start_ts, end_ts, start_price, end_price
                  FROM {tbl}
                  WHERE run_day=%s {where_more}
                  ORDER BY start_ts
                  LIMIT %s
                """, params_more + [limit])
                pts[lvl] = q(conn, """
                  SELECT ts, price, kind
                  FROM zigzag_points
                  WHERE level=%s AND run_day=%s
                  ORDER BY ts
                """, (lvl, day))
                if segs[lvl]:
                    lmin = segs[lvl][0]["start_ts"]; lmax = segs[lvl][-1]["end_ts"]
                    min_ts = lmin if not min_ts or lmin < min_ts else min_ts
                    max_ts = lmax if not max_ts or lmax > max_ts else max_ts
            return jsonify({"segments":segs, "points":pts, "meta":{"cursor_ts": max_ts}})

        elif mode == "id":
            start_id = request.args.get("start_id", type=int)
            if not start_id: return jsonify({"error":"missing start_id"}), 400
            span_minutes = int(request.args.get("span_minutes") or 60)
            # find start_ts from ticks.id
            with conn.cursor() as cur:
                cur.execute("SELECT timestamp FROM ticks WHERE id=%s", (start_id,))
                row = cur.fetchone()
                if not row: return jsonify({"error":"start_id not found"}), 404
                start_ts = row[0]
            end_ts = start_ts + timedelta(minutes=span_minutes)
            cursor_ts = request.args.get("cursor_ts")
            where_more = "AND start_ts > %s" if cursor_ts else ""
            params_more = [start_ts, end_ts] + ([cursor_ts] if cursor_ts else [])
            segs = {}
            pts  = {}
            for lvl in levels:
                tbl = LEVEL_TABLE[lvl]
                segs[lvl] = q(conn, f"""
                  SELECT start_ts, end_ts, start_price, end_price
                  FROM {tbl}
                  WHERE start_ts >= %s AND start_ts < %s {where_more}
                  ORDER BY start_ts
                  LIMIT %s
                """, params_more + [limit])
                # points in same window (not paged; small)
                pts[lvl] = q(conn, """
                  SELECT ts, price, kind
                  FROM zigzag_points
                  WHERE level=%s AND ts >= %s AND ts < %s
                  ORDER BY ts
                """, (lvl, start_ts, end_ts))
            max_ts = None
            for lvl in levels:
                if segs[lvl]:
                    t = segs[lvl][-1]["end_ts"]
                    max_ts = t if not max_ts or t > max_ts else max_ts
            return jsonify({"segments":segs, "points":pts, "meta":{"cursor_ts": max_ts}})
        else:
            return jsonify({"error":"bad mode"}), 400
