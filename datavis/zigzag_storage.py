from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence

import psycopg2.extras

from datavis.db import db_connection
from datavis.zigzag import ZIG_CHILD, ZIG_LEVELS, ZIG_TABLES


DEFAULT_SYMBOL = "XAUUSD"
SEGMENT_COLUMNS = [
    "id",
    "symbol",
    "starttickid",
    "endtickid",
    "confirmtickid",
    "starttime",
    "endtime",
    "confirmtime",
    "startprice",
    "endprice",
    "highprice",
    "lowprice",
    "dir",
    "tickcount",
    "childcount",
    "dursec",
    "amplitude",
    "score",
    "status",
    "childstartid",
    "childendid",
    "parentid",
    "createdat",
    "updatedat",
]


def table_name(level: str) -> str:
    if level not in ZIG_TABLES:
        raise ValueError("Unsupported zig level: {0}".format(level))
    return ZIG_TABLES[level]


def save_zig_rows(level: str, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    table = table_name(level)
    values = [
        (
            row["symbol"],
            row["starttickid"],
            row["endtickid"],
            row["confirmtickid"],
            row["starttime"],
            row["endtime"],
            row["confirmtime"],
            row["startprice"],
            row["endprice"],
            row["highprice"],
            row["lowprice"],
            row["dir"],
            row["tickcount"],
            row.get("childcount", 0),
            row["dursec"],
            row["amplitude"],
            row["score"],
            row.get("status", "confirmed"),
            row.get("childstartid"),
            row.get("childendid"),
            row.get("parentid"),
        )
        for row in rows
    ]
    returning_sql = ", ".join(SEGMENT_COLUMNS)
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO public.{table} (
                    symbol, starttickid, endtickid, confirmtickid, starttime, endtime, confirmtime,
                    startprice, endprice, highprice, lowprice, dir, tickcount, childcount,
                    dursec, amplitude, score, status, childstartid, childendid, parentid
                )
                VALUES %s
                ON CONFLICT (symbol, starttickid, endtickid, confirmtickid)
                DO UPDATE SET
                    starttime = EXCLUDED.starttime,
                    endtime = EXCLUDED.endtime,
                    confirmtime = EXCLUDED.confirmtime,
                    startprice = EXCLUDED.startprice,
                    endprice = EXCLUDED.endprice,
                    highprice = EXCLUDED.highprice,
                    lowprice = EXCLUDED.lowprice,
                    dir = EXCLUDED.dir,
                    tickcount = EXCLUDED.tickcount,
                    childcount = EXCLUDED.childcount,
                    dursec = EXCLUDED.dursec,
                    amplitude = EXCLUDED.amplitude,
                    score = EXCLUDED.score,
                    status = EXCLUDED.status,
                    childstartid = EXCLUDED.childstartid,
                    childendid = EXCLUDED.childendid,
                    parentid = COALESCE(public.{table}.parentid, EXCLUDED.parentid),
                    updatedat = NOW()
                RETURNING {returning_sql}
                """.format(table=table, returning_sql=returning_sql),
                values,
                page_size=max(1, len(values)),
            )
            persisted = [dict(zip(SEGMENT_COLUMNS, result)) for result in cur.fetchall()]
        conn.commit()
    return persisted


def assign_parent_links(parent_level: str, parent_rows: Sequence[Dict[str, Any]]) -> int:
    child_level = ZIG_CHILD.get(parent_level)
    if not child_level or not parent_rows:
        return 0
    child_table = table_name(child_level)
    updated = 0
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            for row in parent_rows:
                cur.execute(
                    """
                    UPDATE public.{child_table}
                    SET parentid = %s,
                        updatedat = NOW()
                    WHERE symbol = %s
                      AND endtickid > %s
                      AND endtickid <= %s
                      AND (parentid IS NULL OR parentid <> %s)
                    """.format(child_table=child_table),
                    (
                        int(row["id"]),
                        row["symbol"],
                        int(row["starttickid"]),
                        int(row["endtickid"]),
                        int(row["id"]),
                    ),
                )
                updated += int(cur.rowcount or 0)
        conn.commit()
    return updated


def persist_level_rows(level: str, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    persisted = save_zig_rows(level, rows)
    assign_parent_links(level, persisted)
    return persisted


def fetch_level_rows_for_window(symbol: str, level: str, start_id: int, end_id: int, confirmed_through_id: int) -> List[Dict[str, Any]]:
    table = table_name(level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT {columns}
                FROM public.{table}
                WHERE symbol = %s
                  AND starttickid <= %s
                  AND endtickid >= %s
                  AND confirmtickid <= %s
                ORDER BY endtickid ASC, confirmtickid ASC, id ASC
                """.format(columns=", ".join(SEGMENT_COLUMNS), table=table),
                (symbol, end_id, start_id, confirmed_through_id),
            )
            return [dict(row) for row in cur.fetchall()]


def fetch_level_rows_after_confirm(symbol: str, level: str, after_id: int, end_id: Optional[int] = None) -> List[Dict[str, Any]]:
    table = table_name(level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT {columns}
                    FROM public.{table}
                    WHERE symbol = %s
                      AND confirmtickid > %s
                    ORDER BY confirmtickid ASC, id ASC
                    """.format(columns=", ".join(SEGMENT_COLUMNS), table=table),
                    (symbol, after_id),
                )
            else:
                cur.execute(
                    """
                    SELECT {columns}
                    FROM public.{table}
                    WHERE symbol = %s
                      AND confirmtickid > %s
                      AND confirmtickid <= %s
                    ORDER BY confirmtickid ASC, id ASC
                    """.format(columns=", ".join(SEGMENT_COLUMNS), table=table),
                    (symbol, after_id, end_id),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_level_storage_bounds(symbol: str, level: str) -> Dict[str, Any]:
    table = table_name(level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(starttickid) AS firsttickid,
                    MAX(endtickid) AS lasttickid,
                    MAX(confirmtickid) AS lastconfirmtickid,
                    COUNT(*)::bigint AS rowcount,
                    MIN(starttime) AS firsttime,
                    MAX(endtime) AS lasttime
                FROM public.{table}
                WHERE symbol = %s
                """.format(table=table),
                (symbol,),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firstTickId": int(row["firsttickid"]) if row.get("firsttickid") is not None else None,
        "lastTickId": int(row["lasttickid"]) if row.get("lasttickid") is not None else None,
        "lastConfirmTickId": int(row["lastconfirmtickid"]) if row.get("lastconfirmtickid") is not None else None,
        "rowCount": int(row.get("rowcount") or 0),
        "firstTime": row.get("firsttime"),
        "lastTime": row.get("lasttime"),
    }


def load_zig_state(job_name: str) -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, jobname, jobtype, symbol, lasttickid, lasttime, statejson, createdat, updatedat
                FROM public.zigstate
                WHERE jobname = %s
                """,
                (job_name,),
            )
            row = cur.fetchone()
    if not row:
        return None
    payload = dict(row)
    state_json = payload.get("statejson")
    if isinstance(state_json, str):
        payload["statejson"] = json.loads(state_json)
    return payload


def save_zig_state(
    *,
    job_name: str,
    job_type: str,
    symbol: str,
    last_tick_id: int,
    last_time: Any,
    statejson: Dict[str, Any],
) -> None:
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.zigstate (
                    jobname, jobtype, symbol, lasttickid, lasttime, statejson
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (jobname)
                DO UPDATE SET
                    jobtype = EXCLUDED.jobtype,
                    symbol = EXCLUDED.symbol,
                    lasttickid = EXCLUDED.lasttickid,
                    lasttime = EXCLUDED.lasttime,
                    statejson = EXCLUDED.statejson,
                    updatedat = NOW()
                """,
                (job_name, job_type, symbol, int(last_tick_id), last_time, json.dumps(statejson)),
            )
        conn.commit()


def fetch_zig_sync_diagnostics(symbol: str, job_name: str) -> Dict[str, Any]:
    state_row = load_zig_state(job_name)
    levels = {level: fetch_level_storage_bounds(symbol, level) for level in ZIG_LEVELS}
    return {
        "jobState": {
            "jobName": job_name,
            "lastTickId": int(state_row["lasttickid"]) if state_row and state_row.get("lasttickid") is not None else None,
            "lastTime": state_row.get("lasttime") if state_row else None,
        },
        "levels": levels,
    }
