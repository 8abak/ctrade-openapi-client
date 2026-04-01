from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import psycopg2.extras

from datavis.db import db_connection
from datavis.envelope_storage import resolve_backfill_range
from datavis.envelope_zig import EnvelopeZigConfig
from datavis.ott_storage import DEFAULT_SYMBOL
from datavis.zigzag_storage import table_name


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.envelopezigpoint (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    sourceid BIGINT,
    symbol TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('micro', 'med', 'maxi', 'macro')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    basis DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    upper DOUBLE PRECISION,
    lower DOUBLE PRECISION,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopezigpoint_identity_uk UNIQUE (tickid, confirmtickid, symbol, level, length, bandwidth, mult)
);

CREATE TABLE IF NOT EXISTS public.envelopezigstate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('micro', 'med', 'maxi', 'macro')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    starttickid BIGINT,
    endtickid BIGINT,
    startts TIMESTAMPTZ,
    endts TIMESTAMPTZ,
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastconfirmtickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopezigstate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS envelopezigpoint_lookup_idx
    ON public.envelopezigpoint (symbol, level, length, bandwidth, mult, tickid DESC);

CREATE INDEX IF NOT EXISTS envelopezigpoint_confirm_idx
    ON public.envelopezigpoint (symbol, level, length, bandwidth, mult, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS envelopezigstate_lookup_idx
    ON public.envelopezigstate (jobtype, symbol, level, length, bandwidth, mult, updatedat DESC);
"""


def ensure_envelope_zig_schema() -> None:
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()


def fetch_level_source_rows_after(
    symbol: str,
    level: str,
    after_confirm_tick_id: int,
    limit: int,
    *,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_envelope_zig_schema()
    table = table_name(level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT
                        id AS sourceid,
                        symbol,
                        endtickid AS tickid,
                        confirmtickid,
                        endtime AS timestamp,
                        confirmtime,
                        endprice AS price
                    FROM public.{table}
                    WHERE symbol = %s
                      AND confirmtickid > %s
                    ORDER BY confirmtickid ASC, endtickid ASC, id ASC
                    LIMIT %s
                    """.format(table=table),
                    (symbol, after_confirm_tick_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        id AS sourceid,
                        symbol,
                        endtickid AS tickid,
                        confirmtickid,
                        endtime AS timestamp,
                        confirmtime,
                        endprice AS price
                    FROM public.{table}
                    WHERE symbol = %s
                      AND confirmtickid > %s
                      AND confirmtickid <= %s
                    ORDER BY confirmtickid ASC, endtickid ASC, id ASC
                    LIMIT %s
                    """.format(table=table),
                    (symbol, after_confirm_tick_id, end_id, limit),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_level_source_rows_upto_confirm(
    symbol: str,
    level: str,
    confirm_tick_id: int,
    limit: int,
) -> List[Dict[str, Any]]:
    ensure_envelope_zig_schema()
    table = table_name(level)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM (
                    SELECT
                        id AS sourceid,
                        symbol,
                        endtickid AS tickid,
                        confirmtickid,
                        endtime AS timestamp,
                        confirmtime,
                        endprice AS price
                    FROM public.{table}
                    WHERE symbol = %s
                      AND confirmtickid <= %s
                    ORDER BY confirmtickid DESC, endtickid DESC, id DESC
                    LIMIT %s
                ) seeded
                ORDER BY confirmtickid ASC, tickid ASC, sourceid ASC
                """.format(table=table),
                (symbol, confirm_tick_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def fetch_envelope_zig_rows_for_window(
    symbol: str,
    config: EnvelopeZigConfig,
    *,
    start_id: int,
    end_id: int,
) -> List[Dict[str, Any]]:
    ensure_envelope_zig_schema()
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    tickid, confirmtickid, sourceid, timestamp, confirmtime,
                    price, basis, mae, upper, lower
                FROM public.envelopezigpoint
                WHERE symbol = %s
                  AND level = %s
                  AND length = %s
                  AND bandwidth = %s
                  AND mult = %s
                  AND tickid BETWEEN %s AND %s
                ORDER BY confirmtickid ASC, tickid ASC
                """,
                (symbol, config.level, config.length, config.bandwidth, config.mult, start_id, end_id),
            )
            return [dict(row) for row in cur.fetchall()]


def fetch_envelope_zig_rows_after_confirm(
    symbol: str,
    config: EnvelopeZigConfig,
    *,
    after_confirm_tick_id: int,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_envelope_zig_schema()
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT
                        tickid, confirmtickid, sourceid, timestamp, confirmtime,
                        price, basis, mae, upper, lower
                    FROM public.envelopezigpoint
                    WHERE symbol = %s
                      AND level = %s
                      AND length = %s
                      AND bandwidth = %s
                      AND mult = %s
                      AND confirmtickid > %s
                    ORDER BY confirmtickid ASC, tickid ASC
                    """,
                    (symbol, config.level, config.length, config.bandwidth, config.mult, after_confirm_tick_id),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        tickid, confirmtickid, sourceid, timestamp, confirmtime,
                        price, basis, mae, upper, lower
                    FROM public.envelopezigpoint
                    WHERE symbol = %s
                      AND level = %s
                      AND length = %s
                      AND bandwidth = %s
                      AND mult = %s
                      AND confirmtickid > %s
                      AND confirmtickid <= %s
                    ORDER BY confirmtickid ASC, tickid ASC
                    """,
                    (
                        symbol,
                        config.level,
                        config.length,
                        config.bandwidth,
                        config.mult,
                        after_confirm_tick_id,
                        end_id,
                    ),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_envelope_zig_storage_bounds(symbol: str, config: EnvelopeZigConfig) -> Dict[str, Any]:
    ensure_envelope_zig_schema()
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(tickid) AS firsttickid,
                    MAX(tickid) AS lasttickid,
                    MAX(confirmtickid) AS lastconfirmtickid,
                    COUNT(*)::bigint AS rowcount,
                    COUNT(*) FILTER (WHERE basis IS NOT NULL)::bigint AS basisrowcount,
                    COUNT(*) FILTER (WHERE upper IS NOT NULL AND lower IS NOT NULL)::bigint AS bandrowcount,
                    MIN(timestamp) AS firstts,
                    MAX(timestamp) AS lastts
                FROM public.envelopezigpoint
                WHERE symbol = %s
                  AND level = %s
                  AND length = %s
                  AND bandwidth = %s
                  AND mult = %s
                """,
                (symbol, config.level, config.length, config.bandwidth, config.mult),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firsttickid": int(row["firsttickid"]) if row.get("firsttickid") is not None else None,
        "lasttickid": int(row["lasttickid"]) if row.get("lasttickid") is not None else None,
        "lastconfirmtickid": int(row["lastconfirmtickid"]) if row.get("lastconfirmtickid") is not None else None,
        "rowcount": int(row.get("rowcount") or 0),
        "basisrowcount": int(row.get("basisrowcount") or 0),
        "bandrowcount": int(row.get("bandrowcount") or 0),
        "firstts": row.get("firstts"),
        "lastts": row.get("lastts"),
    }


def load_envelope_zig_state(job_name: str) -> Optional[Dict[str, Any]]:
    ensure_envelope_zig_schema()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id, jobname, jobtype, symbol, level, length, bandwidth, mult,
                    starttickid, endtickid, startts, endts, lasttickid,
                    lastconfirmtickid, lastts, statejson
                FROM public.envelopezigstate
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


def persist_envelope_zig_progress(
    *,
    job_name: str,
    job_type: str,
    symbol: str,
    config: EnvelopeZigConfig,
    start_tick_id: Optional[int],
    end_tick_id: Optional[int],
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    last_tick_id: int,
    last_confirm_tick_id: int,
    last_ts: Optional[datetime],
    statejson: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
) -> int:
    ensure_envelope_zig_schema()
    config = config.normalized()
    values = [
        (
            row["tickid"],
            row["confirmtickid"],
            row.get("sourceid"),
            row["symbol"],
            config.level,
            config.length,
            config.bandwidth,
            config.mult,
            row["timestamp"],
            row["confirmtime"],
            row["price"],
            row["basis"],
            row["mae"],
            row["upper"],
            row["lower"],
        )
        for row in rows
    ]

    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            if values:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO public.envelopezigpoint (
                        tickid, confirmtickid, sourceid, symbol, level, length, bandwidth, mult,
                        timestamp, confirmtime, price, basis, mae, upper, lower
                    )
                    VALUES %s
                    ON CONFLICT (tickid, confirmtickid, symbol, level, length, bandwidth, mult)
                    DO UPDATE SET
                        sourceid = EXCLUDED.sourceid,
                        timestamp = EXCLUDED.timestamp,
                        confirmtime = EXCLUDED.confirmtime,
                        price = EXCLUDED.price,
                        basis = EXCLUDED.basis,
                        mae = EXCLUDED.mae,
                        upper = EXCLUDED.upper,
                        lower = EXCLUDED.lower,
                        updatedat = NOW()
                    """,
                    values,
                    page_size=min(1000, len(values)),
                )

            cur.execute(
                """
                INSERT INTO public.envelopezigstate (
                    jobname, jobtype, symbol, level, length, bandwidth, mult,
                    starttickid, endtickid, startts, endts, lasttickid,
                    lastconfirmtickid, lastts, statejson
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (jobname)
                DO UPDATE SET
                    jobtype = EXCLUDED.jobtype,
                    symbol = EXCLUDED.symbol,
                    level = EXCLUDED.level,
                    length = EXCLUDED.length,
                    bandwidth = EXCLUDED.bandwidth,
                    mult = EXCLUDED.mult,
                    starttickid = EXCLUDED.starttickid,
                    endtickid = EXCLUDED.endtickid,
                    startts = EXCLUDED.startts,
                    endts = EXCLUDED.endts,
                    lasttickid = EXCLUDED.lasttickid,
                    lastconfirmtickid = EXCLUDED.lastconfirmtickid,
                    lastts = EXCLUDED.lastts,
                    statejson = EXCLUDED.statejson,
                    updatedat = NOW()
                """,
                (
                    job_name,
                    job_type,
                    symbol,
                    config.level,
                    config.length,
                    config.bandwidth,
                    config.mult,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts,
                    last_tick_id,
                    last_confirm_tick_id,
                    last_ts,
                    json.dumps(statejson),
                ),
            )
        conn.commit()
    return len(values)


def fetch_envelope_zig_sync_diagnostics(
    symbol: str,
    config: EnvelopeZigConfig,
    *,
    requested_end_tick_id: Optional[int] = None,
) -> Dict[str, Any]:
    config = config.normalized()
    storage = fetch_envelope_zig_storage_bounds(symbol, config)
    job_state = load_envelope_zig_state(config.worker_job_name(symbol))
    latest_stored_tick_id = storage.get("lasttickid")
    gap_count = 0
    if latest_stored_tick_id is not None and requested_end_tick_id is not None:
        gap_count = max(0, int(requested_end_tick_id) - int(latest_stored_tick_id))
    return {
        "storage": {
            "firstTickId": storage.get("firsttickid"),
            "lastTickId": storage.get("lasttickid"),
            "lastConfirmTickId": storage.get("lastconfirmtickid"),
            "rowCount": storage.get("rowcount"),
            "basisRowCount": storage.get("basisrowcount"),
            "bandRowCount": storage.get("bandrowcount"),
            "firstTs": storage.get("firstts"),
            "lastTs": storage.get("lastts"),
        },
        "jobState": {
            "lastTickId": int(job_state["lasttickid"]) if job_state and job_state.get("lasttickid") is not None else None,
            "lastConfirmTickId": int(job_state["lastconfirmtickid"]) if job_state and job_state.get("lastconfirmtickid") is not None else None,
            "lastTs": job_state.get("lastts") if job_state else None,
            "jobName": job_state.get("jobname") if job_state else None,
        },
        "requested": {
            "lastTickId": requested_end_tick_id,
            "gapCountAheadOfStorage": gap_count,
        },
        "latestStoredTickId": latest_stored_tick_id,
    }
