from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import psycopg2.extras

from datavis.db import db_connection
from datavis.envelope import EnvelopeConfig
from datavis.ott_storage import (
    DEFAULT_SYMBOL,
    fetch_bootstrap_tick_rows,
    fetch_next_tick_rows,
    fetch_tick_batch_after,
    fetch_tick_id_bounds,
)


def fetch_tick_row(symbol: str, tick_id: int) -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND id = %s
                LIMIT 1
                """,
                (symbol, tick_id),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_tick_at_or_after(symbol: str, timestamp_value: datetime) -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp >= %s
                ORDER BY timestamp ASC, id ASC
                LIMIT 1
                """,
                (symbol, timestamp_value),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def fetch_tick_at_or_before(symbol: str, timestamp_value: datetime) -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp <= %s
                ORDER BY timestamp DESC, id DESC
                LIMIT 1
                """,
                (symbol, timestamp_value),
            )
            row = cur.fetchone()
    return dict(row) if row else None


def resolve_backfill_range(
    symbol: str,
    *,
    start_id: Optional[int] = None,
    end_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    if start_id is not None and start_time is not None:
        raise ValueError("Choose either --start-id or --start-time, not both.")
    if end_id is not None and end_time is not None:
        raise ValueError("Choose either --end-id or --end-time, not both.")

    bounds = fetch_tick_id_bounds(symbol)
    if bounds.get("firstid") is None or bounds.get("lastid") is None:
        raise RuntimeError("No ticks are available for the requested symbol.")

    if start_id is not None:
        start_row = fetch_tick_row(symbol, int(start_id))
        if not start_row:
            raise RuntimeError("Start tick id {0} was not found.".format(start_id))
    elif start_time is not None:
        start_row = fetch_tick_at_or_after(symbol, start_time)
        if not start_row:
            raise RuntimeError("No ticks exist at or after start time {0}.".format(start_time.isoformat()))
    else:
        start_row = {"id": int(bounds["firstid"]), "timestamp": bounds["firstts"]}

    if end_id is not None:
        end_row = fetch_tick_row(symbol, int(end_id))
        if not end_row:
            raise RuntimeError("End tick id {0} was not found.".format(end_id))
    elif end_time is not None:
        end_row = fetch_tick_at_or_before(symbol, end_time)
        if not end_row:
            raise RuntimeError("No ticks exist at or before end time {0}.".format(end_time.isoformat()))
    else:
        end_row = {"id": int(bounds["lastid"]), "timestamp": bounds["lastts"]}

    start_tick_id = int(start_row["id"])
    end_tick_id = int(end_row["id"])
    if start_tick_id > end_tick_id:
        raise RuntimeError(
            "Resolved start tick id {0} is after end tick id {1}.".format(start_tick_id, end_tick_id)
        )

    return {
        "starttickid": start_tick_id,
        "endtickid": end_tick_id,
        "startts": start_row["timestamp"],
        "endts": end_row["timestamp"],
        "firsttickid": int(bounds["firstid"]),
        "lasttickid": int(bounds["lastid"]),
        "firstts": bounds["firstts"],
        "lastts": bounds["lastts"],
    }


def fetch_tick_rows_in_id_range(symbol: str, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    if start_id > end_id:
        return []
    limit = max(1, int(end_id) - int(start_id) + 1)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, timestamp, bid, ask, mid, spread,
                       COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                FROM public.ticks
                WHERE symbol = %s AND id BETWEEN %s AND %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (symbol, start_id, end_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def fetch_envelope_rows_for_tick_ids(symbol: str, tick_ids: Sequence[int], config: EnvelopeConfig) -> Dict[int, Dict[str, Any]]:
    if not tick_ids:
        return {}
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT tickid, timestamp, price, basis, mae, upper, lower
                FROM public.envelopetick
                WHERE symbol = %s
                  AND source = %s
                  AND length = %s
                  AND bandwidth = %s
                  AND mult = %s
                  AND tickid = ANY(%s)
                ORDER BY tickid ASC
                """,
                (symbol, config.source, config.length, config.bandwidth, config.mult, list(tick_ids)),
            )
            return {int(row["tickid"]): dict(row) for row in cur.fetchall()}


def fetch_envelope_storage_bounds(symbol: str, config: EnvelopeConfig) -> Dict[str, Any]:
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(tickid) AS firsttickid,
                    MAX(tickid) AS lasttickid,
                    COUNT(*)::bigint AS rowcount,
                    COUNT(*) FILTER (WHERE basis IS NOT NULL)::bigint AS basisrowcount,
                    COUNT(*) FILTER (WHERE upper IS NOT NULL AND lower IS NOT NULL)::bigint AS bandrowcount,
                    MIN(timestamp) AS firstts,
                    MAX(timestamp) AS lastts
                FROM public.envelopetick
                WHERE symbol = %s
                  AND source = %s
                  AND length = %s
                  AND bandwidth = %s
                  AND mult = %s
                """,
                (symbol, config.source, config.length, config.bandwidth, config.mult),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firsttickid": int(row["firsttickid"]) if row.get("firsttickid") is not None else None,
        "lasttickid": int(row["lasttickid"]) if row.get("lasttickid") is not None else None,
        "rowcount": int(row.get("rowcount") or 0),
        "basisrowcount": int(row.get("basisrowcount") or 0),
        "bandrowcount": int(row.get("bandrowcount") or 0),
        "firstts": row.get("firstts"),
        "lastts": row.get("lastts"),
    }


def load_envelope_job_state(job_name: str) -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, jobname, jobtype, symbol, source, length, bandwidth, mult,
                       starttickid, endtickid, startts, endts, lasttickid, lastts, statejson
                FROM public.envelopejobstate
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


def persist_envelope_progress(
    *,
    job_name: str,
    job_type: str,
    symbol: str,
    config: EnvelopeConfig,
    start_tick_id: Optional[int],
    end_tick_id: Optional[int],
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    last_tick_id: int,
    last_ts: Optional[datetime],
    statejson: Dict[str, Any],
    rows: Sequence[Dict[str, Any]],
) -> int:
    config = config.normalized()
    values = [
        (
            row["tickid"],
            row["symbol"],
            row["source"],
            row["length"],
            row["bandwidth"],
            row["mult"],
            row["timestamp"],
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
                    INSERT INTO public.envelopetick (
                        tickid, symbol, source, length, bandwidth, mult, timestamp,
                        price, basis, mae, upper, lower
                    )
                    VALUES %s
                    ON CONFLICT (tickid, symbol, source, length, bandwidth, mult)
                    DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
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
                INSERT INTO public.envelopejobstate (
                    jobname, jobtype, symbol, source, length, bandwidth, mult,
                    starttickid, endtickid, startts, endts, lasttickid, lastts, statejson
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (jobname)
                DO UPDATE SET
                    jobtype = EXCLUDED.jobtype,
                    symbol = EXCLUDED.symbol,
                    source = EXCLUDED.source,
                    length = EXCLUDED.length,
                    bandwidth = EXCLUDED.bandwidth,
                    mult = EXCLUDED.mult,
                    starttickid = EXCLUDED.starttickid,
                    endtickid = EXCLUDED.endtickid,
                    startts = EXCLUDED.startts,
                    endts = EXCLUDED.endts,
                    lasttickid = EXCLUDED.lasttickid,
                    lastts = EXCLUDED.lastts,
                    statejson = EXCLUDED.statejson,
                    updatedat = NOW()
                """,
                (
                    job_name,
                    job_type,
                    symbol,
                    config.source,
                    config.length,
                    config.bandwidth,
                    config.mult,
                    start_tick_id,
                    end_tick_id,
                    start_ts,
                    end_ts,
                    last_tick_id,
                    last_ts,
                    json.dumps(statejson),
                ),
            )
        conn.commit()
    return len(values)


def fetch_envelope_sync_diagnostics(
    symbol: str,
    config: EnvelopeConfig,
    *,
    requested_start_tick_id: Optional[int] = None,
    requested_end_tick_id: Optional[int] = None,
) -> Dict[str, Any]:
    config = config.normalized()
    storage = fetch_envelope_storage_bounds(symbol, config)
    job_state = load_envelope_job_state(config.worker_job_name(symbol))

    latest_stored_tick_id = storage.get("lasttickid")
    requested_gap_count = 0
    if latest_stored_tick_id is not None and requested_end_tick_id is not None:
        requested_gap_count = max(0, int(requested_end_tick_id) - int(latest_stored_tick_id))

    return {
        "storage": {
            "firstTickId": storage.get("firsttickid"),
            "lastTickId": latest_stored_tick_id,
            "rowCount": storage.get("rowcount"),
            "basisRowCount": storage.get("basisrowcount"),
            "bandRowCount": storage.get("bandrowcount"),
            "firstTs": storage.get("firstts"),
            "lastTs": storage.get("lastts"),
        },
        "jobState": {
            "lastTickId": int(job_state["lasttickid"]) if job_state and job_state.get("lasttickid") is not None else None,
            "lastTs": job_state.get("lastts") if job_state else None,
            "jobName": job_state.get("jobname") if job_state else None,
        },
        "requested": {
            "firstTickId": requested_start_tick_id,
            "lastTickId": requested_end_tick_id,
            "gapCountAheadOfStorage": requested_gap_count,
        },
        "latestStoredTickId": latest_stored_tick_id,
    }
