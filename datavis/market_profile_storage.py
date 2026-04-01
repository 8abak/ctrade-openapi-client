from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2.extras

from datavis.db import db_connection
from datavis.envelope_storage import resolve_backfill_range
from datavis.market_profile import MarketProfileConfig, compute_profile_metrics
from datavis.ott_storage import DEFAULT_SYMBOL, fetch_tick_batch_after


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.marketprofile (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    binsize DOUBLE PRECISION NOT NULL CHECK (binsize > 0),
    maxgapms INTEGER NOT NULL CHECK (maxgapms > 0),
    sessionlabel TEXT NOT NULL,
    sessionstart TIMESTAMPTZ NOT NULL,
    sessionend TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    firsttickid BIGINT,
    lasttickid BIGINT,
    firstts TIMESTAMPTZ,
    lastts TIMESTAMPTZ,
    totalweightms DOUBLE PRECISION NOT NULL DEFAULT 0,
    totalticks BIGINT NOT NULL DEFAULT 0,
    poc DOUBLE PRECISION,
    vah DOUBLE PRECISION,
    val DOUBLE PRECISION,
    hvns JSONB NOT NULL DEFAULT '[]'::jsonb,
    lvns JSONB NOT NULL DEFAULT '[]'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofile_identity_uk UNIQUE (symbol, source, binsize, maxgapms, sessionstart)
);

CREATE TABLE IF NOT EXISTS public.marketprofilebin (
    id BIGSERIAL PRIMARY KEY,
    profileid BIGINT NOT NULL REFERENCES public.marketprofile(id) ON DELETE CASCADE,
    pricebin DOUBLE PRECISION NOT NULL,
    weightms DOUBLE PRECISION NOT NULL DEFAULT 0,
    tickcount BIGINT NOT NULL DEFAULT 0,
    ispoc BOOLEAN NOT NULL DEFAULT FALSE,
    isvah BOOLEAN NOT NULL DEFAULT FALSE,
    isval BOOLEAN NOT NULL DEFAULT FALSE,
    ishvn BOOLEAN NOT NULL DEFAULT FALSE,
    islvn BOOLEAN NOT NULL DEFAULT FALSE,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofilebin_identity_uk UNIQUE (profileid, pricebin)
);

CREATE TABLE IF NOT EXISTS public.marketprofilestate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    binsize DOUBLE PRECISION NOT NULL CHECK (binsize > 0),
    maxgapms INTEGER NOT NULL CHECK (maxgapms > 0),
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofilestate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS marketprofile_lookup_idx
    ON public.marketprofile (symbol, source, binsize, maxgapms, sessionstart DESC);

CREATE INDEX IF NOT EXISTS marketprofile_lasttick_idx
    ON public.marketprofile (symbol, source, binsize, maxgapms, lasttickid DESC);

CREATE INDEX IF NOT EXISTS marketprofilebin_profile_idx
    ON public.marketprofilebin (profileid, pricebin);

CREATE INDEX IF NOT EXISTS marketprofilestate_lookup_idx
    ON public.marketprofilestate (symbol, source, binsize, maxgapms, updatedat DESC);
"""


def ensure_market_profile_schema() -> None:
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()


def serialize_nodes(nodes: Sequence[Dict[str, Any]]) -> str:
    return json.dumps(
        [
            {
                "price": float(node["price"]),
                "low": float(node["low"]),
                "high": float(node["high"]),
                "weightms": float(node.get("weightms") or 0),
                "tickcount": int(node.get("tickcount") or 0),
            }
            for node in nodes
        ]
    )


def load_market_profile_state(job_name: str) -> Optional[Dict[str, Any]]:
    ensure_market_profile_schema()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, jobname, jobtype, symbol, source, binsize, maxgapms, lasttickid, lastts, statejson
                FROM public.marketprofilestate
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


def save_market_profile_state(
    *,
    job_name: str,
    job_type: str,
    symbol: str,
    config: MarketProfileConfig,
    last_tick_id: int,
    last_ts: Optional[datetime],
    statejson: Dict[str, Any],
) -> None:
    ensure_market_profile_schema()
    config = config.normalized()
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.marketprofilestate (
                    jobname, jobtype, symbol, source, binsize, maxgapms, lasttickid, lastts, statejson
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (jobname)
                DO UPDATE SET
                    jobtype = EXCLUDED.jobtype,
                    symbol = EXCLUDED.symbol,
                    source = EXCLUDED.source,
                    binsize = EXCLUDED.binsize,
                    maxgapms = EXCLUDED.maxgapms,
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
                    config.binsize,
                    config.maxgapms,
                    int(last_tick_id),
                    last_ts,
                    json.dumps(statejson),
                ),
            )
        conn.commit()


def upsert_market_profile(
    *,
    symbol: str,
    config: MarketProfileConfig,
    session_label: str,
    session_start: datetime,
    session_end: datetime,
    first_tick_id: int,
    first_ts: datetime,
    last_tick_id: int,
    last_ts: datetime,
    status: str,
) -> int:
    ensure_market_profile_schema()
    config = config.normalized()
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.marketprofile (
                    symbol, source, binsize, maxgapms, sessionlabel, sessionstart, sessionend,
                    status, firsttickid, lasttickid, firstts, lastts
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, source, binsize, maxgapms, sessionstart)
                DO UPDATE SET
                    sessionlabel = EXCLUDED.sessionlabel,
                    sessionend = EXCLUDED.sessionend,
                    status = EXCLUDED.status,
                    firsttickid = LEAST(COALESCE(public.marketprofile.firsttickid, EXCLUDED.firsttickid), EXCLUDED.firsttickid),
                    lasttickid = GREATEST(COALESCE(public.marketprofile.lasttickid, EXCLUDED.lasttickid), EXCLUDED.lasttickid),
                    firstts = LEAST(COALESCE(public.marketprofile.firstts, EXCLUDED.firstts), EXCLUDED.firstts),
                    lastts = GREATEST(COALESCE(public.marketprofile.lastts, EXCLUDED.lastts), EXCLUDED.lastts),
                    updatedat = NOW()
                RETURNING id
                """,
                (
                    symbol,
                    config.source,
                    config.binsize,
                    config.maxgapms,
                    session_label,
                    session_start,
                    session_end,
                    status,
                    first_tick_id,
                    last_tick_id,
                    first_ts,
                    last_ts,
                ),
            )
            profile_id = int(cur.fetchone()[0])
        conn.commit()
    return profile_id


def apply_bin_deltas(profile_id: int, deltas: Iterable[Tuple[float, float, int]]) -> None:
    delta_rows = [
        (int(profile_id), float(price_bin), float(weight_ms), int(tick_count))
        for price_bin, weight_ms, tick_count in deltas
    ]
    if not delta_rows:
        return
    ensure_market_profile_schema()
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO public.marketprofilebin (
                    profileid, pricebin, weightms, tickcount
                )
                VALUES %s
                ON CONFLICT (profileid, pricebin)
                DO UPDATE SET
                    weightms = public.marketprofilebin.weightms + EXCLUDED.weightms,
                    tickcount = public.marketprofilebin.tickcount + EXCLUDED.tickcount,
                    updatedat = NOW()
                """,
                delta_rows,
                page_size=min(1000, len(delta_rows)),
            )
        conn.commit()


def fetch_profile_bins(profile_id: int) -> List[Dict[str, Any]]:
    ensure_market_profile_schema()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT pricebin, weightms, tickcount
                FROM public.marketprofilebin
                WHERE profileid = %s
                ORDER BY pricebin ASC
                """,
                (profile_id,),
            )
            return [dict(row) for row in cur.fetchall()]


def update_profile_flags(profile_id: int, flags: Dict[float, Dict[str, Any]]) -> None:
    ensure_market_profile_schema()
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.marketprofilebin
                SET ispoc = FALSE,
                    isvah = FALSE,
                    isval = FALSE,
                    ishvn = FALSE,
                    islvn = FALSE,
                    updatedat = NOW()
                WHERE profileid = %s
                """,
                (profile_id,),
            )
            if flags:
                values = [
                    (
                        int(profile_id),
                        float(price_bin),
                        bool(details.get("ispoc")),
                        bool(details.get("isvah")),
                        bool(details.get("isval")),
                        bool(details.get("ishvn")),
                        bool(details.get("islvn")),
                    )
                    for price_bin, details in flags.items()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE public.marketprofilebin AS target
                    SET ispoc = source.ispoc,
                        isvah = source.isvah,
                        isval = source.isval,
                        ishvn = source.ishvn,
                        islvn = source.islvn,
                        updatedat = NOW()
                    FROM (VALUES %s) AS source(profileid, pricebin, ispoc, isvah, isval, ishvn, islvn)
                    WHERE target.profileid = source.profileid
                      AND target.pricebin = source.pricebin
                    """,
                    values,
                    page_size=min(1000, len(values)),
                )
        conn.commit()


def refresh_market_profile_summary(
    *,
    profile_id: int,
    config: MarketProfileConfig,
    status: str,
    last_tick_id: int,
    last_ts: datetime,
) -> Dict[str, Any]:
    config = config.normalized()
    bins = fetch_profile_bins(profile_id)
    metrics = compute_profile_metrics(
        bins,
        binsize=config.binsize,
        valueareapercent=config.valueareapercent,
        nodelimit=config.nodelimit,
    )
    update_profile_flags(profile_id, metrics.get("flags") or {})
    ensure_market_profile_schema()
    with db_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.marketprofile
                SET status = %s,
                    lasttickid = GREATEST(COALESCE(lasttickid, %s), %s),
                    lastts = GREATEST(COALESCE(lastts, %s), %s),
                    totalweightms = %s,
                    totalticks = %s,
                    poc = %s,
                    vah = %s,
                    val = %s,
                    hvns = %s::jsonb,
                    lvns = %s::jsonb,
                    updatedat = NOW()
                WHERE id = %s
                """,
                (
                    status,
                    last_tick_id,
                    last_tick_id,
                    last_ts,
                    last_ts,
                    float(metrics["totalweightms"]),
                    int(metrics["totalticks"]),
                    metrics.get("poc"),
                    metrics.get("vah"),
                    metrics.get("val"),
                    serialize_nodes(metrics.get("hvns") or []),
                    serialize_nodes(metrics.get("lvns") or []),
                    int(profile_id),
                ),
            )
        conn.commit()
    return metrics


def fetch_market_profile_storage_bounds(symbol: str, config: MarketProfileConfig) -> Dict[str, Any]:
    ensure_market_profile_schema()
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(firsttickid) AS firsttickid,
                    MAX(lasttickid) AS lasttickid,
                    COUNT(*)::bigint AS rowcount,
                    MIN(sessionstart) AS firstts,
                    MAX(lastts) AS lastts
                FROM public.marketprofile
                WHERE symbol = %s
                  AND source = %s
                  AND binsize = %s
                  AND maxgapms = %s
                """,
                (symbol, config.source, config.binsize, config.maxgapms),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firsttickid": int(row["firsttickid"]) if row.get("firsttickid") is not None else None,
        "lasttickid": int(row["lasttickid"]) if row.get("lasttickid") is not None else None,
        "rowcount": int(row.get("rowcount") or 0),
        "firstts": row.get("firstts"),
        "lastts": row.get("lastts"),
    }


def fetch_market_profile_sync_diagnostics(
    symbol: str,
    config: MarketProfileConfig,
    *,
    requested_end_tick_id: Optional[int] = None,
) -> Dict[str, Any]:
    storage = fetch_market_profile_storage_bounds(symbol, config)
    job_state = load_market_profile_state(config.worker_job_name(symbol))
    gap_count = 0
    if storage.get("lasttickid") is not None and requested_end_tick_id is not None:
        gap_count = max(0, int(requested_end_tick_id) - int(storage["lasttickid"]))
    return {
        "storage": {
            "firstTickId": storage.get("firsttickid"),
            "lastTickId": storage.get("lasttickid"),
            "rowCount": storage.get("rowcount"),
            "firstTs": storage.get("firstts"),
            "lastTs": storage.get("lastts"),
        },
        "jobState": {
            "jobName": job_state.get("jobname") if job_state else None,
            "lastTickId": int(job_state["lasttickid"]) if job_state and job_state.get("lasttickid") is not None else None,
            "lastTs": job_state.get("lastts") if job_state else None,
        },
        "requested": {
            "lastTickId": requested_end_tick_id,
            "gapCountAheadOfStorage": gap_count,
        },
        "latestStoredTickId": storage.get("lasttickid"),
    }


def _decode_json_field(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value]
    if isinstance(value, str) and value:
        return [dict(item) for item in json.loads(value)]
    return []


def fetch_market_profile_rows_for_window(
    symbol: str,
    config: MarketProfileConfig,
    *,
    start_id: int,
    end_id: int,
) -> List[Dict[str, Any]]:
    ensure_market_profile_schema()
    config = config.normalized()
    range_info = resolve_backfill_range(symbol, start_id=start_id, end_id=end_id)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM public.marketprofile
                WHERE symbol = %s
                  AND source = %s
                  AND binsize = %s
                  AND maxgapms = %s
                  AND sessionstart < %s
                  AND sessionend > %s
                ORDER BY sessionstart ASC, id ASC
                """,
                (
                    symbol,
                    config.source,
                    config.binsize,
                    config.maxgapms,
                    range_info["endts"],
                    range_info["startts"],
                ),
            )
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["hvns"] = _decode_json_field(row.get("hvns"))
        row["lvns"] = _decode_json_field(row.get("lvns"))
    return rows


def fetch_market_profile_rows_after_tick(
    symbol: str,
    config: MarketProfileConfig,
    *,
    after_id: int,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    ensure_market_profile_schema()
    config = config.normalized()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT *
                    FROM public.marketprofile
                    WHERE symbol = %s
                      AND source = %s
                      AND binsize = %s
                      AND maxgapms = %s
                      AND lasttickid > %s
                    ORDER BY lasttickid ASC, id ASC
                    """,
                    (symbol, config.source, config.binsize, config.maxgapms, after_id),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM public.marketprofile
                    WHERE symbol = %s
                      AND source = %s
                      AND binsize = %s
                      AND maxgapms = %s
                      AND lasttickid > %s
                      AND firsttickid <= %s
                    ORDER BY lasttickid ASC, id ASC
                    """,
                    (symbol, config.source, config.binsize, config.maxgapms, after_id, end_id),
                )
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["hvns"] = _decode_json_field(row.get("hvns"))
        row["lvns"] = _decode_json_field(row.get("lvns"))
    return rows
