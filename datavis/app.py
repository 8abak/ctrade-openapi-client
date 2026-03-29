#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import secrets
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
from psycopg2 import extensions as pg_extensions
from psycopg2 import sql as pg_sql
import sqlparse
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datavis.db import db_connect as shared_db_connect
from datavis.envelope import (
    DEFAULT_ENVELOPE_BANDWIDTH,
    DEFAULT_ENVELOPE_LENGTH,
    DEFAULT_ENVELOPE_MULT,
    DEFAULT_ENVELOPE_SOURCE,
    EnvelopeConfig,
)
from datavis.envelope_storage import (
    fetch_envelope_rows_for_tick_ids,
    fetch_envelope_sync_diagnostics,
    resolve_backfill_range,
)
from datavis.ott import (
    DEFAULT_OTT_LENGTH,
    DEFAULT_OTT_MA_TYPE,
    DEFAULT_OTT_PERCENT,
    DEFAULT_OTT_SIGNAL_MODE,
    DEFAULT_OTT_SOURCE,
    OttConfig,
)
from datavis.ott_storage import (
    fetch_backtest_overlay,
    fetch_bootstrap_tick_rows,
    fetch_next_tick_rows,
    fetch_ott_rows_for_tick_ids,
    fetch_ott_sync_diagnostics,
    resolve_last_week_range,
    run_and_store_backtest,
)
from datavis.zigzag import ZIG_LEVELS, zig_worker_job_name
from datavis.zigzag_storage import (
    fetch_level_rows_after_confirm,
    fetch_level_rows_for_window,
    fetch_zig_sync_diagnostics,
    load_zig_state,
)
BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
ASSETS_DIR = FRONTEND_DIR / "assets"

load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://", 1)

TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
DEFAULT_WINDOW = 2000
MAX_WINDOW = 10000
MAX_STREAM_BATCH = 1000
MAX_QUERY_ROWS = 1000
DEFAULT_SQL_PREVIEW_LIMIT = 100
MAX_SQL_PREVIEW_LIMIT = 500
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
STREAM_POLL_SECONDS = float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "1.0"))
STREAM_KEEPALIVE_SECONDS = 15.0
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")
SYSTEM_SCHEMA_PREFIXES = ("pg_toast", "pg_temp_")
DEFAULT_REVIEW_TIMEZONE = "Australia/Sydney"
TRANSACTION_CONTROL_HEADS = {
    "BEGIN",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
}


class QueryRequest(BaseModel):
    sql: str


class OttBacktestRunRequest(BaseModel):
    source: str = DEFAULT_OTT_SOURCE
    matype: str = DEFAULT_OTT_MA_TYPE
    length: int = DEFAULT_OTT_LENGTH
    percent: float = DEFAULT_OTT_PERCENT
    signalmode: str = DEFAULT_OTT_SIGNAL_MODE
    rangepreset: str = "lastweek"
    force: bool = False


security = HTTPBasic(auto_error=False)


app = FastAPI(title="datavis.au", version="1.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False, autocommit: bool = False):
    if DATABASE_URL:
        conn = psycopg2.connect(ensure_database_url())
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
    else:
        conn = shared_db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def require_sql_admin(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> str:
    if not SQL_ADMIN_USER or not SQL_ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SQL admin credentials are not configured.",
        )

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SQL admin authentication is required.",
            headers={"WWW-Authenticate": 'Basic realm="datavis SQL"'},
        )

    valid_user = secrets.compare_digest(credentials.username or "", SQL_ADMIN_USER)
    valid_password = secrets.compare_digest(credentials.password or "", SQL_ADMIN_PASSWORD)
    if not (valid_user and valid_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid SQL admin credentials.",
            headers={"WWW-Authenticate": 'Basic realm="datavis SQL"'},
        )

    return credentials.username


def serialize_tick_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["timestamp"]
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": row["bid"],
        "ask": row["ask"],
        "mid": row["mid"],
        "spread": row["spread"],
        "price": row["price"],
    }


def build_ott_config(source: str, matype: str, length: int, percent: float) -> OttConfig:
    try:
        return OttConfig(source=source, matype=matype, length=length, percent=percent).normalized()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def build_envelope_config(source: str, length: int, bandwidth: float, mult: float) -> EnvelopeConfig:
    try:
        return EnvelopeConfig(source=source, length=length, bandwidth=bandwidth, mult=mult).normalized()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def tick_source_price(row: Dict[str, Any], source: str) -> Optional[float]:
    if source == "ask":
        return row.get("ask")
    if source == "bid":
        return row.get("bid")
    return row.get("mid") if row.get("mid") is not None else row.get("price")


def serialize_ott_row(tick_row: Dict[str, Any], ott_row: Optional[Dict[str, Any]], config: OttConfig) -> Dict[str, Any]:
    timestamp = tick_row["timestamp"]
    base = {
        "tickid": tick_row["id"],
        "symbol": tick_row["symbol"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": tick_row["bid"],
        "ask": tick_row["ask"],
        "mid": tick_row["mid"],
        "spread": tick_row["spread"],
        "price": tick_source_price(tick_row, config.source),
        "available": ott_row is not None,
        "source": config.source,
        "matype": config.matype,
        "length": config.length,
        "percent": config.percent,
        "mavg": None,
        "fark": None,
        "longstop": None,
        "shortstop": None,
        "dir": None,
        "mt": None,
        "ott": None,
        "ott2": None,
        "ott3": None,
        "supportbuy": False,
        "supportsell": False,
        "pricebuy": False,
        "pricesell": False,
        "colorbuy": False,
        "colorsell": False,
        "ottcolor": None,
        "highlightup": False,
        "highlightdown": False,
    }
    if ott_row is None:
        return base

    ott2 = ott_row.get("ott2")
    ott3 = ott_row.get("ott3")
    mavg = ott_row.get("mavg")
    ott = ott_row.get("ott")
    base.update(
        {
            "price": ott_row.get("price", base["price"]),
            "mavg": mavg,
            "fark": ott_row.get("fark"),
            "longstop": ott_row.get("longstop"),
            "shortstop": ott_row.get("shortstop"),
            "dir": ott_row.get("dir"),
            "mt": ott_row.get("mt"),
            "ott": ott,
            "ott2": ott2,
            "ott3": ott3,
            "supportbuy": bool(ott_row.get("supportbuy")),
            "supportsell": bool(ott_row.get("supportsell")),
            "pricebuy": bool(ott_row.get("pricebuy")),
            "pricesell": bool(ott_row.get("pricesell")),
            "colorbuy": bool(ott_row.get("colorbuy")),
            "colorsell": bool(ott_row.get("colorsell")),
            "ottcolor": None if ott2 is None or ott3 is None else ("green" if ott2 > ott3 else "red"),
            "highlightup": bool(mavg is not None and ott is not None and mavg > ott),
            "highlightdown": bool(mavg is not None and ott is not None and mavg < ott),
        }
    )
    return base


def build_ott_response(
    tick_rows: List[Dict[str, Any]],
    config: OttConfig,
    *,
    mode: Optional[str] = None,
    requested_start_id: Optional[int] = None,
    advanced_from_id: Optional[int] = None,
    signalmode: str = DEFAULT_OTT_SIGNAL_MODE,
) -> Dict[str, Any]:
    tick_ids = [int(row["id"]) for row in tick_rows]
    ott_rows = fetch_ott_rows_for_tick_ids(TICK_SYMBOL, tick_ids, config)
    rows = [serialize_ott_row(row, ott_rows.get(int(row["id"])), config) for row in tick_rows]
    available_count = sum(1 for row in rows if row["available"])
    requested_first_id = rows[0]["tickid"] if rows else requested_start_id
    requested_last_id = rows[-1]["tickid"] if rows else requested_start_id
    diagnostics = fetch_ott_sync_diagnostics(
        TICK_SYMBOL,
        config,
        requested_start_tick_id=requested_first_id,
        requested_end_tick_id=requested_last_id,
        signalmode=signalmode if mode == "review" and requested_first_id is not None and requested_last_id is not None else None,
    )
    latest_stored_tick_id = diagnostics["latestStoredTickId"]
    gap_ahead_of_storage = int(diagnostics["requested"]["gapCountAheadOfStorage"] or 0)
    signal_counts = diagnostics.get("signalCounts")
    if not rows:
        status_text = "no-ticks"
        message = "No ticks matched the requested OTT window."
    elif available_count == 0:
        if latest_stored_tick_id is None:
            status_text = "empty"
            message = "No stored OTT rows exist for the requested symbol/source/MA/length/percent. Run the OTT backfill or worker."
        else:
            status_text = "ahead"
            message = (
                "Stored OTT rows currently stop at tick {0}; requested ticks reach {1}. "
                "Run the OTT worker to extend storage."
            ).format(latest_stored_tick_id, requested_last_id)
    elif available_count < len(rows):
        if gap_ahead_of_storage > 0 and latest_stored_tick_id is not None:
            status_text = "ahead"
            message = (
                "Stored OTT rows currently stop at tick {0}; requested ticks reach {1}. "
                "Missing {2} row(s) in this window."
            ).format(latest_stored_tick_id, requested_last_id, len(rows) - available_count)
        else:
            status_text = "partial"
            message = "Some ticks in the requested window do not have stored OTT rows yet."
    elif mode == "review" and signal_counts and int(signal_counts.get("totalCount") or 0) == 0:
        status_text = "no-signals"
        message = "No {0} signals were found in the selected review range.".format(signalmode.lower())
    else:
        status_text = "ok"
        message = None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "availableRowCount": available_count,
        "missingRowCount": len(rows) - available_count,
        "status": status_text,
        "message": message,
        "firstId": rows[0]["tickid"] if rows else None,
        "lastId": rows[-1]["tickid"] if rows else requested_start_id,
        "requestedId": requested_start_id,
        "advancedFromId": advanced_from_id,
        "latestStoredTickId": latest_stored_tick_id,
        "storageFirstTickId": diagnostics["storage"]["firstTickId"],
        "storageRowCount": diagnostics["storage"]["rowCount"],
        "jobStateLastTickId": diagnostics["jobState"]["lastTickId"],
        "gapAheadOfStorage": gap_ahead_of_storage,
        "signalMode": signalmode,
        "signalCounts": signal_counts,
        "coverage": diagnostics,
        "mode": mode,
        "symbol": TICK_SYMBOL,
        "source": config.source,
        "matype": config.matype,
        "length": config.length,
        "percent": config.percent,
    }


def serialize_envelope_row(
    tick_row: Dict[str, Any],
    envelope_row: Optional[Dict[str, Any]],
    config: EnvelopeConfig,
) -> Dict[str, Any]:
    timestamp = tick_row["timestamp"]
    basis_available = bool(envelope_row is not None and envelope_row.get("basis") is not None)
    band_available = bool(
        envelope_row is not None
        and envelope_row.get("upper") is not None
        and envelope_row.get("lower") is not None
    )
    base = {
        "tickid": tick_row["id"],
        "symbol": tick_row["symbol"],
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": tick_row["bid"],
        "ask": tick_row["ask"],
        "mid": tick_row["mid"],
        "spread": tick_row["spread"],
        "price": tick_source_price(tick_row, config.source),
        "stored": envelope_row is not None,
        "available": band_available,
        "basisAvailable": basis_available,
        "bandAvailable": band_available,
        "source": config.source,
        "length": config.length,
        "bandwidth": config.bandwidth,
        "mult": config.mult,
        "basis": None,
        "mae": None,
        "upper": None,
        "lower": None,
    }
    if envelope_row is None:
        return base

    base.update(
        {
            "price": envelope_row.get("price", base["price"]),
            "basis": envelope_row.get("basis"),
            "mae": envelope_row.get("mae"),
            "upper": envelope_row.get("upper"),
            "lower": envelope_row.get("lower"),
        }
    )
    return base


def build_envelope_response(
    tick_rows: List[Dict[str, Any]],
    config: EnvelopeConfig,
    *,
    mode: Optional[str] = None,
    requested_start_id: Optional[int] = None,
    advanced_from_id: Optional[int] = None,
) -> Dict[str, Any]:
    tick_ids = [int(row["id"]) for row in tick_rows]
    envelope_rows = fetch_envelope_rows_for_tick_ids(TICK_SYMBOL, tick_ids, config)
    rows = [serialize_envelope_row(row, envelope_rows.get(int(row["id"])), config) for row in tick_rows]
    stored_count = sum(1 for row in rows if row["stored"])
    basis_available_count = sum(1 for row in rows if row["basisAvailable"])
    band_available_count = sum(1 for row in rows if row["bandAvailable"])
    requested_first_id = rows[0]["tickid"] if rows else requested_start_id
    requested_last_id = rows[-1]["tickid"] if rows else requested_start_id
    diagnostics = fetch_envelope_sync_diagnostics(
        TICK_SYMBOL,
        config,
        requested_start_tick_id=requested_first_id,
        requested_end_tick_id=requested_last_id,
    )
    latest_stored_tick_id = diagnostics["latestStoredTickId"]
    gap_ahead_of_storage = int(diagnostics["requested"]["gapCountAheadOfStorage"] or 0)

    if not rows:
        status_text = "no-ticks"
        message = "No ticks matched the requested envelope window."
    elif stored_count == 0:
        if latest_stored_tick_id is None:
            status_text = "empty"
            message = "No stored envelope rows exist for the requested source/length/bandwidth/mult yet. Run the envelope worker or backfill."
        else:
            status_text = "ahead"
            message = (
                "Stored envelope rows currently stop at tick {0}; requested ticks reach {1}. "
                "Run the envelope worker or backfill to extend storage."
            ).format(latest_stored_tick_id, requested_last_id)
    elif band_available_count == 0:
        if gap_ahead_of_storage > 0 and latest_stored_tick_id is not None:
            status_text = "ahead"
            message = (
                "Stored envelope rows currently stop at tick {0}; requested ticks reach {1}. "
                "Missing {2} row(s) in this window."
            ).format(latest_stored_tick_id, requested_last_id, len(rows) - stored_count)
        elif basis_available_count > 0:
            status_text = "warming"
            message = "Envelope basis is available, but upper/lower bands are still warming up in this window."
        else:
            status_text = "warming"
            message = "Envelope warmup is not complete in the requested window yet."
    elif band_available_count < len(rows):
        if gap_ahead_of_storage > 0 and latest_stored_tick_id is not None:
            status_text = "ahead"
            message = (
                "Stored envelope rows currently stop at tick {0}; requested ticks reach {1}. "
                "Missing {2} row(s) in this window."
            ).format(latest_stored_tick_id, requested_last_id, len(rows) - stored_count)
        else:
            status_text = "partial"
            message = "Envelope history is only partially available in the requested window."
    else:
        status_text = "ok"
        message = None

    return {
        "rows": rows,
        "rowCount": len(rows),
        "storedRowCount": stored_count,
        "basisAvailableRowCount": basis_available_count,
        "availableRowCount": band_available_count,
        "missingRowCount": len(rows) - band_available_count,
        "status": status_text,
        "message": message,
        "firstId": rows[0]["tickid"] if rows else None,
        "lastId": rows[-1]["tickid"] if rows else requested_start_id,
        "requestedId": requested_start_id,
        "advancedFromId": advanced_from_id,
        "latestStoredTickId": latest_stored_tick_id,
        "storageFirstTickId": diagnostics["storage"]["firstTickId"],
        "storageRowCount": diagnostics["storage"]["rowCount"],
        "storageBandRowCount": diagnostics["storage"]["bandRowCount"],
        "jobStateLastTickId": diagnostics["jobState"]["lastTickId"],
        "gapAheadOfStorage": gap_ahead_of_storage,
        "coverage": diagnostics,
        "mode": mode,
        "symbol": TICK_SYMBOL,
        "source": config.source,
        "length": config.length,
        "bandwidth": config.bandwidth,
        "mult": config.mult,
    }


def normalize_zig_levels(raw_levels: Optional[str]) -> List[str]:
    if not raw_levels:
        return list(ZIG_LEVELS)
    selected: List[str] = []
    for token in str(raw_levels).split(","):
        level = token.strip().lower()
        if level in ZIG_LEVELS and level not in selected:
            selected.append(level)
    if not selected:
        raise HTTPException(status_code=400, detail="Unsupported Zig level selection.")
    return selected


def parse_optional_timestamp(raw_value: Optional[str], timezone_name: str = DEFAULT_REVIEW_TIMEZONE) -> Optional[datetime]:
    if not raw_value:
        return None
    try:
        target_tz = ZoneInfo(timezone_name or DEFAULT_REVIEW_TIMEZONE)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=400, detail="Unsupported timezone.") from exc
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid timestamp.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=target_tz)
    else:
        parsed = parsed.astimezone(target_tz)
    return parsed.astimezone(timezone.utc)


def serialize_zig_row(row: Dict[str, Any], level: str) -> Dict[str, Any]:
    start_time = row["starttime"]
    end_time = row["endtime"]
    confirm_time = row["confirmtime"]
    return {
        "level": level,
        "id": row["id"],
        "symbol": row["symbol"],
        "starttickid": row["starttickid"],
        "endtickid": row["endtickid"],
        "confirmtickid": row["confirmtickid"],
        "starttime": start_time.isoformat(),
        "endtime": end_time.isoformat(),
        "confirmtime": confirm_time.isoformat(),
        "startTimeMs": dt_to_ms(start_time),
        "endTimeMs": dt_to_ms(end_time),
        "confirmTimeMs": dt_to_ms(confirm_time),
        "startprice": row["startprice"],
        "endprice": row["endprice"],
        "highprice": row["highprice"],
        "lowprice": row["lowprice"],
        "dir": row["dir"],
        "tickcount": row["tickcount"],
        "childcount": row.get("childcount"),
        "dursec": row["dursec"],
        "amplitude": row["amplitude"],
        "score": row["score"],
        "status": row["status"],
        "childstartid": row.get("childstartid"),
        "childendid": row.get("childendid"),
        "parentid": row.get("parentid"),
    }


def serialize_zig_state_point(point: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not point:
        return None
    point_time = datetime.fromisoformat(point["timestamp"]) if point.get("timestamp") else None
    confirm_time = datetime.fromisoformat(point["confirmtime"]) if point.get("confirmtime") else None
    return {
        "tickid": point.get("tickid"),
        "timestamp": point.get("timestamp"),
        "timestampMs": dt_to_ms(point_time),
        "price": point.get("price"),
        "kind": point.get("kind"),
        "sourceid": point.get("sourceid"),
        "confirmtickid": point.get("confirmtickid"),
        "confirmtime": point.get("confirmtime"),
        "confirmTimeMs": dt_to_ms(confirm_time),
        "seq": point.get("seq"),
    }


def serialize_zig_state_payload(state_row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not state_row:
        return None
    statejson = state_row.get("statejson") or {}
    return {
        "jobName": state_row.get("jobname"),
        "jobType": state_row.get("jobtype"),
        "symbol": state_row.get("symbol"),
        "lastTickId": state_row.get("lasttickid"),
        "lastTime": serialize_value(state_row.get("lasttime")),
        "version": statejson.get("version"),
        "meta": statejson.get("meta") if isinstance(statejson.get("meta"), dict) else None,
        "levels": {
            level: {
                "direction": (statejson.get(level) or {}).get("direction"),
                "lastConfirmed": serialize_zig_state_point((statejson.get(level) or {}).get("lastconfirmed")),
                "candidate": serialize_zig_state_point((statejson.get(level) or {}).get("candidate")),
                "counterEvent": serialize_zig_state_point((statejson.get(level) or {}).get("counterevent")),
            }
            for level in ZIG_LEVELS
        },
    }


def summarize_zig_status(requested_end_id: Optional[int]) -> Tuple[str, Optional[str], Dict[str, Any], Optional[Dict[str, Any]]]:
    job_name = zig_worker_job_name(TICK_SYMBOL)
    diagnostics = fetch_zig_sync_diagnostics(TICK_SYMBOL, job_name)
    state_row = load_zig_state(job_name)
    last_tick_id = diagnostics["jobState"]["lastTickId"]
    storage_tick_ids = [
        int(details["lastTickId"])
        for details in diagnostics["levels"].values()
        if details.get("lastTickId") is not None
    ]
    storage_last_tick_id = max(storage_tick_ids) if storage_tick_ids else None
    if last_tick_id is None:
        if storage_last_tick_id is not None and (requested_end_id is None or int(storage_last_tick_id) >= int(requested_end_id)):
            return ("ok", None, diagnostics, state_row)
        return (
            "empty",
            "No stored Zig state exists yet. Run the Zig worker or backfill.",
            diagnostics,
            state_row,
        )
    if requested_end_id is not None and int(last_tick_id) < int(requested_end_id):
        return (
            "ahead",
            "Zig worker currently processed tick {0}; requested ticks reach {1}.".format(last_tick_id, requested_end_id),
            diagnostics,
            state_row,
        )
    return ("ok", None, diagnostics, state_row)


def build_zig_window_payload(start_id: int, end_id: int, selected_levels: List[str]) -> Dict[str, Any]:
    status_text, message, diagnostics, state_row = summarize_zig_status(end_id)
    levels_payload = {}
    total_rows = 0
    for level in selected_levels:
        rows = fetch_level_rows_for_window(TICK_SYMBOL, level, start_id, end_id, end_id)
        serialized_rows = [serialize_zig_row(row, level) for row in rows]
        levels_payload[level] = {
            "rows": serialized_rows,
            "rowCount": len(serialized_rows),
            "latestConfirmTickId": diagnostics["levels"][level]["lastConfirmTickId"],
        }
        total_rows += len(serialized_rows)
    return {
        "levels": levels_payload,
        "selectedLevels": selected_levels,
        "rowCount": total_rows,
        "status": status_text,
        "message": message,
        "range": {
            "startId": start_id,
            "endId": end_id,
        },
        "jobStateLastTickId": diagnostics["jobState"]["lastTickId"],
        "jobStateLastTime": serialize_value(diagnostics["jobState"]["lastTime"]),
        "state": serialize_zig_state_payload(state_row),
    }


def build_zig_next_payload(after_id: int, end_id: Optional[int], selected_levels: List[str]) -> Dict[str, Any]:
    status_text, message, diagnostics, state_row = summarize_zig_status(end_id)
    levels_payload = {}
    total_rows = 0
    for level in selected_levels:
        rows = fetch_level_rows_after_confirm(TICK_SYMBOL, level, after_id, end_id=end_id)
        serialized_rows = [serialize_zig_row(row, level) for row in rows]
        levels_payload[level] = {
            "rows": serialized_rows,
            "rowCount": len(serialized_rows),
            "latestConfirmTickId": diagnostics["levels"][level]["lastConfirmTickId"],
        }
        total_rows += len(serialized_rows)
    return {
        "levels": levels_payload,
        "selectedLevels": selected_levels,
        "rowCount": total_rows,
        "status": status_text,
        "message": message,
        "afterId": after_id,
        "endId": end_id,
        "jobStateLastTickId": diagnostics["jobState"]["lastTickId"],
        "jobStateLastTime": serialize_value(diagnostics["jobState"]["lastTime"]),
        "state": serialize_zig_state_payload(state_row),
    }


def resolve_range_preset(rangepreset: str) -> Dict[str, Any]:
    if rangepreset != "lastweek":
        raise HTTPException(status_code=400, detail="Unsupported OTT backtest range preset.")
    try:
        return resolve_last_week_range(TICK_SYMBOL)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def serialize_backtest_run(run: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if run is None:
        return None
    return {
        "id": run["id"],
        "symbol": run["symbol"],
        "source": run["source"],
        "matype": run["matype"],
        "length": run["length"],
        "percent": run["percent"],
        "signalmode": run["signalmode"],
        "starttickid": run["starttickid"],
        "endtickid": run["endtickid"],
        "startts": serialize_value(run["startts"]),
        "endts": serialize_value(run["endts"]),
        "tradecount": run["tradecount"],
        "grosspnl": run["grosspnl"],
        "netpnl": run["netpnl"],
        "createdat": serialize_value(run["createdat"]),
        "reused": bool(run.get("reused")),
    }


def serialize_backtest_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": trade["id"],
        "runid": trade["runid"],
        "entrytickid": trade["entrytickid"],
        "exittickid": trade["exittickid"],
        "entryts": serialize_value(trade["entryts"]),
        "entryTsMs": dt_to_ms(trade["entryts"]),
        "exitts": serialize_value(trade["exitts"]),
        "exitTsMs": dt_to_ms(trade["exitts"]),
        "direction": trade["direction"],
        "entryprice": trade["entryprice"],
        "exitprice": trade["exitprice"],
        "pnl": trade["pnl"],
        "pnlpoints": trade["pnlpoints"],
        "barsorticksheld": trade["barsorticksheld"],
        "signalentrytype": trade["signalentrytype"],
        "signalexittype": trade["signalexittype"],
        "createdat": serialize_value(trade["createdat"]),
    }


def summarize_range_status(
    *,
    signalmode: str,
    signal_counts: Optional[Dict[str, Any]],
    trade_count: Optional[int] = None,
    scope_label: str,
) -> Tuple[str, Optional[str]]:
    if signal_counts and int(signal_counts.get("totalCount") or 0) == 0:
        return ("no-signals", "No {0} signals were found in the {1}.".format(signalmode.lower(), scope_label))
    if trade_count is not None and int(trade_count) == 0:
        return ("no-trades", "No backtest trades were found in the {0}.".format(scope_label))
    return ("ok", None)


def fetch_tick_bounds() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    MIN(id) AS first_id,
                    MAX(id) AS last_id,
                    MIN(timestamp) AS first_timestamp,
                    MAX(timestamp) AS last_timestamp
                FROM public.ticks
                WHERE symbol = %s
                """,
                (TICK_SYMBOL,),
            )
            row = dict(cur.fetchone() or {})
    return {
        "firstId": row.get("first_id"),
        "lastId": row.get("last_id"),
        "firstTimestamp": row.get("first_timestamp"),
        "lastTimestamp": row.get("last_timestamp"),
    }


def fetch_bootstrap_rows(
    mode: str,
    start_id: Optional[int],
    window: int,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    window = clamp_int(window, 1, MAX_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if mode == "live":
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM (
                        SELECT id, symbol, timestamp, bid, ask, mid, spread
                        FROM public.ticks
                        WHERE symbol = %s
                        ORDER BY id DESC
                        LIMIT %s
                    ) recent
                    ORDER BY id ASC
                    """,
                    (TICK_SYMBOL, window),
                )
            else:
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                if end_id is None:
                    cur.execute(
                        """
                        SELECT id, symbol, timestamp, bid, ask, mid, spread,
                               COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                        FROM public.ticks
                        WHERE symbol = %s AND id >= %s
                        ORDER BY id ASC
                        LIMIT %s
                        """,
                        (TICK_SYMBOL, start_id, window),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, symbol, timestamp, bid, ask, mid, spread,
                               COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                        FROM public.ticks
                        WHERE symbol = %s AND id >= %s AND id <= %s
                        ORDER BY id ASC
                        LIMIT %s
                        """,
                        (TICK_SYMBOL, start_id, end_id, window),
                    )
            return [dict(row) for row in cur.fetchall()]


def fetch_rows_after(after_id: int, limit: int, end_id: Optional[int] = None) -> List[Dict[str, Any]]:
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if end_id is None:
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id > %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (TICK_SYMBOL, after_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT id, symbol, timestamp, bid, ask, mid, spread,
                           COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                    FROM public.ticks
                    WHERE symbol = %s AND id > %s AND id <= %s
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (TICK_SYMBOL, after_id, end_id, limit),
                )
            return [dict(row) for row in cur.fetchall()]


def fetch_window_ending_at(end_id: int, window: int) -> List[Dict[str, Any]]:
    window = clamp_int(window, 1, MAX_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, symbol, timestamp, bid, ask, mid, spread,
                       COALESCE(mid, ROUND(((bid + ask) / 2.0)::numeric, 2)::double precision) AS price
                FROM (
                    SELECT id, symbol, timestamp, bid, ask, mid, spread
                    FROM public.ticks
                    WHERE symbol = %s AND id <= %s
                    ORDER BY id DESC
                    LIMIT %s
                ) recent
                ORDER BY id ASC
                """,
                (TICK_SYMBOL, end_id, window),
            )
            return [dict(row) for row in cur.fetchall()]


def parse_review_timestamp(raw_value: str, timezone_name: str) -> datetime:
    try:
        target_tz = ZoneInfo(timezone_name or DEFAULT_REVIEW_TIMEZONE)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=400, detail="Unsupported review timezone.") from exc

    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid review timestamp.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=target_tz)
    else:
        parsed = parsed.astimezone(target_tz)
    return parsed.astimezone(timezone.utc)


def resolve_tick_at_timestamp(timestamp_value: datetime) -> Dict[str, Any]:
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
                (TICK_SYMBOL, timestamp_value),
            )
            next_row = cur.fetchone()
            cur.execute(
                """
                SELECT id, timestamp
                FROM public.ticks
                WHERE symbol = %s AND timestamp < %s
                ORDER BY timestamp DESC, id DESC
                LIMIT 1
                """,
                (TICK_SYMBOL, timestamp_value),
            )
            previous_row = cur.fetchone()

    if not previous_row and not next_row:
        raise HTTPException(status_code=404, detail="No ticks are available for review.")

    if previous_row and next_row:
        previous_delta = abs((timestamp_value - previous_row["timestamp"]).total_seconds())
        next_delta = abs((next_row["timestamp"] - timestamp_value).total_seconds())
        resolved = next_row if next_delta < previous_delta else previous_row
    else:
        resolved = next_row or previous_row

    return {
        "id": int(resolved["id"]),
        "timestamp": resolved["timestamp"],
    }


def is_system_schema(schema_name: str) -> bool:
    return schema_name in SYSTEM_SCHEMAS or schema_name.startswith(SYSTEM_SCHEMA_PREFIXES)


def quote_identifier(identifier: str) -> str:
    return '"{0}"'.format(identifier.replace('"', '""'))


def qualified_name(schema_name: str, object_name: str) -> str:
    return "{0}.{1}".format(quote_identifier(schema_name), quote_identifier(object_name))


def split_sql_script(sql_text: str) -> List[str]:
    text = (sql_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="SQL text is required.")
    return [statement.strip() for statement in sqlparse.split(text) if statement.strip()]


def statement_head(statement: str) -> str:
    parsed = sqlparse.parse(statement)
    if not parsed:
        return ""

    for token in parsed[0].tokens:
        if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
            continue
        normalized = token.normalized.upper().strip()
        if normalized:
            return normalized.split(None, 1)[0]
    return ""


def has_explicit_transaction_control(statements: List[str]) -> bool:
    return any(statement_head(statement) in TRANSACTION_CONTROL_HEADS for statement in statements)


def describe_columns(description: Any) -> List[Dict[str, Any]]:
    if not description:
        return []
    return [
        {
            "name": item.name,
            "typeCode": item.type_code,
        }
        for item in description
    ]


def fetch_result_rows(cur: Any, max_rows: int = MAX_QUERY_ROWS) -> Tuple[List[List[Any]], bool]:
    rows = cur.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    rows = rows[:max_rows]
    return [[serialize_value(value) for value in row] for row in rows], truncated


def line_column_from_position(statement: str, position: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not position:
        return None, None

    try:
        absolute_position = max(int(position) - 1, 0)
    except (TypeError, ValueError):
        return None, None

    prefix = statement[:absolute_position]
    line = prefix.count("\n") + 1
    column = absolute_position - prefix.rfind("\n")
    return line, column


def fetch_sql_context(conn: Any) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                current_database() AS database_name,
                current_schema() AS current_schema,
                current_user AS current_user,
                current_setting('server_version') AS server_version
            """
        )
        row = cur.fetchone() or {}
    return {
        "database": row.get("database_name"),
        "currentSchema": row.get("current_schema"),
        "currentUser": row.get("current_user"),
        "serverVersion": row.get("server_version"),
    }


def serialize_pg_error(exc: Exception, statement: Optional[str] = None, statement_index: Optional[int] = None) -> Dict[str, Any]:
    diag = getattr(exc, "diag", None)
    position = getattr(diag, "statement_position", None) if diag else None
    line, column = line_column_from_position(statement or "", position)
    return {
        "message": getattr(diag, "message_primary", None) or str(exc),
        "detail": getattr(diag, "message_detail", None) if diag else None,
        "hint": getattr(diag, "message_hint", None) if diag else None,
        "context": getattr(diag, "context", None) if diag else None,
        "position": int(position) if position and str(position).isdigit() else None,
        "line": line,
        "column": column,
        "sqlstate": getattr(exc, "pgcode", None),
        "schema": getattr(diag, "schema_name", None) if diag else None,
        "table": getattr(diag, "table_name", None) if diag else None,
        "columnName": getattr(diag, "column_name", None) if diag else None,
        "constraint": getattr(diag, "constraint_name", None) if diag else None,
        "statementIndex": statement_index,
        "statement": statement,
    }


def build_statement_result(conn: Any, cur: Any, statement: str, statement_index: int, started: float, notices_start: int) -> Dict[str, Any]:
    columns = describe_columns(cur.description)
    rows: List[List[Any]] = []
    truncated = False
    if columns:
        rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS)

    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    command_tag = cur.statusmessage or statement_head(statement)
    statement_type = command_tag.split()[0].upper() if command_tag else statement_head(statement)
    return {
        "index": statement_index,
        "statement": statement,
        "statementType": statement_type,
        "commandTag": command_tag,
        "rowCount": max(cur.rowcount, 0) if cur.rowcount is not None else None,
        "elapsedMs": elapsed_ms,
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
        "maxRows": MAX_QUERY_ROWS,
        "hasResultSet": bool(columns),
        "notices": conn.notices[notices_start:],
    }


def schema_payload() -> Dict[str, Any]:
    schema_map: Dict[str, Dict[str, Any]] = {}
    object_lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    kind_map = {
        "r": ("tables", "table"),
        "p": ("tables", "table"),
        "v": ("views", "view"),
        "m": ("materializedViews", "materialized_view"),
        "S": ("sequences", "sequence"),
    }

    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS object_name,
                    c.relkind,
                    COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s
                  ON s.relid = c.oid
                WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                ORDER BY n.nspname, c.relkind, c.relname
                """
            )
            for row in cur.fetchall():
                schema_name = row["schema_name"]
                schema_entry = schema_map.setdefault(
                    schema_name,
                    {
                        "schema": schema_name,
                        "counts": {
                            "tables": 0,
                            "views": 0,
                            "materializedViews": 0,
                            "sequences": 0,
                            "functions": 0,
                        },
                        "objects": {
                            "tables": [],
                            "views": [],
                            "materializedViews": [],
                            "sequences": [],
                            "functions": [],
                        },
                    },
                )
                list_name, object_kind = kind_map[row["relkind"]]
                entry = {
                    "name": row["object_name"],
                    "schema": schema_name,
                    "kind": object_kind,
                    "rowEstimate": row["row_estimate"],
                    "columns": [],
                    "indexes": [],
                }
                schema_entry["objects"][list_name].append(entry)
                schema_entry["counts"][list_name] += 1
                object_lookup[(schema_name, row["object_name"], object_kind)] = entry

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS object_name,
                    c.relkind,
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    a.attnotnull AS not_null,
                    pg_get_expr(d.adbin, d.adrelid) AS default_value,
                    a.attidentity <> '' AS is_identity,
                    a.attgenerated <> '' AS is_generated,
                    a.attnum AS ordinal_position
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                JOIN pg_attribute a
                  ON a.attrelid = c.oid
                 AND a.attnum > 0
                 AND NOT a.attisdropped
                LEFT JOIN pg_attrdef d
                  ON d.adrelid = a.attrelid
                 AND d.adnum = a.attnum
                WHERE c.relkind IN ('r', 'p', 'v', 'm')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                ORDER BY n.nspname, c.relname, a.attnum
                """
            )
            for row in cur.fetchall():
                object_kind = kind_map[row["relkind"]][1]
                entry = object_lookup.get((row["schema_name"], row["object_name"], object_kind))
                if entry is None:
                    continue
                entry["columns"].append(
                    {
                        "name": row["column_name"],
                        "dataType": row["data_type"],
                        "notNull": row["not_null"],
                        "default": row["default_value"],
                        "isIdentity": row["is_identity"],
                        "isGenerated": row["is_generated"],
                    }
                )

            cur.execute(
                """
                SELECT
                    schemaname AS schema_name,
                    tablename AS object_name,
                    indexname AS index_name,
                    indexdef
                FROM pg_indexes
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                  AND schemaname NOT LIKE 'pg_toast%%'
                  AND schemaname NOT LIKE 'pg_temp_%%'
                ORDER BY schemaname, tablename, indexname
                """
            )
            for row in cur.fetchall():
                entry = object_lookup.get((row["schema_name"], row["object_name"], "table"))
                if entry is None:
                    entry = object_lookup.get((row["schema_name"], row["object_name"], "materialized_view"))
                if entry is None:
                    continue
                entry["indexes"].append(
                    {
                        "name": row["index_name"],
                        "definition": row["indexdef"],
                    }
                )

            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    p.proname AS function_name,
                    pg_get_function_identity_arguments(p.oid) AS arguments,
                    pg_get_function_result(p.oid) AS returns
                FROM pg_proc p
                JOIN pg_namespace n
                  ON n.oid = p.pronamespace
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%%'
                  AND n.nspname NOT LIKE 'pg_temp_%%'
                  AND p.prokind = 'f'
                ORDER BY n.nspname, p.proname, 3
                """
            )
            for row in cur.fetchall():
                schema_entry = schema_map.setdefault(
                    row["schema_name"],
                    {
                        "schema": row["schema_name"],
                        "counts": {
                            "tables": 0,
                            "views": 0,
                            "materializedViews": 0,
                            "sequences": 0,
                            "functions": 0,
                        },
                        "objects": {
                            "tables": [],
                            "views": [],
                            "materializedViews": [],
                            "sequences": [],
                            "functions": [],
                        },
                    },
                )
                schema_entry["objects"]["functions"].append(
                    {
                        "name": row["function_name"],
                        "schema": row["schema_name"],
                        "kind": "function",
                        "signature": row["arguments"],
                        "returns": row["returns"],
                    }
                )
                schema_entry["counts"]["functions"] += 1

    schemas = list(schema_map.values())
    total_objects = sum(
        entry["counts"]["tables"]
        + entry["counts"]["views"]
        + entry["counts"]["materializedViews"]
        + entry["counts"]["sequences"]
        + entry["counts"]["functions"]
        for entry in schemas
    )
    return {
        "context": context,
        "schemas": schemas,
        "schemaCount": len(schemas),
        "objectCount": total_objects,
    }


def relation_columns(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                a.attnotnull AS not_null,
                pg_get_expr(d.adbin, d.adrelid) AS default_value,
                a.attidentity <> '' AS is_identity,
                a.attgenerated <> '' AS is_generated,
                a.attnum AS ordinal_position
            FROM pg_class c
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            JOIN pg_attribute a
              ON a.attrelid = c.oid
             AND a.attnum > 0
             AND NOT a.attisdropped
            LEFT JOIN pg_attrdef d
              ON d.adrelid = a.attrelid
             AND d.adnum = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
            ORDER BY a.attnum
            """,
            (schema_name, object_name),
        )
        return [
            {
                "name": row["column_name"],
                "dataType": row["data_type"],
                "notNull": row["not_null"],
                "default": row["default_value"],
                "isIdentity": row["is_identity"],
                "isGenerated": row["is_generated"],
            }
            for row in cur.fetchall()
        ]


def table_indexes(conn: Any, schema_name: str, object_name: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                indexname AS index_name,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s
              AND tablename = %s
            ORDER BY indexname
            """,
            (schema_name, object_name),
        )
        return [
            {
                "name": row["index_name"],
                "definition": row["indexdef"],
            }
            for row in cur.fetchall()
        ]


def load_object_details(schema_name: str, object_name: str, object_kind: str) -> Dict[str, Any]:
    if is_system_schema(schema_name):
        raise HTTPException(status_code=404, detail="Object not found.")

    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        if object_kind == "function":
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        p.oid,
                        p.proname,
                        pg_get_function_identity_arguments(p.oid) AS arguments,
                        pg_get_function_result(p.oid) AS returns,
                        pg_get_functiondef(p.oid) AS definition
                    FROM pg_proc p
                    JOIN pg_namespace n
                      ON n.oid = p.pronamespace
                    WHERE n.nspname = %s
                      AND p.proname = %s
                      AND p.prokind = 'f'
                    ORDER BY 3
                    """,
                    (schema_name, object_name),
                )
                overloads = cur.fetchall()
            if not overloads:
                raise HTTPException(status_code=404, detail="Function not found.")
            return {
                "context": context,
                "object": {
                    "schema": schema_name,
                    "name": object_name,
                    "kind": "function",
                    "overloads": [dict(row) for row in overloads],
                },
                "actions": {},
            }

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    c.oid,
                    c.relkind,
                    COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate,
                    pg_total_relation_size(c.oid) AS total_bytes,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                    obj_description(c.oid, 'pg_class') AS comment
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s
                  ON s.relid = c.oid
                WHERE n.nspname = %s
                  AND c.relname = %s
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
                """,
                (schema_name, object_name),
            )
            object_row = cur.fetchone()

        if not object_row:
            raise HTTPException(status_code=404, detail="Object not found.")

        relkind = object_row["relkind"]
        kind_map = {
            "r": "table",
            "p": "table",
            "v": "view",
            "m": "materialized_view",
            "S": "sequence",
        }
        actual_kind = kind_map[relkind]
        if object_kind != actual_kind:
            raise HTTPException(status_code=404, detail="Object not found.")

        columns = relation_columns(conn, schema_name, object_name) if actual_kind != "sequence" else []
        indexes = table_indexes(conn, schema_name, object_name) if actual_kind in {"table", "materialized_view"} else []
        definition = None
        sequence_state = None

        if actual_kind in {"view", "materialized_view"}:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_get_viewdef(%s, true)", (object_row["oid"],))
                row = cur.fetchone()
                definition = row[0] if row else None
        elif actual_kind == "sequence":
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        start_value,
                        minimum_value,
                        maximum_value,
                        increment,
                        cycle_option,
                        cache_size,
                        last_value
                    FROM pg_sequences
                    WHERE schemaname = %s
                      AND sequencename = %s
                    """,
                    (schema_name, object_name),
                )
                sequence_state = cur.fetchone()

        qualified = qualified_name(schema_name, object_name)
        actions = {
            "insertSelect": "SELECT *\nFROM {0}\nLIMIT 100;".format(qualified),
            "insertPreview": "SELECT *\nFROM {0}\nORDER BY 1\nLIMIT 100;".format(qualified),
            "insertExplain": "EXPLAIN ANALYZE\nSELECT *\nFROM {0}\nLIMIT 100;".format(qualified),
        }

        return {
            "context": context,
            "object": {
                "schema": schema_name,
                "name": object_name,
                "kind": actual_kind,
                "rowEstimate": object_row["row_estimate"],
                "totalBytes": object_row["total_bytes"],
                "totalSize": object_row["total_size"],
                "comment": object_row["comment"],
                "columns": columns,
                "indexes": indexes,
                "definition": definition,
                "sequence": dict(sequence_state) if sequence_state else None,
            },
            "actions": actions,
        }


def preview_relation(
    schema_name: str,
    object_name: str,
    limit: int,
    offset: int,
    order_by: Optional[str],
    order_dir: str,
) -> Dict[str, Any]:
    if is_system_schema(schema_name):
        raise HTTPException(status_code=404, detail="Relation not found.")

    started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relname = %s
                  AND c.relkind IN ('r', 'p', 'v', 'm')
                """,
                (schema_name, object_name),
            )
            relation_row = cur.fetchone()
        if not relation_row:
            raise HTTPException(status_code=404, detail="Preview is only available for tables and views.")

        columns = relation_columns(conn, schema_name, object_name)
        column_names = {column["name"] for column in columns}
        if order_by and order_by not in column_names:
            raise HTTPException(status_code=400, detail="Unknown sort column: {0}".format(order_by))

        relation_sql = pg_sql.SQL(".").join([pg_sql.Identifier(schema_name), pg_sql.Identifier(object_name)])
        query = pg_sql.SQL("SELECT * FROM {}").format(relation_sql)
        if order_by:
            direction_sql = pg_sql.SQL("DESC") if order_dir.lower() == "desc" else pg_sql.SQL("ASC")
            query += pg_sql.SQL(" ORDER BY {} {}").format(pg_sql.Identifier(order_by), direction_sql)
        query += pg_sql.SQL(" LIMIT %s OFFSET %s")

        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
            cur.execute(query, (limit + 1, offset))
            result_columns = describe_columns(cur.description)
            rows, truncated = fetch_result_rows(cur, limit)

        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
        return {
            "context": context,
            "result": {
                "index": 1,
                "title": "{0}.{1}".format(schema_name, object_name),
                "statement": "preview",
                "statementType": "SELECT",
                "commandTag": "SELECT",
                "rowCount": len(rows),
                "elapsedMs": elapsed_ms,
                "columns": result_columns,
                "rows": rows,
                "truncated": truncated,
                "maxRows": limit,
                "hasResultSet": True,
                "source": {
                    "schema": schema_name,
                    "name": object_name,
                    "kind": "preview",
                    "orderBy": order_by,
                    "orderDir": order_dir.lower(),
                    "offset": offset,
                    "limit": limit,
                },
            },
        }


def execute_query(sql_text: str) -> Dict[str, Any]:
    statements = split_sql_script(sql_text)
    started = time.perf_counter()
    explicit_transactions = has_explicit_transaction_control(statements)
    results: List[Dict[str, Any]] = []

    with db_connection(readonly=False, autocommit=explicit_transactions) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = %s", (str(STATEMENT_TIMEOUT_MS),))
                cur.execute("SET lock_timeout = %s", (str(LOCK_TIMEOUT_MS),))
                cur.execute("SET idle_in_transaction_session_timeout = '5000'")

                for index, statement in enumerate(statements, start=1):
                    notices_start = len(conn.notices)
                    statement_started = time.perf_counter()
                    cur.execute(statement)
                    results.append(
                        build_statement_result(
                            conn=conn,
                            cur=cur,
                            statement=statement,
                            statement_index=index,
                            started=statement_started,
                            notices_start=notices_start,
                        )
                    )

            if not explicit_transactions:
                conn.commit()

            context = fetch_sql_context(conn)
        except Exception as exc:
            if conn.status != pg_extensions.STATUS_READY:
                conn.rollback()
            raise HTTPException(
                status_code=400,
                detail=serialize_pg_error(
                    exc,
                    statement=statements[len(results)] if len(results) < len(statements) else None,
                    statement_index=len(results) + 1 if len(results) < len(statements) else None,
                ),
            ) from exc

    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    return {
        "success": True,
        "statementCount": len(statements),
        "transactionMode": "explicit" if explicit_transactions else "script",
        "elapsedMs": elapsed_ms,
        "context": context,
        "results": results,
    }


def stream_events(after_id: int, limit: int) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()

    try:
        while True:
            rows = fetch_rows_after(last_id, limit)
            if rows:
                payload_rows = [serialize_tick_row(row) for row in rows]
                last_id = payload_rows[-1]["id"]
                yield "data: {0}\n\n".format(
                    json.dumps(
                        {
                            "rows": payload_rows,
                            "lastId": last_id,
                            "rowCount": len(payload_rows),
                        }
                    )
                )
                last_heartbeat = time.monotonic()
            else:
                now = time.monotonic()
                if now - last_heartbeat >= STREAM_KEEPALIVE_SECONDS:
                    yield ": keepalive\n\n"
                    last_heartbeat = now
                time.sleep(STREAM_POLL_SECONDS)
    except GeneratorExit:
        return


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/sql", include_in_schema=False)
def sql_page(_: str = Depends(require_sql_admin)) -> FileResponse:
    return FileResponse(FRONTEND_DIR / "sql.html")


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT MAX(id) AS last_id, MAX(timestamp) AS last_timestamp
                FROM public.ticks
                WHERE symbol = %s
                """,
                (TICK_SYMBOL,),
            )
            row = cur.fetchone() or {}
    return {
        "ok": True,
        "service": "datavis",
        "symbol": TICK_SYMBOL,
        "lastId": row.get("last_id"),
        "lastTimestamp": row.get("last_timestamp"),
    }


@app.get("/api/live/review-start")
def live_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    requested_ts = parse_review_timestamp(timestamp, timezoneName)
    resolved = resolve_tick_at_timestamp(requested_ts)
    resolved_local = resolved["timestamp"].astimezone(ZoneInfo(timezoneName))
    requested_local = requested_ts.astimezone(ZoneInfo(timezoneName))
    return {
        "symbol": TICK_SYMBOL,
        "timezone": timezoneName,
        "requestedTimestamp": requested_ts.isoformat(),
        "requestedLocal": requested_local.isoformat(),
        "resolvedId": resolved["id"],
        "resolvedTimestamp": resolved["timestamp"].isoformat(),
        "resolvedLocal": resolved_local.isoformat(),
    }


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
) -> Dict[str, Any]:
    review_bounds = fetch_tick_bounds() if mode == "review" else None
    review_end_id = review_bounds["lastId"] if review_bounds else None
    review_end_timestamp = review_bounds["lastTimestamp"] if review_bounds else None
    rows = [serialize_tick_row(row) for row in fetch_bootstrap_rows(mode, id, window, end_id=review_end_id)]
    last_row_id = rows[-1]["id"] if rows else None
    return {
        "rows": rows,
        "rowCount": len(rows),
        "firstId": rows[0]["id"] if rows else None,
        "lastId": last_row_id,
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "priceColumn": "mid",
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
    }


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    rows = [serialize_tick_row(row) for row in fetch_rows_after(afterId, limit, end_id=endId)]
    last_seen_id = rows[-1]["id"] if rows else afterId
    return {
        "rows": rows,
        "rowCount": len(rows),
        "lastId": last_seen_id,
        "endId": endId,
        "endReached": bool(endId is not None and last_seen_id >= endId),
    }


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(afterId, limit),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/ott/bootstrap")
def ott_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
    endId: Optional[int] = Query(None, ge=1),
    source: str = Query(DEFAULT_OTT_SOURCE, pattern="^(ask|bid|mid)$"),
    signalmode: str = Query(DEFAULT_OTT_SIGNAL_MODE, pattern="^(support|price|color)$"),
    matype: str = Query(DEFAULT_OTT_MA_TYPE, pattern="^(SMA|EMA|WMA|TMA|VAR|WWMA|ZLEMA|TSF)$"),
    length: int = Query(DEFAULT_OTT_LENGTH, ge=1, le=10000),
    percent: float = Query(DEFAULT_OTT_PERCENT, ge=0),
) -> Dict[str, Any]:
    config = build_ott_config(source, matype, length, percent)
    try:
        rows = fetch_bootstrap_tick_rows(TICK_SYMBOL, mode, id, window, end_id=endId)
        return build_ott_response(rows, config, mode=mode, requested_start_id=id, signalmode=signalmode)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="OTT bootstrap failed: {0}".format(exc)) from exc


@app.get("/api/ott/next")
def ott_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    source: str = Query(DEFAULT_OTT_SOURCE, pattern="^(ask|bid|mid)$"),
    signalmode: str = Query(DEFAULT_OTT_SIGNAL_MODE, pattern="^(support|price|color)$"),
    matype: str = Query(DEFAULT_OTT_MA_TYPE, pattern="^(SMA|EMA|WMA|TMA|VAR|WWMA|ZLEMA|TSF)$"),
    length: int = Query(DEFAULT_OTT_LENGTH, ge=1, le=10000),
    percent: float = Query(DEFAULT_OTT_PERCENT, ge=0),
) -> Dict[str, Any]:
    config = build_ott_config(source, matype, length, percent)
    try:
        rows = fetch_next_tick_rows(TICK_SYMBOL, afterId, limit, end_id=endId)
        return build_ott_response(rows, config, advanced_from_id=afterId, signalmode=signalmode)
    except Exception as exc:
        raise HTTPException(status_code=500, detail="OTT incremental fetch failed: {0}".format(exc)) from exc


@app.get("/api/envelope/bootstrap")
def envelope_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
    endId: Optional[int] = Query(None, ge=1),
    source: str = Query(DEFAULT_ENVELOPE_SOURCE, pattern="^(ask|bid|mid)$"),
    length: int = Query(DEFAULT_ENVELOPE_LENGTH, ge=1, le=10000),
    bandwidth: float = Query(DEFAULT_ENVELOPE_BANDWIDTH, gt=0),
    mult: float = Query(DEFAULT_ENVELOPE_MULT, ge=0),
) -> Dict[str, Any]:
    config = build_envelope_config(source, length, bandwidth, mult)
    try:
        rows = fetch_bootstrap_tick_rows(TICK_SYMBOL, mode, id, window, end_id=endId)
        return build_envelope_response(rows, config, mode=mode, requested_start_id=id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Envelope bootstrap failed: {0}".format(exc)) from exc


@app.get("/api/envelope/next")
def envelope_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    source: str = Query(DEFAULT_ENVELOPE_SOURCE, pattern="^(ask|bid|mid)$"),
    length: int = Query(DEFAULT_ENVELOPE_LENGTH, ge=1, le=10000),
    bandwidth: float = Query(DEFAULT_ENVELOPE_BANDWIDTH, gt=0),
    mult: float = Query(DEFAULT_ENVELOPE_MULT, ge=0),
) -> Dict[str, Any]:
    config = build_envelope_config(source, length, bandwidth, mult)
    try:
        rows = fetch_next_tick_rows(TICK_SYMBOL, afterId, limit, end_id=endId)
        return build_envelope_response(rows, config, advanced_from_id=afterId)
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Envelope incremental fetch failed: {0}".format(exc)) from exc


@app.get("/api/zig/window")
def zig_window(
    startId: Optional[int] = Query(None, ge=1),
    endId: Optional[int] = Query(None, ge=1),
    startTime: str = Query("", min_length=0),
    endTime: str = Query("", min_length=0),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_WINDOW),
    levels: str = Query(",".join(ZIG_LEVELS), min_length=1),
) -> Dict[str, Any]:
    selected_levels = normalize_zig_levels(levels)
    try:
        if startId is not None or endId is not None:
            range_info = resolve_backfill_range(
                TICK_SYMBOL,
                start_id=startId,
                end_id=endId,
                start_time=None,
                end_time=None,
            )
            return build_zig_window_payload(int(range_info["starttickid"]), int(range_info["endtickid"]), selected_levels)

        if startTime or endTime:
            range_info = resolve_backfill_range(
                TICK_SYMBOL,
                start_id=None,
                end_id=None,
                start_time=parse_optional_timestamp(startTime, timezoneName),
                end_time=parse_optional_timestamp(endTime, timezoneName),
            )
            return build_zig_window_payload(int(range_info["starttickid"]), int(range_info["endtickid"]), selected_levels)

        rows = fetch_bootstrap_rows("live", None, window)
        if not rows:
            raise HTTPException(status_code=404, detail="No ticks are available for the requested Zig window.")
        return build_zig_window_payload(int(rows[0]["id"]), int(rows[-1]["id"]), selected_levels)
    except HTTPException:
        raise
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Zig window fetch failed: {0}".format(exc)) from exc


@app.get("/api/zig/next")
def zig_next(
    afterId: int = Query(..., ge=0),
    endId: Optional[int] = Query(None, ge=1),
    levels: str = Query(",".join(ZIG_LEVELS), min_length=1),
) -> Dict[str, Any]:
    selected_levels = normalize_zig_levels(levels)
    try:
        return build_zig_next_payload(afterId, endId, selected_levels)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Zig incremental fetch failed: {0}".format(exc)) from exc


@app.post("/api/ott/backtest/run")
def ott_backtest_run(payload: OttBacktestRunRequest) -> Dict[str, Any]:
    config = build_ott_config(payload.source, payload.matype, payload.length, payload.percent)
    range_info = resolve_range_preset(payload.rangepreset)
    try:
        run = run_and_store_backtest(
            symbol=TICK_SYMBOL,
            config=config,
            signalmode=payload.signalmode,
            start_tick_id=range_info["starttickid"],
            end_tick_id=range_info["endtickid"],
            force=payload.force,
        )
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="OTT backtest run failed: {0}".format(exc)) from exc

    diagnostics = fetch_ott_sync_diagnostics(
        TICK_SYMBOL,
        config,
        requested_start_tick_id=range_info["starttickid"],
        requested_end_tick_id=range_info["endtickid"],
        signalmode=payload.signalmode,
    )
    status_text, message = summarize_range_status(
        signalmode=payload.signalmode,
        signal_counts=diagnostics.get("signalCounts"),
        trade_count=int(run["tradecount"]),
        scope_label="selected backtest range",
    )

    return {
        "run": serialize_backtest_run(run),
        "cached": bool(run.get("reused")),
        "rangePreset": payload.rangepreset,
        "range": range_info,
        "status": status_text,
        "message": message,
        "signalCounts": diagnostics.get("signalCounts"),
        "coverage": diagnostics,
    }


@app.get("/api/ott/backtest/overlay")
def ott_backtest_overlay(
    source: str = Query(DEFAULT_OTT_SOURCE, pattern="^(ask|bid|mid)$"),
    matype: str = Query(DEFAULT_OTT_MA_TYPE, pattern="^(SMA|EMA|WMA|TMA|VAR|WWMA|ZLEMA|TSF)$"),
    length: int = Query(DEFAULT_OTT_LENGTH, ge=1, le=10000),
    percent: float = Query(DEFAULT_OTT_PERCENT, ge=0),
    signalmode: str = Query(DEFAULT_OTT_SIGNAL_MODE, pattern="^(support|price|color)$"),
    rangePreset: str = Query("lastweek", pattern="^(lastweek)$"),
    startId: Optional[int] = Query(None, ge=1),
    endId: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    config = build_ott_config(source, matype, length, percent)
    range_info = resolve_range_preset(rangePreset)
    try:
        overlay = fetch_backtest_overlay(
            symbol=TICK_SYMBOL,
            config=config,
            signalmode=signalmode,
            run_start_tick_id=range_info["starttickid"],
            run_end_tick_id=range_info["endtickid"],
            visible_start_tick_id=startId,
            visible_end_tick_id=endId,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="OTT backtest overlay failed: {0}".format(exc)) from exc
    visible_start_tick_id = startId if startId is not None else range_info["starttickid"]
    visible_end_tick_id = endId if endId is not None else range_info["endtickid"]
    diagnostics = fetch_ott_sync_diagnostics(
        TICK_SYMBOL,
        config,
        requested_start_tick_id=visible_start_tick_id,
        requested_end_tick_id=visible_end_tick_id,
        signalmode=signalmode,
    )
    status_text, message = summarize_range_status(
        signalmode=signalmode,
        signal_counts=diagnostics.get("signalCounts"),
        trade_count=int(overlay["tradecount"]),
        scope_label="selected review range",
    )
    if overlay["run"] is None:
        status_text = "not-cached"
        message = "No cached backtest yet. Click Run Backtest."
    return {
        "run": serialize_backtest_run(overlay["run"]),
        "trades": [serialize_backtest_trade(trade) for trade in overlay["trades"]],
        "tradeCount": overlay["tradecount"],
        "cached": overlay["run"] is not None,
        "rangePreset": rangePreset,
        "range": range_info,
        "status": status_text,
        "message": message,
        "signalCounts": diagnostics.get("signalCounts"),
        "coverage": diagnostics,
    }


@app.get("/api/sql/schema")
def sql_schema(_: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return schema_payload()


@app.get("/api/sql/object")
def sql_object(
    schema: str = Query(..., min_length=1),
    name: str = Query(..., min_length=1),
    kind: str = Query(..., pattern="^(table|view|materialized_view|sequence|function)$"),
    _: str = Depends(require_sql_admin),
) -> Dict[str, Any]:
    return load_object_details(schema, name, kind)


@app.get("/api/sql/table-preview")
def sql_table_preview(
    schema: str = Query(..., min_length=1),
    name: str = Query(..., min_length=1),
    limit: int = Query(DEFAULT_SQL_PREVIEW_LIMIT, ge=1, le=MAX_SQL_PREVIEW_LIMIT),
    offset: int = Query(0, ge=0),
    orderBy: Optional[str] = Query(None, min_length=1),
    orderDir: str = Query("asc", pattern="^(asc|desc)$"),
    _: str = Depends(require_sql_admin),
) -> Dict[str, Any]:
    return preview_relation(schema, name, limit, offset, orderBy, orderDir)


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: str = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)
