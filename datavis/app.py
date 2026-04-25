#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import csv
import re
import secrets
import base64
import hmac
import hashlib
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
import sqlparse
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from datavis.backbone import BACKBONE_SOURCE
from datavis.backbone import BIGBONES_SOURCE
from datavis.backbone import load_state_row as load_backbone_state_row
from datavis.backbone import resolve_current_day_ref as resolve_current_backbone_day_ref
from datavis.backbone import resolve_day_ref_for_timestamp as resolve_backbone_day_ref_for_timestamp
from datavis.db import db_connect as shared_db_connect
from datavis.mavg import list_page_config_rows as list_mavg_config_rows
from datavis.mavg import query_point_rows_after_value_id as query_mavg_points_after_value_id
from datavis.mavg import query_point_rows_for_tick_range as query_mavg_points_for_tick_range
from datavis.mavg import query_point_rows_for_time_range as query_mavg_points_for_time_range
from datavis.rects import RectPaperService, RectServiceError
from datavis.smart_scalp import SmartScalpError, SmartScalpService
from datavis.structure import StructureEngine, replay_ticks
from datavis.trading import CTraderGateway, load_broker_config

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
ASSETS_DIR = FRONTEND_DIR / "assets"

load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://", 1)

TICK_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD")
DEFAULT_WINDOW = int(os.getenv("DATAVIS_WINDOW", "2000"))
MAX_TICK_WINDOW = int(os.getenv("DATAVIS_MAX_WINDOW", "10000"))
DEFAULT_BACKBONE_REVIEW_WINDOW = int(os.getenv("DATAVIS_BACKBONE_REVIEW_WINDOW", "1200"))
MAX_BACKBONE_REVIEW_WINDOW = int(os.getenv("DATAVIS_BACKBONE_REVIEW_MAX_WINDOW", "6000"))
DEFAULT_BACKBONE_CANDLE_COUNT = int(os.getenv("DATAVIS_BACKBONE_CANDLE_COUNT", "35"))
MAX_BACKBONE_CANDLE_COUNT = int(os.getenv("DATAVIS_BACKBONE_MAX_CANDLES", "400"))
DEFAULT_BACKBONE_DETAIL_TICKS = int(os.getenv("DATAVIS_BACKBONE_DETAIL_TICKS", "2000"))
DEFAULT_BACKBONE_LAYER = "backbone"
DEFAULT_HISTORY_LIMIT = 2000
DEFAULT_BIGPICTURE_POINTS = 2000
MAX_BIGPICTURE_POINTS = int(os.getenv("DATAVIS_BIGPICTURE_MAX_POINTS", "2400"))
MAX_STREAM_BATCH = 1000
MAX_QUERY_ROWS = int(os.getenv("DATAVIS_SQL_MAX_ROWS", "1000"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
SQL_EXPORT_DIR = BASE_DIR / "logs" / "sql_exports"
SQL_EXPORT_BATCH_SIZE = max(1000, int(os.getenv("DATAVIS_SQL_EXPORT_BATCH_SIZE", "5000")))
SQL_EXPORT_MAX_ROWS = max(1, int(os.getenv("DATAVIS_SQL_EXPORT_MAX_ROWS", "500000")))
SQL_EXPORT_TIMEOUT_MS = max(STATEMENT_TIMEOUT_MS, int(os.getenv("DATAVIS_SQL_EXPORT_TIMEOUT_MS", "60000")))
SQL_EXPORT_LOG_EVERY_ROWS = max(SQL_EXPORT_BATCH_SIZE, int(os.getenv("DATAVIS_SQL_EXPORT_LOG_EVERY_ROWS", "50000")))
SQL_ADMIN_USER = os.getenv("DATAVIS_SQL_ADMIN_USER", "").strip()
SQL_ADMIN_PASSWORD = os.getenv("DATAVIS_SQL_ADMIN_PASSWORD", "")
STREAM_POLL_SECONDS = max(0.02, float(os.getenv("DATAVIS_STREAM_POLL_SECONDS", "0.05")))
STREAM_IDLE_POLL_SECONDS = max(
    STREAM_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_IDLE_POLL_SECONDS", "0.10")),
)
STREAM_HEARTBEAT_SECONDS = max(
    STREAM_IDLE_POLL_SECONDS,
    float(os.getenv("DATAVIS_STREAM_HEARTBEAT_SECONDS", "5.0")),
)
REVIEW_STREAM_BATCH = int(os.getenv("DATAVIS_REVIEW_STREAM_BATCH", "256"))
REVIEW_MAX_DELAY_MS = max(50, int(os.getenv("DATAVIS_REVIEW_MAX_DELAY_MS", "1500")))
REVIEW_MIN_DELAY_MS = max(0, int(os.getenv("DATAVIS_REVIEW_MIN_DELAY_MS", "5")))
DEFAULT_REVIEW_TIMEZONE = "Australia/Sydney"
TRADE_USERNAME = os.getenv("DATAVIS_TRADE_USERNAME", "babak").strip() or "babak"
TRADE_PASSWORD = os.getenv("DATAVIS_TRADE_PASSWORD", "")
TRADE_COOKIE_NAME = os.getenv("DATAVIS_TRADE_COOKIE_NAME", "datavis_trade_session").strip() or "datavis_trade_session"
TRADE_SESSION_TTL_SECONDS = max(300, int(os.getenv("DATAVIS_TRADE_SESSION_TTL_SECONDS", "43200")))
TRADE_SESSION_SECRET = os.getenv("DATAVIS_TRADE_SESSION_SECRET", "").encode("utf-8")
TRADE_COOKIE_SECURE = os.getenv("DATAVIS_TRADE_COOKIE_SECURE", "0").strip().lower() in {"1", "true", "yes", "on"}
TRADE_HISTORY_DEFAULT_LIMIT = 40
TRADE_HISTORY_MAX_LIMIT = 120
TRADE_DEFAULT_LOT_SIZE = Decimal("0.01")

app = FastAPI(
    title="datavis.au",
    version="3.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
security = HTTPBasic(auto_error=False)
RUNTIME_TRADE_SESSION_SECRET = TRADE_SESSION_SECRET or secrets.token_bytes(32)
BROKER_CONFIG = load_broker_config(BASE_DIR)
TRADE_GATEWAY = CTraderGateway(BROKER_CONFIG)
AUDIT_LOGGER = logging.getLogger("datavis.trade.audit")
PERF_LOGGER = logging.getLogger("datavis.perf")
STREAM_LOGGER = logging.getLogger("datavis.stream")
MAVG_LOGGER = logging.getLogger("datavis.mavg")
SQL_EXPORT_LOGGER = logging.getLogger("datavis.sql.export")
SQL_SCHEMA_CACHE_TTL_MS = max(1000, int(os.getenv("DATAVIS_SQL_SCHEMA_CACHE_MS", "30000")))
HOT_PATH_LOG_THRESHOLD_MS = max(5.0, float(os.getenv("DATAVIS_HOT_PATH_LOG_MS", "75")))
STREAM_ACTIVITY_LOCK = threading.Lock()
STREAM_ACTIVITY_COUNTS: Dict[str, int] = {}
SQL_SCHEMA_CACHE_LOCK = threading.Lock()
SQL_SCHEMA_CACHE: Dict[str, Any] = {"expiresAtMs": 0, "payload": None}
BACKBONE_LAYER_SOURCES = {
    "backbone": BACKBONE_SOURCE,
    "bigbones": BIGBONES_SOURCE,
}
BACKBONE_LAYER_LABELS = {
    "backbone": "Backbone",
    "bigbones": "BigBones",
}


class QueryRequest(BaseModel):
    sql: str


class QueryExportRequest(BaseModel):
    query: str
    filename: Optional[str] = Field(None, max_length=200)


class TradeLoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=1, max_length=512)


class TradeMarketOrderRequest(BaseModel):
    side: str = Field(..., min_length=3, max_length=4)
    volume: Optional[int] = Field(None, ge=1)
    lotSize: Optional[float] = Field(None, gt=0)
    stopLoss: Optional[float] = None
    takeProfit: Optional[float] = None

    @field_validator("side")
    @classmethod
    def validate_side(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        return normalized


class TradePositionCloseRequest(BaseModel):
    positionId: int = Field(..., ge=1)
    volume: int = Field(..., ge=1)


class TradePositionAmendRequest(BaseModel):
    positionId: int = Field(..., ge=1)
    stopLoss: Optional[float] = None
    takeProfit: Optional[float] = None
    clearStopLoss: bool = False
    clearTakeProfit: bool = False

    @field_validator("takeProfit", "stopLoss")
    @classmethod
    def validate_optional_price(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("price must be greater than 0")
        return float(value)


class TradeSmartContextRequest(BaseModel):
    page: str = Field("live", min_length=1, max_length=32)
    mode: str = Field("live", min_length=1, max_length=32)
    run: str = Field("stop", min_length=1, max_length=32)


class TradeSmartEntryArmRequest(BaseModel):
    side: str = Field(..., min_length=3, max_length=4)
    armed: bool = False

    @field_validator("side")
    @classmethod
    def validate_side(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        return normalized


class TradeSmartCloseArmRequest(BaseModel):
    armed: bool = False


class TradeSmartConfigRequest(BaseModel):
    showSummary: Optional[bool] = None
    entryBaselineWindow: Optional[int] = Field(None, ge=1)
    entryTriggerWindow: Optional[int] = Field(None, ge=1)
    entryTriggerThreshold: Optional[float] = Field(None, gt=0)
    entryVelocityThreshold: Optional[float] = Field(None, gt=0)
    entryMinMove: Optional[float] = Field(None, ge=0)
    entryMinDirectionRatio: Optional[float] = Field(None, ge=0, le=1)
    entryMaxSpreadFactor: Optional[float] = Field(None, gt=0)
    entryMinActiveRange: Optional[float] = Field(None, ge=0)
    closeBaselineWindow: Optional[int] = Field(None, ge=1)
    closeTriggerWindow: Optional[int] = Field(None, ge=1)
    closeWeakeningThreshold: Optional[float] = Field(None, gt=0)
    closeReversalThreshold: Optional[float] = Field(None, gt=0)
    closeMinPullback: Optional[float] = Field(None, ge=0)
    minimumProfit: Optional[float] = Field(None, gt=0)
    cooldownSeconds: Optional[int] = Field(None, ge=0)
    maxHoldSeconds: Optional[int] = Field(None, ge=0)


class RectCreateRequest(BaseModel):
    mode: str = Field("review", min_length=1, max_length=16)
    leftx: int = Field(..., ge=1)
    rightx: int = Field(..., ge=1)
    firstprice: float = Field(..., gt=0)
    secondprice: float = Field(..., gt=0)
    smartcloseenabled: bool = True
    metadata: Optional[Dict[str, Any]] = None

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"live", "review"}:
            raise ValueError("mode must be live or review")
        return normalized


class RectUpdateRequest(RectCreateRequest):
    pass


class RectSmartCloseRequest(BaseModel):
    mode: str = Field("review", min_length=1, max_length=16)
    enabled: bool = True

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"live", "review"}:
            raise ValueError("mode must be live or review")
        return normalized


class RectModeRequest(BaseModel):
    mode: str = Field("review", min_length=1, max_length=16)

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"live", "review"}:
            raise ValueError("mode must be live or review")
        return normalized

def ensure_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return DATABASE_URL


@contextmanager
def db_connection(readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
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


def now_ms() -> int:
    return int(time.time() * 1000)


def elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 2)


def hot_path_log(name: str, *, elapsed: float, **fields: Any) -> None:
    if elapsed < HOT_PATH_LOG_THRESHOLD_MS:
        return
    extra = " ".join(
        "{0}={1}".format(key, fields[key])
        for key in sorted(fields)
        if fields[key] is not None
    )
    PERF_LOGGER.info("%s elapsed_ms=%.2f%s", name, elapsed, (" " + extra) if extra else "")


def stream_open(name: str) -> int:
    with STREAM_ACTIVITY_LOCK:
        active = int(STREAM_ACTIVITY_COUNTS.get(name) or 0) + 1
        STREAM_ACTIVITY_COUNTS[name] = active
    STREAM_LOGGER.info("stream_open name=%s active=%s duplicate=%s", name, active, active > 1)
    return active


def stream_close(name: str) -> int:
    with STREAM_ACTIVITY_LOCK:
        active = max(0, int(STREAM_ACTIVITY_COUNTS.get(name) or 0) - 1)
        if active:
            STREAM_ACTIVITY_COUNTS[name] = active
        else:
            STREAM_ACTIVITY_COUNTS.pop(name, None)
    STREAM_LOGGER.info("stream_close name=%s active=%s", name, active)
    return active


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def ms_to_dt(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value.hex()
    return value


def normalize_backbone_layer(layer: Optional[str]) -> str:
    normalized = str(layer or DEFAULT_BACKBONE_LAYER).strip().lower()
    return normalized if normalized in BACKBONE_LAYER_SOURCES else DEFAULT_BACKBONE_LAYER


def serialize_tick_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["timestamp"]
    bid = float(row["bid"]) if row.get("bid") is not None else None
    ask = float(row["ask"]) if row.get("ask") is not None else None
    mid = float(row["mid"]) if row.get("mid") is not None else None
    if mid is None and bid is not None and ask is not None:
        mid = round((float(bid) + float(ask)) / 2.0, 5)
    spread = float(row["spread"]) if row.get("spread") is not None else None
    if spread is None and bid is not None and ask is not None:
        spread = round(float(ask) - float(bid), 5)
    return {
        "id": int(row["id"]),
        "symbol": row.get("symbol", TICK_SYMBOL),
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
    }


def serialize_tick_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_tick_row(row) for row in rows]


def serialize_mavg_config_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "name": str(row.get("name") or ""),
        "method": str(row.get("method") or ""),
        "source": str(row.get("source") or ""),
        "windowseconds": int(row.get("windowseconds") or 0),
        "showOnLive": bool(row.get("showonlive")),
        "showOnBig": bool(row.get("showonbig")),
        "color": row.get("color"),
    }


def serialize_mavg_point_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ticktime = row.get("ticktime")
    return {
        "valueId": int(row["id"]),
        "configId": int(row["configid"]),
        "tickId": int(row["tickid"]),
        "timestamp": serialize_value(ticktime),
        "timestampMs": dt_to_ms(ticktime),
        "value": float(row["value"]),
    }


def serialize_mavg_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_mavg_point_row(row) for row in rows]


def serialize_motion_signal_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tickid": int(row["tickid"]),
        "timestamp": serialize_value(row.get("timestamp")),
        "side": str(row.get("side") or ""),
        "mid": float(row["mid"]) if row.get("mid") is not None else None,
        "spread": float(row["spread"]) if row.get("spread") is not None else None,
        "velocity3": float(row["velocity3"]) if row.get("velocity3") is not None else None,
        "acceleration3": float(row["acceleration3"]) if row.get("acceleration3") is not None else None,
        "efficiency3": float(row["efficiency3"]) if row.get("efficiency3") is not None else None,
        "spreadmultiple3": float(row["spreadmultiple3"]) if row.get("spreadmultiple3") is not None else None,
        "velocity10": float(row["velocity10"]) if row.get("velocity10") is not None else None,
        "acceleration10": float(row["acceleration10"]) if row.get("acceleration10") is not None else None,
        "outcome": row.get("outcome"),
        "score": float(row["score"]) if row.get("score") is not None else None,
    }


def empty_mavg_payload() -> Dict[str, Any]:
    return {"mavgConfigs": [], "mavgPoints": [], "mavgCursorId": None}


def log_mavg_query_failure(operation: str, *, page: str, detail: str = "") -> None:
    suffix = " {0}".format(detail) if detail else ""
    MAVG_LOGGER.exception("Moving-average query failed: operation=%s page=%s%s", operation, page, suffix)


def safe_query_mavg_points_for_tick_range(
    cur: Any,
    *,
    page: str,
    start_id: int,
    end_id: int,
) -> List[Dict[str, Any]]:
    try:
        return query_mavg_points_for_tick_range(cur, page=page, start_id=start_id, end_id=end_id)
    except Exception:
        log_mavg_query_failure(
            "tick_range_rows",
            page=page,
            detail="start_id={0} end_id={1}".format(start_id, end_id),
        )
        return []


def mavg_payload_for_tick_range(
    cur: Any,
    *,
    page: str,
    start_id: Optional[int],
    end_id: Optional[int],
    include_configs: bool,
) -> Dict[str, Any]:
    empty_payload = empty_mavg_payload() if include_configs else {"mavgPoints": [], "mavgCursorId": None}
    if start_id is None or end_id is None or end_id < start_id:
        return empty_payload
    try:
        config_rows = list_mavg_config_rows(cur, page=page) if include_configs else []
        if include_configs and not config_rows:
            return empty_mavg_payload()
        point_rows = query_mavg_points_for_tick_range(cur, page=page, start_id=int(start_id), end_id=int(end_id))
        return {
            "mavgConfigs": [serialize_mavg_config_row(row) for row in config_rows] if include_configs else [],
            "mavgPoints": serialize_mavg_points(point_rows),
            "mavgCursorId": max((int(row["id"]) for row in point_rows), default=None),
        }
    except Exception:
        log_mavg_query_failure(
            "tick_range",
            page=page,
            detail="start_id={0} end_id={1}".format(start_id, end_id),
        )
        return empty_payload


def mavg_payload_for_time_range(
    cur: Any,
    *,
    page: str,
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    target_points: int,
) -> Dict[str, Any]:
    if start_ts is None or end_ts is None:
        return empty_mavg_payload()
    try:
        config_rows = list_mavg_config_rows(cur, page=page)
        if not config_rows:
            return empty_mavg_payload()
        point_rows = query_mavg_points_for_time_range(
            cur,
            page=page,
            start_ts=start_ts,
            end_ts=end_ts,
            target_points=target_points,
        )
        return {
            "mavgConfigs": [serialize_mavg_config_row(row) for row in config_rows],
            "mavgPoints": serialize_mavg_points(point_rows),
            "mavgCursorId": max((int(row["id"]) for row in point_rows), default=None),
        }
    except Exception:
        log_mavg_query_failure(
            "time_range",
            page=page,
            detail="start_ts={0} end_ts={1} target_points={2}".format(
                serialize_value(start_ts),
                serialize_value(end_ts),
                target_points,
            ),
        )
        return empty_mavg_payload()


def mavg_updates_payload(
    cur: Any,
    *,
    page: str,
    after_value_id: int,
) -> Dict[str, Any]:
    cursor_id = max(0, int(after_value_id or 0))
    try:
        point_rows = query_mavg_points_after_value_id(cur, page=page, after_value_id=cursor_id)
        return {
            "mavgPoints": serialize_mavg_points(point_rows),
            "mavgCursorId": max((int(row["id"]) for row in point_rows), default=cursor_id),
        }
    except Exception:
        log_mavg_query_failure("updates", page=page, detail="after_value_id={0}".format(cursor_id))
        return {"mavgPoints": [], "mavgCursorId": cursor_id}


def serialize_metrics_payload(
    *,
    fetch_ms: float,
    serialize_ms: float,
    latest_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "serverSentAtMs": now_ms(),
        "fetchLatencyMs": fetch_ms,
        "serializeLatencyMs": serialize_ms,
        "dbLatestId": latest_row.get("id") if latest_row else None,
        "dbLatestTimestamp": serialize_value(latest_row.get("timestamp")) if latest_row else None,
        "dbLatestTimestampMs": dt_to_ms(latest_row.get("timestamp")) if latest_row else None,
    }


def format_sse(payload: Dict[str, Any], *, event_name: Optional[str] = None) -> str:
    if event_name:
        return "event: {0}\ndata: {1}\n\n".format(event_name, json.dumps(payload))
    return "data: {0}\n\n".format(json.dumps(payload))


def require_sql_admin(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> Optional[str]:
    if not SQL_ADMIN_USER or not SQL_ADMIN_PASSWORD:
        return None

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


@app.exception_handler(HTTPException)
async def datavis_http_exception_handler(request: Request, exc: HTTPException):
    if request.url.path.startswith("/api/sql/export-csv"):
        detail = exc.detail
        if isinstance(detail, dict):
            message = detail.get("error") or detail.get("message") or "CSV export failed."
            content = {"ok": False, "error": message, "detail": detail}
        else:
            message = str(detail)
            content = {"ok": False, "error": message, "detail": message}
        return JSONResponse(status_code=exc.status_code, content=content, headers=exc.headers)
    return await http_exception_handler(request, exc)


def _trade_session_sign(raw_payload: str) -> str:
    return hmac.new(RUNTIME_TRADE_SESSION_SECRET, raw_payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _trade_session_encode(username: str) -> str:
    now_ts = int(time.time())
    payload_json = json.dumps(
        {"u": username, "iat": now_ts, "exp": now_ts + TRADE_SESSION_TTL_SECONDS},
        separators=(",", ":"),
    )
    payload = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8").rstrip("=")
    signature = _trade_session_sign(payload)
    return payload + "." + signature


def _trade_session_decode(token: str) -> Optional[Dict[str, Any]]:
    if not token or "." not in token:
        return None
    payload_b64, signature = token.rsplit(".", 1)
    expected = _trade_session_sign(payload_b64)
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))
    except json.JSONDecodeError:
        return None
    except Exception:
        return None
    exp = int(payload.get("exp") or 0)
    if exp <= int(time.time()):
        return None
    username = str(payload.get("u") or "").strip()
    if not username:
        return None
    return payload


def _set_trade_cookie(response: Response, username: str) -> None:
    response.set_cookie(
        key=TRADE_COOKIE_NAME,
        value=_trade_session_encode(username),
        max_age=TRADE_SESSION_TTL_SECONDS,
        httponly=True,
        secure=TRADE_COOKIE_SECURE,
        samesite="lax",
        path="/",
    )


def _clear_trade_cookie(response: Response) -> None:
    response.delete_cookie(
        key=TRADE_COOKIE_NAME,
        httponly=True,
        secure=TRADE_COOKIE_SECURE,
        samesite="lax",
        path="/",
    )


def require_trade_auth(request: Request) -> str:
    ensure_trade_login_configured()
    payload = _trade_session_decode(request.cookies.get(TRADE_COOKIE_NAME, ""))
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Trade login required.")
    SMART_SCALP_SERVICE.touch_auth()
    return str(payload["u"])


def trade_login_configured() -> bool:
    return bool(TRADE_PASSWORD)


def trade_auth_status_payload(
    *,
    authenticated: bool,
    username: Optional[str],
    error: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    auth_configured = trade_login_configured()
    resolved_message = message
    if not resolved_message:
        if not auth_configured:
            resolved_message = "Trade login is not configured on the server."
        elif authenticated:
            resolved_message = "Trade session active."
        else:
            resolved_message = "Trade login required."
    return {
        "authenticated": bool(authenticated and auth_configured),
        "username": username if authenticated and auth_configured else None,
        "authConfigured": auth_configured,
        "brokerConfigured": TRADE_GATEWAY.configured,
        "configured": TRADE_GATEWAY.configured,
        "broker": TRADE_GATEWAY.status(),
        "error": error,
        "message": resolved_message,
    }


def trade_auth_not_configured_response() -> JSONResponse:
    payload = trade_auth_status_payload(
        authenticated=False,
        username=None,
        error="TRADE_AUTH_NOT_CONFIGURED",
        message="Trade login is not configured on the server.",
    )
    payload["detail"] = payload["message"]
    return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=payload)


def ensure_trade_login_configured() -> None:
    if not trade_login_configured():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Trade login is not configured on the server.",
        )


def trade_symbol_info() -> Dict[str, Any]:
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        info = TRADE_GATEWAY.symbol_info()
    except Exception as exc:
        _handle_trade_gateway_error(exc)
    info["defaultLotSize"] = float(TRADE_DEFAULT_LOT_SIZE)
    return info


def trade_volume_from_request(payload: TradeMarketOrderRequest) -> int:
    if payload.lotSize is not None:
        symbol_info = trade_symbol_info()
        lot_size_units = int(symbol_info.get("lotSize") or 0)
        step_volume = int(symbol_info.get("stepVolume") or 1)
        min_volume = int(symbol_info.get("minVolume") or 1)
        if lot_size_units <= 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Broker lot-size mapping is unavailable.")
        lots = Decimal(str(payload.lotSize))
        broker_volume = int((lots * Decimal(lot_size_units)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        if broker_volume < min_volume:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Lot size must be at least {symbol_info.get('minLotSize') or float(TRADE_DEFAULT_LOT_SIZE):g}.",
            )
        if step_volume > 1 and broker_volume % step_volume != 0:
            step_lot = symbol_info.get("lotStep")
            step_hint = f" in {step_lot:g} lot steps" if isinstance(step_lot, (int, float)) and step_lot > 0 else ""
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Lot size is not aligned to broker volume increments{step_hint}.",
            )
        return broker_volume
    if payload.volume is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="lotSize or volume is required.")
    return int(payload.volume)


def describe_columns(description: Any) -> List[Dict[str, Any]]:
    if not description:
        return []
    return [{"name": item.name, "typeCode": item.type_code} for item in description]


def fetch_result_rows(cur: Any, limit: int) -> tuple[List[List[Any]], bool]:
    fetched = cur.fetchmany(limit + 1)
    truncated = len(fetched) > limit
    rows = fetched[:limit]
    return [[serialize_value(value) for value in row] for row in rows], truncated


def split_sql_script(sql_text: str) -> List[str]:
    text = (sql_text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="SQL text is required.")
    return [statement.strip() for statement in sqlparse.split(text) if statement.strip()]


def statement_head(statement: str) -> str:
    parsed = sqlparse.parse(statement)
    if not parsed:
        return "SQL"
    for token in parsed[0].tokens:
        if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
            continue
        normalized = token.normalized.upper().strip()
        if normalized:
            return normalized.split(None, 1)[0]
    return "SQL"


def line_column_from_position(sql_text: str, position: Optional[Any]) -> tuple[Optional[int], Optional[int]]:
    if not position or not str(position).isdigit():
        return None, None
    absolute_position = max(1, int(position))
    prefix = sql_text[: absolute_position - 1]
    line = prefix.count("\n") + 1
    column = absolute_position - prefix.rfind("\n")
    return line, column


def serialize_pg_error(exc: Exception, statement: Optional[str] = None) -> Dict[str, Any]:
    diag = getattr(exc, "diag", None)
    position = getattr(diag, "statement_position", None) if diag else None
    line, column = line_column_from_position(statement or "", position)
    return {
        "message": getattr(diag, "message_primary", None) or str(exc),
        "detail": getattr(diag, "message_detail", None) if diag else None,
        "hint": getattr(diag, "message_hint", None) if diag else None,
        "position": int(position) if position and str(position).isdigit() else None,
        "line": line,
        "column": column,
        "sqlstate": getattr(exc, "pgcode", None),
        "statement": statement,
    }


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


def list_sql_tables() -> Dict[str, Any]:
    now = now_ms()
    with SQL_SCHEMA_CACHE_LOCK:
        cached = SQL_SCHEMA_CACHE.get("payload")
        expires_at = int(SQL_SCHEMA_CACHE.get("expiresAtMs") or 0)
        if cached is not None and now < expires_at:
            return json.loads(json.dumps(cached))

    started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        context = fetch_sql_context(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    n.nspname AS schema_name,
                    c.relname AS table_name,
                    CASE c.relkind WHEN 'p' THEN 'partitioned table' ELSE 'table' END AS kind,
                    COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0)::bigint AS row_estimate,
                    EXISTS (
                        SELECT 1
                        FROM pg_attribute a
                        WHERE a.attrelid = c.oid
                          AND a.attname = 'id'
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                    ) AS has_id
                FROM pg_class c
                JOIN pg_namespace n
                  ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s
                  ON s.relid = c.oid
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                ORDER BY n.nspname ASC, c.relname ASC
                """
            )
            tables = [
                {
                    "schema": row["schema_name"],
                    "name": row["table_name"],
                    "kind": row["kind"],
                    "rowEstimate": int(row["row_estimate"] or 0),
                    "hasId": bool(row["has_id"]),
                }
                for row in cur.fetchall()
            ]
    public_tables = [table for table in tables if table["schema"] == "public"]
    payload = {
        "context": context,
        "tables": tables,
        "public": public_tables,
    }
    with SQL_SCHEMA_CACHE_LOCK:
        SQL_SCHEMA_CACHE["payload"] = payload
        SQL_SCHEMA_CACHE["expiresAtMs"] = now_ms() + SQL_SCHEMA_CACHE_TTL_MS
    hot_path_log(
        "sql_schema",
        elapsed=elapsed_ms(started),
        public_count=len(public_tables),
        table_count=len(tables),
    )
    return json.loads(json.dumps(payload))


def execute_query(sql_text: str) -> Dict[str, Any]:
    statements = split_sql_script(sql_text)
    started = time.perf_counter()
    active_statement: Optional[str] = None

    with db_connection(readonly=False, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (STATEMENT_TIMEOUT_MS,))
                cur.execute("SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT_MS,))
                results = []
                for index, statement in enumerate(statements, start=1):
                    active_statement = statement
                    statement_started = time.perf_counter()
                    cur.execute(statement)
                    has_result_set = cur.description is not None
                    rows, truncated = fetch_result_rows(cur, MAX_QUERY_ROWS) if has_result_set else ([], False)
                    results.append(
                        {
                            "index": index,
                            "statement": statement,
                            "statementType": statement_head(statement),
                            "commandTag": getattr(cur, "statusmessage", None) or statement_head(statement),
                            "rowCount": len(rows) if has_result_set else max(0, cur.rowcount),
                            "elapsedMs": elapsed_ms(statement_started),
                            "columns": describe_columns(cur.description),
                            "rows": rows,
                            "truncated": truncated,
                            "maxRows": MAX_QUERY_ROWS,
                            "hasResultSet": has_result_set,
                        }
                    )
            context = fetch_sql_context(conn)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise HTTPException(
                status_code=400,
                detail=serialize_pg_error(exc, statement=active_statement),
            ) from exc

    payload = {
        "success": True,
        "statementCount": len(statements),
        "elapsedMs": elapsed_ms(started),
        "context": context,
        "results": results,
    }
    with SQL_SCHEMA_CACHE_LOCK:
        SQL_SCHEMA_CACHE["expiresAtMs"] = 0
        SQL_SCHEMA_CACHE["payload"] = None
    hot_path_log(
        "sql_query",
        elapsed=float(payload["elapsedMs"]),
        statement_count=len(statements),
        result_sets=sum(1 for item in results if item.get("hasResultSet")),
        rows_returned=sum(int(item.get("rowCount") or 0) for item in results if item.get("hasResultSet")),
    )
    return payload


def require_exportable_select_statement(sql_text: str) -> str:
    statements = split_sql_script(sql_text)
    if len(statements) != 1:
        raise HTTPException(status_code=400, detail="CSV export only supports a single SELECT or WITH query.")

    statement = statements[0].strip()
    parsed = sqlparse.parse(statement)
    if not parsed:
        raise HTTPException(status_code=400, detail="SQL text is required.")

    statement_type = str(parsed[0].get_type() or "").upper()
    head = statement_head(statement).upper()
    if head not in {"SELECT", "WITH"} or statement_type != "SELECT":
        raise HTTPException(status_code=400, detail="CSV export only allows read-only SELECT or WITH queries.")
    return statement


def default_sql_export_filename() -> str:
    return datetime.now().strftime("sql_export_%Y%m%d_%H%M%S.csv")


def sanitize_sql_export_filename(filename: Optional[str]) -> str:
    raw = str(filename or "").strip()
    if not raw:
        raw = default_sql_export_filename()
    raw = raw.replace("\\", "/").rsplit("/", 1)[-1]
    if raw.lower().endswith(".csv"):
        raw = raw[:-4]
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
    if not safe_stem:
        safe_stem = default_sql_export_filename()[:-4]
    safe_stem = safe_stem[:120].rstrip("._-") or default_sql_export_filename()[:-4]
    return safe_stem + ".csv"


def csv_export_column_names(description: Any) -> List[str]:
    names: List[str] = []
    for index, item in enumerate(description or [], start=1):
        name = getattr(item, "name", None)
        if not name and isinstance(item, (list, tuple)) and item:
            name = item[0]
        names.append(str(name or f"column_{index}"))
    return names


def csv_export_row_values(row: Any) -> List[Any]:
    return [serialize_value(value) for value in row]


def resolve_sql_export_target(filename: Optional[str]) -> tuple[str, Path, str]:
    exports_dir = SQL_EXPORT_DIR.resolve()
    exports_dir.mkdir(parents=True, exist_ok=True)
    supplied_name = bool(str(filename or "").strip())
    safe_name = sanitize_sql_export_filename(filename)
    export_path = (exports_dir / safe_name).resolve()
    if not supplied_name and export_path.exists():
        stem = export_path.stem
        suffix = export_path.suffix or ".csv"
        counter = 1
        while export_path.exists():
            safe_name = "{0}_{1:02d}{2}".format(stem, counter, suffix)
            export_path = (exports_dir / safe_name).resolve()
            counter += 1
    try:
        export_path.relative_to(exports_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid CSV export filename.") from exc
    relative_path = (Path("logs") / "sql_exports" / safe_name).as_posix()
    return safe_name, export_path, relative_path


def resolve_sql_export_download(filename: str) -> tuple[str, Path]:
    requested = str(filename or "").strip()
    if not requested:
        raise HTTPException(status_code=400, detail="Export filename is required.")
    safe_name = sanitize_sql_export_filename(requested)
    if safe_name != requested:
        raise HTTPException(status_code=400, detail="Invalid export filename.")
    export_path = (SQL_EXPORT_DIR.resolve() / safe_name).resolve()
    try:
        export_path.relative_to(SQL_EXPORT_DIR.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid export filename.") from exc
    return safe_name, export_path


def remove_sql_export_file(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        SQL_EXPORT_LOGGER.warning("sql_export_cleanup_failed path=%s", path, exc_info=True)


def export_query_to_csv(sql_text: str, filename: Optional[str] = None) -> Dict[str, Any]:
    statement = require_exportable_select_statement(sql_text)
    SQL_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name, export_path, relative_path = resolve_sql_export_target(filename)
    download_url = "/api/sql/export-csv/{0}".format(safe_name)
    started = time.perf_counter()
    row_count = 0
    export_sql = f"SELECT * FROM ({statement}) AS sql_export_source LIMIT %s"
    next_progress_log_at = SQL_EXPORT_LOG_EVERY_ROWS
    active_stage = "initializing"
    SQL_EXPORT_LOGGER.info(
        "sql_export_started filename=%s path=%s batch_size=%s max_rows=%s timeout_ms=%s",
        safe_name,
        relative_path,
        SQL_EXPORT_BATCH_SIZE,
        SQL_EXPORT_MAX_ROWS,
        SQL_EXPORT_TIMEOUT_MS,
    )

    with db_connection(readonly=True, autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                active_stage = "set_timeouts"
                cur.execute("SET LOCAL statement_timeout = %s", (SQL_EXPORT_TIMEOUT_MS,))
                cur.execute("SET LOCAL lock_timeout = %s", (LOCK_TIMEOUT_MS,))
                active_stage = "execute_query"
                cur.execute(export_sql, (SQL_EXPORT_MAX_ROWS + 1,))
                if cur.description is None:
                    raise HTTPException(status_code=400, detail="CSV export query did not return a result set.")
                column_names = csv_export_column_names(cur.description)
                SQL_EXPORT_LOGGER.info(
                    "sql_export_query_ready filename=%s path=%s columns=%s",
                    safe_name,
                    relative_path,
                    len(column_names),
                )
                active_stage = "open_output"
                with export_path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    active_stage = "write_header"
                    writer.writerow(column_names)
                    while True:
                        active_stage = "fetch_rows"
                        batch = cur.fetchmany(SQL_EXPORT_BATCH_SIZE)
                        if not batch:
                            break
                        batch_size = len(batch)
                        if row_count + batch_size > SQL_EXPORT_MAX_ROWS:
                            allowed = max(0, SQL_EXPORT_MAX_ROWS - row_count)
                            if allowed > 0:
                                active_stage = "write_rows"
                                writer.writerows(csv_export_row_values(row) for row in batch[:allowed])
                                row_count += allowed
                            raise HTTPException(
                                status_code=400,
                                detail=(
                                    "CSV export exceeded the configured row limit of {0}. "
                                    "Refine the query or raise DATAVIS_SQL_EXPORT_MAX_ROWS."
                                ).format(SQL_EXPORT_MAX_ROWS),
                            )
                        active_stage = "write_rows"
                        writer.writerows(csv_export_row_values(row) for row in batch)
                        row_count += batch_size
                        while row_count >= next_progress_log_at:
                            SQL_EXPORT_LOGGER.info(
                                "sql_export_progress filename=%s path=%s rows=%s",
                                safe_name,
                                relative_path,
                                next_progress_log_at,
                            )
                            next_progress_log_at += SQL_EXPORT_LOG_EVERY_ROWS
            conn.commit()
        except HTTPException as exc:
            conn.rollback()
            remove_sql_export_file(export_path)
            SQL_EXPORT_LOGGER.warning(
                "sql_export_failed filename=%s path=%s rows=%s stage=%s reason=%s",
                safe_name,
                relative_path,
                row_count,
                active_stage,
                exc.detail,
            )
            raise
        except Exception as exc:
            conn.rollback()
            remove_sql_export_file(export_path)
            SQL_EXPORT_LOGGER.exception(
                "sql_export_failed filename=%s path=%s rows=%s stage=%s",
                safe_name,
                relative_path,
                row_count,
                active_stage,
            )
            if isinstance(exc, psycopg2.Error):
                detail = serialize_pg_error(exc, statement=statement)
                detail["stage"] = active_stage
                detail["filename"] = safe_name
                detail["path"] = relative_path
                raise HTTPException(
                    status_code=400,
                    detail=detail,
                ) from exc
            raise HTTPException(
                status_code=500,
                detail={
                    "message": str(exc) or exc.__class__.__name__,
                    "exceptionType": exc.__class__.__name__,
                    "statement": statement,
                    "stage": active_stage,
                    "filename": safe_name,
                    "path": relative_path,
                },
            ) from exc

    payload = {
        "ok": True,
        "filename": safe_name,
        "path": relative_path,
        "rows": row_count,
        "download_url": download_url,
    }
    SQL_EXPORT_LOGGER.info(
        "sql_export_completed filename=%s path=%s rows=%s elapsed_ms=%.2f",
        safe_name,
        relative_path,
        row_count,
        elapsed_ms(started),
    )
    return payload


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


def tick_columns() -> str:
    return "id, symbol, timestamp, bid, ask, mid, spread"


def query_tick_bounds(cur: Any) -> Dict[str, Any]:
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
    return dict(cur.fetchone() or {})


def query_latest_tick(cur: Any) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, timestamp
        FROM public.ticks
        WHERE symbol = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (TICK_SYMBOL,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def query_bootstrap_rows(
    cur: Any,
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    end_id: Optional[int],
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    if mode == "live":
        cur.execute(
            """
            SELECT {select_sql}
            FROM (
                SELECT {select_sql}
                FROM public.ticks
                WHERE symbol = %s
                ORDER BY id DESC
                LIMIT %s
            ) recent
            ORDER BY id ASC
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, window),
        )
        return [dict(row) for row in cur.fetchall()]

    if start_id is None:
        raise HTTPException(status_code=400, detail="Review mode requires an id value.")
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id >= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_id, window),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id >= %s AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_id, end_id, window),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_after(
    cur: Any,
    after_id: int,
    limit: int,
    *,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id > %s AND id <= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_rows_before(cur: Any, before_id: int, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id < %s
            ORDER BY id DESC
            LIMIT %s
        ) older
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, before_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_window_ending_at(cur: Any, cursor_id: int, window: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s AND id <= %s
            ORDER BY id DESC
            LIMIT %s
        ) recent
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, cursor_id, window),
    )
    return [dict(row) for row in cur.fetchall()]


def query_rows_between(cur: Any, start_id: int, end_id: int, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.ticks
        WHERE symbol = %s AND id >= %s AND id <= %s
        ORDER BY id ASC
        LIMIT %s
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, start_id, end_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_tick_by_id(cur: Any, tick_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM public.ticks
        WHERE symbol = %s AND id = %s
        LIMIT 1
        """,
        (TICK_SYMBOL, tick_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def query_rows_between_timestamps(
    cur: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.ticks
        WHERE symbol = %s
          AND timestamp >= %s
          AND timestamp <= %s
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, start_ts, end_ts),
    )
    return [dict(row) for row in cur.fetchall()]


def query_window_ending_at_timestamp(
    cur: Any,
    *,
    end_ts: datetime,
    seconds: int,
) -> List[Dict[str, Any]]:
    return query_rows_between_timestamps(
        cur,
        start_ts=end_ts - timedelta(seconds=max(1, int(seconds))),
        end_ts=end_ts,
    )


def query_ticks_in_time_range(
    cur: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    if limit is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY id ASC
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_ts, end_ts),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, start_ts, end_ts, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_tick_range_bounds_for_time(
    cur: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            MIN(id) AS first_id,
            MAX(id) AS last_id,
            MIN(timestamp) AS first_timestamp,
            MAX(timestamp) AS last_timestamp
        FROM public.ticks
        WHERE symbol = %s
          AND timestamp >= %s
          AND timestamp <= %s
        """,
        (TICK_SYMBOL, start_ts, end_ts),
    )
    return dict(cur.fetchone() or {})


def query_bigpicture_rows(
    cur: Any,
    *,
    start_ts: datetime,
    end_ts: datetime,
    target_points: int,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    normalized_target = clamp_int(target_points, 200, MAX_BIGPICTURE_POINTS)
    if end_ts <= start_ts:
        end_ts = start_ts + timedelta(milliseconds=1)
    range_bounds = query_tick_range_bounds_for_time(cur, start_ts=start_ts, end_ts=end_ts)
    row_count = int(range_bounds.get("row_count") or 0)
    if row_count <= 0:
        return [], range_bounds
    if row_count <= normalized_target:
        return query_ticks_in_time_range(cur, start_ts=start_ts, end_ts=end_ts), range_bounds

    bucket_count = max(1, normalized_target // 4)
    select_sql = tick_columns()
    cur.execute(
        """
        WITH params AS (
            SELECT
                %s::timestamptz AS start_ts,
                %s::timestamptz AS end_ts,
                %s::int AS bucket_count
        ),
        ranked AS (
            SELECT
                {select_sql},
                LEAST(
                    p.bucket_count,
                    GREATEST(
                        1,
                        width_bucket(
                            EXTRACT(EPOCH FROM t.timestamp),
                            EXTRACT(EPOCH FROM p.start_ts),
                            EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                            p.bucket_count
                        )
                    )
                ) AS bucket,
                row_number() OVER (
                    PARTITION BY LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM t.timestamp),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY t.timestamp ASC, t.id ASC
                ) AS rn_first,
                row_number() OVER (
                    PARTITION BY LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM t.timestamp),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY t.timestamp DESC, t.id DESC
                ) AS rn_last,
                row_number() OVER (
                    PARTITION BY LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM t.timestamp),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY t.mid ASC NULLS LAST, t.timestamp ASC, t.id ASC
                ) AS rn_low,
                row_number() OVER (
                    PARTITION BY LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM t.timestamp),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY t.mid DESC NULLS LAST, t.timestamp ASC, t.id ASC
                ) AS rn_high
            FROM public.ticks t
            CROSS JOIN params p
            WHERE t.symbol = %s
              AND t.timestamp >= p.start_ts
              AND t.timestamp <= p.end_ts
        )
        SELECT id, symbol, timestamp, bid, ask, mid, spread
        FROM ranked
        WHERE rn_first = 1 OR rn_last = 1 OR rn_low = 1 OR rn_high = 1
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (start_ts, end_ts, bucket_count, TICK_SYMBOL),
    )
    return [dict(row) for row in cur.fetchall()], range_bounds


def build_bigpicture_payload(
    *,
    rows: List[Dict[str, Any]],
    requested_start_ts: Optional[datetime],
    requested_end_ts: Optional[datetime],
    source_bounds: Dict[str, Any],
    global_bounds: Dict[str, Any],
    fetch_ms: float,
    mavg_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    first_row = rows[0] if rows else None
    last_row = rows[-1] if rows else None
    payload = {
        "symbol": TICK_SYMBOL,
        "points": serialize_tick_rows(rows),
        "returnedPointCount": len(rows),
        "sourceRowCount": int(source_bounds.get("row_count") or len(rows)),
        "requestedStartTs": serialize_value(requested_start_ts),
        "requestedEndTs": serialize_value(requested_end_ts),
        "requestedStartTsMs": dt_to_ms(requested_start_ts),
        "requestedEndTsMs": dt_to_ms(requested_end_ts),
        "actualStartTs": serialize_value(first_row.get("timestamp") if first_row else None),
        "actualEndTs": serialize_value(last_row.get("timestamp") if last_row else None),
        "actualStartTsMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "actualEndTsMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "firstId": first_row.get("id") if first_row else None,
        "lastId": last_row.get("id") if last_row else None,
        "hasMoreLeft": bool(global_bounds.get("firstId") and first_row and int(first_row["id"]) > int(global_bounds["firstId"])),
        "hasMoreRight": bool(global_bounds.get("lastId") and last_row and int(last_row["id"]) < int(global_bounds["lastId"])),
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
        latest_row=last_row,
    )
    payload.update(mavg_payload or empty_mavg_payload())
    return payload


def load_bigpicture_bootstrap_payload(points: int) -> Dict[str, Any]:
    target_points = clamp_int(points, 200, MAX_BIGPICTURE_POINTS)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            global_bounds = query_tick_bounds(cur)
            rows = query_bootstrap_rows(cur, mode="live", start_id=None, window=target_points, end_id=None)
            mavg_payload = mavg_payload_for_time_range(
                cur,
                page="big",
                start_ts=rows[0]["timestamp"] if rows else None,
                end_ts=rows[-1]["timestamp"] if rows else None,
                target_points=target_points,
            )
    return build_bigpicture_payload(
        rows=rows,
        requested_start_ts=rows[0]["timestamp"] if rows else None,
        requested_end_ts=rows[-1]["timestamp"] if rows else None,
        source_bounds={"row_count": len(rows)},
        global_bounds=global_bounds,
        fetch_ms=elapsed_ms(fetch_started),
        mavg_payload=mavg_payload,
    )


def load_bigpicture_window_payload(
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    points: int,
) -> Dict[str, Any]:
    start_ts = ms_to_dt(start_ts_ms)
    end_ts = ms_to_dt(end_ts_ms)
    if start_ts is None or end_ts is None:
        raise HTTPException(status_code=400, detail="Invalid big picture range.")
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            global_bounds = query_tick_bounds(cur)
            rows, source_bounds = query_bigpicture_rows(
                cur,
                start_ts=start_ts,
                end_ts=end_ts,
                target_points=points,
            )
            mavg_payload = mavg_payload_for_time_range(
                cur,
                page="big",
                start_ts=start_ts,
                end_ts=end_ts,
                target_points=points,
            )
    if not rows:
        raise HTTPException(status_code=404, detail="No ticks were found for the requested big picture range.")
    return build_bigpicture_payload(
        rows=rows,
        requested_start_ts=start_ts,
        requested_end_ts=end_ts,
        source_bounds=source_bounds,
        global_bounds=global_bounds,
        fetch_ms=elapsed_ms(fetch_started),
        mavg_payload=mavg_payload,
    )


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

    return {"id": int(resolved["id"]), "timestamp": resolved["timestamp"]}


def empty_structure_payload() -> Dict[str, Any]:
    return {
        "structureBars": [],
        "rangeBoxes": [],
        "structureEvents": [],
    }


def structure_snapshot(rows: List[Dict[str, Any]], *, enabled: bool) -> Dict[str, Any]:
    if not enabled or not rows:
        return empty_structure_payload()
    try:
        return replay_ticks(TICK_SYMBOL, rows)
    except Exception:
        return empty_structure_payload()


def apply_structure_flags(payload: Dict[str, Any], *, show_events: bool, show_structure: bool, show_ranges: bool) -> Dict[str, Any]:
    if not show_events:
        payload["structureEvents"] = []
    if not show_structure:
        payload["structureBars"] = []
    if not show_ranges:
        payload["rangeBoxes"] = []
    return payload


def rect_snapshot_for_mode(mode: str) -> Optional[Dict[str, Any]]:
    try:
        return RECT_PAPER_SERVICE.current_rect(mode)
    except Exception:
        return None


def build_range_payload(
    *,
    mode: str,
    window: int,
    rows: List[Dict[str, Any]],
    replay_rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
    mavg_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_row = replay_rows[0] if replay_rows else None
    last_row = replay_rows[-1] if replay_rows else None
    first_row_id = first_row["id"] if first_row else None
    last_row_id = last_row["id"] if last_row else None
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload = {
        "rows": serialize_tick_rows(rows) if show_ticks else [],
        "rowCount": len(rows) if show_ticks else 0,
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        **snapshot,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=last_row,
    )
    payload.update(mavg_payload or empty_mavg_payload())
    return payload


def load_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
                "firstTimestamp": bounds_row.get("first_timestamp"),
                "lastTimestamp": bounds_row.get("last_timestamp"),
            }
            review_end_id = bounds["lastId"] if mode == "review" else None
            review_end_timestamp = bounds["lastTimestamp"] if mode == "review" else None
            rows = query_bootstrap_rows(
                cur,
                mode=mode,
                start_id=start_id,
                window=effective_window,
                end_id=review_end_id,
            )
            replay_first_id = int(rows[0]["id"]) if rows else None
            replay_last_id = int(rows[-1]["id"]) if rows else None
            mavg_payload = mavg_payload_for_tick_range(
                cur,
                page="live",
                start_id=replay_first_id,
                end_id=replay_last_id,
                include_configs=True,
            )
    payload = build_range_payload(
        mode=mode,
        window=effective_window,
        rows=rows,
        replay_rows=rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_ticks=show_ticks,
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
        mavg_payload=mavg_payload,
    )
    payload["rect"] = rect_snapshot_for_mode(mode)
    return payload


def load_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id)
            last_seen_id = int(tick_rows[-1]["id"]) if tick_rows else after_id
            replay_rows = (
                query_window_ending_at(cur, last_seen_id, effective_window)
                if (show_events or show_structure or show_ranges) and last_seen_id
                else []
            )
            mavg_payload = mavg_payload_for_tick_range(
                cur,
                page="live",
                start_id=(after_id + 1) if tick_rows else None,
                end_id=last_seen_id if tick_rows else None,
                include_configs=False,
            )
    serialize_started = time.perf_counter()
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload = {
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "lastId": last_seen_id,
        "endId": end_id,
        "endReached": bool(end_id is not None and last_seen_id >= end_id),
        **snapshot,
        **mavg_payload,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=tick_rows[-1] if tick_rows else None,
        ),
    }
    payload["rect"] = rect_snapshot_for_mode("review" if end_id is not None else "live")
    return payload


def load_previous_payload(
    *,
    before_id: int,
    current_last_id: Optional[int],
    limit: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            bounds_row = query_tick_bounds(cur)
            bounds = {"firstId": bounds_row.get("first_id"), "lastId": bounds_row.get("last_id")}
            previous_rows = query_rows_before(cur, before_id, effective_limit)
            first_row = previous_rows[0] if previous_rows else None
            range_end_id = current_last_id or (previous_rows[-1]["id"] if previous_rows else None)
            if (show_events or show_structure or show_ranges) and first_row and range_end_id:
                replay_rows = query_rows_between(cur, int(first_row["id"]), int(range_end_id), MAX_TICK_WINDOW)
            else:
                replay_rows = []
            replay_first_id = int(replay_rows[0]["id"]) if replay_rows else (int(first_row["id"]) if first_row else None)
            replay_last_id = int(replay_rows[-1]["id"]) if replay_rows else (int(range_end_id) if range_end_id else None)
            mavg_payload = mavg_payload_for_tick_range(
                cur,
                page="live",
                start_id=replay_first_id,
                end_id=replay_last_id,
                include_configs=True,
            )
    serialize_started = time.perf_counter()
    snapshot = apply_structure_flags(
        structure_snapshot(replay_rows, enabled=show_events or show_structure or show_ranges),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    first_row_id = first_row["id"] if first_row else None
    payload = {
        "rows": serialize_tick_rows(previous_rows) if show_ticks else [],
        "rowCount": len(previous_rows) if show_ticks else 0,
        "firstId": first_row_id,
        "lastId": range_end_id,
        "beforeId": before_id,
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        **snapshot,
        **mavg_payload,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=(replay_rows[-1] if replay_rows else (previous_rows[-1] if previous_rows else None)),
        ),
    }
    return payload



def backbone_pivot_columns() -> str:
    return "id, dayid, tickid, ticktime, price, pivottype, threshold, source, createdat"


def backbone_move_columns() -> str:
    return (
        "id, dayid, starttickid, endtickid, starttime, endtime, "
        "startprice, endprice, direction, pricedelta, tickcount, "
        "thresholdatconfirm, source, createdat"
    )


def serialize_backbone_state_row(row: Optional[Dict[str, Any]], *, brokerday: Optional[date], day_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "dayId": int(day_id or row.get("dayid") or 0) or None,
        "brokerday": serialize_value(brokerday),
        "source": row.get("source") or BACKBONE_SOURCE,
        "lastProcessedTickId": int(row.get("lastprocessedtickid") or 0) or None,
        "confirmedPivotTickId": int(row.get("confirmedpivottickid") or 0) or None,
        "confirmedPivotTime": serialize_value(row.get("confirmedpivottime")),
        "confirmedPivotTimeMs": dt_to_ms(row.get("confirmedpivottime")),
        "confirmedPivotPrice": float(row.get("confirmedpivotprice") or 0.0) if row.get("confirmedpivotprice") is not None else None,
        "direction": row.get("direction"),
        "candidateExtremeTickId": int(row.get("candidateextremetickid") or 0) or None,
        "candidateExtremeTime": serialize_value(row.get("candidateextremetime")),
        "candidateExtremeTimeMs": dt_to_ms(row.get("candidateextremetime")),
        "candidateExtremePrice": float(row.get("candidateextremeprice") or 0.0) if row.get("candidateextremeprice") is not None else None,
        "currentThreshold": float(row.get("currentthreshold") or 0.0) if row.get("currentthreshold") is not None else None,
        "updatedAt": serialize_value(row.get("updatedat")),
        "updatedAtMs": dt_to_ms(row.get("updatedat")),
    }


def serialize_backbone_pivot_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ticktime = row.get("ticktime")
    return {
        "id": int(row.get("id") or 0),
        "dayId": int(row.get("dayid") or 0),
        "tickId": int(row.get("tickid") or 0),
        "tickTime": serialize_value(ticktime),
        "tickTimeMs": dt_to_ms(ticktime),
        "price": float(row.get("price") or 0.0),
        "pivotType": row.get("pivottype"),
        "threshold": float(row.get("threshold") or 0.0) if row.get("threshold") is not None else None,
        "source": row.get("source") or BACKBONE_SOURCE,
        "createdAt": serialize_value(row.get("createdat")),
    }


def serialize_backbone_pivot_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_backbone_pivot_row(row) for row in rows]


def serialize_backbone_move_row(row: Dict[str, Any]) -> Dict[str, Any]:
    starttime = row.get("starttime")
    endtime = row.get("endtime")
    return {
        "id": int(row.get("id") or 0),
        "dayId": int(row.get("dayid") or 0),
        "startTickId": int(row.get("starttickid") or 0),
        "endTickId": int(row.get("endtickid") or 0),
        "startTime": serialize_value(starttime),
        "endTime": serialize_value(endtime),
        "startTimeMs": dt_to_ms(starttime),
        "endTimeMs": dt_to_ms(endtime),
        "startPrice": float(row.get("startprice") or 0.0),
        "endPrice": float(row.get("endprice") or 0.0),
        "direction": row.get("direction"),
        "priceDelta": float(row.get("pricedelta") or 0.0),
        "tickCount": int(row.get("tickcount") or 0),
        "thresholdAtConfirm": float(row.get("thresholdatconfirm") or 0.0) if row.get("thresholdatconfirm") is not None else None,
        "source": row.get("source") or BACKBONE_SOURCE,
        "createdAt": serialize_value(row.get("createdat")),
        "durationMs": max(0, (dt_to_ms(endtime) or 0) - (dt_to_ms(starttime) or 0)),
    }


def serialize_backbone_move_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_backbone_move_row(row) for row in rows]


def serialize_backbone_candle_row(row: Dict[str, Any]) -> Dict[str, Any]:
    starttime = row.get("starttime")
    endtime = row.get("endtime")
    return {
        "moveId": int(row.get("id") or 0),
        "dayId": int(row.get("dayid") or 0),
        "startTickId": int(row.get("starttickid") or 0),
        "endTickId": int(row.get("endtickid") or 0),
        "startTime": serialize_value(starttime),
        "endTime": serialize_value(endtime),
        "startTimeMs": dt_to_ms(starttime),
        "endTimeMs": dt_to_ms(endtime),
        "open": float(row.get("startprice") or 0.0),
        "high": float(row.get("highprice") or row.get("startprice") or 0.0),
        "low": float(row.get("lowprice") or row.get("endprice") or 0.0),
        "close": float(row.get("endprice") or 0.0),
        "direction": row.get("direction"),
        "tickCount": int(row.get("tickcount") or 0),
        "priceDelta": float(row.get("pricedelta") or 0.0),
        "thresholdAtConfirm": float(row.get("thresholdatconfirm") or 0.0) if row.get("thresholdatconfirm") is not None else None,
        "source": row.get("source") or BACKBONE_SOURCE,
        "durationMs": max(0, (dt_to_ms(endtime) or 0) - (dt_to_ms(starttime) or 0)),
    }


def serialize_backbone_candle_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_backbone_candle_row(row) for row in rows]


def query_backbone_item_count(cur: Any, *, table_name: str, day_id: int, source: str = BACKBONE_SOURCE) -> int:
    if table_name not in {"backbonepivots", "backbonemoves"}:
        raise ValueError("Unsupported backbone table.")
    cur.execute(
        "SELECT COUNT(*) AS row_count FROM public.{table_name} WHERE dayid = %s AND source = %s".format(table_name=table_name),
        (day_id, source),
    )
    row = dict(cur.fetchone() or {})
    return int(row.get("row_count") or 0)


def query_backbone_last_pivot_before(
    cur: Any,
    *,
    day_id: int,
    before_tick_id: int,
    source: str = BACKBONE_SOURCE,
) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonepivots
        WHERE dayid = %s
          AND source = %s
          AND tickid < %s
        ORDER BY tickid DESC, id DESC
        LIMIT 1
        """.format(select_sql=backbone_pivot_columns()),
        (day_id, source, before_tick_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def query_backbone_pivots_in_range(
    cur: Any,
    *,
    day_id: int,
    start_id: int,
    end_id: int,
    source: str = BACKBONE_SOURCE,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonepivots
        WHERE dayid = %s
          AND source = %s
          AND tickid >= %s
          AND tickid <= %s
        ORDER BY tickid ASC, id ASC
        """.format(select_sql=backbone_pivot_columns()),
        (day_id, source, start_id, end_id),
    )
    return [dict(row) for row in cur.fetchall()]


def query_recent_ticks_for_day(cur: Any, *, dayref: Any, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp < %s
            ORDER BY id DESC
            LIMIT %s
        ) recent
        ORDER BY id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, dayref.starttime, dayref.endtime, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_ticks_for_day_from_id(cur: Any, *, dayref: Any, start_id: int, limit: int) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.ticks
        WHERE symbol = %s
          AND id >= %s
          AND timestamp >= %s
          AND timestamp < %s
        ORDER BY id ASC
        LIMIT %s
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, start_id, dayref.starttime, dayref.endtime, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_candles(
    cur: Any,
    *,
    day_id: int,
    limit: int,
    source: str,
    start_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    select_sql = backbone_move_columns()
    if start_id is None:
        cur.execute(
            """
            SELECT
                m.*,
                COALESCE(agg.highprice, GREATEST(m.startprice, m.endprice)) AS highprice,
                COALESCE(agg.lowprice, LEAST(m.startprice, m.endprice)) AS lowprice
            FROM (
                SELECT {select_sql}
                FROM public.backbonemoves
                WHERE dayid = %s
                  AND source = %s
                ORDER BY endtickid DESC, id DESC
                LIMIT %s
            ) m
            LEFT JOIN LATERAL (
                SELECT
                    MAX(COALESCE(t.mid, (t.bid + t.ask) / 2.0)) AS highprice,
                    MIN(COALESCE(t.mid, (t.bid + t.ask) / 2.0)) AS lowprice
                FROM public.ticks t
                WHERE t.symbol = %s
                  AND t.id >= m.starttickid
                  AND t.id <= m.endtickid
            ) agg ON TRUE
            ORDER BY m.endtickid ASC, m.id ASC
            """.format(select_sql=select_sql),
            (day_id, source, limit, TICK_SYMBOL),
        )
    else:
        cur.execute(
            """
            SELECT
                m.*,
                COALESCE(agg.highprice, GREATEST(m.startprice, m.endprice)) AS highprice,
                COALESCE(agg.lowprice, LEAST(m.startprice, m.endprice)) AS lowprice
            FROM (
                SELECT {select_sql}
                FROM public.backbonemoves
                WHERE dayid = %s
                  AND source = %s
                  AND endtickid >= %s
                ORDER BY endtickid ASC, id ASC
                LIMIT %s
            ) m
            LEFT JOIN LATERAL (
                SELECT
                    MAX(COALESCE(t.mid, (t.bid + t.ask) / 2.0)) AS highprice,
                    MIN(COALESCE(t.mid, (t.bid + t.ask) / 2.0)) AS lowprice
                FROM public.ticks t
                WHERE t.symbol = %s
                  AND t.id >= m.starttickid
                  AND t.id <= m.endtickid
            ) agg ON TRUE
            ORDER BY m.endtickid ASC, m.id ASC
            """.format(select_sql=select_sql),
            (day_id, source, start_id, limit, TICK_SYMBOL),
        )
    return [dict(row) for row in cur.fetchall()]


def build_backbone_candles_payload(
    *,
    dayref: Any,
    state_row: Optional[Dict[str, Any]],
    candles: List[Dict[str, Any]],
    pivot_total: int,
    move_total: int,
    mode: str,
    requested_count: int,
    layer: str,
    fetch_ms: float,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    serialized_candles = serialize_backbone_candle_rows(candles)
    first_id = serialized_candles[0]["startTickId"] if serialized_candles else None
    last_id = serialized_candles[-1]["endTickId"] if serialized_candles else None
    source = BACKBONE_LAYER_SOURCES[layer]
    payload = {
        "view": "candles",
        "layer": layer,
        "layerLabel": BACKBONE_LAYER_LABELS[layer],
        "mode": mode,
        "symbol": TICK_SYMBOL,
        "source": source,
        "dayId": int(dayref.dayid) if dayref else None,
        "brokerday": serialize_value(dayref.brokerday) if dayref else None,
        "dayStart": serialize_value(dayref.starttime) if dayref else None,
        "dayEnd": serialize_value(dayref.endtime) if dayref else None,
        "dayStartMs": dt_to_ms(dayref.starttime) if dayref else None,
        "dayEndMs": dt_to_ms(dayref.endtime) if dayref else None,
        "state": serialize_backbone_state_row(state_row, brokerday=(dayref.brokerday if dayref else None), day_id=(dayref.dayid if dayref else None)),
        "candles": serialized_candles,
        "candleCount": len(serialized_candles),
        "requestedCandles": requested_count,
        "pivotTotal": pivot_total,
        "moveTotal": move_total,
        "firstId": first_id,
        "lastId": last_id,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row={"id": last_id, "timestamp": candles[-1].get("endtime")} if candles else None,
    )
    return payload


def load_backbone_candles_payload(*, count: int, start_id: Optional[int], layer: str) -> Dict[str, Any]:
    layer = normalize_backbone_layer(layer)
    source = BACKBONE_LAYER_SOURCES[layer]
    effective_count = clamp_int(count, 1, MAX_BACKBONE_CANDLE_COUNT)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        if start_id is None:
            dayref = resolve_current_backbone_day_ref(conn, symbol=TICK_SYMBOL)
            if dayref is None:
                return build_backbone_candles_payload(
                    dayref=None,
                    state_row=None,
                    candles=[],
                    pivot_total=0,
                    move_total=0,
                    mode="live",
                    requested_count=effective_count,
                    layer=layer,
                    fetch_ms=elapsed_ms(fetch_started),
                )
        else:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                tick_row = query_tick_by_id(cur, start_id)
            if tick_row is None:
                raise HTTPException(status_code=404, detail="Backbone start tick was not found.")
            dayref = resolve_backbone_day_ref_for_timestamp(conn, symbol=TICK_SYMBOL, timestamp=tick_row["timestamp"])
        state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid, source=source)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            candles = query_backbone_candles(cur, day_id=dayref.dayid, limit=effective_count, source=source, start_id=start_id)
            pivot_total = query_backbone_item_count(cur, table_name="backbonepivots", day_id=dayref.dayid, source=source)
            move_total = query_backbone_item_count(cur, table_name="backbonemoves", day_id=dayref.dayid, source=source)
    return build_backbone_candles_payload(
        dayref=dayref,
        state_row=state_row,
        candles=candles,
        pivot_total=pivot_total,
        move_total=move_total,
        mode="review" if start_id is not None else "live",
        requested_count=effective_count,
        layer=layer,
        fetch_ms=elapsed_ms(fetch_started),
    )


def build_backbone_detail_payload(
    *,
    dayref: Any,
    state_row: Optional[Dict[str, Any]],
    tick_rows: List[Dict[str, Any]],
    pivots: List[Dict[str, Any]],
    pivot_total: int,
    move_total: int,
    mode: str,
    requested_ticks: int,
    live_leg: Optional[Dict[str, Any]],
    fetch_ms: float,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    serialized_rows = serialize_tick_rows(tick_rows)
    serialized_pivots = serialize_backbone_pivot_rows(pivots)
    first_id = serialized_rows[0]["id"] if serialized_rows else None
    last_id = serialized_rows[-1]["id"] if serialized_rows else None
    payload = {
        "view": "detailed",
        "layer": DEFAULT_BACKBONE_LAYER,
        "layerLabel": BACKBONE_LAYER_LABELS[DEFAULT_BACKBONE_LAYER],
        "mode": mode,
        "symbol": TICK_SYMBOL,
        "source": BACKBONE_SOURCE,
        "dayId": int(dayref.dayid) if dayref else None,
        "brokerday": serialize_value(dayref.brokerday) if dayref else None,
        "dayStart": serialize_value(dayref.starttime) if dayref else None,
        "dayEnd": serialize_value(dayref.endtime) if dayref else None,
        "dayStartMs": dt_to_ms(dayref.starttime) if dayref else None,
        "dayEndMs": dt_to_ms(dayref.endtime) if dayref else None,
        "state": serialize_backbone_state_row(state_row, brokerday=(dayref.brokerday if dayref else None), day_id=(dayref.dayid if dayref else None)),
        "rows": serialized_rows,
        "rowCount": len(serialized_rows),
        "pivots": serialized_pivots,
        "pivotCount": len(serialized_pivots),
        "pivotTotal": pivot_total,
        "moveTotal": move_total,
        "requestedTicks": requested_ticks,
        "firstId": first_id,
        "lastId": last_id,
        "liveLeg": live_leg,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=tick_rows[-1] if tick_rows else None,
    )
    return payload


def load_backbone_detail_payload(*, ticks: int, start_id: Optional[int]) -> Dict[str, Any]:
    effective_ticks = clamp_int(ticks, 1, MAX_TICK_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if start_id is None:
                dayref = resolve_current_backbone_day_ref(conn, symbol=TICK_SYMBOL)
                if dayref is None:
                    return build_backbone_detail_payload(
                        dayref=None,
                        state_row=None,
                        tick_rows=[],
                        pivots=[],
                        pivot_total=0,
                        move_total=0,
                        mode="live",
                        requested_ticks=effective_ticks,
                        live_leg=None,
                        fetch_ms=elapsed_ms(fetch_started),
                    )
                tick_rows = query_recent_ticks_for_day(cur, dayref=dayref, limit=effective_ticks)
            else:
                tick_row = query_tick_by_id(cur, start_id)
                if tick_row is None:
                    raise HTTPException(status_code=404, detail="Backbone start tick was not found.")
                dayref = resolve_backbone_day_ref_for_timestamp(conn, symbol=TICK_SYMBOL, timestamp=tick_row["timestamp"])
                tick_rows = query_ticks_for_day_from_id(cur, dayref=dayref, start_id=start_id, limit=effective_ticks)
            state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid, source=BACKBONE_SOURCE)
            first_id = int(tick_rows[0]["id"]) if tick_rows else None
            last_id = int(tick_rows[-1]["id"]) if tick_rows else None
            visible_pivots: List[Dict[str, Any]] = []
            if first_id is not None and last_id is not None:
                anchor_pivot = query_backbone_last_pivot_before(cur, day_id=dayref.dayid, before_tick_id=first_id, source=BACKBONE_SOURCE)
                if anchor_pivot:
                    visible_pivots.append(anchor_pivot)
                visible_pivots.extend(query_backbone_pivots_in_range(cur, day_id=dayref.dayid, start_id=first_id, end_id=last_id, source=BACKBONE_SOURCE))
            pivot_total = query_backbone_item_count(cur, table_name="backbonepivots", day_id=dayref.dayid, source=BACKBONE_SOURCE)
            move_total = query_backbone_item_count(cur, table_name="backbonemoves", day_id=dayref.dayid, source=BACKBONE_SOURCE)
        live_leg = None
        if start_id is None and tick_rows and state_row:
            latest_row = tick_rows[-1]
            confirmed_tick_id = int(state_row.get("confirmedpivottickid") or 0)
            latest_tick_id = int(latest_row.get("id") or 0)
            if confirmed_tick_id > 0 and latest_tick_id >= confirmed_tick_id:
                latest_price = latest_row.get("mid")
                if latest_price is None and latest_row.get("bid") is not None and latest_row.get("ask") is not None:
                    latest_price = (float(latest_row["bid"]) + float(latest_row["ask"])) / 2.0
                if latest_price is not None:
                    candidate_tick_id = int(state_row.get("candidateextremetickid") or 0) or None
                    candidate_time = state_row.get("candidateextremetime")
                    candidate_price = float(state_row.get("candidateextremeprice") or 0.0) if state_row.get("candidateextremeprice") is not None else None
                    live_leg = {
                        "startTickId": confirmed_tick_id,
                        "startTime": serialize_value(state_row.get("confirmedpivottime")),
                        "startTimeMs": dt_to_ms(state_row.get("confirmedpivottime")),
                        "startPrice": float(state_row.get("confirmedpivotprice") or 0.0) if state_row.get("confirmedpivotprice") is not None else None,
                        "candidateTickId": candidate_tick_id,
                        "candidateTime": serialize_value(candidate_time),
                        "candidateTimeMs": dt_to_ms(candidate_time),
                        "candidatePrice": candidate_price,
                        "endTickId": latest_tick_id,
                        "endTime": serialize_value(latest_row.get("timestamp")),
                        "endTimeMs": dt_to_ms(latest_row.get("timestamp")),
                        "endPrice": float(latest_price),
                        "direction": state_row.get("direction"),
                        "threshold": float(state_row.get("currentthreshold") or 0.0) if state_row.get("currentthreshold") is not None else None,
                        "provisional": True,
                    }
    return build_backbone_detail_payload(
        dayref=dayref,
        state_row=state_row,
        tick_rows=tick_rows,
        pivots=visible_pivots,
        pivot_total=pivot_total,
        move_total=move_total,
        mode="review" if start_id is not None else "live",
        requested_ticks=effective_ticks,
        live_leg=live_leg,
        fetch_ms=elapsed_ms(fetch_started),
    )


def query_backbone_pivots_for_day(cur: Any, *, day_id: int, source: str = BACKBONE_SOURCE) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonepivots
        WHERE dayid = %s
          AND source = %s
        ORDER BY tickid ASC, id ASC
        """.format(select_sql=backbone_pivot_columns()),
        (day_id, source),
    )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_moves_for_day(cur: Any, *, day_id: int, source: str = BACKBONE_SOURCE) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonemoves
        WHERE dayid = %s
          AND source = %s
        ORDER BY endtickid ASC, id ASC
        """.format(select_sql=backbone_move_columns()),
        (day_id, source),
    )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_pivots_from(
    cur: Any,
    *,
    day_id: int,
    start_id: int,
    limit: int,
    source: str = BACKBONE_SOURCE,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonepivots
        WHERE dayid = %s
          AND source = %s
          AND tickid >= %s
        ORDER BY tickid ASC, id ASC
        LIMIT %s
        """.format(select_sql=backbone_pivot_columns()),
        (day_id, source, start_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_moves_from(
    cur: Any,
    *,
    day_id: int,
    start_id: int,
    limit: int,
    source: str = BACKBONE_SOURCE,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.backbonemoves
        WHERE dayid = %s
          AND source = %s
          AND endtickid >= %s
        ORDER BY endtickid ASC, id ASC
        LIMIT %s
        """.format(select_sql=backbone_move_columns()),
        (day_id, source, start_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_pivots_after(
    cur: Any,
    *,
    day_id: int,
    after_id: int,
    limit: int,
    end_id: Optional[int] = None,
    source: str = BACKBONE_SOURCE,
) -> List[Dict[str, Any]]:
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.backbonepivots
            WHERE dayid = %s
              AND source = %s
              AND tickid > %s
            ORDER BY tickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=backbone_pivot_columns()),
            (day_id, source, after_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.backbonepivots
            WHERE dayid = %s
              AND source = %s
              AND tickid > %s
              AND tickid <= %s
            ORDER BY tickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=backbone_pivot_columns()),
            (day_id, source, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_backbone_moves_after(
    cur: Any,
    *,
    day_id: int,
    after_id: int,
    limit: int,
    end_id: Optional[int] = None,
    source: str = BACKBONE_SOURCE,
) -> List[Dict[str, Any]]:
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.backbonemoves
            WHERE dayid = %s
              AND source = %s
              AND endtickid > %s
            ORDER BY endtickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=backbone_move_columns()),
            (day_id, source, after_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.backbonemoves
            WHERE dayid = %s
              AND source = %s
              AND endtickid > %s
              AND endtickid <= %s
            ORDER BY endtickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=backbone_move_columns()),
            (day_id, source, after_id, end_id, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def build_backbone_payload(
    *,
    mode: str,
    start_id: Optional[int],
    day_id: Optional[int],
    brokerday: Optional[date],
    state_row: Optional[Dict[str, Any]],
    pivots: List[Dict[str, Any]],
    moves: List[Dict[str, Any]],
    tick_rows: List[Dict[str, Any]],
    tick_bounds: Dict[str, Any],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    show_ticks: bool,
    fetch_ms: float,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_candidates = [
        int(start_id or 0) or None if mode == "review" else None,
        int(pivots[0]["tickid"]) if pivots else None,
        int(moves[0]["starttickid"]) if moves else None,
        int(tick_rows[0]["id"]) if tick_rows else None,
        int(tick_bounds.get("first_id") or 0) or None if mode == "live" else None,
    ]
    last_candidates = [
        int(start_id or 0) or None if mode == "review" else None,
        int(state_row.get("lastprocessedtickid") or 0) if state_row and mode == "live" else None,
        int(pivots[-1]["tickid"]) if pivots else None,
        int(moves[-1]["endtickid"]) if moves else None,
        int(tick_rows[-1]["id"]) if tick_rows else None,
    ]
    first_id = min([value for value in first_candidates if value is not None], default=None)
    last_id = max([value for value in last_candidates if value is not None], default=None)
    payload = {
        "mode": mode,
        "symbol": TICK_SYMBOL,
        "source": BACKBONE_SOURCE,
        "dayId": int(day_id or 0) or None,
        "brokerday": serialize_value(brokerday),
        "dayStart": serialize_value(tick_bounds.get("first_timestamp")),
        "dayEnd": serialize_value(tick_bounds.get("last_timestamp")),
        "dayStartMs": dt_to_ms(tick_bounds.get("first_timestamp")),
        "dayEndMs": dt_to_ms(tick_bounds.get("last_timestamp")),
        "state": serialize_backbone_state_row(state_row, brokerday=brokerday, day_id=day_id),
        "pivots": serialize_backbone_pivot_rows(pivots),
        "pivotCount": len(pivots),
        "moves": serialize_backbone_move_rows(moves),
        "moveCount": len(moves),
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "firstId": first_id,
        "lastId": last_id,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "reviewEndTimestampMs": dt_to_ms(review_end_timestamp),
        "hasMoreLeft": bool(mode == "review" and first_id and tick_bounds.get("first_id") and first_id > tick_bounds["first_id"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_id is not None and last_id >= review_end_id),
    }
    latest_row = tick_rows[-1] if tick_rows else {"id": last_id, "timestamp": tick_bounds.get("last_timestamp")}
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=latest_row,
    )
    return payload


def load_backbone_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    show_ticks: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_BACKBONE_REVIEW_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        if mode == "live":
            dayref = resolve_current_backbone_day_ref(conn, symbol=TICK_SYMBOL)
            if dayref is None:
                return build_backbone_payload(
                    mode=mode,
                    start_id=None,
                    day_id=None,
                    brokerday=None,
                    state_row=None,
                    pivots=[],
                    moves=[],
                    tick_rows=[],
                    tick_bounds={},
                    review_end_id=None,
                    review_end_timestamp=None,
                    show_ticks=show_ticks,
                    fetch_ms=elapsed_ms(fetch_started),
                )
            state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid)
            day_end_query = dayref.endtime - timedelta(microseconds=1)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                pivots = query_backbone_pivots_for_day(cur, day_id=dayref.dayid)
                moves = query_backbone_moves_for_day(cur, day_id=dayref.dayid)
                tick_bounds = query_tick_range_bounds_for_time(cur, start_ts=dayref.starttime, end_ts=day_end_query)
                tick_rows = query_ticks_in_time_range(cur, start_ts=dayref.starttime, end_ts=day_end_query) if show_ticks else []
            return build_backbone_payload(
                mode=mode,
                start_id=None,
                day_id=dayref.dayid,
                brokerday=dayref.brokerday,
                state_row=state_row,
                pivots=pivots,
                moves=moves,
                tick_rows=tick_rows,
                tick_bounds=tick_bounds,
                review_end_id=None,
                review_end_timestamp=None,
                show_ticks=show_ticks,
                fetch_ms=elapsed_ms(fetch_started),
            )

        if start_id is None:
            raise HTTPException(status_code=400, detail="Review mode requires an id value.")

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tick_row = query_tick_by_id(cur, start_id)
            if tick_row is None:
                raise HTTPException(status_code=404, detail="Review start tick was not found.")
            dayref = resolve_backbone_day_ref_for_timestamp(conn, symbol=TICK_SYMBOL, timestamp=tick_row["timestamp"])
            state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid)
            pivots = query_backbone_pivots_from(cur, day_id=dayref.dayid, start_id=start_id, limit=effective_window)
            moves = query_backbone_moves_from(cur, day_id=dayref.dayid, start_id=start_id, limit=effective_window)
            day_end_query = dayref.endtime - timedelta(microseconds=1)
            tick_bounds = query_tick_range_bounds_for_time(cur, start_ts=dayref.starttime, end_ts=day_end_query)
            visible_end_id = max(
                [start_id]
                + ([int(pivots[-1]["tickid"])] if pivots else [])
                + ([int(moves[-1]["endtickid"])] if moves else [])
            )
            tick_rows = query_rows_between(cur, start_id, visible_end_id, MAX_TICK_WINDOW) if show_ticks else []
        return build_backbone_payload(
            mode=mode,
            start_id=start_id,
            day_id=dayref.dayid,
            brokerday=dayref.brokerday,
            state_row=state_row,
            pivots=pivots,
            moves=moves,
            tick_rows=tick_rows,
            tick_bounds=tick_bounds,
            review_end_id=int(tick_bounds.get("last_id") or 0) or None,
            review_end_timestamp=tick_bounds.get("last_timestamp"),
            show_ticks=show_ticks,
            fetch_ms=elapsed_ms(fetch_started),
        )


def load_backbone_next_payload(
    *,
    after_id: int,
    limit: int,
    day_id: Optional[int],
    end_id: Optional[int],
    show_ticks: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        resolved_day_id = day_id
        brokerday = None
        if resolved_day_id is None:
            dayref = resolve_current_backbone_day_ref(conn, symbol=TICK_SYMBOL)
            if dayref is None:
                return {
                    "dayId": None,
                    "brokerday": None,
                    "pivotUpdates": [],
                    "moveUpdates": [],
                    "rows": [],
                    "rowCount": 0,
                    "lastId": after_id,
                    "endId": end_id,
                    "endReached": False,
                    "state": None,
                    "metrics": serialize_metrics_payload(fetch_ms=elapsed_ms(fetch_started), serialize_ms=0.0, latest_row=None),
                }
            resolved_day_id = dayref.dayid
            brokerday = dayref.brokerday
        state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=resolved_day_id)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            pivot_rows = query_backbone_pivots_after(cur, day_id=resolved_day_id, after_id=after_id, limit=effective_limit, end_id=end_id)
            move_rows = query_backbone_moves_after(cur, day_id=resolved_day_id, after_id=after_id, limit=effective_limit, end_id=end_id)
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id) if show_ticks else []
    last_id = max(
        [after_id]
        + [int(row.get("tickid") or 0) for row in pivot_rows]
        + [int(row.get("endtickid") or 0) for row in move_rows]
        + [int(row.get("id") or 0) for row in tick_rows]
    )
    return {
        "dayId": resolved_day_id,
        "brokerday": serialize_value(brokerday),
        "pivotUpdates": serialize_backbone_pivot_rows(pivot_rows),
        "pivotCount": len(pivot_rows),
        "moveUpdates": serialize_backbone_move_rows(move_rows),
        "moveCount": len(move_rows),
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "lastId": last_id,
        "endId": end_id,
        "endReached": bool(end_id is not None and last_id >= end_id),
        "state": serialize_backbone_state_row(state_row, brokerday=brokerday, day_id=resolved_day_id),
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=0.0,
            latest_row=(tick_rows[-1] if tick_rows else {"id": last_id, "timestamp": state_row.get("updatedat") if state_row else None}),
        ),
    }


def stream_backbone_events(
    *,
    after_id: int,
    limit: int,
    show_ticks: bool,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    active_day_id: Optional[int] = None
    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            while True:
                dayref = resolve_current_backbone_day_ref(conn, symbol=TICK_SYMBOL)
                if dayref is None:
                    if time.monotonic() - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                        payload = {
                            "dayId": None,
                            "brokerday": None,
                            "pivotUpdates": [],
                            "moveUpdates": [],
                            "rows": [],
                            "rowCount": 0,
                            "lastId": last_id,
                            "streamMode": "heartbeat",
                            "state": None,
                            **serialize_metrics_payload(fetch_ms=0.0, serialize_ms=0.0, latest_row=None),
                        }
                        yield format_sse(payload, event_name="heartbeat")
                        last_heartbeat = time.monotonic()
                    time.sleep(idle_sleep)
                    idle_sleep = STREAM_IDLE_POLL_SECONDS
                    continue

                if active_day_id is None:
                    active_day_id = dayref.dayid
                elif dayref.dayid != active_day_id:
                    active_day_id = dayref.dayid
                    last_id = 0
                    state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid)
                    payload = {
                        "dayId": dayref.dayid,
                        "brokerday": serialize_value(dayref.brokerday),
                        "dayChanged": True,
                        "pivotUpdates": [],
                        "moveUpdates": [],
                        "rows": [],
                        "rowCount": 0,
                        "lastId": 0,
                        "streamMode": "reset",
                        "state": serialize_backbone_state_row(state_row, brokerday=dayref.brokerday, day_id=dayref.dayid),
                        **serialize_metrics_payload(fetch_ms=0.0, serialize_ms=0.0, latest_row=None),
                    }
                    yield format_sse(payload)
                    last_heartbeat = time.monotonic()
                    idle_sleep = STREAM_POLL_SECONDS
                    continue

                state_row = load_backbone_state_row(conn, symbol=TICK_SYMBOL, dayid=dayref.dayid)
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    fetch_started = time.perf_counter()
                    pivot_rows = query_backbone_pivots_after(cur, day_id=dayref.dayid, after_id=last_id, limit=effective_limit)
                    move_rows = query_backbone_moves_after(cur, day_id=dayref.dayid, after_id=last_id, limit=effective_limit)
                    tick_rows = query_rows_after(cur, last_id, effective_limit) if show_ticks else []
                    fetch_ms = elapsed_ms(fetch_started)

                if pivot_rows or move_rows or tick_rows:
                    last_id = max(
                        [last_id]
                        + [int(row.get("tickid") or 0) for row in pivot_rows]
                        + [int(row.get("endtickid") or 0) for row in move_rows]
                        + [int(row.get("id") or 0) for row in tick_rows]
                    )
                    payload = {
                        "dayId": dayref.dayid,
                        "brokerday": serialize_value(dayref.brokerday),
                        "pivotUpdates": serialize_backbone_pivot_rows(pivot_rows),
                        "pivotCount": len(pivot_rows),
                        "moveUpdates": serialize_backbone_move_rows(move_rows),
                        "moveCount": len(move_rows),
                        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
                        "rowCount": len(tick_rows) if show_ticks else 0,
                        "lastId": last_id,
                        "streamMode": "delta",
                        "state": serialize_backbone_state_row(state_row, brokerday=dayref.brokerday, day_id=dayref.dayid),
                        **serialize_metrics_payload(
                            fetch_ms=fetch_ms,
                            serialize_ms=0.0,
                            latest_row=(tick_rows[-1] if tick_rows else {"id": last_id, "timestamp": state_row.get("updatedat") if state_row else None}),
                        ),
                    }
                    yield format_sse(payload)
                    last_heartbeat = time.monotonic()
                    idle_sleep = STREAM_POLL_SECONDS
                    continue

                now = time.monotonic()
                if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                    payload = {
                        "dayId": dayref.dayid,
                        "brokerday": serialize_value(dayref.brokerday),
                        "pivotUpdates": [],
                        "moveUpdates": [],
                        "rows": [],
                        "rowCount": 0,
                        "lastId": last_id,
                        "streamMode": "heartbeat",
                        "state": serialize_backbone_state_row(state_row, brokerday=dayref.brokerday, day_id=dayref.dayid),
                        **serialize_metrics_payload(
                            fetch_ms=0.0,
                            serialize_ms=0.0,
                            latest_row={"id": last_id, "timestamp": state_row.get("updatedat") if state_row else None},
                        ),
                    }
                    yield format_sse(payload, event_name="heartbeat")
                    last_heartbeat = now
                time.sleep(idle_sleep)
                idle_sleep = STREAM_IDLE_POLL_SECONDS
    except GeneratorExit:
        return


def stream_events(
    *,
    after_id: int,
    after_mavg_id: int,
    limit: int,
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
    max_window: int = MAX_TICK_WINDOW,
    rect_mode: Optional[str] = None,
    stream_name: str = "live_stream",
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    last_mavg_id = max(0, after_mavg_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, max_window)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    structure_enabled = show_events or show_structure or show_ranges
    engine = StructureEngine(symbol=TICK_SYMBOL) if structure_enabled else None

    stream_open(stream_name)
    try:
        try:
            with db_connection(readonly=True, autocommit=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if engine is not None and last_id:
                        seed_rows = query_window_ending_at(cur, last_id, effective_window)
                        for row in seed_rows:
                            try:
                                engine.process_tick(row)
                            except Exception:
                                engine = None
                                break

                    while True:
                        fetch_started = time.perf_counter()
                        tick_rows = query_rows_after(cur, last_id, effective_limit)
                        fetch_ms = elapsed_ms(fetch_started)
                        mavg_payload = mavg_updates_payload(cur, page="live", after_value_id=last_mavg_id)
                        mavg_points = list(mavg_payload.get("mavgPoints") or [])
                        last_mavg_id = int(mavg_payload.get("mavgCursorId") or last_mavg_id)
                        if tick_rows or mavg_points:
                            serialize_started = time.perf_counter()
                            latest_tick_row = tick_rows[-1] if tick_rows else query_latest_tick(cur)
                            payload_rows = serialize_tick_rows(tick_rows) if show_ticks else []
                            updates = {"bars": [], "rangeBoxes": [], "events": []}
                            if engine is not None:
                                try:
                                    for row in tick_rows:
                                        delta = engine.process_tick(row)
                                        updates["bars"].extend(delta["bars"])
                                        updates["rangeBoxes"].extend(delta["rangeBoxes"])
                                        updates["events"].extend(delta["events"])
                                except Exception:
                                    updates = {"bars": [], "rangeBoxes": [], "events": []}
                                    engine = None
                            rect_snapshot = rect_snapshot_for_mode(rect_mode) if rect_mode else None
                            if rect_mode:
                                for row in tick_rows:
                                    rect_snapshot = RECT_PAPER_SERVICE.process_tick(rect_mode, row) or rect_snapshot

                            if tick_rows:
                                last_id = int(latest_tick_row["id"])
                            payload = {
                                "rows": payload_rows,
                                "rowCount": len(payload_rows),
                                "structureBarUpdates": updates["bars"] if show_structure else [],
                                "rangeBoxUpdates": updates["rangeBoxes"] if show_ranges else [],
                                "structureEvents": updates["events"] if show_events else [],
                                "mavgPoints": mavg_points,
                                "mavgCursorId": last_mavg_id,
                                "lastId": last_id,
                                "streamMode": "delta",
                                "rect": rect_snapshot,
                                **serialize_metrics_payload(
                                    fetch_ms=fetch_ms,
                                    serialize_ms=elapsed_ms(serialize_started),
                                    latest_row=latest_tick_row,
                                ),
                            }
                            yield format_sse(payload)
                            last_heartbeat = time.monotonic()
                            idle_sleep = STREAM_POLL_SECONDS
                            continue

                        now = time.monotonic()
                        if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                            latest_row = query_latest_tick(cur)
                            payload = {
                                "rows": [],
                                "rowCount": 0,
                                "structureBarUpdates": [],
                                "rangeBoxUpdates": [],
                                "structureEvents": [],
                                "mavgPoints": [],
                                "mavgCursorId": last_mavg_id,
                                "lastId": last_id,
                                "streamMode": "heartbeat",
                                "rect": rect_snapshot_for_mode(rect_mode) if rect_mode else None,
                                "pollSleepMs": round(idle_sleep * 1000.0, 2),
                                **serialize_metrics_payload(
                                    fetch_ms=fetch_ms,
                                    serialize_ms=0.0,
                                    latest_row=latest_row,
                                ),
                            }
                            yield format_sse(payload, event_name="heartbeat")
                            last_heartbeat = now
                        time.sleep(idle_sleep)
                        idle_sleep = STREAM_IDLE_POLL_SECONDS
        except GeneratorExit:
            return
    finally:
        stream_close(stream_name)


def stream_review_events(
    *,
    after_id: int,
    end_id: int,
    speed: float,
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
    rect_mode: str = "review",
    stream_name: str = "live_review_stream",
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
    effective_batch = clamp_int(REVIEW_STREAM_BATCH, 1, MAX_STREAM_BATCH)
    speed_multiplier = max(0.1, float(speed or 1.0))
    structure_enabled = show_events or show_structure or show_ranges
    engine = StructureEngine(symbol=TICK_SYMBOL) if structure_enabled else None
    previous_row: Optional[Dict[str, Any]] = None

    stream_open(stream_name)
    try:
        try:
            with db_connection(readonly=True, autocommit=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if engine is not None and last_id:
                        seed_rows = query_window_ending_at(cur, last_id, effective_window)
                        for row in seed_rows:
                            try:
                                engine.process_tick(row)
                            except Exception:
                                engine = None
                                break
                        previous_row = seed_rows[-1] if seed_rows else None

                    while last_id < end_id:
                        fetch_started = time.perf_counter()
                        batch_rows = query_rows_after(cur, last_id, effective_batch, end_id=end_id)
                        fetch_ms = elapsed_ms(fetch_started)
                        mavg_rows = safe_query_mavg_points_for_tick_range(
                            cur,
                            page="live",
                            start_id=(last_id + 1) if batch_rows else 0,
                            end_id=int(batch_rows[-1]["id"]) if batch_rows else 0,
                        ) if batch_rows else []
                        mavg_rows_by_tickid: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
                        for mavg_row in mavg_rows:
                            mavg_rows_by_tickid[int(mavg_row["tickid"])].append(mavg_row)
                        if not batch_rows:
                            payload = {
                                "rows": [],
                                "rowCount": 0,
                                "structureBarUpdates": [],
                                "rangeBoxUpdates": [],
                                "structureEvents": [],
                                "mavgPoints": [],
                                "lastId": last_id,
                                "endId": end_id,
                                "endReached": True,
                                "streamMode": "complete",
                                "rect": rect_snapshot_for_mode(rect_mode),
                                **serialize_metrics_payload(
                                    fetch_ms=fetch_ms,
                                    serialize_ms=0.0,
                                    latest_row=query_latest_tick(cur),
                                ),
                            }
                            yield format_sse(payload)
                            return

                        for row in batch_rows:
                            delay_ms = 0
                            if previous_row and previous_row.get("timestamp") and row.get("timestamp"):
                                delta_ms = max(0, dt_to_ms(row["timestamp"]) - dt_to_ms(previous_row["timestamp"]))
                                delay_ms = min(REVIEW_MAX_DELAY_MS, int(round(delta_ms / speed_multiplier)))
                            if delay_ms > 0:
                                time.sleep(max(REVIEW_MIN_DELAY_MS, delay_ms) / 1000.0)

                            serialize_started = time.perf_counter()
                            updates = {"bars": [], "rangeBoxes": [], "events": []}
                            if engine is not None:
                                try:
                                    delta = engine.process_tick(row)
                                    updates["bars"].extend(delta["bars"])
                                    updates["rangeBoxes"].extend(delta["rangeBoxes"])
                                    updates["events"].extend(delta["events"])
                                except Exception:
                                    updates = {"bars": [], "rangeBoxes": [], "events": []}
                                    engine = None

                            last_id = int(row["id"])
                            rect_snapshot = RECT_PAPER_SERVICE.process_tick(rect_mode, row)
                            payload = {
                                "rows": serialize_tick_rows([row]) if show_ticks else [],
                                "rowCount": 1 if show_ticks else 0,
                                "structureBarUpdates": updates["bars"] if show_structure else [],
                                "rangeBoxUpdates": updates["rangeBoxes"] if show_ranges else [],
                                "structureEvents": updates["events"] if show_events else [],
                                "mavgPoints": serialize_mavg_points(mavg_rows_by_tickid.get(last_id, [])),
                                "mavgCursorId": max((int(item["id"]) for item in mavg_rows_by_tickid.get(last_id, [])), default=None),
                                "lastId": last_id,
                                "endId": end_id,
                                "endReached": bool(last_id >= end_id),
                                "streamMode": "delta",
                                "rect": rect_snapshot or rect_snapshot_for_mode(rect_mode),
                                "playbackDelayMs": delay_ms,
                                **serialize_metrics_payload(
                                    fetch_ms=fetch_ms,
                                    serialize_ms=elapsed_ms(serialize_started),
                                    latest_row=row,
                                ),
                            }
                            yield format_sse(payload)
                            previous_row = row
                            if last_id >= end_id:
                                return
        except GeneratorExit:
            return
    finally:
        stream_close(stream_name)


def smart_scalp_ticks_after(after_id: int, limit: int) -> List[Dict[str, Any]]:
    effective_after_id = max(0, int(after_id or 0))
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_rows_after(cur, effective_after_id, effective_limit)


def smart_scalp_recent_ticks(limit: int) -> List[Dict[str, Any]]:
    effective_limit = clamp_int(limit, 1, MAX_TICK_WINDOW)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            return query_bootstrap_rows(cur, mode="live", start_id=None, window=effective_limit, end_id=None)


def smart_scalp_latest_tick() -> Optional[Dict[str, Any]]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            row = query_latest_tick(cur)
            return dict(row) if row else None


def smart_scalp_snapshot() -> Dict[str, Any]:
    snapshot, _ = TRADE_GATEWAY.snapshot_or_last_known()
    return snapshot


def smart_scalp_broker_status() -> Dict[str, Any]:
    return TRADE_GATEWAY.status()


def _audit_trade_action(
    *,
    action: str,
    source: str,
    reason: str,
    side: Optional[str] = None,
    position_id: Optional[int] = None,
    volume: Optional[int] = None,
    lot_size: Optional[float] = None,
) -> None:
    AUDIT_LOGGER.info(
        "trade_audit action=%s source=%s side=%s position_id=%s volume=%s lot_size=%s reason=%s",
        action,
        source,
        side,
        position_id,
        volume,
        lot_size,
        reason,
    )


def smart_scalp_place_market_order(
    *,
    side: str,
    volume: float,
    stop_loss: Optional[float],
    take_profit: Optional[float],
    reason: Optional[str] = None,
    source: str = "smart",
) -> Dict[str, Any]:
    payload = TradeMarketOrderRequest(side=side, lotSize=float(volume), stopLoss=stop_loss, takeProfit=take_profit)
    broker_volume = trade_volume_from_request(payload)
    result = TRADE_GATEWAY.place_market_order(
        side=payload.side,
        volume=broker_volume,
        stop_loss=payload.stopLoss,
        take_profit=payload.takeProfit,
    )
    _audit_trade_action(
        action="market_order",
        source=source,
        reason=reason or "Smart scalp auto-entry trigger.",
        side=payload.side,
        volume=broker_volume,
        lot_size=float(volume),
    )
    return result


def smart_scalp_close_position(
    *,
    position_id: int,
    volume: int,
    reason: Optional[str] = None,
    source: str = "smart",
) -> Dict[str, Any]:
    result = TRADE_GATEWAY.close_position(position_id=position_id, volume=volume)
    _audit_trade_action(
        action="close_position",
        source=source,
        reason=reason or "Smart scalp auto-close trigger.",
        position_id=position_id,
        volume=volume,
    )
    return result


SMART_SCALP_SERVICE = SmartScalpService(
    symbol=TICK_SYMBOL,
    fetch_ticks_after=smart_scalp_ticks_after,
    fetch_recent_ticks=smart_scalp_recent_ticks,
    fetch_latest_tick=smart_scalp_latest_tick,
    fetch_snapshot=smart_scalp_snapshot,
    fetch_broker_status=smart_scalp_broker_status,
    place_market_order=smart_scalp_place_market_order,
    close_position=smart_scalp_close_position,
    smart_lot_size=0.01,
)
RECT_PAPER_SERVICE = RectPaperService(db_factory=db_connection, symbol=TICK_SYMBOL)


def _trade_not_configured() -> bool:
    return not TRADE_GATEWAY.configured


def _handle_trade_gateway_error(exc: Exception) -> None:
    detail = str(exc) or "Trade request failed."
    SMART_SCALP_SERVICE.reset(reason=detail, restore_close_preference=True)
    status_code = getattr(exc, "status_code", None) or status.HTTP_502_BAD_GATEWAY
    if not isinstance(status_code, int):
        status_code = status.HTTP_502_BAD_GATEWAY
    lowered = detail.lower()
    if status_code == status.HTTP_502_BAD_GATEWAY:
        if "not configured" in lowered:
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        elif "must" in lowered or "required" in lowered:
            status_code = status.HTTP_400_BAD_REQUEST
    error_code = getattr(exc, "code", None)
    if not error_code:
        if status_code == status.HTTP_400_BAD_REQUEST:
            error_code = "TRADE_REQUEST_INVALID"
        elif status_code == status.HTTP_503_SERVICE_UNAVAILABLE:
            error_code = "BROKER_UNAVAILABLE"
        else:
            error_code = "TRADE_REQUEST_FAILED"
    raise HTTPException(
        status_code=status_code,
        detail={
            "error": error_code,
            "message": detail,
            "brokerConfigured": TRADE_GATEWAY.configured,
            "configured": TRADE_GATEWAY.configured,
            "broker": TRADE_GATEWAY.status(),
        },
    ) from exc


def _handle_smart_scalp_error(exc: Exception) -> None:
    if isinstance(exc, SmartScalpError):
        status_code = int(getattr(exc, "status_code", status.HTTP_400_BAD_REQUEST))
        error_code = str(getattr(exc, "code", "") or "SMART_SCALP_ERROR")
        message = str(exc) or "Smart scalp request failed."
    else:
        status_code = status.HTTP_502_BAD_GATEWAY
        error_code = "SMART_SCALP_FAILED"
        message = str(exc) or "Smart scalp request failed."
    raise HTTPException(
        status_code=status_code,
        detail={
            "error": error_code,
            "message": message,
            "brokerConfigured": TRADE_GATEWAY.configured,
            "configured": TRADE_GATEWAY.configured,
            "broker": TRADE_GATEWAY.status(),
            "smart": SMART_SCALP_SERVICE.snapshot_state(),
        },
    ) from exc


def _handle_rect_error(exc: Exception) -> None:
    if isinstance(exc, RectServiceError):
        status_code = int(getattr(exc, "status_code", status.HTTP_400_BAD_REQUEST))
        error_code = str(getattr(exc, "code", "") or "RECT_ERROR")
        message = str(exc) or "Rectangle request failed."
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        error_code = "RECT_FAILED"
        message = str(exc) or "Rectangle request failed."
    raise HTTPException(
        status_code=status_code,
        detail={
            "error": error_code,
            "message": message,
            "rect": None,
        },
    ) from exc


@app.on_event("startup")
def app_startup() -> None:
    SMART_SCALP_SERVICE.start()
    RECT_PAPER_SERVICE.start()


@app.on_event("shutdown")
def app_shutdown() -> None:
    SMART_SCALP_SERVICE.stop()
    RECT_PAPER_SERVICE.stop()


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/backbone", include_in_schema=False)
def backbone_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "backbone.html")


@app.get("/bigPic", include_in_schema=False)
def bigpicture_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "bigpicture.html")


@app.get("/bigpic", include_in_schema=False)
@app.get("/bigpicture", include_in_schema=False)
def bigpicture_alias_page() -> RedirectResponse:
    return RedirectResponse(url="/bigPic", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@app.get("/sql", include_in_schema=False)
def sql_page(_: Optional[str] = Depends(require_sql_admin)) -> FileResponse:
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
            row = dict(cur.fetchone() or {})
    return {
        "ok": True,
        "symbol": TICK_SYMBOL,
        "lastId": row.get("last_id"),
        "lastTimestamp": serialize_value(row.get("last_timestamp")),
        "lastTimestampMs": dt_to_ms(row.get("last_timestamp")),
        "serverTimeMs": now_ms(),
    }


@app.get("/api/sql/schema")
def sql_schema(_: Optional[str] = Depends(require_sql_admin)) -> Dict[str, Any]:
    return list_sql_tables()


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: Optional[str] = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)


@app.post("/api/sql/export-csv", response_model=None)
def sql_export_csv(payload: QueryExportRequest, _: Optional[str] = Depends(require_sql_admin)):
    try:
        return export_query_to_csv(payload.query, payload.filename)
    except HTTPException as exc:
        detail = exc.detail
        if isinstance(detail, dict):
            message = detail.get("error") or detail.get("message") or "CSV export failed."
            content = {"ok": False, "error": message, "detail": detail}
        else:
            content = {"ok": False, "error": str(detail), "detail": str(detail)}
        return JSONResponse(status_code=exc.status_code, content=content)
    except Exception as exc:
        SQL_EXPORT_LOGGER.exception("sql_export_endpoint_failed query=%s", payload.query)
        detail = {
            "message": str(exc) or exc.__class__.__name__,
            "exceptionType": exc.__class__.__name__,
        }
        return JSONResponse(status_code=500, content={"ok": False, "error": detail["message"], "detail": detail})


@app.get("/api/sql/export-csv/{filename}")
def sql_export_csv_download(filename: str, _: Optional[str] = Depends(require_sql_admin)) -> FileResponse:
    safe_name, export_path = resolve_sql_export_download(filename)
    if not export_path.is_file():
        raise HTTPException(status_code=404, detail="Export file not found.")
    return FileResponse(path=export_path, media_type="text/csv", filename=safe_name)


@app.get("/api/motion/signals/recent")
def motion_signals_recent(limit: int = Query(200, ge=1, le=1000)) -> Dict[str, Any]:
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    tickid,
                    timestamp,
                    side,
                    mid,
                    spread,
                    velocity3,
                    acceleration3,
                    efficiency3,
                    spreadmultiple3,
                    velocity10,
                    acceleration10,
                    outcome,
                    score
                FROM public.motionsignal
                ORDER BY timestamp DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = [dict(row) for row in cur.fetchall()]
    return {
        "signals": [serialize_motion_signal_row(row) for row in rows],
        "count": len(rows),
        "serverTimeMs": now_ms(),
    }


@app.post("/api/trade/login")
def trade_login(payload: TradeLoginRequest, response: Response) -> Any:
    if not trade_login_configured():
        return trade_auth_not_configured_response()
    username = (payload.username or "").strip()
    password = payload.password or ""
    valid_user = secrets.compare_digest(username, TRADE_USERNAME)
    valid_password = secrets.compare_digest(password, TRADE_PASSWORD)
    if not (valid_user and valid_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid trade credentials.")
    _set_trade_cookie(response, username)
    SMART_SCALP_SERVICE.reset(reason="Trade login successful. Smart Close defaults to ON.", restore_close_preference=True)
    SMART_SCALP_SERVICE.touch_auth()
    return {"ok": True, "username": username}


@app.post("/api/trade/logout")
def trade_logout(response: Response) -> Dict[str, Any]:
    _clear_trade_cookie(response)
    SMART_SCALP_SERVICE.reset(reason="Trade session logged out. Smart Close remains server-side.", restore_close_preference=True)
    return {"ok": True}


@app.get("/api/trade/me")
def trade_me(request: Request, response: Response) -> Dict[str, Any]:
    if not trade_login_configured():
        _clear_trade_cookie(response)
        SMART_SCALP_SERVICE.reset(reason="Trade login is not configured on the server.")
        return trade_auth_status_payload(
            authenticated=False,
            username=None,
            error="TRADE_AUTH_NOT_CONFIGURED",
            message="Trade login is not configured on the server.",
        )
    payload = _trade_session_decode(request.cookies.get(TRADE_COOKIE_NAME, ""))
    username = str(payload["u"]) if payload else None
    if username:
        SMART_SCALP_SERVICE.touch_auth()
    return trade_auth_status_payload(authenticated=bool(username), username=username)


@app.get("/api/trade/open")
def trade_open(username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        snapshot, stale_snapshot = TRADE_GATEWAY.snapshot_or_last_known()
        volume_info = dict(snapshot.get("volumeInfo") or {})
        volume_info["defaultLotSize"] = float(TRADE_DEFAULT_LOT_SIZE)
        return {
            "symbol": snapshot.get("symbol"),
            "symbolId": snapshot.get("symbolId"),
            "symbolDigits": snapshot.get("symbolDigits"),
            "volumeInfo": volume_info,
            "positions": snapshot.get("positions", []),
            "pendingOrders": snapshot.get("pendingOrders", []),
            "snapshotMeta": snapshot.get("snapshotMeta"),
            "staleSnapshot": stale_snapshot,
            "smart": SMART_SCALP_SERVICE.snapshot_state(),
            "broker": TRADE_GATEWAY.status(),
            "serverTimeMs": now_ms(),
        }
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.get("/api/trade/history")
def trade_history(
    limit: int = Query(TRADE_HISTORY_DEFAULT_LIMIT, ge=1, le=TRADE_HISTORY_MAX_LIMIT),
    username: str = Depends(require_trade_auth),
) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        payload = TRADE_GATEWAY.history(limit=clamp_int(limit, 1, TRADE_HISTORY_MAX_LIMIT))
        volume_info = dict(payload.get("volumeInfo") or {})
        volume_info["defaultLotSize"] = float(TRADE_DEFAULT_LOT_SIZE)
        payload["volumeInfo"] = volume_info
        payload["broker"] = TRADE_GATEWAY.status()
        payload["smart"] = SMART_SCALP_SERVICE.snapshot_state()
        payload["serverTimeMs"] = now_ms()
        return payload
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.post("/api/trade/order/market")
def trade_order_market(payload: TradeMarketOrderRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        volume = trade_volume_from_request(payload)
        result = TRADE_GATEWAY.place_market_order(
            side=payload.side,
            volume=volume,
            stop_loss=payload.stopLoss,
            take_profit=payload.takeProfit,
        )
        _audit_trade_action(
            action="market_order",
            source="manual",
            reason="Manual market order submitted.",
            side=payload.side,
            volume=volume,
            lot_size=payload.lotSize,
        )
        SMART_SCALP_SERVICE.reset(reason="Manual market order submitted. Smart Close restored.", restore_close_preference=True)
        return {
            "ok": True,
            "result": result,
            "submittedVolume": volume,
            "submittedLotSize": payload.lotSize,
            "smart": SMART_SCALP_SERVICE.snapshot_state(),
            "broker": TRADE_GATEWAY.status(),
            "serverTimeMs": now_ms(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.post("/api/trade/position/close")
def trade_position_close(payload: TradePositionCloseRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        result = TRADE_GATEWAY.close_position(position_id=payload.positionId, volume=payload.volume)
        _audit_trade_action(
            action="close_position",
            source="manual",
            reason="Manual close submitted.",
            position_id=payload.positionId,
            volume=payload.volume,
        )
        SMART_SCALP_SERVICE.reset(reason="Manual close submitted. Smart Close restored.", restore_close_preference=True)
        return {
            "ok": True,
            "result": result,
            "smart": SMART_SCALP_SERVICE.snapshot_state(),
            "broker": TRADE_GATEWAY.status(),
            "serverTimeMs": now_ms(),
        }
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.post("/api/trade/position/amend-sltp")
def trade_position_amend(payload: TradePositionAmendRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    if payload.clearStopLoss and payload.stopLoss is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="stopLoss and clearStopLoss cannot be combined.")
    if payload.clearTakeProfit and payload.takeProfit is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="takeProfit and clearTakeProfit cannot be combined.")
    if payload.stopLoss is None and payload.takeProfit is None and not payload.clearStopLoss and not payload.clearTakeProfit:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least one of stopLoss or takeProfit is required.")
    try:
        result = TRADE_GATEWAY.amend_position_sltp(
            position_id=payload.positionId,
            stop_loss=payload.stopLoss,
            take_profit=payload.takeProfit,
            clear_stop_loss=payload.clearStopLoss,
            clear_take_profit=payload.clearTakeProfit,
        )
        return {"ok": True, "result": result, "broker": TRADE_GATEWAY.status(), "serverTimeMs": now_ms()}
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.get("/api/trade/smart")
def trade_smart_state(username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    return SMART_SCALP_SERVICE.snapshot_state()


@app.post("/api/trade/smart/context")
def trade_smart_context(payload: TradeSmartContextRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    try:
        return SMART_SCALP_SERVICE.set_context(page=payload.page, mode=payload.mode, run=payload.run)
    except Exception as exc:
        _handle_smart_scalp_error(exc)


@app.post("/api/trade/smart/entry")
def trade_smart_entry(payload: TradeSmartEntryArmRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        return SMART_SCALP_SERVICE.arm_entry(side=payload.side, armed=payload.armed)
    except Exception as exc:
        _handle_smart_scalp_error(exc)


@app.post("/api/trade/smart/close")
def trade_smart_close(payload: TradeSmartCloseArmRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        return SMART_SCALP_SERVICE.arm_close(armed=payload.armed)
    except Exception as exc:
        _handle_smart_scalp_error(exc)


@app.post("/api/trade/smart/config")
def trade_smart_config(payload: TradeSmartConfigRequest, username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    try:
        return SMART_SCALP_SERVICE.update_config(payload.model_dump(exclude_none=True))
    except Exception as exc:
        _handle_smart_scalp_error(exc)


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


@app.get("/api/live/rect")
def live_rect_state(mode: str = Query("review", pattern="^(live|review)$")) -> Dict[str, Any]:
    return {"rect": rect_snapshot_for_mode(mode)}


@app.post("/api/live/rect")
def live_rect_create(payload: RectCreateRequest) -> Dict[str, Any]:
    try:
        rect = RECT_PAPER_SERVICE.create_rect(
            mode=payload.mode,
            leftx=payload.leftx,
            rightx=payload.rightx,
            firstprice=payload.firstprice,
            secondprice=payload.secondprice,
            smartcloseenabled=payload.smartcloseenabled,
            metadata=payload.metadata,
        )
        return {"rect": rect}
    except Exception as exc:
        _handle_rect_error(exc)


@app.patch("/api/live/rect/{rect_id}")
def live_rect_update(rect_id: int, payload: RectUpdateRequest) -> Dict[str, Any]:
    try:
        rect = RECT_PAPER_SERVICE.update_rect(
            rect_id=rect_id,
            mode=payload.mode,
            leftx=payload.leftx,
            rightx=payload.rightx,
            firstprice=payload.firstprice,
            secondprice=payload.secondprice,
            smartcloseenabled=payload.smartcloseenabled,
        )
        return {"rect": rect}
    except Exception as exc:
        _handle_rect_error(exc)


@app.post("/api/live/rect/{rect_id}/smart-close")
def live_rect_smart_close(rect_id: int, payload: RectSmartCloseRequest) -> Dict[str, Any]:
    try:
        rect = RECT_PAPER_SERVICE.set_smart_close(rect_id=rect_id, mode=payload.mode, enabled=payload.enabled)
        return {"rect": rect}
    except Exception as exc:
        _handle_rect_error(exc)


@app.post("/api/live/rect/{rect_id}/clear")
def live_rect_clear(rect_id: int, payload: RectModeRequest) -> Dict[str, Any]:
    try:
        RECT_PAPER_SERVICE.clear_rect(rect_id=rect_id, mode=payload.mode)
        return {"rect": None}
    except Exception as exc:
        _handle_rect_error(exc)


@app.post("/api/live/rect/{rect_id}/manual-close")
def live_rect_manual_close(rect_id: int, payload: RectModeRequest) -> Dict[str, Any]:
    try:
        rect = RECT_PAPER_SERVICE.manual_close(rect_id=rect_id, mode=payload.mode)
        return {"rect": rect}
    except Exception as exc:
        _handle_rect_error(exc)


@app.get("/api/backbone/review-start")
def backbone_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/backbone/candles")
def backbone_candles(
    candles: int = Query(DEFAULT_BACKBONE_CANDLE_COUNT, ge=1, le=MAX_BACKBONE_CANDLE_COUNT),
    layer: str = Query(DEFAULT_BACKBONE_LAYER),
    id: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    return load_backbone_candles_payload(count=candles, start_id=id, layer=layer)


@app.get("/api/backbone/detail")
def backbone_detail(
    ticks: int = Query(DEFAULT_BACKBONE_DETAIL_TICKS, ge=1, le=MAX_TICK_WINDOW),
    id: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    return load_backbone_detail_payload(ticks=ticks, start_id=id)


@app.get("/api/bigpicture/bootstrap")
def bigpicture_bootstrap(
    points: int = Query(DEFAULT_BIGPICTURE_POINTS, ge=200, le=MAX_BIGPICTURE_POINTS),
) -> Dict[str, Any]:
    return load_bigpicture_bootstrap_payload(points)


@app.get("/api/bigpicture/window")
def bigpicture_window(
    startTsMs: int = Query(..., ge=1),
    endTsMs: int = Query(..., ge=1),
    points: int = Query(DEFAULT_BIGPICTURE_POINTS, ge=200, le=MAX_BIGPICTURE_POINTS),
) -> Dict[str, Any]:
    return load_bigpicture_window_payload(
        start_ts_ms=startTsMs,
        end_ts_ms=endTsMs,
        points=points,
    )


@app.get("/api/live/bootstrap")
def live_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/live/next")
def live_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/live/previous")
def live_previous(
    beforeId: int = Query(..., ge=1),
    currentLastId: Optional[int] = Query(None, ge=1),
    limit: int = Query(DEFAULT_HISTORY_LIMIT, ge=1, le=MAX_TICK_WINDOW),
    mode: str = Query("live", pattern="^(live|review)$"),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    payload = load_previous_payload(
        before_id=beforeId,
        current_last_id=currentLastId,
        limit=limit,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )
    payload["rect"] = rect_snapshot_for_mode(mode)
    return payload


@app.get("/api/live/stream")
def live_stream(
    afterId: int = Query(0, ge=0),
    afterMavgId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(
            after_id=afterId,
            after_mavg_id=afterMavgId,
            limit=limit,
            window=window,
            show_ticks=showTicks,
            show_events=showEvents,
            show_structure=showStructure,
            show_ranges=showRanges,
            rect_mode="live",
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/live/review-stream")
def live_review_stream(
    afterId: int = Query(0, ge=0),
    endId: int = Query(..., ge=1),
    speed: float = Query(1.0, gt=0),
    window: int = Query(DEFAULT_WINDOW, ge=1, le=MAX_TICK_WINDOW),
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_review_events(
            after_id=afterId,
            end_id=endId,
            speed=speed,
            window=window,
            show_ticks=showTicks,
            show_events=showEvents,
            show_structure=showStructure,
            show_ranges=showRanges,
            rect_mode="review",
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )




@app.get("/api/backbone/bootstrap")
def backbone_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_BACKBONE_REVIEW_WINDOW, ge=1, le=MAX_BACKBONE_REVIEW_WINDOW),
    showTicks: bool = Query(False),
) -> Dict[str, Any]:
    return load_backbone_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        show_ticks=showTicks,
    )


@app.get("/api/backbone/next")
def backbone_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    dayId: Optional[int] = Query(None, ge=1),
    endId: Optional[int] = Query(None, ge=1),
    showTicks: bool = Query(False),
) -> Dict[str, Any]:
    return load_backbone_next_payload(
        after_id=afterId,
        limit=limit,
        day_id=dayId,
        end_id=endId,
        show_ticks=showTicks,
    )


@app.get("/api/backbone/stream")
def backbone_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    showTicks: bool = Query(False),
) -> StreamingResponse:
    return StreamingResponse(
        stream_backbone_events(
            after_id=afterId,
            limit=limit,
            show_ticks=showTicks,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

