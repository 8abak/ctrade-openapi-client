#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import secrets
import base64
import hmac
import hashlib
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import psycopg2.extras
import sqlparse
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from datavis.auction import AuctionService, AuctionStateStore, current_session_window
from datavis.db import db_connect as shared_db_connect
from datavis.rects import RectPaperService, RectServiceError
from datavis.separation import LEVELS as SEPARATION_LEVELS
from datavis.separation import brokerday_for_timestamp
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
DEFAULT_AUCTION_WINDOW = int(os.getenv("DATAVIS_AUCTION_WINDOW", "2000"))
MAX_AUCTION_WINDOW = int(os.getenv("DATAVIS_AUCTION_MAX_WINDOW", "10000"))
DEFAULT_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_WINDOW", "50"))
MAX_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_MAX_WINDOW", "200000"))
MAX_STRUCTURE_SOURCE_TICKS = int(os.getenv("DATAVIS_STRUCTURE_SOURCE_MAX_TICKS", "200000"))
DEFAULT_SEPARATION_WINDOW = int(os.getenv("DATAVIS_SEPARATION_WINDOW", "160"))
MAX_SEPARATION_WINDOW = int(os.getenv("DATAVIS_SEPARATION_MAX_WINDOW", "4000"))
DEFAULT_HISTORY_LIMIT = 2000
DEFAULT_BIGPICTURE_POINTS = 2000
MAX_BIGPICTURE_POINTS = int(os.getenv("DATAVIS_BIGPICTURE_MAX_POINTS", "2400"))
MAX_AUCTION_HISTORY_SESSIONS = int(os.getenv("DATAVIS_AUCTION_HISTORY_MAX_SESSIONS", "96"))
MAX_STREAM_BATCH = 1000
MAX_QUERY_ROWS = int(os.getenv("DATAVIS_SQL_MAX_ROWS", "1000"))
STATEMENT_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_TIMEOUT_MS", "15000"))
LOCK_TIMEOUT_MS = int(os.getenv("DATAVIS_SQL_LOCK_TIMEOUT_MS", "3000"))
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
AUCTION_SNAPSHOT_STREAM_SECONDS = max(
    STREAM_IDLE_POLL_SECONDS,
    float(os.getenv("DATAVIS_AUCTION_SNAPSHOT_STREAM_SECONDS", "0.75")),
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

app = FastAPI(title="datavis.au", version="3.0.0")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
security = HTTPBasic(auto_error=False)
RUNTIME_TRADE_SESSION_SECRET = TRADE_SESSION_SECRET or secrets.token_bytes(32)
BROKER_CONFIG = load_broker_config(BASE_DIR)
TRADE_GATEWAY = CTraderGateway(BROKER_CONFIG)
AUDIT_LOGGER = logging.getLogger("datavis.trade.audit")


class QueryRequest(BaseModel):
    sql: str


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


def serialize_auction_chart_row(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = row["timestamp"]
    mid = float(row["mid"]) if row.get("mid") is not None else None
    spread = float(row["spread"]) if row.get("spread") is not None else None
    return {
        "id": int(row["id"]),
        "timestamp": timestamp.isoformat(),
        "timestampMs": dt_to_ms(timestamp),
        "mid": mid,
        "spread": spread,
    }


def serialize_auction_chart_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_auction_chart_row(row) for row in rows]


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


def list_public_tables() -> Dict[str, Any]:
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
                ORDER BY c.relname ASC
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
    return {"context": context, "tables": tables}


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

    return {
        "success": True,
        "statementCount": len(statements),
        "elapsedMs": elapsed_ms(started),
        "context": context,
        "results": results,
    }


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


def auction_chart_tick_columns() -> str:
    return "id, timestamp, mid, spread"


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
    focus_kind: str = "",
) -> List[Dict[str, Any]]:
    select_sql = tick_columns()
    if mode == "live":
        normalized_focus = str(focus_kind or "").strip().lower()
        if normalized_focus == "brokerday":
            latest_row = query_latest_tick(cur)
            latest_timestamp = latest_row.get("timestamp") if latest_row else None
            if latest_timestamp is None:
                return []
            _, session_start_dt, session_end_dt = current_session_window("brokerday", latest_timestamp)
            cur.execute(
                """
                SELECT {select_sql}
                FROM (
                    SELECT {select_sql}
                    FROM public.ticks
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY id DESC
                    LIMIT %s
                ) recent
                ORDER BY id ASC
                """.format(select_sql=select_sql),
                (TICK_SYMBOL, session_start_dt, session_end_dt, window),
            )
        else:
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


def query_auction_chart_rows_after(
    cur: Any,
    after_id: int,
    limit: int,
) -> List[Dict[str, Any]]:
    select_sql = auction_chart_tick_columns()
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


def serialize_auction_history_ref(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "sessionId": int(row["auctionhistorysessionid"]),
        "refKind": row.get("refkind"),
        "price": float(row["price"]) if row.get("price") is not None else None,
        "strength": float(row["strength"]) if row.get("strength") is not None else None,
        "validFromTs": serialize_value(row.get("validfromts")),
        "validToTs": serialize_value(row.get("validtots")),
        "validFromTsMs": dt_to_ms(row.get("validfromts")),
        "validToTsMs": dt_to_ms(row.get("validtots")),
        "notesJson": row.get("notesjson") or {},
    }


def serialize_auction_history_event(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "sessionId": int(row["auctionhistorysessionid"]),
        "eventTs": serialize_value(row.get("eventts")),
        "eventTsMs": dt_to_ms(row.get("eventts")),
        "eventKind": row.get("eventkind"),
        "price1": float(row["price1"]) if row.get("price1") is not None else None,
        "price2": float(row["price2"]) if row.get("price2") is not None else None,
        "direction": row.get("direction"),
        "strength": float(row["strength"]) if row.get("strength") is not None else None,
        "confirmed": bool(row.get("confirmed")),
        "payload": row.get("payloadjson") or {},
    }


def load_auction_history_payload(
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    include_refs: bool,
    include_events: bool,
    limit_sessions: int,
) -> Dict[str, Any]:
    start_ts = ms_to_dt(start_ts_ms)
    end_ts = ms_to_dt(end_ts_ms)
    if start_ts is None or end_ts is None:
        raise HTTPException(status_code=400, detail="Invalid auction history range.")
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    effective_limit = clamp_int(limit_sessions, 1, MAX_AUCTION_HISTORY_SESSIONS)

    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id, symbol, sessionkind, startts, endts, asofts, windowseconds,
                    openprice, highprice, lowprice, closeprice,
                    pocprice, vahprice, valprice, ibhigh, iblow,
                    statekind, opentype, inventorytype,
                    valuedrift, balancescore, trendscore, transitionscore,
                    summaryjson
                FROM public.auctionhistorysession
                WHERE symbol = %s
                  AND endts >= %s
                  AND startts <= %s
                ORDER BY startts ASC
                LIMIT %s
                """,
                (TICK_SYMBOL, start_ts, end_ts, effective_limit),
            )
            session_rows = [dict(row) for row in cur.fetchall()]
            session_ids = [int(row["id"]) for row in session_rows]
            refs_by_session: Dict[int, List[Dict[str, Any]]] = {session_id: [] for session_id in session_ids}
            events_by_session: Dict[int, List[Dict[str, Any]]] = {session_id: [] for session_id in session_ids}

            if session_ids and include_refs:
                cur.execute(
                    """
                    SELECT
                        id, auctionhistorysessionid, refkind, price, strength,
                        validfromts, validtots, notesjson
                    FROM public.auctionhistoryref
                    WHERE auctionhistorysessionid = ANY(%s)
                    ORDER BY auctionhistorysessionid ASC, validfromts ASC NULLS LAST, id ASC
                    """,
                    (session_ids,),
                )
                for row in cur.fetchall():
                    payload = serialize_auction_history_ref(dict(row))
                    refs_by_session[payload["sessionId"]].append(payload)

            if session_ids and include_events:
                cur.execute(
                    """
                    SELECT
                        id, auctionhistorysessionid, eventts, eventkind, price1, price2,
                        direction, strength, confirmed, payloadjson
                    FROM public.auctionhistoryevent
                    WHERE auctionhistorysessionid = ANY(%s)
                    ORDER BY auctionhistorysessionid ASC, eventts ASC, id ASC
                    """,
                    (session_ids,),
                )
                for row in cur.fetchall():
                    payload = serialize_auction_history_event(dict(row))
                    events_by_session[payload["sessionId"]].append(payload)

    sessions = []
    for row in session_rows:
        session_id = int(row["id"])
        sessions.append(
            {
                "id": session_id,
                "symbol": row.get("symbol"),
                "sessionKind": row.get("sessionkind"),
                "startTs": serialize_value(row.get("startts")),
                "endTs": serialize_value(row.get("endts")),
                "asOfTs": serialize_value(row.get("asofts")),
                "startTsMs": dt_to_ms(row.get("startts")),
                "endTsMs": dt_to_ms(row.get("endts")),
                "asOfTsMs": dt_to_ms(row.get("asofts")),
                "windowSeconds": int(row.get("windowseconds") or 0),
                "openPrice": float(row["openprice"]) if row.get("openprice") is not None else None,
                "highPrice": float(row["highprice"]) if row.get("highprice") is not None else None,
                "lowPrice": float(row["lowprice"]) if row.get("lowprice") is not None else None,
                "closePrice": float(row["closeprice"]) if row.get("closeprice") is not None else None,
                "pocPrice": float(row["pocprice"]) if row.get("pocprice") is not None else None,
                "vahPrice": float(row["vahprice"]) if row.get("vahprice") is not None else None,
                "valPrice": float(row["valprice"]) if row.get("valprice") is not None else None,
                "ibHigh": float(row["ibhigh"]) if row.get("ibhigh") is not None else None,
                "ibLow": float(row["iblow"]) if row.get("iblow") is not None else None,
                "stateKind": row.get("statekind"),
                "openType": row.get("opentype"),
                "inventoryType": row.get("inventorytype"),
                "valueDrift": float(row["valuedrift"]) if row.get("valuedrift") is not None else None,
                "balanceScore": float(row["balancescore"]) if row.get("balancescore") is not None else None,
                "trendScore": float(row["trendscore"]) if row.get("trendscore") is not None else None,
                "transitionScore": float(row["transitionscore"]) if row.get("transitionscore") is not None else None,
                "summary": row.get("summaryjson") or {},
                "refs": refs_by_session.get(session_id, []),
                "events": events_by_session.get(session_id, []),
            }
        )

    return {
        "symbol": TICK_SYMBOL,
        "requestedStartTsMs": start_ts_ms,
        "requestedEndTsMs": end_ts_ms,
        "sessions": sessions,
        "sessionCount": len(sessions),
    }


def build_bigpicture_payload(
    *,
    rows: List[Dict[str, Any]],
    requested_start_ts: Optional[datetime],
    requested_end_ts: Optional[datetime],
    source_bounds: Dict[str, Any],
    global_bounds: Dict[str, Any],
    fetch_ms: float,
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
        "overlays": {
            "auctionRefs": [],
            "events": [],
            "rectangles": [],
            "structureLines": [],
        },
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=0.0,
        latest_row=last_row,
    )
    return payload


def load_bigpicture_bootstrap_payload(points: int) -> Dict[str, Any]:
    target_points = clamp_int(points, 200, MAX_BIGPICTURE_POINTS)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            global_bounds = query_tick_bounds(cur)
            rows = query_bootstrap_rows(cur, mode="live", start_id=None, window=target_points, end_id=None)
    return build_bigpicture_payload(
        rows=rows,
        requested_start_ts=rows[0]["timestamp"] if rows else None,
        requested_end_ts=rows[-1]["timestamp"] if rows else None,
        source_bounds={"row_count": len(rows)},
        global_bounds=global_bounds,
        fetch_ms=elapsed_ms(fetch_started),
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
    if not rows:
        raise HTTPException(status_code=404, detail="No ticks were found for the requested big picture range.")
    return build_bigpicture_payload(
        rows=rows,
        requested_start_ts=start_ts,
        requested_end_ts=end_ts,
        source_bounds=source_bounds,
        global_bounds=global_bounds,
        fetch_ms=elapsed_ms(fetch_started),
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


def structure_item_entries(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for bar in snapshot.get("structureBars", []):
        entries.append(
            {
                "kind": "structure",
                "id": bar.get("id"),
                "startTickId": bar.get("startTickId"),
                "endTickId": bar.get("endTickId"),
                "startTimestamp": bar.get("startTimestamp"),
                "endTimestamp": bar.get("endTimestamp"),
                "startTimestampMs": bar.get("startTimestampMs"),
                "endTimestampMs": bar.get("endTimestampMs"),
            }
        )
    for box in snapshot.get("rangeBoxes", []):
        entries.append(
            {
                "kind": "range",
                "id": box.get("id"),
                "startTickId": box.get("startTickId"),
                "endTickId": box.get("endTickId"),
                "startTimestamp": box.get("startTimestamp"),
                "endTimestamp": box.get("endTimestamp"),
                "startTimestampMs": box.get("startTimestampMs"),
                "endTimestampMs": box.get("endTimestampMs"),
            }
        )
    return sorted(
        entries,
        key=lambda item: (
            int(item.get("endTickId") or 0),
            int(item.get("startTickId") or 0),
            0 if item.get("kind") == "structure" else 1,
            int(item.get("id") or 0),
        ),
    )


def structure_item_count(snapshot: Dict[str, Any]) -> int:
    return len(snapshot.get("structureBars", [])) + len(snapshot.get("rangeBoxes", []))


def trim_structure_snapshot(snapshot: Dict[str, Any], item_window: int, *, side: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    entries = structure_item_entries(snapshot)
    if not entries:
        return empty_structure_payload(), {
            "itemCount": 0,
            "firstId": None,
            "lastId": None,
            "firstTimestamp": None,
            "lastTimestamp": None,
            "firstTimestampMs": None,
            "lastTimestampMs": None,
        }

    kept_entries = entries[:item_window] if side == "head" else entries[-item_window:]
    kept_structure_ids = {entry["id"] for entry in kept_entries if entry.get("kind") == "structure"}
    kept_range_ids = {entry["id"] for entry in kept_entries if entry.get("kind") == "range"}
    trimmed = {
        "structureBars": [bar for bar in snapshot.get("structureBars", []) if bar.get("id") in kept_structure_ids],
        "rangeBoxes": [box for box in snapshot.get("rangeBoxes", []) if box.get("id") in kept_range_ids],
        "structureEvents": [],
    }

    first_id = min(int(entry.get("startTickId") or 0) for entry in kept_entries) if kept_entries else None
    last_id = max(int(entry.get("endTickId") or 0) for entry in kept_entries) if kept_entries else None
    first_entry = min(
        kept_entries,
        key=lambda entry: (
            int(entry.get("startTimestampMs") or 0),
            int(entry.get("startTickId") or 0),
        ),
    )
    last_entry = max(
        kept_entries,
        key=lambda entry: (
            int(entry.get("endTimestampMs") or 0),
            int(entry.get("endTickId") or 0),
        ),
    )
    if first_id is not None and last_id is not None:
        trimmed["structureEvents"] = [
            event
            for event in snapshot.get("structureEvents", [])
            if first_id <= int(event.get("tickId") or 0) <= last_id
        ]

    return trimmed, {
        "itemCount": len(kept_entries),
        "firstId": first_id,
        "lastId": last_id,
        "firstTimestamp": first_entry.get("startTimestamp"),
        "lastTimestamp": last_entry.get("endTimestamp"),
        "firstTimestampMs": first_entry.get("startTimestampMs"),
        "lastTimestampMs": last_entry.get("endTimestampMs"),
    }


def structure_scan_batch_size(item_window: int) -> int:
    return max(2000, min(20000, max(1, item_window) * 200))


def collect_structure_rows_ending_at(
    cur: Any,
    *,
    end_id: int,
    item_window: int,
    lower_bound_id: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    batch_size = structure_scan_batch_size(item_window)
    max_rows = min(MAX_STRUCTURE_SOURCE_TICKS, max(batch_size, item_window * 4000))
    rows = query_window_ending_at(cur, end_id, min(batch_size, max_rows))
    if lower_bound_id is not None:
        rows = [row for row in rows if int(row["id"]) >= lower_bound_id]
    snapshot = structure_snapshot(rows, enabled=True)

    while structure_item_count(snapshot) < item_window and rows and len(rows) < max_rows:
        first_id = int(rows[0]["id"])
        if lower_bound_id is not None and first_id <= lower_bound_id:
            break
        fetch_limit = min(batch_size, max_rows - len(rows))
        older_rows = query_rows_before(cur, first_id, fetch_limit)
        if lower_bound_id is not None:
            older_rows = [row for row in older_rows if int(row["id"]) >= lower_bound_id]
        if not older_rows:
            break
        rows = older_rows + rows
        snapshot = structure_snapshot(rows, enabled=True)

    return rows, snapshot


def collect_structure_rows_starting_at(
    cur: Any,
    *,
    start_id: int,
    item_window: int,
    end_id: Optional[int],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    batch_size = structure_scan_batch_size(item_window)
    max_rows = min(MAX_STRUCTURE_SOURCE_TICKS, max(batch_size, item_window * 4000))
    rows = query_bootstrap_rows(
        cur,
        mode="review",
        start_id=start_id,
        window=min(batch_size, max_rows),
        end_id=end_id,
    )
    snapshot = structure_snapshot(rows, enabled=True)

    while structure_item_count(snapshot) < item_window and rows and len(rows) < max_rows:
        last_id = int(rows[-1]["id"])
        if end_id is not None and last_id >= end_id:
            break
        fetch_limit = min(batch_size, max_rows - len(rows))
        newer_rows = query_rows_after(cur, last_id, fetch_limit, end_id=end_id)
        if not newer_rows:
            break
        rows.extend(newer_rows)
        snapshot = structure_snapshot(rows, enabled=True)

    return rows, snapshot


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
    return payload


def build_structure_view_payload(
    *,
    mode: str,
    window: int,
    replay_rows: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    trim_side: str,
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    trimmed_snapshot, trimmed_meta = trim_structure_snapshot(snapshot, window, side=trim_side)
    first_row_id = trimmed_meta["firstId"]
    last_row_id = trimmed_meta["lastId"]
    duration_ms = 0
    if trimmed_meta["firstTimestampMs"] is not None and trimmed_meta["lastTimestampMs"] is not None:
        duration_ms = max(0, int(trimmed_meta["lastTimestampMs"]) - int(trimmed_meta["firstTimestampMs"]))
    display_snapshot = apply_structure_flags(
        trimmed_snapshot,
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload = {
        "sourceTickCount": len(replay_rows),
        "itemCount": trimmed_meta["itemCount"],
        "tickSpan": max(0, (int(last_row_id) - int(first_row_id) + 1)) if first_row_id and last_row_id else 0,
        "durationMs": duration_ms,
        "firstId": first_row_id,
        "lastId": last_row_id,
        "firstTimestamp": trimmed_meta["firstTimestamp"],
        "lastTimestamp": trimmed_meta["lastTimestamp"],
        "firstTimestampMs": trimmed_meta["firstTimestampMs"],
        "lastTimestampMs": trimmed_meta["lastTimestampMs"],
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row_id and first_row_id > bounds["firstId"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row_id is not None and last_row_id >= review_end_id),
        **display_snapshot,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=replay_rows[-1] if replay_rows else None,
    )
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
    )
    payload["rect"] = rect_snapshot_for_mode(mode)
    return payload


def load_structure_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
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
            if mode == "review":
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                replay_rows, snapshot = collect_structure_rows_starting_at(
                    cur,
                    start_id=start_id,
                    item_window=effective_window,
                    end_id=review_end_id,
                )
            else:
                live_end_id = int(bounds["lastId"] or 0)
                replay_rows, snapshot = collect_structure_rows_ending_at(
                    cur,
                    end_id=live_end_id,
                    item_window=effective_window,
                )
    return build_structure_view_payload(
        mode=mode,
        window=effective_window,
        replay_rows=replay_rows,
        snapshot=snapshot,
        trim_side="head" if mode == "review" else "tail",
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )


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
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=tick_rows[-1] if tick_rows else None,
        ),
    }
    payload["rect"] = rect_snapshot_for_mode("review" if end_id is not None else "live")
    return payload


def load_structure_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id)
            last_seen_id = int(tick_rows[-1]["id"]) if tick_rows else after_id
            replay_rows, snapshot = (
                collect_structure_rows_ending_at(cur, end_id=last_seen_id, item_window=effective_window)
                if last_seen_id
                else ([], empty_structure_payload())
            )
            bounds_row = query_tick_bounds(cur)
            bounds = {
                "firstId": bounds_row.get("first_id"),
                "lastId": bounds_row.get("last_id"),
            }
    payload = build_structure_view_payload(
        mode="review" if end_id is not None else "live",
        window=effective_window,
        replay_rows=replay_rows,
        snapshot=snapshot,
        trim_side="tail",
        review_end_id=end_id,
        review_end_timestamp=None,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload["endId"] = end_id
    payload["endReached"] = bool(end_id is not None and last_seen_id >= end_id)
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
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=elapsed_ms(serialize_started),
            latest_row=(replay_rows[-1] if replay_rows else (previous_rows[-1] if previous_rows else None)),
        ),
    }
    return payload


def load_structure_previous_payload(
    *,
    before_id: int,
    current_last_id: Optional[int],
    window: int,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_STRUCTURE_WINDOW)
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
            range_end_id = current_last_id
            if range_end_id:
                replay_rows, snapshot = collect_structure_rows_ending_at(
                    cur,
                    end_id=int(range_end_id),
                    item_window=effective_window,
                )
            else:
                replay_rows, snapshot = [], empty_structure_payload()
    payload = build_structure_view_payload(
        mode="review",
        window=effective_window,
        replay_rows=replay_rows,
        snapshot=snapshot,
        trim_side="tail",
        review_end_id=None,
        review_end_timestamp=None,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        show_events=show_events,
        show_structure=show_structure,
        show_ranges=show_ranges,
    )
    payload["beforeId"] = before_id
    return payload


def normalize_separation_levels(levels: str, show_all: bool) -> List[str]:
    if show_all:
        return list(SEPARATION_LEVELS)
    requested = [part.strip().lower() for part in str(levels or "").split(",") if part.strip()]
    filtered = [level for level in requested if level in SEPARATION_LEVELS]
    return filtered or list(SEPARATION_LEVELS)


def separation_segment_columns() -> str:
    return (
        "id, symbol, brokerday, level, status, sourcemode, starttickid, endtickid, "
        "starttime, endtime, startprice, endprice, highprice, lowprice, tickcount, "
        "netmove, rangeprice, pathlength, efficiency, thickness, direction, shapetype, "
        "angle, unitprice, version, createdat, updatedat"
    )


def query_current_separation_brokerday(cur: Any) -> Optional[date]:
    latest = query_latest_tick(cur)
    latest_timestamp = latest.get("timestamp") if latest else None
    if latest_timestamp is None:
        return None
    return brokerday_for_timestamp(latest_timestamp)


def query_separation_bounds(
    cur: Any,
    *,
    levels: List[str],
    include_open: bool,
    brokerday: Optional[date] = None,
) -> Dict[str, Any]:
    where = [
        "symbol = %s",
        "level = ANY(%s)",
    ]
    params: List[Any] = [TICK_SYMBOL, levels]
    if not include_open:
        where.append("status = 'closed'")
    if brokerday is not None:
        where.append("brokerday = %s")
        params.append(brokerday)
    cur.execute(
        """
        SELECT
            MIN(starttickid) AS first_id,
            MAX(endtickid) AS last_id,
            MIN(starttime) AS first_timestamp,
            MAX(endtime) AS last_timestamp
        FROM public.separationsegments
        WHERE {where_sql}
        """.format(where_sql=" AND ".join(where)),
        tuple(params),
    )
    return dict(cur.fetchone() or {})


def query_live_separation_segments(
    cur: Any,
    *,
    levels: List[str],
    window: int,
    include_open: bool,
) -> tuple[Optional[date], List[Dict[str, Any]], Dict[str, Any]]:
    brokerday = query_current_separation_brokerday(cur)
    if brokerday is None:
        return None, [], {"first_id": None, "last_id": None, "first_timestamp": None, "last_timestamp": None}
    select_sql = separation_segment_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.separationsegments
            WHERE symbol = %s
              AND brokerday = %s
              AND level = ANY(%s)
              AND (%s OR status = 'closed')
            ORDER BY endtickid DESC, starttickid DESC, id DESC
            LIMIT %s
        ) recent
        ORDER BY endtickid ASC, starttickid ASC, id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, brokerday, levels, include_open, window),
    )
    rows = [dict(row) for row in cur.fetchall()]
    bounds = query_separation_bounds(cur, levels=levels, include_open=include_open, brokerday=brokerday)
    return brokerday, rows, bounds


def query_review_separation_segments(
    cur: Any,
    *,
    start_id: int,
    window: int,
    levels: List[str],
    include_open: bool,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    select_sql = separation_segment_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM public.separationsegments
        WHERE symbol = %s
          AND endtickid >= %s
          AND level = ANY(%s)
          AND (%s OR status = 'closed')
        ORDER BY endtickid ASC, starttickid ASC, id ASC
        LIMIT %s
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, start_id, levels, include_open, window),
    )
    rows = [dict(row) for row in cur.fetchall()]
    bounds = query_separation_bounds(cur, levels=levels, include_open=include_open)
    return rows, bounds


def query_separation_segments_after(
    cur: Any,
    *,
    after_id: int,
    limit: int,
    levels: List[str],
    include_open: bool,
    end_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    select_sql = separation_segment_columns()
    if end_id is None:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.separationsegments
            WHERE symbol = %s
              AND endtickid > %s
              AND level = ANY(%s)
              AND (%s OR status = 'closed')
            ORDER BY endtickid ASC, starttickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, levels, include_open, limit),
        )
    else:
        cur.execute(
            """
            SELECT {select_sql}
            FROM public.separationsegments
            WHERE symbol = %s
              AND endtickid > %s
              AND endtickid <= %s
              AND level = ANY(%s)
              AND (%s OR status = 'closed')
            ORDER BY endtickid ASC, starttickid ASC, id ASC
            LIMIT %s
            """.format(select_sql=select_sql),
            (TICK_SYMBOL, after_id, end_id, levels, include_open, limit),
        )
    return [dict(row) for row in cur.fetchall()]


def query_separation_segments_before(
    cur: Any,
    *,
    before_id: int,
    limit: int,
    levels: List[str],
    include_open: bool,
) -> List[Dict[str, Any]]:
    select_sql = separation_segment_columns()
    cur.execute(
        """
        SELECT {select_sql}
        FROM (
            SELECT {select_sql}
            FROM public.separationsegments
            WHERE symbol = %s
              AND endtickid < %s
              AND level = ANY(%s)
              AND (%s OR status = 'closed')
            ORDER BY endtickid DESC, starttickid DESC, id DESC
            LIMIT %s
        ) older
        ORDER BY endtickid ASC, starttickid ASC, id ASC
        """.format(select_sql=select_sql),
        (TICK_SYMBOL, before_id, levels, include_open, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def serialize_separation_segment_row(row: Dict[str, Any]) -> Dict[str, Any]:
    starttime = row.get("starttime")
    endtime = row.get("endtime")
    return {
        "id": int(row["id"]),
        "symbol": row.get("symbol", TICK_SYMBOL),
        "brokerday": serialize_value(row.get("brokerday")),
        "level": row.get("level"),
        "status": row.get("status"),
        "sourcemode": row.get("sourcemode"),
        "starttickid": int(row.get("starttickid") or 0),
        "endtickid": int(row.get("endtickid") or 0),
        "starttime": serialize_value(starttime),
        "endtime": serialize_value(endtime),
        "starttimeMs": dt_to_ms(starttime),
        "endtimeMs": dt_to_ms(endtime),
        "startprice": float(row.get("startprice") or 0.0),
        "endprice": float(row.get("endprice") or 0.0),
        "highprice": float(row.get("highprice") or 0.0),
        "lowprice": float(row.get("lowprice") or 0.0),
        "tickcount": int(row.get("tickcount") or 0),
        "netmove": float(row.get("netmove") or 0.0),
        "rangeprice": float(row.get("rangeprice") or 0.0),
        "pathlength": float(row.get("pathlength") or 0.0),
        "efficiency": float(row.get("efficiency") or 0.0),
        "thickness": float(row.get("thickness") or 0.0),
        "direction": row.get("direction"),
        "shapetype": row.get("shapetype"),
        "angle": float(row.get("angle") or 0.0),
        "unitprice": float(row.get("unitprice") or 0.0),
        "version": int(row.get("version") or 0),
        "durationMs": max(0, (dt_to_ms(endtime) or 0) - (dt_to_ms(starttime) or 0)),
    }


def serialize_separation_segment_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [serialize_separation_segment_row(row) for row in rows]


def build_separation_payload(
    *,
    mode: str,
    window: int,
    levels: List[str],
    segments: List[Dict[str, Any]],
    tick_rows: List[Dict[str, Any]],
    bounds: Dict[str, Any],
    brokerday: Optional[date],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    include_open: bool,
    show_ticks: bool,
    fetch_ms: float,
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_segment = segments[0] if segments else None
    last_segment = segments[-1] if segments else None
    first_id = int(first_segment["starttickid"]) if first_segment and first_segment.get("starttickid") is not None else None
    last_id = int(last_segment["endtickid"]) if last_segment and last_segment.get("endtickid") is not None else None
    payload = {
        "segments": serialize_separation_segment_rows(segments),
        "itemCount": len(segments),
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "firstId": first_id,
        "lastId": last_id,
        "firstTimestamp": serialize_value(first_segment.get("starttime") if first_segment else None),
        "lastTimestamp": serialize_value(last_segment.get("endtime") if last_segment else None),
        "firstTimestampMs": dt_to_ms(first_segment.get("starttime") if first_segment else None),
        "lastTimestampMs": dt_to_ms(last_segment.get("endtime") if last_segment else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "brokerday": serialize_value(brokerday),
        "levels": levels,
        "includeOpen": include_open,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("first_id") and first_id and first_id > bounds["first_id"]),
        "endReached": bool(mode == "review" and review_end_id is not None and last_id is not None and last_id >= review_end_id),
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=tick_rows[-1] if tick_rows else query_like_tick_from_segment(last_segment),
    )
    return payload


def query_like_tick_from_segment(segment: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not segment:
        return None
    return {"id": segment.get("endtickid"), "timestamp": segment.get("endtime")}


def load_separation_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    levels: List[str],
    include_open: bool,
    show_ticks: bool,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_SEPARATION_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if mode == "live":
                brokerday, segments, bounds = query_live_separation_segments(
                    cur,
                    levels=levels,
                    window=effective_window,
                    include_open=include_open,
                )
            else:
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                brokerday = None
                segments, bounds = query_review_separation_segments(
                    cur,
                    start_id=start_id,
                    window=effective_window,
                    levels=levels,
                    include_open=include_open,
                )
            tick_rows = []
            if show_ticks and segments:
                tick_rows = query_rows_between(
                    cur,
                    int(segments[0]["starttickid"]),
                    int(segments[-1]["endtickid"]),
                    MAX_TICK_WINDOW,
                )
            tick_bounds = query_tick_bounds(cur)
            review_end_id = tick_bounds.get("last_id") if mode == "review" else None
            review_end_timestamp = tick_bounds.get("last_timestamp") if mode == "review" else None
    return build_separation_payload(
        mode=mode,
        window=effective_window,
        levels=levels,
        segments=segments,
        tick_rows=tick_rows,
        bounds=bounds,
        brokerday=brokerday,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        include_open=include_open,
        show_ticks=show_ticks,
        fetch_ms=elapsed_ms(fetch_started),
    )


def load_separation_next_payload(
    *,
    after_id: int,
    limit: int,
    end_id: Optional[int],
    levels: List[str],
    include_open: bool,
    show_ticks: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            segments = query_separation_segments_after(
                cur,
                after_id=after_id,
                limit=effective_limit,
                levels=levels,
                include_open=include_open,
                end_id=end_id,
            )
            tick_rows = query_rows_after(cur, after_id, effective_limit, end_id=end_id) if show_ticks else []
    last_id = after_id
    if segments:
        last_id = max(last_id, max(int(row.get("endtickid") or 0) for row in segments))
    if tick_rows:
        last_id = max(last_id, int(tick_rows[-1]["id"]))
    return {
        "segments": serialize_separation_segment_rows(segments),
        "itemCount": len(segments),
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "lastId": last_id,
        "endId": end_id,
        "endReached": bool(end_id is not None and last_id >= end_id),
        "levels": levels,
        "includeOpen": include_open,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=0.0,
            latest_row=(tick_rows[-1] if tick_rows else query_like_tick_from_segment(segments[-1] if segments else None)),
        ),
    }


def load_separation_previous_payload(
    *,
    before_id: int,
    limit: int,
    levels: List[str],
    include_open: bool,
    show_ticks: bool,
) -> Dict[str, Any]:
    effective_limit = clamp_int(limit, 1, MAX_SEPARATION_WINDOW)
    fetch_started = time.perf_counter()
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            segments = query_separation_segments_before(
                cur,
                before_id=before_id,
                limit=effective_limit,
                levels=levels,
                include_open=include_open,
            )
            bounds = query_separation_bounds(cur, levels=levels, include_open=include_open)
            tick_rows = []
            if show_ticks and segments:
                tick_rows = query_rows_between(
                    cur,
                    int(segments[0]["starttickid"]),
                    int(segments[-1]["endtickid"]),
                    MAX_TICK_WINDOW,
                )
    first_id = int(segments[0]["starttickid"]) if segments else None
    last_id = int(segments[-1]["endtickid"]) if segments else None
    return {
        "segments": serialize_separation_segment_rows(segments),
        "itemCount": len(segments),
        "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
        "rowCount": len(tick_rows) if show_ticks else 0,
        "firstId": first_id,
        "lastId": last_id,
        "beforeId": before_id,
        "hasMoreLeft": bool(bounds.get("first_id") and first_id and first_id > bounds["first_id"]),
        "levels": levels,
        "includeOpen": include_open,
        "metrics": serialize_metrics_payload(
            fetch_ms=elapsed_ms(fetch_started),
            serialize_ms=0.0,
            latest_row=(tick_rows[-1] if tick_rows else query_like_tick_from_segment(segments[-1] if segments else None)),
        ),
    }


def stream_separation_events(
    *,
    after_id: int,
    limit: int,
    levels: List[str],
    include_open: bool,
    show_ticks: bool,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                while True:
                    fetch_started = time.perf_counter()
                    segments = query_separation_segments_after(
                        cur,
                        after_id=last_id,
                        limit=effective_limit,
                        levels=levels,
                        include_open=include_open,
                    )
                    tick_rows = query_rows_after(cur, last_id, effective_limit) if show_ticks else []
                    fetch_ms = elapsed_ms(fetch_started)
                    if segments or tick_rows:
                        last_id = max(
                            last_id,
                            max((int(row.get("endtickid") or 0) for row in segments), default=last_id),
                            max((int(row.get("id") or 0) for row in tick_rows), default=last_id),
                        )
                        payload = {
                            "segmentUpdates": serialize_separation_segment_rows(segments),
                            "itemCount": len(segments),
                            "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
                            "rowCount": len(tick_rows) if show_ticks else 0,
                            "lastId": last_id,
                            "streamMode": "delta",
                            "levels": levels,
                            "includeOpen": include_open,
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=0.0,
                                latest_row=(tick_rows[-1] if tick_rows else query_like_tick_from_segment(segments[-1] if segments else None)),
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
                            "segmentUpdates": [],
                            "itemCount": 0,
                            "rows": [],
                            "rowCount": 0,
                            "lastId": last_id,
                            "streamMode": "heartbeat",
                            "levels": levels,
                            "includeOpen": include_open,
                            **serialize_metrics_payload(fetch_ms=fetch_ms, serialize_ms=0.0, latest_row=latest_row),
                        }
                        yield format_sse(payload, event_name="heartbeat")
                        last_heartbeat = now
                    time.sleep(idle_sleep)
                    idle_sleep = STREAM_IDLE_POLL_SECONDS
    except GeneratorExit:
        return


def stream_separation_review_events(
    *,
    after_id: int,
    end_id: int,
    speed: float,
    levels: List[str],
    include_open: bool,
    show_ticks: bool,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_batch = clamp_int(REVIEW_STREAM_BATCH, 1, MAX_STREAM_BATCH)
    speed_multiplier = max(0.1, float(speed or 1.0))
    previous_time_ms: Optional[int] = None
    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                while last_id < end_id:
                    fetch_started = time.perf_counter()
                    batch_segments = query_separation_segments_after(
                        cur,
                        after_id=last_id,
                        limit=effective_batch,
                        levels=levels,
                        include_open=include_open,
                        end_id=end_id,
                    )
                    fetch_ms = elapsed_ms(fetch_started)
                    if not batch_segments:
                        latest_row = query_latest_tick(cur)
                        yield format_sse(
                            {
                                "segmentUpdates": [],
                                "itemCount": 0,
                                "rows": [],
                                "rowCount": 0,
                                "lastId": last_id,
                                "endId": end_id,
                                "endReached": True,
                                "streamMode": "complete",
                                "levels": levels,
                                "includeOpen": include_open,
                                **serialize_metrics_payload(fetch_ms=fetch_ms, serialize_ms=0.0, latest_row=latest_row),
                            }
                        )
                        return

                    grouped: Dict[int, List[Dict[str, Any]]] = {}
                    for segment in batch_segments:
                        grouped.setdefault(int(segment.get("endtickid") or 0), []).append(segment)

                    for group_end_id in sorted(grouped):
                        group = grouped[group_end_id]
                        group_time_ms = max((dt_to_ms(item.get("endtime")) or 0) for item in group)
                        delay_ms = 0
                        if previous_time_ms is not None and group_time_ms:
                            delta_ms = max(0, group_time_ms - previous_time_ms)
                            delay_ms = min(REVIEW_MAX_DELAY_MS, int(round(delta_ms / speed_multiplier)))
                        if delay_ms > 0:
                            time.sleep(max(REVIEW_MIN_DELAY_MS, delay_ms) / 1000.0)
                        tick_rows = query_rows_between(cur, last_id + 1, group_end_id, MAX_STREAM_BATCH) if show_ticks else []
                        last_id = max(last_id, group_end_id)
                        previous_time_ms = group_time_ms or previous_time_ms
                        yield format_sse(
                            {
                                "segmentUpdates": serialize_separation_segment_rows(group),
                                "itemCount": len(group),
                                "rows": serialize_tick_rows(tick_rows) if show_ticks else [],
                                "rowCount": len(tick_rows) if show_ticks else 0,
                                "lastId": last_id,
                                "endId": end_id,
                                "endReached": bool(last_id >= end_id),
                                "streamMode": "delta",
                                "levels": levels,
                                "includeOpen": include_open,
                                "playbackDelayMs": delay_ms,
                                **serialize_metrics_payload(
                                    fetch_ms=fetch_ms,
                                    serialize_ms=0.0,
                                    latest_row=(tick_rows[-1] if tick_rows else query_like_tick_from_segment(group[-1])),
                                ),
                            }
                        )
                        if last_id >= end_id:
                            return
    except GeneratorExit:
        return

def normalize_auction_focus_kind(value: str) -> str:
    normalized = (value or "brokerday").strip().lower()
    valid = {"rolling15m", "rolling60m", "rolling240m", "rolling24h", "brokerday", "london", "newyork"}
    return normalized if normalized in valid else "brokerday"


def build_auction_view_payload(
    *,
    mode: str,
    window: int,
    rows: List[Dict[str, Any]],
    review_end_id: Optional[int],
    review_end_timestamp: Optional[datetime],
    bounds: Dict[str, Any],
    fetch_ms: float,
    snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    serialize_started = time.perf_counter()
    first_row = rows[0] if rows else None
    last_row = rows[-1] if rows else None
    payload = {
        "rows": serialize_auction_chart_rows(rows),
        "rowCount": len(rows),
        "firstId": first_row.get("id") if first_row else None,
        "lastId": last_row.get("id") if last_row else None,
        "firstTimestamp": serialize_value(first_row.get("timestamp") if first_row else None),
        "lastTimestamp": serialize_value(last_row.get("timestamp") if last_row else None),
        "firstTimestampMs": dt_to_ms(first_row.get("timestamp") if first_row else None),
        "lastTimestampMs": dt_to_ms(last_row.get("timestamp") if last_row else None),
        "mode": mode,
        "window": window,
        "symbol": TICK_SYMBOL,
        "reviewEndId": review_end_id,
        "reviewEndTimestamp": serialize_value(review_end_timestamp),
        "hasMoreLeft": bool(bounds.get("firstId") and first_row and first_row.get("id") and int(first_row["id"]) > int(bounds["firstId"])),
        "endReached": bool(mode == "review" and review_end_id is not None and last_row and int(last_row["id"]) >= int(review_end_id)),
        "auction": snapshot,
    }
    payload["metrics"] = serialize_metrics_payload(
        fetch_ms=fetch_ms,
        serialize_ms=elapsed_ms(serialize_started),
        latest_row=last_row,
    )
    return payload


def load_auction_bootstrap_payload(
    *,
    mode: str,
    start_id: Optional[int],
    window: int,
    focus_kind: str,
) -> Dict[str, Any]:
    effective_window = clamp_int(window, 1, MAX_AUCTION_WINDOW)
    normalized_focus = normalize_auction_focus_kind(focus_kind)
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
                focus_kind=normalized_focus,
            )
            if mode == "live":
                snapshot = AUCTION_SERVICE.sync_live(focus_kind=normalized_focus)
            else:
                if start_id is None:
                    raise HTTPException(status_code=400, detail="Review mode requires an id value.")
                last_chart_row = rows[-1] if rows else query_tick_by_id(cur, start_id)
                if not last_chart_row:
                    raise HTTPException(status_code=404, detail="Review start could not be resolved.")
                end_ts = last_chart_row["timestamp"]
                context_rows = query_window_ending_at_timestamp(cur, end_ts=end_ts, seconds=48 * 60 * 60)
                snapshot = AUCTION_SERVICE.build_review_snapshot(rows=context_rows, focus_kind=normalized_focus)
    return build_auction_view_payload(
        mode=mode,
        window=effective_window,
        rows=rows,
        review_end_id=review_end_id,
        review_end_timestamp=review_end_timestamp,
        bounds=bounds,
        fetch_ms=elapsed_ms(fetch_started),
        snapshot=snapshot,
    )


def stream_events(
    *,
    after_id: int,
    limit: int,
    window: int,
    show_ticks: bool,
    show_events: bool,
    show_structure: bool,
    show_ranges: bool,
    max_window: int = MAX_TICK_WINDOW,
    seed_by_item_window: bool = False,
    rect_mode: Optional[str] = None,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    effective_window = clamp_int(window, 1, max_window)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    structure_enabled = show_events or show_structure or show_ranges
    engine = StructureEngine(symbol=TICK_SYMBOL) if structure_enabled else None

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if engine is not None and last_id:
                    seed_rows = (
                        collect_structure_rows_ending_at(cur, end_id=last_id, item_window=effective_window)[0]
                        if seed_by_item_window
                        else query_window_ending_at(cur, last_id, effective_window)
                    )
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
                    if tick_rows:
                        serialize_started = time.perf_counter()
                        latest_tick_row = tick_rows[-1]
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

                        last_id = int(latest_tick_row["id"])
                        payload = {
                            "rows": payload_rows,
                            "rowCount": len(payload_rows),
                            "structureBarUpdates": updates["bars"] if show_structure else [],
                            "rangeBoxUpdates": updates["rangeBoxes"] if show_ranges else [],
                            "structureEvents": updates["events"] if show_events else [],
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
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_window = clamp_int(window, 1, MAX_TICK_WINDOW)
    effective_batch = clamp_int(REVIEW_STREAM_BATCH, 1, MAX_STREAM_BATCH)
    speed_multiplier = max(0.1, float(speed or 1.0))
    structure_enabled = show_events or show_structure or show_ranges
    engine = StructureEngine(symbol=TICK_SYMBOL) if structure_enabled else None
    previous_row: Optional[Dict[str, Any]] = None

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
                    if not batch_rows:
                        payload = {
                            "rows": [],
                            "rowCount": 0,
                            "structureBarUpdates": [],
                            "rangeBoxUpdates": [],
                            "structureEvents": [],
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


def stream_auction_tick_events(
    *,
    after_id: int,
    limit: int,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_limit = clamp_int(limit, 1, MAX_STREAM_BATCH)
    last_heartbeat = time.monotonic()
    idle_sleep = STREAM_POLL_SECONDS
    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                while True:
                    fetch_started = time.perf_counter()
                    tick_rows = query_auction_chart_rows_after(cur, last_id, effective_limit)
                    fetch_ms = elapsed_ms(fetch_started)
                    if tick_rows:
                        serialize_started = time.perf_counter()
                        last_id = int(tick_rows[-1]["id"])
                        payload = {
                            "rows": serialize_auction_chart_rows(tick_rows),
                            "rowCount": len(tick_rows),
                            "lastId": last_id,
                            "streamMode": "delta",
                            **serialize_metrics_payload(
                                fetch_ms=fetch_ms,
                                serialize_ms=elapsed_ms(serialize_started),
                                latest_row=tick_rows[-1],
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
                            "lastId": last_id,
                            "streamMode": "heartbeat",
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


def stream_auction_events(
    *,
    focus_kind: str,
) -> Generator[str, None, None]:
    normalized_focus = normalize_auction_focus_kind(focus_kind)
    last_heartbeat = time.monotonic()
    last_snapshot_id: Optional[int] = None
    last_snapshot_ts_ms: Optional[int] = None
    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                while True:
                    build_started = time.perf_counter()
                    snapshot = AUCTION_SERVICE.sync_live(focus_kind=normalized_focus)
                    snapshot_build_ms = elapsed_ms(build_started)
                    latest_row = query_latest_tick(cur)
                    snapshot_id = int(snapshot.get("lastProcessedId") or 0) if snapshot else 0
                    snapshot_ts_ms = int(snapshot.get("asOfTsMs") or 0) if snapshot else 0
                    if snapshot_id != last_snapshot_id or snapshot_ts_ms != last_snapshot_ts_ms:
                        payload = {
                            "streamMode": "snapshot",
                            "auction": snapshot,
                            "snapshotBuildLatencyMs": snapshot_build_ms,
                            **serialize_metrics_payload(
                                fetch_ms=snapshot_build_ms,
                                serialize_ms=0.0,
                                latest_row=latest_row,
                            ),
                        }
                        yield format_sse(payload)
                        last_snapshot_id = snapshot_id
                        last_snapshot_ts_ms = snapshot_ts_ms
                        last_heartbeat = time.monotonic()
                    else:
                        now = time.monotonic()
                        if now - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                            payload = {
                                "streamMode": "heartbeat",
                                "auction": None,
                                "snapshotBuildLatencyMs": snapshot_build_ms,
                                **serialize_metrics_payload(
                                    fetch_ms=snapshot_build_ms,
                                    serialize_ms=0.0,
                                    latest_row=latest_row,
                                ),
                            }
                            yield format_sse(payload, event_name="heartbeat")
                            last_heartbeat = now
                    time.sleep(AUCTION_SNAPSHOT_STREAM_SECONDS)
    except GeneratorExit:
        return


def stream_auction_review_events(
    *,
    after_id: int,
    end_id: int,
    speed: float,
    focus_kind: str,
) -> Generator[str, None, None]:
    last_id = max(0, after_id)
    effective_batch = clamp_int(REVIEW_STREAM_BATCH, 1, MAX_STREAM_BATCH)
    speed_multiplier = max(0.1, float(speed or 1.0))
    normalized_focus = normalize_auction_focus_kind(focus_kind)
    review_store = AuctionStateStore(symbol=TICK_SYMBOL)
    previous_row: Optional[Dict[str, Any]] = None

    try:
        with db_connection(readonly=True, autocommit=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if last_id > 0:
                    anchor_row = query_tick_by_id(cur, last_id)
                    if anchor_row:
                        seed_rows = query_window_ending_at_timestamp(cur, end_ts=anchor_row["timestamp"], seconds=48 * 60 * 60)
                        review_store.apply_rows(seed_rows)
                        previous_row = seed_rows[-1] if seed_rows else anchor_row

                while last_id < end_id:
                    fetch_started = time.perf_counter()
                    batch_rows = query_rows_after(cur, last_id, effective_batch, end_id=end_id)
                    fetch_ms = elapsed_ms(fetch_started)
                    if not batch_rows:
                        payload = {
                            "rows": [],
                            "rowCount": 0,
                            "lastId": last_id,
                            "endId": end_id,
                            "endReached": True,
                            "streamMode": "complete",
                            "auction": review_store.build_snapshot(focus_kind=normalized_focus),
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
                        review_store.apply_rows([row])
                        last_id = int(row["id"])
                        payload = {
                            "rows": serialize_auction_chart_rows([row]),
                            "rowCount": 1,
                            "lastId": last_id,
                            "endId": end_id,
                            "endReached": bool(last_id >= end_id),
                            "streamMode": "delta",
                            "auction": review_store.build_snapshot(focus_kind=normalized_focus),
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
AUCTION_SERVICE = AuctionService(db_factory=db_connection, symbol=TICK_SYMBOL)


def _trade_not_configured() -> bool:
    return not TRADE_GATEWAY.configured


def _handle_trade_gateway_error(exc: Exception) -> None:
    detail = str(exc) or "Trade request failed."
    SMART_SCALP_SERVICE.reset(reason=detail)
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
    AUCTION_SERVICE.start()


@app.on_event("shutdown")
def app_shutdown() -> None:
    SMART_SCALP_SERVICE.stop()
    RECT_PAPER_SERVICE.stop()
    AUCTION_SERVICE.stop()


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/structure", include_in_schema=False)
def structure_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "structure.html")


@app.get("/separation", include_in_schema=False)
def separation_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "separation.html")


@app.get("/auction", include_in_schema=False)
def auction_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "auction.html")


@app.get("/bigpicture", include_in_schema=False)
def bigpicture_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "bigpicture.html")


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
    return list_public_tables()


@app.post("/api/sql/query")
def sql_query(payload: QueryRequest, _: Optional[str] = Depends(require_sql_admin)) -> Dict[str, Any]:
    return execute_query(payload.sql)


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
    SMART_SCALP_SERVICE.reset(reason="Trade session logged out.")
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
        SMART_SCALP_SERVICE.reset(reason="Trade session restored. Smart Close defaults to ON.", restore_close_preference=True)
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


@app.get("/api/trade/pending")
def trade_pending(username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
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
            "volumeInfo": volume_info,
            "pendingOrders": snapshot.get("pendingOrders", []),
            "snapshotMeta": snapshot.get("snapshotMeta"),
            "staleSnapshot": stale_snapshot,
            "broker": TRADE_GATEWAY.status(),
            "serverTimeMs": now_ms(),
        }
    except Exception as exc:
        _handle_trade_gateway_error(exc)


@app.get("/api/trade/debug/auth")
def trade_debug_auth(username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    return {
        "ok": True,
        "debug": TRADE_GATEWAY.auth_debug_info(),
        "smart": SMART_SCALP_SERVICE.snapshot_state(),
        "serverTimeMs": now_ms(),
    }


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
        SMART_SCALP_SERVICE.reset(reason="Manual market order submitted.")
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
        SMART_SCALP_SERVICE.reset(reason="Manual close submitted.")
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


@app.get("/api/structure/review-start")
def structure_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/separation/review-start")
def separation_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/auction/review-start")
def auction_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


@app.get("/api/auction/bootstrap")
def auction_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_AUCTION_WINDOW, ge=1, le=MAX_AUCTION_WINDOW),
    focusKind: str = Query("brokerday", min_length=1, max_length=32),
) -> Dict[str, Any]:
    return load_auction_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        focus_kind=focusKind,
    )


@app.get("/api/auction/history")
def auction_history(
    startTsMs: int = Query(..., ge=1),
    endTsMs: int = Query(..., ge=1),
    includeRefs: bool = Query(True),
    includeEvents: bool = Query(True),
    limitSessions: int = Query(36, ge=1, le=MAX_AUCTION_HISTORY_SESSIONS),
) -> Dict[str, Any]:
    return load_auction_history_payload(
        start_ts_ms=startTsMs,
        end_ts_ms=endTsMs,
        include_refs=includeRefs,
        include_events=includeEvents,
        limit_sessions=limitSessions,
    )


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


@app.get("/api/auction/stream")
def auction_stream(
    focusKind: str = Query("brokerday", min_length=1, max_length=32),
) -> StreamingResponse:
    return StreamingResponse(
        stream_auction_events(
            focus_kind=focusKind,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/auction/tick-stream")
def auction_tick_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(64, ge=1, le=MAX_STREAM_BATCH),
) -> StreamingResponse:
    return StreamingResponse(
        stream_auction_tick_events(
            after_id=afterId,
            limit=limit,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/auction/review-stream")
def auction_review_stream(
    afterId: int = Query(0, ge=0),
    endId: int = Query(..., ge=1),
    speed: float = Query(1.0, gt=0),
    focusKind: str = Query("brokerday", min_length=1, max_length=32),
) -> StreamingResponse:
    return StreamingResponse(
        stream_auction_review_events(
            after_id=afterId,
            end_id=endId,
            speed=speed,
            focus_kind=focusKind,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/structure/bootstrap")
def structure_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/next")
def structure_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/previous")
def structure_previous(
    beforeId: int = Query(..., ge=1),
    currentLastId: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_structure_previous_payload(
        before_id=beforeId,
        current_last_id=currentLastId,
        window=window,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


@app.get("/api/structure/stream")
def structure_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    window: int = Query(DEFAULT_STRUCTURE_WINDOW, ge=1, le=MAX_STRUCTURE_WINDOW),
    showEvents: bool = Query(False),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> StreamingResponse:
    return StreamingResponse(
        stream_events(
            after_id=afterId,
            limit=limit,
            window=window,
            show_ticks=False,
            show_events=showEvents,
            show_structure=showStructure,
            show_ranges=showRanges,
            max_window=MAX_STRUCTURE_WINDOW,
            seed_by_item_window=True,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/separation/status")
def separation_status(
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
) -> Dict[str, Any]:
    resolved_levels = normalize_separation_levels(levels, showAll)
    with db_connection(readonly=True) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            brokerday = query_current_separation_brokerday(cur)
            latest_tick = query_latest_tick(cur)
            bounds = query_separation_bounds(cur, levels=resolved_levels, include_open=includeOpen, brokerday=brokerday)
            state_rows = []
            if brokerday is not None:
                cur.execute(
                    """
                    SELECT *
                    FROM public.separationstate
                    WHERE symbol = %s
                      AND brokerday = %s
                      AND level = ANY(%s)
                    ORDER BY level
                    """,
                    (TICK_SYMBOL, brokerday, resolved_levels),
                )
                state_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT *
                FROM public.separationruns
                WHERE symbol = %s
                ORDER BY startedat DESC
                LIMIT 10
                """,
                (TICK_SYMBOL,),
            )
            runs = [dict(row) for row in cur.fetchall()]
    return {
        "symbol": TICK_SYMBOL,
        "brokerday": serialize_value(brokerday),
        "levels": resolved_levels,
        "includeOpen": includeOpen,
        "latestTickId": latest_tick.get("id") if latest_tick else None,
        "latestTickTimestamp": serialize_value(latest_tick.get("timestamp")) if latest_tick else None,
        "bounds": {
            "firstId": bounds.get("first_id"),
            "lastId": bounds.get("last_id"),
            "firstTimestamp": serialize_value(bounds.get("first_timestamp")),
            "lastTimestamp": serialize_value(bounds.get("last_timestamp")),
        },
        "state": [{key: serialize_value(value) for key, value in row.items()} for row in state_rows],
        "runs": [{key: serialize_value(value) for key, value in row.items()} for row in runs],
        "serverTimeMs": now_ms(),
    }


@app.get("/api/separation/bootstrap")
def separation_bootstrap(
    mode: str = Query("live", pattern="^(live|review)$"),
    id: Optional[int] = Query(None, ge=1),
    window: int = Query(DEFAULT_SEPARATION_WINDOW, ge=1, le=MAX_SEPARATION_WINDOW),
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
    showTicks: bool = Query(False),
) -> Dict[str, Any]:
    return load_separation_bootstrap_payload(
        mode=mode,
        start_id=id,
        window=window,
        levels=normalize_separation_levels(levels, showAll),
        include_open=includeOpen,
        show_ticks=showTicks,
    )


@app.get("/api/separation/next")
def separation_next(
    afterId: int = Query(..., ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    endId: Optional[int] = Query(None, ge=1),
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
    showTicks: bool = Query(False),
) -> Dict[str, Any]:
    return load_separation_next_payload(
        after_id=afterId,
        limit=limit,
        end_id=endId,
        levels=normalize_separation_levels(levels, showAll),
        include_open=includeOpen,
        show_ticks=showTicks,
    )


@app.get("/api/separation/previous")
def separation_previous(
    beforeId: int = Query(..., ge=1),
    limit: int = Query(DEFAULT_SEPARATION_WINDOW, ge=1, le=MAX_SEPARATION_WINDOW),
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
    showTicks: bool = Query(False),
) -> Dict[str, Any]:
    return load_separation_previous_payload(
        before_id=beforeId,
        limit=limit,
        levels=normalize_separation_levels(levels, showAll),
        include_open=includeOpen,
        show_ticks=showTicks,
    )


@app.get("/api/separation/stream")
def separation_stream(
    afterId: int = Query(0, ge=0),
    limit: int = Query(250, ge=1, le=MAX_STREAM_BATCH),
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
    showTicks: bool = Query(False),
) -> StreamingResponse:
    return StreamingResponse(
        stream_separation_events(
            after_id=afterId,
            limit=limit,
            levels=normalize_separation_levels(levels, showAll),
            include_open=includeOpen,
            show_ticks=showTicks,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/separation/review-stream")
def separation_review_stream(
    afterId: int = Query(0, ge=0),
    endId: int = Query(..., ge=1),
    speed: float = Query(1.0, gt=0),
    levels: str = Query("micro,median,macro"),
    showAll: bool = Query(True),
    includeOpen: bool = Query(True),
    showTicks: bool = Query(False),
) -> StreamingResponse:
    return StreamingResponse(
        stream_separation_review_events(
            after_id=afterId,
            end_id=endId,
            speed=speed,
            levels=normalize_separation_levels(levels, showAll),
            include_open=includeOpen,
            show_ticks=showTicks,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
