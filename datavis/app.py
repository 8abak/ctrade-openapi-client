#!/usr/bin/env python3
from __future__ import annotations

import json
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

from datavis.db import db_connect as shared_db_connect
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
DEFAULT_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_WINDOW", "50"))
MAX_STRUCTURE_WINDOW = int(os.getenv("DATAVIS_STRUCTURE_MAX_WINDOW", "200000"))
MAX_STRUCTURE_SOURCE_TICKS = int(os.getenv("DATAVIS_STRUCTURE_SOURCE_MAX_TICKS", "200000"))
DEFAULT_HISTORY_LIMIT = 2000
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
    return build_range_payload(
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
    return {
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
    return {
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

                        last_id = int(latest_tick_row["id"])
                        payload = {
                            "rows": payload_rows,
                            "rowCount": len(payload_rows),
                            "structureBarUpdates": updates["bars"] if show_structure else [],
                            "rangeBoxUpdates": updates["rangeBoxes"] if show_ranges else [],
                            "structureEvents": updates["events"] if show_events else [],
                            "lastId": last_id,
                            "streamMode": "delta",
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
    return TRADE_GATEWAY.snapshot()


def smart_scalp_broker_status() -> Dict[str, Any]:
    return TRADE_GATEWAY.status()


def smart_scalp_place_market_order(*, side: str, volume: float, stop_loss: Optional[float], take_profit: Optional[float]) -> Dict[str, Any]:
    payload = TradeMarketOrderRequest(side=side, lotSize=float(volume), stopLoss=stop_loss, takeProfit=take_profit)
    broker_volume = trade_volume_from_request(payload)
    return TRADE_GATEWAY.place_market_order(
        side=payload.side,
        volume=broker_volume,
        stop_loss=payload.stopLoss,
        take_profit=payload.takeProfit,
    )


def smart_scalp_close_position(*, position_id: int, volume: int) -> Dict[str, Any]:
    return TRADE_GATEWAY.close_position(position_id=position_id, volume=volume)


SMART_SCALP_SERVICE = SmartScalpService(
    symbol=TICK_SYMBOL,
    fetch_ticks_after=smart_scalp_ticks_after,
    fetch_recent_ticks=smart_scalp_recent_ticks,
    fetch_latest_tick=smart_scalp_latest_tick,
    fetch_snapshot=smart_scalp_snapshot,
    fetch_broker_status=smart_scalp_broker_status,
    place_market_order=smart_scalp_place_market_order,
    close_position=smart_scalp_close_position,
    smart_lot_size=float(TRADE_DEFAULT_LOT_SIZE),
)


def _trade_not_configured() -> bool:
    return not TRADE_GATEWAY.configured


def _handle_trade_gateway_error(exc: Exception) -> None:
    detail = str(exc) or "Trade request failed."
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


@app.on_event("startup")
def app_startup() -> None:
    SMART_SCALP_SERVICE.start()


@app.on_event("shutdown")
def app_shutdown() -> None:
    SMART_SCALP_SERVICE.stop()


@app.get("/", include_in_schema=False)
def home_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/live", include_in_schema=False)
def live_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "live.html")


@app.get("/structure", include_in_schema=False)
def structure_page() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "structure.html")


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
        SMART_SCALP_SERVICE.touch_auth()
    return trade_auth_status_payload(authenticated=bool(username), username=username)


@app.get("/api/trade/open")
def trade_open(username: str = Depends(require_trade_auth)) -> Dict[str, Any]:
    _ = username
    if _trade_not_configured():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Broker integration is not configured.")
    try:
        snapshot = TRADE_GATEWAY.snapshot()
        volume_info = dict(snapshot.get("volumeInfo") or {})
        volume_info["defaultLotSize"] = float(TRADE_DEFAULT_LOT_SIZE)
        return {
            "symbol": snapshot.get("symbol"),
            "symbolId": snapshot.get("symbolId"),
            "symbolDigits": snapshot.get("symbolDigits"),
            "volumeInfo": volume_info,
            "positions": snapshot.get("positions", []),
            "pendingOrders": snapshot.get("pendingOrders", []),
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
        snapshot = TRADE_GATEWAY.snapshot()
        volume_info = dict(snapshot.get("volumeInfo") or {})
        volume_info["defaultLotSize"] = float(TRADE_DEFAULT_LOT_SIZE)
        return {
            "symbol": snapshot.get("symbol"),
            "symbolId": snapshot.get("symbolId"),
            "volumeInfo": volume_info,
            "pendingOrders": snapshot.get("pendingOrders", []),
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


@app.get("/api/structure/review-start")
def structure_review_start(
    timestamp: str = Query(..., min_length=1),
    timezoneName: str = Query(DEFAULT_REVIEW_TIMEZONE, min_length=1),
) -> Dict[str, Any]:
    return live_review_start(timestamp=timestamp, timezoneName=timezoneName)


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
    showTicks: bool = Query(True),
    showEvents: bool = Query(True),
    showStructure: bool = Query(True),
    showRanges: bool = Query(True),
) -> Dict[str, Any]:
    return load_previous_payload(
        before_id=beforeId,
        current_last_id=currentLastId,
        limit=limit,
        show_ticks=showTicks,
        show_events=showEvents,
        show_structure=showStructure,
        show_ranges=showRanges,
    )


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
