from __future__ import annotations

import copy
import logging
import os
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from twisted.internet import reactor

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAmendPositionSLTPReq,
    ProtoOAApplicationAuthReq,
    ProtoOAClosePositionReq,
    ProtoOADealListReq,
    ProtoOAErrorRes,
    ProtoOAExecutionEvent,
    ProtoOAGetPositionUnrealizedPnLReq,
    ProtoOANewOrderReq,
    ProtoOAOrderErrorEvent,
    ProtoOAReconcileReq,
    ProtoOASymbolByIdReq,
    ProtoOASymbolsListReq,
    ProtoOATraderReq,
)
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
    BUY,
    MARKET,
    ORDER_STATUS_ACCEPTED,
    POSITION_STATUS_OPEN,
    ProtoOADealStatus,
    ProtoOAOrderStatus,
    ProtoOAOrderType,
    ProtoOAPositionStatus,
    SELL,
)
from datavis.broker_creds import (
    locked_creds_file,
    read_creds_file,
    resolve_creds_file,
    token_tail,
    write_creds_file_atomic,
)
from datavis.ctrader_auth import (
    is_app_already_authorized as shared_is_app_already_authorized,
    is_token_error as shared_is_token_error,
    payload_error_code as shared_payload_error_code,
    payload_message as shared_payload_message,
)


@dataclass
class BrokerConfig:
    client_id: str
    client_secret: str
    account_id: int
    access_token: str
    refresh_token: str
    symbol: str
    symbol_id: Optional[int]
    connection_type: str
    creds_file: Path
    token_source: str

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.account_id and self.access_token)


class TradeGatewayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
    ):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _now_ms() -> int:
    return int(time.time() * 1000)


def load_broker_config(base_dir: Path) -> BrokerConfig:
    creds_file = resolve_creds_file(base_dir)
    creds: Dict[str, Any] = {}
    resolved_creds_file = creds_file
    try:
        creds = read_creds_file(creds_file)
    except Exception:
        creds = {}

    if not creds and not (os.getenv("DATAVIS_CTRADER_CREDS_FILE") or "").strip():
        fallback_creds = Path("~/cTrade/creds.json").expanduser().resolve()
        if fallback_creds != creds_file:
            try:
                fallback_payload = read_creds_file(fallback_creds)
                if fallback_payload:
                    creds = fallback_payload
                    resolved_creds_file = fallback_creds
            except Exception:
                pass

    client_id = (os.getenv("DATAVIS_CTRADER_CLIENT_ID", "").strip() or str(creds.get("clientId", "")).strip())
    client_secret = os.getenv("DATAVIS_CTRADER_CLIENT_SECRET", "").strip() or str(creds.get("clientSecret", "")).strip()
    file_access_token = str(creds.get("accessToken", "")).strip()
    file_refresh_token = str(creds.get("refreshToken", "")).strip()
    env_access_token = os.getenv("DATAVIS_CTRADER_ACCESS_TOKEN", "").strip()
    env_refresh_token = os.getenv("DATAVIS_CTRADER_REFRESH_TOKEN", "").strip()
    access_token = file_access_token or env_access_token
    refresh_token = file_refresh_token or env_refresh_token
    symbol = os.getenv("DATAVIS_CTRADER_SYMBOL", "").strip() or str(creds.get("symbol", "XAUUSD")).strip() or "XAUUSD"

    account_raw = os.getenv("DATAVIS_CTRADER_ACCOUNT_ID", "").strip() or str(creds.get("accountId", "")).strip()
    symbol_id_raw = os.getenv("DATAVIS_CTRADER_SYMBOL_ID", "").strip() or str(creds.get("symbolId", "")).strip()
    connection_type = (
        os.getenv("DATAVIS_CTRADER_CONNECTION_TYPE", "").strip()
        or str(creds.get("connectionType", "live")).strip()
        or "live"
    ).lower()

    account_id = int(account_raw) if account_raw.isdigit() else 0
    symbol_id = int(symbol_id_raw) if symbol_id_raw.isdigit() else None
    if file_access_token or file_refresh_token:
        token_source = "creds_file"
    elif env_access_token or env_refresh_token:
        token_source = "environment"
    else:
        token_source = "missing"

    return BrokerConfig(
        client_id=client_id,
        client_secret=client_secret,
        account_id=account_id,
        access_token=access_token,
        refresh_token=refresh_token,
        symbol=symbol,
        symbol_id=symbol_id,
        connection_type=connection_type if connection_type in {"live", "demo"} else "live",
        creds_file=resolved_creds_file,
        token_source=token_source,
    )


def dt_from_ms(timestamp_ms: Optional[int]) -> Optional[str]:
    if timestamp_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def normalize_money(value: Any, digits: Optional[int]) -> Optional[float]:
    if value is None:
        return None
    try:
        raw = int(value)
    except Exception:
        return None
    if digits is None:
        return float(raw)
    try:
        return float(raw) / float(10 ** int(digits))
    except Exception:
        return float(raw)


class CTraderGateway:
    _reactor_lock = threading.Lock()
    _reactor_started = False

    def __init__(self, config: BrokerConfig):
        self._config = config
        self._logger = logging.getLogger("datavis.trading")
        self._client: Optional[Client] = None
        self._client_lock = threading.RLock()
        self._connected = threading.Event()
        self._app_authed = False
        self._authed = False
        self._symbol_cache: Dict[str, Any] = {}
        self._operation_lock = threading.RLock()
        self._refresh_condition = threading.Condition(threading.Lock())
        self._refresh_in_flight = False
        self._snapshot_condition = threading.Condition(threading.Lock())
        self._snapshot_in_flight = False
        self._last_refresh_result: tuple[bool, Optional[str]] = (False, None)
        self._last_loaded_from_disk_at_ms: Optional[int] = None
        self._last_auth_existing_token_at_ms: Optional[int] = None
        self._last_refresh_attempt_at_ms: Optional[int] = None
        self._last_refresh_success_at_ms: Optional[int] = None
        self._last_token_persist_at_ms: Optional[int] = None
        self._last_token_persist_ok: Optional[bool] = None
        self._last_snapshot: Optional[Dict[str, Any]] = None
        self._last_snapshot_at_ms = 0
        self._last_snapshot_error: Optional[str] = None
        self._last_snapshot_error_at_ms: Optional[int] = None
        self._snapshot_cache_ttl_ms = max(250, int(os.getenv("DATAVIS_TRADE_SNAPSHOT_CACHE_MS", "2500")))
        self._history_snapshot_max_age_ms = max(
            self._snapshot_cache_ttl_ms,
            int(os.getenv("DATAVIS_TRADE_HISTORY_SNAPSHOT_MAX_AGE_MS", "15000")),
        )
        self._trader_money_digits_cache: Optional[int] = None
        self._trader_money_digits_at_ms = 0
        self._trader_info_cache_ttl_ms = max(1000, int(os.getenv("DATAVIS_TRADE_TRADER_INFO_CACHE_MS", "300000")))
        self._last_error: Optional[str] = None
        self._last_error_code: Optional[str] = None
        self._logger.info(
            "cTrader creds initialized creds_file=%s token_source=%s access_tail=%s refresh_tail=%s",
            self._config.creds_file,
            self._config.token_source,
            token_tail(self._config.access_token),
            token_tail(self._config.refresh_token),
        )

    @property
    def configured(self) -> bool:
        return self._config.configured

    @property
    def symbol(self) -> str:
        return self._config.symbol

    @property
    def account_id(self) -> int:
        return self._config.account_id

    def status(self) -> Dict[str, Any]:
        configured = self.configured
        reason = (self._last_error or "").strip() or None
        code = self._last_error_code
        if not configured:
            reason = self._config_reason()
            code = code or "BROKER_NOT_CONFIGURED"
            state = "not_configured"
        elif self._connected.is_set() and self._authed and not reason:
            state = "ready"
        elif reason:
            state = "error"
        else:
            state = "unavailable"
        return {
            "configured": configured,
            "connected": self._connected.is_set(),
            "authenticated": bool(self._authed),
            "ready": state == "ready",
            "state": state,
            "reason": reason,
            "code": code,
            "symbol": self._config.symbol,
            "symbolId": self._config.symbol_id,
            "connectionType": self._config.connection_type,
            "lastError": reason,
        }

    def auth_debug_info(self) -> Dict[str, Any]:
        with self._refresh_condition:
            refresh_in_flight = self._refresh_in_flight
            last_refresh_result = self._last_refresh_result
        return {
            "credsFile": str(self._config.creds_file),
            "tokenSource": self._config.token_source,
            "accessTokenTail": token_tail(self._config.access_token),
            "refreshTokenTail": token_tail(self._config.refresh_token),
            "connected": self._connected.is_set(),
            "authenticated": bool(self._authed),
            "refreshInFlight": refresh_in_flight,
            "lastRefreshResult": {
                "ok": bool(last_refresh_result[0]),
                "message": last_refresh_result[1],
            },
            "lastLoadedFromDiskAtMs": self._last_loaded_from_disk_at_ms,
            "lastAuthExistingTokenAtMs": self._last_auth_existing_token_at_ms,
            "lastRefreshAttemptAtMs": self._last_refresh_attempt_at_ms,
            "lastRefreshSucceededAtMs": self._last_refresh_success_at_ms,
            "lastTokenPersistAtMs": self._last_token_persist_at_ms,
            "lastTokenPersistOk": self._last_token_persist_ok,
            "lastHealthySnapshotAtMs": self._last_snapshot_at_ms or None,
            "lastSnapshotErrorAtMs": self._last_snapshot_error_at_ms,
            "lastSnapshotError": self._last_snapshot_error,
            "snapshotCache": {
                "ttlMs": self._snapshot_cache_ttl_ms,
                "historyMaxAgeMs": self._history_snapshot_max_age_ms,
                "freshUntilMs": (self._last_snapshot_at_ms + self._snapshot_cache_ttl_ms) if self._last_snapshot_at_ms else None,
                "hasSnapshot": self._last_snapshot is not None,
            },
            "traderInfoCache": {
                "moneyDigits": self._trader_money_digits_cache,
                "cachedAtMs": self._trader_money_digits_at_ms or None,
                "ttlMs": self._trader_info_cache_ttl_ms,
            },
            "brokerStatus": self.status(),
        }

    def _config_reason(self) -> str:
        if not self._config.client_id or not self._config.client_secret:
            return "Broker application credentials missing."
        if not self._config.account_id:
            return "Trader account not configured."
        if not self._config.access_token:
            return "Broker token missing."
        return "Broker integration is not configured."

    def _set_error(self, message: str, code: Optional[str] = None) -> None:
        self._last_error = message
        self._last_error_code = code

    def _clear_error(self) -> None:
        self._last_error = None
        self._last_error_code = None

    def _error(
        self,
        message: str,
        *,
        code: Optional[str] = None,
        status_code: Optional[int] = None,
    ) -> TradeGatewayError:
        self._set_error(message, code)
        return TradeGatewayError(message, code=code, status_code=status_code)

    @staticmethod
    def _payload_message(payload: Any) -> str:
        return shared_payload_message(payload)

    @staticmethod
    def _payload_error_code(payload: Any) -> str:
        return shared_payload_error_code(payload)

    def _is_token_error(self, payload: Any) -> bool:
        return shared_is_token_error(payload)

    def _is_app_already_authorized(self, payload: Any) -> bool:
        return shared_is_app_already_authorized(payload)

    def _translate_gateway_error(
        self,
        message: str,
        *,
        error_code: Optional[str] = None,
        default_code: str = "BROKER_ERROR",
        default_status: int = 502,
    ) -> TradeGatewayError:
        normalized = (message or "").strip() or "Trade request failed."
        lowered = normalized.lower()
        resolved_error_code = (error_code or "").strip().upper()
        if resolved_error_code in {"CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"} or "invalid access token" in lowered:
            return self._error("Broker session expired.", code="BROKER_SESSION_EXPIRED", status_code=503)
        if "access denied" in lowered and "credential" in lowered:
            return self._error("Gateway login failed.", code="BROKER_LOGIN_FAILED", status_code=503)
        if "already authorized" in lowered:
            return self._error("Gateway login already active.", code="BROKER_APP_ALREADY_AUTHORIZED", status_code=503)
        if "not found on the broker account" in lowered or "symbol metadata" in lowered:
            return self._error("Symbol not resolved.", code="BROKER_SYMBOL_NOT_RESOLVED", status_code=503)
        if "unable to connect" in lowered or "timed out" in lowered or "disconnected" in lowered:
            return self._error("Broker unavailable.", code="BROKER_UNAVAILABLE", status_code=503)
        return self._error(normalized, code=default_code, status_code=default_status)

    @classmethod
    def _ensure_reactor_started(cls) -> None:
        with cls._reactor_lock:
            if cls._reactor_started:
                return

            def run_reactor() -> None:
                reactor.run(installSignalHandlers=False)

            thread = threading.Thread(target=run_reactor, name="datavis-ctrader-reactor", daemon=True)
            thread.start()
            cls._reactor_started = True

    def _on_connected(self, _: Client) -> None:
        self._connected.set()
        self._app_authed = False
        self._authed = False
        self._clear_error()

    def _on_disconnected(self, _: Client, reason: Any) -> None:
        self._connected.clear()
        self._app_authed = False
        self._authed = False
        self._set_error("Disconnected from cTrader.", "BROKER_DISCONNECTED")
        _ = reason

    def _run_on_reactor(self, fn: Any, *, timeout: float = 8.0) -> Any:
        future: Future = Future()

        def run() -> None:
            try:
                result = fn()
                if not future.done():
                    future.set_result(result)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)

        reactor.callFromThread(run)
        return future.result(timeout=timeout)

    def _ensure_client(self) -> None:
        if not self.configured:
            raise self._error(self._config_reason(), code="BROKER_NOT_CONFIGURED", status_code=503)

        with self._client_lock:
            self._ensure_reactor_started()
            if self._client is None:
                host = EndPoints.PROTOBUF_LIVE_HOST if self._config.connection_type == "live" else EndPoints.PROTOBUF_DEMO_HOST
                self._client = Client(host=host, port=EndPoints.PROTOBUF_PORT, protocol=TcpProtocol)
                self._client.setConnectedCallback(self._on_connected)
                self._client.setDisconnectedCallback(self._on_disconnected)
            self._run_on_reactor(lambda: self._client.startService(), timeout=8.0)

    def _send_proto(self, payload: Any, *, timeout: float = 10.0) -> Any:
        if self._client is None:
            raise TradeGatewayError("cTrader client is not initialized.")

        response_future: Future = Future()

        def dispatch() -> None:
            try:
                deferred = self._client.send(payload, responseTimeoutInSeconds=max(4, int(timeout)))

                def on_ok(message: Any) -> Any:
                    if not response_future.done():
                        response_future.set_result(message)
                    return message

                def on_err(failure: Any) -> Any:
                    message = failure.getErrorMessage() if hasattr(failure, "getErrorMessage") else str(failure)
                    if not response_future.done():
                        response_future.set_exception(TradeGatewayError(message))
                    return failure

                deferred.addCallbacks(on_ok, on_err)
            except Exception as exc:
                if not response_future.done():
                    response_future.set_exception(exc)

        reactor.callFromThread(dispatch)
        try:
            raw = response_future.result(timeout=timeout + 2.0)
        except FutureTimeout as exc:
            raise TradeGatewayError("cTrader request timed out.") from exc
        return Protobuf.extract(raw)

    def _raise_if_error(self, payload: Any) -> None:
        if isinstance(payload, ProtoOAErrorRes):
            raise self._translate_gateway_error(
                self._payload_message(payload),
                error_code=self._payload_error_code(payload),
            )
        if isinstance(payload, ProtoOAOrderErrorEvent):
            raise self._translate_gateway_error(
                self._payload_message(payload),
                error_code=self._payload_error_code(payload),
                default_code="BROKER_ORDER_REJECTED",
            )
        if isinstance(payload, ProtoOAExecutionEvent) and getattr(payload, "errorCode", ""):
            raise self._translate_gateway_error(
                str(getattr(payload, "errorCode", "") or ""),
                error_code=str(getattr(payload, "errorCode", "") or ""),
                default_code="BROKER_EXECUTION_ERROR",
            )

    def _sync_tokens_from_disk(self, *, context: str, log_if_unchanged: bool = False) -> bool:
        path = self._config.creds_file
        try:
            creds = read_creds_file(path)
        except Exception as exc:
            self._logger.warning(
                "cTrader token reload failed context=%s creds_file=%s error=%s",
                context,
                path,
                exc,
            )
            return False

        disk_access_token = str(creds.get("accessToken", "")).strip()
        disk_refresh_token = str(creds.get("refreshToken", "")).strip()
        updated = False
        if disk_access_token and disk_access_token != self._config.access_token:
            self._config.access_token = disk_access_token
            updated = True
        if disk_refresh_token and disk_refresh_token != self._config.refresh_token:
            self._config.refresh_token = disk_refresh_token
            updated = True
        self._last_loaded_from_disk_at_ms = _now_ms()
        if updated:
            self._config.token_source = "creds_file"
            self._logger.info(
                "cTrader token reload updated memory context=%s creds_file=%s access_tail=%s refresh_tail=%s",
                context,
                path,
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
            )
        elif log_if_unchanged:
            self._logger.info(
                "cTrader token reload unchanged context=%s creds_file=%s access_tail=%s refresh_tail=%s",
                context,
                path,
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
            )
        return updated

    def _persist_tokens_locked(self) -> bool:
        path = self._config.creds_file
        try:
            current: Dict[str, Any] = {}
            try:
                current = read_creds_file(path)
            except Exception:
                current = {}
            current["accessToken"] = self._config.access_token
            if self._config.refresh_token:
                current["refreshToken"] = self._config.refresh_token
            write_creds_file_atomic(path, current)
            self._last_token_persist_at_ms = _now_ms()
            self._last_token_persist_ok = True
            self._config.token_source = "creds_file"
            self._logger.info(
                "cTrader token persist succeeded creds_file=%s access_tail=%s refresh_tail=%s",
                path,
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
            )
            return True
        except Exception as exc:
            self._last_token_persist_ok = False
            self._logger.warning(
                "cTrader token persist failed creds_file=%s access_tail=%s refresh_tail=%s error=%s",
                path,
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
                exc,
            )
            return False

    def _refresh_access_token(self) -> tuple[bool, Optional[str]]:
        if not self._config.refresh_token:
            self._logger.warning(
                "cTrader refresh skipped reason=no_refresh_token access_tail=%s refresh_tail=%s",
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
            )
            return False, "Broker session expired."
        if not self._config.client_id or not self._config.client_secret:
            self._logger.warning("cTrader refresh skipped reason=missing_client_credentials")
            return False, "Gateway login failed."

        with self._refresh_condition:
            if self._refresh_in_flight:
                self._logger.info("cTrader refresh waiting for in-flight refresh")
                while self._refresh_in_flight:
                    self._refresh_condition.wait(timeout=30.0)
                return self._last_refresh_result
            self._refresh_in_flight = True

        result: tuple[bool, Optional[str]] = (False, "Broker token refresh failed.")
        try:
            result = self._refresh_access_token_once()
            return result
        finally:
            with self._refresh_condition:
                self._refresh_in_flight = False
                self._last_refresh_result = result
                self._refresh_condition.notify_all()

    def _refresh_access_token_once(self) -> tuple[bool, Optional[str]]:
        request_access_token = self._config.access_token
        request_refresh_token = self._config.refresh_token
        self._last_refresh_attempt_at_ms = _now_ms()
        self._logger.info(
            "cTrader refresh attempted creds_file=%s access_tail=%s refresh_tail=%s",
            self._config.creds_file,
            token_tail(request_access_token),
            token_tail(request_refresh_token),
        )

        try:
            with locked_creds_file(self._config.creds_file):
                disk_access_before = self._config.access_token
                disk_updated = self._sync_tokens_from_disk(context="pre_refresh_lock", log_if_unchanged=False)
                if disk_updated and self._config.access_token != disk_access_before:
                    self._logger.info(
                        "cTrader refresh reused rotated access token from disk creds_file=%s access_tail=%s refresh_tail=%s",
                        self._config.creds_file,
                        token_tail(self._config.access_token),
                        token_tail(self._config.refresh_token),
                    )
                    self._last_refresh_success_at_ms = _now_ms()
                    return True, None

                response = requests.post(
                    EndPoints.TOKEN_URI,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._config.refresh_token,
                        "client_id": self._config.client_id,
                        "client_secret": self._config.client_secret,
                    },
                    timeout=20,
                )
                if response.status_code != 200:
                    self._logger.warning(
                        "cTrader refresh failed status=%s access_tail=%s refresh_tail=%s",
                        response.status_code,
                        token_tail(self._config.access_token),
                        token_tail(self._config.refresh_token),
                    )
                    return False, "Broker token refresh failed."

                payload = response.json()
                error_code = str(payload.get("errorCode", "") or "").strip().upper()
                description = str(payload.get("description", "") or "").strip()
                access_token = str(payload.get("access_token", "")).strip()
                if not access_token:
                    if error_code == "ACCESS_DENIED":
                        self._logger.warning(
                            "cTrader refresh denied access_tail=%s refresh_tail=%s",
                            token_tail(self._config.access_token),
                            token_tail(self._config.refresh_token),
                        )
                        return False, "Broker session expired."
                    if description:
                        self._logger.warning(
                            "cTrader refresh failed description=%s access_tail=%s refresh_tail=%s",
                            description,
                            token_tail(self._config.access_token),
                            token_tail(self._config.refresh_token),
                        )
                        return False, description
                    self._logger.warning(
                        "cTrader refresh failed missing_access_token access_tail=%s refresh_tail=%s",
                        token_tail(self._config.access_token),
                        token_tail(self._config.refresh_token),
                    )
                    return False, "Broker token refresh failed."

                self._config.access_token = access_token
                refreshed = str(payload.get("refresh_token", "")).strip()
                if refreshed:
                    self._config.refresh_token = refreshed

                persisted = self._persist_tokens_locked()
                self._last_refresh_success_at_ms = _now_ms()
                self._logger.info(
                    "cTrader refresh succeeded persisted=%s access_tail=%s refresh_tail=%s",
                    persisted,
                    token_tail(self._config.access_token),
                    token_tail(self._config.refresh_token),
                )
                if not persisted:
                    return False, "Broker token refresh persistence failed."
                return True, None
        except Exception as exc:
            self._logger.warning(
                "cTrader refresh exception access_tail=%s refresh_tail=%s error=%s",
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
                exc,
            )
            return False, "Broker token refresh failed."

    def _authenticate(self) -> None:
        self._sync_tokens_from_disk(context="pre_auth", log_if_unchanged=False)
        if not self._app_authed:
            app_req = ProtoOAApplicationAuthReq()
            app_req.clientId = self._config.client_id
            app_req.clientSecret = self._config.client_secret
            app_res = self._send_proto(app_req, timeout=12.0)
            if isinstance(app_res, ProtoOAErrorRes) and self._is_app_already_authorized(app_res):
                self._app_authed = True
            else:
                self._raise_if_error(app_res)
                self._app_authed = True

        account_req = ProtoOAAccountAuthReq()
        account_req.ctidTraderAccountId = int(self._config.account_id)
        account_req.accessToken = self._config.access_token
        self._last_auth_existing_token_at_ms = _now_ms()
        self._logger.info(
            "cTrader auth using existing access token access_tail=%s refresh_tail=%s",
            token_tail(self._config.access_token),
            token_tail(self._config.refresh_token),
        )
        account_res = self._send_proto(account_req, timeout=12.0)
        if isinstance(account_res, ProtoOAErrorRes) and self._is_token_error(account_res):
            self._logger.warning(
                "cTrader auth existing access token rejected access_tail=%s refresh_tail=%s",
                token_tail(self._config.access_token),
                token_tail(self._config.refresh_token),
            )
            if self._sync_tokens_from_disk(context="auth_token_rejected", log_if_unchanged=False):
                account_req.accessToken = self._config.access_token
                self._logger.info(
                    "cTrader auth retrying with disk token access_tail=%s refresh_tail=%s",
                    token_tail(self._config.access_token),
                    token_tail(self._config.refresh_token),
                )
                account_res = self._send_proto(account_req, timeout=12.0)
            if isinstance(account_res, ProtoOAErrorRes) and self._is_token_error(account_res):
                refreshed, refresh_message = self._refresh_access_token()
                if not refreshed:
                    raise self._error(
                        refresh_message or "Broker session expired.",
                        code="BROKER_SESSION_EXPIRED",
                        status_code=503,
                    )
                account_req.accessToken = self._config.access_token
                account_res = self._send_proto(account_req, timeout=12.0)
                if isinstance(account_res, ProtoOAErrorRes) and self._is_token_error(account_res):
                    raise self._error("Broker session expired.", code="BROKER_SESSION_EXPIRED", status_code=503)
        self._raise_if_error(account_res)
        self._authed = True
        self._clear_error()
        self._logger.info(
            "cTrader auth succeeded access_tail=%s refresh_tail=%s",
            token_tail(self._config.access_token),
            token_tail(self._config.refresh_token),
        )

    def ensure_ready(self) -> None:
        self._ensure_client()
        if not self._connected.wait(timeout=10.0):
            raise self._error("Broker unavailable.", code="BROKER_UNAVAILABLE", status_code=503)

        with self._operation_lock:
            if not self._authed:
                self._authenticate()
                self._symbol_cache = {}

    def _resolve_symbol(self) -> Dict[str, Any]:
        if self._symbol_cache:
            return self._symbol_cache

        self.ensure_ready()

        symbols_req = ProtoOASymbolsListReq()
        symbols_req.ctidTraderAccountId = int(self._config.account_id)
        symbols_req.includeArchivedSymbols = False
        symbols_res = self._send_proto(symbols_req, timeout=12.0)
        self._raise_if_error(symbols_res)

        symbol_id = self._config.symbol_id
        symbol_name_lookup = self._config.symbol.strip().upper()
        if symbol_id is None:
            for item in getattr(symbols_res, "symbol", []):
                if str(getattr(item, "symbolName", "")).strip().upper() == symbol_name_lookup:
                    symbol_id = int(item.symbolId)
                    break
        if symbol_id is None:
            raise self._error("Symbol not resolved.", code="BROKER_SYMBOL_NOT_RESOLVED", status_code=503)

        by_id_req = ProtoOASymbolByIdReq()
        by_id_req.ctidTraderAccountId = int(self._config.account_id)
        by_id_req.symbolId.append(int(symbol_id))
        by_id_res = self._send_proto(by_id_req, timeout=12.0)
        self._raise_if_error(by_id_res)

        symbol_meta = None
        for item in getattr(by_id_res, "symbol", []):
            if int(getattr(item, "symbolId", 0)) == int(symbol_id):
                symbol_meta = item
                break
        if symbol_meta is None:
            raise self._error("Symbol not resolved.", code="BROKER_SYMBOL_NOT_RESOLVED", status_code=503)

        self._config.symbol_id = int(symbol_id)
        self._symbol_cache = {
            "symbolId": int(symbol_id),
            "symbolName": self._config.symbol,
            "digits": int(getattr(symbol_meta, "digits", 2)),
            "minVolume": int(getattr(symbol_meta, "minVolume", 1) or 1),
            "stepVolume": int(getattr(symbol_meta, "stepVolume", 1) or 1),
            "lotSize": int(getattr(symbol_meta, "lotSize", 0) or 0),
            "pipPosition": int(getattr(symbol_meta, "pipPosition", 0) or 0),
        }
        return self._symbol_cache

    def _trader_money_digits(self) -> Optional[int]:
        cache_age_ms = _now_ms() - int(self._trader_money_digits_at_ms or 0)
        if self._trader_money_digits_at_ms and cache_age_ms >= 0 and cache_age_ms <= self._trader_info_cache_ttl_ms:
            return self._trader_money_digits_cache
        trader_req = ProtoOATraderReq()
        trader_req.ctidTraderAccountId = int(self._config.account_id)
        trader_res = self._send_proto(trader_req, timeout=10.0)
        self._raise_if_error(trader_res)
        trader = getattr(trader_res, "trader", None)
        if trader is None:
            return None
        try:
            if trader.HasField("moneyDigits"):
                self._trader_money_digits_cache = int(trader.moneyDigits)
                self._trader_money_digits_at_ms = _now_ms()
                return self._trader_money_digits_cache
        except Exception:
            return None
        return None

    def _reconcile(self) -> Any:
        self.ensure_ready()
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        payload = self._send_proto(req, timeout=12.0)
        self._raise_if_error(payload)
        return payload

    def _unrealized_map(self) -> Dict[int, Dict[str, Optional[float]]]:
        req = ProtoOAGetPositionUnrealizedPnLReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        payload = self._send_proto(req, timeout=12.0)
        self._raise_if_error(payload)
        money_digits = int(getattr(payload, "moneyDigits", 0))
        mapped: Dict[int, Dict[str, Optional[float]]] = {}
        for item in getattr(payload, "positionUnrealizedPnL", []):
            position_id = int(getattr(item, "positionId", 0))
            mapped[position_id] = {
                "grossUnrealizedPnl": normalize_money(getattr(item, "grossUnrealizedPnL", None), money_digits),
                "netUnrealizedPnl": normalize_money(getattr(item, "netUnrealizedPnL", None), money_digits),
            }
        return mapped

    @staticmethod
    def _trade_side_name(side: int) -> str:
        return "buy" if int(side) == int(BUY) else "sell"

    @staticmethod
    def _enum_name(enum_obj: Any, value: int) -> str:
        try:
            return enum_obj.Name(int(value))
        except Exception:
            return str(value)

    @staticmethod
    def _volume_to_lots(volume: Any, lot_size: Any) -> Optional[float]:
        try:
            volume_value = int(volume)
            lot_size_value = int(lot_size)
        except Exception:
            return None
        if lot_size_value <= 0:
            return None
        return round(float(volume_value) / float(lot_size_value), 8)

    def _record_healthy_snapshot(self, snapshot: Dict[str, Any]) -> None:
        self._last_snapshot = copy.deepcopy(snapshot)
        self._last_snapshot_at_ms = int(snapshot.get("snapshotMeta", {}).get("lastHealthyAtMs") or _now_ms())
        self._last_snapshot_error = None
        self._last_snapshot_error_at_ms = None

    def _cached_snapshot_copy(self, *, max_age_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if self._last_snapshot is None:
            return None
        ttl_ms = self._snapshot_cache_ttl_ms if max_age_ms is None else max(0, int(max_age_ms))
        if ttl_ms <= 0:
            return None
        age_ms = _now_ms() - int(self._last_snapshot_at_ms or 0)
        if age_ms < 0 or age_ms > ttl_ms:
            return None
        return copy.deepcopy(self._last_snapshot)

    def _invalidate_snapshot_cache(self) -> None:
        with self._snapshot_condition:
            self._last_snapshot_at_ms = 0
            self._snapshot_condition.notify_all()

    def snapshot_or_last_known(self, *, max_age_ms: Optional[int] = None) -> tuple[Dict[str, Any], bool]:
        try:
            return self.snapshot(max_age_ms=max_age_ms), False
        except Exception as exc:
            self._last_snapshot_error = str(exc) or "Broker snapshot failed."
            self._last_snapshot_error_at_ms = _now_ms()
            error_code = str(getattr(exc, "code", "") or "").strip().upper()
            if self._last_snapshot is None or error_code in {
                "BROKER_LOGIN_FAILED",
                "BROKER_NOT_CONFIGURED",
                "BROKER_SESSION_EXPIRED",
            }:
                raise
            self._logger.warning(
                "cTrader snapshot fallback using last healthy snapshot last_healthy_at_ms=%s error=%s",
                self._last_snapshot_at_ms,
                self._last_snapshot_error,
            )
            fallback = copy.deepcopy(self._last_snapshot)
            snapshot_meta = dict(fallback.get("snapshotMeta") or {})
            snapshot_meta["stale"] = True
            snapshot_meta["lastHealthyAtMs"] = self._last_snapshot_at_ms or None
            snapshot_meta["fallbackReason"] = self._last_snapshot_error
            fallback["snapshotMeta"] = snapshot_meta
            return fallback, True

    def symbol_info(self) -> Dict[str, Any]:
        symbol = self._resolve_symbol()
        lot_size = int(symbol.get("lotSize") or 0)
        min_volume = int(symbol.get("minVolume") or 1)
        step_volume = int(symbol.get("stepVolume") or 1)
        return {
            "symbol": self._config.symbol,
            "symbolId": symbol["symbolId"],
            "symbolDigits": symbol["digits"],
            "minVolume": min_volume,
            "stepVolume": step_volume,
            "lotSize": lot_size,
            "minLotSize": self._volume_to_lots(min_volume, lot_size),
            "lotStep": self._volume_to_lots(step_volume, lot_size),
        }

    def snapshot(self, *, max_age_ms: Optional[int] = None, force: bool = False) -> Dict[str, Any]:
        cached = None if force else self._cached_snapshot_copy(max_age_ms=max_age_ms)
        if cached is not None:
            return cached
        with self._snapshot_condition:
            cached = None if force else self._cached_snapshot_copy(max_age_ms=max_age_ms)
            if cached is not None:
                return cached
            while self._snapshot_in_flight:
                self._snapshot_condition.wait(timeout=5.0)
                cached = None if force else self._cached_snapshot_copy(max_age_ms=max_age_ms)
                if cached is not None:
                    return cached
            self._snapshot_in_flight = True
        try:
            return self._build_snapshot_from_broker()
        finally:
            with self._snapshot_condition:
                self._snapshot_in_flight = False
                self._snapshot_condition.notify_all()

    def _build_snapshot_from_broker(self) -> Dict[str, Any]:
        symbol = self._resolve_symbol()
        reconcile = self._reconcile()
        unrealized = self._unrealized_map()
        lot_size = int(symbol.get("lotSize") or 0)

        positions: List[Dict[str, Any]] = []
        pending_orders: List[Dict[str, Any]] = []

        for item in getattr(reconcile, "position", []):
            status_raw = int(getattr(item, "positionStatus", 0))
            if status_raw != int(POSITION_STATUS_OPEN):
                continue
            trade_data = getattr(item, "tradeData", None)
            if trade_data is None:
                continue
            if int(getattr(trade_data, "symbolId", 0)) != int(symbol["symbolId"]):
                continue
            position_id = int(getattr(item, "positionId", 0))
            pnl = unrealized.get(position_id, {})
            open_timestamp_ms = int(getattr(trade_data, "openTimestamp", 0) or 0)
            positions.append(
                {
                    "positionId": position_id,
                    "side": self._trade_side_name(int(getattr(trade_data, "tradeSide", SELL))),
                    "volume": int(getattr(trade_data, "volume", 0)),
                    "volumeLots": self._volume_to_lots(getattr(trade_data, "volume", 0), lot_size),
                    "symbolId": int(getattr(trade_data, "symbolId", 0)),
                    "symbol": self._config.symbol,
                    "entryPrice": float(getattr(item, "price", 0.0) or 0.0),
                    "stopLoss": float(getattr(item, "stopLoss", 0.0)) if item.HasField("stopLoss") else None,
                    "takeProfit": float(getattr(item, "takeProfit", 0.0)) if item.HasField("takeProfit") else None,
                    "trailingStopLoss": bool(getattr(item, "trailingStopLoss", False)),
                    "openTimestampMs": open_timestamp_ms if open_timestamp_ms > 0 else None,
                    "openTimestamp": dt_from_ms(open_timestamp_ms) if open_timestamp_ms > 0 else None,
                    "positionStatus": self._enum_name(ProtoOAPositionStatus, status_raw),
                    "grossUnrealizedPnl": pnl.get("grossUnrealizedPnl"),
                    "netUnrealizedPnl": pnl.get("netUnrealizedPnl"),
                }
            )

        for order in getattr(reconcile, "order", []):
            trade_data = getattr(order, "tradeData", None)
            if trade_data is None:
                continue
            if int(getattr(trade_data, "symbolId", 0)) != int(symbol["symbolId"]):
                continue
            status_raw = int(getattr(order, "orderStatus", 0))
            if status_raw != int(ORDER_STATUS_ACCEPTED):
                continue
            open_timestamp_ms = int(getattr(trade_data, "openTimestamp", 0) or 0)
            pending_orders.append(
                {
                    "orderId": int(getattr(order, "orderId", 0)),
                    "positionId": int(getattr(order, "positionId", 0)) if order.HasField("positionId") else None,
                    "side": self._trade_side_name(int(getattr(trade_data, "tradeSide", SELL))),
                    "volume": int(getattr(trade_data, "volume", 0)),
                    "volumeLots": self._volume_to_lots(getattr(trade_data, "volume", 0), lot_size),
                    "orderType": self._enum_name(ProtoOAOrderType, int(getattr(order, "orderType", 0))),
                    "orderStatus": self._enum_name(ProtoOAOrderStatus, status_raw),
                    "limitPrice": float(getattr(order, "limitPrice", 0.0)) if order.HasField("limitPrice") else None,
                    "stopPrice": float(getattr(order, "stopPrice", 0.0)) if order.HasField("stopPrice") else None,
                    "stopLoss": float(getattr(order, "stopLoss", 0.0)) if order.HasField("stopLoss") else None,
                    "takeProfit": float(getattr(order, "takeProfit", 0.0)) if order.HasField("takeProfit") else None,
                    "timestampMs": open_timestamp_ms if open_timestamp_ms > 0 else None,
                    "timestamp": dt_from_ms(open_timestamp_ms) if open_timestamp_ms > 0 else None,
                }
            )

        positions.sort(key=lambda item: int(item["positionId"]))
        pending_orders.sort(key=lambda item: int(item["orderId"]))
        snapshot_at_ms = _now_ms()
        snapshot = {
            "symbol": self._config.symbol,
            "symbolId": symbol["symbolId"],
            "symbolDigits": symbol["digits"],
            "volumeInfo": self.symbol_info(),
            "positions": positions,
            "pendingOrders": pending_orders,
            "snapshotMeta": {
                "stale": False,
                "lastHealthyAtMs": snapshot_at_ms,
            },
        }
        self._record_healthy_snapshot(snapshot)
        return snapshot

    def place_market_order(
        self,
        *,
        side: str,
        volume: int,
        stop_loss: Optional[float],
        take_profit: Optional[float],
    ) -> Dict[str, Any]:
        if side not in {"buy", "sell"}:
            raise self._error("Order side must be buy or sell.", code="INVALID_ORDER_SIDE", status_code=400)
        symbol = self._resolve_symbol()
        step = int(symbol.get("stepVolume") or 1)
        min_volume = int(symbol.get("minVolume") or 1)
        if volume < min_volume:
            raise self._error(f"Volume must be at least {min_volume}.", code="INVALID_VOLUME", status_code=400)
        if step > 1 and volume % step != 0:
            raise self._error(f"Volume must be a multiple of {step}.", code="INVALID_VOLUME", status_code=400)

        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        req.symbolId = int(symbol["symbolId"])
        req.orderType = int(MARKET)
        req.tradeSide = int(BUY if side == "buy" else SELL)
        req.volume = int(volume)
        if stop_loss is not None:
            req.stopLoss = float(stop_loss)
        if take_profit is not None:
            req.takeProfit = float(take_profit)

        payload = self._send_proto(req, timeout=12.0)
        self._raise_if_error(payload)
        self._clear_error()
        self._invalidate_snapshot_cache()
        return self._normalize_execution_payload(payload)

    def close_position(self, *, position_id: int, volume: int) -> Dict[str, Any]:
        self.ensure_ready()
        if position_id <= 0:
            raise self._error("Position id is required.", code="INVALID_POSITION_ID", status_code=400)
        if volume <= 0:
            raise self._error("Close volume must be greater than zero.", code="INVALID_VOLUME", status_code=400)

        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        req.positionId = int(position_id)
        req.volume = int(volume)
        payload = self._send_proto(req, timeout=12.0)
        self._raise_if_error(payload)
        self._clear_error()
        self._invalidate_snapshot_cache()
        return self._normalize_execution_payload(payload)

    def amend_position_sltp(
        self,
        *,
        position_id: int,
        stop_loss: Optional[float],
        take_profit: Optional[float],
        clear_stop_loss: bool = False,
        clear_take_profit: bool = False,
    ) -> Dict[str, Any]:
        self.ensure_ready()
        if position_id <= 0:
            raise self._error("Position id is required.", code="INVALID_POSITION_ID", status_code=400)
        if stop_loss is None and take_profit is None and not clear_stop_loss and not clear_take_profit:
            raise self._error("At least one of stopLoss or takeProfit is required.", code="INVALID_PROTECTION_EDIT", status_code=400)

        req = ProtoOAAmendPositionSLTPReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        req.positionId = int(position_id)
        if stop_loss is not None:
            req.stopLoss = float(stop_loss)
        elif clear_stop_loss:
            req.stopLoss = 0.0
        if take_profit is not None:
            req.takeProfit = float(take_profit)
        elif clear_take_profit:
            req.takeProfit = 0.0
        payload = self._send_proto(req, timeout=12.0)
        self._raise_if_error(payload)
        self._clear_error()
        self._invalidate_snapshot_cache()
        return self._normalize_execution_payload(payload)

    def history(self, *, limit: int) -> Dict[str, Any]:
        self.ensure_ready()
        symbol = self._resolve_symbol()
        snapshot, _ = self.snapshot_or_last_known(max_age_ms=self._history_snapshot_max_age_ms)
        lot_size = int(symbol.get("lotSize") or 0)
        open_position_ids = {int(item["positionId"]) for item in snapshot.get("positions", [])}
        now = datetime.now(tz=timezone.utc)
        from_ts = int((now - timedelta(days=30)).timestamp() * 1000)
        to_ts = int(now.timestamp() * 1000)

        req = ProtoOADealListReq()
        req.ctidTraderAccountId = int(self._config.account_id)
        req.fromTimestamp = from_ts
        req.toTimestamp = to_ts
        req.maxRows = max(20, min(500, int(limit) * 4))
        payload = self._send_proto(req, timeout=15.0)
        self._raise_if_error(payload)

        trader_digits = self._trader_money_digits()
        deals: List[Dict[str, Any]] = []
        for deal in getattr(payload, "deal", []):
            if int(getattr(deal, "symbolId", 0)) != int(symbol["symbolId"]):
                continue
            exec_ts = int(getattr(deal, "executionTimestamp", 0) or 0)
            deal_digits = int(getattr(deal, "moneyDigits", trader_digits or 0))
            close_detail = getattr(deal, "closePositionDetail", None) if deal.HasField("closePositionDetail") else None
            realized_gross = normalize_money(getattr(close_detail, "grossProfit", None), deal_digits) if close_detail else None
            realized_swap = normalize_money(getattr(close_detail, "swap", None), deal_digits) if close_detail else None
            realized_commission = normalize_money(getattr(close_detail, "commission", None), deal_digits) if close_detail else None
            realized_net = None
            if realized_gross is not None:
                realized_net = float(realized_gross or 0.0)
                if realized_swap is not None:
                    realized_net += float(realized_swap or 0.0)
                if realized_commission is not None:
                    realized_net -= float(realized_commission or 0.0)

            deals.append(
                {
                    "dealId": int(getattr(deal, "dealId", 0)),
                    "orderId": int(getattr(deal, "orderId", 0)),
                    "positionId": int(getattr(deal, "positionId", 0)),
                    "side": self._trade_side_name(int(getattr(deal, "tradeSide", SELL))),
                    "volume": int(getattr(deal, "volume", 0)),
                    "volumeLots": self._volume_to_lots(getattr(deal, "volume", 0), lot_size),
                    "filledVolume": int(getattr(deal, "filledVolume", 0)),
                    "price": float(getattr(deal, "executionPrice", 0.0) or 0.0),
                    "timestampMs": exec_ts if exec_ts > 0 else None,
                    "timestamp": dt_from_ms(exec_ts) if exec_ts > 0 else None,
                    "dealStatus": self._enum_name(ProtoOADealStatus, int(getattr(deal, "dealStatus", 0))),
                    "entryPrice": float(getattr(close_detail, "entryPrice", 0.0) or 0.0) if close_detail else None,
                    "realizedGrossPnl": realized_gross,
                    "realizedSwap": realized_swap,
                    "realizedCommission": realized_commission,
                    "realizedNetPnl": realized_net,
                }
            )

        deals.sort(key=lambda item: int(item.get("timestampMs") or 0))
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for deal in deals:
            grouped.setdefault(int(deal["positionId"]), []).append(deal)

        trades: List[Dict[str, Any]] = []
        for position_id, items in grouped.items():
            ordered = sorted(items, key=lambda item: int(item.get("timestampMs") or 0))
            first = ordered[0]
            close_candidates = [item for item in ordered if item.get("entryPrice") is not None or item.get("realizedNetPnl") is not None]
            closed = bool(close_candidates) and position_id not in open_position_ids
            exit_item = close_candidates[-1] if close_candidates else None
            realized_sum = sum(float(item.get("realizedNetPnl") or 0.0) for item in ordered if item.get("realizedNetPnl") is not None)
            trades.append(
                {
                    "positionId": position_id,
                    "side": first.get("side"),
                    "volume": first.get("volume"),
                    "volumeLots": self._volume_to_lots(first.get("volume"), lot_size),
                    "entryPrice": first.get("price"),
                    "entryTimestampMs": first.get("timestampMs"),
                    "entryTimestamp": first.get("timestamp"),
                    "exitPrice": exit_item.get("price") if exit_item else None,
                    "exitTimestampMs": exit_item.get("timestampMs") if exit_item else None,
                    "exitTimestamp": exit_item.get("timestamp") if exit_item else None,
                    "isOpen": position_id in open_position_ids,
                    "isClosed": closed,
                    "realizedNetPnl": realized_sum if close_candidates else None,
                    "deals": ordered[-8:],
                }
            )

        trades.sort(
            key=lambda item: int(item.get("exitTimestampMs") or item.get("entryTimestampMs") or 0),
            reverse=True,
        )
        if limit > 0:
            trades = trades[:limit]
        deals = sorted(deals, key=lambda item: int(item.get("timestampMs") or 0), reverse=True)[: max(limit * 3, 30)]

        return {
            "symbol": self._config.symbol,
            "symbolId": symbol["symbolId"],
            "volumeInfo": self.symbol_info(),
            "trades": trades,
            "deals": deals,
            "hasMore": bool(getattr(payload, "hasMore", False)),
        }

    @staticmethod
    def _normalize_execution_payload(payload: Any) -> Dict[str, Any]:
        base = {
            "payload": type(payload).__name__,
            "accepted": True,
        }
        if isinstance(payload, ProtoOAExecutionEvent):
            position = getattr(payload, "position", None) if payload.HasField("position") else None
            order = getattr(payload, "order", None) if payload.HasField("order") else None
            deal = getattr(payload, "deal", None) if payload.HasField("deal") else None
            base.update(
                {
                    "executionType": int(getattr(payload, "executionType", 0)),
                    "positionId": int(getattr(position, "positionId", 0)) if position is not None else None,
                    "orderId": int(getattr(order, "orderId", 0)) if order is not None else None,
                    "dealId": int(getattr(deal, "dealId", 0)) if deal is not None else None,
                    "position": {
                        "positionId": int(getattr(position, "positionId", 0)),
                        "price": float(getattr(position, "price", 0.0) or 0.0),
                        "stopLoss": float(getattr(position, "stopLoss", 0.0))
                        if position is not None and position.HasField("stopLoss")
                        else None,
                        "takeProfit": float(getattr(position, "takeProfit", 0.0))
                        if position is not None and position.HasField("takeProfit")
                        else None,
                    }
                    if position is not None
                    else None,
                }
            )
        return base
