from __future__ import annotations

import json
import os
import threading
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
    creds_file: Optional[Path]

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


def load_broker_config(base_dir: Path) -> BrokerConfig:
    creds_candidates: List[Path] = []
    env_creds_file = (os.getenv("DATAVIS_CTRADER_CREDS_FILE") or "").strip()
    if env_creds_file:
        creds_candidates.append(Path(env_creds_file).expanduser())
    creds_candidates.append((Path(base_dir) / "creds.json").resolve())
    home_creds = Path("~/cTrade/creds.json").expanduser()
    if home_creds not in creds_candidates:
        creds_candidates.append(home_creds)

    creds: Dict[str, Any] = {}
    resolved_creds_file: Optional[Path] = None
    for candidate in creds_candidates:
        try:
            if candidate.exists():
                creds = json.loads(candidate.read_text(encoding="utf-8"))
                resolved_creds_file = candidate
                break
        except Exception:
            continue

    client_id = (os.getenv("DATAVIS_CTRADER_CLIENT_ID", "").strip() or str(creds.get("clientId", "")).strip())
    client_secret = os.getenv("DATAVIS_CTRADER_CLIENT_SECRET", "").strip() or str(creds.get("clientSecret", "")).strip()
    access_token = os.getenv("DATAVIS_CTRADER_ACCESS_TOKEN", "").strip() or str(creds.get("accessToken", "")).strip()
    refresh_token = os.getenv("DATAVIS_CTRADER_REFRESH_TOKEN", "").strip() or str(creds.get("refreshToken", "")).strip()
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
        self._client: Optional[Client] = None
        self._client_lock = threading.RLock()
        self._connected = threading.Event()
        self._app_authed = False
        self._authed = False
        self._symbol_cache: Dict[str, Any] = {}
        self._operation_lock = threading.RLock()
        self._last_error: Optional[str] = None
        self._last_error_code: Optional[str] = None

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
        return str(getattr(payload, "description", "") or getattr(payload, "errorCode", "") or "").strip()

    @staticmethod
    def _payload_error_code(payload: Any) -> str:
        return str(getattr(payload, "errorCode", "") or "").strip().upper()

    def _is_token_error(self, payload: Any) -> bool:
        error_code = self._payload_error_code(payload)
        message = self._payload_message(payload).lower()
        return (
            error_code in {"CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"}
            or "invalid access token" in message
            or "access token" in message
        )

    def _is_app_already_authorized(self, payload: Any) -> bool:
        message = self._payload_message(payload).lower()
        return "already authorized" in message

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

    def _refresh_access_token(self) -> tuple[bool, Optional[str]]:
        if not self._config.refresh_token:
            return False, "Broker session expired."
        if not self._config.client_id or not self._config.client_secret:
            return False, "Gateway login failed."

        try:
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
                return False, "Broker token refresh failed."
            payload = response.json()
            error_code = str(payload.get("errorCode", "") or "").strip().upper()
            description = str(payload.get("description", "") or "").strip()
            access_token = str(payload.get("access_token", "")).strip()
            if not access_token:
                if error_code == "ACCESS_DENIED":
                    return False, "Broker session expired."
                if description:
                    return False, description
                return False, "Broker token refresh failed."
            self._config.access_token = access_token
            refreshed = str(payload.get("refresh_token", "")).strip()
            if refreshed:
                self._config.refresh_token = refreshed
            self._persist_tokens()
            return True, None
        except Exception:
            return False, "Broker token refresh failed."

    def _persist_tokens(self) -> None:
        path = self._config.creds_file
        if path is None:
            return
        try:
            current: Dict[str, Any] = {}
            if path.exists():
                current = json.loads(path.read_text(encoding="utf-8"))
            current["accessToken"] = self._config.access_token
            if self._config.refresh_token:
                current["refreshToken"] = self._config.refresh_token
            path.write_text(json.dumps(current, indent=2), encoding="utf-8")
        except Exception:
            return

    def _authenticate(self) -> None:
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
        trader_req = ProtoOATraderReq()
        trader_req.ctidTraderAccountId = int(self._config.account_id)
        trader_res = self._send_proto(trader_req, timeout=10.0)
        self._raise_if_error(trader_res)
        trader = getattr(trader_res, "trader", None)
        if trader is None:
            return None
        try:
            if trader.HasField("moneyDigits"):
                return int(trader.moneyDigits)
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

    def snapshot(self) -> Dict[str, Any]:
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
        return {
            "symbol": self._config.symbol,
            "symbolId": symbol["symbolId"],
            "symbolDigits": symbol["digits"],
            "volumeInfo": self.symbol_info(),
            "positions": positions,
            "pendingOrders": pending_orders,
        }

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
        return self._normalize_execution_payload(payload)

    def history(self, *, limit: int) -> Dict[str, Any]:
        self.ensure_ready()
        symbol = self._resolve_symbol()
        snapshot = self.snapshot()
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
