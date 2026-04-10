from __future__ import annotations

import time
from typing import Any, Callable, Optional

from ctrader_open_api import Protobuf
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAAccountAuthRes,
    ProtoOAAccountDisconnectEvent,
    ProtoOAAccountsTokenInvalidatedEvent,
    ProtoOAApplicationAuthReq,
    ProtoOAApplicationAuthRes,
    ProtoOAClientDisconnectEvent,
    ProtoOAErrorRes,
    ProtoOASpotEvent,
    ProtoOASubscribeSpotsReq,
    ProtoOASubscribeSpotsRes,
)
from datavis.ctrader_auth import (
    CTraderTokenManager,
    is_account_not_authorized,
    is_app_already_authorized,
    is_http_rate_limited,
    is_rate_limit_error,
    is_token_error,
    payload_error_code,
    payload_message,
)
from datavis.broker_creds import token_tail


class ExponentialBackoff:
    def __init__(self, *, initial: float, maximum: float, multiplier: float = 2.0) -> None:
        self._initial = max(0.1, float(initial))
        self._maximum = max(self._initial, float(maximum))
        self._multiplier = max(1.1, float(multiplier))
        self._next = self._initial

    def reset(self) -> None:
        self._next = self._initial

    def next_delay(self) -> float:
        delay = self._next
        self._next = min(self._maximum, max(self._initial, self._next * self._multiplier))
        return delay


class ReconnectRetryPolicy:
    def __init__(self, *, log: Callable[[str], None], initial: float = 1.0, maximum: float = 60.0) -> None:
        self._log = log
        self._initial = max(0.5, float(initial))
        self._maximum = max(self._initial, float(maximum))
        self._forced_until_monotonic = 0.0
        self._last_reason = "startup"
        self._last_logged_signature: Optional[tuple[int, int, str]] = None

    def note_disconnect(self, reason: str) -> None:
        self._last_reason = str(reason or "unknown_disconnect").strip() or "unknown_disconnect"

    def on_connected(self) -> None:
        self._last_logged_signature = None

    def enforce_cooldown(self, *, delay: float, reason: str) -> None:
        target = time.monotonic() + max(0.0, float(delay))
        if target > self._forced_until_monotonic:
            self._forced_until_monotonic = target
        self._last_reason = str(reason or self._last_reason).strip() or self._last_reason

    def __call__(self, failures: int) -> float:
        failures = max(1, int(failures))
        base_delay = min(self._maximum, self._initial * (2 ** max(0, failures - 1)))
        forced_delay = max(0.0, self._forced_until_monotonic - time.monotonic())
        delay = max(base_delay, forced_delay)
        signature = (failures, int(delay * 10), self._last_reason)
        if signature != self._last_logged_signature:
            self._log(f"reconnect scheduled reason={self._last_reason} delay={delay:.1f}s failures={failures}")
            self._last_logged_signature = signature
        return delay


class TickCollectorController:
    def __init__(
        self,
        *,
        client: Any,
        reactor_api: Any,
        stop_event: Any,
        token_manager: CTraderTokenManager,
        account_id: int,
        symbol_id: int,
        connection_type: str,
        on_tick: Callable[[int, int, int], None],
        log: Callable[[str], None],
        reconnect_policy: Optional[ReconnectRetryPolicy] = None,
    ) -> None:
        self._client = client
        self._reactor = reactor_api
        self._stop_event = stop_event
        self._token_manager = token_manager
        self._account_id = int(account_id)
        self._symbol_id = int(symbol_id)
        self._connection_type = str(connection_type or "live")
        self._on_tick = on_tick
        self._log = log
        self._reconnect_policy = reconnect_policy
        self._socket_connected = False
        self._app_authed = False
        self._account_authed = False
        self._subscribed = False
        self._auth_in_flight = False
        self._subscription_in_flight = False
        self._auth_retry_call: Optional[Any] = None
        self._subscription_retry_call: Optional[Any] = None
        self._account_auth_backoff = ExponentialBackoff(initial=3.0, maximum=60.0)
        self._rate_limit_backoff = ExponentialBackoff(initial=5.0, maximum=180.0)
        self._refresh_backoff = ExponentialBackoff(initial=10.0, maximum=300.0)

    def connected(self, _: Any) -> None:
        self._socket_connected = True
        self._app_authed = False
        self._account_authed = False
        self._subscribed = False
        self._auth_in_flight = False
        self._subscription_in_flight = False
        self._cancel_call(self._auth_retry_call)
        self._cancel_call(self._subscription_retry_call)
        self._auth_retry_call = None
        self._subscription_retry_call = None
        if self._reconnect_policy is not None:
            self._reconnect_policy.on_connected()
        self._log(f"socket connected connection={self._connection_type}")
        self._schedule_auth_retry(reason="socket_connected", delay=0.0, replace=True)

    def disconnected(self, _: Any, reason: Any) -> None:
        self._socket_connected = False
        self._app_authed = False
        self._account_authed = False
        self._subscribed = False
        self._auth_in_flight = False
        self._subscription_in_flight = False
        self._cancel_call(self._auth_retry_call)
        self._cancel_call(self._subscription_retry_call)
        self._auth_retry_call = None
        self._subscription_retry_call = None
        reason_text = self._reason_text(reason)
        self._log(f"reconnect reason={reason_text}")
        if self._reconnect_policy is not None:
            self._reconnect_policy.note_disconnect(reason_text)

    def on_message(self, _: Any, message: Any) -> None:
        payload_type = getattr(message, "payloadType", None)
        if payload_type == ProtoOASpotEvent().payloadType:
            self._handle_spot_event(message)
            return

        if payload_type == ProtoOAClientDisconnectEvent().payloadType:
            event = Protobuf.extract(message)
            self._log(f"reconnect reason=client_disconnect:{str(getattr(event, 'reason', '') or 'unknown').strip()}")
            return

        if payload_type == ProtoOAAccountDisconnectEvent().payloadType:
            event = Protobuf.extract(message)
            if int(getattr(event, "ctidTraderAccountId", 0) or 0) == self._account_id:
                self._account_authed = False
                self._subscribed = False
                delay = self._account_auth_backoff.next_delay()
                self._log(
                    f"account disconnect account={self._account_id} cooldown={delay:.1f}s"
                )
                self._schedule_auth_retry(reason="account_disconnect_event", delay=delay, replace=True)
            return

        if payload_type == ProtoOAAccountsTokenInvalidatedEvent().payloadType:
            event = Protobuf.extract(message)
            account_ids = {int(item) for item in getattr(event, "ctidTraderAccountIds", [])}
            if self._account_id in account_ids:
                self._account_authed = False
                self._subscribed = False
                delay = self._account_auth_backoff.next_delay()
                reason = str(getattr(event, "reason", "") or "token_invalidated").strip() or "token_invalidated"
                self._log(f"account token invalidated reason={reason} cooldown={delay:.1f}s")
                self._schedule_auth_retry(reason="token_invalidated_event", delay=delay, replace=True)
            return

        client_msg_id = str(getattr(message, "clientMsgId", "") or "").strip()
        if payload_type == ProtoOAErrorRes().payloadType and not client_msg_id:
            self._handle_async_error(Protobuf.extract(message))

    def _handle_spot_event(self, message: Any) -> None:
        try:
            spot = Protobuf.extract(message)
            self._on_tick(
                int(getattr(spot, "timestamp", 0) or 0),
                int(getattr(spot, "bid", 0) or 0),
                int(getattr(spot, "ask", 0) or 0),
            )
        except Exception as exc:
            self._log(f"spot parse error={exc}")

    def _handle_async_error(self, payload: Any) -> None:
        code = payload_error_code(payload)
        description = payload_message(payload)
        self._log(f"ctrader async error code={code} desc={description}")
        if is_rate_limit_error(payload):
            delay = self._rate_limit_backoff.next_delay()
            self._log(f"rate-limit cooldown stage=async_error delay={delay:.1f}s")
            self._enforce_reconnect_cooldown(delay=delay, reason="async_rate_limited")
            self._schedule_auth_retry(reason="async_rate_limited", delay=delay, replace=True)
            return
        if is_token_error(payload):
            self._account_authed = False
            self._subscribed = False
            delay = self._account_auth_backoff.next_delay()
            self._schedule_auth_retry(reason="async_token_error", delay=delay, replace=True)
            return
        if is_account_not_authorized(payload):
            self._account_authed = False
            self._subscribed = False
            delay = self._account_auth_backoff.next_delay()
            self._schedule_auth_retry(reason="async_account_not_authorized", delay=delay, replace=True)

    def _begin_auth_flow(self, reason: str) -> None:
        self._auth_retry_call = None
        if self._stop_event.is_set() or not self._socket_connected:
            return
        if self._auth_in_flight:
            return
        self._auth_in_flight = True
        self._subscription_in_flight = False
        self._subscribed = False
        self._token_manager.sync_from_disk(context="pre_auth", log=self._log, log_if_unchanged=False)
        if self._app_authed:
            self._send_account_auth(reason=reason, allow_disk_retry=True, allow_refresh_retry=True)
            return
        self._send_app_auth(reason=reason)

    def _send_app_auth(self, *, reason: str) -> None:
        request = ProtoOAApplicationAuthReq()
        request.clientId = self._token_manager.client_id
        request.clientSecret = self._token_manager.client_secret
        deferred = self._client.send(request, responseTimeoutInSeconds=12)
        deferred.addCallback(lambda raw: self._handle_app_auth_response(raw, reason=reason))
        deferred.addErrback(lambda failure: self._handle_request_failure(failure, stage="app_auth", reason=reason))

    def _handle_app_auth_response(self, raw: Any, *, reason: str) -> Any:
        payload = Protobuf.extract(raw)
        if isinstance(payload, ProtoOAErrorRes):
            code = payload_error_code(payload)
            description = payload_message(payload)
            if is_app_already_authorized(payload):
                self._app_authed = True
                self._log("app auth success state=already_logged_in")
                self._send_account_auth(reason=reason, allow_disk_retry=True, allow_refresh_retry=True)
                return raw
            self._auth_in_flight = False
            if is_rate_limit_error(payload):
                delay = self._rate_limit_backoff.next_delay()
                self._log(f"rate-limit cooldown stage=app_auth delay={delay:.1f}s code={code} desc={description}")
                self._enforce_reconnect_cooldown(delay=delay, reason="app_auth_rate_limited")
                self._schedule_auth_retry(reason="app_auth_rate_limited", delay=delay, replace=True)
                return raw
            delay = self._account_auth_backoff.next_delay()
            self._log(f"app auth failed code={code} desc={description} cooldown={delay:.1f}s")
            self._schedule_auth_retry(reason="app_auth_failed", delay=delay, replace=True)
            return raw

        if not isinstance(payload, ProtoOAApplicationAuthRes):
            self._auth_in_flight = False
            delay = self._account_auth_backoff.next_delay()
            self._log(f"app auth failed unexpected_payload={type(payload).__name__} cooldown={delay:.1f}s")
            self._schedule_auth_retry(reason="app_auth_unexpected_payload", delay=delay, replace=True)
            return raw

        self._app_authed = True
        self._log("app auth success")
        self._send_account_auth(reason=reason, allow_disk_retry=True, allow_refresh_retry=True)
        return raw

    def _send_account_auth(self, *, reason: str, allow_disk_retry: bool, allow_refresh_retry: bool) -> None:
        if self._stop_event.is_set() or not self._socket_connected or not self._app_authed:
            self._auth_in_flight = False
            return
        self._token_manager.sync_from_disk(context="pre_account_auth", log=self._log, log_if_unchanged=False)
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = self._account_id
        request.accessToken = self._token_manager.access_token
        deferred = self._client.send(request, responseTimeoutInSeconds=12)
        deferred.addCallback(
            lambda raw: self._handle_account_auth_response(
                raw,
                reason=reason,
                allow_disk_retry=allow_disk_retry,
                allow_refresh_retry=allow_refresh_retry,
            )
        )
        deferred.addErrback(lambda failure: self._handle_request_failure(failure, stage="account_auth", reason=reason))

    def _handle_account_auth_response(
        self,
        raw: Any,
        *,
        reason: str,
        allow_disk_retry: bool,
        allow_refresh_retry: bool,
    ) -> Any:
        payload = Protobuf.extract(raw)
        if isinstance(payload, ProtoOAErrorRes):
            code = payload_error_code(payload)
            description = payload_message(payload)
            if is_rate_limit_error(payload):
                self._auth_in_flight = False
                delay = self._rate_limit_backoff.next_delay()
                self._log(f"rate-limit cooldown stage=account_auth delay={delay:.1f}s code={code} desc={description}")
                self._enforce_reconnect_cooldown(delay=delay, reason="account_auth_rate_limited")
                self._schedule_auth_retry(reason="account_auth_rate_limited", delay=delay, replace=True)
                return raw
            if is_account_not_authorized(payload):
                self._auth_in_flight = False
                self._account_authed = False
                self._subscribed = False
                delay = self._account_auth_backoff.next_delay()
                self._log(
                    f"account auth failed code={code} desc={description} cooldown={delay:.1f}s"
                )
                self._schedule_auth_retry(reason="account_not_authorized", delay=delay, replace=True)
                return raw
            if is_token_error(payload):
                if allow_disk_retry:
                    token_before = self._token_manager.access_token
                    disk_updated = self._token_manager.sync_from_disk(
                        context="account_auth_token_rejected",
                        log=self._log,
                        log_if_unchanged=False,
                    )
                    if disk_updated and self._token_manager.access_token != token_before:
                        self._log(
                            f"account auth retrying with disk token access_tail={token_tail(self._token_manager.access_token)}"
                        )
                        self._send_account_auth(reason=reason, allow_disk_retry=False, allow_refresh_retry=allow_refresh_retry)
                        return raw
                if allow_refresh_retry:
                    self._attempt_refresh_then_retry_account_auth(reason=reason)
                    return raw
                self._auth_in_flight = False
                delay = self._account_auth_backoff.next_delay()
                self._log(f"account auth failed code={code} desc={description} cooldown={delay:.1f}s")
                self._schedule_auth_retry(reason="account_auth_token_rejected", delay=delay, replace=True)
                return raw
            self._auth_in_flight = False
            delay = self._account_auth_backoff.next_delay()
            self._log(f"account auth failed code={code} desc={description} cooldown={delay:.1f}s")
            self._schedule_auth_retry(reason="account_auth_failed", delay=delay, replace=True)
            return raw

        if not isinstance(payload, ProtoOAAccountAuthRes):
            self._auth_in_flight = False
            delay = self._account_auth_backoff.next_delay()
            self._log(f"account auth failed unexpected_payload={type(payload).__name__} cooldown={delay:.1f}s")
            self._schedule_auth_retry(reason="account_auth_unexpected_payload", delay=delay, replace=True)
            return raw

        self._auth_in_flight = False
        self._account_authed = True
        self._account_auth_backoff.reset()
        self._log(f"account auth success account={self._account_id}")
        self._send_subscribe()
        return raw

    def _attempt_refresh_then_retry_account_auth(self, *, reason: str) -> None:
        delay_before_refresh = self._refresh_backoff.next_delay()
        refreshed, refresh_message = self._token_manager.refresh_access_token(
            context="tickcollector_account_auth",
            log=self._log,
        )
        if refreshed:
            self._refresh_backoff.reset()
            self._send_account_auth(reason=reason, allow_disk_retry=False, allow_refresh_retry=False)
            return

        self._auth_in_flight = False
        delay = delay_before_refresh
        if is_http_rate_limited(None, refresh_message):
            rate_limit_delay = self._rate_limit_backoff.next_delay()
            delay = max(delay, rate_limit_delay)
            self._log(f"rate-limit cooldown stage=refresh delay={delay:.1f}s")
            self._enforce_reconnect_cooldown(delay=delay, reason="refresh_rate_limited")
        else:
            self._log(f"refresh failed reason={str(refresh_message or 'unknown').strip()}")
        self._schedule_auth_retry(reason="refresh_failed", delay=delay, replace=True)

    def _send_subscribe(self) -> None:
        if self._stop_event.is_set() or not self._socket_connected or not self._account_authed:
            return
        if self._subscribed or self._subscription_in_flight:
            return
        request = ProtoOASubscribeSpotsReq()
        request.ctidTraderAccountId = self._account_id
        request.symbolId.append(self._symbol_id)
        request.subscribeToSpotTimestamp = True
        self._subscription_in_flight = True
        deferred = self._client.send(request, responseTimeoutInSeconds=12)
        deferred.addCallback(lambda raw: self._handle_subscribe_response(raw))
        deferred.addErrback(lambda failure: self._handle_request_failure(failure, stage="subscribe", reason="subscribe"))

    def _handle_subscribe_response(self, raw: Any) -> Any:
        self._subscription_in_flight = False
        payload = Protobuf.extract(raw)
        if isinstance(payload, ProtoOAErrorRes):
            code = payload_error_code(payload)
            description = payload_message(payload)
            if is_rate_limit_error(payload):
                delay = self._rate_limit_backoff.next_delay()
                self._log(f"rate-limit cooldown stage=subscribe delay={delay:.1f}s code={code} desc={description}")
                self._enforce_reconnect_cooldown(delay=delay, reason="subscribe_rate_limited")
                self._schedule_subscription_retry(reason="subscribe_rate_limited", delay=delay, replace=True)
                return raw
            if is_account_not_authorized(payload):
                self._account_authed = False
                self._subscribed = False
                delay = self._account_auth_backoff.next_delay()
                self._log(f"subscription blocked code={code} desc={description} cooldown={delay:.1f}s")
                self._schedule_auth_retry(reason="subscribe_account_not_authorized", delay=delay, replace=True)
                return raw
            if is_token_error(payload):
                self._account_authed = False
                self._subscribed = False
                self._attempt_refresh_then_retry_account_auth(reason="subscribe_token_error")
                return raw
            delay = self._account_auth_backoff.next_delay()
            self._log(f"subscription failed code={code} desc={description} cooldown={delay:.1f}s")
            self._schedule_subscription_retry(reason="subscribe_failed", delay=delay, replace=True)
            return raw

        if not isinstance(payload, ProtoOASubscribeSpotsRes):
            delay = self._account_auth_backoff.next_delay()
            self._log(f"subscription failed unexpected_payload={type(payload).__name__} cooldown={delay:.1f}s")
            self._schedule_subscription_retry(reason="subscribe_unexpected_payload", delay=delay, replace=True)
            return raw

        self._subscribed = True
        self._log(f"subscription success symbolId={self._symbol_id}")
        return raw

    def _handle_request_failure(self, failure: Any, *, stage: str, reason: str) -> Any:
        message = self._failure_text(failure)
        if stage == "subscribe":
            self._subscription_in_flight = False
        else:
            self._auth_in_flight = False
        if self._stop_event.is_set() or not self._socket_connected:
            return failure
        delay = self._account_auth_backoff.next_delay()
        self._log(f"{stage} request failed reason={message} cooldown={delay:.1f}s")
        if stage == "subscribe":
            self._schedule_subscription_retry(reason=f"{reason}_request_failed", delay=delay, replace=True)
        else:
            self._schedule_auth_retry(reason=f"{reason}_request_failed", delay=delay, replace=True)
        return failure

    def _schedule_auth_retry(self, *, reason: str, delay: float, replace: bool) -> None:
        if self._stop_event.is_set() or not self._socket_connected:
            return
        if self._auth_retry_call is not None and self._call_active(self._auth_retry_call):
            if not replace:
                return
            self._cancel_call(self._auth_retry_call)
        self._auth_retry_call = self._reactor.callLater(max(0.0, float(delay)), self._begin_auth_flow, str(reason))

    def _schedule_subscription_retry(self, *, reason: str, delay: float, replace: bool) -> None:
        if self._stop_event.is_set() or not self._socket_connected or not self._account_authed:
            return
        if self._subscription_retry_call is not None and self._call_active(self._subscription_retry_call):
            if not replace:
                return
            self._cancel_call(self._subscription_retry_call)
        self._subscription_retry_call = self._reactor.callLater(
            max(0.0, float(delay)),
            self._retry_subscription,
            str(reason),
        )

    def _retry_subscription(self, reason: str) -> None:
        self._subscription_retry_call = None
        if self._stop_event.is_set() or not self._socket_connected or not self._account_authed:
            return
        _ = reason
        self._send_subscribe()

    def _enforce_reconnect_cooldown(self, *, delay: float, reason: str) -> None:
        if self._reconnect_policy is not None:
            self._reconnect_policy.enforce_cooldown(delay=delay, reason=reason)

    @staticmethod
    def _call_active(call: Any) -> bool:
        try:
            return bool(call is not None and call.active())
        except Exception:
            return False

    def _cancel_call(self, call: Any) -> None:
        try:
            if call is not None and call.active():
                call.cancel()
        except Exception:
            return

    @staticmethod
    def _reason_text(reason: Any) -> str:
        text = str(reason or "").strip()
        return text or "unknown_disconnect"

    @staticmethod
    def _failure_text(failure: Any) -> str:
        if hasattr(failure, "getErrorMessage"):
            try:
                return str(failure.getErrorMessage() or "").strip() or str(failure)
            except Exception:
                return str(failure)
        return str(failure)
