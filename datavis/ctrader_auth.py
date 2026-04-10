from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import requests

from ctrader_open_api import EndPoints
from datavis.broker_creds import locked_creds_file, read_creds_file, token_tail, write_creds_file_atomic


LogFn = Optional[Callable[[str], None]]


def payload_message(payload: Any) -> str:
    return str(getattr(payload, "description", "") or getattr(payload, "errorCode", "") or "").strip()


def payload_error_code(payload: Any) -> str:
    return str(getattr(payload, "errorCode", "") or "").strip().upper()


def is_token_error(payload: Any) -> bool:
    error_code = payload_error_code(payload)
    message = payload_message(payload).lower()
    return (
        error_code in {"CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"}
        or "invalid access token" in message
        or "access token" in message
    )


def is_app_already_authorized(payload: Any) -> bool:
    return "already authorized" in payload_message(payload).lower()


def is_rate_limit_error(payload: Any) -> bool:
    error_code = payload_error_code(payload)
    message = payload_message(payload).lower()
    return (
        error_code == "BLOCKED_PAYLOAD_TYPE"
        or "too many requests" in message
        or "rate limit" in message
        or "rate limited" in message
    )


def is_account_not_authorized(payload: Any) -> bool:
    message = payload_message(payload).lower()
    return payload_error_code(payload) == "INVALID_REQUEST" and "trading account is not authorized" in message


def is_http_rate_limited(status_code: Optional[int], message: Optional[str] = None) -> bool:
    if int(status_code or 0) == 429:
        return True
    return "too many requests" in str(message or "").lower()


class CTraderTokenManager:
    def __init__(
        self,
        *,
        creds_file: Path,
        client_id: str,
        client_secret: str,
        access_token: str,
        refresh_token: str,
    ) -> None:
        self._creds_file = Path(creds_file)
        self._client_id = str(client_id or "").strip()
        self._client_secret = str(client_secret or "").strip()
        self._access_token = str(access_token or "").strip()
        self._refresh_token = str(refresh_token or "").strip()
        self._refresh_condition = threading.Condition(threading.Lock())
        self._refresh_in_flight = False
        self._last_refresh_result: Tuple[bool, Optional[str]] = (False, "Broker token refresh failed.")

    @property
    def access_token(self) -> str:
        return self._access_token

    @property
    def refresh_token(self) -> str:
        return self._refresh_token

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def client_secret(self) -> str:
        return self._client_secret

    def sync_from_disk(self, *, context: str, log: LogFn = None, log_if_unchanged: bool = False) -> bool:
        try:
            creds = read_creds_file(self._creds_file)
        except Exception as exc:
            if log is not None:
                log(f"token reload failed context={context} error={exc}")
            return False

        disk_access_token = str(creds.get("accessToken", "")).strip()
        disk_refresh_token = str(creds.get("refreshToken", "")).strip()
        updated = False
        if disk_access_token and disk_access_token != self._access_token:
            self._access_token = disk_access_token
            updated = True
        if disk_refresh_token and disk_refresh_token != self._refresh_token:
            self._refresh_token = disk_refresh_token
            updated = True

        if updated and log is not None:
            log(
                f"token reload updated context={context} access_tail={token_tail(self._access_token)} "
                f"refresh_tail={token_tail(self._refresh_token)}"
            )
        elif log_if_unchanged and log is not None:
            log(
                f"token reload unchanged context={context} access_tail={token_tail(self._access_token)} "
                f"refresh_tail={token_tail(self._refresh_token)}"
            )
        return updated

    def refresh_access_token(self, *, context: str, log: LogFn = None) -> Tuple[bool, Optional[str]]:
        if not self._refresh_token:
            if log is not None:
                log(
                    f"refresh skipped reason=no_refresh_token access_tail={token_tail(self._access_token)} "
                    f"refresh_tail={token_tail(self._refresh_token)}"
                )
            return False, "Broker session expired."
        if not self._client_id or not self._client_secret:
            if log is not None:
                log("refresh skipped reason=missing_client_credentials")
            return False, "Gateway login failed."

        with self._refresh_condition:
            if self._refresh_in_flight:
                if log is not None:
                    log("refresh waiting for in-flight refresh")
                while self._refresh_in_flight:
                    self._refresh_condition.wait(timeout=30.0)
                return self._last_refresh_result
            self._refresh_in_flight = True

        result: Tuple[bool, Optional[str]] = (False, "Broker token refresh failed.")
        try:
            result = self._refresh_access_token_once(context=context, log=log)
            return result
        finally:
            with self._refresh_condition:
                self._refresh_in_flight = False
                self._last_refresh_result = result
                self._refresh_condition.notify_all()

    def _refresh_access_token_once(self, *, context: str, log: LogFn = None) -> Tuple[bool, Optional[str]]:
        if log is not None:
            log(
                f"refresh attempted context={context} access_tail={token_tail(self._access_token)} "
                f"refresh_tail={token_tail(self._refresh_token)}"
            )

        try:
            with locked_creds_file(self._creds_file):
                disk_access_before = self._access_token
                disk_updated = self.sync_from_disk(context="pre_refresh_lock", log=log, log_if_unchanged=False)
                if disk_updated and self._access_token != disk_access_before:
                    if log is not None:
                        log(
                            f"refresh reused rotated access token from disk access_tail={token_tail(self._access_token)} "
                            f"refresh_tail={token_tail(self._refresh_token)}"
                        )
                    return True, None

                response = requests.post(
                    EndPoints.TOKEN_URI,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self._refresh_token,
                        "client_id": self._client_id,
                        "client_secret": self._client_secret,
                    },
                    timeout=20,
                )

                response_message: Optional[str] = None
                payload: Dict[str, Any] = {}
                try:
                    payload = response.json()
                except Exception:
                    payload = {}
                if payload:
                    response_message = str(payload.get("description", "") or payload.get("error", "") or "").strip() or None

                if response.status_code != 200:
                    if log is not None:
                        log(
                            f"refresh failed status={response.status_code} access_tail={token_tail(self._access_token)} "
                            f"refresh_tail={token_tail(self._refresh_token)}"
                        )
                    if is_http_rate_limited(response.status_code, response_message):
                        return False, f"HTTP {response.status_code} Too Many Requests"
                    return False, response_message or f"HTTP {response.status_code}"

                error_code = str(payload.get("errorCode", "") or "").strip().upper()
                description = str(payload.get("description", "") or "").strip()
                access_token = str(payload.get("access_token", "")).strip()
                if not access_token:
                    if error_code == "ACCESS_DENIED":
                        if log is not None:
                            log(
                                f"refresh denied access_tail={token_tail(self._access_token)} "
                                f"refresh_tail={token_tail(self._refresh_token)}"
                            )
                        return False, "Broker session expired."
                    if description:
                        if log is not None:
                            log(
                                f"refresh failed description={description} access_tail={token_tail(self._access_token)} "
                                f"refresh_tail={token_tail(self._refresh_token)}"
                            )
                        return False, description
                    if log is not None:
                        log(
                            f"refresh failed missing_access_token access_tail={token_tail(self._access_token)} "
                            f"refresh_tail={token_tail(self._refresh_token)}"
                        )
                    return False, "Broker token refresh failed."

                self._access_token = access_token
                refreshed = str(payload.get("refresh_token", "")).strip()
                if refreshed:
                    self._refresh_token = refreshed

                persisted = self._persist_tokens_locked(log=log)
                if log is not None:
                    log(
                        f"refresh succeeded persisted={str(bool(persisted)).lower()} "
                        f"access_tail={token_tail(self._access_token)} refresh_tail={token_tail(self._refresh_token)}"
                    )
                if not persisted:
                    return False, "Broker token refresh persistence failed."
                return True, None
        except Exception as exc:
            if log is not None:
                log(
                    f"refresh exception access_tail={token_tail(self._access_token)} "
                    f"refresh_tail={token_tail(self._refresh_token)} error={exc}"
                )
            return False, str(exc) or "Broker token refresh failed."

    def _persist_tokens_locked(self, *, log: LogFn = None) -> bool:
        try:
            current: Dict[str, Any] = {}
            try:
                current = read_creds_file(self._creds_file)
            except Exception:
                current = {}
            current["accessToken"] = self._access_token
            if self._refresh_token:
                current["refreshToken"] = self._refresh_token
            write_creds_file_atomic(self._creds_file, current)
            return True
        except Exception as exc:
            if log is not None:
                log(
                    f"token persist failed access_tail={token_tail(self._access_token)} "
                    f"refresh_tail={token_tail(self._refresh_token)} error={exc}"
                )
            return False
