from __future__ import annotations

import shutil
import subprocess
from typing import Any, Dict, Iterable, List

from datavis.control.config import ControlSettings
from datavis.control.models import ServiceSnapshot


class ServiceManager:
    def __init__(self, settings: ControlSettings) -> None:
        self._settings = settings

    def is_supported(self) -> bool:
        return shutil.which("systemctl") is not None

    def status(self, service_name: str) -> ServiceSnapshot:
        if service_name not in self._settings.managed_services:
            raise ValueError(f"unsupported service: {service_name}")
        if not self.is_supported():
            return ServiceSnapshot(name=service_name, status_text="systemctl unavailable", probe_supported=False)
        cmd = [
            "systemctl",
            "show",
            f"{service_name}.service",
            "--property=ActiveState,SubState,Result,NRestarts",
            "--no-pager",
        ]
        completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            return ServiceSnapshot(
                name=service_name,
                active_state="unknown",
                sub_state="unknown",
                status_text=(completed.stderr or completed.stdout or "systemctl probe failed").strip()[:2000],
                probe_supported=True,
            )
        data: Dict[str, str] = {}
        for line in (completed.stdout or "").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip()
        return ServiceSnapshot(
            name=service_name,
            active_state=data.get("ActiveState", "unknown") or "unknown",
            sub_state=data.get("SubState", "unknown") or "unknown",
            status_text=data.get("Result") or None,
            restart_count=int(data.get("NRestarts") or 0),
            probe_supported=True,
        )

    def status_many(self, service_names: Iterable[str]) -> List[ServiceSnapshot]:
        return [self.status(name) for name in service_names]

    def restart(self, service_name: str) -> ServiceSnapshot:
        self._run_service_command("restart", service_name)
        return self.status(service_name)

    def start(self, service_name: str) -> ServiceSnapshot:
        self._run_service_command("start", service_name)
        return self.status(service_name)

    def stop(self, service_name: str) -> ServiceSnapshot:
        self._run_service_command("stop", service_name)
        return self.status(service_name)

    def reset_failed(self, service_name: str, *, ignore_errors: bool = False) -> str | None:
        try:
            self._run_service_command("reset-failed", service_name)
        except RuntimeError as exc:
            if ignore_errors:
                return str(exc)
            raise
        return None

    def restart_with_reset_tolerance(self, service_name: str) -> Dict[str, Any]:
        warning = self.reset_failed(service_name, ignore_errors=True)
        snapshot = self.restart(service_name).model_dump()
        snapshot["requestedAction"] = "restart"
        if warning:
            snapshot["warnings"] = [warning]
        return snapshot

    def ensure_running(self, service_name: str) -> Dict[str, Any]:
        snapshot = self.status(service_name)
        payload = snapshot.model_dump()
        payload["requestedAction"] = "noop"
        if not snapshot.probe_supported:
            return payload
        if snapshot.active_state == "active" and snapshot.sub_state == "running":
            return payload
        warning = self.reset_failed(service_name, ignore_errors=True)
        action = "restart" if snapshot.active_state == "active" else "start"
        next_snapshot = self.restart(service_name) if action == "restart" else self.start(service_name)
        payload = next_snapshot.model_dump()
        payload["requestedAction"] = action
        payload["previousState"] = snapshot.model_dump()
        if warning:
            payload["warnings"] = [warning]
        return payload

    def journal_tail(self, service_name: str, *, lines: int) -> List[str]:
        if service_name not in self._settings.managed_services:
            raise ValueError(f"unsupported service: {service_name}")
        if shutil.which("journalctl") is None:
            return []
        cmd = [
            "journalctl",
            "-u",
            f"{service_name}.service",
            "-n",
            str(max(1, lines)),
            "--no-pager",
            "-o",
            "cat",
        ]
        completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            return []
        return [line.rstrip() for line in (completed.stdout or "").splitlines() if line.strip()]

    def _run_service_command(self, action: str, service_name: str) -> None:
        if service_name not in self._settings.managed_services:
            raise ValueError(f"unsupported service: {service_name}")
        if not self.is_supported():
            raise RuntimeError("systemctl is unavailable on this host")
        try:
            subprocess.run(["systemctl", action, f"{service_name}.service"], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
            raise RuntimeError(f"systemctl {action} {service_name}.service failed: {detail[:2000]}") from exc
