from __future__ import annotations

import shutil
import subprocess
from typing import Dict, Iterable, List

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

    def reset_failed(self, service_name: str) -> None:
        self._run_service_command("reset-failed", service_name)

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
        subprocess.run(["systemctl", action, f"{service_name}.service"], check=True, capture_output=True, text=True)
