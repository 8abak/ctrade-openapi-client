#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]
DEFAULT_MANIFEST_PATH = REPO_ROOT / "deploy" / "update_steps.json"
DEFAULT_STATE_DIR = Path(os.getenv("DATAVIS_DEPLOY_STATE_DIR", "/home/ec2-user/.datavis"))
DEFAULT_LOG_PATH = Path(os.getenv("DATAVIS_UPDATE_LOG_PATH", str(DEFAULT_STATE_DIR / "update_steps.log")))
DEFAULT_STATE_PATH = Path(os.getenv("DATAVIS_UPDATE_STATE_PATH", str(DEFAULT_STATE_DIR / "update_steps_state.json")))
DEFAULT_DATABASE_URL = os.getenv(
    "DATAVIS_DEFAULT_DATABASE_URL",
    "postgresql://babak:babak33044@localhost:5432/trading",
)
SUPPORTED_ACTION_TYPES = {
    "run_sql_file",
    "install_systemd_unit",
    "daemon_reload",
    "restart_service",
    "enable_service",
    "start_service",
    "run_command",
    "backfill_command",
    "verify_command",
}


class ManifestError(Exception):
    pass


class ActionFailed(Exception):
    pass


@dataclass(frozen=True)
class Action:
    id: str
    description: str
    type: str
    required: bool
    safe_to_rerun: bool
    timeout_seconds: Optional[int]
    command: Optional[str]
    file: Optional[str]
    service: Optional[str]


class StepLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, message: str) -> None:
        rendered = "[run-update-steps] {0}".format(message)
        print(rendered, flush=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(rendered + "\n")


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.payload = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"manifests": {}}
        with self.path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict):
            raise ManifestError("state file must contain a JSON object")
        manifests = data.get("manifests")
        if not isinstance(manifests, dict):
            data["manifests"] = {}
        return data

    def manifest_actions(self, version: str) -> Dict[str, Any]:
        manifests = self.payload.setdefault("manifests", {})
        manifest_state = manifests.setdefault(version, {"actions": {}})
        actions = manifest_state.setdefault("actions", {})
        if not isinstance(actions, dict):
            manifest_state["actions"] = {}
            actions = manifest_state["actions"]
        return actions

    def get_action_state(self, version: str, action_id: str) -> Dict[str, Any]:
        actions = self.manifest_actions(version)
        action_state = actions.setdefault(action_id, {})
        if not isinstance(action_state, dict):
            actions[action_id] = {}
            action_state = actions[action_id]
        return action_state

    def save_manifest_metadata(self, version: str, description: str) -> None:
        manifest_state = self.payload.setdefault("manifests", {}).setdefault(version, {"actions": {}})
        manifest_state["description"] = description
        manifest_state["updated_at"] = utc_now()
        self.save()

    def mark_status(
        self,
        *,
        version: str,
        action: Action,
        status: str,
        message: str,
    ) -> None:
        action_state = self.get_action_state(version, action.id)
        action_state["description"] = action.description
        action_state["type"] = action.type
        action_state["required"] = action.required
        action_state["safe_to_rerun"] = action.safe_to_rerun
        action_state["status"] = status
        action_state["message"] = message
        action_state["updated_at"] = utc_now()
        self.save()

    def save(self) -> None:
        temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(self.payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_path.replace(self.path)


class Runner:
    def __init__(
        self,
        *,
        manifest_path: Path,
        log: StepLogger,
        state: StateStore,
        skip_actions: Set[str],
        force_actions: Set[str],
        force_all: bool,
        dry_run: bool,
    ) -> None:
        self.manifest_path = manifest_path
        self.log = log
        self.state = state
        self.skip_actions = skip_actions
        self.force_actions = force_actions
        self.force_all = force_all
        self.dry_run = dry_run
        self.repo_root = REPO_ROOT
        self.pending_daemon_reload = False
        self.manifest_version = ""
        self.manifest_description = ""

    def run(self) -> int:
        manifest = load_manifest(self.manifest_path)
        self.manifest_version = manifest["version"]
        self.manifest_description = manifest["description"]
        self.state.save_manifest_metadata(self.manifest_version, self.manifest_description)
        actions = manifest["actions"]
        self.log.log(
            "Loaded manifest version={0} description={1!r} actions={2}".format(
                self.manifest_version,
                self.manifest_description,
                len(actions),
            )
        )

        optional_failures = 0
        for action in actions:
            try:
                self._run_action(action)
            except ActionFailed as exc:
                self.state.mark_status(
                    version=self.manifest_version,
                    action=action,
                    status="failed",
                    message=str(exc),
                )
                if action.required:
                    self.log.log("Required action failed: {0}".format(action.id))
                    raise
                optional_failures += 1
                self.log.log("Optional action failed and will be ignored: {0}".format(action.id))

        if self.pending_daemon_reload:
            self.log.log("Auto-running pending systemd daemon-reload after unit installation")
            self._run_systemd_daemon_reload(timeout_seconds=60)
            self.pending_daemon_reload = False

        self.log.log(
            "Manifest completed: version={0}, optional_failures={1}".format(
                self.manifest_version,
                optional_failures,
            )
        )
        return 0

    def _run_action(self, action: Action) -> None:
        if action.id in self.skip_actions:
            self.log.log("Skipping action {0} because it was explicitly skipped".format(action.id))
            self.state.mark_status(
                version=self.manifest_version,
                action=action,
                status="skipped",
                message="skipped by caller",
            )
            return

        if self._should_skip_completed_action(action):
            self.log.log(
                "Skipping action {0} because it already succeeded and safe_to_rerun=false".format(action.id)
            )
            self.state.mark_status(
                version=self.manifest_version,
                action=action,
                status="skipped",
                message="already completed successfully",
            )
            return

        self.log.log(
            "Starting action {0} type={1} required={2} safe_to_rerun={3}".format(
                action.id,
                action.type,
                str(action.required).lower(),
                str(action.safe_to_rerun).lower(),
            )
        )
        self.log.log("Action description: {0}".format(action.description))

        if self.dry_run:
            self.log.log("Dry run: action {0} not executed".format(action.id))
            return

        self._dispatch_action(action)
        self.state.mark_status(
            version=self.manifest_version,
            action=action,
            status="success",
            message="completed successfully",
        )
        self.log.log("Completed action {0}".format(action.id))

    def _should_skip_completed_action(self, action: Action) -> bool:
        if self.force_all or action.id in self.force_actions or action.safe_to_rerun:
            return False
        action_state = self.state.get_action_state(self.manifest_version, action.id)
        return action_state.get("status") == "success"

    def _dispatch_action(self, action: Action) -> None:
        if action.type == "run_sql_file":
            self._run_sql_file(action)
            return
        if action.type == "install_systemd_unit":
            self._install_systemd_unit(action)
            return
        if action.type == "daemon_reload":
            self._run_systemd_daemon_reload(timeout_seconds=action.timeout_seconds or 60)
            self.pending_daemon_reload = False
            return
        if action.type == "restart_service":
            self._ensure_daemon_reload_if_pending()
            self._systemctl(action, "restart")
            self._assert_service_active(action.service or "")
            return
        if action.type == "enable_service":
            self._ensure_daemon_reload_if_pending()
            self._systemctl(action, "enable")
            return
        if action.type == "start_service":
            self._ensure_daemon_reload_if_pending()
            self._systemctl(action, "start")
            self._assert_service_active(action.service or "")
            return
        if action.type in {"run_command", "backfill_command", "verify_command"}:
            self._run_shell_command(action)
            return
        raise ActionFailed("unsupported action type: {0}".format(action.type))

    def _run_sql_file(self, action: Action) -> None:
        sql_path = resolve_repo_file(action.file, allowed_prefixes=("deploy/sql",))
        database_url = database_url()
        if not database_url:
            raise ActionFailed("DATABASE_URL or DATAVIS_DB_URL is not configured")
        self._run_subprocess(
            ["psql", database_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)],
            timeout_seconds=action.timeout_seconds,
        )

    def _install_systemd_unit(self, action: Action) -> None:
        unit_path = resolve_repo_file(action.file, allowed_prefixes=("deploy/systemd",))
        service_name = normalize_service_name(action.service)
        target_path = Path("/etc/systemd/system") / service_name
        self._run_subprocess(
            ["sudo", "install", "-m", "0644", str(unit_path), str(target_path)],
            timeout_seconds=action.timeout_seconds,
        )
        self.pending_daemon_reload = True

    def _run_systemd_daemon_reload(self, *, timeout_seconds: int) -> None:
        self._run_subprocess(
            ["sudo", "systemctl", "daemon-reload"],
            timeout_seconds=timeout_seconds,
        )

    def _systemctl(self, action: Action, operation: str) -> None:
        service_name = normalize_service_name(action.service)
        self._run_subprocess(
            ["sudo", "systemctl", operation, service_name],
            timeout_seconds=action.timeout_seconds,
        )

    def _assert_service_active(self, service_name: str) -> None:
        normalized = normalize_service_name(service_name)
        self._run_subprocess(
            ["sudo", "systemctl", "is-active", "--quiet", normalized],
            timeout_seconds=15,
        )

    def _run_shell_command(self, action: Action) -> None:
        if not action.command:
            raise ActionFailed("command is required for {0}".format(action.type))
        self._run_subprocess(
            ["bash", "-lc", action.command],
            timeout_seconds=action.timeout_seconds,
        )

    def _ensure_daemon_reload_if_pending(self) -> None:
        if not self.pending_daemon_reload:
            return
        self.log.log("Auto-running systemd daemon-reload before service action")
        self._run_systemd_daemon_reload(timeout_seconds=60)
        self.pending_daemon_reload = False

    def _run_subprocess(self, command: List[str], *, timeout_seconds: Optional[int]) -> None:
        printable = " ".join(shell_quote(part) for part in command)
        self.log.log("Executing: {0}".format(printable))
        try:
            completed = subprocess.run(
                command,
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ActionFailed("command timed out after {0}s: {1}".format(timeout_seconds, printable)) from exc
        if completed.stdout:
            for line in completed.stdout.splitlines():
                self.log.log("stdout: {0}".format(line))
        if completed.stderr:
            for line in completed.stderr.splitlines():
                self.log.log("stderr: {0}".format(line))
        if completed.returncode != 0:
            raise ActionFailed("command exited with code {0}: {1}".format(completed.returncode, printable))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def shell_quote(value: str) -> str:
    if not value:
        return "''"
    if all(ch.isalnum() or ch in "@%_+=:,./-" for ch in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    return DEFAULT_DATABASE_URL.strip()


def normalize_service_name(service_name: Optional[str]) -> str:
    value = str(service_name or "").strip()
    if not value:
        raise ActionFailed("service is required")
    if "/" in value or "\\" in value:
        raise ActionFailed("service must not contain path separators: {0}".format(value))
    if not value.endswith(".service"):
        value += ".service"
    return value


def resolve_repo_file(relative_path: Optional[str], *, allowed_prefixes: Iterable[str]) -> Path:
    raw_value = str(relative_path or "").strip()
    if not raw_value:
        raise ActionFailed("file is required")
    candidate = (REPO_ROOT / raw_value).resolve()
    try:
        candidate.relative_to(REPO_ROOT)
    except ValueError as exc:
        raise ActionFailed("file must stay within the repository: {0}".format(raw_value)) from exc
    normalized = raw_value.replace("\\", "/")
    if not any(normalized.startswith(prefix + "/") or normalized == prefix for prefix in allowed_prefixes):
        raise ActionFailed("file is outside the allowed deploy directories: {0}".format(raw_value))
    if not candidate.is_file():
        raise ActionFailed("file does not exist: {0}".format(raw_value))
    return candidate


def parse_timeout(raw_value: Any) -> Optional[int]:
    if raw_value in (None, ""):
        return None
    try:
        timeout = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ManifestError("timeout_seconds must be an integer") from exc
    if timeout <= 0:
        raise ManifestError("timeout_seconds must be greater than zero")
    return timeout


def parse_action(raw_action: Any) -> Action:
    if not isinstance(raw_action, dict):
        raise ManifestError("each action must be a JSON object")

    action_id = str(raw_action.get("id") or "").strip()
    description = str(raw_action.get("description") or "").strip()
    action_type = str(raw_action.get("type") or "").strip()
    if not action_id:
        raise ManifestError("each action requires a non-empty id")
    if not description:
        raise ManifestError("action {0} requires a description".format(action_id))
    if action_type not in SUPPORTED_ACTION_TYPES:
        raise ManifestError("action {0} has unsupported type {1}".format(action_id, action_type))

    required = raw_action.get("required")
    safe_to_rerun = raw_action.get("safe_to_rerun")
    if not isinstance(required, bool):
        raise ManifestError("action {0} must set required to true or false".format(action_id))
    if not isinstance(safe_to_rerun, bool):
        raise ManifestError("action {0} must set safe_to_rerun to true or false".format(action_id))

    timeout_seconds = parse_timeout(raw_action.get("timeout_seconds"))
    command = raw_action.get("command")
    file_path = raw_action.get("file")
    service = raw_action.get("service")

    if action_type in {"run_sql_file", "install_systemd_unit"} and not str(file_path or "").strip():
        raise ManifestError("action {0} requires a file field".format(action_id))
    if action_type in {"restart_service", "enable_service", "start_service", "install_systemd_unit"} and not str(service or "").strip():
        raise ManifestError("action {0} requires a service field".format(action_id))
    if action_type in {"run_command", "backfill_command", "verify_command"} and not str(command or "").strip():
        raise ManifestError("action {0} requires a command field".format(action_id))
    if action_type == "daemon_reload" and any(str(raw_action.get(field) or "").strip() for field in ("command", "file", "service")):
        raise ManifestError("action {0} daemon_reload must not set command, file, or service".format(action_id))

    return Action(
        id=action_id,
        description=description,
        type=action_type,
        required=required,
        safe_to_rerun=safe_to_rerun,
        timeout_seconds=timeout_seconds,
        command=str(command).strip() if command is not None else None,
        file=str(file_path).strip() if file_path is not None else None,
        service=str(service).strip() if service is not None else None,
    )


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise ManifestError("manifest file not found: {0}".format(path))
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ManifestError("manifest must contain a JSON object")
    version = str(payload.get("version") or "").strip()
    description = str(payload.get("description") or "").strip()
    raw_actions = payload.get("actions")
    if not version:
        raise ManifestError("manifest requires a non-empty version")
    if not description:
        raise ManifestError("manifest requires a non-empty description")
    if not isinstance(raw_actions, list):
        raise ManifestError("manifest actions must be an array")

    actions: List[Action] = []
    action_ids: Set[str] = set()
    for raw_action in raw_actions:
        action = parse_action(raw_action)
        if action.id in action_ids:
            raise ManifestError("duplicate action id: {0}".format(action.id))
        action_ids.add(action.id)
        actions.append(action)
    return {"version": version, "description": description, "actions": actions}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute typed deploy/update actions from deploy/update_steps.json.")
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST_PATH),
        help="Path to the update manifest JSON file.",
    )
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_PATH),
        help="Path to the persistent update runner log file.",
    )
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_PATH),
        help="Path to the update runner state file.",
    )
    parser.add_argument(
        "--skip-action",
        action="append",
        default=[],
        help="Skip the named action id. Can be passed multiple times.",
    )
    parser.add_argument(
        "--force-action",
        action="append",
        default=[],
        help="Force rerun of the named action id even when safe_to_rerun=false. Can be passed multiple times.",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Force rerun of every action, including actions marked safe_to_rerun=false.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the manifest and log the planned actions without executing them.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    manifest_path = Path(args.manifest).resolve()
    log_path = Path(args.log_file).resolve()
    state_path = Path(args.state_file).resolve()
    log = StepLogger(log_path)
    try:
        runner = Runner(
            manifest_path=manifest_path,
            log=log,
            state=StateStore(state_path),
            skip_actions={value.strip() for value in args.skip_action if value.strip()},
            force_actions={value.strip() for value in args.force_action if value.strip()},
            force_all=bool(args.force_all),
            dry_run=bool(args.dry_run),
        )
        return runner.run()
    except ManifestError as exc:
        log.log("Manifest error: {0}".format(exc))
        return 1
    except ActionFailed as exc:
        log.log("Action failed: {0}".format(exc))
        return 1
    except KeyboardInterrupt:
        log.log("Interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
