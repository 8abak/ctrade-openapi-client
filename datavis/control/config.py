from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

from datavis.research.config import ResearchSettings, load_settings as load_research_settings


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")


def _clean_env_text(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1].strip()
    return text


def _env_text(name: str, default: str = "") -> str:
    return _clean_env_text(os.getenv(name, default))


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_text(name, "1" if default else "0").lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    value = int(_env_text(name, str(default)) or default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    value = float(_env_text(name, str(default)) or default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_path(name: str, default: Path) -> Path:
    raw = _env_text(name, "")
    return Path(raw) if raw else default


def _env_tuple(name: str, default: Tuple[str, ...]) -> Tuple[str, ...]:
    raw = _env_text(name, "")
    if not raw:
        return default
    values = []
    seen = set()
    for chunk in raw.split(","):
        item = _clean_env_text(chunk)
        if not item or item in seen:
            continue
        values.append(item)
        seen.add(item)
    return tuple(values or default)


def _normalize_api_style(raw: str) -> str:
    normalized = (_clean_env_text(raw).lower() or "responses")
    aliases = {
        "responses": "responses",
        "response": "responses",
        "chat": "chat_completions",
        "chat_completion": "chat_completions",
        "chat_completions": "chat_completions",
    }
    return aliases.get(normalized, "responses")


@dataclass(frozen=True)
class ControlSettings:
    database_url: str
    runtime_dir: Path
    artifact_dir: Path
    journal_dir: Path
    log_dir: Path
    api_host: str
    api_port: int
    api_token: str
    enable_loop: bool
    poll_seconds: float
    incident_max_retries: int
    max_patch_files: int
    max_patch_line_changes: int
    max_patch_bytes: int
    max_restarts_per_hour: int
    max_rollbacks_per_incident: int
    job_stuck_seconds: int
    decision_stuck_seconds: int
    recent_failure_window_seconds: int
    max_context_chars: int
    max_log_lines: int
    research_env_file: Path
    shared_env_file: Path
    control_env_file: Path
    control_service_name: str
    engineering_orchestrator_service_name: str
    research_services: Tuple[str, ...]
    managed_services: Tuple[str, ...]
    openai_endpoint: str
    openai_api_style: str
    openai_api_key: str
    openai_model: str
    openai_timeout_seconds: float
    openai_max_output_tokens: int
    openai_temperature: float
    supervisor_max_briefing_chars: int
    research_settings: ResearchSettings

    def is_allowed_patch_path(self, path: Path) -> bool:
        try:
            resolved = path.resolve()
        except FileNotFoundError:
            resolved = path.absolute()
        repo_research = (BASE_DIR / "datavis" / "research").resolve()
        repo_control = (BASE_DIR / "datavis" / "control").resolve()
        for root in (repo_research, repo_control):
            try:
                resolved.relative_to(root)
                return True
            except ValueError:
                continue
        return resolved in {
            self.research_env_file.resolve(),
            self.shared_env_file.resolve(),
            self.control_env_file.resolve(),
        }


def load_settings() -> ControlSettings:
    research_settings = load_research_settings()
    runtime_dir = _env_path("DATAVIS_CONTROL_RUNTIME_DIR", BASE_DIR / "runtime" / "control")
    research_services = _env_tuple(
        "DATAVIS_CONTROL_RESEARCH_SERVICES",
        (
            research_settings.worker_name,
            research_settings.supervisor_name,
            research_settings.orchestrator_name,
        ),
    )
    control_service_name = _env_text("DATAVIS_CONTROL_API_SERVICE_NAME", "research-control") or "research-control"
    engineering_orchestrator_service_name = _env_text("DATAVIS_CONTROL_ORCHESTRATOR_SERVICE_NAME", "engineering-orchestrator") or "engineering-orchestrator"
    managed_services = tuple(dict.fromkeys([*research_services, control_service_name, engineering_orchestrator_service_name]))
    return ControlSettings(
        database_url=research_settings.database_url,
        runtime_dir=runtime_dir,
        artifact_dir=_env_path("DATAVIS_CONTROL_ARTIFACT_DIR", runtime_dir / "artifacts"),
        journal_dir=_env_path("DATAVIS_CONTROL_JOURNAL_DIR", runtime_dir / "journals"),
        log_dir=_env_path("DATAVIS_CONTROL_LOG_DIR", runtime_dir / "logs"),
        api_host=_env_text("DATAVIS_CONTROL_API_HOST", "127.0.0.1") or "127.0.0.1",
        api_port=_env_int("DATAVIS_CONTROL_API_PORT", 8010, minimum=1024, maximum=65535),
        api_token=_env_text("DATAVIS_CONTROL_API_TOKEN", ""),
        enable_loop=_env_bool("DATAVIS_CONTROL_ENABLE_LOOP", True),
        poll_seconds=_env_float("DATAVIS_CONTROL_POLL_SECONDS", 15.0, minimum=2.0, maximum=300.0),
        incident_max_retries=_env_int("DATAVIS_CONTROL_MAX_RETRIES_PER_INCIDENT", 3, minimum=1, maximum=8),
        max_patch_files=_env_int("DATAVIS_CONTROL_MAX_PATCH_FILES", 2, minimum=1, maximum=8),
        max_patch_line_changes=_env_int("DATAVIS_CONTROL_MAX_PATCH_LINE_CHANGES", 120, minimum=10, maximum=1000),
        max_patch_bytes=_env_int("DATAVIS_CONTROL_MAX_PATCH_BYTES", 20000, minimum=512, maximum=250000),
        max_restarts_per_hour=_env_int("DATAVIS_CONTROL_MAX_RESTARTS_PER_HOUR", 6, minimum=1, maximum=48),
        max_rollbacks_per_incident=_env_int("DATAVIS_CONTROL_MAX_ROLLBACKS_PER_INCIDENT", 2, minimum=0, maximum=8),
        job_stuck_seconds=_env_int("DATAVIS_CONTROL_JOB_STUCK_SECONDS", 1800, minimum=60, maximum=86400),
        decision_stuck_seconds=_env_int("DATAVIS_CONTROL_DECISION_STUCK_SECONDS", 1800, minimum=60, maximum=86400),
        recent_failure_window_seconds=_env_int("DATAVIS_CONTROL_RECENT_FAILURE_WINDOW_SECONDS", 21600, minimum=300, maximum=604800),
        max_context_chars=_env_int("DATAVIS_CONTROL_MAX_CONTEXT_CHARS", 20000, minimum=2000, maximum=100000),
        max_log_lines=_env_int("DATAVIS_CONTROL_MAX_LOG_LINES", 40, minimum=5, maximum=400),
        research_env_file=_env_path("DATAVIS_CONTROL_RESEARCH_ENV_FILE", Path("/etc/datavis-research.env")),
        shared_env_file=_env_path("DATAVIS_CONTROL_SHARED_ENV_FILE", Path("/etc/datavis.env")),
        control_env_file=_env_path("DATAVIS_CONTROL_ENV_FILE", Path("/etc/datavis-control.env")),
        control_service_name=control_service_name,
        engineering_orchestrator_service_name=engineering_orchestrator_service_name,
        research_services=research_services,
        managed_services=managed_services,
        openai_endpoint=_env_text("DATAVIS_CONTROL_OPENAI_ENDPOINT", research_settings.supervisor_endpoint or "https://api.openai.com/v1/responses"),
        openai_api_style=_normalize_api_style(_env_text("DATAVIS_CONTROL_OPENAI_API_STYLE", research_settings.supervisor_api_style)),
        openai_api_key=_env_text("DATAVIS_CONTROL_OPENAI_API_KEY", "") or _env_text("OPENAI_API_KEY", ""),
        openai_model=_env_text("DATAVIS_CONTROL_OPENAI_MODEL", research_settings.supervisor_model),
        openai_timeout_seconds=_env_float("DATAVIS_CONTROL_OPENAI_TIMEOUT_SECONDS", 45.0, minimum=5.0, maximum=300.0),
        openai_max_output_tokens=_env_int("DATAVIS_CONTROL_OPENAI_MAX_OUTPUT_TOKENS", 1200, minimum=100, maximum=4000),
        openai_temperature=_env_float("DATAVIS_CONTROL_OPENAI_TEMPERATURE", 0.0, minimum=0.0, maximum=1.0),
        supervisor_max_briefing_chars=_env_int("DATAVIS_CONTROL_SUPERVISOR_MAX_BRIEFING_CHARS", 24000, minimum=2000, maximum=100000),
        research_settings=research_settings,
    )


def ensure_runtime_dirs(settings: ControlSettings) -> None:
    for path in (settings.runtime_dir, settings.artifact_dir, settings.journal_dir, settings.log_dir):
        path.mkdir(parents=True, exist_ok=True)

