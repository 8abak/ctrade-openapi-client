from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")


def _env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    value = int(os.getenv(name, str(default)).strip() or default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    value = float(os.getenv(name, str(default)).strip() or default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name, "").strip()
    return Path(raw) if raw else default


@dataclass(frozen=True)
class ResearchSettings:
    database_url: str
    symbol: str
    runtime_dir: Path
    artifact_dir: Path
    journal_dir: Path
    log_dir: Path
    statement_timeout_ms: int
    lock_timeout_ms: int
    worker_poll_seconds: float
    orchestrator_poll_seconds: float
    supervisor_poll_seconds: float
    idle_sleep_seconds: float
    chunk_sleep_seconds: float
    chunk_rows: int
    write_batch_rows: int
    seed_slice_rows: int
    seed_warmup_rows: int
    max_slice_rows: int
    max_warmup_rows: int
    iteration_budget: int
    max_candidates: int
    max_examples: int
    supervisor_endpoint: str
    supervisor_api_style: str
    supervisor_api_key: str
    supervisor_model: str
    supervisor_timeout_seconds: float
    supervisor_max_output_tokens: int
    supervisor_max_briefing_chars: int
    supervisor_temperature: float
    worker_name: str
    orchestrator_name: str
    supervisor_name: str


def load_settings() -> ResearchSettings:
    runtime_dir = _env_path("DATAVIS_RESEARCH_RUNTIME_DIR", BASE_DIR / "runtime" / "research")
    return ResearchSettings(
        database_url=os.getenv("DATABASE_URL", "").strip(),
        symbol=(os.getenv("DATAVIS_RESEARCH_SYMBOL", os.getenv("DATAVIS_SYMBOL", "XAUUSD")).strip() or "XAUUSD").upper(),
        runtime_dir=runtime_dir,
        artifact_dir=_env_path("DATAVIS_RESEARCH_ARTIFACT_DIR", runtime_dir / "artifacts"),
        journal_dir=_env_path("DATAVIS_RESEARCH_JOURNAL_DIR", runtime_dir / "journals"),
        log_dir=_env_path("DATAVIS_RESEARCH_LOG_DIR", runtime_dir / "logs"),
        statement_timeout_ms=_env_int("DATAVIS_RESEARCH_STATEMENT_TIMEOUT_MS", 12000, minimum=1000),
        lock_timeout_ms=_env_int("DATAVIS_RESEARCH_LOCK_TIMEOUT_MS", 3000, minimum=250),
        worker_poll_seconds=_env_float("DATAVIS_RESEARCH_WORKER_POLL_SECONDS", 4.0, minimum=0.5),
        orchestrator_poll_seconds=_env_float("DATAVIS_RESEARCH_ORCHESTRATOR_POLL_SECONDS", 6.0, minimum=0.5),
        supervisor_poll_seconds=_env_float("DATAVIS_RESEARCH_SUPERVISOR_POLL_SECONDS", 6.0, minimum=0.5),
        idle_sleep_seconds=_env_float("DATAVIS_RESEARCH_IDLE_SLEEP_SECONDS", 1.0, minimum=0.0),
        chunk_sleep_seconds=_env_float("DATAVIS_RESEARCH_CHUNK_SLEEP_SECONDS", 0.05, minimum=0.0, maximum=5.0),
        chunk_rows=_env_int("DATAVIS_RESEARCH_CHUNK_ROWS", 250, minimum=25, maximum=2000),
        write_batch_rows=_env_int("DATAVIS_RESEARCH_WRITE_BATCH_ROWS", 200, minimum=25, maximum=1000),
        seed_slice_rows=_env_int("DATAVIS_RESEARCH_SEED_SLICE_ROWS", 8000, minimum=500, maximum=30000),
        seed_warmup_rows=_env_int("DATAVIS_RESEARCH_SEED_WARMUP_ROWS", 160, minimum=20, maximum=2000),
        max_slice_rows=_env_int("DATAVIS_RESEARCH_MAX_SLICE_ROWS", 16000, minimum=500, maximum=50000),
        max_warmup_rows=_env_int("DATAVIS_RESEARCH_MAX_WARMUP_ROWS", 600, minimum=20, maximum=5000),
        iteration_budget=_env_int("DATAVIS_RESEARCH_ITERATION_BUDGET", 8, minimum=1, maximum=128),
        max_candidates=_env_int("DATAVIS_RESEARCH_MAX_CANDIDATES", 12, minimum=1, maximum=50),
        max_examples=_env_int("DATAVIS_RESEARCH_MAX_EXAMPLES", 5, minimum=1, maximum=20),
        supervisor_endpoint=os.getenv("DATAVIS_RESEARCH_OPENAI_ENDPOINT", "https://api.openai.com/v1/responses").strip(),
        supervisor_api_style=(os.getenv("DATAVIS_RESEARCH_OPENAI_API_STYLE", "responses").strip().lower() or "responses"),
        supervisor_api_key=os.getenv("OPENAI_API_KEY", "").strip() or os.getenv("DATAVIS_RESEARCH_OPENAI_API_KEY", "").strip(),
        supervisor_model=os.getenv("DATAVIS_RESEARCH_OPENAI_MODEL", "").strip(),
        supervisor_timeout_seconds=_env_float("DATAVIS_RESEARCH_OPENAI_TIMEOUT_SECONDS", 45.0, minimum=5.0, maximum=300.0),
        supervisor_max_output_tokens=_env_int("DATAVIS_RESEARCH_OPENAI_MAX_OUTPUT_TOKENS", 700, minimum=100, maximum=4000),
        supervisor_max_briefing_chars=_env_int("DATAVIS_RESEARCH_SUPERVISOR_MAX_BRIEFING_CHARS", 20000, minimum=2000, maximum=100000),
        supervisor_temperature=_env_float("DATAVIS_RESEARCH_OPENAI_TEMPERATURE", 0.1, minimum=0.0, maximum=1.0),
        worker_name=os.getenv("DATAVIS_RESEARCH_WORKER_NAME", "research-worker").strip() or "research-worker",
        orchestrator_name=os.getenv("DATAVIS_RESEARCH_ORCHESTRATOR_NAME", "research-orchestrator").strip() or "research-orchestrator",
        supervisor_name=os.getenv("DATAVIS_RESEARCH_SUPERVISOR_NAME", "research-supervisor").strip() or "research-supervisor",
    )


def ensure_runtime_dirs(settings: ResearchSettings) -> None:
    for path in (settings.runtime_dir, settings.artifact_dir, settings.journal_dir, settings.log_dir):
        path.mkdir(parents=True, exist_ok=True)

