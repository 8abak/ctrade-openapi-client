from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from psycopg2.extras import Json

from datavis.research.config import ResearchSettings, ensure_runtime_dirs
from datavis.research.db import connection


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ResearchJournal:
    def __init__(self, settings: ResearchSettings, component: str) -> None:
        self._settings = settings
        self._component = component
        ensure_runtime_dirs(settings)

    @property
    def component_path(self) -> Path:
        return self._settings.journal_dir / f"{self._component}.jsonl"

    def write(
        self,
        *,
        level: str,
        event_type: str,
        message: str,
        payload: Optional[Dict[str, Any]] = None,
        job_id: Optional[int] = None,
        run_id: Optional[int] = None,
        decision_id: Optional[int] = None,
        conn: Any | None = None,
    ) -> None:
        record = {
            "createdAt": utc_now().isoformat(),
            "component": self._component,
            "level": str(level or "INFO").upper(),
            "eventType": event_type,
            "message": message,
            "jobId": job_id,
            "runId": run_id,
            "decisionId": decision_id,
            "payload": payload or {},
        }
        self._append_file_record(record)
        self._insert_db_record(record, conn=conn)

    def _append_file_record(self, record: Dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        with self.component_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")

    def _insert_db_record(self, record: Dict[str, Any], *, conn: Any | None) -> None:
        try:
            if conn is not None:
                self._insert_db_record_inner(conn, record)
                return
            with connection(self._settings, readonly=False, autocommit=True, application_name=f"datavis.research.{self._component}") as own_conn:
                self._insert_db_record_inner(own_conn, record)
        except Exception:
            return

    @staticmethod
    def _insert_db_record_inner(conn: Any, record: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.journal (component, level, event_type, job_id, run_id, decision_id, message, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record["component"],
                    record["level"],
                    record["eventType"],
                    record["jobId"],
                    record["runId"],
                    record["decisionId"],
                    record["message"],
                    Json(record["payload"]),
                ),
            )


def write_run_artifacts(
    settings: ResearchSettings,
    *,
    run_id: int,
    summary_payload: Dict[str, Any],
) -> Dict[str, str]:
    ensure_runtime_dirs(settings)
    run_dir = settings.artifact_dir / f"run-{int(run_id):06d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    json_path = run_dir / "summary.json"
    md_path = run_dir / "summary.md"
    json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    md_path.write_text(_render_markdown_summary(run_id=run_id, summary_payload=summary_payload), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _render_markdown_summary(*, run_id: int, summary_payload: Dict[str, Any]) -> str:
    best = summary_payload.get("bestCandidate") or {}
    headline = summary_payload.get("headline") or "Entry research run summary"
    metrics = best.get("validationMetrics") or {}
    lines = [
        f"# Research Run {run_id}",
        "",
        headline,
        "",
        f"- Verdict hint: {summary_payload.get('verdictHint', 'n/a')}",
        f"- Candidate family: {summary_payload.get('config', {}).get('candidate_family', 'n/a')}",
        f"- Label variant: {summary_payload.get('config', {}).get('label_variant', 'n/a')}",
        f"- Validation clean precision: {metrics.get('cleanPrecision', 'n/a')}",
        f"- Validation signals: {metrics.get('signalCount', 'n/a')}",
        f"- Entries/day: {metrics.get('entriesPerDay', 'n/a')}",
        f"- Stability range: {metrics.get('walkForwardRange', 'n/a')}",
        "",
        "## Supervisor Briefing",
        "",
        "```json",
        json.dumps(summary_payload.get("briefing") or {}, indent=2, sort_keys=True, default=str),
        "```",
    ]
    return "\n".join(lines) + "\n"
