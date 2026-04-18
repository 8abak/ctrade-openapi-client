from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2.extras

from datavis.control.config import BASE_DIR, ControlSettings
from datavis.control.models import IncidentCandidate
from datavis.control.research_manager import ResearchManager
from datavis.control.service_manager import ServiceManager
from datavis.control.store import EngineeringStore


class FailureDetector:
    def __init__(
        self,
        settings: ControlSettings,
        *,
        store: EngineeringStore,
        research_manager: ResearchManager,
        service_manager: ServiceManager,
    ) -> None:
        self._settings = settings
        self._store = store
        self._research_manager = research_manager
        self._service_manager = service_manager

    def detect(self, conn: Any) -> Optional[IncidentCandidate]:
        if self._store.get_active_incident(conn):
            return None
        candidate = self._detect_service_failure(conn)
        if candidate is not None:
            return candidate
        candidate = self._detect_stuck_job(conn)
        if candidate is not None:
            return candidate
        candidate = self._detect_failed_job_or_decision(conn)
        if candidate is not None:
            return candidate
        candidate = self._detect_missing_artifact(conn)
        if candidate is not None:
            return candidate
        return None

    def build_briefing(self, conn: Any, incident: Dict[str, Any]) -> Dict[str, Any]:
        details = dict(incident.get("details") or {})
        incident_type = str(incident.get("incident_type") or "code_bug")
        affected_services = list(incident.get("affected_services") or [])
        return {
            "incident": {
                "id": int(incident["id"]),
                "type": incident_type,
                "severity": incident.get("severity"),
                "summary": incident.get("summary"),
                "fingerprint": incident.get("fingerprint"),
                "retryCount": int(incident.get("retry_count") or 0),
                "maxRetries": int(incident.get("max_retries") or self._settings.incident_max_retries),
                "details": details,
            },
            "researchStatus": self._research_manager.status(conn),
            "latestRun": self._research_manager.latest_run(conn),
            "latestErrors": self._research_manager.latest_errors(conn, limit=8),
            "recentResearchJournals": self._research_manager.recent_journals(conn, limit=20),
            "serviceStatus": [item.model_dump() for item in self._service_manager.status_many(affected_services or self._settings.research_services)],
            "serviceLogs": {
                service: self._service_manager.journal_tail(service, lines=min(self._settings.max_log_lines, 20))
                for service in (affected_services or self._settings.research_services[:1])
            },
            "recentPatchHistory": self._store.list_patch_history(conn, limit=5),
            "recentSmokeTests": self._store.list_recent_smoketests(conn, incident_id=int(incident["id"]), limit=5),
            "fileExcerpts": self._file_excerpts_for_incident_type(incident_type),
        }

    def _detect_service_failure(self, conn: Any) -> Optional[IncidentCandidate]:
        for snapshot in self._service_manager.status_many(self._settings.research_services):
            if not snapshot.probe_supported:
                break
            if snapshot.active_state in {"active", "activating"}:
                continue
            fingerprint = self._fingerprint("service", snapshot.name, snapshot.active_state, snapshot.sub_state, snapshot.status_text or "")
            if self._recent_incident_exists(conn, fingerprint):
                continue
            return IncidentCandidate(
                incident_type="service_runtime_issue",
                severity="error",
                fingerprint=fingerprint,
                summary=f"Research service {snapshot.name} is {snapshot.active_state}/{snapshot.sub_state}.",
                details={"service": snapshot.model_dump()},
                failure_signature=snapshot.status_text or snapshot.active_state,
                affected_services=[snapshot.name],
            )
        return None

    def _detect_stuck_job(self, conn: Any) -> Optional[IncidentCandidate]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT j.id,
                       j.status,
                       j.started_at,
                       j.worker_name,
                       j.config,
                       r.id AS run_id,
                       d.id AS decision_id
                FROM research.job j
                LEFT JOIN research.run r ON r.job_id = j.id AND r.status = 'running'
                LEFT JOIN research.decision d ON d.run_id = r.id AND d.status = 'running'
                WHERE j.status = 'running'
                  AND j.started_at < NOW() - (%s || ' seconds')::interval
                ORDER BY j.started_at ASC
                LIMIT 1
                """,
                (self._settings.job_stuck_seconds,),
            )
            row = cur.fetchone()
        if not row:
            return None
        payload = dict(row)
        fingerprint = self._fingerprint("stuck-job", payload["id"], (payload.get("config") or {}).get("config_fingerprint"))
        if self._recent_incident_exists(conn, fingerprint):
            return None
        return IncidentCandidate(
            incident_type="stalled_loop",
            severity="error",
            fingerprint=fingerprint,
            summary=f"Research job {int(payload['id'])} has been running beyond the bounded timeout.",
            details={"job": payload, "stuckSeconds": self._settings.job_stuck_seconds},
            related_job_id=int(payload["id"]),
            related_run_id=int(payload["run_id"]) if payload.get("run_id") else None,
            related_decision_id=int(payload["decision_id"]) if payload.get("decision_id") else None,
            affected_services=[self._settings.research_settings.worker_name],
        )

    def _detect_failed_job_or_decision(self, conn: Any) -> Optional[IncidentCandidate]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                (
                    SELECT
                        'job' AS source,
                        j.id,
                        j.error_text AS error_text,
                        j.finished_at AS created_at,
                        j.config,
                        NULL::BIGINT AS decision_id,
                        NULL::TEXT AS decision_status,
                        NULL::TEXT AS validation_error,
                        NULL::TEXT AS raw_response
                    FROM research.job j
                    WHERE j.status = 'failed'
                      AND j.finished_at >= NOW() - (%s || ' seconds')::interval
                )
                UNION ALL
                (
                    SELECT
                        'decision' AS source,
                        d.id,
                        COALESCE(d.validation_error, d.reason, d.raw_response) AS error_text,
                        COALESCE(d.completed_at, d.started_at, d.requested_at) AS created_at,
                        r.config,
                        d.id AS decision_id,
                        d.status AS decision_status,
                        d.validation_error,
                        d.raw_response
                    FROM research.decision d
                    JOIN research.run r ON r.id = d.run_id
                    WHERE d.status IN ('rejected')
                      AND COALESCE(d.completed_at, d.started_at, d.requested_at) >= NOW() - (%s || ' seconds')::interval
                )
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (self._settings.recent_failure_window_seconds, self._settings.recent_failure_window_seconds),
            )
            row = cur.fetchone()
        if not row:
            return None
        payload = dict(row)
        classification = self._classify_failure(str(payload.get("error_text") or ""), source=str(payload["source"]))
        fingerprint = self._fingerprint(classification["incidentType"], payload["source"], payload["id"], classification["signature"])
        if self._recent_incident_exists(conn, fingerprint):
            return None
        return IncidentCandidate(
            incident_type=classification["incidentType"],
            severity=classification["severity"],
            fingerprint=fingerprint,
            summary=classification["summary"],
            details={
                "source": payload["source"],
                "errorText": str(payload.get("error_text") or "")[:4000],
                "configFingerprint": (payload.get("config") or {}).get("config_fingerprint"),
                "suggestedConfigChanges": classification.get("suggestedConfigChanges") or {},
                "suggestedPatchTemplate": classification.get("suggestedPatchTemplate"),
            },
            failure_signature=classification["signature"],
            related_job_id=int(payload["id"]) if payload["source"] == "job" else None,
            related_decision_id=int(payload["decision_id"]) if payload.get("decision_id") else None,
            affected_services=classification["services"],
        )

    def _detect_missing_artifact(self, conn: Any) -> Optional[IncidentCandidate]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.id, r.job_id, r.finished_at
                FROM research.run r
                WHERE r.status = 'completed'
                  AND r.finished_at >= NOW() - INTERVAL '1 day'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM research.artifact a
                      WHERE a.run_id = r.id
                  )
                ORDER BY r.finished_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            return None
        payload = dict(row)
        fingerprint = self._fingerprint("missing-artifact", payload["id"])
        if self._recent_incident_exists(conn, fingerprint):
            return None
        return IncidentCandidate(
            incident_type="missing_artifact",
            severity="warning",
            fingerprint=fingerprint,
            summary=f"Research run {int(payload['id'])} completed without persisted artifacts.",
            details={"run": payload},
            related_run_id=int(payload["id"]),
            related_job_id=int(payload["job_id"]) if payload.get("job_id") else None,
            affected_services=[self._settings.research_settings.worker_name],
        )

    def _classify_failure(self, error_text: str, *, source: str) -> Dict[str, Any]:
        text = (error_text or "").lower()
        if any(token in text for token in ("too many feature toggles", "slice_rows exceeds limit", "unsupported label variant", "unsupported candidate family")):
            return {
                "incidentType": "research_parameter_issue",
                "severity": "warning",
                "summary": "Research configuration overflow or parameter guardrail rejection detected.",
                "signature": "research-parameter-guardrail",
                "suggestedPatchTemplate": "fix_feature_toggle_clamp",
                "services": [self._settings.research_settings.orchestrator_name if source == "decision" else self._settings.research_settings.worker_name],
            }
        if any(token in text for token in ("permission denied", "operation not permitted", "read-only file system")):
            return {
                "incidentType": "permission_path_issue",
                "severity": "error",
                "summary": "Filesystem permission or ownership failure detected in the research runtime.",
                "signature": "permission-path",
                "suggestedPatchTemplate": "fix_permission_safe_journal_write" if "journal" in text else None,
                "services": [self._settings.research_settings.worker_name, self._settings.research_settings.supervisor_name],
            }
        if any(token in text for token in ("unknown parameter", "unsupported parameter", "/v1/chat/completions", "/v1/responses", "did not contain extractable text")):
            return {
                "incidentType": "openai_api_request_issue",
                "severity": "error",
                "summary": "OpenAI endpoint or request-shape failure detected in the engineering surface.",
                "signature": "openai-request-shape",
                "suggestedConfigChanges": {
                    "DATAVIS_RESEARCH_OPENAI_ENDPOINT": "https://api.openai.com/v1/responses",
                    "DATAVIS_RESEARCH_OPENAI_API_STYLE": "responses",
                },
                "suggestedPatchTemplate": "fix_openai_request_payload",
                "services": [self._settings.research_settings.supervisor_name],
            }
        if any(token in text for token in ("api key", "401", "403", "missing required", "no such file or directory")):
            return {
                "incidentType": "config_env_issue",
                "severity": "error",
                "summary": "Environment configuration or missing runtime file failure detected.",
                "signature": "config-env",
                "services": [self._settings.research_settings.supervisor_name],
            }
        if any(token in text for token in ("connection refused", "server closed the connection unexpectedly", "could not connect", "timeout expired")):
            return {
                "incidentType": "service_runtime_issue",
                "severity": "error",
                "summary": "Service or database runtime failure detected.",
                "signature": "service-runtime",
                "services": [self._settings.research_settings.worker_name, self._settings.research_settings.orchestrator_name],
            }
        if "omitted a reason" in text or "validation_error" in text or "unsafe supervisor instruction" in text:
            return {
                "incidentType": "code_bug",
                "severity": "warning",
                "summary": "Supervisor decision parsing or defaulting bug detected.",
                "signature": "decision-defaults",
                "suggestedPatchTemplate": "fix_decision_defaults",
                "services": [self._settings.research_settings.supervisor_name, self._settings.research_settings.orchestrator_name],
            }
        return {
            "incidentType": "code_bug",
            "severity": "error",
            "summary": "Unhandled research loop failure detected.",
            "signature": (text[:120] or "generic-code-bug"),
            "services": [self._settings.research_settings.worker_name],
        }

    def _file_excerpts_for_incident_type(self, incident_type: str) -> Dict[str, str]:
        if incident_type == "openai_api_request_issue":
            files = [
                (BASE_DIR / "datavis" / "research" / "supervisor_client.py", "_invoke_openai"),
                (BASE_DIR / "datavis" / "research" / "supervisor_client.py", "_normalize_endpoint"),
            ]
        elif incident_type == "research_parameter_issue":
            files = [
                (BASE_DIR / "datavis" / "research" / "guardrails.py", "sanitize_parameters"),
                (BASE_DIR / "datavis" / "research" / "mutation.py", "feature_toggles"),
            ]
        elif incident_type == "permission_path_issue":
            files = [
                (BASE_DIR / "datavis" / "research" / "journal.py", "_append_file_record"),
                (BASE_DIR / "datavis" / "research" / "config.py", "ensure_runtime_dirs"),
            ]
        else:
            files = [
                (BASE_DIR / "datavis" / "research" / "guardrails.py", "coerce_supervisor_decision_payload"),
                (BASE_DIR / "datavis" / "research" / "supervisor_client.py", "review"),
            ]
        excerpts: Dict[str, str] = {}
        for path, marker in files:
            excerpt = self._load_excerpt(path, marker)
            if excerpt:
                excerpts[str(path.relative_to(BASE_DIR))] = excerpt
        return excerpts

    def _load_excerpt(self, path: Path, marker: str) -> str:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return ""
        start = 0
        for index, line in enumerate(lines):
            if marker in line:
                start = max(0, index - 8)
                break
        end = min(len(lines), start + 36)
        excerpt = "\n".join(lines[start:end])
        return excerpt[: self._settings.max_context_chars]

    def _recent_incident_exists(self, conn: Any, fingerprint: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM research.engineering_incident
                WHERE fingerprint = %s
                  AND updated_at >= NOW() - INTERVAL '2 hours'
                LIMIT 1
                """,
                (fingerprint,),
            )
            return cur.fetchone() is not None

    @staticmethod
    def _fingerprint(*parts: Any) -> str:
        encoded = json.dumps([str(part or "") for part in parts], separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha1(encoded).hexdigest()[:16]
