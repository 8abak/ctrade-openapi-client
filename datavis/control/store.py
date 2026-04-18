from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg2.extras
from psycopg2.extras import Json

from datavis.control.config import ControlSettings
from datavis.control.models import IncidentCandidate


ENGINEERING_STATE_KEY = "engineering_loop_control"
ACTIVE_INCIDENT_STATUSES = ("open", "analyzing", "executing", "validating")


class EngineeringStore:
    def __init__(self, settings: ControlSettings) -> None:
        self._settings = settings

    def default_state(self) -> Dict[str, Any]:
        return {
            "enabled": self._settings.enable_loop,
            "paused": False,
            "current_incident_id": None,
            "last_incident_id": None,
            "last_action_id": None,
            "last_resolution": None,
        }

    def ensure_state(self, conn: Any) -> Dict[str, Any]:
        state = self.get_state(conn)
        if state:
            return state
        payload = self.default_state()
        self.set_state(conn, payload)
        return payload

    def get_state(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM research.engineering_state WHERE key = %s", (ENGINEERING_STATE_KEY,))
            row = cur.fetchone()
        return dict((row or [None])[0] or {})

    def set_state(self, conn: Any, value: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_state (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                (ENGINEERING_STATE_KEY, Json(value)),
            )

    def get_active_incident(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.engineering_incident
                WHERE status = ANY(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (list(ACTIVE_INCIDENT_STATUSES),),
            )
            row = cur.fetchone()
        return dict(row) if row else {}

    def create_incident(self, conn: Any, candidate: IncidentCandidate) -> Dict[str, Any]:
        existing = self.get_active_incident(conn)
        if existing:
            return existing
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_incident (
                    status,
                    incident_type,
                    severity,
                    fingerprint,
                    summary,
                    details,
                    failure_signature,
                    source_job_id,
                    source_run_id,
                    source_decision_id,
                    affected_services,
                    retry_count,
                    max_retries
                )
                VALUES ('open', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
                RETURNING *
                """,
                (
                    candidate.incident_type,
                    candidate.severity,
                    candidate.fingerprint,
                    candidate.summary,
                    Json(candidate.details),
                    candidate.failure_signature,
                    candidate.related_job_id,
                    candidate.related_run_id,
                    candidate.related_decision_id,
                    Json(candidate.affected_services),
                    self._settings.incident_max_retries,
                ),
            )
            row = dict(cur.fetchone())
        state = self.ensure_state(conn)
        state["current_incident_id"] = int(row["id"])
        state["last_incident_id"] = int(row["id"])
        self.set_state(conn, state)
        return row

    def transition_incident(
        self,
        conn: Any,
        *,
        incident_id: int,
        status: str,
        summary: Optional[str] = None,
        resolution_payload: Optional[Dict[str, Any]] = None,
        action_id: Optional[int] = None,
        increment_retry: bool = False,
    ) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE research.engineering_incident
                SET status = %s,
                    summary = COALESCE(%s, summary),
                    updated_at = NOW(),
                    resolved_at = CASE WHEN %s = 'resolved' THEN NOW() ELSE resolved_at END,
                    escalated_at = CASE WHEN %s = 'escalated' THEN NOW() ELSE escalated_at END,
                    resolution = COALESCE(%s, resolution),
                    current_action_id = COALESCE(%s, current_action_id),
                    retry_count = retry_count + CASE WHEN %s THEN 1 ELSE 0 END
                WHERE id = %s
                RETURNING *
                """,
                (
                    status,
                    summary,
                    status,
                    status,
                    Json(resolution_payload) if resolution_payload is not None else None,
                    action_id,
                    increment_retry,
                    incident_id,
                ),
            )
            row = dict(cur.fetchone())
        state = self.ensure_state(conn)
        state["current_incident_id"] = int(row["id"]) if status in ACTIVE_INCIDENT_STATUSES else None
        state["last_incident_id"] = int(row["id"])
        if action_id is not None:
            state["last_action_id"] = action_id
        if status not in ACTIVE_INCIDENT_STATUSES:
            state["last_resolution"] = status
        self.set_state(conn, state)
        return row

    def start_action(
        self,
        conn: Any,
        *,
        incident_id: int,
        action_type: str,
        rationale: str,
        requested_payload: Dict[str, Any],
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_action (
                    incident_id, action_type, status, rationale, requested_by, requested_payload, started_at
                )
                VALUES (%s, %s, 'running', %s, %s, %s, NOW())
                RETURNING id
                """,
                (incident_id, action_type, rationale[:4000], "engineering-orchestrator", Json(requested_payload)),
            )
            action_id = int(cur.fetchone()[0])
        self.transition_incident(conn, incident_id=incident_id, status="executing", action_id=action_id)
        return action_id

    def finish_action(
        self,
        conn: Any,
        *,
        action_id: int,
        status: str,
        result_payload: Optional[Dict[str, Any]] = None,
        error_text: Optional[str] = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.engineering_action
                SET status = %s,
                    finished_at = NOW(),
                    result_payload = COALESCE(%s, result_payload),
                    error_text = %s
                WHERE id = %s
                """,
                (status, Json(result_payload) if result_payload is not None else None, (error_text or "")[:4000] or None, action_id),
            )

    def create_patch(
        self,
        conn: Any,
        *,
        incident_id: int,
        action_id: int,
        patch_type: str,
        target_files: List[str],
        diff_path: str,
        backup_path: str,
        lines_changed: int,
        bytes_changed: int,
        metadata: Dict[str, Any],
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_patch (
                    incident_id, action_id, patch_type, status, target_files, diff_path, backup_path,
                    lines_changed, bytes_changed, metadata
                )
                VALUES (%s, %s, %s, 'applied', %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    incident_id,
                    action_id,
                    patch_type,
                    Json(target_files),
                    diff_path,
                    backup_path,
                    lines_changed,
                    bytes_changed,
                    Json(metadata),
                ),
            )
            return int(cur.fetchone()[0])

    def mark_patch_rolled_back(self, conn: Any, *, patch_id: int, rollback_path: str, metadata: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.engineering_patch
                SET status = 'rolled_back',
                    rollback_path = %s,
                    metadata = metadata || %s::jsonb
                WHERE id = %s
                """,
                (rollback_path, Json(metadata), patch_id),
            )

    def record_smoketest(
        self,
        conn: Any,
        *,
        incident_id: int,
        action_id: Optional[int],
        test_name: str,
        status: str,
        result_json: Dict[str, Any],
        output_path: Optional[str],
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_smoketest (
                    incident_id, action_id, test_name, status, result_json, output_path, started_at, finished_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (incident_id, action_id, test_name, status, Json(result_json), output_path),
            )

    def list_recent_incidents(self, conn: Any, *, limit: int = 20) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.engineering_incident
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    def list_patch_history(self, conn: Any, *, limit: int = 20) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT p.*, i.summary AS incident_summary
                FROM research.engineering_patch p
                JOIN research.engineering_incident i ON i.id = p.incident_id
                ORDER BY p.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    def list_recent_smoketests(self, conn: Any, *, incident_id: Optional[int] = None, limit: int = 20) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if incident_id is None:
                cur.execute(
                    """
                    SELECT *
                    FROM research.engineering_smoketest
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM research.engineering_smoketest
                    WHERE incident_id = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (incident_id, limit),
                )
            return [dict(row) for row in cur.fetchall()]

    def get_latest_patch(self, conn: Any, *, incident_id: Optional[int] = None) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if incident_id is None:
                cur.execute("SELECT * FROM research.engineering_patch ORDER BY id DESC LIMIT 1")
            else:
                cur.execute("SELECT * FROM research.engineering_patch WHERE incident_id = %s ORDER BY id DESC LIMIT 1", (incident_id,))
            row = cur.fetchone()
        return dict(row) if row else {}

    def restart_actions_last_hour(self, conn: Any) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM research.engineering_action
                WHERE action_type = 'restart_services'
                  AND started_at >= NOW() - INTERVAL '1 hour'
                """
            )
            return int(cur.fetchone()[0])
