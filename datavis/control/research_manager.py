from __future__ import annotations

from typing import Any, Dict, List, Optional

import psycopg2.extras
from psycopg2.extras import Json

from datavis.control.config import ControlSettings
from datavis.control.panel_state import resolve_research_runtime
from datavis.control.service_manager import ServiceManager
from datavis.research.guardrails import default_parameters
from datavis.research.guardrails import sanitize_parameters
from datavis.research.state import CONTROL_STATE_KEY, default_control_state, ensure_control_state, set_state


class ResearchManager:
    def __init__(self, settings: ControlSettings, service_manager: ServiceManager) -> None:
        self._settings = settings
        self._service_manager = service_manager

    def status(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM research.vw_loop_status")
            row = cur.fetchone()
            status = dict(row) if row else {}
        status["services"] = [item.model_dump() for item in self._service_manager.status_many(self._settings.research_services)]
        return status

    def latest_run(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json
                FROM research.run r
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                ORDER BY r.id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return dict(row) if row else {}

    def latest_errors(self, conn: Any, *, limit: int = 20) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                (
                    SELECT 'job' AS source, id, error_text AS error, finished_at AS created_at, jsonb_build_object('status', status, 'jobType', job_type) AS metadata
                    FROM research.job
                    WHERE error_text IS NOT NULL AND error_text <> ''
                )
                UNION ALL
                (
                    SELECT 'decision' AS source, id, COALESCE(validation_error, reason, raw_response) AS error, completed_at AS created_at, jsonb_build_object('status', status, 'decision', decision) AS metadata
                    FROM research.decision
                    WHERE validation_error IS NOT NULL OR status = 'rejected'
                )
                UNION ALL
                (
                    SELECT 'run' AS source, id, status AS error, finished_at AS created_at, jsonb_build_object('status', status, 'jobId', job_id) AS metadata
                    FROM research.run
                    WHERE status = 'failed'
                )
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]

    def recent_journals(self, conn: Any, *, limit: int = 100, component: Optional[str] = None) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if component:
                cur.execute(
                    """
                    SELECT *
                    FROM research.journal
                    WHERE component = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (component, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM research.journal
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            return [dict(row) for row in cur.fetchall()]

    def pause(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        control = ensure_control_state(conn, self._settings.research_settings)
        control["paused"] = True
        control["paused_by"] = "engineering_control"
        control["pause_reason"] = reason[:512]
        set_state(conn, CONTROL_STATE_KEY, control)
        return control

    def resume(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        control = ensure_control_state(conn, self._settings.research_settings)
        if str(control.get("paused_by") or "") == "engineering_control":
            control["paused"] = False
            control["paused_by"] = None
            control["pause_reason"] = reason[:512]
            set_state(conn, CONTROL_STATE_KEY, control)
        return control

    def reset(self, conn: Any, *, mode: str, reason: str) -> Dict[str, Any]:
        if mode == "hard":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE TABLE
                        research.candidate_result,
                        research.feature_snapshot,
                        research.entry_label,
                        research.artifact,
                        research.decision,
                        research.runsummary,
                        research.run,
                        research.job
                    RESTART IDENTITY CASCADE
                    """
                )
            payload = default_control_state(self._settings.research_settings)
            payload["reset_reason"] = reason[:512]
            set_state(conn, CONTROL_STATE_KEY, payload)
            return payload
        control = ensure_control_state(conn, self._settings.research_settings)
        control.update(default_control_state(self._settings.research_settings))
        control["reset_reason"] = reason[:512]
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.job
                SET status = 'failed',
                    finished_at = NOW(),
                    error_text = COALESCE(NULLIF(error_text, ''), 'control-plane soft reset'),
                    last_heartbeat_at = NOW()
                WHERE status IN ('pending', 'running')
                """
            )
            cur.execute(
                """
                UPDATE research.run
                SET status = 'failed',
                    finished_at = NOW()
                WHERE status = 'running'
                """
            )
            cur.execute(
                """
                UPDATE research.decision
                SET status = CASE WHEN status = 'running' THEN 'rejected' ELSE status END,
                    validation_error = COALESCE(validation_error, 'control-plane soft reset'),
                    applied_at = COALESCE(applied_at, NOW())
                WHERE status IN ('pending', 'running', 'rejected')
                """
            )
        set_state(conn, CONTROL_STATE_KEY, control)
        return control

    def requeue(self, conn: Any, *, job_id: Optional[int], reason: str) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if job_id is None:
                cur.execute(
                    """
                    SELECT *
                    FROM research.job
                    WHERE status = 'failed'
                    ORDER BY id DESC
                    LIMIT 1
                    """
                )
            else:
                cur.execute("SELECT * FROM research.job WHERE id = %s", (job_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("no failed research job available to requeue")
            payload = dict(row)
            params = sanitize_parameters(payload["config"] or {}, limits=self._build_limits())
            cur.execute(
                """
                INSERT INTO research.job (
                    job_type, status, priority, requested_by, config, guardrails, max_attempts, parent_decision_id, parent_job_id
                )
                VALUES ('entry_research', 'pending', 100, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    "engineering-control",
                    Json(params.model_dump()),
                    Json({"bounded": True, "repairReason": reason[:512], "requeuedFromJobId": int(payload["id"])}),
                    int(payload.get("max_attempts") or 2),
                    payload.get("parent_decision_id"),
                    int(payload["id"]),
                ),
            )
            new_job_id = int(cur.fetchone()["id"])
        control = ensure_control_state(conn, self._settings.research_settings)
        if str(control.get("paused_by") or "") == "engineering_control":
            control["paused"] = False
            control["paused_by"] = None
            set_state(conn, CONTROL_STATE_KEY, control)
        return {"requeuedFromJobId": int(payload["id"]), "jobId": new_job_id}

    def restart_services(self, service_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        targets = service_names or list(self._settings.research_services)
        snapshots = []
        for service_name in targets:
            self._service_manager.reset_failed(service_name)
            snapshots.append(self._service_manager.restart(service_name).model_dump())
        return snapshots

    def seed_next_job(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        runtime_policy = resolve_research_runtime(conn, self._settings, self._settings.research_settings)
        slice_ladder = list(runtime_policy.get("approvedSliceLadder") or self._settings.research_settings.slice_ladder)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT config, iteration
                FROM research.run
                ORDER BY id DESC
                LIMIT 1
                """
            )
            latest = dict(cur.fetchone() or {})
            if latest:
                latest_config = sanitize_parameters(latest.get("config") or {}, limits=self._build_limits())
                next_payload = latest_config.model_copy(
                    update={
                        "iteration": int(latest.get("iteration") or latest_config.iteration) + 1,
                        "slice_rows": int(slice_ladder[0] if slice_ladder else latest_config.slice_rows),
                        "side_lock": runtime_policy.get("preferredSideLock") or latest_config.side_lock,
                        "mutation_note": reason[:512],
                    }
                ).model_dump()
            else:
                params = default_parameters(
                    symbol=self._settings.research_settings.symbol,
                    slice_rows=int(slice_ladder[0] if slice_ladder else self._settings.research_settings.seed_slice_rows),
                    warmup_rows=self._settings.research_settings.seed_warmup_rows,
                    iteration=1,
                )
                next_payload = params.model_copy(
                    update={
                        "side_lock": runtime_policy.get("preferredSideLock") or params.side_lock,
                        "mutation_note": reason[:512],
                    }
                ).model_dump()
            params = sanitize_parameters(next_payload, limits=self._build_limits())
            cur.execute(
                """
                INSERT INTO research.job (
                    job_type, status, priority, requested_by, config, guardrails, max_attempts
                )
                VALUES ('entry_research', 'pending', 100, %s, %s, %s, 2)
                RETURNING id
                """,
                (
                    "control-panel",
                    Json(params.model_dump()),
                    Json({"bounded": True, "action": "seed_next_job", "reason": reason[:512]}),
                ),
            )
            job_id = int(cur.fetchone()["id"])
        return {"jobId": job_id, "config": params.model_dump()}

    def _build_limits(self):  # type: ignore[no-untyped-def]
        from datavis.research.guardrails import SearchGuardrails

        settings = self._settings.research_settings
        return SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
            max_slice_offset_rows=settings.max_slice_offset_rows,
            max_next_actions=settings.max_next_jobs,
        )
