from __future__ import annotations

import time
from typing import Any, Dict, Optional

import psycopg2.extras
from psycopg2.extras import Json

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import SearchGuardrails, default_parameters, sanitize_parameters, validate_supervisor_decision
from datavis.research.journal import ResearchJournal
from datavis.research.state import CONTROL_STATE_KEY, default_control_state, ensure_control_state, get_state, set_state


class ResearchOrchestrator:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings
        self._journal = ResearchJournal(settings, "orchestrator")
        self._limits = SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
        )

    def run_forever(self, conn_factory: Any) -> None:
        self._journal.write(level="INFO", event_type="orchestrator.start", message="orchestrator loop started")
        while True:
            with conn_factory(readonly=False, autocommit=False) as conn:
                did_work = self.run_once(conn)
                conn.commit()
            if not did_work:
                time.sleep(self._settings.orchestrator_poll_seconds)

    def run_once(self, conn: Any) -> bool:
        control = ensure_control_state(conn, self._settings)
        if control.get("final_verdict"):
            return False
        if self._apply_budget_stop(conn, control):
            return True
        if not control.get("seeded"):
            self._seed_first_job(conn, control)
            return True
        if self._queue_pending_decision(conn):
            return True
        if self._apply_completed_decision(conn, control):
            return True
        if self._handle_rejected_decision(conn, control):
            return True
        return False

    def _apply_budget_stop(self, conn: Any, control: Dict[str, Any]) -> bool:
        iterations_completed = int(control.get("iterations_completed") or 0)
        iteration_budget = int(control.get("iteration_budget") or self._settings.iteration_budget)
        if iterations_completed < iteration_budget:
            return False
        control["final_verdict"] = "stopped_by_budget_guardrail"
        control["final_reason"] = f"iteration budget {iteration_budget} reached"
        set_state(conn, CONTROL_STATE_KEY, control)
        self._journal.write(
            level="INFO",
            event_type="orchestrator.stop.budget",
            message="loop stopped by budget guardrail",
            payload={"iterationsCompleted": iterations_completed, "iterationBudget": iteration_budget},
            conn=conn,
        )
        return True

    def _seed_first_job(self, conn: Any, control: Dict[str, Any]) -> None:
        params = default_parameters(
            symbol=self._settings.symbol,
            slice_rows=self._settings.seed_slice_rows,
            warmup_rows=self._settings.seed_warmup_rows,
            iteration=1,
        )
        self._insert_job(conn, params.model_dump(), requested_by="orchestrator.seed", parent_decision_id=None, parent_job_id=None)
        control["seeded"] = True
        set_state(conn, CONTROL_STATE_KEY, control)
        self._journal.write(
            level="INFO",
            event_type="orchestrator.seeded",
            message="seeded initial entry research job",
            payload=params.model_dump(),
            conn=conn,
        )

    def _queue_pending_decision(self, conn: Any) -> bool:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.id AS run_id,
                       r.config,
                       rs.briefing_json,
                       rs.verdict_hint
                FROM research.run r
                JOIN research.runsummary rs ON rs.run_id = r.id
                LEFT JOIN research.decision d ON d.run_id = r.id
                WHERE r.status = 'completed'
                  AND d.id IS NULL
                ORDER BY r.finished_at ASC NULLS LAST, r.id ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return False
            briefing = dict(row["briefing_json"] or {})
            briefing["config"] = dict(row["config"] or {})
            briefing["priorDecisions"] = self._fetch_prior_decisions(conn)
            cur.execute(
                """
                INSERT INTO research.decision (run_id, status, briefing, orchestrator_name)
                VALUES (%s, 'pending', %s, %s)
                """,
                (int(row["run_id"]), Json(briefing), self._settings.orchestrator_name),
            )
        self._journal.write(
            level="INFO",
            event_type="orchestrator.decision.queued",
            message=f"queued supervisor decision for run {int(row['run_id'])}",
            run_id=int(row["run_id"]),
            payload={"verdictHint": row["verdict_hint"]},
            conn=conn,
        )
        return True

    def _apply_completed_decision(self, conn: Any, control: Dict[str, Any]) -> bool:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT d.id,
                       d.run_id,
                       d.decision_json,
                       d.status,
                       r.job_id,
                       r.config
                FROM research.decision d
                JOIN research.run r ON r.id = d.run_id
                WHERE d.status = 'completed'
                  AND d.applied_at IS NULL
                ORDER BY d.completed_at ASC NULLS LAST, d.id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()
            if not row:
                return False
            payload = dict(row)
            base_params = sanitize_parameters(payload["config"] or {}, limits=self._limits)
            decision, next_params = validate_supervisor_decision(payload["decision_json"] or {}, base_parameters=base_params, limits=self._limits)
            if decision.decision == "stop":
                control["final_verdict"] = decision.stop_reason or "no_robust_edge_found"
                control["final_reason"] = decision.reason
            else:
                assert next_params is not None
                self._insert_job(
                    conn,
                    next_params.model_dump(),
                    requested_by="orchestrator.supervisor",
                    parent_decision_id=int(payload["id"]),
                    parent_job_id=int(payload["job_id"]),
                )
                control["last_decision_id"] = int(payload["id"])
            set_state(conn, CONTROL_STATE_KEY, control)
            cur.execute("UPDATE research.decision SET applied_at = NOW() WHERE id = %s", (int(payload["id"]),))
        self._journal.write(
            level="INFO",
            event_type="orchestrator.decision.applied",
            message=f"applied decision {int(payload['id'])}",
            decision_id=int(payload["id"]),
            run_id=int(payload["run_id"]),
            payload={"decision": decision.decision, "finalVerdict": control.get("final_verdict")},
            conn=conn,
        )
        return True

    def _handle_rejected_decision(self, conn: Any, control: Dict[str, Any]) -> bool:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, run_id, validation_error
                FROM research.decision
                WHERE status = 'rejected'
                  AND applied_at IS NULL
                ORDER BY completed_at ASC NULLS LAST, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()
            if not row:
                return False
            payload = dict(row)
            control["final_verdict"] = "supervisor_instruction_rejected"
            control["final_reason"] = str(payload.get("validation_error") or "unsafe supervisor instruction")
            set_state(conn, CONTROL_STATE_KEY, control)
            cur.execute("UPDATE research.decision SET applied_at = NOW() WHERE id = %s", (int(payload["id"]),))
        self._journal.write(
            level="ERROR",
            event_type="orchestrator.decision.rejected",
            message=f"stopped after rejected decision {int(payload['id'])}",
            decision_id=int(payload["id"]),
            run_id=int(payload["run_id"]),
            payload={"error": payload.get("validation_error")},
            conn=conn,
        )
        return True

    def _fetch_prior_decisions(self, conn: Any) -> list[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT decision, reason, stop_reason, decision_json
                FROM research.decision
                WHERE status IN ('completed', 'applied')
                ORDER BY completed_at DESC NULLS LAST, id DESC
                LIMIT 3
                """
            )
            return [dict(row) for row in cur.fetchall()]

    def _insert_job(
        self,
        conn: Any,
        config: Dict[str, Any],
        *,
        requested_by: str,
        parent_decision_id: Optional[int],
        parent_job_id: Optional[int],
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.job (
                    job_type, status, priority, requested_by, config, guardrails, max_attempts, parent_decision_id, parent_job_id
                )
                VALUES ('entry_research', 'pending', 100, %s, %s, %s, 2, %s, %s)
                """,
                (
                    requested_by,
                    Json(config),
                    Json({"bounded": True, "oneActiveWorkerJob": True}),
                    parent_decision_id,
                    parent_job_id,
                ),
            )
