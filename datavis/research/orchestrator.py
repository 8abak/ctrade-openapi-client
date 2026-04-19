from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Sequence

import psycopg2.extras
from psycopg2.extras import Json

from datavis.control.config import load_settings as load_control_settings
from datavis.control.panel_state import mission_briefing_payload, resolve_research_runtime
from datavis.research.config import ResearchSettings
from datavis.research.guardrails import SearchGuardrails, default_parameters, sanitize_parameters, validate_supervisor_decision
from datavis.research.journal import ResearchJournal, write_decision_artifacts
from datavis.research.models import EntryResearchParameterPatch, EntryResearchParameters, SupervisorDecision
from datavis.research.mutation import evaluate_stop_guardrails, generate_mutation_proposals, summarize_history
from datavis.research.state import CONTROL_STATE_KEY, ensure_control_state, set_state


CONTROL_SETTINGS = load_control_settings()


class ResearchOrchestrator:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings
        self._journal = ResearchJournal(settings, "orchestrator")
        self._limits = SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
            max_slice_offset_rows=settings.max_slice_offset_rows,
            max_next_actions=settings.max_next_jobs,
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
        runtime_policy = resolve_research_runtime(conn, CONTROL_SETTINGS, self._settings)
        if not runtime_policy.get("enabled", True):
            return False
        control = ensure_control_state(conn, self._settings)
        if control.get("final_verdict"):
            return False
        if self._apply_budget_stop(conn, control, runtime_policy=runtime_policy):
            return True
        if not control.get("seeded"):
            self._seed_first_job(conn, control, runtime_policy=runtime_policy)
            return True
        if self._queue_pending_decision(conn, runtime_policy=runtime_policy):
            return True
        if self._apply_completed_decision(conn, control, runtime_policy=runtime_policy):
            return True
        if self._handle_rejected_decision(conn, control):
            return True
        return False

    def _apply_budget_stop(self, conn: Any, control: Dict[str, Any], *, runtime_policy: Dict[str, Any]) -> bool:
        iterations_completed = int(control.get("iterations_completed") or 0)
        iteration_budget = int(runtime_policy.get("iterationBudget") or control.get("iteration_budget") or self._settings.iteration_budget)
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

    def _seed_first_job(self, conn: Any, control: Dict[str, Any], *, runtime_policy: Dict[str, Any]) -> None:
        slice_ladder = list(runtime_policy.get("approvedSliceLadder") or self._settings.slice_ladder)
        seed_slice = int(slice_ladder[0] if slice_ladder else self._settings.seed_slice_rows)
        params = default_parameters(
            symbol=self._settings.symbol,
            slice_rows=seed_slice,
            warmup_rows=self._settings.seed_warmup_rows,
            iteration=1,
        ).model_copy(update={"study_brokerday": control.get("selected_study_day")})
        if runtime_policy.get("preferredSideLock") in {"long", "short"}:
            params = params.model_copy(update={"side_lock": runtime_policy["preferredSideLock"]})
        self._insert_job(conn, params, requested_by="orchestrator.seed", parent_decision_id=None, parent_job_id=None, action="continue", reason="initial seed job")
        control["seeded"] = True
        set_state(conn, CONTROL_STATE_KEY, control)
        self._journal.write(
            level="INFO",
            event_type="orchestrator.seeded",
            message="seeded initial entry research job",
            payload=params.model_dump(),
            conn=conn,
        )

    def _queue_pending_decision(self, conn: Any, *, runtime_policy: Dict[str, Any]) -> bool:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.id AS run_id,
                       r.config,
                       rs.briefing_json,
                       rs.top_candidates_json,
                       rs.verdict_hint,
                       rs.metrics_json
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
            payload = dict(row)
            briefing = dict(payload["briefing_json"] or {})
            base_params = sanitize_parameters(payload["config"] or {}, limits=self._limits)
            history_rows = self._fetch_recent_runs(conn)
            history_summary = summarize_history(history_rows)
            pending_fingerprints = self._fetch_pending_job_fingerprints(conn)
            proposals = generate_mutation_proposals(
                base_params=base_params,
                summary_payload=self._rebuild_summary_fragment(briefing, payload.get("top_candidates_json") or []),
                settings=self._settings,
                source_run_id=int(payload["run_id"]),
                seen_fingerprints=history_summary.get("seenFingerprints") or [],
                pending_fingerprints=pending_fingerprints,
                policy=runtime_policy,
            )
            briefing["config"] = base_params.model_dump()
            briefing["priorDecisions"] = self._fetch_prior_decisions(conn)
            briefing["searchHistory"] = history_summary
            briefing["mission"] = mission_briefing_payload(runtime_policy["mission"])
            briefing["stopPolicy"] = {
                "minRunsBeforeStop": runtime_policy["minRunsBeforeStop"],
                "failedDirectionStopCount": runtime_policy["failedDirectionStopCount"],
                "iterationBudget": runtime_policy["iterationBudget"],
            }
            briefing["proposedNextJobs"] = proposals
            cur.execute(
                """
                INSERT INTO research.decision (run_id, status, briefing, orchestrator_name)
                VALUES (%s, 'pending', %s, %s)
                """,
                (int(payload["run_id"]), Json(briefing), self._settings.orchestrator_name),
            )
        self._journal.write(
            level="INFO",
            event_type="orchestrator.decision.queued",
            message=f"queued supervisor decision for run {int(payload['run_id'])}",
            run_id=int(payload["run_id"]),
            payload={"verdictHint": payload["verdict_hint"], "proposalCount": len(proposals)},
            conn=conn,
        )
        return True

    def _apply_completed_decision(self, conn: Any, control: Dict[str, Any], *, runtime_policy: Dict[str, Any]) -> bool:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT d.id,
                       d.run_id,
                       d.briefing,
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
            decision = validate_supervisor_decision(payload["decision_json"] or {})
            briefing = dict(payload.get("briefing") or {})
            base_params = sanitize_parameters(payload["config"] or {}, limits=self._limits)
            run_id = int(payload["run_id"])
            history_rows = self._fetch_recent_runs(conn)
            history_summary = summarize_history(history_rows)
            latest_metrics = dict((((briefing.get("bestCandidate") or {}).get("validationMetrics")) or {}))
            stop_policy = evaluate_stop_guardrails(
                decision_stop_reason=decision.stop_reason,
                control=control,
                latest_metrics=latest_metrics,
                history_summary=history_summary,
                settings=self._settings,
                policy=runtime_policy,
            )
            next_jobs = self._resolve_next_jobs(
                conn,
                decision=decision,
                base_params=base_params,
                briefing=briefing,
                run_id=run_id,
                runtime_policy=runtime_policy,
            )
            stop_accepted = bool(decision.decision == "stop" and stop_policy["allowStop"])
            applied_action = decision.decision
            if decision.decision == "stop" and not stop_accepted:
                applied_action = "stop_overridden"
                control["last_stop_override_reason"] = stop_policy["policyNote"]
                self._journal.write(
                    level="INFO",
                    event_type="orchestrator.stop.overridden",
                    message=f"overrode early stop for decision {int(payload['id'])}",
                    decision_id=int(payload["id"]),
                    run_id=run_id,
                    payload={"policyNote": stop_policy["policyNote"], "proposalCount": len(next_jobs)},
                    conn=conn,
                )
            elif stop_accepted:
                control["final_verdict"] = stop_policy["acceptedReason"]
                control["final_reason"] = decision.reason
            if not stop_accepted:
                inserted_jobs = []
                for job in next_jobs[: int(runtime_policy.get("maxNextJobs") or self._settings.max_next_jobs)]:
                    inserted = self._insert_job(
                        conn,
                        sanitize_parameters(job["parameters"], limits=self._limits),
                        requested_by="orchestrator.supervisor",
                        parent_decision_id=int(payload["id"]),
                        parent_job_id=int(payload["job_id"]),
                        action=str(job.get("action") or decision.decision),
                        reason=str(job.get("reason") or decision.reason),
                    )
                    if inserted:
                        inserted_jobs.append(job)
                next_jobs = inserted_jobs
                if not next_jobs:
                    control["final_verdict"] = "hard_technical_failure"
                    control["final_reason"] = "no bounded novel next job could be enqueued"
            control["last_decision_id"] = int(payload["id"])
            if next_jobs:
                control["last_selected_fingerprint"] = next_jobs[0]["configFingerprint"]
            set_state(conn, CONTROL_STATE_KEY, control)
            cur.execute("UPDATE research.decision SET applied_at = NOW() WHERE id = %s", (int(payload["id"]),))
            decision_artifact = {
                "decision": decision.decision,
                "appliedAction": applied_action,
                "stopAccepted": stop_accepted,
                "reason": decision.reason,
                "policyNote": stop_policy["policyNote"],
                "nextJobs": next_jobs,
            }
            artifact_paths = write_decision_artifacts(self._settings, run_id=run_id, decision_id=int(payload["id"]), payload=decision_artifact)
            self._insert_decision_artifacts(conn, decision_id=int(payload["id"]), run_id=run_id, artifact_paths=artifact_paths)
        self._journal.write(
            level="INFO",
            event_type="orchestrator.decision.applied",
            message=f"applied decision {int(payload['id'])}",
            decision_id=int(payload["id"]),
            run_id=run_id,
            payload={
                "decision": decision.decision,
                "appliedAction": applied_action,
                "stopAccepted": stop_accepted,
                "nextJobCount": len(next_jobs),
                "finalVerdict": control.get("final_verdict"),
            },
            conn=conn,
        )
        return True

    def _resolve_next_jobs(
        self,
        conn: Any,
        *,
        decision: SupervisorDecision,
        base_params: EntryResearchParameters,
        briefing: Dict[str, Any],
        run_id: int,
        runtime_policy: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        next_jobs: List[Dict[str, Any]] = []
        pending_fingerprints = self._fetch_pending_job_fingerprints(conn)
        seen_fingerprints = summarize_history(self._fetch_recent_runs(conn)).get("seenFingerprints") or []
        for action in decision.next_actions[: int(runtime_policy.get("maxNextJobs") or self._settings.max_next_jobs)]:
            patch = action.parameters or EntryResearchParameterPatch()
            next_params = sanitize_parameters(
                base_params.model_copy(
                    update={
                        **patch.model_dump(exclude_none=True),
                        "iteration": base_params.iteration + 1,
                        "source_run_id": run_id,
                        "mutation_note": action.reason[:512],
                    }
                ).model_dump(),
                limits=self._limits,
            )
            next_jobs.append(
                {
                    "action": action.action,
                    "reason": action.reason,
                    "configFingerprint": next_params.config_fingerprint,
                    "parameters": next_params.model_dump(),
                }
            )
        if next_jobs:
            return self._filter_novel_jobs(next_jobs, seen_fingerprints=seen_fingerprints, pending_fingerprints=pending_fingerprints)
        fallback_jobs = list(briefing.get("proposedNextJobs") or [])
        return self._filter_novel_jobs(fallback_jobs, seen_fingerprints=seen_fingerprints, pending_fingerprints=pending_fingerprints)

    @staticmethod
    def _filter_novel_jobs(
        jobs: Sequence[Dict[str, Any]],
        *,
        seen_fingerprints: Sequence[str],
        pending_fingerprints: Sequence[str],
    ) -> List[Dict[str, Any]]:
        blocked = {str(item) for item in seen_fingerprints if item}
        blocked.update(str(item) for item in pending_fingerprints if item)
        filtered = []
        local = set()
        for job in jobs:
            fingerprint = str(job.get("configFingerprint") or "")
            if not fingerprint or fingerprint in blocked or fingerprint in local:
                continue
            filtered.append(job)
            local.add(fingerprint)
        return filtered

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

    def _fetch_recent_runs(self, conn: Any) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.id,
                       r.status,
                       r.config,
                       rs.verdict_hint,
                       rs.metrics_json
                FROM research.run r
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                ORDER BY r.id DESC
                LIMIT %s
                """,
                (self._settings.max_history_runs,),
            )
            rows = []
            for row in cur.fetchall():
                payload = dict(row)
                payload["metrics"] = dict(payload.pop("metrics_json") or {})
                rows.append(payload)
            return rows

    def _fetch_pending_job_fingerprints(self, conn: Any) -> List[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT config->>'config_fingerprint'
                FROM research.job
                WHERE status IN ('pending', 'running')
                  AND (config->>'config_fingerprint') IS NOT NULL
                """
            )
            return [str(row[0]) for row in cur.fetchall() if row and row[0]]

    def _fetch_prior_decisions(self, conn: Any) -> list[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT decision, reason, stop_reason, decision_json
                FROM research.decision
                WHERE status IN ('completed', 'applied')
                ORDER BY completed_at DESC NULLS LAST, id DESC
                LIMIT 5
                """
            )
            return [dict(row) for row in cur.fetchall()]

    @staticmethod
    def _rebuild_summary_fragment(briefing: Dict[str, Any], top_candidates: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        best = dict(briefing.get("bestCandidate") or {})
        return {
            "bestCandidate": {
                "candidateName": best.get("name"),
                "rule": {
                    "name": best.get("name"),
                    "family": best.get("family"),
                    "side": best.get("side"),
                    "predicates": list(best.get("predicates") or []),
                },
                "trainMetrics": dict(best.get("trainMetrics") or {}),
                "validationMetrics": dict(best.get("validationMetrics") or {}),
                "contrastSummary": dict(best.get("contrastSummary") or briefing.get("contrastSummary") or {}),
            },
            "candidateResults": list(top_candidates or []),
        }

    def _insert_job(
        self,
        conn: Any,
        params: EntryResearchParameters,
        *,
        requested_by: str,
        parent_decision_id: Optional[int],
        parent_job_id: Optional[int],
        action: str,
        reason: str,
    ) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM research.job
                WHERE config->>'config_fingerprint' = %s
                  AND status IN ('pending', 'running', 'completed')
                LIMIT 1
                """,
                (params.config_fingerprint,),
            )
            if cur.fetchone():
                return False
            cur.execute(
                """
                INSERT INTO research.job (
                    job_type, status, priority, requested_by, config, guardrails, max_attempts, parent_decision_id, parent_job_id
                )
                VALUES ('entry_research', 'pending', 100, %s, %s, %s, 2, %s, %s)
                """,
                (
                    requested_by,
                    Json(params.model_dump()),
                    Json(
                        {
                            "bounded": True,
                            "oneActiveWorkerJob": True,
                            "action": action,
                            "reason": reason[:512],
                        }
                    ),
                    parent_decision_id,
                    parent_job_id,
                ),
            )
        return True

    def _insert_decision_artifacts(self, conn: Any, *, decision_id: int, run_id: int, artifact_paths: Dict[str, str]) -> None:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO research.artifact (run_id, decision_id, artifact_type, path, metadata)
                VALUES %s
                """,
                [
                    (run_id, decision_id, artifact_type, path, Json({}))
                    for artifact_type, path in artifact_paths.items()
                ],
                page_size=len(artifact_paths),
            )
