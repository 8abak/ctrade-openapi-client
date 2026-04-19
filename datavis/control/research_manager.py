from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

import psycopg2.extras
from psycopg2.extras import Json

from datavis.control.config import ControlSettings
from datavis.control.panel_state import resolve_research_runtime
from datavis.control.service_manager import ServiceManager
from datavis.research.guardrails import default_parameters
from datavis.research.guardrails import sanitize_parameters
from datavis.research.mutation import generate_mutation_proposals, summarize_history
from datavis.research.state import CONTROL_STATE_KEY, default_control_state, ensure_control_state, normalize_brokerday_text, set_state


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

    def selected_study_day(self, conn: Any) -> Optional[str]:
        control = ensure_control_state(conn, self._settings.research_settings)
        return normalize_brokerday_text(control.get("selected_study_day"))

    def set_selected_study_day(self, conn: Any, *, brokerday_text: Optional[str]) -> Dict[str, Any]:
        selected_day = normalize_brokerday_text(brokerday_text)
        if selected_day and not self._brokerday_exists(conn, selected_day):
            raise ValueError(f"broker day {selected_day} is not available for {self._settings.research_settings.symbol}")
        control = ensure_control_state(conn, self._settings.research_settings)
        control["selected_study_day"] = selected_day
        set_state(conn, CONTROL_STATE_KEY, control)
        return self.study_day_state(conn, control=control)

    def study_day_state(self, conn: Any, *, control: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        resolved_control = dict(control or ensure_control_state(conn, self._settings.research_settings))
        selected_day = normalize_brokerday_text(resolved_control.get("selected_study_day"))
        available_days = self.list_available_brokerdays(conn, limit=30)
        latest_available = available_days[0] if available_days else None
        return {
            "selectedStudyDay": selected_day,
            "effectiveStudyDay": selected_day or latest_available,
            "availableBrokerdays": available_days,
            "latestAvailableBrokerday": latest_available,
        }

    def pause(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        control = ensure_control_state(conn, self._settings.research_settings)
        control["paused"] = True
        control["paused_by"] = "engineering_control"
        control["pause_reason"] = reason[:512]
        set_state(conn, CONTROL_STATE_KEY, control)
        return control

    def resume(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        control = ensure_control_state(conn, self._settings.research_settings)
        cleared_final_verdict = str(control.get("final_verdict") or "")
        self._wake_research_loop(control, reason=reason, cleared_final_verdict=cleared_final_verdict or None)
        pending_or_running = self._latest_job(conn, statuses=("pending", "running"))
        selected_day = normalize_brokerday_text(control.get("selected_study_day"))
        if pending_or_running:
            control["seeded"] = True
            job_day = self._job_study_day(pending_or_running)
            if selected_day and job_day and job_day != selected_day:
                control["last_resume_day_mismatch"] = {
                    "selectedStudyDay": selected_day,
                    "queuedJobId": int(pending_or_running["id"]),
                    "queuedJobStudyDay": job_day,
                }
        if cleared_final_verdict:
            control["last_resume_cleared_final_verdict"] = cleared_final_verdict
        service_actions = self._ensure_research_services_ready()
        if service_actions:
            control["last_resume_service_actions"] = service_actions
        set_state(conn, CONTROL_STATE_KEY, control)
        seed_result = None
        if pending_or_running is None:
            seed_result = self.seed_next_job(conn, reason=f"resume wake: {reason[:480]}")
            control = ensure_control_state(conn, self._settings.research_settings)
        result = dict(control)
        result["serviceActions"] = service_actions
        if seed_result is not None:
            result["seedResult"] = seed_result
        result["message"] = "Research loop resumed and consumption wake-up was attempted."
        return result

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
        self._wake_research_loop(control, reason=reason)
        control["seeded"] = True
        service_actions = self._ensure_research_services_ready()
        if service_actions:
            control["last_requeue_service_actions"] = service_actions
        set_state(conn, CONTROL_STATE_KEY, control)
        return {
            "requeuedFromJobId": int(payload["id"]),
            "jobId": new_job_id,
            "serviceActions": service_actions,
            "message": "Failed research job requeued and wake-up was attempted.",
        }

    def restart_services(self, service_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        targets = service_names or list(self._settings.research_services)
        snapshots = []
        for service_name in targets:
            snapshots.append(self._service_manager.restart_with_reset_tolerance(service_name))
        return snapshots

    def seed_next_job(self, conn: Any, *, reason: str) -> Dict[str, Any]:
        runtime_policy = resolve_research_runtime(conn, self._settings, self._settings.research_settings)
        selected_day = self.selected_study_day(conn)
        existing_pending = self._latest_job(conn, statuses=("pending", "running"))
        if existing_pending:
            job_day = self._job_study_day(existing_pending)
            if selected_day and job_day != selected_day:
                raise ValueError(
                    f"queued work targets broker day {job_day or 'latest available'}; clear or finish that work before seeding {selected_day}"
                )
            control = ensure_control_state(conn, self._settings.research_settings)
            self._wake_research_loop(control, reason=reason)
            control["seeded"] = True
            service_actions = self._ensure_research_services_ready()
            if service_actions:
                control["last_seed_service_actions"] = service_actions
            set_state(conn, CONTROL_STATE_KEY, control)
            return {
                "jobId": int(existing_pending["id"]),
                "config": dict(existing_pending.get("config") or {}),
                "reusedExisting": True,
                "proposalDerived": self._job_is_proposal_derived(existing_pending),
                "serviceActions": service_actions,
                "message": "Existing queued work was kept and the worker wake-up path was triggered.",
            }

        selected = self._select_seed_job(conn, runtime_policy=runtime_policy, reason=reason, study_day=selected_day)
        if selected is None:
            if self._has_any_run(conn):
                return {
                    "jobId": None,
                    "config": {},
                    "reusedExisting": False,
                    "proposalDerived": False,
                    "message": "no novel bounded job was available to seed",
                }
            slice_ladder = list(runtime_policy.get("approvedSliceLadder") or self._settings.research_settings.slice_ladder)
            params = default_parameters(
                symbol=self._settings.research_settings.symbol,
                slice_rows=int(slice_ladder[0] if slice_ladder else self._settings.research_settings.seed_slice_rows),
                warmup_rows=self._settings.research_settings.seed_warmup_rows,
                iteration=1,
            )
            selected = {
                "params": sanitize_parameters(
                    params.model_copy(
                        update={
                            "study_brokerday": selected_day,
                            "side_lock": runtime_policy.get("preferredSideLock") or params.side_lock,
                            "mutation_note": reason[:512],
                        }
                    ).model_dump(),
                    limits=self._build_limits(),
                ),
                "action": "seed_next_job",
                "reason": reason[:512],
                "proposalSource": "control-panel-default",
                "mutatedFields": [],
            }

        inserted = self._insert_seed_job(
            conn,
            params=selected["params"],
            action=str(selected.get("action") or "seed_next_job"),
            reason=str(selected.get("reason") or reason),
            proposal_source=str(selected.get("proposalSource") or "control-panel"),
            mutated_fields=list(selected.get("mutatedFields") or []),
        )
        if inserted.get("reusedExisting"):
            return inserted
        control = ensure_control_state(conn, self._settings.research_settings)
        self._wake_research_loop(control, reason=reason)
        control["seeded"] = True
        control["last_seeded_job_id"] = inserted.get("jobId")
        if selected_day:
            control["selected_study_day"] = selected_day
        service_actions = self._ensure_research_services_ready()
        if service_actions:
            control["last_seed_service_actions"] = service_actions
        set_state(conn, CONTROL_STATE_KEY, control)
        inserted["serviceActions"] = service_actions
        return inserted

    def _build_limits(self):  # type: ignore[no-untyped-def]
        from datavis.research.guardrails import SearchGuardrails

        settings = self._settings.research_settings
        return SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
            max_slice_offset_rows=settings.max_slice_offset_rows,
            max_next_actions=settings.max_next_jobs,
        )

    def _ensure_research_services_ready(self) -> List[Dict[str, str]]:
        actions: List[Dict[str, str]] = []
        for service_name in self._settings.research_services:
            snapshot = self._service_manager.ensure_running(service_name)
            requested_action = str(snapshot.get("requestedAction") or "noop")
            warnings = list(snapshot.get("warnings") or [])
            if requested_action == "noop" and not warnings:
                continue
            action_payload: Dict[str, str] = {
                "service": service_name,
                "action": requested_action,
            }
            if warnings:
                action_payload["warning"] = warnings[0]
            actions.append(action_payload)
        return actions

    @staticmethod
    def _wake_research_loop(
        control: Dict[str, Any],
        *,
        reason: str,
        cleared_final_verdict: str | None = None,
    ) -> None:
        control["paused"] = False
        control["paused_by"] = None
        control["pause_reason"] = reason[:512]
        control["stop_requested"] = False
        control["final_verdict"] = None
        control["final_reason"] = None
        if cleared_final_verdict:
            control["last_resume_cleared_final_verdict"] = cleared_final_verdict

    def _latest_job(self, conn: Any, *, statuses: Sequence[str]) -> Optional[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.job
                WHERE status = ANY(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (list(statuses),),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def _has_any_run(self, conn: Any) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM research.run LIMIT 1")
            return cur.fetchone() is not None

    def _job_exists_for_fingerprint(self, conn: Any, fingerprint: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM research.job
                WHERE config->>'config_fingerprint' = %s
                LIMIT 1
                """,
                (fingerprint,),
            )
            return cur.fetchone() is not None

    def _find_pending_or_running_job(self, conn: Any, fingerprint: str) -> Optional[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.job
                WHERE config->>'config_fingerprint' = %s
                  AND status IN ('pending', 'running')
                ORDER BY id DESC
                LIMIT 1
                """,
                (fingerprint,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def _insert_seed_job(
        self,
        conn: Any,
        *,
        params: Any,
        action: str,
        reason: str,
        proposal_source: str,
        mutated_fields: Sequence[str],
    ) -> Dict[str, Any]:
        fingerprint = str(params.config_fingerprint or "")
        existing = self._find_pending_or_running_job(conn, fingerprint)
        if existing:
            return {
                "jobId": int(existing["id"]),
                "config": dict(existing.get("config") or {}),
                "reusedExisting": True,
                "proposalDerived": self._job_is_proposal_derived(existing),
                "message": "matching pending/running job already exists",
            }
        if self._job_exists_for_fingerprint(conn, fingerprint):
            return {
                "jobId": None,
                "config": params.model_dump(),
                "reusedExisting": False,
                "proposalDerived": action != "seed_next_job" or bool(params.source_run_id or params.seed_rule or params.mutation_note),
                "message": "matching job fingerprint already exists in history; skipped duplicate seed",
            }
        seed_rule = dict(params.seed_rule.model_dump() if getattr(params, "seed_rule", None) else {})
        guardrails = {
            "bounded": True,
            "action": action,
            "reason": reason[:512],
            "proposalSource": proposal_source,
            "mutatedFields": [str(item) for item in mutated_fields if item][:16],
            "seedRuleRef": {
                "name": seed_rule.get("name"),
                "family": seed_rule.get("family"),
                "side": seed_rule.get("side"),
            } if seed_rule else None,
        }
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                    Json(guardrails),
                ),
            )
            job_id = int(cur.fetchone()["id"])
        return {
            "jobId": job_id,
            "config": params.model_dump(),
            "reusedExisting": False,
            "proposalDerived": action != "seed_next_job" or bool(params.source_run_id or params.seed_rule or params.mutation_note),
            "message": "seeded bounded research job",
        }

    def _select_seed_job(
        self,
        conn: Any,
        *,
        runtime_policy: Mapping[str, Any],
        reason: str,
        study_day: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        for proposal in self._latest_decision_proposals(conn):
            selected = self._proposal_to_seed_job(
                conn,
                proposal=proposal,
                proposal_source="decision-briefing",
                default_reason=reason,
                study_day=study_day,
            )
            if selected:
                return selected
        for proposal in self._latest_runsummary_proposals(conn):
            selected = self._proposal_to_seed_job(
                conn,
                proposal=proposal,
                proposal_source="runsummary-mutation",
                default_reason=reason,
                study_day=study_day,
            )
            if selected:
                return selected

        latest_completed = self._latest_completed_run_summary(conn)
        if not latest_completed:
            return None
        base_params = sanitize_parameters(latest_completed["config"], limits=self._build_limits())
        proposals = generate_mutation_proposals(
            base_params=base_params,
            summary_payload=latest_completed["summaryPayload"],
            settings=self._settings.research_settings,
            source_run_id=int(latest_completed["runId"]),
            seen_fingerprints=summarize_history(self._recent_runs(conn)).get("seenFingerprints") or [],
            pending_fingerprints=self._pending_or_running_fingerprints(conn),
            policy=runtime_policy,
        )
        for proposal in proposals:
            selected = self._proposal_to_seed_job(
                conn,
                proposal=proposal,
                proposal_source="generated-fallback",
                default_reason=reason,
                study_day=study_day,
            )
            if selected:
                return selected
        return None

    def _proposal_to_seed_job(
        self,
        conn: Any,
        *,
        proposal: Mapping[str, Any],
        proposal_source: str,
        default_reason: str,
        study_day: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        params_payload = dict(proposal.get("parameters") or {})
        if not params_payload:
            return None
        if study_day:
            params_payload["study_brokerday"] = study_day
        if not params_payload.get("mutation_note"):
            params_payload["mutation_note"] = str(proposal.get("reason") or default_reason)[:512]
        params = sanitize_parameters(params_payload, limits=self._build_limits())
        if self._job_exists_for_fingerprint(conn, str(params.config_fingerprint or "")):
            return None
        return {
            "params": params,
            "action": str(proposal.get("action") or "seed_next_job"),
            "reason": str(proposal.get("reason") or default_reason)[:512],
            "proposalSource": proposal_source,
            "mutatedFields": list(proposal.get("mutatedFields") or []),
        }

    def _latest_decision_proposals(self, conn: Any) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT briefing
                FROM research.decision
                WHERE briefing IS NOT NULL
                ORDER BY id DESC
                LIMIT 5
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
        proposals: List[Dict[str, Any]] = []
        for row in rows:
            briefing = dict(row.get("briefing") or {})
            proposals.extend(list(briefing.get("proposedNextJobs") or []))
        return proposals

    def _latest_runsummary_proposals(self, conn: Any) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT rs.briefing_json
                FROM research.runsummary rs
                JOIN research.run r ON r.id = rs.run_id
                ORDER BY r.id DESC
                LIMIT 5
                """
            )
            rows = [dict(row) for row in cur.fetchall()]
        proposals: List[Dict[str, Any]] = []
        for row in rows:
            briefing = dict(row.get("briefing_json") or {})
            proposals.extend(list(briefing.get("mutationProposals") or []))
        return proposals

    def _latest_completed_run_summary(self, conn: Any) -> Optional[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.id AS run_id,
                       r.config,
                       rs.briefing_json,
                       rs.top_candidates_json
                FROM research.run r
                JOIN research.runsummary rs ON rs.run_id = r.id
                WHERE r.status = 'completed'
                ORDER BY r.id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            return None
        payload = dict(row)
        briefing = dict(payload.get("briefing_json") or {})
        best = dict(briefing.get("bestCandidate") or {})
        return {
            "runId": int(payload["run_id"]),
            "config": dict(payload.get("config") or {}),
            "summaryPayload": {
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
                "candidateResults": list(payload.get("top_candidates_json") or []),
            },
        }

    def _recent_runs(self, conn: Any) -> List[Dict[str, Any]]:
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
                (self._settings.research_settings.max_history_runs,),
            )
            rows = []
            for row in cur.fetchall():
                payload = dict(row)
                payload["metrics"] = dict(payload.pop("metrics_json") or {})
                rows.append(payload)
        return rows

    def _pending_or_running_fingerprints(self, conn: Any) -> List[str]:
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

    def list_available_brokerdays(self, conn: Any, *, limit: int = 30) -> List[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT (((timestamp AT TIME ZONE 'Australia/Sydney') - INTERVAL '8 hours')::date) AS brokerday
                FROM public.ticks
                WHERE symbol = %s
                ORDER BY brokerday DESC
                LIMIT %s
                """,
                (self._settings.research_settings.symbol, limit),
            )
            return [normalize_brokerday_text(row[0]) for row in cur.fetchall() if row and row[0]]

    def _brokerday_exists(self, conn: Any, brokerday_text: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM public.ticks
                WHERE symbol = %s
                  AND (((timestamp AT TIME ZONE 'Australia/Sydney') - INTERVAL '8 hours')::date) = %s::date
                LIMIT 1
                """,
                (self._settings.research_settings.symbol, brokerday_text),
            )
            return cur.fetchone() is not None

    @staticmethod
    def _job_study_day(job: Mapping[str, Any]) -> Optional[str]:
        config = dict(job.get("config") or {})
        return normalize_brokerday_text(config.get("study_brokerday"))

    @staticmethod
    def _job_is_proposal_derived(job: Mapping[str, Any]) -> bool:
        config = dict(job.get("config") or {})
        guardrails = dict(job.get("guardrails") or {})
        action = str(guardrails.get("action") or "")
        return bool(
            config.get("source_run_id")
            or config.get("seed_rule")
            or config.get("mutation_note")
            or action not in {"", "seed_next_job"}
        )
