from __future__ import annotations

import time
from typing import Any, Dict

from datavis.control.config import ControlSettings
from datavis.control.executor import RepairExecutor
from datavis.control.failure_detector import FailureDetector
from datavis.control.journal import EngineeringJournal
from datavis.control.panel_state import mission_briefing_payload, resolve_engineering_runtime
from datavis.control.research_manager import ResearchManager
from datavis.control.smoke import SmokeRunner
from datavis.control.store import EngineeringStore
from datavis.control.supervisor import EngineeringSupervisor


class EngineeringOrchestrator:
    def __init__(
        self,
        settings: ControlSettings,
        *,
        store: EngineeringStore,
        detector: FailureDetector,
        supervisor: EngineeringSupervisor,
        executor: RepairExecutor,
        smoke_runner: SmokeRunner,
        research_manager: ResearchManager,
    ) -> None:
        self._settings = settings
        self._store = store
        self._detector = detector
        self._supervisor = supervisor
        self._executor = executor
        self._smoke_runner = smoke_runner
        self._research_manager = research_manager
        self._journal = EngineeringJournal(settings, "orchestrator")

    def run_forever(self, conn_factory: Any) -> None:
        self._journal.write(level="INFO", event_type="engineering.start", message="engineering orchestrator loop started")
        while True:
            with conn_factory(readonly=False, autocommit=False) as conn:
                did_work = self.run_once(conn)
                conn.commit()
            if not did_work:
                time.sleep(self._settings.poll_seconds)

    def run_once(self, conn: Any) -> bool:
        runtime_policy = resolve_engineering_runtime(conn, self._settings, self._settings.research_settings)
        if not runtime_policy.get("enabled", True):
            return False
        state = self._store.ensure_state(conn)
        if not state.get("enabled", True) or state.get("paused", False) or state.get("manual_takeover", False):
            return False
        incident = self._store.get_active_incident(conn)
        if not incident:
            candidate = self._detector.detect(conn)
            if candidate is None:
                return False
            incident = self._store.create_incident(conn, candidate)
            self._research_manager.pause(conn, reason=candidate.summary)
            self._journal.write(
                level="WARNING",
                event_type="engineering.incident.opened",
                message=candidate.summary,
                incident_id=int(incident["id"]),
                payload=candidate.model_dump(),
                conn=conn,
            )
        return self._process_incident(conn, incident)

    def _process_incident(self, conn: Any, incident: Dict[str, Any]) -> bool:
        incident_id = int(incident["id"])
        action_id = None
        try:
            self._store.transition_incident(conn, incident_id=incident_id, status="analyzing")
            briefing = self._detector.build_briefing(conn, incident)
            briefing["operatorMission"] = mission_briefing_payload(runtime_policy["mission"])
            briefing["engineeringPolicy"] = {
                "maxRetriesPerIncident": runtime_policy["maxRetriesPerIncident"],
                "restartRateLimitPerHour": runtime_policy["restartRateLimitPerHour"],
                "maxPatchFiles": runtime_policy["maxPatchFiles"],
                "maxPatchLineChanges": runtime_policy["maxPatchLineChanges"],
                "maxPatchBytes": runtime_policy["maxPatchBytes"],
            }
            decision, raw_response = self._supervisor.review_incident(briefing, conn=conn)
            action_id = self._store.start_action(
                conn,
                incident_id=incident_id,
                action_type=decision.decision,
                rationale=decision.reason,
                requested_payload={"decision": decision.model_dump(), "briefing": briefing, "rawSupervisorResponse": raw_response},
            )
            self._journal.write(
                level="INFO",
                event_type="engineering.decision.selected",
                message=f"selected {decision.decision} for incident {incident_id}",
                incident_id=incident_id,
                action_id=action_id,
                payload=decision.model_dump(),
                conn=conn,
            )
            execution = self._executor.execute(conn, incident=incident, decision=decision, action_id=action_id)
            smoke_names = decision.smoke_tests or ["import_modules", "engineering_supervisor_schema"]
            self._store.transition_incident(conn, incident_id=incident_id, status="validating", action_id=action_id)
            smoke_results = self._smoke_runner.run(test_names=smoke_names, incident_id=incident_id, action_id=action_id, conn=conn)
            failed_smokes = [item.model_dump() for item in smoke_results if item.status == "failed"]
            if failed_smokes:
                rollback = None
                if execution.get("patch", {}).get("applied"):
                    rollback = self._executor.rollback_last_patch(conn, incident_id=incident_id)
                self._store.finish_action(
                    conn,
                    action_id=action_id,
                    status="failed",
                    result_payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results], "rollback": rollback},
                    error_text="smoke tests failed",
                )
                updated = self._store.transition_incident(
                    conn,
                    incident_id=incident_id,
                    status="open",
                    summary="Smoke test validation failed after bounded repair attempt.",
                    resolution_payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results], "rollback": rollback},
                    action_id=action_id,
                    increment_retry=True,
                )
                if int(updated.get("retry_count") or 0) >= int(updated.get("max_retries") or self._settings.incident_max_retries):
                    self._store.transition_incident(
                        conn,
                        incident_id=incident_id,
                        status="escalated",
                        summary="Incident escalated after exhausting bounded repair retries.",
                        resolution_payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results], "rollback": rollback},
                        action_id=action_id,
                    )
                    self._journal.write(
                        level="ERROR",
                        event_type="engineering.incident.escalated",
                        message=f"incident {incident_id} escalated after retry exhaustion",
                        incident_id=incident_id,
                        action_id=action_id,
                        payload={"failedSmokeTests": failed_smokes},
                        conn=conn,
                    )
                return True
            final_status = "escalated" if decision.decision == "escalate_manual_review" else "resolved"
            self._store.finish_action(
                conn,
                action_id=action_id,
                status="succeeded",
                result_payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results]},
            )
            if final_status == "resolved":
                self._research_manager.resume(conn, reason=f"incident {incident_id} resolved")
            self._store.transition_incident(
                conn,
                incident_id=incident_id,
                status=final_status,
                summary="Incident resolved by bounded engineering control plane." if final_status == "resolved" else "Incident requires manual review.",
                resolution_payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results]},
                action_id=action_id,
            )
            self._journal.write(
                level="INFO" if final_status == "resolved" else "WARNING",
                event_type=f"engineering.incident.{final_status}",
                message=f"incident {incident_id} {final_status}",
                incident_id=incident_id,
                action_id=action_id,
                payload={"execution": execution, "smokeResults": [item.model_dump() for item in smoke_results]},
                conn=conn,
            )
            return True
        except Exception as exc:
            if action_id is not None:
                self._store.finish_action(conn, action_id=action_id, status="failed", error_text=str(exc))
            updated = self._store.transition_incident(
                conn,
                incident_id=incident_id,
                status="open",
                summary=f"Engineering action failed: {str(exc)[:200]}",
                resolution_payload={"error": str(exc)},
                action_id=action_id,
                increment_retry=True,
            )
            if int(updated.get("retry_count") or 0) >= int(updated.get("max_retries") or self._settings.incident_max_retries):
                self._store.transition_incident(
                    conn,
                    incident_id=incident_id,
                    status="escalated",
                    summary="Incident escalated after executor failure exhaustion.",
                    resolution_payload={"error": str(exc)},
                    action_id=action_id,
                )
            self._journal.write(
                level="ERROR",
                event_type="engineering.incident.failed",
                message=f"incident {incident_id} repair attempt failed",
                incident_id=incident_id,
                action_id=action_id,
                payload={"error": str(exc)},
                conn=conn,
            )
            return True
