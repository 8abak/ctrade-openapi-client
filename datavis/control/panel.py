from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional

import psycopg2.extras

from datavis.control.panel_state import (
    audit_operator_action,
    load_mission,
    load_panel_settings,
    mission_briefing_payload,
    resolve_engineering_runtime,
    resolve_research_runtime,
    save_mission,
    save_panel_settings,
)
from datavis.control.runtime import ControlRuntime
from datavis.research.guardrails import APPROVED_CANDIDATE_FAMILIES
from datavis.separation import brokerday_bounds, brokerday_for_timestamp


def serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value.hex()
    return value


def serialize_mapping(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: serialize_value(value) for key, value in payload.items()}


def _as_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            text = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _elapsed_seconds(started_at: Any) -> Optional[int]:
    started = _as_utc_datetime(started_at)
    if started is None:
        return None
    delta = datetime.now(timezone.utc) - started
    return max(0, int(delta.total_seconds()))


def _event_sort_key(*timestamps: Any) -> tuple[int, str]:
    for timestamp in timestamps:
        resolved = _as_utc_datetime(timestamp)
        if resolved is not None:
            return (1, resolved.isoformat())
    return (0, "")


def _setup_fingerprint(rule_json: Mapping[str, Any], *, fallback_name: str = "") -> str:
    encoded = json.dumps(dict(rule_json or {}), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    digest = hashlib.sha1(encoded).hexdigest()[:16]
    name = str(fallback_name or (rule_json or {}).get("name") or "setup").lower().replace(" ", "-")
    name = "".join(ch for ch in name if ch.isalnum() or ch in {"-", "_", ":"})[:48] or "setup"
    return f"{name}-{digest}"


def _dominant_bucket(payload: Mapping[str, Any]) -> Optional[str]:
    best_key = None
    best_score = None
    for key, value in dict(payload or {}).items():
        item = dict(value or {})
        score = (
            float(item.get("cleanPrecision") or 0.0),
            int(item.get("signals") or item.get("signalCount") or 0),
        )
        if best_score is None or score > best_score:
            best_score = score
            best_key = str(key)
    return best_key


def _candidate_passed(metrics: Mapping[str, Any]) -> bool:
    return (
        float(metrics.get("cleanPrecision") or 0.0) >= 0.55
        and int(metrics.get("signalCount") or 0) >= 4
        and float(metrics.get("walkForwardRange") or 0.0) <= 0.25
    )


def _compile_predicates(rule_json: Mapping[str, Any]) -> Any:
    predicates = list((rule_json or {}).get("predicates") or [])

    def predicate(features: Mapping[str, Any]) -> bool:
        for item in predicates:
            feature = str(item.get("feature") or "")
            if feature not in features:
                return False
            value = float(features[feature])
            threshold = float(item.get("threshold") or 0.0)
            operator = str(item.get("operator") or ">=")
            if operator == ">=" and value < threshold:
                return False
            if operator == "<=" and value > threshold:
                return False
        return True

    return predicate


def _downsample_rows(rows: List[Dict[str, Any]], *, max_points: int) -> List[Dict[str, Any]]:
    if len(rows) <= max_points:
        return rows
    step = max(1, len(rows) // max_points)
    sampled = [rows[index] for index in range(0, len(rows), step)]
    if sampled[-1]["id"] != rows[-1]["id"]:
        sampled.append(rows[-1])
    return sampled


class ControlPanelService:
    def __init__(self, runtime: ControlRuntime) -> None:
        self._runtime = runtime
        self._settings = runtime.settings

    def health(self, conn: Any) -> Dict[str, Any]:
        overview = self.overview(conn)
        return {
            "ok": True,
            "researchLoopEnabled": overview["research"]["policy"]["enabled"],
            "engineeringLoopEnabled": overview["engineering"]["policy"]["enabled"],
            "activeIncidentId": (overview["engineering"].get("activeIncident") or {}).get("id"),
            "latestRunId": (overview["research"].get("latestRun") or {}).get("id"),
            "latestBrokerday": overview["brokerday"],
            "missionTitle": overview["mission"]["missionTitle"],
        }

    def get_mission(self, conn: Any) -> Dict[str, Any]:
        return load_mission(conn, self._settings.research_settings)

    def update_mission(self, conn: Any, payload: Mapping[str, Any], *, actor: str) -> Dict[str, Any]:
        mission = save_mission(conn, payload, self._settings.research_settings)
        audit_operator_action(
            conn,
            actor=actor,
            action_type="mission.update",
            scope="mission",
            target_id=None,
            payload=dict(payload or {}),
            result=mission,
        )
        return mission

    def get_settings(self, conn: Any) -> Dict[str, Any]:
        return load_panel_settings(conn, self._settings, self._settings.research_settings)

    def update_settings(self, conn: Any, payload: Mapping[str, Any], *, actor: str) -> Dict[str, Any]:
        settings = save_panel_settings(
            conn,
            payload,
            control_settings=self._settings,
            research_settings=self._settings.research_settings,
        )
        audit_operator_action(
            conn,
            actor=actor,
            action_type="settings.update",
            scope="settings",
            target_id=None,
            payload=dict(payload or {}),
            result=settings,
        )
        return settings

    def control_snapshot(self, conn: Any) -> Dict[str, Any]:
        mission = self.get_mission(conn)
        research_policy = resolve_research_runtime(conn, self._settings, self._settings.research_settings)
        engineering_policy = resolve_engineering_runtime(conn, self._settings, self._settings.research_settings)
        research_status = self.research_status(conn)
        current_incident = self.current_incident(conn)
        latest_action = self._latest_engineering_action(conn)
        latest_smokes = self._runtime.store.list_recent_smoketests(conn, limit=5)
        story = self._build_story(
            research_status=research_status,
            current_incident=current_incident,
            latest_action=latest_action,
        )
        return {
            "mission": mission,
            "brokerday": (
                (research_status.get("currentRun") or {}).get("brokerday")
                or (research_status.get("lastCompletedRun") or {}).get("brokerday")
                or (research_status.get("bestCandidate") or {}).get("brokerday")
            ),
            "research": {**research_status, "policy": research_policy},
            "engineering": {
                "state": self._derive_engineering_state(current_incident=current_incident),
                "policy": engineering_policy,
                "activeIncident": current_incident,
                "latestAction": latest_action,
                "latestSmokeTests": [serialize_mapping(item) for item in latest_smokes],
            },
            "story": story,
            "latestIncidentSummary": current_incident.get("summary"),
            "latestEngineeringAction": latest_action,
            "latestCandidateSummary": research_status.get("bestCandidate") or {},
            "latestMetrics": ((research_status.get("bestCandidate") or {}).get("metrics") or {}),
        }

    def overview(self, conn: Any) -> Dict[str, Any]:
        return self.control_snapshot(conn)

    def research_status(self, conn: Any) -> Dict[str, Any]:
        status = self._runtime.research_manager.status(conn)
        control = dict(status.get("value") or {})
        latest_run = self._runtime.research_manager.latest_run(conn)
        latest_errors = self._runtime.research_manager.latest_errors(conn, limit=8)
        queue_counts = self._job_counts(conn)
        last_completed_run = self._latest_run_by_status(conn, status_filter="completed")
        current_run = self._latest_run_by_status(conn, status_filter="running")
        current_job = self._latest_job_by_status(conn, statuses=("running",))
        next_job = self._latest_job_by_status(conn, statuses=("pending",))
        next_proposals = self._latest_mutation_proposals(conn)
        current_incident = self.current_incident(conn)
        best_candidate = self._best_candidate_summary(conn)
        last_completed_result = self._completed_run_result(conn, run=last_completed_run, best_candidate=best_candidate)
        latest_claim = self._latest_journal_event(conn, event_types=("worker.job.claimed",))
        latest_worker_event = self._latest_component_journal(conn, component="worker")
        latest_research_event = self._latest_journal_entry(conn)
        activity = self._research_activity(
            conn,
            status=status,
            control=control,
            queue_counts=queue_counts,
            latest_run=latest_run,
            last_completed_run=last_completed_run,
            current_run=current_run,
            current_job=current_job,
            next_job=next_job,
            current_incident=current_incident,
            latest_claim=latest_claim,
            latest_worker_event=latest_worker_event,
            latest_research_event=latest_research_event,
            next_proposals=next_proposals,
        )
        summary = self._research_summary(activity, best_candidate=best_candidate, last_completed_result=last_completed_result)
        return {
            "state": self._derive_research_state(status, queue_counts=queue_counts, current_run=current_run),
            "status": serialize_mapping(status),
            "control": serialize_mapping(control),
            "latestRun": self._serialize_run_payload(latest_run),
            "lastCompletedRun": self._serialize_run_payload(last_completed_run),
            "currentRun": self._serialize_run_payload(current_run),
            "currentJob": self._serialize_job_payload(current_job),
            "nextJob": self._serialize_job_payload(next_job),
            "latestErrors": [serialize_mapping(item) for item in latest_errors],
            "queueCounts": queue_counts,
            "nextProposals": next_proposals,
            "bestCandidate": best_candidate,
            "lastCompletedResult": last_completed_result,
            "activity": activity,
            "summary": summary,
            "recommendedAction": activity.get("recommendedAction") or "No action required.",
            "recommendedActionKey": activity.get("recommendedActionKey") or "noop",
        }

    def list_research_runs(self, conn: Any, *, limit: int = 20) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json, COALESCE(cc.candidate_count, 0) AS candidate_count
                FROM research.run r
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                LEFT JOIN (
                    SELECT run_id, COUNT(*) AS candidate_count
                    FROM research.candidate_result
                    GROUP BY run_id
                ) cc ON cc.run_id = r.id
                ORDER BY r.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = [dict(row) for row in cur.fetchall()]
        return [self._serialize_run_payload(row) for row in rows]

    def list_research_jobs(self, conn: Any, *, limit: int = 40, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status_filter:
                cur.execute(
                    """
                    SELECT *
                    FROM research.job
                    WHERE status = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (status_filter, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM research.job
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = [dict(row) for row in cur.fetchall()]
        return [self._serialize_job_payload(row) for row in rows]

    def list_incidents(self, conn: Any, *, limit: int = 30, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status_filter:
                cur.execute(
                    """
                    SELECT *
                    FROM research.engineering_incident
                    WHERE status = %s
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (status_filter, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM research.engineering_incident
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            incidents = [dict(row) for row in cur.fetchall()]
        return [self._incident_details(conn, item) for item in incidents]

    def current_incident(self, conn: Any) -> Dict[str, Any]:
        incident = self._runtime.store.get_active_incident(conn)
        if incident:
            return self._incident_details(conn, incident)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.engineering_incident
                ORDER BY id DESC
                LIMIT 1
                """
            )
            latest = dict(cur.fetchone() or {})
        return self._incident_details(conn, latest) if latest else {}

    def _latest_job_by_status(self, conn: Any, *, statuses: Iterable[str]) -> Dict[str, Any]:
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
        return dict(row) if row else {}

    def _latest_component_journal(self, conn: Any, *, component: str) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, component, event_type, message, created_at, job_id, run_id, decision_id, payload
                FROM research.journal
                WHERE component = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (component,),
            )
            row = cur.fetchone()
        return serialize_mapping(dict(row or {}))

    def _latest_journal_entry(self, conn: Any) -> Dict[str, Any]:
        items: List[Dict[str, Any]] = []
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, component, event_type, message, created_at
                FROM research.journal
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row:
                payload = dict(row)
                payload["source"] = "research"
                items.append(payload)
            cur.execute(
                """
                SELECT id, component, event_type, message, created_at
                FROM research.engineering_journal
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row:
                payload = dict(row)
                payload["source"] = "engineering"
                items.append(payload)
            cur.execute(
                """
                SELECT id, action_type, actor, created_at
                FROM research.control_operator_action
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row:
                payload = dict(row)
                items.append(
                    {
                        "id": payload["id"],
                        "component": "panel",
                        "event_type": payload["action_type"],
                        "message": f"{payload['actor']} {payload['action_type']}",
                        "created_at": payload["created_at"],
                        "source": "operator",
                    }
                )
        if not items:
            return {}
        items.sort(key=lambda item: _event_sort_key(item.get("created_at")), reverse=True)
        return serialize_mapping(items[0])

    def _best_candidate_summary(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cr.*, r.id AS run_id, r.brokerday, rs.verdict_hint
                FROM research.candidate_result cr
                JOIN research.run r ON r.id = cr.run_id
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                WHERE r.status = 'completed'
                ORDER BY
                    COALESCE((cr.validation_metrics->>'cleanPrecision')::double precision, 0.0) DESC,
                    COALESCE((cr.validation_metrics->>'entriesPerDay')::double precision, 0.0) DESC,
                    r.id DESC,
                    cr.rank ASC
                LIMIT 1
                """
            )
            row = dict(cur.fetchone() or {})
        if not row:
            return {}
        rule_json = dict(row.get("rule_json") or {})
        return {
            "runId": row.get("run_id"),
            "setupFingerprint": str(row.get("setup_fingerprint") or _setup_fingerprint(rule_json, fallback_name=str(row.get("candidate_name") or ""))),
            "candidateName": row.get("candidate_name"),
            "family": row.get("family"),
            "side": row.get("side"),
            "brokerday": serialize_value(row.get("brokerday")),
            "verdictHint": row.get("verdict_hint"),
            "rule": rule_json,
            "metrics": dict(row.get("validation_metrics") or {}),
        }

    def _completed_run_result(
        self,
        conn: Any,
        *,
        run: Mapping[str, Any],
        best_candidate: Mapping[str, Any],
    ) -> Dict[str, Any]:
        if not run:
            return {}
        run_id = int(run["id"])
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS candidate_count
                FROM research.candidate_result
                WHERE run_id = %s
                """,
                (run_id,),
            )
            candidate_count = int((cur.fetchone() or {}).get("candidate_count") or 0)
            cur.execute(
                """
                SELECT *
                FROM research.candidate_result
                WHERE run_id = %s
                ORDER BY is_selected DESC, rank ASC, id ASC
                LIMIT 1
                """,
                (run_id,),
            )
            selected_row = dict(cur.fetchone() or {})
        selected_rule = dict(selected_row.get("rule_json") or {})
        selected_fingerprint = str(
            selected_row.get("setup_fingerprint") or _setup_fingerprint(selected_rule, fallback_name=str(selected_row.get("candidate_name") or ""))
        ) if selected_row else None
        return {
            "runId": run_id,
            "brokerday": serialize_value(run.get("brokerday")),
            "family": ((run.get("config") or {}).get("candidate_family") if run else None),
            "fingerprint": ((run.get("config") or {}).get("config_fingerprint") if run else None),
            "verdict": run.get("verdict_hint"),
            "headline": run.get("headline"),
            "metrics": dict(run.get("metrics_json") or run.get("metrics") or {}),
            "candidateCount": candidate_count,
            "selectedCandidate": {
                "candidateName": selected_row.get("candidate_name"),
                "family": selected_row.get("family"),
                "side": selected_row.get("side"),
                "setupFingerprint": selected_fingerprint,
            } if selected_row else {},
            "bestImproved": bool(selected_fingerprint and selected_fingerprint == best_candidate.get("setupFingerprint")),
            "finishedAt": serialize_value(run.get("finished_at")),
        }

    @staticmethod
    def _incident_root_cause(incident: Mapping[str, Any]) -> str:
        if not incident:
            return "none"
        resolution = dict(incident.get("resolution") or {})
        latest_error = str(resolution.get("error") or "")
        if latest_error:
            return latest_error[:240]
        actions = list(incident.get("actions") or [])
        if actions and actions[0].get("error_text"):
            return str(actions[0]["error_text"])[:240]
        smoke_tests = list(incident.get("smokeTests") or [])
        failed_smoke = next((item for item in smoke_tests if str(item.get("status") or "") == "failed"), None)
        if failed_smoke:
            return str((failed_smoke.get("result_json") or {}).get("detail") or failed_smoke.get("test_name") or incident.get("summary") or "")[:240]
        return str(incident.get("summary") or "incident details unavailable")[:240]

    @staticmethod
    def _verdict_meaning(verdict: str) -> str:
        mapping = {
            "strong_narrow_regime_found": "The latest completed run found a stable bounded regime worth reviewing carefully.",
            "good_precision_but_too_low_frequency": "Precision looked acceptable, but the setup did not fire often enough to count as robust.",
            "moderate_edge_not_near_target": "There may be signal, but it has not reached the required quality bar yet.",
            "unstable_out_of_sample": "The setup changed too much across the holdout slice to trust it yet.",
            "no_robust_edge_found": "Several bounded directions have been tried without finding a stable same-day edge.",
            "stopped_by_budget_guardrail": "The loop used its bounded iteration budget and stopped safely.",
            "hard_technical_failure": "The loop could not enqueue a safe bounded next step and stopped for safety.",
            "supervisor_instruction_rejected": "A supervisor response was rejected by guardrails, so the loop stopped safely.",
        }
        return mapping.get(verdict, "The latest verdict is recorded, but no plain-English explanation is defined for it yet.")

    def _build_story(
        self,
        *,
        research_status: Mapping[str, Any],
        current_incident: Mapping[str, Any],
        latest_action: Mapping[str, Any],
    ) -> Dict[str, Any]:
        activity = dict(research_status.get("activity") or {})
        summary = dict(research_status.get("summary") or {})
        best_candidate = dict(research_status.get("bestCandidate") or {})
        last_completed = dict(research_status.get("lastCompletedResult") or {})
        queue_depth = int(activity.get("queueDepth") or 0)
        current_run_id = activity.get("currentRunId")
        current_job_id = activity.get("currentJobId")
        current_family = activity.get("currentFamily") or "unknown family"
        current_fingerprint = activity.get("currentFingerprint") or "unknown fingerprint"
        if current_run_id and current_job_id:
            running_now = f"Run {current_run_id} is active on job {current_job_id}, testing {current_family} ({current_fingerprint})."
        elif current_incident and self._derive_engineering_state(current_incident=current_incident) == "repairing":
            running_now = f"Engineering is actively repairing incident #{current_incident.get('id')}."
        elif queue_depth > 0:
            running_now = f"No run is active right now; {queue_depth} job(s) are queued."
        else:
            running_now = "No run is active right now."

        if last_completed:
            what_just_happened = (
                f"Last completed run: Run {last_completed.get('runId')} on {last_completed.get('brokerday') or 'unknown broker day'} "
                f"tested {last_completed.get('family') or 'unknown family'} and returned {last_completed.get('verdict') or 'unknown'}."
            )
        elif current_incident:
            what_just_happened = f"Latest material event: incident #{current_incident.get('id')} was recorded."
        else:
            what_just_happened = "No completed research run has been recorded yet."

        if current_incident and self._derive_engineering_state(current_incident=current_incident) == "escalated":
            blocked_now = f"Blocked by escalated incident #{current_incident.get('id')}: {self._incident_root_cause(current_incident)}"
        else:
            blocked_now = summary.get("currentBlocker") or "none"

        if current_run_id and current_job_id:
            machine_next = "When the active run finishes, the supervisor should review the result and produce bounded next jobs."
        elif queue_depth > 0:
            machine_next = "The worker should claim the next queued job."
        elif research_status.get("nextProposals"):
            next_item = dict((research_status.get("nextProposals") or [])[0] or {})
            machine_next = f"Next bounded proposal is ready: {next_item.get('action') or 'proposal'} -> {next_item.get('family') or 'unknown family'}."
        elif best_candidate:
            machine_next = "No bounded next job is queued; review the best current candidate or seed a new bounded run."
        else:
            machine_next = "The loop is waiting for its next bounded instruction."

        latest_verdict_means = summary.get("latestVerdictMeaning") or self._verdict_meaning(str(summary.get("lastVerdict") or ""))
        recommended_action = summary.get("recommendedAction") or "No action required."
        return {
            "whatJustHappened": what_just_happened,
            "runningNow": running_now,
            "blockedNow": blocked_now,
            "machineNext": machine_next,
            "latestVerdictMeans": latest_verdict_means,
            "recommendedAction": recommended_action,
            "recommendedActionKey": summary.get("recommendedActionKey") or "noop",
            "bestCandidate": (
                f"{best_candidate.get('candidateName') or best_candidate.get('setupFingerprint')} "
                f"({best_candidate.get('family') or 'unknown family'})"
                if best_candidate else "No completed candidate yet"
            ),
            "queueDepth": queue_depth,
            "incidentRootCause": self._incident_root_cause(current_incident),
            "latestEngineeringAction": str(latest_action.get("action_type") or "n/a"),
        }

    def list_journals(
        self,
        conn: Any,
        *,
        component: Optional[str] = None,
        level: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 120,
    ) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            research_sql = """
                SELECT *
                FROM research.journal
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """
            cur.execute(research_sql, (limit,))
            for row in cur.fetchall():
                payload = dict(row)
                payload["source"] = "research"
                items.append(payload)
            cur.execute(
                """
                SELECT *
                FROM research.engineering_journal
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            for row in cur.fetchall():
                payload = dict(row)
                payload["source"] = "engineering"
                items.append(payload)
            cur.execute(
                """
                SELECT *
                FROM research.control_operator_action
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            for row in cur.fetchall():
                action = dict(row)
                items.append(
                    {
                        "id": action["id"],
                        "component": "panel",
                        "level": "INFO",
                        "event_type": action["action_type"],
                        "message": f"{action['actor']} {action['action_type']}",
                        "payload": {
                            "actor": action["actor"],
                            "scope": action["scope"],
                            "targetId": action["target_id"],
                            "payload": action["payload"],
                            "result": action["result"],
                        },
                        "created_at": action["created_at"],
                        "source": "operator",
                    }
                )
        filtered = []
        for item in items:
            if component and str(item.get("component") or "") != component:
                continue
            if level and str(item.get("level") or "").upper() != str(level).upper():
                continue
            if event_type and str(item.get("event_type") or "") != event_type:
                continue
            filtered.append(item)
        filtered.sort(key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)), reverse=True)
        return [serialize_mapping(row) for row in filtered[:limit]]

    def list_candidates(
        self,
        conn: Any,
        *,
        brokerday: Optional[str] = None,
        side: Optional[str] = None,
        family: Optional[str] = None,
        status_filter: Optional[str] = None,
        spread_regime: Optional[str] = None,
        session_bucket: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    cr.id,
                    cr.run_id,
                    cr.rank,
                    cr.candidate_name,
                    cr.family,
                    cr.side,
                    cr.is_selected,
                    cr.rule_json,
                    cr.train_metrics,
                    cr.validation_metrics,
                    cr.setup_fingerprint,
                    r.brokerday,
                    r.iteration,
                    rs.verdict_hint
                FROM research.candidate_result cr
                JOIN research.run r ON r.id = cr.run_id
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                ORDER BY cr.id DESC
                LIMIT %s
                """,
                (max(50, limit * 6),),
            )
            rows = [dict(row) for row in cur.fetchall()]
            cur.execute("SELECT * FROM research.candidate_library")
            overrides = {
                str(row["setup_fingerprint"]): dict(row)
                for row in cur.fetchall()
            }
        aggregate: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            rule_json = dict(row.get("rule_json") or {})
            validation = dict(row.get("validation_metrics") or {})
            fingerprint = str(row.get("setup_fingerprint") or _setup_fingerprint(rule_json, fallback_name=str(row.get("candidate_name") or "")))
            item = aggregate.get(fingerprint)
            if item is None:
                override = overrides.get(fingerprint) or {}
                item = {
                    "setupFingerprint": fingerprint,
                    "candidateName": row.get("candidate_name"),
                    "family": row.get("family"),
                    "side": row.get("side"),
                    "rule": rule_json,
                    "status": override.get("status") or ("promoted" if bool(override.get("promoted")) else "active"),
                    "operatorNotes": override.get("operator_notes") or "",
                    "latestRunId": row.get("run_id"),
                    "latestBrokerday": serialize_value(row.get("brokerday")),
                    "latestVerdictHint": row.get("verdict_hint"),
                    "trainMetrics": dict(row.get("train_metrics") or {}),
                    "validationMetrics": validation,
                    "entriesPerDay": float(validation.get("entriesPerDay") or 0.0),
                    "daysSeen": set(),
                    "daysPassed": set(),
                    "daysFailed": set(),
                    "runsSeen": 0,
                    "sessionBucket": _dominant_bucket(validation.get("bySession") or {}),
                    "spreadRegime": _dominant_bucket(validation.get("bySpread") or {}),
                }
                aggregate[fingerprint] = item
            brokerday_value = serialize_value(row.get("brokerday"))
            if brokerday_value:
                item["daysSeen"].add(brokerday_value)
                if _candidate_passed(validation):
                    item["daysPassed"].add(brokerday_value)
                else:
                    item["daysFailed"].add(brokerday_value)
            item["runsSeen"] += 1
            if int(row.get("run_id") or 0) >= int(item.get("latestRunId") or 0):
                item["latestRunId"] = row.get("run_id")
                item["latestBrokerday"] = serialize_value(row.get("brokerday"))
                item["latestVerdictHint"] = row.get("verdict_hint")
                item["trainMetrics"] = dict(row.get("train_metrics") or {})
                item["validationMetrics"] = validation
                item["entriesPerDay"] = float(validation.get("entriesPerDay") or 0.0)
                item["sessionBucket"] = _dominant_bucket(validation.get("bySession") or {})
                item["spreadRegime"] = _dominant_bucket(validation.get("bySpread") or {})
        candidates = []
        for item in aggregate.values():
            payload = dict(item)
            payload["daysSeen"] = len(payload["daysSeen"])
            payload["daysPassed"] = len(payload["daysPassed"])
            payload["daysFailed"] = len(payload["daysFailed"])
            candidates.append(payload)
        filtered = []
        for item in candidates:
            if brokerday and str(item.get("latestBrokerday") or "") != brokerday:
                continue
            if side and str(item.get("side") or "") != side:
                continue
            if family and str(item.get("family") or "") != family:
                continue
            if status_filter and str(item.get("status") or "") != status_filter:
                continue
            if spread_regime and str(item.get("spreadRegime") or "") != spread_regime:
                continue
            if session_bucket and str(item.get("sessionBucket") or "") != session_bucket:
                continue
            filtered.append(item)
        filtered.sort(
            key=lambda item: (
                float((item.get("validationMetrics") or {}).get("cleanPrecision") or 0.0),
                float(item.get("entriesPerDay") or 0.0),
                int(item.get("daysPassed") or 0),
            ),
            reverse=True,
        )
        return filtered[:limit]

    def update_candidate_library(
        self,
        conn: Any,
        *,
        fingerprint: str,
        status: str,
        operator_notes: str,
        actor: str,
    ) -> Dict[str, Any]:
        normalized_status = status if status in {"promoted", "active", "archived"} else "active"
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO research.candidate_library (
                    setup_fingerprint, status, promoted, operator_notes, updated_by
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (setup_fingerprint)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    promoted = EXCLUDED.promoted,
                    operator_notes = EXCLUDED.operator_notes,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = NOW()
                RETURNING *
                """,
                (fingerprint, normalized_status, normalized_status == "promoted", operator_notes[:4000], actor[:128]),
            )
            row = dict(cur.fetchone() or {})
        audit_operator_action(
            conn,
            actor=actor,
            action_type="candidate.update",
            scope="candidate",
            target_id=fingerprint,
            payload={"status": normalized_status, "operatorNotes": operator_notes},
            result=row,
        )
        return serialize_mapping(row)

    def day_review(
        self,
        conn: Any,
        *,
        brokerday_text: Optional[str],
        run_id: Optional[int],
        setup_fingerprint: Optional[str],
    ) -> Dict[str, Any]:
        run = self._resolve_day_review_run(conn, brokerday_text=brokerday_text, run_id=run_id)
        if not run:
            return {"brokerday": brokerday_text, "run": None, "candidates": [], "entries": [], "chart": {"ticks": [], "markers": []}}
        review_candidates = self._load_day_review_candidates(conn, run_id=int(run["id"]), setup_fingerprint=setup_fingerprint)
        features = self._load_feature_snapshots(conn, run_id=int(run["id"]))
        labels = self._load_entry_labels(conn, run_id=int(run["id"]))
        entries = []
        markers = []
        for candidate in review_candidates:
            predicate = _compile_predicates(candidate["rule"])
            side = str(candidate["side"] or "both")
            for tick_id, feature_row in features.items():
                if side not in labels.get(tick_id, {}):
                    continue
                if not predicate(feature_row["features"]):
                    continue
                label = labels[tick_id][side]
                entry = {
                    "tickId": tick_id,
                    "timestamp": feature_row["timestamp"],
                    "side": side,
                    "spread": label["spread"],
                    "targetHit": label["targetHit"],
                    "hitSeconds": label["hitSeconds"],
                    "maxAdverse": label["maxAdverse"],
                    "maxFavorable": label["maxFavorable"],
                    "candidate": candidate["candidateName"],
                    "setupFingerprint": candidate["setupFingerprint"],
                    "sessionBucket": feature_row["sessionBucket"],
                }
                entries.append(entry)
                markers.append(
                    {
                        "tickId": tick_id,
                        "timestamp": feature_row["timestamp"],
                        "price": label["entryPrice"],
                        "side": side,
                        "targetHit": label["targetHit"],
                        "candidate": candidate["candidateName"],
                    }
                )
        entries.sort(key=lambda item: item["tickId"])
        ticks = self._load_chart_ticks(conn, brokerday=str(run["brokerday"]))
        return {
            "brokerday": serialize_value(run["brokerday"]),
            "run": self._serialize_run_payload(run),
            "candidates": review_candidates,
            "entries": entries[:500],
            "chart": {
                "ticks": ticks,
                "markers": markers[:500],
            },
        }

    def record_action(
        self,
        conn: Any,
        *,
        actor: str,
        action_type: str,
        scope: str,
        target_id: Optional[str],
        payload: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> None:
        audit_operator_action(
            conn,
            actor=actor,
            action_type=action_type,
            scope=scope,
            target_id=target_id,
            payload=payload,
            result=result,
        )

    def _serialize_run_payload(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        payload = serialize_mapping(dict(row or {}))
        metrics = dict((row or {}).get("metrics_json") or (row or {}).get("metrics") or {})
        payload["metrics"] = metrics
        payload["config"] = dict((row or {}).get("config") or {})
        payload["candidateCount"] = int((row or {}).get("candidate_count") or payload.get("candidate_count") or 0)
        payload["elapsedSeconds"] = _elapsed_seconds((row or {}).get("started_at"))
        if not payload.get("brokerday"):
            brokerday_value = self._lookup_run_brokerday(row)
            if brokerday_value:
                payload["brokerday"] = brokerday_value
        return payload

    def _serialize_job_payload(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        payload = serialize_mapping(dict(row or {}))
        config = dict((row or {}).get("config") or {})
        guardrails = dict((row or {}).get("guardrails") or {})
        payload["config"] = config
        payload["guardrails"] = guardrails
        payload["proposal"] = self._job_proposal_payload(config=config, guardrails=guardrails)
        payload["elapsedSeconds"] = _elapsed_seconds((row or {}).get("started_at"))
        payload["sourceRunId"] = config.get("source_run_id")
        payload["sourceDecisionId"] = (row or {}).get("parent_decision_id")
        payload["sourceJobId"] = (row or {}).get("parent_job_id")
        return payload

    def _lookup_run_brokerday(self, row: Mapping[str, Any]) -> Optional[str]:
        brokerday_value = row.get("brokerday")
        if brokerday_value:
            return serialize_value(brokerday_value)
        return None

    def _derive_research_state(
        self,
        status: Mapping[str, Any],
        *,
        queue_counts: Mapping[str, int],
        current_run: Mapping[str, Any] | None = None,
    ) -> str:
        value = dict(status.get("value") or {})
        if value.get("paused"):
            return "paused"
        if value.get("final_verdict"):
            return "stopped"
        if queue_counts.get("failed", 0) > 0:
            return "degraded"
        latest_run = dict(current_run or status.get("latest_run") or {})
        if str(latest_run.get("status") or "") == "running":
            return "running"
        return "idle"

    def _derive_engineering_state(self, *, current_incident: Mapping[str, Any]) -> str:
        if not current_incident:
            return "idle"
        status = str(current_incident.get("status") or "")
        mapping = {
            "open": "investigating",
            "analyzing": "investigating",
            "executing": "repairing",
            "validating": "repairing",
            "escalated": "escalated",
            "resolved": "idle",
        }
        return mapping.get(status, status or "idle")

    def _job_counts(self, conn: Any) -> Dict[str, int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM research.job
                GROUP BY status
                """
            )
            rows = {str(row[0]): int(row[1]) for row in cur.fetchall()}
        return {
            "pending": int(rows.get("pending") or 0),
            "running": int(rows.get("running") or 0),
            "failed": int(rows.get("failed") or 0),
            "completed": int(rows.get("completed") or 0),
        }

    def _latest_mutation_proposals(self, conn: Any) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT briefing
                FROM research.decision
                WHERE briefing IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """
            )
            decision_row = dict(cur.fetchone() or {})
            decision_briefing = dict(decision_row.get("briefing") or {})
            if decision_briefing.get("proposedNextJobs"):
                return self._serialize_proposals(decision_briefing.get("proposedNextJobs") or [], source="decision-briefing")
            cur.execute(
                """
                SELECT rs.briefing_json
                FROM research.runsummary rs
                JOIN research.run r ON r.id = rs.run_id
                ORDER BY r.id DESC
                LIMIT 1
                """
            )
            row = dict(cur.fetchone() or {})
        briefing = dict(row.get("briefing_json") or {})
        return self._serialize_proposals(briefing.get("mutationProposals") or [], source="runsummary-mutation")

    def _latest_candidate_summary(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cr.*, r.brokerday, rs.verdict_hint
                FROM research.candidate_result cr
                JOIN research.run r ON r.id = cr.run_id
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                WHERE cr.is_selected = TRUE
                ORDER BY cr.id DESC
                LIMIT 1
                """
            )
            row = dict(cur.fetchone() or {})
        if not row:
            return {}
        rule_json = dict(row.get("rule_json") or {})
        return {
            "setupFingerprint": str(row.get("setup_fingerprint") or _setup_fingerprint(rule_json, fallback_name=str(row.get("candidate_name") or ""))),
            "candidateName": row.get("candidate_name"),
            "family": row.get("family"),
            "side": row.get("side"),
            "brokerday": serialize_value(row.get("brokerday")),
            "verdictHint": row.get("verdict_hint"),
            "rule": rule_json,
            "metrics": dict(row.get("validation_metrics") or {}),
        }

    def _latest_engineering_action(self, conn: Any) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.engineering_action
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = dict(cur.fetchone() or {})
        return serialize_mapping(row)

    def _latest_run_by_status(self, conn: Any, *, status_filter: str) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json
                FROM research.run r
                LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                WHERE r.status = %s
                ORDER BY r.id DESC
                LIMIT 1
                """,
                (status_filter,),
            )
            row = cur.fetchone()
        return dict(row) if row else {}

    def _latest_journal_event(self, conn: Any, *, event_types: Iterable[str]) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, component, event_type, message, created_at, job_id, run_id, payload
                FROM research.journal
                WHERE event_type = ANY(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (list(event_types),),
            )
            row = cur.fetchone()
        return serialize_mapping(dict(row or {}))

    def _research_activity(
        self,
        conn: Any,
        *,
        status: Mapping[str, Any],
        control: Mapping[str, Any],
        queue_counts: Mapping[str, int],
        latest_run: Mapping[str, Any],
        last_completed_run: Mapping[str, Any],
        current_run: Mapping[str, Any],
        current_job: Mapping[str, Any],
        next_job: Mapping[str, Any],
        current_incident: Mapping[str, Any],
        latest_claim: Mapping[str, Any],
        latest_worker_event: Mapping[str, Any],
        latest_research_event: Mapping[str, Any],
        next_proposals: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        services = list(status.get("services") or [])
        worker_service = self._service_snapshot(services, self._settings.research_settings.worker_name)
        orchestrator_service = self._service_snapshot(services, self._settings.research_settings.orchestrator_name)
        supervisor_service = self._service_snapshot(services, self._settings.research_settings.supervisor_name)
        latest_orchestrator_event = self._latest_journal_event(
            conn,
            event_types=("orchestrator.seeded", "orchestrator.decision.queued", "orchestrator.decision.applied"),
        )
        latest_supervisor_event = self._latest_component_journal(conn, component="supervisor")
        worker_consuming = bool(current_job)
        engineering_state = self._derive_engineering_state(current_incident=current_incident)
        latest_decision = dict(status.get("latest_decision") or {})
        current_config = dict(current_job.get("config") or current_run.get("config") or {})
        current_proposal = self._job_proposal_payload(config=current_config, guardrails=dict(current_job.get("guardrails") or {}))
        incident_root_cause = self._incident_root_cause(current_incident)

        if current_job:
            current_phase = "worker"
            current_step = "executing entry research"
        elif str(latest_decision.get("status") or "") == "running":
            current_phase = "supervisor"
            current_step = "reviewing the latest completed run"
        elif str(latest_decision.get("status") or "") == "pending":
            current_phase = "supervisor"
            current_step = "awaiting supervisor pickup"
        elif next_job:
            current_phase = "queue"
            current_step = "awaiting worker claim"
        elif control.get("final_verdict"):
            current_phase = "stopped"
            current_step = f"stopped after {control.get('final_verdict')}"
        else:
            current_phase = "idle"
            current_step = "waiting for the next bounded job"

        blocker = "none"
        action = "No action required."
        action_key = "noop"
        if engineering_state == "escalated":
            blocker = f"Engineering incident #{current_incident.get('id')} is escalated: {incident_root_cause}"
            action = "Investigate escalated incident"
            action_key = "investigate_incident"
        elif current_job:
            blocker = "none"
            action = "No action required while the active run is executing."
            action_key = "noop"
        elif control.get("paused"):
            blocker = "Research is paused."
            action = "Resume Research"
            action_key = "resume_research"
        elif control.get("final_verdict") and queue_counts.get("pending", 0) > 0:
            blocker = f"Queued jobs exist, but research is stopped by final verdict {control.get('final_verdict')}."
            action = "Resume Research"
            action_key = "resume_research"
        elif control.get("final_verdict"):
            blocker = f"Research is stopped after final verdict {control.get('final_verdict')}."
            action = "Resume Research if you want the loop to continue past that verdict."
            action_key = "resume_research"
        elif queue_counts.get("pending", 0) > 0 and not self._service_is_running(worker_service):
            blocker = "Queued jobs exist, but the worker service is not running."
            action = "Restart worker claim path"
            action_key = "restart_worker"
        elif queue_counts.get("pending", 0) > 0 and not worker_consuming:
            blocker = "Queued jobs are not being claimed."
            action = "Inspect worker claim path"
            action_key = "inspect_worker"
        elif not current_job and not next_job and next_proposals:
            blocker = "No runnable job is queued yet."
            action = "Seed Next Job"
            action_key = "seed_next_job"
        elif not current_job and not next_job and control.get("seeded") and not next_proposals:
            blocker = "The loop is idle with no queued bounded work."
            action = "Review the latest verdict or seed a new bounded job."
            action_key = "review_or_seed"

        return {
            "loopState": self._derive_research_state(status, queue_counts=queue_counts, current_run=current_run),
            "lastCompletedRunId": last_completed_run.get("id"),
            "lastCompletedBrokerday": serialize_value(last_completed_run.get("brokerday")),
            "lastFamilyTried": ((last_completed_run.get("config") or {}).get("candidate_family") if last_completed_run else None),
            "lastVerdict": last_completed_run.get("verdict_hint"),
            "lastCompletedAt": serialize_value(last_completed_run.get("finished_at")),
            "currentRunId": current_run.get("id") or (latest_run.get("id") if str(latest_run.get("status") or "") == "running" else None),
            "currentJobId": current_job.get("id"),
            "currentBrokerday": serialize_value(current_run.get("brokerday")),
            "currentFamily": current_config.get("candidate_family"),
            "currentFingerprint": current_config.get("config_fingerprint"),
            "currentProposalKind": current_proposal.get("proposalKind"),
            "currentProposalSource": current_proposal.get("proposalSource"),
            "currentMutationNote": current_proposal.get("mutationNote"),
            "currentSourceRunId": current_config.get("source_run_id"),
            "currentSourceDecisionId": current_job.get("parent_decision_id"),
            "currentSourceJobId": current_job.get("parent_job_id"),
            "currentPhase": current_phase,
            "currentStep": current_step,
            "queueDepth": int(queue_counts.get("pending", 0) or 0) + int(queue_counts.get("running", 0) or 0),
            "pendingJobs": int(queue_counts.get("pending", 0) or 0),
            "runningJobs": int(queue_counts.get("running", 0) or 0),
            "failedJobs": int(queue_counts.get("failed", 0) or 0),
            "completedJobs": int(queue_counts.get("completed", 0) or 0),
            "workerLastClaimedAt": latest_claim.get("created_at"),
            "workerLastClaimedJobId": latest_claim.get("job_id"),
            "workerActivelyConsuming": worker_consuming,
            "workerServiceState": self._service_state_text(worker_service),
            "workerHeartbeatAt": serialize_value(current_job.get("last_heartbeat_at") or latest_worker_event.get("created_at")),
            "workerLastEventAt": latest_worker_event.get("created_at"),
            "workerLastEventType": latest_worker_event.get("event_type"),
            "workerLastEventMessage": latest_worker_event.get("message"),
            "orchestratorActive": self._service_is_running(orchestrator_service),
            "orchestratorServiceState": self._service_state_text(orchestrator_service),
            "orchestratorLastEventAt": latest_orchestrator_event.get("created_at"),
            "supervisorServiceState": self._service_state_text(supervisor_service),
            "supervisorLastEventAt": latest_supervisor_event.get("created_at"),
            "researchFinalVerdict": control.get("final_verdict"),
            "researchPaused": bool(control.get("paused")),
            "stopRequested": bool(control.get("stop_requested")),
            "engineeringState": engineering_state,
            "engineeringBlocking": engineering_state == "escalated",
            "activeRunElapsedSeconds": _elapsed_seconds(current_run.get("started_at")),
            "latestEventAt": latest_research_event.get("created_at"),
            "latestEventSource": latest_research_event.get("component"),
            "latestEventType": latest_research_event.get("event_type"),
            "latestEventMessage": latest_research_event.get("message"),
            "currentBlocker": blocker,
            "recommendedAction": action,
            "recommendedActionKey": action_key,
        }

    def _research_summary(
        self,
        activity: Mapping[str, Any],
        *,
        best_candidate: Mapping[str, Any],
        last_completed_result: Mapping[str, Any],
    ) -> Dict[str, Any]:
        run_id = activity.get("lastCompletedRunId")
        brokerday = activity.get("lastCompletedBrokerday")
        run_label = f"Run {run_id}" if run_id else "No completed run yet"
        if run_id and brokerday:
            run_label = f"Run {run_id} on {brokerday}"
        best_label = "No completed candidate yet"
        if best_candidate:
            best_label = f"{best_candidate.get('candidateName') or best_candidate.get('setupFingerprint')} ({best_candidate.get('family') or 'unknown family'})"
        return {
            "lastCompletedRun": run_label,
            "lastFamilyTried": activity.get("lastFamilyTried") or "n/a",
            "lastVerdict": activity.get("lastVerdict") or "n/a",
            "pendingJobs": activity.get("pendingJobs") or 0,
            "queueDepth": activity.get("queueDepth") or 0,
            "workerLastClaimedAt": activity.get("workerLastClaimedAt") or None,
            "lastCompletedAt": activity.get("lastCompletedAt") or None,
            "bestCandidate": best_label,
            "latestVerdictMeaning": self._verdict_meaning(str(activity.get("lastVerdict") or "")),
            "currentBlocker": activity.get("currentBlocker") or "none",
            "recommendedAction": activity.get("recommendedAction") or "No action required.",
            "recommendedActionKey": activity.get("recommendedActionKey") or "noop",
            "currentRunId": activity.get("currentRunId"),
            "currentJobId": activity.get("currentJobId"),
            "currentPhase": activity.get("currentPhase") or "idle",
            "lastCompletedResult": dict(last_completed_result or {}),
        }

    @staticmethod
    def _service_snapshot(services: Iterable[Mapping[str, Any]], name: str) -> Dict[str, Any]:
        for service in services:
            if str(service.get("name") or "") == name:
                return dict(service)
        return {}

    @staticmethod
    def _service_is_running(service: Mapping[str, Any]) -> bool:
        return str(service.get("active_state") or "") == "active" and str(service.get("sub_state") or "") == "running"

    @staticmethod
    def _service_state_text(service: Mapping[str, Any]) -> str:
        if not service:
            return "unknown"
        active = str(service.get("active_state") or "unknown")
        sub = str(service.get("sub_state") or "unknown")
        return f"{active}/{sub}"

    def _serialize_proposals(self, proposals: Iterable[Mapping[str, Any]], *, source: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for proposal in proposals:
            payload = dict(proposal or {})
            params = dict(payload.get("parameters") or {})
            seed_rule = dict(params.get("seed_rule") or {})
            seed_rule_ref = None
            if seed_rule:
                seed_rule_ref = " / ".join(
                    [
                        str(seed_rule.get("name") or "seed"),
                        str(seed_rule.get("family") or "family"),
                        str(seed_rule.get("side") or "side"),
                    ]
                )
            items.append(
                {
                    "action": payload.get("action") or "proposal",
                    "reason": payload.get("reason") or "",
                    "configFingerprint": payload.get("configFingerprint") or params.get("config_fingerprint"),
                    "family": params.get("candidate_family"),
                    "proposalSource": source,
                    "sourceRunId": params.get("source_run_id"),
                    "mutationNote": params.get("mutation_note"),
                    "seedRuleRef": seed_rule_ref,
                    "mutatedFields": list(payload.get("mutatedFields") or []),
                    "source": source,
                    "parameters": params,
                }
            )
        return items

    @staticmethod
    def _job_proposal_payload(*, config: Mapping[str, Any], guardrails: Mapping[str, Any]) -> Dict[str, Any]:
        seed_rule = dict(config.get("seed_rule") or {})
        seed_rule_ref = None
        if seed_rule:
            seed_rule_ref = " / ".join(
                [
                    str(seed_rule.get("name") or "seed"),
                    str(seed_rule.get("family") or "family"),
                    str(seed_rule.get("side") or "side"),
                ]
            )
        action = str(guardrails.get("action") or "")
        derived = bool(config.get("source_run_id") or config.get("mutation_note") or seed_rule or action not in {"", "seed_next_job"})
        return {
            "proposalKind": action or "seed_next_job",
            "proposalSource": guardrails.get("proposalSource"),
            "family": config.get("candidate_family"),
            "fingerprint": config.get("config_fingerprint"),
            "seedRuleRef": seed_rule_ref,
            "mutationNote": config.get("mutation_note") or guardrails.get("reason"),
            "mutatedFields": list(guardrails.get("mutatedFields") or []),
            "proposalDerived": derived,
            "derivedFromRunId": config.get("source_run_id"),
            "derivedFromDecisionId": guardrails.get("sourceDecisionId"),
            "seedRule": seed_rule,
        }

    def _incident_details(self, conn: Any, incident: Mapping[str, Any]) -> Dict[str, Any]:
        if not incident:
            return {}
        incident_id = int(incident["id"])
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.engineering_action
                WHERE incident_id = %s
                ORDER BY id DESC
                LIMIT 10
                """,
                (incident_id,),
            )
            actions = [serialize_mapping(dict(row)) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT *
                FROM research.engineering_patch
                WHERE incident_id = %s
                ORDER BY id DESC
                LIMIT 10
                """,
                (incident_id,),
            )
            patches = [serialize_mapping(dict(row)) for row in cur.fetchall()]
        smoketests = [serialize_mapping(row) for row in self._runtime.store.list_recent_smoketests(conn, incident_id=incident_id, limit=10)]
        payload = serialize_mapping(dict(incident))
        payload["actions"] = actions
        payload["patches"] = patches
        payload["smokeTests"] = smoketests
        payload["filesTouched"] = [
            target
            for patch in patches
            for target in list(patch.get("target_files") or [])
        ]
        payload["rootCause"] = self._incident_root_cause(payload)
        return payload

    def _resolve_day_review_run(self, conn: Any, *, brokerday_text: Optional[str], run_id: Optional[int]) -> Dict[str, Any]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if run_id is not None:
                cur.execute(
                    """
                    SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json
                    FROM research.run r
                    LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                    WHERE r.id = %s
                    LIMIT 1
                    """,
                    (run_id,),
                )
            elif brokerday_text:
                cur.execute(
                    """
                    SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json
                    FROM research.run r
                    LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                    WHERE r.brokerday = %s
                    ORDER BY r.id DESC
                    LIMIT 1
                    """,
                    (brokerday_text,),
                )
            else:
                cur.execute(
                    """
                    SELECT r.*, rs.verdict_hint, rs.headline, rs.metrics_json
                    FROM research.run r
                    LEFT JOIN research.runsummary rs ON rs.run_id = r.id
                    WHERE r.status = 'completed'
                    ORDER BY r.id DESC
                    LIMIT 1
                    """
                )
            return dict(cur.fetchone() or {})

    def _load_day_review_candidates(self, conn: Any, *, run_id: int, setup_fingerprint: Optional[str]) -> List[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM research.candidate_result
                WHERE run_id = %s
                ORDER BY rank ASC, id ASC
                LIMIT 8
                """,
                (run_id,),
            )
            rows = [dict(row) for row in cur.fetchall()]
        candidates = []
        for row in rows:
            rule_json = dict(row.get("rule_json") or {})
            fingerprint = str(row.get("setup_fingerprint") or _setup_fingerprint(rule_json, fallback_name=str(row.get("candidate_name") or "")))
            if setup_fingerprint and fingerprint != setup_fingerprint:
                continue
            candidates.append(
                {
                    "setupFingerprint": fingerprint,
                    "candidateName": row.get("candidate_name"),
                    "side": row.get("side"),
                    "family": row.get("family"),
                    "rule": rule_json,
                    "validationMetrics": dict(row.get("validation_metrics") or {}),
                }
            )
        if setup_fingerprint:
            return candidates[:1]
        return candidates[:3]

    def _load_feature_snapshots(self, conn: Any, *, run_id: int) -> Dict[int, Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT tick_id, tick_timestamp, session_bucket, feature_json
                FROM research.feature_snapshot
                WHERE run_id = %s
                ORDER BY tick_id ASC
                """,
                (run_id,),
            )
            rows = [dict(row) for row in cur.fetchall()]
        payload = {}
        for row in rows:
            payload[int(row["tick_id"])] = {
                "timestamp": serialize_value(row["tick_timestamp"]),
                "sessionBucket": row["session_bucket"],
                "features": dict(row.get("feature_json") or {}),
            }
        return payload

    def _load_entry_labels(self, conn: Any, *, run_id: int) -> Dict[int, Dict[str, Dict[str, Any]]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT tick_id, side, entry_price, spread_at_entry, hit_2x, hit_seconds, max_adverse, max_favorable
                FROM research.entry_label
                WHERE run_id = %s
                ORDER BY tick_id ASC
                """,
                (run_id,),
            )
            rows = [dict(row) for row in cur.fetchall()]
        payload: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            tick_id = int(row["tick_id"])
            payload.setdefault(tick_id, {})[str(row["side"])] = {
                "entryPrice": float(row["entry_price"]),
                "spread": float(row["spread_at_entry"]),
                "targetHit": bool(row["hit_2x"]),
                "hitSeconds": float(row["hit_seconds"]) if row.get("hit_seconds") is not None else None,
                "maxAdverse": float(row["max_adverse"]),
                "maxFavorable": float(row["max_favorable"]),
            }
        return payload

    def _load_chart_ticks(self, conn: Any, *, brokerday: str) -> List[Dict[str, Any]]:
        day_value = date.fromisoformat(brokerday)
        start_ts, end_ts = brokerday_bounds(day_value)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, timestamp, bid, ask, mid, spread
                FROM public.ticks
                WHERE symbol = %s
                  AND timestamp >= %s
                  AND timestamp < %s
                ORDER BY id ASC
                """,
                (self._settings.research_settings.symbol, start_ts, end_ts),
            )
            rows = [dict(row) for row in cur.fetchall()]
        serialized = [
            {
                "id": int(row["id"]),
                "timestamp": serialize_value(row["timestamp"]),
                "mid": float(row["mid"] if row.get("mid") is not None else (float(row["bid"]) + float(row["ask"])) / 2.0),
                "spread": float(row["spread"] if row.get("spread") is not None else max(0.0, float(row["ask"]) - float(row["bid"]))),
            }
            for row in rows
        ]
        return _downsample_rows(serialized, max_points=1400)
