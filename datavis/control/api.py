from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

from datavis.control.db import connection
from datavis.control.models import EngineeringSupervisorDecision, IncidentCandidate, PatchTemplateName
from datavis.control.runtime import get_control_runtime


class RestartRequest(BaseModel):
    services: List[str] = Field(default_factory=list, max_length=8)


class ResetRequest(BaseModel):
    mode: str = Field("soft", pattern="^(soft|hard)$")
    reason: str = Field("manual control reset", min_length=1, max_length=512)


class RequeueRequest(BaseModel):
    jobId: Optional[int] = Field(None, ge=1)
    reason: str = Field("manual control requeue", min_length=1, max_length=512)


class PauseRequest(BaseModel):
    reason: str = Field("manual control pause", min_length=1, max_length=512)


class SmokeRequest(BaseModel):
    tests: List[str] = Field(default_factory=list, max_length=8)


class PatchRequest(BaseModel):
    patchTemplate: PatchTemplateName
    restartServices: List[str] = Field(default_factory=list, max_length=8)


RUNTIME = get_control_runtime()
SETTINGS = RUNTIME.settings
SERVICE_MANAGER = RUNTIME.service_manager
STORE = RUNTIME.store
RESEARCH_MANAGER = RUNTIME.research_manager
SUPERVISOR = RUNTIME.supervisor
EXECUTOR = RUNTIME.executor
SMOKE_RUNNER = RUNTIME.smoke_runner
DETECTOR = RUNTIME.detector
ORCHESTRATOR = RUNTIME.orchestrator

app = FastAPI(title="datavis research control", version="1.0.0")


def require_control_auth(x_control_token: Optional[str] = Header(None)) -> None:
    if not SETTINGS.api_token:
        return
    if x_control_token == SETTINGS.api_token:
        return
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid control token")


@app.get("/control/health", dependencies=[Depends(require_control_auth)])
def control_health() -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": True,
        "apiHost": SETTINGS.api_host,
        "apiPort": SETTINGS.api_port,
        "loopEnabled": SETTINGS.enable_loop,
        "serviceProbeSupported": SERVICE_MANAGER.is_supported(),
    }
    try:
        with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.health") as conn:
            payload["db"] = "ok"
            payload["engineeringState"] = STORE.ensure_state(conn)
            payload["activeIncident"] = STORE.get_active_incident(conn)
    except Exception as exc:
        payload["ok"] = False
        payload["db"] = "error"
        payload["error"] = str(exc)
    return payload


@app.get("/control/research/status", dependencies=[Depends(require_control_auth)])
def research_status() -> Dict[str, Any]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.status") as conn:
        return RESEARCH_MANAGER.status(conn)


@app.get("/control/research/latest-run", dependencies=[Depends(require_control_auth)])
def research_latest_run() -> Dict[str, Any]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.latest_run") as conn:
        return RESEARCH_MANAGER.latest_run(conn)


@app.get("/control/research/latest-errors", dependencies=[Depends(require_control_auth)])
def research_latest_errors(limit: int = Query(20, ge=1, le=100)) -> List[Dict[str, Any]]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.latest_errors") as conn:
        return RESEARCH_MANAGER.latest_errors(conn, limit=limit)


@app.get("/control/research/journals", dependencies=[Depends(require_control_auth)])
def research_journals(component: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=200)) -> List[Dict[str, Any]]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.journals") as conn:
        return RESEARCH_MANAGER.recent_journals(conn, component=component, limit=limit)


@app.post("/control/research/restart", dependencies=[Depends(require_control_auth)])
def research_restart(payload: RestartRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.restart") as conn:
        snapshots = RESEARCH_MANAGER.restart_services(payload.services or list(SETTINGS.research_services))
        conn.commit()
        return {"services": snapshots}


@app.post("/control/research/reset", dependencies=[Depends(require_control_auth)])
def research_reset(payload: ResetRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.reset") as conn:
        result = RESEARCH_MANAGER.reset(conn, mode=payload.mode, reason=payload.reason)
        conn.commit()
        return {"state": result}


@app.post("/control/research/requeue", dependencies=[Depends(require_control_auth)])
def research_requeue(payload: RequeueRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.requeue") as conn:
        result = RESEARCH_MANAGER.requeue(conn, job_id=payload.jobId, reason=payload.reason)
        conn.commit()
        return result


@app.post("/control/research/pause", dependencies=[Depends(require_control_auth)])
def research_pause(payload: PauseRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.pause") as conn:
        result = RESEARCH_MANAGER.pause(conn, reason=payload.reason)
        conn.commit()
        return {"state": result}


@app.post("/control/research/resume", dependencies=[Depends(require_control_auth)])
def research_resume(payload: PauseRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.resume") as conn:
        result = RESEARCH_MANAGER.resume(conn, reason=payload.reason)
        conn.commit()
        return {"state": result}


@app.post("/control/repair/run-smoke-tests", dependencies=[Depends(require_control_auth)])
def run_smoke_tests(payload: SmokeRequest) -> Dict[str, Any]:
    tests = payload.tests or ["import_modules", "engineering_supervisor_schema", "control_api_boot", "patch_roundtrip"]
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.smoke") as conn:
        results = SMOKE_RUNNER.run(test_names=tests, incident_id=None, action_id=None, conn=conn)
        conn.commit()
        return {"results": [item.model_dump() for item in results]}


@app.post("/control/repair/apply-approved-patch", dependencies=[Depends(require_control_auth)])
def apply_approved_patch(payload: PatchRequest) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=False, autocommit=False, application_name="datavis.control.api.patch") as conn:
        incident = _ensure_manual_incident(conn, summary=f"Manual approved patch: {payload.patchTemplate}")
        action_id = STORE.start_action(
            conn,
            incident_id=int(incident["id"]),
            action_type="patch_known_issue",
            rationale=f"manual approved patch {payload.patchTemplate}",
            requested_payload={"patchTemplate": payload.patchTemplate, "restartServices": payload.restartServices},
        )
        decision = EngineeringSupervisorDecision(
            decision="patch_known_issue",
            reason=f"Manual approved patch {payload.patchTemplate}",
            confidence_note="Manual bounded patch request through the private control API.",
            patch_template=payload.patchTemplate,  # type: ignore[arg-type]
            restart_services=payload.restartServices,
            smoke_tests=["import_modules", "control_api_boot", "patch_roundtrip"],
        )
        result = EXECUTOR.execute(
            conn,
            incident=incident,
            decision=decision,
            action_id=action_id,
        )
        smoke_results = SMOKE_RUNNER.run(test_names=decision.smoke_tests, incident_id=int(incident["id"]), action_id=action_id, conn=conn)
        failures = [item.model_dump() for item in smoke_results if item.status == "failed"]
        if failures and result.get("patch", {}).get("applied"):
            result["rollback"] = EXECUTOR.rollback_last_patch(conn, incident_id=int(incident["id"]))
        action_status = "failed" if failures else "succeeded"
        incident_status = "open" if failures else "resolved"
        STORE.finish_action(
            conn,
            action_id=action_id,
            status=action_status,
            result_payload={"execution": result, "smokeResults": [item.model_dump() for item in smoke_results]},
            error_text="manual patch smoke tests failed" if failures else None,
        )
        STORE.transition_incident(
            conn,
            incident_id=int(incident["id"]),
            status=incident_status,
            resolution_payload={"execution": result, "smokeResults": [item.model_dump() for item in smoke_results]},
            action_id=action_id,
            increment_retry=bool(failures),
        )
        conn.commit()
        return {"execution": result, "smokeResults": [item.model_dump() for item in smoke_results]}


@app.get("/control/repair/history", dependencies=[Depends(require_control_auth)])
def repair_history(limit: int = Query(20, ge=1, le=100)) -> Dict[str, Any]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.history") as conn:
        return {"patches": STORE.list_patch_history(conn, limit=limit), "incidents": STORE.list_recent_incidents(conn, limit=limit)}


@app.get("/control/repair/current-incident", dependencies=[Depends(require_control_auth)])
def current_incident() -> Dict[str, Any]:
    with connection(SETTINGS, readonly=True, autocommit=True, application_name="datavis.control.api.current_incident") as conn:
        return STORE.get_active_incident(conn)


def _ensure_manual_incident(conn: Any, *, summary: str) -> Dict[str, Any]:
    incident = STORE.get_active_incident(conn)
    if incident:
        return incident
    return STORE.create_incident(
        conn,
        IncidentCandidate(
            incident_type="code_bug",
            severity="info",
            fingerprint=f"manual-{abs(hash(summary)) % 10_000_000:07d}",
            summary=summary,
            details={"manual": True},
            affected_services=[],
        ),
    )
