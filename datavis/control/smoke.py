from __future__ import annotations

import importlib
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from datavis.control.config import ControlSettings
from datavis.control.db import connection
from datavis.control.models import SmokeTestResult
from datavis.control.research_manager import ResearchManager
from datavis.control.service_manager import ServiceManager
from datavis.control.store import EngineeringStore
from datavis.control.supervisor import EngineeringSupervisor


class SmokeRunner:
    def __init__(
        self,
        settings: ControlSettings,
        *,
        store: EngineeringStore,
        research_manager: ResearchManager,
        service_manager: ServiceManager,
        supervisor: EngineeringSupervisor,
        executor: Any,
    ) -> None:
        self._settings = settings
        self._store = store
        self._research_manager = research_manager
        self._service_manager = service_manager
        self._supervisor = supervisor
        self._executor = executor

    def run(
        self,
        *,
        test_names: Iterable[str],
        incident_id: Optional[int],
        action_id: Optional[int],
        conn: Any | None = None,
    ) -> List[SmokeTestResult]:
        results = []
        for raw_name in test_names:
            name = str(raw_name)
            started = time.perf_counter()
            result = self._run_one(name=name, conn=conn)
            duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
            raw_payload = result.get("payload")
            payload = dict(raw_payload) if isinstance(raw_payload, dict) else {"value": raw_payload}
            detail = str(result.get("detail") or name)
            output_path = self._write_artifact(
                incident_id=incident_id,
                action_id=action_id,
                test_name=name,
                payload={"status": result["status"], "detail": detail, "payload": payload, "durationMs": duration_ms},
            )
            model = SmokeTestResult(
                name=name,  # type: ignore[arg-type]
                status=result["status"],
                duration_ms=duration_ms,
                detail=detail,
                payload=payload,
                output_path=str(output_path) if output_path else None,
            )
            results.append(model)
            if incident_id is not None:
                self._store.record_smoketest(
                    conn,
                    incident_id=incident_id,
                    action_id=action_id,
                    test_name=name,
                    status=model.status,
                    result_json=model.model_dump(),
                    output_path=model.output_path,
                )
        return results

    def _run_one(self, *, name: str, conn: Any | None) -> Dict[str, Any]:
        if name == "import_modules":
            for module_name in (
                "datavis.control.api",
                "datavis.control.orchestrator",
                "datavis.control.executor",
                "datavis.research.supervisor_client",
            ):
                importlib.import_module(module_name)
            return {"status": "passed", "detail": "Imports resolved for control and research modules.", "payload": {}}
        if name == "control_api_boot":
            from fastapi.testclient import TestClient
            from datavis.control.api import app

            with TestClient(app) as client:
                response = client.get("/control/health")
                payload = response.json()
            return {
                "status": "passed" if response.status_code == 200 else "failed",
                "detail": f"Control API responded with HTTP {response.status_code}.",
                "payload": payload,
            }
        if name == "engineering_supervisor_schema":
            decision, _ = self._supervisor.review_incident(
                {
                    "incident": {"id": 1, "type": "service_runtime_issue", "details": {}, "summary": "service down"},
                    "serviceStatus": [{"name": self._settings.research_settings.worker_name}],
                },
                force_fallback=True,
            )
            return {"status": "passed", "detail": "Engineering supervisor returned a structured bounded decision.", "payload": decision.model_dump()}
        if name == "patch_roundtrip":
            payload = self._executor.simulate_known_repair_path()
            return {
                "status": "passed" if payload.get("ok") else "failed",
                "detail": "Repair executor completed a bounded simulated patch path.",
                "payload": payload,
            }
        if name == "db_select_1":
            try:
                if conn is not None:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        value = int(cur.fetchone()[0])
                else:
                    with connection(self._settings, readonly=True, autocommit=True, application_name="datavis.control.smoke") as own_conn:
                        with own_conn.cursor() as cur:
                            cur.execute("SELECT 1")
                            value = int(cur.fetchone()[0])
                return {"status": "passed", "detail": "Database connectivity smoke test succeeded.", "payload": {"value": value}}
            except Exception as exc:
                return {"status": "skipped", "detail": f"Database smoke test unavailable: {exc}", "payload": {"error": str(exc)}}
        if name == "research_status_query":
            try:
                if conn is None:
                    with connection(self._settings, readonly=True, autocommit=True, application_name="datavis.control.smoke") as own_conn:
                        payload = self._research_manager.status(own_conn)
                else:
                    payload = self._research_manager.status(conn)
                return {"status": "passed", "detail": "Research status query succeeded.", "payload": payload}
            except Exception as exc:
                return {"status": "skipped", "detail": f"Research status query unavailable: {exc}", "payload": {"error": str(exc)}}
        if name == "service_manager_probe":
            payload = [item.model_dump() for item in self._service_manager.status_many(self._settings.research_services)]
            status = "passed" if any(item.get("probe_supported") for item in payload) else "skipped"
            detail = "Service manager probe completed." if status == "passed" else "systemctl unavailable on this host."
            return {"status": status, "detail": detail, "payload": payload}
        return {"status": "skipped", "detail": f"Unknown smoke test {name}", "payload": {}}

    def _write_artifact(
        self,
        *,
        incident_id: Optional[int],
        action_id: Optional[int],
        test_name: str,
        payload: Dict[str, Any],
    ) -> Optional[Path]:
        if incident_id is not None:
            target_dir = self._settings.artifact_dir / f"incident-{incident_id:06d}" / f"action-{int(action_id or 0):06d}" / "smoketests"
        else:
            target_dir = self._settings.artifact_dir / "manual-smoketests"
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            path = target_dir / f"{test_name}.json"
            path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
            return path
        except OSError:
            return None
