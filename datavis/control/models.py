from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


IncidentType = Literal[
    "research_parameter_issue",
    "code_bug",
    "config_env_issue",
    "permission_path_issue",
    "service_runtime_issue",
    "openai_api_request_issue",
    "stalled_loop",
    "missing_artifact",
]
IncidentSeverity = Literal["info", "warning", "error", "critical"]
RepairDecisionType = Literal[
    "observe_only",
    "repair_config",
    "repair_permissions",
    "patch_known_issue",
    "restart_services",
    "reset_research_state",
    "requeue_job",
    "rollback_last_patch",
    "escalate_manual_review",
]
PatchTemplateName = Literal[
    "fix_openai_endpoint_normalization",
    "fix_openai_request_payload",
    "fix_decision_defaults",
    "fix_feature_toggle_clamp",
    "fix_permission_safe_journal_write",
]
ResetMode = Literal["soft", "hard"]
SmokeTestName = Literal[
    "import_modules",
    "control_api_boot",
    "engineering_supervisor_schema",
    "patch_roundtrip",
    "db_select_1",
    "research_status_query",
    "service_manager_probe",
]


class IncidentCandidate(BaseModel):
    incident_type: IncidentType
    severity: IncidentSeverity = "warning"
    fingerprint: str = Field(..., min_length=8, max_length=80)
    summary: str = Field(..., min_length=1, max_length=2000)
    details: Dict[str, Any] = Field(default_factory=dict)
    failure_signature: Optional[str] = Field(None, max_length=512)
    related_job_id: Optional[int] = Field(None, ge=1)
    related_run_id: Optional[int] = Field(None, ge=1)
    related_decision_id: Optional[int] = Field(None, ge=1)
    affected_services: List[str] = Field(default_factory=list, max_length=8)

    @field_validator("affected_services")
    @classmethod
    def dedupe_services(cls, values: List[str]) -> List[str]:
        deduped: List[str] = []
        seen = set()
        for raw in values or []:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            deduped.append(value)
            seen.add(value)
        return deduped


class EngineeringSupervisorDecision(BaseModel):
    decision: RepairDecisionType
    reason: str = Field(..., min_length=1, max_length=4000)
    confidence_note: str = Field(..., min_length=1, max_length=2000)
    patch_template: Optional[PatchTemplateName] = None
    config_changes: Dict[str, Any] = Field(default_factory=dict)
    restart_services: List[str] = Field(default_factory=list, max_length=8)
    requeue_job_id: Optional[int] = Field(None, ge=1)
    reset_mode: Optional[ResetMode] = None
    smoke_tests: List[SmokeTestName] = Field(default_factory=list, max_length=8)
    pause_research: bool = True

    @field_validator("restart_services")
    @classmethod
    def dedupe_restart_services(cls, values: List[str]) -> List[str]:
        deduped: List[str] = []
        seen = set()
        for raw in values or []:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            deduped.append(value)
            seen.add(value)
        return deduped

    @model_validator(mode="after")
    def validate_shape(self) -> "EngineeringSupervisorDecision":
        if self.decision == "patch_known_issue" and self.patch_template is None:
            raise ValueError("patch_known_issue requires patch_template")
        if self.decision == "repair_config" and not self.config_changes:
            raise ValueError("repair_config requires config_changes")
        if self.decision == "restart_services" and not self.restart_services:
            raise ValueError("restart_services requires restart_services")
        if self.decision == "requeue_job" and self.requeue_job_id is None:
            raise ValueError("requeue_job requires requeue_job_id")
        if self.decision == "reset_research_state" and self.reset_mode is None:
            raise ValueError("reset_research_state requires reset_mode")
        return self


class ServiceSnapshot(BaseModel):
    name: str
    active_state: str = "unknown"
    sub_state: str = "unknown"
    status_text: Optional[str] = None
    restart_count: int = 0
    probe_supported: bool = False


class SmokeTestResult(BaseModel):
    name: SmokeTestName
    status: Literal["passed", "failed", "skipped"]
    duration_ms: float = Field(0.0, ge=0.0)
    detail: str = Field(..., min_length=1, max_length=4000)
    payload: Dict[str, Any] = Field(default_factory=dict)
    output_path: Optional[str] = None

