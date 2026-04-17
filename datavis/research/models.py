from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


DecisionType = Literal["continue", "refine", "stop"]
NextActionType = Literal["run_entry_research", "stop"]
JobType = Literal["entry_research"]


class EntryResearchParameters(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    iteration: int = Field(1, ge=1, le=512)
    slice_rows: int = Field(..., ge=500, le=50000)
    warmup_rows: int = Field(..., ge=20, le=5000)
    label_variant: str = Field(..., min_length=1, max_length=64)
    candidate_family: str = Field(..., min_length=1, max_length=64)
    threshold_profile: str = Field(..., min_length=1, max_length=64)
    feature_toggles: List[str] = Field(default_factory=list, max_length=16)
    session_filter: List[str] = Field(default_factory=list, max_length=8)
    spread_filter: str = Field(..., min_length=1, max_length=64)
    dedup_rule: str = Field(..., min_length=1, max_length=64)
    train_validation_plan: str = Field(..., min_length=1, max_length=64)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = str(value or "").strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("feature_toggles", "session_filter")
    @classmethod
    def dedupe_values(cls, values: List[str]) -> List[str]:
        deduped: List[str] = []
        seen = set()
        for raw in values or []:
            value = str(raw or "").strip()
            if not value or value in seen:
                continue
            deduped.append(value)
            seen.add(value)
        return deduped


class EntryResearchParameterPatch(BaseModel):
    symbol: Optional[str] = Field(None, min_length=1, max_length=32)
    iteration: Optional[int] = Field(None, ge=1, le=512)
    slice_rows: Optional[int] = Field(None, ge=500, le=50000)
    warmup_rows: Optional[int] = Field(None, ge=20, le=5000)
    label_variant: Optional[str] = Field(None, min_length=1, max_length=64)
    candidate_family: Optional[str] = Field(None, min_length=1, max_length=64)
    threshold_profile: Optional[str] = Field(None, min_length=1, max_length=64)
    feature_toggles: Optional[List[str]] = Field(None, max_length=16)
    session_filter: Optional[List[str]] = Field(None, max_length=8)
    spread_filter: Optional[str] = Field(None, min_length=1, max_length=64)
    dedup_rule: Optional[str] = Field(None, min_length=1, max_length=64)
    train_validation_plan: Optional[str] = Field(None, min_length=1, max_length=64)


class SupervisorNextAction(BaseModel):
    type: NextActionType
    parameters: Optional[EntryResearchParameterPatch] = None


class SupervisorDecision(BaseModel):
    decision: DecisionType
    reason: str = Field(..., min_length=1, max_length=4000)
    confidence_note: str = Field(..., min_length=1, max_length=2000)
    stop_reason: Optional[str] = Field(None, max_length=128)
    verdict: Optional[str] = Field(None, max_length=256)
    next_action: Optional[SupervisorNextAction] = None


class JobRecord(BaseModel):
    id: int
    job_type: str
    status: str
    requested_by: str
    config: Dict[str, Any]
    attempt_count: int = 0
    max_attempts: int = 1
    parent_decision_id: Optional[int] = None
    parent_job_id: Optional[int] = None


class RunRecord(BaseModel):
    id: int
    job_id: int
    status: str
    symbol: str
    iteration: int
    config: Dict[str, Any]
    started_at: datetime
    finished_at: Optional[datetime] = None


class DecisionRecord(BaseModel):
    id: int
    run_id: int
    status: str
    briefing: Dict[str, Any]
    decision_json: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None

