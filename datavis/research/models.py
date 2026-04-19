from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


DecisionType = Literal["continue", "refine", "explore_new_family", "increase_slice", "split_by_pattern", "stop"]
NextActionType = Literal["continue", "refine", "explore_new_family", "increase_slice", "split_by_pattern"]
JobType = Literal["entry_research"]
SideLock = Literal["both", "long", "short"]
PredicateOperator = Literal[">=", "<="]


class PredicateSpec(BaseModel):
    feature: str = Field(..., min_length=1, max_length=64)
    operator: PredicateOperator
    threshold: float = Field(..., ge=-1_000_000.0, le=1_000_000.0)


class CandidateSeed(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    family: str = Field(..., min_length=1, max_length=64)
    side: Literal["long", "short"]
    predicates: List[PredicateSpec] = Field(default_factory=list, max_length=6)


class ContrastHint(BaseModel):
    feature: str = Field(..., min_length=1, max_length=64)
    operator: PredicateOperator
    threshold: float = Field(..., ge=-1_000_000.0, le=1_000_000.0)
    score: float = Field(0.0, ge=0.0, le=1_000_000.0)
    reason: Optional[str] = Field(None, max_length=512)


class EntryResearchParameters(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    iteration: int = Field(1, ge=1, le=512)
    study_brokerday: Optional[str] = Field(None, max_length=10)
    slice_rows: int = Field(..., ge=500, le=50000)
    slice_offset_rows: int = Field(0, ge=0, le=250000)
    warmup_rows: int = Field(..., ge=20, le=5000)
    label_variant: str = Field(..., min_length=1, max_length=64)
    candidate_family: str = Field(..., min_length=1, max_length=64)
    threshold_profile: str = Field(..., min_length=1, max_length=64)
    feature_toggles: List[str] = Field(default_factory=list, max_length=16)
    session_filter: List[str] = Field(default_factory=list, max_length=8)
    spread_filter: str = Field(..., min_length=1, max_length=64)
    dedup_rule: str = Field(..., min_length=1, max_length=64)
    train_validation_plan: str = Field(..., min_length=1, max_length=64)
    side_lock: SideLock = "both"
    seed_rule: Optional[CandidateSeed] = None
    contrast_hints: List[ContrastHint] = Field(default_factory=list, max_length=6)
    source_run_id: Optional[int] = Field(None, ge=1)
    mutation_note: Optional[str] = Field(None, max_length=512)
    config_fingerprint: Optional[str] = Field(None, max_length=64)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = str(value or "").strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("study_brokerday")
    @classmethod
    def normalize_study_brokerday(cls, value: Optional[str]) -> Optional[str]:
        text = str(value or "").strip()
        if not text:
            return None
        return date.fromisoformat(text).isoformat()

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
    study_brokerday: Optional[str] = Field(None, max_length=10)
    slice_rows: Optional[int] = Field(None, ge=500, le=50000)
    slice_offset_rows: Optional[int] = Field(None, ge=0, le=250000)
    warmup_rows: Optional[int] = Field(None, ge=20, le=5000)
    label_variant: Optional[str] = Field(None, min_length=1, max_length=64)
    candidate_family: Optional[str] = Field(None, min_length=1, max_length=64)
    threshold_profile: Optional[str] = Field(None, min_length=1, max_length=64)
    feature_toggles: Optional[List[str]] = Field(None, max_length=16)
    session_filter: Optional[List[str]] = Field(None, max_length=8)
    spread_filter: Optional[str] = Field(None, min_length=1, max_length=64)
    dedup_rule: Optional[str] = Field(None, min_length=1, max_length=64)
    train_validation_plan: Optional[str] = Field(None, min_length=1, max_length=64)
    side_lock: Optional[SideLock] = None
    seed_rule: Optional[CandidateSeed] = None
    contrast_hints: Optional[List[ContrastHint]] = Field(None, max_length=6)
    source_run_id: Optional[int] = Field(None, ge=1)
    mutation_note: Optional[str] = Field(None, max_length=512)
    config_fingerprint: Optional[str] = Field(None, max_length=64)

    @field_validator("study_brokerday")
    @classmethod
    def normalize_patch_study_brokerday(cls, value: Optional[str]) -> Optional[str]:
        text = str(value or "").strip()
        if not text:
            return None
        return date.fromisoformat(text).isoformat()


class SupervisorNextAction(BaseModel):
    action: NextActionType
    reason: str = Field(..., min_length=1, max_length=2000)
    parameters: Optional[EntryResearchParameterPatch] = None


class SupervisorDecision(BaseModel):
    decision: DecisionType
    reason: str = Field(..., min_length=1, max_length=4000)
    confidence_note: str = Field(..., min_length=1, max_length=2000)
    stop_reason: Optional[str] = Field(None, max_length=128)
    verdict: Optional[str] = Field(None, max_length=256)
    next_actions: List[SupervisorNextAction] = Field(default_factory=list, max_length=8)


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
