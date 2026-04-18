from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping

from psycopg2.extras import Json

from datavis.control.config import ControlSettings
from datavis.research.config import ResearchSettings
from datavis.research.guardrails import APPROVED_CANDIDATE_FAMILIES, APPROVED_SIDE_LOCKS
from datavis.research.state import get_state, set_state


CONTROL_MISSION_KEY = "control_mission"
CONTROL_SETTINGS_KEY = "control_panel_settings"


def _clean_text(value: Any, *, fallback: str = "", max_length: int = 4000) -> str:
    text = str(value or fallback).strip()
    return text[:max_length]


def _clean_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _clean_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        resolved = default
    return max(minimum, min(maximum, resolved))


def _clean_text_list(values: Any, *, max_items: int = 24, max_length: int = 256) -> List[str]:
    if isinstance(values, str):
        raw_values = [item.strip() for item in values.replace("\n", ",").split(",")]
    elif isinstance(values, Iterable):
        raw_values = [str(item or "").strip() for item in values]
    else:
        raw_values = []
    cleaned: List[str] = []
    seen = set()
    for item in raw_values:
        if not item:
            continue
        normalized = item[:max_length]
        if normalized in seen:
            continue
        cleaned.append(normalized)
        seen.add(normalized)
        if len(cleaned) >= max_items:
            break
    return cleaned


def _clean_family_map(values: Any) -> Dict[str, bool]:
    source = dict(values or {}) if isinstance(values, Mapping) else {}
    payload: Dict[str, bool] = {}
    for family in APPROVED_CANDIDATE_FAMILIES:
        payload[family] = _clean_bool(source.get(family), default=True)
    if not any(payload.values()):
        payload[APPROVED_CANDIDATE_FAMILIES[0]] = True
    return payload


def default_mission_payload(settings: ResearchSettings) -> Dict[str, Any]:
    return normalize_mission_payload(
        {
            "missionTitle": "Find tradable entry setups one broker day at a time",
            "mainObjective": (
                "Find candidate entry points first, then discover similarities among winners, test them on same-day "
                "holdout and prior-day context, and continue until the system finds the best justified entry-only result "
                "or a real final verdict."
            ),
            "tradableDefinition": (
                "Similar positions, properties, indicators, and setup structures that move at least 2x current spread "
                "fast enough to get risk-free as early as possible, with the highest sustainable accuracy."
            ),
            "scoringPriority": "accuracy > speed > stability > frequency",
            "currentPhase": "entry-only",
            "allowedDirections": [
                "find strong candidate entry points",
                "cluster similarities among winning entries",
                "prefer low-spread regimes when evidence supports it",
                "validate on same-day holdout before accepting strength",
            ],
            "forbiddenDirections": [
                "live trading or execution changes",
                "hold/exit logic expansion before entry quality is proven",
                "unbounded brute force scans",
                "manual guardrail bypasses",
            ],
            "minimumRunsBeforeStop": settings.min_runs_before_stop,
            "sameDayHoldoutRequired": True,
            "priorDayValidationRequired": False,
            "preferredSideLock": "both",
            "guidanceNotes": (
                "Entry-only first. Seek regimes that reach 2x spread quickly enough to reduce risk early. "
                "Do not stop on one weak run when bounded next directions remain."
            ),
        }
    )


def normalize_mission_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    side_lock = _clean_text(payload.get("preferredSideLock"), fallback="both", max_length=16).lower() or "both"
    if side_lock not in APPROVED_SIDE_LOCKS:
        side_lock = "both"
    return {
        "missionTitle": _clean_text(payload.get("missionTitle"), max_length=240),
        "mainObjective": _clean_text(payload.get("mainObjective"), max_length=4000),
        "tradableDefinition": _clean_text(payload.get("tradableDefinition"), max_length=4000),
        "scoringPriority": _clean_text(payload.get("scoringPriority"), fallback="accuracy > speed > stability > frequency", max_length=240),
        "currentPhase": _clean_text(payload.get("currentPhase"), fallback="entry-only", max_length=120),
        "allowedDirections": _clean_text_list(payload.get("allowedDirections"), max_items=24, max_length=240),
        "forbiddenDirections": _clean_text_list(payload.get("forbiddenDirections"), max_items=24, max_length=240),
        "minimumRunsBeforeStop": _clean_int(payload.get("minimumRunsBeforeStop"), default=5, minimum=1, maximum=128),
        "sameDayHoldoutRequired": _clean_bool(payload.get("sameDayHoldoutRequired"), default=True),
        "priorDayValidationRequired": _clean_bool(payload.get("priorDayValidationRequired"), default=False),
        "preferredSideLock": side_lock,
        "guidanceNotes": _clean_text(payload.get("guidanceNotes"), max_length=4000),
    }


def default_panel_settings(control_settings: ControlSettings, research_settings: ResearchSettings) -> Dict[str, Any]:
    return normalize_panel_settings(
        {
            "researchLoopEnabled": True,
            "engineeringLoopEnabled": control_settings.enable_loop,
            "maxRetriesPerIncident": control_settings.incident_max_retries,
            "maxNextJobs": research_settings.max_next_jobs,
            "maxPatchFiles": control_settings.max_patch_files,
            "maxPatchLineChanges": control_settings.max_patch_line_changes,
            "maxPatchBytes": control_settings.max_patch_bytes,
            "restartRateLimitPerHour": control_settings.max_restarts_per_hour,
            "failedDirectionStopCount": research_settings.failed_direction_stop_count,
            "iterationBudget": research_settings.iteration_budget,
            "approvedSliceLadder": list(research_settings.slice_ladder),
            "approvedCandidateFamilies": {family: True for family in APPROVED_CANDIDATE_FAMILIES},
            "researchModelOverride": "",
            "engineeringModelOverride": "",
        },
        control_settings=control_settings,
        research_settings=research_settings,
    )


def normalize_panel_settings(
    payload: Mapping[str, Any],
    *,
    control_settings: ControlSettings,
    research_settings: ResearchSettings,
) -> Dict[str, Any]:
    raw_ladder = payload.get("approvedSliceLadder")
    if isinstance(raw_ladder, str):
        ladder_values = [chunk.strip() for chunk in raw_ladder.split(",")]
    else:
        ladder_values = list(raw_ladder or [])
    ladder = sorted(
        {
            _clean_int(value, default=research_settings.seed_slice_rows, minimum=500, maximum=research_settings.max_slice_rows)
            for value in ladder_values
            if str(value or "").strip()
        }
    )
    if not ladder:
        ladder = list(research_settings.slice_ladder)
    return {
        "researchLoopEnabled": _clean_bool(payload.get("researchLoopEnabled"), default=True),
        "engineeringLoopEnabled": _clean_bool(payload.get("engineeringLoopEnabled"), default=control_settings.enable_loop),
        "maxRetriesPerIncident": _clean_int(payload.get("maxRetriesPerIncident"), default=control_settings.incident_max_retries, minimum=1, maximum=8),
        "maxNextJobs": _clean_int(payload.get("maxNextJobs"), default=research_settings.max_next_jobs, minimum=1, maximum=8),
        "maxPatchFiles": _clean_int(payload.get("maxPatchFiles"), default=control_settings.max_patch_files, minimum=1, maximum=8),
        "maxPatchLineChanges": _clean_int(payload.get("maxPatchLineChanges"), default=control_settings.max_patch_line_changes, minimum=10, maximum=2000),
        "maxPatchBytes": _clean_int(payload.get("maxPatchBytes"), default=control_settings.max_patch_bytes, minimum=512, maximum=250000),
        "restartRateLimitPerHour": _clean_int(payload.get("restartRateLimitPerHour"), default=control_settings.max_restarts_per_hour, minimum=1, maximum=48),
        "failedDirectionStopCount": _clean_int(payload.get("failedDirectionStopCount"), default=research_settings.failed_direction_stop_count, minimum=2, maximum=16),
        "iterationBudget": _clean_int(payload.get("iterationBudget"), default=research_settings.iteration_budget, minimum=1, maximum=128),
        "approvedSliceLadder": ladder,
        "approvedCandidateFamilies": _clean_family_map(payload.get("approvedCandidateFamilies")),
        "researchModelOverride": _clean_text(payload.get("researchModelOverride"), max_length=160),
        "engineeringModelOverride": _clean_text(payload.get("engineeringModelOverride"), max_length=160),
    }


def load_mission(conn: Any, settings: ResearchSettings) -> Dict[str, Any]:
    payload = get_state(conn, CONTROL_MISSION_KEY) or default_mission_payload(settings)
    return normalize_mission_payload(payload)


def save_mission(conn: Any, payload: Mapping[str, Any], settings: ResearchSettings) -> Dict[str, Any]:
    merged = default_mission_payload(settings)
    merged.update(dict(payload or {}))
    normalized = normalize_mission_payload(merged)
    set_state(conn, CONTROL_MISSION_KEY, normalized)
    return normalized


def load_panel_settings(conn: Any, control_settings: ControlSettings, research_settings: ResearchSettings) -> Dict[str, Any]:
    payload = get_state(conn, CONTROL_SETTINGS_KEY) or default_panel_settings(control_settings, research_settings)
    return normalize_panel_settings(payload, control_settings=control_settings, research_settings=research_settings)


def save_panel_settings(
    conn: Any,
    payload: Mapping[str, Any],
    *,
    control_settings: ControlSettings,
    research_settings: ResearchSettings,
) -> Dict[str, Any]:
    merged = default_panel_settings(control_settings, research_settings)
    merged.update(dict(payload or {}))
    normalized = normalize_panel_settings(merged, control_settings=control_settings, research_settings=research_settings)
    set_state(conn, CONTROL_SETTINGS_KEY, normalized)
    return normalized


def resolve_research_runtime(conn: Any, control_settings: ControlSettings, research_settings: ResearchSettings) -> Dict[str, Any]:
    mission = load_mission(conn, research_settings)
    panel_settings = load_panel_settings(conn, control_settings, research_settings)
    allowed_families = [family for family in APPROVED_CANDIDATE_FAMILIES if panel_settings["approvedCandidateFamilies"].get(family, False)]
    if not allowed_families:
        allowed_families = [APPROVED_CANDIDATE_FAMILIES[0]]
    return {
        "enabled": bool(panel_settings["researchLoopEnabled"]),
        "minRunsBeforeStop": int(mission["minimumRunsBeforeStop"]),
        "failedDirectionStopCount": int(panel_settings["failedDirectionStopCount"]),
        "iterationBudget": int(panel_settings["iterationBudget"]),
        "maxNextJobs": int(panel_settings["maxNextJobs"]),
        "approvedSliceLadder": list(panel_settings["approvedSliceLadder"]),
        "allowedCandidateFamilies": allowed_families,
        "preferredSideLock": mission["preferredSideLock"],
        "sameDayHoldoutRequired": bool(mission["sameDayHoldoutRequired"]),
        "priorDayValidationRequired": bool(mission["priorDayValidationRequired"]),
        "researchModelOverride": panel_settings["researchModelOverride"],
        "mission": mission,
        "settings": panel_settings,
    }


def resolve_engineering_runtime(conn: Any, control_settings: ControlSettings, research_settings: ResearchSettings) -> Dict[str, Any]:
    mission = load_mission(conn, research_settings)
    panel_settings = load_panel_settings(conn, control_settings, research_settings)
    return {
        "enabled": bool(panel_settings["engineeringLoopEnabled"]),
        "maxRetriesPerIncident": int(panel_settings["maxRetriesPerIncident"]),
        "maxPatchFiles": int(panel_settings["maxPatchFiles"]),
        "maxPatchLineChanges": int(panel_settings["maxPatchLineChanges"]),
        "maxPatchBytes": int(panel_settings["maxPatchBytes"]),
        "restartRateLimitPerHour": int(panel_settings["restartRateLimitPerHour"]),
        "engineeringModelOverride": panel_settings["engineeringModelOverride"],
        "mission": mission,
        "settings": panel_settings,
    }


def mission_briefing_payload(mission: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "title": mission.get("missionTitle"),
        "objective": mission.get("mainObjective"),
        "tradableDefinition": mission.get("tradableDefinition"),
        "scoringPriority": mission.get("scoringPriority"),
        "currentPhase": mission.get("currentPhase"),
        "allowedDirections": list(mission.get("allowedDirections") or []),
        "forbiddenDirections": list(mission.get("forbiddenDirections") or []),
        "minimumRunsBeforeStop": mission.get("minimumRunsBeforeStop"),
        "sameDayHoldoutRequired": bool(mission.get("sameDayHoldoutRequired")),
        "priorDayValidationRequired": bool(mission.get("priorDayValidationRequired")),
        "preferredSideLock": mission.get("preferredSideLock"),
        "guidanceNotes": mission.get("guidanceNotes"),
    }


def audit_operator_action(
    conn: Any,
    *,
    actor: str,
    action_type: str,
    scope: str,
    target_id: str | None,
    payload: Mapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO research.control_operator_action (
                actor, action_type, scope, target_id, payload, result
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                _clean_text(actor, fallback="private-admin", max_length=128),
                _clean_text(action_type, max_length=120),
                _clean_text(scope, max_length=120),
                _clean_text(target_id, max_length=120) or None,
                Json(dict(payload or {})),
                Json(dict(result or {})),
            ),
        )
