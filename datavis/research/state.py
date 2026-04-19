from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from psycopg2.extras import Json

from datavis.research.config import ResearchSettings


CONTROL_STATE_KEY = "entry_loop_control"


def default_control_state(settings: ResearchSettings) -> Dict[str, Any]:
    return {
        "paused": False,
        "stop_requested": False,
        "final_verdict": None,
        "final_reason": None,
        "iteration_budget": settings.iteration_budget,
        "min_runs_before_stop": settings.min_runs_before_stop,
        "iterations_completed": 0,
        "last_run_id": None,
        "last_decision_id": None,
        "last_stop_override_reason": None,
        "last_selected_fingerprint": None,
        "selected_study_day": None,
        "seeded": False,
    }


def ensure_control_state(conn: Any, settings: ResearchSettings) -> Dict[str, Any]:
    state = get_state(conn, CONTROL_STATE_KEY)
    if state:
        return state
    payload = default_control_state(settings)
    set_state(conn, CONTROL_STATE_KEY, payload)
    return payload


def get_state(conn: Any, key: str) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM research.state WHERE key = %s", (key,))
        row = cur.fetchone()
    if not row:
        return {}
    value = row[0]
    return dict(value or {})


def set_state(conn: Any, key: str, value: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO research.state (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """,
            (key, Json(value)),
        )


def normalize_brokerday_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    return date.fromisoformat(text).isoformat()
