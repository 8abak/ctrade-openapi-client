"""
label_micro_events.py
Purpose: detect micro entries inside the latest CLOSED macro segment.
Bootstrap: ensure tables and return no-op summary.
"""

from typing import Dict, Any
from sqlalchemy import create_engine, text
import os

_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://babak:babak33044@localhost:5432/trading",
)
_engine = create_engine(_DB_URL)

# reuse schema creation from macro module to keep single source of truth
_DDL_MIN = """
CREATE TABLE IF NOT EXISTS micro_events (
  event_id     BIGSERIAL PRIMARY KEY,
  segment_id   BIGINT,
  tick_id      BIGINT NOT NULL,
  event_type   TEXT NOT NULL CHECK (event_type IN ('pullback_end','breakout','retest_hold')),
  features     JSONB,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_micro_events_segment ON micro_events(segment_id);
CREATE INDEX IF NOT EXISTS idx_micro_events_type ON micro_events(event_type);
"""


def _ensure_tables():
    with _engine.begin() as conn:
        conn.execute(text(_DDL_MIN))


def DetectMicroEventsForLatestClosedSegment() -> Dict[str, Any]:
    """
    TODO: scan the latest CLOSED macro segment and insert micro candidates (idempotent).
    Current: ensure table exists and return zero-add summary.
    """
    _ensure_tables()
    return {"events_added": 0}
