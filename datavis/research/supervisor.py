from __future__ import annotations

import time
from typing import Any, Dict, Optional

import psycopg2.extras
from psycopg2.extras import Json

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import SearchGuardrails, sanitize_parameters, validate_supervisor_decision
from datavis.research.journal import ResearchJournal
from datavis.research.supervisor_client import OpenAISupervisorClient


class ResearchSupervisor:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings
        self._journal = ResearchJournal(settings, "supervisor")
        self._client = OpenAISupervisorClient(settings)
        self._limits = SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
        )

    def run_forever(self, conn_factory: Any) -> None:
        self._journal.write(level="INFO", event_type="supervisor.start", message="supervisor loop started")
        while True:
            if not self._client.is_enabled():
                self._journal.write(
                    level="WARNING",
                    event_type="supervisor.disabled",
                    message="supervisor idle because model or API key is missing",
                )
                time.sleep(max(30.0, self._settings.supervisor_poll_seconds))
                continue
            with conn_factory(readonly=False, autocommit=False) as conn:
                did_work = self.run_once(conn)
                conn.commit()
            if not did_work:
                time.sleep(self._settings.supervisor_poll_seconds)

    def run_once(self, conn: Any) -> bool:
        row = self._claim_decision(conn)
        if row is None:
            return False
        decision_id = int(row["id"])
        try:
            base_params = sanitize_parameters((row.get("briefing") or {}).get("config") or {}, limits=self._limits)
            try:
                decision_payload, raw_text = self._client.review(dict(row["briefing"] or {}))
            except Exception as exc:
                self._requeue_decision(conn, decision_id=decision_id, error_text=str(exc))
                self._journal.write(
                    level="WARNING",
                    event_type="supervisor.decision.retry",
                    message=f"retrying decision {decision_id} after API failure",
                    decision_id=decision_id,
                    run_id=int(row["run_id"]),
                    payload={"error": str(exc)},
                    conn=conn,
                )
                return True
            validated_decision, _ = validate_supervisor_decision(decision_payload, base_parameters=base_params, limits=self._limits)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE research.decision
                    SET status = 'completed',
                        completed_at = NOW(),
                        raw_response = %s,
                        decision = %s,
                        reason = %s,
                        stop_reason = %s,
                        decision_json = %s
                    WHERE id = %s
                    """,
                    (
                        raw_text,
                        validated_decision.decision,
                        validated_decision.reason,
                        validated_decision.stop_reason,
                        Json(validated_decision.model_dump()),
                        decision_id,
                    ),
                )
            self._journal.write(
                level="INFO",
                event_type="supervisor.decision.completed",
                message=f"completed decision {decision_id}",
                decision_id=decision_id,
                run_id=int(row["run_id"]),
                payload={"decision": validated_decision.decision, "stopReason": validated_decision.stop_reason},
                conn=conn,
            )
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE research.decision
                    SET status = 'rejected',
                        completed_at = NOW(),
                        validation_error = %s
                    WHERE id = %s
                    """,
                    (str(exc)[:4000], decision_id),
                )
            self._journal.write(
                level="ERROR",
                event_type="supervisor.decision.rejected",
                message=f"rejected decision {decision_id}",
                decision_id=decision_id,
                run_id=int(row["run_id"]),
                payload={"error": str(exc)},
                conn=conn,
            )
        return True

    @staticmethod
    def _requeue_decision(conn: Any, *, decision_id: int, error_text: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.decision
                SET status = 'pending',
                    started_at = NULL,
                    supervisor_name = NULL,
                    validation_error = %s
                WHERE id = %s
                """,
                (error_text[:4000], decision_id),
            )

    def _claim_decision(self, conn: Any) -> Optional[Dict[str, Any]]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, run_id, briefing
                FROM research.decision
                WHERE status = 'pending'
                ORDER BY requested_at ASC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            payload = dict(row)
            cur.execute(
                """
                UPDATE research.decision
                SET status = 'running',
                    started_at = NOW(),
                    supervisor_name = %s
                WHERE id = %s
                """,
                (self._settings.supervisor_name, int(payload["id"])),
            )
        return payload
