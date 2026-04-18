from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from psycopg2.extras import Json

from datavis.control.config import ControlSettings, ensure_runtime_dirs
from datavis.control.db import connection


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EngineeringJournal:
    def __init__(self, settings: ControlSettings, component: str) -> None:
        self._settings = settings
        self._component = component
        ensure_runtime_dirs(settings)

    @property
    def component_path(self) -> Path:
        return self._settings.journal_dir / f"{self._component}.jsonl"

    def write(
        self,
        *,
        level: str,
        event_type: str,
        message: str,
        payload: Optional[Dict[str, Any]] = None,
        incident_id: Optional[int] = None,
        action_id: Optional[int] = None,
        patch_id: Optional[int] = None,
        conn: Any | None = None,
    ) -> None:
        record = {
            "createdAt": utc_now().isoformat(),
            "component": self._component,
            "level": str(level or "INFO").upper(),
            "eventType": event_type,
            "message": message,
            "incidentId": incident_id,
            "actionId": action_id,
            "patchId": patch_id,
            "payload": payload or {},
        }
        self._append_file_record(record)
        self._insert_db_record(record, conn=conn)

    def _append_file_record(self, record: Dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        for path in self._path_candidates():
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                    handle.write("\n")
                return
            except OSError:
                continue

    def _path_candidates(self) -> list[Path]:
        fallback = Path(tempfile.gettempdir()) / "datavis-control" / "journals" / f"{self._component}.jsonl"
        return [self.component_path, fallback]

    def _insert_db_record(self, record: Dict[str, Any], *, conn: Any | None) -> None:
        try:
            if conn is not None:
                self._insert_db_record_inner(conn, record)
                return
            with connection(self._settings, readonly=False, autocommit=True, application_name=f"datavis.control.{self._component}") as own_conn:
                self._insert_db_record_inner(own_conn, record)
        except Exception:
            return

    @staticmethod
    def _insert_db_record_inner(conn: Any, record: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.engineering_journal (
                    component, level, event_type, incident_id, action_id, patch_id, message, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    record["component"],
                    record["level"],
                    record["eventType"],
                    record["incidentId"],
                    record["actionId"],
                    record["patchId"],
                    record["message"],
                    Json(record["payload"]),
                ),
            )

