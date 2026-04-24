from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import psycopg2.errors

from datavis import app


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def cursor(self, *args, **kwargs):
        return _FakeCursor()


class _FakeDbConnection:
    def __enter__(self):
        return _FakeConnection()

    def __exit__(self, exc_type, exc, tb):
        return False


class MavgFailSafeTests(unittest.TestCase):
    def test_mavg_tick_range_returns_empty_when_config_table_is_missing(self):
        with (
            self.assertLogs("datavis.mavg", level="ERROR") as logs,
            patch(
                "datavis.app.list_mavg_config_rows",
                side_effect=psycopg2.errors.UndefinedTable('relation "public.mavgconfig" does not exist'),
            ),
        ):
            payload = app.mavg_payload_for_tick_range(
                object(),
                page="live",
                start_id=1,
                end_id=2,
                include_configs=True,
            )

        self.assertEqual(payload, {"mavgConfigs": [], "mavgPoints": [], "mavgCursorId": None})
        self.assertTrue(any("Moving-average query failed" in line for line in logs.output))

    def test_bootstrap_returns_ticks_when_mavg_query_fails(self):
        tick_rows = [
            {
                "id": 101,
                "timestamp": datetime(2026, 4, 24, 7, 0, tzinfo=timezone.utc),
                "bid": 3300.10,
                "ask": 3300.30,
                "mid": 3300.20,
                "spread": 0.20,
            },
            {
                "id": 102,
                "timestamp": datetime(2026, 4, 24, 7, 0, 1, tzinfo=timezone.utc),
                "bid": 3300.15,
                "ask": 3300.35,
                "mid": 3300.25,
                "spread": 0.20,
            },
        ]

        with (
            self.assertLogs("datavis.mavg", level="ERROR") as logs,
            patch("datavis.app.db_connection", return_value=_FakeDbConnection()),
            patch(
                "datavis.app.query_tick_bounds",
                return_value={
                    "first_id": 101,
                    "last_id": 102,
                    "first_timestamp": tick_rows[0]["timestamp"],
                    "last_timestamp": tick_rows[-1]["timestamp"],
                },
            ),
            patch("datavis.app.query_bootstrap_rows", return_value=tick_rows),
            patch("datavis.app.list_mavg_config_rows", side_effect=RuntimeError("mavg unavailable")),
            patch("datavis.app.rect_snapshot_for_mode", return_value=None),
        ):
            payload = app.load_bootstrap_payload(
                mode="live",
                start_id=None,
                window=50,
                show_ticks=True,
                show_events=False,
                show_structure=False,
                show_ranges=False,
            )

        self.assertEqual(payload["rowCount"], 2)
        self.assertEqual([row["id"] for row in payload["rows"]], [101, 102])
        self.assertEqual(payload["mavgConfigs"], [])
        self.assertEqual(payload["mavgPoints"], [])
        self.assertIsNone(payload["mavgCursorId"])
        self.assertTrue(any("Moving-average query failed" in line for line in logs.output))


if __name__ == "__main__":
    unittest.main()
