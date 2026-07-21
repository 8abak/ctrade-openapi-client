from __future__ import annotations

import csv
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from datavis.research.fresh_data import (
    FreshDataConfig,
    FreshDataError,
    scan_fresh_csv,
)
from datavis.research.fresh_sessions import SessionAuditConfig, broker_session_bounds


UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
SYDNEY = ZoneInfo("Australia/Sydney")
FIELDS = ["id", "symbol", "timestamp", "bid", "ask", "mid", "kal"]


def row(
    tick_id: int,
    timestamp: datetime | str,
    bid: object = 100.0,
    ask: object = 100.2,
    symbol: str = "XAUUSD",
):
    return {
        "id": tick_id,
        "symbol": symbol,
        "timestamp": timestamp if isinstance(timestamp, str) else timestamp.isoformat(),
        "bid": bid,
        "ask": ask,
        "mid": 999999,
        "kal": "ignored",
    }


def config(**changes) -> FreshDataConfig:
    values = {
        "session_audit": SessionAuditConfig(
            open_tolerance_seconds=90,
            close_tolerance_seconds=90,
            friday_close_tolerance_seconds=360,
            unexpected_gap_seconds=60,
        )
    }
    values.update(changes)
    return FreshDataConfig(**values)


class FreshDataTests(unittest.TestCase):
    def setUp(self):
        self.prefix = f"fresh_data_test_{uuid4().hex}_"
        self.created: list[Path] = []

    def tearDown(self):
        for path in self.created:
            path.unlink(missing_ok=True)

    def write_csv(self, name: str, rows) -> Path:
        path = Path.cwd() / f"{self.prefix}{name}"
        self.created.append(path)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_cross_chunk_dedup_preserves_equal_time_quote_changes_and_timezone(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        moment = (bounds.start_utc + timedelta(seconds=1)).astimezone(SYDNEY)
        path = self.write_csv(
            "equal.csv",
            [
                row(10, moment, 100.0, 100.2),
                # The same instant in another offset remains an exact duplicate.
                row(11, moment.astimezone(UTC), 100.0, 100.2),
                row(12, moment, 100.1, 100.3),
                row(13, moment + timedelta(seconds=1), 100.1, 100.3),
            ],
        )
        emitted = []
        inventory = scan_fresh_csv(
            path,
            config=config(chunk_rows=1),
            on_tick=emitted.append,
        )

        self.assertEqual([item.tick.id for item in emitted], [10, 12, 13])
        self.assertEqual(emitted[0].tick.timestamp.utcoffset(), timedelta(hours=11))
        self.assertEqual(inventory.input_row_count, 4)
        self.assertEqual(inventory.normalized_quote_count, 3)
        self.assertEqual(inventory.duplicate_quote_count, 1)
        self.assertEqual(inventory.duplicate_group_count, 1)
        session = inventory.session_for_anchor(bounds.anchor)
        self.assertEqual(session.raw_row_count, 4)
        self.assertEqual(session.normalized_quote_count, 3)
        self.assertEqual(session.interarrival_seconds.count, 2)
        self.assertEqual(session.interarrival_seconds.minimum, 0.0)
        self.assertEqual(session.interarrival_seconds.median, 0.5)
        self.assertAlmostEqual(session.interarrival_seconds.p95, 0.95)

    def test_dedup_and_order_state_cross_file_boundaries(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        moment = bounds.start_utc + timedelta(seconds=1)
        first = self.write_csv("one.csv", [row(1, moment, 100.0, 100.2)])
        second = self.write_csv(
            "two.csv",
            [
                row(2, moment, 100.0, 100.2),
                row(3, moment, 100.1, 100.3),
            ],
        )
        emitted = []
        inventory = scan_fresh_csv(
            [first, second],
            config=config(chunk_rows=1),
            on_tick=emitted.append,
        )
        self.assertEqual([item.tick.id for item in emitted], [1, 3])
        self.assertEqual(inventory.duplicate_quote_count, 1)
        self.assertEqual([item.row_count for item in inventory.sources], [1, 2])

    def test_new_york_boundaries_dst_and_outside_rows_are_quarantined(self):
        friday = broker_session_bounds(date(2026, 3, 6))
        monday = broker_session_bounds(date(2026, 3, 9))
        tuesday = broker_session_bounds(date(2026, 3, 10))
        path = self.write_csv(
            "schedule.csv",
            [
                row(1, friday.end_utc - timedelta(seconds=1)),
                row(2, datetime(2026, 3, 6, 17, 30, tzinfo=NEW_YORK)),
                row(3, datetime(2026, 3, 8, 17, 59, 59, tzinfo=NEW_YORK)),
                row(4, monday.start_new_york),
                row(5, monday.end_new_york),
                row(6, tuesday.start_new_york),
            ],
        )
        emitted = []
        inventory = scan_fresh_csv(path, config=config(), on_tick=emitted.append)

        self.assertEqual(friday.start_sydney.hour, 10)
        self.assertEqual(monday.start_sydney.hour, 9)
        self.assertEqual([item.anchor for item in inventory.sessions], [
            date(2026, 3, 6),
            date(2026, 3, 9),
            date(2026, 3, 10),
        ])
        self.assertEqual(inventory.no_session_rows.raw_row_count, 2)
        self.assertEqual(inventory.maintenance_rows.raw_row_count, 1)
        self.assertEqual(inventory.session_rows.raw_row_count, 3)
        self.assertEqual(
            [item.assignment.status for item in emitted],
            ["session", "no_session", "no_session", "session", "maintenance", "session"],
        )

    def test_invalid_quotes_and_missing_ids_are_reported_not_silenced(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        start = bounds.start_utc + timedelta(seconds=1)
        path = self.write_csv(
            "quality.csv",
            [
                row(1, start, 100.0, 100.2),
                row(3, start + timedelta(seconds=1), 101.0, 100.0),
                row(5, start + timedelta(seconds=2), 100.1, 100.3),
            ],
        )
        emitted = []
        inventory = scan_fresh_csv(path, config=config(), on_tick=emitted.append)
        session = inventory.session_for_anchor(bounds.anchor)

        self.assertEqual([item.tick.id for item in emitted], [1, 5])
        self.assertEqual(inventory.invalid_quote_count, 1)
        self.assertEqual(session.invalid_quote_count, 1)
        self.assertEqual(session.noncontiguous_id_transition_count, 2)
        self.assertEqual(session.missing_id_count, 2)
        self.assertEqual(session.coverage_status, "ineligible")
        self.assertEqual(inventory.invalid_quote_samples[0].reason, "crossed quote")

    def test_session_with_only_invalid_quotes_is_ineligible_not_empty(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        path = self.write_csv(
            "invalid_only.csv",
            [row(1, bounds.start_utc + timedelta(seconds=1), 101.0, 100.0)],
        )

        inventory = scan_fresh_csv(path, config=config())
        session = inventory.session_for_anchor(bounds.anchor)

        self.assertEqual(session.raw_row_count, 1)
        self.assertEqual(session.normalized_quote_count, 0)
        self.assertEqual(session.invalid_quote_count, 1)
        self.assertEqual(session.coverage_status, "ineligible")

    def test_boundary_completeness_gap_threshold_and_empty_expected_anchor(self):
        anchor = date(2026, 1, 15)
        empty_anchor = date(2026, 1, 16)
        bounds = broker_session_bounds(anchor)
        path = self.write_csv(
            "complete.csv",
            [
                row(1, bounds.start_utc + timedelta(seconds=10)),
                # Equality with the threshold is allowed.
                row(2, bounds.start_utc + timedelta(seconds=5_010)),
                row(3, bounds.end_utc - timedelta(seconds=10)),
            ],
        )
        inventory = scan_fresh_csv(
            path,
            config=config(
                expected_anchors=(anchor, empty_anchor),
                session_audit=SessionAuditConfig(
                    open_tolerance_seconds=20,
                    close_tolerance_seconds=20,
                    friday_close_tolerance_seconds=360,
                    unexpected_gap_seconds=5_000,
                ),
            ),
        )
        session = inventory.session_for_anchor(anchor)
        empty = inventory.session_for_anchor(empty_anchor)
        self.assertTrue(session.boundary_complete)
        self.assertEqual(session.unexpected_gap_count, 1)
        self.assertEqual(session.coverage_status, "ineligible")
        self.assertEqual(session.interarrival_seconds.minimum, 5_000.0)
        self.assertEqual(empty.coverage_status, "empty")
        self.assertFalse(empty.boundary_complete)

    def test_disorder_and_duplicate_ids_are_fatal_across_chunks(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        moment = bounds.start_utc + timedelta(seconds=1)
        disordered = self.write_csv(
            "disordered.csv",
            [row(2, moment), row(1, moment)],
        )
        with self.assertRaisesRegex(FreshDataError, "ordered"):
            scan_fresh_csv(disordered, config=config(chunk_rows=1))

        duplicate_id = self.write_csv(
            "duplicate-id.csv",
            [row(1, moment), row(1, moment + timedelta(seconds=1))],
        )
        with self.assertRaisesRegex(FreshDataError, "duplicate tick id"):
            scan_fresh_csv(duplicate_id, config=config(chunk_rows=1))

    def test_extra_columns_are_ignored_but_naive_time_and_mixed_symbol_fail(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        path = self.write_csv("extra.csv", [row(1, bounds.start_utc + timedelta(seconds=1))])
        result = scan_fresh_csv(path, config=config())
        self.assertEqual(result.normalized_quote_count, 1)

        naive = self.write_csv("naive.csv", [row(1, "2026-01-15T10:00:01")])
        with self.assertRaisesRegex(FreshDataError, "timezone"):
            scan_fresh_csv(naive, config=config())

        other = self.write_csv(
            "other.csv",
            [row(1, bounds.start_utc + timedelta(seconds=1), symbol="XAUAUD")],
        )
        with self.assertRaisesRegex(FreshDataError, "expected"):
            scan_fresh_csv(other, config=config())


if __name__ == "__main__":
    unittest.main()
