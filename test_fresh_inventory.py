from __future__ import annotations

import copy
import unittest
from datetime import date, datetime, timedelta, timezone

from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_db_source import FreshDbSessionInventory
from datavis.research.fresh_inventory import (
    FreshScannedSession,
    build_fresh_inventory_manifests,
    weekday_anchors,
)
from datavis.research.fresh_sessions import (
    SessionAuditConfig,
    SessionCompletenessAudit,
    broker_session_bounds,
)


AUDIT_CONFIG = SessionAuditConfig(
    open_tolerance_seconds=120,
    close_tolerance_seconds=120,
    friday_close_tolerance_seconds=600,
    unexpected_gap_seconds=300,
)
DATA_CONFIG = FreshDataConfig(
    session_audit=AUDIT_CONFIG,
    expected_symbol="XAUUSD",
    chunk_rows=10_000,
    expected_anchors=(),
    maximum_issue_samples=20,
)


def scanned(anchor: date, *, complete: bool = True) -> FreshScannedSession:
    bounds = broker_session_bounds(anchor)
    first = bounds.start_utc + timedelta(seconds=60) if complete else None
    last = bounds.end_utc - timedelta(seconds=60) if complete else None
    audit = SessionCompletenessAudit(
        symbol="XAUUSD" if complete else None,
        bounds=bounds,
        raw_tick_count=100 if complete else 0,
        normalized_tick_count=100 if complete else 0,
        duplicate_count=0,
        in_session_tick_count=100 if complete else 0,
        outside_requested_session_count=0,
        outside_requested_session_ids=(),
        first_timestamp_utc=first,
        last_timestamp_utc=last,
        open_delay_seconds=60 if complete else None,
        close_lead_seconds=60 if complete else None,
        open_boundary_covered=complete,
        close_boundary_covered=complete,
        boundary_complete=complete,
        unexpected_gaps=(),
        longest_unexpected_gap_seconds=None,
        total_unexpected_gap_seconds=0.0,
        has_unexpected_outage=False,
        is_complete=complete,
    )
    inventory = FreshDbSessionInventory(
        anchor=anchor,
        symbol="XAUUSD",
        bounds=bounds,
        cursor_name=f"session_{anchor:%Y%m%d}",
        fetch_batch_rows=10_000,
        raw_row_count=100 if complete else 0,
        valid_quote_count=100 if complete else 0,
        normalized_quote_count=100 if complete else 0,
        duplicate_quote_count=0,
        duplicate_group_count=0,
        invalid_quote_count=0,
        invalid_quote_samples=(),
        locked_quote_count=0,
        audit=audit,
    )
    return FreshScannedSession(inventory, ("a" if complete else "e") * 64)


class FreshInventoryTests(unittest.TestCase):
    def test_manifest_is_deterministic_and_contains_no_outcome_language(self):
        items = [scanned(date(2026, 1, 2)), scanned(date(2026, 1, 5), complete=False)]
        first = build_fresh_inventory_manifests(items, config=DATA_CONFIG)
        second = build_fresh_inventory_manifests(copy.deepcopy(items), config=DATA_CONFIG)
        self.assertEqual(first, second)
        inventory, corpus, eligible, exclusions = first
        self.assertEqual(eligible, ["2026-01-02"])
        self.assertEqual(exclusions[0]["sessionAnchor"], "2026-01-05")
        self.assertIn("no valid", exclusions[0]["reason"])
        self.assertEqual(inventory["eligibleSessionCount"], 1)
        self.assertEqual(corpus["sessions"][0]["normalizedQuoteCount"], 100)
        text = repr((inventory, corpus)).lower()
        for forbidden in ("pnl", "return", "barrier", "profit", "signal"):
            self.assertNotIn(forbidden, text)

    def test_nonchronological_and_duplicate_anchors_fail(self):
        item = scanned(date(2026, 1, 2))
        with self.assertRaisesRegex(ValueError, "chronological"):
            build_fresh_inventory_manifests([item, item], config=DATA_CONFIG)

    def test_weekday_anchor_range_is_inclusive_and_validated(self):
        anchors = weekday_anchors(date(2026, 1, 1), date(2026, 1, 6))
        self.assertEqual(
            anchors,
            (date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)),
        )
        with self.assertRaises(ValueError):
            weekday_anchors(date(2026, 1, 2), date(2026, 1, 1))
        with self.assertRaises(TypeError):
            weekday_anchors(datetime(2026, 1, 1, tzinfo=timezone.utc), date(2026, 1, 2))


if __name__ == "__main__":
    unittest.main()
