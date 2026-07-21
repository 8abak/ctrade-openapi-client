from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from datavis.research.fresh_sessions import (
    BrokerTick,
    SessionAuditConfig,
    assign_broker_timestamp,
    audit_session_completeness,
    broker_session_bounds,
    normalize_broker_ticks,
    partition_broker_ticks,
    validate_ordered_broker_ticks,
)


UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")


def tick(
    tick_id: int,
    timestamp: datetime,
    bid: float = 100.0,
    ask: float = 100.2,
    symbol: str = "XAUUSD",
) -> BrokerTick:
    return BrokerTick(
        id=tick_id,
        symbol=symbol,
        timestamp=timestamp,
        bid=bid,
        ask=ask,
    )


class FreshSessionTests(unittest.TestCase):
    def test_january_bounds_are_new_york_schedule_converted_to_utc_and_sydney(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        self.assertEqual(
            bounds.start_new_york,
            datetime(2026, 1, 14, 18, 0, tzinfo=NEW_YORK),
        )
        self.assertEqual(
            bounds.end_new_york,
            datetime(2026, 1, 15, 17, 0, tzinfo=NEW_YORK),
        )
        self.assertEqual(bounds.start_utc, datetime(2026, 1, 14, 23, 0, tzinfo=UTC))
        self.assertEqual(bounds.end_utc, datetime(2026, 1, 15, 22, 0, tzinfo=UTC))
        self.assertEqual(
            bounds.start_sydney.isoformat(), "2026-01-15T10:00:00+11:00"
        )
        self.assertEqual(bounds.end_sydney.isoformat(), "2026-01-16T09:00:00+11:00")
        self.assertEqual(bounds.duration_seconds, 23 * 3_600)
        self.assertTrue(bounds.contains(bounds.start_utc))
        self.assertFalse(bounds.contains(bounds.end_utc))

    def test_march_us_dst_crossover_moves_utc_and_sydney_session_hour(self):
        before = broker_session_bounds(date(2026, 3, 6))
        after = broker_session_bounds(date(2026, 3, 9))

        self.assertEqual(before.start_utc, datetime(2026, 3, 5, 23, 0, tzinfo=UTC))
        self.assertEqual(after.start_utc, datetime(2026, 3, 8, 22, 0, tzinfo=UTC))
        self.assertEqual(before.start_sydney.isoformat(), "2026-03-06T10:00:00+11:00")
        self.assertEqual(after.start_sydney.isoformat(), "2026-03-09T09:00:00+11:00")
        self.assertEqual(before.end_utc, datetime(2026, 3, 6, 22, 0, tzinfo=UTC))
        self.assertEqual(after.end_utc, datetime(2026, 3, 9, 21, 0, tzinfo=UTC))

        assignment = assign_broker_timestamp(after.start_utc)
        self.assertEqual(assignment.status, "session")
        self.assertEqual(assignment.anchor, date(2026, 3, 9))
        self.assertEqual(assignment.timestamp_sydney.hour, 9)

    def test_april_sydney_dst_crossover_is_independent_of_new_york(self):
        before = broker_session_bounds(date(2026, 4, 3))
        after = broker_session_bounds(date(2026, 4, 6))

        self.assertEqual(before.start_utc, datetime(2026, 4, 2, 22, 0, tzinfo=UTC))
        self.assertEqual(after.start_utc, datetime(2026, 4, 5, 22, 0, tzinfo=UTC))
        self.assertEqual(before.start_sydney.isoformat(), "2026-04-03T09:00:00+11:00")
        self.assertEqual(after.start_sydney.isoformat(), "2026-04-06T08:00:00+10:00")
        self.assertEqual(after.end_sydney.isoformat(), "2026-04-07T07:00:00+10:00")

    def test_assignment_distinguishes_session_maintenance_and_weekend(self):
        monday = date(2026, 1, 12)
        session = assign_broker_timestamp(
            datetime(2026, 1, 12, 16, 59, 59, tzinfo=NEW_YORK)
        )
        self.assertEqual(session.status, "session")
        self.assertEqual(session.anchor, monday)

        maintenance = assign_broker_timestamp(
            datetime(2026, 1, 12, 17, 30, tzinfo=NEW_YORK)
        )
        self.assertEqual(maintenance.status, "maintenance")
        self.assertIsNone(maintenance.anchor)
        next_session = assign_broker_timestamp(
            datetime(2026, 1, 12, 18, 0, tzinfo=NEW_YORK)
        )
        self.assertEqual(next_session.status, "session")
        self.assertEqual(next_session.anchor, date(2026, 1, 13))

        friday_close = assign_broker_timestamp(
            datetime(2026, 1, 16, 17, 30, tzinfo=NEW_YORK)
        )
        saturday = assign_broker_timestamp(
            datetime(2026, 1, 17, 12, 0, tzinfo=NEW_YORK)
        )
        sunday_preopen = assign_broker_timestamp(
            datetime(2026, 1, 18, 17, 59, 59, tzinfo=NEW_YORK)
        )
        for assignment in (friday_close, saturday, sunday_preopen):
            self.assertEqual(assignment.status, "no_session")
            self.assertIsNone(assignment.anchor)
        sunday_open = assign_broker_timestamp(
            datetime(2026, 1, 18, 18, 0, tzinfo=NEW_YORK)
        )
        self.assertEqual(sunday_open.anchor, date(2026, 1, 19))

    def test_exact_duplicate_collapse_preserves_quote_transitions_and_lowest_id(self):
        moment = datetime(2026, 1, 14, 23, 0, tzinfo=UTC)
        raw = [
            tick(10, moment, 100.0, 100.2),
            tick(11, moment, 100.0, 100.2),
            tick(12, moment, 100.1, 100.3),
            tick(13, moment, 100.0, 100.2),
            tick(14, moment, 100.0, 100.2, symbol="XAUAUD"),
            # A later timestamp may legitimately have a lower collector id;
            # lexicographic (timestamp, id) order is still increasing.
            tick(1, moment + timedelta(milliseconds=1), 100.0, 100.2),
        ]
        result = normalize_broker_ticks(raw)
        self.assertEqual(result.input_count, 6)
        self.assertEqual(result.retained_count, 4)
        self.assertEqual(result.duplicate_count, 2)
        self.assertEqual(result.duplicate_group_count, 1)
        self.assertEqual(result.dropped_ids, (11, 13))
        self.assertEqual([item.id for item in result.ticks], [10, 12, 14, 1])
        self.assertEqual(result.ticks[0].id, 10)
        self.assertEqual(result.ticks[1].bid, 100.1)
        self.assertEqual(result.ticks[2].symbol, "XAUAUD")

    def test_disordered_input_is_rejected_without_sorting(self):
        first = datetime(2026, 1, 14, 23, 0, tzinfo=UTC)
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            validate_ordered_broker_ticks(
                [tick(1, first + timedelta(seconds=1)), tick(2, first)]
            )
        with self.assertRaisesRegex(ValueError, "disorder"):
            normalize_broker_ticks([tick(2, first), tick(1, first)])
        with self.assertRaisesRegex(ValueError, "disorder"):
            normalize_broker_ticks([tick(1, first), tick(1, first)])

    def test_partition_reports_session_maintenance_and_no_session_separately(self):
        points = [
            tick(1, datetime(2026, 1, 12, 21, 59, tzinfo=UTC)),
            tick(2, datetime(2026, 1, 12, 22, 30, tzinfo=UTC)),
            tick(3, datetime(2026, 1, 12, 23, 0, tzinfo=UTC)),
            tick(4, datetime(2026, 1, 17, 18, 0, tzinfo=UTC)),
        ]
        partition = partition_broker_ticks(points)
        self.assertEqual(
            partition.session_anchors, (date(2026, 1, 12), date(2026, 1, 13))
        )
        self.assertEqual([item.id for item in partition.maintenance_ticks], [2])
        self.assertEqual([item.id for item in partition.no_session_ticks], [4])
        self.assertEqual(
            [item.id for item in partition.ticks_for_anchor(date(2026, 1, 13))],
            [3],
        )

    def test_completeness_separates_boundaries_from_unexpected_outages(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        duration = int(bounds.duration_seconds)
        offsets = [20] + [hour * 3_600 for hour in range(1, 23)] + [duration - 20]
        offsets.remove(11 * 3_600)
        points = [
            tick(
                index + 1,
                bounds.start_utc + timedelta(seconds=offset),
                100.0 + index / 100.0,
                100.2 + index / 100.0,
            )
            for index, offset in enumerate(offsets)
        ]

        with_outage = audit_session_completeness(
            points,
            bounds.anchor,
            config=SessionAuditConfig(
                open_tolerance_seconds=30,
                close_tolerance_seconds=30,
                friday_close_tolerance_seconds=300,
                unexpected_gap_seconds=4_000,
            ),
        )
        self.assertEqual(with_outage.open_delay_seconds, 20.0)
        self.assertEqual(with_outage.close_lead_seconds, 20.0)
        self.assertTrue(with_outage.boundary_complete)
        self.assertEqual(with_outage.unexpected_gap_count, 1)
        self.assertEqual(with_outage.longest_unexpected_gap_seconds, 7_200.0)
        self.assertTrue(with_outage.has_unexpected_outage)
        self.assertFalse(with_outage.is_complete)

        no_outage = audit_session_completeness(
            points,
            bounds.anchor,
            config=SessionAuditConfig(
                open_tolerance_seconds=30,
                close_tolerance_seconds=30,
                friday_close_tolerance_seconds=300,
                unexpected_gap_seconds=8_000,
            ),
        )
        self.assertTrue(no_outage.boundary_complete)
        self.assertFalse(no_outage.has_unexpected_outage)
        self.assertTrue(no_outage.is_complete)

        tight_boundaries = audit_session_completeness(
            points,
            bounds.anchor,
            config=SessionAuditConfig(
                open_tolerance_seconds=10,
                close_tolerance_seconds=10,
                friday_close_tolerance_seconds=300,
                unexpected_gap_seconds=8_000,
            ),
        )
        self.assertFalse(tight_boundaries.boundary_complete)
        self.assertFalse(tight_boundaries.has_unexpected_outage)
        self.assertFalse(tight_boundaries.is_complete)

    def test_outside_ticks_and_empty_sessions_cannot_be_complete(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        points = [
            tick(1, bounds.start_utc + timedelta(seconds=1)),
            tick(2, bounds.end_utc - timedelta(seconds=1)),
            tick(3, bounds.end_utc + timedelta(seconds=1)),
        ]
        audit = audit_session_completeness(
            points,
            bounds.anchor,
            config=SessionAuditConfig(
                open_tolerance_seconds=2,
                close_tolerance_seconds=2,
                friday_close_tolerance_seconds=300,
                unexpected_gap_seconds=100_000,
            ),
        )
        self.assertTrue(audit.boundary_complete)
        self.assertEqual(audit.outside_requested_session_count, 1)
        self.assertEqual(audit.outside_requested_session_ids, (3,))
        self.assertFalse(audit.is_complete)

        empty = audit_session_completeness(
            [],
            bounds.anchor,
            config=SessionAuditConfig(2, 2, 300, 100_000),
        )
        self.assertIsNone(empty.symbol)
        self.assertFalse(empty.boundary_complete)
        self.assertFalse(empty.is_complete)

    def test_weekend_anchor_and_invalid_tolerances_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Monday through Friday"):
            broker_session_bounds(date(2026, 1, 17))
        with self.assertRaises(ValueError):
            SessionAuditConfig(-1, 1, 1, 1)
        with self.assertRaises(ValueError):
            SessionAuditConfig(1, 1, 1, 0)


if __name__ == "__main__":
    unittest.main()
