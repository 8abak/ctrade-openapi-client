from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_db_source import (
    BROKER_SESSION_QUERY,
    FreshDbSourceError,
    scan_fresh_db_session,
)
from datavis.research.fresh_sessions import SessionAuditConfig, broker_session_bounds


UTC = timezone.utc
SYDNEY = ZoneInfo("Australia/Sydney")


def source_config(**changes: Any) -> FreshDataConfig:
    values: dict[str, Any] = {
        "session_audit": SessionAuditConfig(
            open_tolerance_seconds=30,
            close_tolerance_seconds=30,
            friday_close_tolerance_seconds=360,
            unexpected_gap_seconds=100_000,
        ),
        "expected_symbol": "XAUUSD",
        "chunk_rows": 2,
    }
    values.update(changes)
    return FreshDataConfig(**values)


def quote(
    tick_id: int,
    timestamp: datetime,
    bid: Any = 100.0,
    ask: Any = 100.2,
    symbol: str = "XAUUSD",
) -> tuple[Any, ...]:
    return (tick_id, symbol, timestamp, bid, ask)


class FakeNamedCursor:
    """Minimal named-cursor fake that makes fetch batching observable."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.position = 0
        self.itersize = 0
        self.arraysize = 0
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_sizes: list[int] = []
        self.closed = False

    def execute(self, query: str, parameters) -> None:
        self.execute_calls.append((query, tuple(parameters)))

    def fetchmany(self, size: int):
        self.fetch_sizes.append(size)
        selected = self.rows[self.position : self.position + size]
        self.position += len(selected)
        return selected

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """DB-API-shaped fake with PostgreSQL's named-cursor extension."""

    def __init__(
        self, rows: list[tuple[Any, ...]], *, autocommit: bool = False
    ) -> None:
        self.autocommit = autocommit
        self.cursor_instance = FakeNamedCursor(rows)
        self.cursor_names: list[str] = []

    def cursor(self, *, name: str) -> FakeNamedCursor:
        self.cursor_names.append(name)
        return self.cursor_instance


class FreshDbSourceTests(unittest.TestCase):
    def test_exact_read_only_query_bounds_named_cursor_and_fetch_batches(self):
        anchor = date(2026, 3, 9)
        bounds = broker_session_bounds(anchor)
        rows = [
            quote(10 + index, bounds.start_utc + timedelta(seconds=index + 1))
            for index in range(5)
        ]
        connection = FakeConnection(rows)

        inventory = scan_fresh_db_session(
            connection,
            anchor,
            config=source_config(),
            cursor_name="fresh_session_test",
        )

        self.assertEqual(connection.cursor_names, ["fresh_session_test"])
        cursor = connection.cursor_instance
        self.assertTrue(cursor.closed)
        self.assertEqual(cursor.itersize, 2)
        self.assertEqual(cursor.arraysize, 2)
        self.assertEqual(cursor.fetch_sizes, [2, 2, 2, 2])
        self.assertEqual(len(cursor.execute_calls), 1)
        sql, parameters = cursor.execute_calls[0]
        self.assertEqual(sql, BROKER_SESSION_QUERY)
        self.assertEqual(
            " ".join(sql.split()),
            "SELECT id, symbol, timestamp, bid, ask FROM public.ticks "
            "WHERE symbol = %s AND timestamp >= %s AND timestamp < %s "
            "ORDER BY timestamp ASC, id ASC",
        )
        self.assertEqual(parameters, ("XAUUSD", bounds.start_utc, bounds.end_utc))
        self.assertEqual(parameters[1].tzinfo, UTC)
        self.assertEqual(parameters[2].tzinfo, UTC)
        self.assertEqual(inventory.raw_row_count, 5)
        self.assertEqual(inventory.normalized_quote_count, 5)

    def test_repeated_quotes_are_retained_as_volume_events_across_chunks(self):
        bounds = broker_session_bounds(date(2026, 1, 15))
        moment = (bounds.start_utc + timedelta(seconds=1)).astimezone(SYDNEY)
        connection = FakeConnection(
            [
                quote(10, moment, Decimal("100.0"), Decimal("100.2")),
                quote(11, moment.astimezone(UTC), 100.0, 100.2),
                quote(12, moment, 100.1, 100.3),
                quote(13, moment, 100.0, 100.2),
                quote(14, moment + timedelta(seconds=1), 100.4, 100.4),
            ]
        )
        emitted = []

        inventory = scan_fresh_db_session(
            connection,
            bounds.anchor,
            config=source_config(chunk_rows=1),
            on_tick=emitted.append,
            cursor_name="dedup_test",
        )

        self.assertEqual([item.tick.id for item in emitted], [10, 11, 12, 13, 14])
        self.assertEqual(inventory.raw_row_count, 5)
        self.assertEqual(inventory.valid_quote_count, 5)
        self.assertEqual(inventory.normalized_quote_count, 5)
        self.assertEqual(inventory.duplicate_quote_count, 2)
        self.assertEqual(inventory.duplicate_group_count, 1)
        self.assertEqual(inventory.locked_quote_count, 1)
        self.assertEqual(emitted[0].assignment.anchor, bounds.anchor)
        self.assertEqual(emitted[0].assignment.status, "session")
        self.assertEqual(emitted[0].tick.timestamp.utcoffset(), timedelta(hours=11))

    def test_boundary_and_gap_qc_matches_session_audit_rules(self):
        anchor = date(2026, 1, 15)
        bounds = broker_session_bounds(anchor)
        connection = FakeConnection(
            [
                quote(1, bounds.start_utc + timedelta(seconds=10)),
                quote(2, bounds.end_utc - timedelta(seconds=10)),
            ]
        )
        settings = source_config(
            session_audit=SessionAuditConfig(
                open_tolerance_seconds=20,
                close_tolerance_seconds=20,
                friday_close_tolerance_seconds=360,
                unexpected_gap_seconds=60,
            )
        )

        inventory = scan_fresh_db_session(
            connection,
            anchor,
            config=settings,
            cursor_name="gap_test",
        )

        self.assertTrue(inventory.boundary_complete)
        self.assertEqual(inventory.open_delay_seconds, 10.0)
        self.assertEqual(inventory.close_lead_seconds, 10.0)
        self.assertEqual(inventory.unexpected_gap_count, 1)
        self.assertTrue(inventory.audit.has_unexpected_outage)
        self.assertFalse(inventory.audit.is_complete)
        self.assertEqual(inventory.coverage_status, "ineligible")

    def test_friday_uses_explicit_friday_close_tolerance(self):
        anchor = date(2026, 1, 16)
        bounds = broker_session_bounds(anchor)
        connection = FakeConnection(
            [
                quote(1, bounds.start_utc + timedelta(seconds=10)),
                quote(2, bounds.end_utc - timedelta(seconds=300)),
            ]
        )

        inventory = scan_fresh_db_session(
            connection,
            anchor,
            config=source_config(),
            cursor_name="friday_test",
        )

        self.assertTrue(inventory.close_boundary_covered)
        self.assertTrue(inventory.is_complete)
        self.assertEqual(inventory.coverage_status, "complete")

    def test_empty_session_returns_empty_audit_without_extra_query(self):
        connection = FakeConnection([])

        inventory = scan_fresh_db_session(
            connection,
            date(2026, 1, 15),
            config=source_config(),
            cursor_name="empty_test",
        )

        self.assertEqual(inventory.raw_row_count, 0)
        self.assertEqual(inventory.normalized_quote_count, 0)
        self.assertEqual(inventory.coverage_status, "empty")
        self.assertIsNone(inventory.first_timestamp_utc)
        self.assertFalse(inventory.boundary_complete)
        self.assertEqual(len(connection.cursor_instance.execute_calls), 1)

    def test_driver_row_outside_lower_or_upper_bound_is_fatal(self):
        anchor = date(2026, 1, 15)
        bounds = broker_session_bounds(anchor)
        for label, timestamp in (
            ("lower", bounds.start_utc - timedelta(microseconds=1)),
            ("upper", bounds.end_utc),
        ):
            with self.subTest(label=label):
                connection = FakeConnection([quote(1, timestamp)])
                with self.assertRaisesRegex(FreshDbSourceError, "outside requested"):
                    scan_fresh_db_session(
                        connection,
                        anchor,
                        config=source_config(),
                        cursor_name=f"outside_{label}",
                    )
                self.assertTrue(connection.cursor_instance.closed)

    def test_symbol_timezone_price_and_shape_validation_is_fatal(self):
        anchor = date(2026, 1, 15)
        bounds = broker_session_bounds(anchor)
        moment = bounds.start_utc + timedelta(seconds=1)
        cases = (
            ("symbol", quote(1, moment, symbol="EURUSD"), "symbol"),
            ("timezone", quote(1, moment.replace(tzinfo=None)), "timezone-aware"),
            ("infinite", quote(1, moment, 100.0, float("inf")), "finite"),
            ("string_price", quote(1, moment, "100.0", 100.2), "numeric"),
            ("shape", (1, "XAUUSD", moment, 100.0), "five selected"),
        )
        for label, bad_row, message in cases:
            with self.subTest(label=label):
                connection = FakeConnection([bad_row])
                with self.assertRaisesRegex(FreshDbSourceError, message):
                    scan_fresh_db_session(
                        connection,
                        anchor,
                        config=source_config(),
                        cursor_name=f"bad_{label}",
                    )
                self.assertTrue(connection.cursor_instance.closed)

    def test_crossed_quote_quarantines_the_entire_session(self):
        anchor = date(2026, 1, 15)
        bounds = broker_session_bounds(anchor)
        connection = FakeConnection(
            [
                quote(1, bounds.start_utc + timedelta(seconds=10)),
                quote(
                    2,
                    bounds.start_utc + timedelta(seconds=20),
                    bid=101.0,
                    ask=100.0,
                ),
                quote(3, bounds.end_utc - timedelta(seconds=10)),
            ]
        )
        emitted = []

        inventory = scan_fresh_db_session(
            connection,
            anchor,
            config=source_config(),
            on_tick=emitted.append,
            cursor_name="crossed_quote_test",
        )

        self.assertEqual([item.tick.id for item in emitted], [1, 3])
        self.assertEqual(inventory.raw_row_count, 3)
        self.assertEqual(inventory.valid_quote_count, 2)
        self.assertEqual(inventory.normalized_quote_count, 2)
        self.assertEqual(inventory.invalid_quote_count, 1)
        self.assertEqual(len(inventory.invalid_quote_samples), 1)
        sample = inventory.invalid_quote_samples[0]
        self.assertEqual(sample.tick_id, 2)
        self.assertEqual(sample.reason, "crossed quote")
        self.assertEqual(inventory.coverage_status, "ineligible")
        self.assertFalse(inventory.is_complete)

    def test_strict_order_and_unique_ids_are_checked_across_fetch_batches(self):
        anchor = date(2026, 1, 15)
        bounds = broker_session_bounds(anchor)
        moment = bounds.start_utc + timedelta(seconds=1)
        cases = (
            (
                "order",
                [quote(2, moment), quote(1, moment, 100.1, 100.3)],
                "strictly ordered",
            ),
            (
                "unique",
                [quote(1, moment), quote(1, moment + timedelta(seconds=1))],
                "duplicate tick id",
            ),
            (
                "unique_crossed",
                [
                    quote(1, moment),
                    quote(
                        1,
                        moment + timedelta(seconds=1),
                        bid=101.0,
                        ask=100.0,
                    ),
                ],
                "duplicate tick id",
            ),
        )
        for label, rows, message in cases:
            with self.subTest(label=label):
                connection = FakeConnection(rows)
                with self.assertRaisesRegex(FreshDbSourceError, message):
                    scan_fresh_db_session(
                        connection,
                        anchor,
                        config=source_config(chunk_rows=1),
                        cursor_name=f"bad_{label}",
                    )

    def test_requires_symbol_transaction_and_safe_cursor_name_before_query(self):
        anchor = date(2026, 1, 15)
        connection = FakeConnection([], autocommit=True)
        with self.assertRaisesRegex(FreshDbSourceError, "autocommit"):
            scan_fresh_db_session(
                connection,
                anchor,
                config=source_config(),
                cursor_name="transaction_test",
            )
        self.assertEqual(connection.cursor_names, [])

        no_symbol = FakeConnection([])
        with self.assertRaisesRegex(ValueError, "expected_symbol"):
            scan_fresh_db_session(
                no_symbol,
                anchor,
                config=source_config(expected_symbol=None),
                cursor_name="symbol_test",
            )
        self.assertEqual(no_symbol.cursor_names, [])

        unsafe_name = FakeConnection([])
        with self.assertRaisesRegex(ValueError, "cursor_name"):
            scan_fresh_db_session(
                unsafe_name,
                anchor,
                config=source_config(),
                cursor_name="unsafe;drop",
            )
        self.assertEqual(unsafe_name.cursor_names, [])


if __name__ == "__main__":
    unittest.main()
