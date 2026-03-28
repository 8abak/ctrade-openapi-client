from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from datavis.envelope import EnvelopeConfig, compute_envelope_rows


def sample_ticks(count: int = 180) -> List[Dict[str, Any]]:
    base = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    rows: List[Dict[str, Any]] = []
    for index in range(count):
        mid = 3015.0 + (index * 0.06) + math.sin(index / 9.0) * 2.4 + math.cos(index / 17.0) * 1.1 + ((index % 11) - 5) * 0.08
        rows.append(
            {
                "id": index + 1,
                "symbol": "XAUUSD",
                "timestamp": base + timedelta(seconds=index + 1),
                "bid": round(mid - 0.10, 5),
                "ask": round(mid + 0.10, 5),
                "mid": round(mid, 5),
                "spread": 0.20,
                "price": round(mid, 5),
            }
        )
    return rows


def reference_envelope(rows: List[Dict[str, Any]], config: EnvelopeConfig) -> List[Dict[str, Any]]:
    config = config.normalized()
    prices = [float(row[config.source]) for row in rows]
    weights = [math.exp(-((float(index) * float(index)) / (config.bandwidth * config.bandwidth * 2.0))) for index in range(config.length)]
    denominator = sum(weights)
    error_length = max(1, config.length - 1)
    abs_errors: List[float] = []
    results: List[Dict[str, Any]] = []

    for index, price in enumerate(prices):
        basis = None
        mae = None
        upper = None
        lower = None
        if (index + 1) >= config.length:
            history = prices[(index - config.length + 1):(index + 1)]
            basis = sum(float(value) * float(weight) for value, weight in zip(reversed(history), weights)) / float(denominator)
            abs_errors.append(abs(price - basis))
            if len(abs_errors) >= error_length:
                recent_errors = abs_errors[-error_length:]
                mae = sum(recent_errors) / float(error_length)
                band = mae * config.mult
                upper = basis + band
                lower = basis - band

        results.append(
            {
                "tickid": rows[index]["id"],
                "price": price,
                "basis": basis,
                "mae": mae,
                "upper": upper,
                "lower": lower,
            }
        )

    return results


class EnvelopeValidationTest(unittest.TestCase):
    def assert_optional_close(self, actual: Optional[float], expected: Optional[float], field: str) -> None:
        if expected is None:
            self.assertIsNone(actual, field)
            return
        self.assertIsNotNone(actual, field)
        self.assertTrue(math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=1e-9), field)

    def test_reference_endpoint_matches_manual_formula(self) -> None:
        config = EnvelopeConfig(source="mid", length=12, bandwidth=3.5, mult=2.2)
        rows = sample_ticks(80)
        actual, _ = compute_envelope_rows(rows, config)
        expected = reference_envelope(rows, config)

        for actual_row, expected_row in zip(actual, expected):
            self.assertEqual(actual_row["tickid"], expected_row["tickid"])
            self.assertTrue(math.isclose(actual_row["price"], expected_row["price"], rel_tol=0.0, abs_tol=1e-9))
            for field in ("basis", "mae", "upper", "lower"):
                self.assert_optional_close(actual_row[field], expected_row[field], field)

    def test_causal_output_is_unchanged_by_future_rows(self) -> None:
        config = EnvelopeConfig(source="mid", length=20, bandwidth=8.0, mult=3.0)
        rows = sample_ticks(160)
        prefix_rows = rows[:120]
        prefix_actual, _ = compute_envelope_rows(prefix_rows, config)
        full_actual, _ = compute_envelope_rows(rows, config)

        for prefix_row, full_row in zip(prefix_actual, full_actual[: len(prefix_rows)]):
            self.assertEqual(prefix_row["tickid"], full_row["tickid"])
            for field in ("basis", "mae", "upper", "lower"):
                self.assert_optional_close(prefix_row[field], full_row[field], field)

    def test_chunked_processing_matches_single_pass(self) -> None:
        config = EnvelopeConfig(source="mid", length=14, bandwidth=4.0, mult=1.8)
        rows = sample_ticks(90)
        full_actual, _ = compute_envelope_rows(rows, config)

        chunked_rows: List[Dict[str, Any]] = []
        state = None
        for index in range(0, len(rows), 7):
            segment = rows[index : index + 7]
            actual, state = compute_envelope_rows(segment, config, state=state)
            chunked_rows.extend(actual)

        self.assertEqual(len(chunked_rows), len(full_actual))
        for chunked_row, full_row in zip(chunked_rows, full_actual):
            self.assertEqual(chunked_row["tickid"], full_row["tickid"])
            for field in ("basis", "mae", "upper", "lower"):
                self.assert_optional_close(chunked_row[field], full_row[field], field)

    def test_resume_from_saved_state_matches_single_pass(self) -> None:
        config = EnvelopeConfig(source="mid", length=18, bandwidth=5.5, mult=2.0)
        rows = sample_ticks(140)
        full_actual, _ = compute_envelope_rows(rows, config)

        first_half, state = compute_envelope_rows(rows[:73], config)
        second_half, _ = compute_envelope_rows(rows[73:], config, state=state)
        resumed_rows = first_half + second_half

        self.assertEqual(len(resumed_rows), len(full_actual))
        for resumed_row, full_row in zip(resumed_rows, full_actual):
            self.assertEqual(resumed_row["tickid"], full_row["tickid"])
            for field in ("basis", "mae", "upper", "lower"):
                self.assert_optional_close(resumed_row[field], full_row[field], field)


if __name__ == "__main__":
    unittest.main()
