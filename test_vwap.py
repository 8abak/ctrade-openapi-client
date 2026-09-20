from datetime import datetime, timedelta, timezone

import pytest

from datavis.app import _is_sydney_day_gap, _sydney_vwap_points, _sydney_vwap_segments


def test_sydney_day_gap_uses_actual_gap_and_accepts_dst_clock() -> None:
    before = datetime(2026, 9, 15, 20, 59, tzinfo=timezone.utc)
    after = before + timedelta(hours=1)

    assert _is_sydney_day_gap(before, after)
    assert not _is_sydney_day_gap(before, before + timedelta(minutes=10))


def test_tick_weighted_vwap_and_three_deviations() -> None:
    rows = [
        {"bucket": datetime(2026, 9, 15, 21, 0, tzinfo=timezone.utc), "tick_id": 10, "tick_count": 2, "price_sum": 200.0, "square_sum": 20000.0},
        {"bucket": datetime(2026, 9, 15, 21, 1, tzinfo=timezone.utc), "tick_id": 20, "tick_count": 2, "price_sum": 204.0, "square_sum": 20808.0},
    ]

    points = _sydney_vwap_points(rows)

    assert points[-1]["vwap"] == pytest.approx(101.0)
    assert points[-1]["upper1"] == pytest.approx(102.0)
    assert points[-1]["lower1"] == pytest.approx(100.0)
    assert points[-1]["upper3"] == pytest.approx(104.0)
    assert points[-1]["lower3"] == pytest.approx(98.0)


def test_mini_map_vwap_keeps_both_broker_days_and_resets_at_sydney_eight() -> None:
    rows = [
        {"bucket": datetime(2026, 9, 15, 21, 59, tzinfo=timezone.utc), "tick_id": 10,
         "tick_count": 2, "price_sum": 200.0, "square_sum": 20000.0},
        {"bucket": datetime(2026, 9, 15, 22, 0, tzinfo=timezone.utc), "tick_id": 20,
         "tick_count": 2, "price_sum": 220.0, "square_sum": 24200.0},
    ]
    points = _sydney_vwap_segments(rows)
    assert len(points) == 2
    assert points[0]["vwap"] == 100
    assert points[1]["vwap"] == 110
    assert points[0]["sessionStartMs"] != points[1]["sessionStartMs"]
    assert points[1]["upper3"] == 110
