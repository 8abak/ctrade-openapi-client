from datetime import datetime, timedelta, timezone

import pytest

import datavis.app as app
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


def test_monday_mini_map_uses_recent_market_ticks_across_weekend(monkeypatch) -> None:
    latest = datetime(2026, 9, 20, 22, 23, tzinfo=timezone.utc)  # Monday 08:23 Sydney
    friday = datetime(2026, 9, 17, 22, 20, tzinfo=timezone.utc)  # Friday 08:20 Sydney
    minute_times = [friday + timedelta(minutes=1), friday + timedelta(hours=15),
                    latest - timedelta(minutes=22), latest]
    minute_rows = [
        {"bucket": stamp.replace(second=0), "last_timestamp": stamp, "tick_id": index,
         "close": 100 + index, "tick_count": 2, "price_sum": 200 + 2 * index,
         "square_sum": 2 * (100 + index) ** 2}
        for index, stamp in enumerate(minute_times, 1)
    ]

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, sql, params):
            if "ORDER BY id DESC" in sql and "OFFSET" not in sql:
                self.rows = [{"id": 900000, "timestamp": latest}]
            elif "OFFSET" in sql:
                assert params[2] == app.ACD_MAP_TARGET_TICKS - 1
                self.rows = [{"timestamp": friday}]
            elif "GROUP BY date_trunc('minute'" in sql:
                self.rows = minute_rows
            elif "MIN(bid) AS opening_low" in sql:
                day = params[1].astimezone(app.ACD_NEW_YORK).date().isoformat()
                self.rows = ([{"opening_low": 95, "opening_high": 105, "start_tick_id": 1,
                               "end_tick_id": 2, "tick_count": 2}]
                             if day in {"2026-09-17", "2026-09-18"} else [{}])
            else:
                raise AssertionError(sql)
        def fetchone(self): return self.rows[0] if self.rows else None
        def fetchall(self): return self.rows

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def cursor(self, **_): return Cursor()

    monkeypatch.setattr(app, "db_connection", lambda **_: Connection())
    payload = app.load_acd_map_payload(2)
    assert payload["windowMode"] == "recent-market-ticks"
    assert payload["windowStartMs"] == app.dt_to_ms(friday)
    assert len(payload["points"]) == 4
    assert len(payload["acds"]) == 2
    assert payload["acds"][0]["activeEndMs"] == payload["acds"][1]["openingStartMs"]
    assert len({point["sessionStartMs"] for point in payload["vwapPoints"]}) == 2
