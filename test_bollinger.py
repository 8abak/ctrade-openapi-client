from datetime import datetime, timedelta, timezone

import pytest

from datavis.bollinger import build_bollinger_payload


def test_bollinger_uses_twenty_candle_closes_and_two_population_deviations():
    start = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
    rows = [
        {"bucket": start + timedelta(minutes=index), "tickid": index + 1, "close": float(index + 1)}
        for index in range(25)
    ]
    payload = build_bollinger_payload(rows, visible_start_id=1)

    assert payload["period"] == 20
    assert payload["deviations"] == 2.0
    assert len(payload["oneMinute"]) == 6
    first = payload["oneMinute"][0]
    assert first["tickId"] == 20
    assert first["middle"] == pytest.approx(10.5)
    assert first["upper"] == pytest.approx(10.5 + 2 * (33.25 ** 0.5))
    assert first["lower"] == pytest.approx(10.5 - 2 * (33.25 ** 0.5))


def test_five_minute_band_uses_last_close_in_each_five_minute_candle():
    start = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
    rows = [
        {"bucket": start + timedelta(minutes=index), "tickid": index + 1, "close": float(index + 1)}
        for index in range(100)
    ]
    payload = build_bollinger_payload(rows, visible_start_id=1)

    assert len(payload["fiveMinute"]) == 1
    assert payload["fiveMinute"][0]["tickId"] == 100
    assert payload["fiveMinute"][0]["middle"] == pytest.approx(52.5)

