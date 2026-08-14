import pytest

from datavis.regression_channel import (
    RegressionChannelError,
    channel_values_at,
    fit_pitchfork,
    fit_regression_channel,
    geometry_values_at,
)
from datavis.smart_scalp import SmartScalpService


def test_fits_and_extends_regression_channel():
    rows = [
        {"id": 100, "mid": 10.0},
        {"id": 101, "mid": 11.2},
        {"id": 102, "mid": 11.8},
        {"id": 103, "mid": 13.1},
    ]

    model = fit_regression_channel(rows, start_tick_id=100, end_tick_id=103, deviations=2)
    extended = channel_values_at(model, 105)

    assert model["sampleCount"] == 4
    assert model["slopePerTick"] == pytest.approx(1.0, abs=0.08)
    assert extended["lower"] < extended["center"] < extended["upper"]
    assert extended["center"] > model["end"]["center"]


def test_rejects_invalid_range():
    with pytest.raises(RegressionChannelError):
        fit_regression_channel([], start_tick_id=10, end_tick_id=10, deviations=2)


def test_channel_close_runs_server_side_for_buy_break_below_lower_band():
    model = fit_regression_channel(
        [{"id": 100, "mid": 10.0}, {"id": 101, "mid": 10.2}, {"id": 102, "mid": 9.8}],
        start_tick_id=100,
        end_tick_id=102,
        deviations=1,
    )
    close_calls = []
    position = {
        "positionId": 77,
        "side": "buy",
        "volume": 1000,
        "volumeLots": 0.01,
        "entryPrice": 10.0,
        "netUnrealizedPnl": -1.0,
    }
    service = SmartScalpService(
        symbol="XAUUSD",
        fetch_ticks_after=lambda _after, _limit: [],
        fetch_recent_ticks=lambda _limit: [{"id": 102, "mid": 9.8}],
        fetch_latest_tick=lambda: {"id": 102, "mid": 9.8},
        fetch_snapshot=lambda: {"positions": [position]},
        fetch_broker_status=lambda: {"ready": True},
        place_market_order=lambda **_kwargs: {},
        close_position=lambda **kwargs: close_calls.append(kwargs) or {"ok": True},
    )

    service.set_close_configuration(mode="channel", channel=model)
    with service._lock:
        service._ingest_tick_locked({"id": 103, "mid": 9.0})
        service._evaluate_locked()

    assert close_calls
    assert close_calls[0]["position_id"] == 77
    assert close_calls[0]["source"] == "channel_close"


def test_pitchfork_builds_parallel_live_boundaries():
    model = fit_pitchfork(
        [{"id": 100, "mid": 10.0}, {"id": 104, "mid": 12.0}, {"id": 108, "mid": 9.0}],
        start_tick_id=100,
        end_tick_id=104,
        anchor_tick_id=108,
    )

    at_end = geometry_values_at(model, 112)

    assert model["kind"] == "pitchfork"
    assert model["pivotTickIds"] == [100, 104, 108]
    assert at_end["lower"] < at_end["center"] < at_end["upper"]


def test_regression_close_uses_center_line_for_sell():
    model = fit_regression_channel(
        [{"id": 100, "mid": 10.0}, {"id": 101, "mid": 10.1}, {"id": 102, "mid": 10.2}],
        start_tick_id=100,
        end_tick_id=102,
        deviations=2,
    )
    model["kind"] = "regression"
    close_calls = []
    position = {"positionId": 88, "side": "sell", "volume": 1000, "entryPrice": 10.0}
    service = SmartScalpService(
        symbol="XAUUSD",
        fetch_ticks_after=lambda _after, _limit: [],
        fetch_recent_ticks=lambda _limit: [{"id": 102, "mid": 10.2}],
        fetch_latest_tick=lambda: {"id": 102, "mid": 10.2},
        fetch_snapshot=lambda: {"positions": [position]},
        fetch_broker_status=lambda: {"ready": True},
        place_market_order=lambda **_kwargs: {},
        close_position=lambda **kwargs: close_calls.append(kwargs) or {"ok": True},
    )

    service.set_close_configuration(mode="regression", channel=model)
    with service._lock:
        service._ingest_tick_locked({"id": 103, "mid": 11.0})
        service._evaluate_locked()

    assert close_calls[0]["source"] == "regression_close"
