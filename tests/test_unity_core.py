from datetime import datetime, timedelta, timezone

from jobs.unity_core import UnityConfig, UnityEngine


def make_ticks(prices, spread=0.2):
    base = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
    rows = []
    half = spread / 2.0
    for i, price in enumerate(prices, start=1):
        rows.append(
            {
                "id": i,
                "timestamp": base + timedelta(seconds=i),
                "bid": float(price) - half,
                "ask": float(price) + half,
                "mid": float(price),
                "spread": float(spread),
            }
        )
    return rows


def run_engine(prices, **overrides):
    cfg = UnityConfig(
        confirmmult=2.0,
        confirmspread=0.0,
        confirmmin=0.5,
        signalminscore=0.0,
        signalminlag=0,
        signalmaxlag=99999,
        signalminmultiple=0.0,
        signalmaturemultiple=999.0,
        signalmaturelag=999999,
        trademinrisk=0.01,
        trademaxrisk=999.0,
        tradenoisebuffer=0.1,
        tradespreadbuffer=0.1,
        tradebuffermin=0.1,
        swingfactor=2.0,
        cleanpivotkeep=6,
        swingback=3,
        **overrides,
    )
    eng = UnityEngine(config=cfg)
    all_changes = []
    for row in make_ticks(prices):
        eng.process_tick(row)
        all_changes.append(eng.drain_changes())
    return eng, all_changes


def test_unity_builds_confirmed_pivots_and_bounded_clean_repaint():
    prices = [
        100, 101, 102, 103, 104, 105,
        104, 103, 102, 101, 100, 99, 98,
        99, 100, 101, 102, 103, 104, 105,
        104, 103, 102, 101, 100, 99, 98, 97,
        98, 99, 100, 101, 102, 103,
    ]
    eng, _changes = run_engine(prices)

    assert len(eng.pivots) >= 3
    assert eng.pivots[0].kind == "high"
    assert eng.pivots[1].kind == "low"
    assert eng.lastcleanfromtick is not None
    assert eng.lastcleanfromtick == eng.pivots[-3].tickid
    assert eng.rows[-1]["cleanstate"] in {"green", "yellow", "red"}


def test_unity_opens_and_closes_paper_trade():
    prices = [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107]
    _eng, changes = run_engine(prices)

    trades = [row for batch in changes for row in batch["trades"]]
    assert trades
    assert any(row["status"] == "open" for row in trades)
    assert any(row["status"] == "closed" for row in trades)
    assert any(row["exitreason"] in {"tp", "trail", "breakeven", "regimechange"} for row in trades if row["status"] == "closed")


def test_unity_skips_favored_signal_when_trade_already_open():
    prices = [
        100, 101, 102, 103,
        102.7, 102.4, 102.1, 101.8, 101.5, 101.2, 100.9, 100.6, 100.3, 100, 99.7, 99.4, 99.1, 98.8, 98.5,
    ]
    _eng, changes = run_engine(prices)

    signals = [row for batch in changes for row in batch["signals"]]
    assert any(row["status"] == "opened" for row in signals)
    assert any(row["status"] == "skipped" and row["skipreason"] == "opentrade" for row in signals)


def test_unity_emits_shadow_candidates_with_causal_snapshot():
    prices = [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103, 104]
    _eng, changes = run_engine(prices)

    signals = [row for batch in changes for row in batch["signals"]]
    candidates = [row for batch in changes for row in batch["candidates"]]

    assert candidates
    assert len(candidates) == len(signals)
    assert all(row["featurever"] == "unity-candidate-v1" for row in candidates)
    assert all(row["regimeto"] in {"green", "red"} for row in candidates)
    assert all("detail" in row["features"] for row in candidates)
    assert all("plan" in row["features"] for row in candidates)


def test_unity_arms_breakeven_and_trailing():
    prices = [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103, 104, 103, 102.5, 102]
    _eng, changes = run_engine(prices)

    events = [row for batch in changes for row in batch["events"]]
    kinds = {row["kind"] for row in events}
    assert "breakeven" in kinds
    assert "trailarm" in kinds
