from datetime import datetime, timedelta, timezone

from datavis.structure_watch import WatchEngine


def tick(i, second, mid, spread=0.2):
    return {
        "id": i,
        "timestamp": datetime(2026, 9, 29, tzinfo=timezone.utc) + timedelta(seconds=second),
        "bid": mid - spread / 2,
        "ask": mid + spread / 2,
    }


def seed(engine):
    for i in range(13):
        engine.process(tick(i + 1, i * 10, 4100.0 if i % 2 else 4101.0))


def test_break_requires_acceptance_retest_and_rejection():
    engine = WatchEngine()
    seed(engine)
    assert engine.process(tick(14, 130, 4101.5)) is None
    assert engine.process(tick(15, 135, 4101.7)) is None
    assert engine.process(tick(16, 141, 4101.6)) is None
    assert engine.process(tick(17, 145, 4101.05)) is None
    event = engine.process(tick(18, 151, 4101.8))
    assert event["type"] == "BREAK_RETEST_CONFIRMED"
    assert event["direction"] == "up"
    assert event["boundary"] == 4101.0
    assert engine.process(tick(19, 152, 4102.0)) is None


def test_wick_and_wide_spread_do_not_emit():
    engine = WatchEngine()
    seed(engine)
    engine.process(tick(14, 130, 4101.5))
    engine.process(tick(15, 135, 4100.0))
    assert engine.pending is None
    assert engine.process(tick(16, 140, 4105.0, spread=2.0)) is None
    assert engine.pending is None


def test_gap_discards_old_range():
    engine = WatchEngine()
    seed(engine)
    assert engine.process(tick(14, 600, 4110.0)) is None
    assert engine.pending is None
    assert len(engine.history) == 1
