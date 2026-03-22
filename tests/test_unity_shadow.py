from datetime import datetime, timedelta, timezone

from jobs.unity_shadow import evaluate_candidate


def make_future_rows(entries):
    base = datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc)
    rows = []
    for tickid, seconds, bid, ask, mid, state in entries:
        rows.append(
            {
                "id": tickid,
                "timestamp": base + timedelta(seconds=seconds),
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "causalstate": state,
            }
        )
    return rows


def test_shadow_baseline_hits_tp_and_builds_scenarios():
    candidate = {
        "signaltickid": 100,
        "time": datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc),
        "side": "long",
        "eligible": True,
        "entryprice": 100.0,
        "risk": 1.0,
        "baselinetp": 101.0,
        "baselinesl": 99.0,
    }
    future_rows = make_future_rows(
        [
            (101, 5, 100.40, 100.60, 100.50, "green"),
            (102, 8, 101.10, 101.30, 101.20, "green"),
        ]
    )

    baseline, scenarios = evaluate_candidate(candidate, future_rows, timeoutsec=900, dayendtickid=500)

    assert baseline["status"] == "resolved"
    assert baseline["firsthit"] == "tp"
    assert baseline["wouldwin"] is True
    assert len(scenarios) == 6
    assert {row["code"] for row in scenarios} == {
        "tp075sl100",
        "tp100sl100",
        "tp125sl100",
        "tp150sl100",
        "tp100sl075",
        "tp100sl125",
    }


def test_shadow_can_score_scenarios_even_when_baseline_is_ineligible():
    candidate = {
        "signaltickid": 200,
        "time": datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc),
        "side": "short",
        "eligible": False,
        "entryprice": 100.0,
        "risk": 1.0,
        "baselinetp": 99.0,
        "baselinesl": 101.0,
    }
    future_rows = make_future_rows(
        [
            (201, 3, 99.60, 99.80, 99.70, "red"),
            (202, 6, 98.90, 99.10, 99.00, "red"),
        ]
    )

    baseline, scenarios = evaluate_candidate(candidate, future_rows, timeoutsec=900, dayendtickid=500)

    assert baseline["status"] == "ineligible"
    assert baseline["firsthit"] == "unresolved"
    assert any(row["status"] == "resolved" and row["firsthit"] == "tp" for row in scenarios)


def test_shadow_resolves_regime_change_before_timeout():
    candidate = {
        "signaltickid": 300,
        "time": datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc),
        "side": "long",
        "eligible": True,
        "entryprice": 100.0,
        "risk": 1.0,
        "baselinetp": 101.0,
        "baselinesl": 99.0,
    }
    future_rows = make_future_rows(
        [
            (301, 4, 100.20, 100.40, 100.30, "green"),
            (302, 9, 100.10, 100.30, 100.20, "red"),
        ]
    )

    baseline, _scenarios = evaluate_candidate(candidate, future_rows, timeoutsec=900, dayendtickid=500)

    assert baseline["status"] == "resolved"
    assert baseline["firsthit"] == "regimechange"
