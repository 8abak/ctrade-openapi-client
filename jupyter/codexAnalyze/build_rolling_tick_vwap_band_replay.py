from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import build_hot_zone_reaction_replay as outcome_tools
import hot_zone_study as study
from market_structure import structure_events
from rolling_tick_vwap_band_study import rolling_indicator


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
WINDOW_MINUTES = 30
BAND_STDEV = 1.0
PIVOT_SECONDS = 3
TOUCH_SECONDS = 30
COOLDOWN_SECONDS = 30
MAX_PRIOR_FIVE_SECOND_JUMP = 1.25
SPREAD_RATIO_LIMIT = 1.50


def causal_candidates(csv_path: Path) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    ticks = study.load_ticks(csv_path)
    bars = study.second_bars(ticks)
    indicator = rolling_indicator(ticks, WINDOW_MINUTES).reindex(bars.index).ffill()
    events = structure_events(bars, pivot_seconds=PIVOT_SECONDS)
    upper = indicator["vwap"] + BAND_STDEV * indicator["stdev"]
    lower = indicator["vwap"] - BAND_STDEV * indicator["stdev"]
    lower_gap = bars["mid"] - lower
    upper_gap = bars["mid"] - upper
    long_touch = lower_gap.rolling(TOUCH_SECONDS, min_periods=5).min().le(0)
    short_touch = upper_gap.rolling(TOUCH_SECONDS, min_periods=5).max().ge(0)
    long_reclaim = lower_gap.ge(0) & lower_gap.le(bars["zone_radius"])
    short_reject = upper_gap.le(0) & upper_gap.ge(-bars["zone_radius"])
    long_slope = indicator["slope_60"].gt(0) & indicator["slope_300"].gt(0)
    short_slope = indicator["slope_60"].lt(0) & indicator["slope_300"].lt(0)
    spread_median = bars["spread"].rolling(300, min_periods=30).median()
    spread_ratio = bars["spread"] / spread_median.replace(0, np.nan)
    max_jump = bars["mid"].diff().abs().rolling(5, min_periods=5).max()
    common = (
        bars["observed"] & spread_ratio.le(SPREAD_RATIO_LIMIT)
        & max_jump.le(MAX_PRIOR_FIVE_SECOND_JUMP)
    ).fillna(False)
    masks = {
        "long": common & events["bullish_structure_shift"] & long_touch & long_reclaim & long_slope,
        "short": common & events["bearish_structure_shift"] & short_touch & short_reject & short_slope,
    }
    raw: list[tuple[pd.Timestamp, str]] = []
    for side, mask in masks.items():
        raw.extend((timestamp, side) for timestamp in bars.index[mask.fillna(False)])
    raw.sort(key=lambda value: value[0])
    selected: list[dict[str, object]] = []
    last_timestamp = {"long": None, "short": None}
    for timestamp, side in raw:
        previous = last_timestamp[side]
        if previous is not None and (timestamp - previous).total_seconds() < COOLDOWN_SECONDS:
            continue
        last_timestamp[side] = timestamp
        bar = bars.loc[timestamp]
        band = lower.loc[timestamp] if side == "long" else upper.loc[timestamp]
        selected.append({
            "sequence": len(selected) + 1,
            "id": f"rolling-tick-vwap-band-{side}-{int(bar['id'])}",
            "tick_id": int(bar["id"]),
            "timestamp_ms": int(timestamp.value // 1_000_000),
            "side": side,
            "bid": float(bar["bid"]),
            "ask": float(bar["ask"]),
            "window_minutes": WINDOW_MINUTES,
            "band_stdev": BAND_STDEV,
            "tick_vwap": float(indicator.at[timestamp, "vwap"]),
            "tick_stdev": float(indicator.at[timestamp, "stdev"]),
            "active_band": float(band),
            "features": {
                "vwap_slope_60_per_minute": float(indicator.at[timestamp, "slope_60"]),
                "vwap_slope_300_per_minute": float(indicator.at[timestamp, "slope_300"]),
                "pivot_seconds": PIVOT_SECONDS,
                "spread_ratio": float(spread_ratio.loc[timestamp]),
                "maximum_prior_5s_jump": float(max_jump.loc[timestamp]),
                "interaction": "band touch, reclaim/rejection, causal structure break",
            },
        })
    return ticks, selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate causal rolling equal-tick VWAP band candidates.")
    parser.add_argument("--day", default="2026-02-12")
    args = parser.parse_args()
    ticks, candidates = causal_candidates(REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{args.day}.csv")
    for candidate in candidates:
        timestamp = pd.Timestamp(int(candidate["timestamp_ms"]), unit="ms", tz="UTC")
        candidate["outcomeAudit"] = outcome_tools.outcome(ticks, timestamp, str(candidate["side"]))
    payload = {
        "schemaVersion": 1,
        "day": args.day,
        "mode": "causal_rolling_equal_tick_vwap_band_replay",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "selectionPolicy": {
            "futureTicksUsed": False,
            "tickWeight": 1,
            "price": "bid/ask midpoint",
            "windowMinutes": WINDOW_MINUTES,
            "bandStdev": BAND_STDEV,
            "direction": "one-minute and five-minute VWAP slopes must agree",
            "trigger": "active band touched in prior 30 seconds, reclaimed/rejected, then causal structure break",
            "continuity": f"maximum prior five-second one-second jump <= {MAX_PRIOR_FIVE_SECOND_JUMP}",
            "spreadRatioLimit": SPREAD_RATIO_LIMIT,
        },
        "outcomePolicy": "outcomeAudit appended only after the complete causal queue is frozen",
        "signals": candidates,
    }
    output = PROJECT_DIR / "history_data" / f"rolling_tick_vwap_band_signals_{args.day}.json"
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(output)
    print(f"candidates={len(candidates)}")
    for candidate in candidates:
        print(candidate["sequence"], candidate["tick_id"], candidate["side"], candidate["outcomeAudit"])


if __name__ == "__main__":
    main()
