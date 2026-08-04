from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import build_hot_zone_reaction_replay as quote_reaction
import hot_zone_study as study
from market_structure import structure_events
from tick_vwap_band_study import equal_tick_vwap


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
BAND_STDEV = 0.5
SLOPE_SECONDS = 900
PIVOT_SECONDS = 3
TOUCH_LOOKBACK_SECONDS = 30
COOLDOWN_SECONDS = 30
MAX_PRIOR_FIVE_SECOND_JUMP = 1.25
SPREAD_RATIO_LIMIT = 1.50


def causal_candidates(csv_path: Path) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    ticks = study.load_ticks(csv_path)
    reset_group = pd.factorize(ticks["timestamp_utc"].dt.floor("D"))[0]
    mean, stdev = equal_tick_vwap(ticks["mid"].to_numpy(float), reset_group)
    tick_indicator = pd.DataFrame({
        "tick_vwap": mean,
        "tick_stdev": stdev,
        "anchor": reset_group,
    }, index=ticks["timestamp_utc"])
    indicator_bars = tick_indicator.resample("1s").last().ffill()
    bars = study.second_bars(ticks)
    indicator_bars = indicator_bars.reindex(bars.index).ffill()
    events = structure_events(bars, pivot_seconds=PIVOT_SECONDS)
    lower_band = indicator_bars["tick_vwap"] - BAND_STDEV * indicator_bars["tick_stdev"]
    lower_gap = bars["mid"] - lower_band
    touched = lower_gap.rolling(TOUCH_LOOKBACK_SECONDS, min_periods=5).min().le(0)
    reclaimed = lower_gap.ge(0) & lower_gap.le(bars["zone_radius"])
    same_anchor = indicator_bars["anchor"].eq(indicator_bars["anchor"].shift(SLOPE_SECONDS))
    slope = (indicator_bars["tick_vwap"] - indicator_bars["tick_vwap"].shift(SLOPE_SECONDS)) / (SLOPE_SECONDS / 60)
    spread_median = bars["spread"].rolling(300, min_periods=30).median()
    spread_ratio = bars["spread"] / spread_median.replace(0, np.nan)
    max_jump = bars["mid"].diff().abs().rolling(5, min_periods=5).max()
    mask = (
        events["bullish_structure_shift"] & bars["observed"] & touched & reclaimed
        & same_anchor & slope.gt(0) & spread_ratio.le(SPREAD_RATIO_LIMIT)
        & max_jump.le(MAX_PRIOR_FIVE_SECOND_JUMP)
    ).fillna(False)
    selected: list[dict[str, object]] = []
    last_timestamp: pd.Timestamp | None = None
    for timestamp in bars.index[mask]:
        if last_timestamp is not None and (timestamp - last_timestamp).total_seconds() < COOLDOWN_SECONDS:
            continue
        last_timestamp = timestamp
        bar = bars.loc[timestamp]
        selected.append({
            "sequence": len(selected) + 1,
            "id": f"tick-vwap-band-long-{int(bar['id'])}",
            "tick_id": int(bar["id"]),
            "timestamp_ms": int(timestamp.value // 1_000_000),
            "side": "long",
            "bid": float(bar["bid"]),
            "ask": float(bar["ask"]),
            "anchor": "00:00 UTC",
            "band_stdev": BAND_STDEV,
            "tick_vwap": float(indicator_bars.at[timestamp, "tick_vwap"]),
            "tick_stdev": float(indicator_bars.at[timestamp, "tick_stdev"]),
            "lower_band": float(lower_band.loc[timestamp]),
            "features": {
                "vwap_slope_per_minute": float(slope.loc[timestamp]),
                "slope_seconds": SLOPE_SECONDS,
                "pivot_seconds": PIVOT_SECONDS,
                "spread_ratio": float(spread_ratio.loc[timestamp]),
                "maximum_prior_5s_jump": float(max_jump.loc[timestamp]),
                "interaction": "lower-band touch, reclaim, bullish structure break",
            },
        })
    return ticks, selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate causal equal-tick VWAP deviation-band candidates.")
    parser.add_argument("--day", default="2026-02-12")
    args = parser.parse_args()
    csv_path = REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{args.day}.csv"
    ticks, candidates = causal_candidates(csv_path)
    for candidate in candidates:
        timestamp = pd.Timestamp(int(candidate["timestamp_ms"]), unit="ms", tz="UTC")
        candidate["outcomeAudit"] = quote_reaction.outcome(ticks, timestamp, "long")
    payload = {
        "schemaVersion": 1,
        "day": args.day,
        "mode": "causal_equal_tick_vwap_deviation_band_replay",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "selectionPolicy": {
            "futureTicksUsed": False,
            "tickWeight": 1,
            "price": "bid/ask midpoint",
            "anchor": "00:00 UTC",
            "bandStdev": BAND_STDEV,
            "direction": f"VWAP {SLOPE_SECONDS}-second slope > 0",
            "trigger": f"lower band touched in prior {TOUCH_LOOKBACK_SECONDS}s, reclaimed, then causal bullish structure break",
            "continuity": f"maximum prior five-second one-second jump <= {MAX_PRIOR_FIVE_SECOND_JUMP}",
            "spreadRatioLimit": SPREAD_RATIO_LIMIT,
        },
        "outcomePolicy": "outcomeAudit appended only after the complete causal queue is frozen",
        "signals": candidates,
    }
    output = PROJECT_DIR / "history_data" / f"tick_vwap_band_signals_{args.day}.json"
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(output)
    print(f"candidates={len(candidates)}")
    for item in candidates:
        print(item["sequence"], item["tick_id"], item["outcomeAudit"])


if __name__ == "__main__":
    main()
