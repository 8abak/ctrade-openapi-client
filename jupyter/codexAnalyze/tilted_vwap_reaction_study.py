from __future__ import annotations

import argparse
import json
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import hot_zone_study as study
from build_rolling_tick_vwap_band_replay import (
    MAX_PRIOR_FIVE_SECOND_JUMP,
    PIVOT_SECONDS,
    SPREAD_RATIO_LIMIT,
    TOUCH_SECONDS,
    WINDOW_MINUTES,
)
from market_structure import structure_events
from rolling_tick_vwap_band_study import rolling_indicator


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
OUTPUT_DIR = PROJECT_DIR / "studies" / "tilted_vwap_reactions"
BANDS = (1.0, 2.0, 3.0)
TILT_THRESHOLDS = (0.0, 0.20, 0.35, 0.50)
FROZEN_TILT_THRESHOLD = 0.35
FROZEN_TILT_MAXIMUM = 0.50
FROZEN_RAW_SLOPE_MINIMUM = 0.20
FROZEN_REACTION_BAND = 1.0
COOLDOWN_SECONDS = 30
OUTCOME_SECONDS = 300


def _outcome(bars: pd.DataFrame, location: int, side: str) -> dict[str, object]:
    timestamp = bars.index[location]
    end = min(len(bars), location + OUTCOME_SECONDS + 1)
    path = bars.iloc[location:end]
    entry = bars.iloc[location]
    if side == "long":
        entry_price = float(entry["ask"])
        pnl = path["bid"] - entry_price
    else:
        entry_price = float(entry["bid"])
        pnl = entry_price - path["ask"]
    cover = path.index[pnl.ge(0)]
    result: dict[str, object] = {
        "entry_price": entry_price,
        "spread_cover_seconds": float((cover[0] - timestamp).total_seconds()) if len(cover) else None,
    }
    for seconds in (5, 15, 60, 300):
        horizon = pnl.loc[: timestamp + pd.Timedelta(seconds=seconds)]
        result[f"mfe_{seconds}"] = float(horizon.max())
        result[f"mae_{seconds}"] = float(max(0.0, -horizon.min()))
        result[f"pnl_{seconds}"] = float(horizon.iloc[-1])
    result["reached_2_units_300s"] = bool(result["mfe_300"] >= 2.0)
    result["reached_3_units_300s"] = bool(result["mfe_300"] >= 3.0)
    return result


def _cooldown(events: list[dict[str, object]]) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    last: dict[tuple[str, float], pd.Timestamp | None] = {}
    for event in sorted(events, key=lambda item: item["timestamp_utc"]):
        key = (str(event["side"]), float(event["band_stdev"]))
        timestamp = pd.Timestamp(event["timestamp_utc"])
        previous = last.get(key)
        if previous is not None and (timestamp - previous).total_seconds() < COOLDOWN_SECONDS:
            continue
        last[key] = timestamp
        selected.append(event)
    return selected


def analyze_day(csv_path: Path) -> list[dict[str, object]]:
    ticks = study.load_ticks(csv_path)
    bars = study.second_bars(ticks)
    indicator = rolling_indicator(ticks, WINDOW_MINUTES).reindex(bars.index).ffill()
    events = structure_events(bars, pivot_seconds=PIVOT_SECONDS)
    normalized_tilt = indicator["slope_300"] * 5.0 / indicator["stdev"].replace(0, np.nan)
    spread_median = bars["spread"].rolling(300, min_periods=30).median()
    spread_ratio = bars["spread"] / spread_median.replace(0, np.nan)
    max_jump = bars["mid"].diff().abs().rolling(5, min_periods=5).max()
    common = (
        bars["observed"]
        & spread_ratio.le(SPREAD_RATIO_LIMIT)
        & max_jump.le(MAX_PRIOR_FIVE_SECOND_JUMP)
    ).fillna(False)
    day = csv_path.stem.removeprefix("ticks_XAUUSD_")
    raw: list[dict[str, object]] = []
    for band in BANDS:
        upper = indicator["vwap"] + band * indicator["stdev"]
        lower = indicator["vwap"] - band * indicator["stdev"]
        lower_gap = bars["mid"] - lower
        upper_gap = bars["mid"] - upper
        long_touch = lower_gap.rolling(TOUCH_SECONDS, min_periods=5).min().le(0)
        short_touch = upper_gap.rolling(TOUCH_SECONDS, min_periods=5).max().ge(0)
        long_reclaim = lower_gap.ge(0) & lower_gap.le(bars["zone_radius"])
        short_reject = upper_gap.le(0) & upper_gap.ge(-bars["zone_radius"])
        masks = {
            "long": common & events["bullish_structure_shift"] & long_touch & long_reclaim & indicator["slope_60"].gt(0) & normalized_tilt.gt(0),
            "short": common & events["bearish_structure_shift"] & short_touch & short_reject & indicator["slope_60"].lt(0) & normalized_tilt.lt(0),
        }
        for side, mask in masks.items():
            for timestamp in bars.index[mask.fillna(False)]:
                location = bars.index.get_loc(timestamp)
                bar = bars.iloc[location]
                tilt = float(normalized_tilt.iloc[location])
                row = {
                    "day": day,
                    "period": "later" if day >= "2026-02-06" else "earlier",
                    "timestamp_utc": timestamp.isoformat(),
                    "tick_id": int(bar["id"]),
                    "side": side,
                    "band_stdev": band,
                    "bid": float(bar["bid"]),
                    "ask": float(bar["ask"]),
                    "spread": float(bar["spread"]),
                    "tick_vwap": float(indicator.at[timestamp, "vwap"]),
                    "tick_stdev": float(indicator.at[timestamp, "stdev"]),
                    "active_band": float(lower.at[timestamp] if side == "long" else upper.at[timestamp]),
                    "vwap_slope_60_per_minute": float(indicator.at[timestamp, "slope_60"]),
                    "vwap_slope_300_per_minute": float(indicator.at[timestamp, "slope_300"]),
                    "normalized_five_minute_tilt_sigma": tilt,
                    "absolute_normalized_tilt": abs(tilt),
                    "spread_ratio": float(spread_ratio.at[timestamp]),
                    "maximum_prior_5s_jump": float(max_jump.at[timestamp]),
                }
                row.update(_outcome(bars, location, side))
                raw.append(row)
    return _cooldown(raw)


def summarize(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for threshold in TILT_THRESHOLDS:
        threshold_events = events.loc[events["absolute_normalized_tilt"].ge(threshold)]
        for band in BANDS:
            band_events = threshold_events.loc[threshold_events["band_stdev"].eq(band)]
            for period in ("earlier", "later", "all"):
                sample = band_events if period == "all" else band_events.loc[band_events["period"].eq(period)]
                rows.append({
                    "minimum_abs_tilt_sigma_per_5m": threshold,
                    "band_stdev": band,
                    "period": period,
                    "events": int(len(sample)),
                    "reach_2_units_300s_rate": float(sample["reached_2_units_300s"].mean()) if len(sample) else np.nan,
                    "reach_3_units_300s_rate": float(sample["reached_3_units_300s"].mean()) if len(sample) else np.nan,
                    "spread_cover_5s_rate": float(sample["spread_cover_seconds"].le(5).mean()) if len(sample) else np.nan,
                    "median_mfe_60": float(sample["mfe_60"].median()) if len(sample) else np.nan,
                    "median_mae_60": float(sample["mae_60"].median()) if len(sample) else np.nan,
                    "mean_pnl_60": float(sample["pnl_60"].mean()) if len(sample) else np.nan,
                })
    return pd.DataFrame(rows)


def summarize_frozen(events: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for band in BANDS:
        band_events = events.loc[events["band_stdev"].eq(band)]
        for period in ("earlier", "later", "all"):
            sample = band_events if period == "all" else band_events.loc[band_events["period"].eq(period)]
            rows.append({
                "band_stdev": band,
                "period": period,
                "events": int(len(sample)),
                "reach_2_units_300s_rate": float(sample["reached_2_units_300s"].mean()) if len(sample) else None,
                "reach_3_units_300s_rate": float(sample["reached_3_units_300s"].mean()) if len(sample) else None,
                "spread_cover_5s_rate": float(sample["spread_cover_seconds"].le(5).mean()) if len(sample) else None,
                "median_mfe_60": float(sample["mfe_60"].median()) if len(sample) else None,
                "median_mae_60": float(sample["mae_60"].median()) if len(sample) else None,
                "mean_pnl_60": float(sample["pnl_60"].mean()) if len(sample) else None,
            })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Study tilted rolling VWAP reactions for 2-3 unit moves.")
    parser.add_argument("--day")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--reuse-events", action="store_true")
    args = parser.parse_args()
    pattern = f"ticks_XAUUSD_{args.day}.csv" if args.day else "ticks_XAUUSD_2026-*.csv"
    paths = sorted((REPO_ROOT / "logs" / "sql_exports").glob(pattern))
    cached_events = OUTPUT_DIR / "tilted_vwap_reaction_events.csv"
    rows: list[dict[str, object]] = []
    if args.reuse_events and not args.day and cached_events.exists():
        events = pd.read_csv(cached_events)
        print(f"reused {len(events)} cached causal events", flush=True)
    else:
        if args.workers > 1 and len(paths) > 1:
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                for path, day_rows in zip(paths, executor.map(analyze_day, paths)):
                    print(f"completed {path.name}", flush=True)
                    rows.extend(day_rows)
        else:
            for path in paths:
                print(f"analyzing {path.name}", flush=True)
                rows.extend(analyze_day(path))
        events = pd.DataFrame(rows)
    summary = summarize(events)
    frozen = events.loc[
        events["absolute_normalized_tilt"].between(
            FROZEN_TILT_THRESHOLD, FROZEN_TILT_MAXIMUM, inclusive="both"
        )
        & events["vwap_slope_300_per_minute"].abs().ge(FROZEN_RAW_SLOPE_MINIMUM)
        & events["band_stdev"].eq(FROZEN_REACTION_BAND)
    ].copy()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    events.to_csv(OUTPUT_DIR / "tilted_vwap_reaction_events.csv", index=False)
    summary.to_csv(OUTPUT_DIR / "tilted_vwap_reaction_summary.csv", index=False)
    frozen.to_csv(OUTPUT_DIR / "tilted_vwap_frozen_candidates.csv", index=False)
    report = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "futureTicksUsedForSelection": False,
        "targetOutcomesUsedOnlyForAudit": True,
        "hypothesis": "reaction entries at 1/2/3 sigma improve when five-minute VWAP tilt is material",
        "frozenRule": {
            "windowMinutes": WINDOW_MINUTES,
            "bandsStdev": list(BANDS),
            "eligibleReactionBandStdev": FROZEN_REACTION_BAND,
            "minimumAbsoluteFiveMinuteTiltSigma": FROZEN_TILT_THRESHOLD,
            "maximumAbsoluteFiveMinuteTiltSigma": FROZEN_TILT_MAXIMUM,
            "minimumAbsoluteVwapSlopePriceUnitsPerMinute": FROZEN_RAW_SLOPE_MINIMUM,
            "trigger": "band touch and reclaim/rejection plus causal structure shift",
            "desiredMoveAudit": "2 and 3 executable XAUUSD price units within 300 seconds",
            "otherBandsStatus": "2-sigma and 3-sigma are calculated and displayed but remain research-only due to small samples",
        },
        "frozenSummary": summarize_frozen(frozen),
    }
    (OUTPUT_DIR / "tilted_vwap_reaction_study.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
