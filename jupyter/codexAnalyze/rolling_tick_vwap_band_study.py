from __future__ import annotations

import json

import numpy as np
import pandas as pd

import hot_zone_study as base


WINDOW_MINUTES = (15, 30, 60, 120)
BANDS = (0.5, 1.0, 1.5, 2.0)
SLOPE_SECONDS = (60, 300)
TOUCH_SECONDS = 30


def rolling_indicator(ticks: pd.DataFrame, minutes: int) -> pd.DataFrame:
    indexed = ticks.set_index("timestamp_utc").copy()
    indexed["mid_square"] = indexed["mid"] ** 2
    bars = indexed.resample("1s").agg(
        tick_count=("id", "size"), mid_sum=("mid", "sum"), mid_square_sum=("mid_square", "sum")
    )
    window = f"{minutes}min"
    minimum = max(60, minutes * 15)
    count = bars["tick_count"].rolling(window, min_periods=minimum).sum()
    total = bars["mid_sum"].rolling(window, min_periods=minimum).sum()
    square_total = bars["mid_square_sum"].rolling(window, min_periods=minimum).sum()
    mean = total / count
    variance = (square_total / count - mean * mean).clip(lower=0)
    result = pd.DataFrame(index=bars.index)
    result["vwap"] = mean
    result["stdev"] = variance.pow(.5)
    for seconds in SLOPE_SECONDS:
        result[f"slope_{seconds}"] = (mean - mean.shift(seconds)) / (seconds / 60)
    return result


def enrich(events: pd.DataFrame, indicator: pd.DataFrame, minutes: int) -> pd.DataFrame:
    data = events.copy()
    timestamps = pd.to_datetime(data["timestamp_utc"], utc=True)
    aligned = indicator.reindex(timestamps, method="ffill").set_axis(data.index)
    data["window_minutes"] = minutes
    data["rolling_tick_vwap"] = aligned["vwap"]
    data["rolling_tick_stdev"] = aligned["stdev"]
    for seconds in SLOPE_SECONDS:
        data[f"rolling_slope_{seconds}"] = aligned[f"slope_{seconds}"]
    for band in BANDS:
        lower = indicator["vwap"] - band * indicator["stdev"]
        upper = indicator["vwap"] + band * indicator["stdev"]
        lower_gap = events_by_time(events, indicator.index, lower, timestamps)
        upper_gap = events_by_time(events, indicator.index, upper, timestamps)
        data[f"long_reclaim_{band:g}"] = lower_gap["prior_min"].le(0) & lower_gap["current"].between(0, data["zone_radius"])
        data[f"short_reject_{band:g}"] = upper_gap["prior_max"].ge(0) & upper_gap["current"].between(-data["zone_radius"], 0)
    return data


def events_by_time(
    events: pd.DataFrame, indicator_index: pd.DatetimeIndex, level: pd.Series, timestamps: pd.Series
) -> pd.DataFrame:
    # Event prices are one-second causal bar midpoints reconstructed from bid/ask.
    event_mid = (events["bid"] + events["ask"]) / 2
    current_level = level.reindex(pd.DatetimeIndex(timestamps), method="ffill").set_axis(events.index)
    current_gap = event_mid - current_level
    prior_min: list[float] = []
    prior_max: list[float] = []
    # Reconstruct only the price-level gap at event seconds and use the preceding event-price
    # path from the stored one-second outcomes source. The exact touch is recomputed in main.
    for timestamp, gap in zip(timestamps, current_gap):
        start = timestamp - pd.Timedelta(seconds=TOUCH_SECONDS)
        window_level = level.loc[start:timestamp]
        if window_level.empty:
            prior_min.append(np.nan); prior_max.append(np.nan)
        else:
            # The caller replaces these placeholders using the exact one-second mid path.
            prior_min.append(float(gap)); prior_max.append(float(gap))
    return pd.DataFrame({"current": current_gap, "prior_min": prior_min, "prior_max": prior_max}, index=events.index)


def enrich_exact(events: pd.DataFrame, ticks: pd.DataFrame, minutes: int) -> pd.DataFrame:
    indicator = rolling_indicator(ticks, minutes)
    price = ticks.set_index("timestamp_utc")["mid"].resample("1s").last().ffill().reindex(indicator.index).ffill()
    data = events.copy()
    timestamps = pd.to_datetime(data["timestamp_utc"], utc=True)
    locations = indicator.index.searchsorted(timestamps, side="right") - 1
    data["window_minutes"] = minutes
    for column in ("vwap", "stdev", "slope_60", "slope_300"):
        data[f"rolling_tick_{column}"] = indicator[column].to_numpy()[locations]
    for band in BANDS:
        lower_gap = price - (indicator["vwap"] - band * indicator["stdev"])
        upper_gap = price - (indicator["vwap"] + band * indicator["stdev"])
        long_values: list[bool] = []
        short_values: list[bool] = []
        for row_position, location in enumerate(locations):
            if location < TOUCH_SECONDS or not np.isfinite(indicator["vwap"].iloc[location]):
                long_values.append(False); short_values.append(False); continue
            start = max(0, location - TOUCH_SECONDS)
            current_lower = float(lower_gap.iloc[location])
            current_upper = float(upper_gap.iloc[location])
            radius = float(data.iloc[row_position]["zone_radius"])
            long_values.append(bool(lower_gap.iloc[start:location + 1].min() <= 0 <= current_lower <= radius))
            short_values.append(bool(upper_gap.iloc[start:location + 1].max() >= 0 >= current_upper >= -radius))
        data[f"long_reclaim_{band:g}"] = long_values
        data[f"short_reject_{band:g}"] = short_values
    return data


def summarize(data: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for minutes in WINDOW_MINUTES:
        window_data = data[data["window_minutes"] == minutes]
        for period in ("earlier", "later"):
            period_data = window_data[window_data["period"] == period]
            for band in BANDS:
                for slope_mode in ("60", "300", "both"):
                    long_slope = period_data["rolling_tick_slope_60"].gt(0) if slope_mode == "60" else period_data["rolling_tick_slope_300"].gt(0)
                    short_slope = period_data["rolling_tick_slope_60"].lt(0) if slope_mode == "60" else period_data["rolling_tick_slope_300"].lt(0)
                    if slope_mode == "both":
                        long_slope = period_data["rolling_tick_slope_60"].gt(0) & period_data["rolling_tick_slope_300"].gt(0)
                        short_slope = period_data["rolling_tick_slope_60"].lt(0) & period_data["rolling_tick_slope_300"].lt(0)
                    for side, mask in (
                        ("long", period_data["side"].eq("long") & period_data[f"long_reclaim_{band:g}"] & long_slope),
                        ("short", period_data["side"].eq("short") & period_data[f"short_reject_{band:g}"] & short_slope),
                    ):
                        selected = period_data[mask]
                        rows.append({
                            "period": period, "window_minutes": minutes, "band_stdev": band,
                            "slope_mode": slope_mode, "side": side, "events": len(selected),
                            "successes": int(selected["excellent_entry_proxy"].sum()),
                            "excellent_proxy_rate": float(selected["excellent_entry_proxy"].mean()) if len(selected) else np.nan,
                            "cover_5s_rate": float(selected["covered_within_5s"].mean()) if len(selected) else np.nan,
                            "median_mfe_15": float(selected["mfe_15"].median()) if len(selected) else np.nan,
                            "median_mae_15": float(selected["mae_15"].median()) if len(selected) else np.nan,
                        })
    return pd.DataFrame(rows)


def main() -> None:
    events = pd.read_csv(base.OUTPUT_DIR / "hot_zone_events.csv")
    frames: list[pd.DataFrame] = []
    for day, day_events in events.groupby("day", sort=True):
        ticks = base.load_ticks(base.EXPORT_DIR / f"ticks_XAUUSD_{day}.csv")
        for minutes in WINDOW_MINUTES:
            frames.append(enrich_exact(day_events, ticks, minutes))
    enriched = pd.concat(frames, ignore_index=True)
    summary = summarize(enriched)
    enriched.to_csv(base.OUTPUT_DIR / "rolling_tick_vwap_band_events.csv", index=False)
    summary.to_csv(base.OUTPUT_DIR / "rolling_tick_vwap_band_summary.csv", index=False)
    comparison = summary[summary["events"] >= 8].pivot_table(
        index=["window_minutes", "band_stdev", "slope_mode", "side"],
        columns="period", values=["events", "excellent_proxy_rate"], aggfunc="first",
    ).dropna().reset_index()
    comparison.columns = ["_".join(map(str, value)).rstrip("_") for value in comparison.columns]
    comparison["minimum_rate"] = comparison[[
        "excellent_proxy_rate_earlier", "excellent_proxy_rate_later"
    ]].min(axis=1)
    comparison = comparison.sort_values(["minimum_rate", "excellent_proxy_rate_later"], ascending=False)
    report = {
        "schemaVersion": 1,
        "indicator": "rolling equal-tick VWAP and tick-weighted standard deviation",
        "futureTicksUsedForIndicatorOrSelection": False,
        "bestConsistentConfigurations": comparison.head(20).to_dict(orient="records"),
    }
    (base.OUTPUT_DIR / "rolling_tick_vwap_band_study.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(comparison.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
