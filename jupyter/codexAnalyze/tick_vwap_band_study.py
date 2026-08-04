from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import hot_zone_study as base


BANDS = (0.0, 0.5, 1.0, 1.5, 2.0)
SLOPE_WINDOWS = (60, 300, 900)
TOUCH_LOOKBACK_SECONDS = 30


def equal_tick_vwap(mid: np.ndarray, reset_group: np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray]:
    """Causal expanding mean and population stdev, reset at each contiguous anchor group."""
    if reset_group is None:
        reset_group = np.zeros(len(mid), dtype=int)
    mean = np.empty(len(mid), dtype=float)
    stdev = np.empty(len(mid), dtype=float)
    starts = np.r_[0, np.flatnonzero(reset_group[1:] != reset_group[:-1]) + 1]
    ends = np.r_[starts[1:], len(mid)]
    for start, end in zip(starts, ends):
        values = mid[start:end]
        count = np.arange(1, len(values) + 1, dtype=float)
        group_mean = np.cumsum(values, dtype=float) / count
        second_moment = np.cumsum(values * values, dtype=float) / count
        mean[start:end] = group_mean
        stdev[start:end] = np.sqrt(np.maximum(second_moment - group_mean * group_mean, 0.0))
    return mean, stdev


def enrich_day(events: pd.DataFrame, ticks: pd.DataFrame, anchor: str) -> pd.DataFrame:
    result = events.copy()
    mid = ticks["mid"].to_numpy(float)
    if anchor == "broker_day":
        reset_group = np.zeros(len(ticks), dtype=int)
    elif anchor == "utc_day":
        reset_group = pd.factorize(ticks["timestamp_utc"].dt.floor("D"))[0]
    else:
        raise ValueError(f"Unknown anchor: {anchor}")
    mean, stdev = equal_tick_vwap(mid, reset_group)
    result["anchor"] = anchor
    time_ns = ticks["timestamp_utc"].astype("int64").to_numpy()
    id_to_location = pd.Series(np.arange(len(ticks)), index=ticks["id"]).to_dict()
    for column in ("tick_vwap", "tick_stdev", "current_z", "slope_60", "slope_300", "slope_900"):
        result[column] = np.nan
    for band in BANDS:
        result[f"long_touch_reclaim_{band:g}"] = False
        result[f"short_touch_reject_{band:g}"] = False
        result[f"band_distance_{band:g}"] = np.nan
    for row_index, event in result.iterrows():
        location = id_to_location.get(int(event["tick_id"]))
        if location is None or location < 1 or stdev[location] <= 0:
            continue
        result.at[row_index, "tick_vwap"] = mean[location]
        result.at[row_index, "tick_stdev"] = stdev[location]
        result.at[row_index, "current_z"] = (mid[location] - mean[location]) / stdev[location]
        for seconds in SLOPE_WINDOWS:
            prior = int(np.searchsorted(time_ns, time_ns[location] - seconds * 1_000_000_000, side="right")) - 1
            prior = max(0, prior)
            result.at[row_index, f"slope_{seconds}"] = (mean[location] - mean[prior]) / (seconds / 60)
        start = int(np.searchsorted(
            time_ns, time_ns[location] - TOUCH_LOOKBACK_SECONDS * 1_000_000_000, side="left"
        ))
        while start < location and reset_group[start] != reset_group[location]:
            start += 1
        window_mid = mid[start:location + 1]
        window_mean = mean[start:location + 1]
        window_std = stdev[start:location + 1]
        radius = float(event["zone_radius"])
        for band in BANDS:
            lower_gap = window_mid - (window_mean - band * window_std)
            upper_gap = window_mid - (window_mean + band * window_std)
            current_lower_gap = float(lower_gap[-1])
            current_upper_gap = float(upper_gap[-1])
            result.at[row_index, f"long_touch_reclaim_{band:g}"] = bool(
                np.min(lower_gap) <= 0 <= current_lower_gap <= radius
            )
            result.at[row_index, f"short_touch_reject_{band:g}"] = bool(
                np.max(upper_gap) >= 0 >= current_upper_gap >= -radius
            )
            result.at[row_index, f"band_distance_{band:g}"] = min(
                abs(current_lower_gap), abs(current_upper_gap)
            )
    return result


def parameter_summary(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor in ("broker_day", "utc_day"):
        for period in ("earlier", "later"):
            period_events = events[(events["period"] == period) & (events["anchor"] == anchor)]
            for band in BANDS:
                for seconds in SLOPE_WINDOWS:
                    long_mask = (
                        period_events["side"].eq("long")
                        & period_events[f"long_touch_reclaim_{band:g}"]
                        & period_events[f"slope_{seconds}"].gt(0)
                    )
                    short_mask = (
                        period_events["side"].eq("short")
                        & period_events[f"short_touch_reject_{band:g}"]
                        & period_events[f"slope_{seconds}"].lt(0)
                    )
                    for side, mask in (("long", long_mask), ("short", short_mask)):
                        selected = period_events[mask]
                        rows.append({
                            "anchor": anchor, "period": period, "band_stdev": band,
                            "slope_seconds": seconds, "side": side, "events": len(selected),
                            "successes": int(selected["excellent_entry_proxy"].sum()),
                            "excellent_proxy_rate": float(selected["excellent_entry_proxy"].mean()) if len(selected) else np.nan,
                            "cover_5s_rate": float(selected["covered_within_5s"].mean()) if len(selected) else np.nan,
                            "median_mfe_15": float(selected["mfe_15"].median()) if len(selected) else np.nan,
                            "median_mae_15": float(selected["mae_15"].median()) if len(selected) else np.nan,
                        })
    return pd.DataFrame(rows)


def main() -> None:
    source = base.OUTPUT_DIR / "hot_zone_events.csv"
    events = pd.read_csv(source)
    enriched: list[pd.DataFrame] = []
    for day, day_events in events.groupby("day", sort=True):
        ticks = base.load_ticks(base.EXPORT_DIR / f"ticks_XAUUSD_{day}.csv")
        for anchor in ("broker_day", "utc_day"):
            enriched.append(enrich_day(day_events, ticks, anchor))
    result = pd.concat(enriched, ignore_index=True).sort_values(["day", "timestamp_utc"])
    summary = parameter_summary(result)
    result.to_csv(base.OUTPUT_DIR / "tick_vwap_band_events.csv", index=False)
    summary.to_csv(base.OUTPUT_DIR / "tick_vwap_band_summary.csv", index=False)
    consistent = summary[summary["events"] >= 8].pivot_table(
        index=["anchor", "band_stdev", "slope_seconds", "side"], columns="period",
        values=["events", "excellent_proxy_rate"], aggfunc="first",
    ).dropna().reset_index()
    consistent.columns = ["_".join(map(str, column)).rstrip("_") for column in consistent.columns]
    consistent["minimum_rate"] = consistent[[
        "excellent_proxy_rate_earlier", "excellent_proxy_rate_later"
    ]].min(axis=1)
    consistent = consistent.sort_values(
        ["minimum_rate", "excellent_proxy_rate_later"], ascending=False
    )
    report = {
        "schemaVersion": 1,
        "indicator": "session-anchored equal-tick VWAP with expanding standard-deviation bands",
        "formula": {
            "price": "tick midpoint",
            "weight": "one per tick",
            "vwapProxy": "cumulative mean(mid)",
            "stdev": "sqrt(cumulative mean(mid^2) - cumulative mean(mid)^2)",
            "causal": True,
        },
        "confirmation": "rising VWAP: touch/reclaim lower band plus bullish structure break; falling VWAP mirrors for short",
        "bestConsistentConfigurations": consistent.head(12).to_dict(orient="records"),
    }
    (base.OUTPUT_DIR / "tick_vwap_band_study.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(consistent.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
