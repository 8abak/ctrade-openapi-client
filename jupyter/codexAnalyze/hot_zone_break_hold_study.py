from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import hot_zone_study as base


HOLD_QUOTES = 3
ARM_LOOKBACK_QUOTES = 30
COOLDOWN_SECONDS = 3 * 60
MAX_CROSS_JUMP = 1.50


def exact_outcome(ticks: pd.DataFrame, location: int, side: str) -> dict[str, float | int | None]:
    row = ticks.iloc[location]
    entry = float(row["ask"] if side == "long" else row["bid"])
    executable = ticks["bid"].to_numpy() if side == "long" else ticks["ask"].to_numpy()
    sign = 1.0 if side == "long" else -1.0
    times = ticks["timestamp_utc"].astype("int64").to_numpy()
    result: dict[str, float | int | None] = {"entry_price": entry}
    for horizon in (5, 15, 60):
        end = int(np.searchsorted(times, times[location] + horizon * 1_000_000_000, side="right"))
        path = sign * (executable[location + 1:end] - entry)
        result[f"mfe_{horizon}"] = float(path.max()) if len(path) else np.nan
        result[f"mae_{horizon}"] = float(max(0, -path.min())) if len(path) else np.nan
        result[f"pnl_{horizon}"] = float(path[-1]) if len(path) else np.nan
    end = int(np.searchsorted(times, times[location] + 15_000_000_000, side="right"))
    path = sign * (executable[location + 1:end] - entry)
    covered = np.flatnonzero(path >= 0)
    result["cover_ms"] = (
        int((times[location + 1 + covered[0]] - times[location]) / 1_000_000) if len(covered) else None
    )
    return result


def level_arrays(ticks: pd.DataFrame, prior: base.DayLevels) -> dict[str, np.ndarray]:
    count = len(ticks)
    arrays = {name: np.full(count, value) for name, value in prior.__dict__.items()}
    arrays["session_open"] = np.full(count, float(ticks["mid"].iloc[0]))
    arrays["quote_average"] = ticks["mid"].expanding().mean().to_numpy()
    opening_end = ticks["timestamp_utc"].iloc[0] + pd.Timedelta(minutes=30)
    opening = ticks[ticks["timestamp_utc"] <= opening_end]
    known = int(ticks["timestamp_utc"].searchsorted(opening_end, side="left"))
    opening_high = np.full(count, np.nan)
    opening_low = np.full(count, np.nan)
    opening_high[known:] = float(opening["mid"].max())
    opening_low[known:] = float(opening["mid"].min())
    arrays["opening_range_high"] = opening_high
    arrays["opening_range_low"] = opening_low
    return arrays


def candidates(day: str, ticks: pd.DataFrame, prior: base.DayLevels) -> list[dict[str, object]]:
    mid = ticks["mid"]
    bid_change = ticks["bid"].diff()
    ask_change = ticks["ask"].diff()
    jump = mid.diff().abs().rolling(HOLD_QUOTES, min_periods=HOLD_QUOTES).max()
    typical_spread = ticks["spread"].rolling(1200, min_periods=100).median()
    radius = pd.concat(
        [pd.Series(0.35, index=ticks.index), 2 * typical_spread], axis=1
    ).max(axis=1).clip(upper=1.25)
    timestamps_ns = ticks["timestamp_utc"].astype("int64").to_numpy()
    rows: list[dict[str, object]] = []
    for level_name, raw_level in level_arrays(ticks, prior).items():
        level = pd.Series(raw_level, index=ticks.index)
        gap = mid - level
        valid = level.notna() & typical_spread.notna()
        normal_market = (ticks["spread"] <= 1.5 * typical_spread) & (jump <= MAX_CROSS_JUMP)
        prior_min = gap.shift(HOLD_QUOTES).rolling(ARM_LOOKBACK_QUOTES, min_periods=10).min()
        prior_max = gap.shift(HOLD_QUOTES).rolling(ARM_LOOKBACK_QUOTES, min_periods=10).max()
        long_mask = (
            valid & normal_market & gap.gt(0) & gap.shift(1).gt(0) & gap.shift(2).gt(0)
            & prior_min.lt(0) & gap.abs().le(radius)
            & bid_change.gt(0) & ask_change.gt(0)
        )
        short_mask = (
            valid & normal_market & gap.lt(0) & gap.shift(1).lt(0) & gap.shift(2).lt(0)
            & prior_max.gt(0) & gap.abs().le(radius)
            & bid_change.lt(0) & ask_change.lt(0)
        )
        for side, mask in (("long", long_mask), ("short", short_mask)):
            last_ns: int | None = None
            for location in np.flatnonzero(mask.to_numpy()):
                if (
                    last_ns is not None
                    and timestamps_ns[location] - last_ns < COOLDOWN_SECONDS * 1_000_000_000
                ):
                    continue
                if location + 1 >= len(ticks):
                    continue
                last_ns = timestamps_ns[location]
                outcome = exact_outcome(ticks, int(location), side)
                spread = float(ticks.iloc[location]["spread"])
                cover_ms = outcome["cover_ms"]
                excellent = bool(
                    cover_ms is not None and cover_ms <= 5_000
                    and float(outcome["mfe_15"]) >= 1.0
                    and float(outcome["mae_15"]) <= max(spread + 0.25, 0.60)
                )
                rows.append({
                    "day": day,
                    "period": "later" if day >= base.LATER_PERIOD_START else "earlier",
                    "timestamp_utc": ticks.iloc[location]["timestamp_utc"].isoformat(),
                    "tick_id": int(ticks.iloc[location]["id"]),
                    "side": side,
                    "level": level_name,
                    "level_price": float(level.iloc[location]),
                    "mid": float(mid.iloc[location]),
                    "distance": float(abs(gap.iloc[location])),
                    "spread": spread,
                    "cross_jump": float(jump.iloc[location]),
                    **outcome,
                    "excellent_entry_proxy": excellent,
                })
    return rows


def main() -> None:
    paths = sorted(base.EXPORT_DIR.glob("ticks_XAUUSD_*.csv"))
    rows: list[dict[str, object]] = []
    previous_ticks = base.load_ticks(paths[0])
    for path in paths[1:]:
        day = path.stem.rsplit("_", 1)[-1]
        current_ticks = base.load_ticks(path)
        rows.extend(candidates(day, current_ticks, base.prior_day_levels(previous_ticks)))
        previous_ticks = current_ticks
    events = pd.DataFrame(rows).sort_values(["day", "timestamp_utc", "level"])
    summary = events.groupby(["period", "level", "side"]).agg(
        events=("tick_id", "size"), successes=("excellent_entry_proxy", "sum"),
        excellent_proxy_rate=("excellent_entry_proxy", "mean"),
        median_cover_ms=("cover_ms", "median"), median_mfe_15=("mfe_15", "median"),
        median_mae_15=("mae_15", "median"), median_mfe_60=("mfe_60", "median"),
    ).reset_index()
    events.to_csv(base.OUTPUT_DIR / "break_hold_events.csv", index=False)
    summary.to_csv(base.OUTPUT_DIR / "break_hold_summary.csv", index=False)
    target = events[(events["day"] == "2026-02-11") & events["tick_id"].between(31335920, 31335940)]
    report = {
        "schemaVersion": 1,
        "rule": {
            "description": "cross a known level, hold beyond it for three quotes, enter on synchronized bid/ask movement",
            "causal": True,
            "holdQuotes": HOLD_QUOTES,
            "armLookbackQuotes": ARM_LOOKBACK_QUOTES,
            "maximumCrossJump": MAX_CROSS_JUMP,
            "cooldownSecondsPerLevelAndSide": COOLDOWN_SECONDS,
        },
        "approvedFeb11EntryNearbyDetections": target.to_dict(orient="records"),
    }
    (base.OUTPUT_DIR / "break_hold_study.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(summary[(summary["events"] >= 10)].sort_values(
        ["period", "excellent_proxy_rate"], ascending=[True, False]
    ).to_string(index=False))
    print("\nFeb 11 approved-entry neighborhood:\n", target.to_string(index=False))


if __name__ == "__main__":
    main()
