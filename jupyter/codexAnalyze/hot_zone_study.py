from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from market_structure import structure_events


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
EXPORT_DIR = REPO_ROOT / "logs" / "sql_exports"
OUTPUT_DIR = PROJECT_DIR / "studies" / "hot_zones"
PIVOT_SECONDS = 3
COOLDOWN_SECONDS = 30
OPENING_RANGE_SECONDS = 30 * 60
LATER_PERIOD_START = "2026-02-06"


@dataclass(frozen=True)
class DayLevels:
    previous_high: float
    previous_low: float
    previous_close: float
    classic_pivot: float
    resistance_1: float
    support_1: float
    previous_midrange: float


def load_ticks(path: Path) -> pd.DataFrame:
    ticks = pd.read_csv(path, usecols=["id", "timestamp", "bid", "ask", "spread"])
    ticks["timestamp_utc"] = pd.to_datetime(ticks["timestamp"], format="mixed", utc=True)
    ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2
    return ticks


def second_bars(ticks: pd.DataFrame) -> pd.DataFrame:
    indexed = ticks.set_index("timestamp_utc")
    bars = indexed.resample("1s").agg(
        id=("id", "last"), bid=("bid", "last"), ask=("ask", "last"),
        spread=("spread", "last"), mid=("mid", "last"),
        tick_count=("id", "size"), quote_mid_sum=("mid", "sum"),
    )
    observed = bars["id"].notna()
    bars[["bid", "ask", "spread", "mid"]] = bars[["bid", "ask", "spread", "mid"]].ffill()
    bars["observed"] = observed
    bars["time_average"] = bars["mid"].expanding().mean()
    bars["quote_average"] = bars["quote_mid_sum"].cumsum() / bars["tick_count"].cumsum().replace(0, np.nan)
    bars["zone_radius"] = pd.concat(
        [
            pd.Series(0.35, index=bars.index),
            2 * bars["spread"].rolling(300, min_periods=30).median(),
            4 * bars["mid"].diff().abs().rolling(300, min_periods=30).median(),
        ],
        axis=1,
    ).max(axis=1).clip(upper=1.25)
    return bars


def prior_day_levels(ticks: pd.DataFrame) -> DayLevels:
    high = float(ticks["mid"].max())
    low = float(ticks["mid"].min())
    close = float(ticks["mid"].iloc[-1])
    pivot = (high + low + close) / 3
    return DayLevels(
        previous_high=high,
        previous_low=low,
        previous_close=close,
        classic_pivot=pivot,
        resistance_1=2 * pivot - low,
        support_1=2 * pivot - high,
        previous_midrange=(high + low) / 2,
    )


def causal_level_frame(bars: pd.DataFrame, prior: DayLevels) -> pd.DataFrame:
    levels = pd.DataFrame(index=bars.index)
    for name, value in prior.__dict__.items():
        levels[name] = value
    levels["session_open"] = float(bars["mid"].iloc[0])
    levels["time_average"] = bars["time_average"]
    levels["quote_average"] = bars["quote_average"]
    opening = bars.iloc[:OPENING_RANGE_SECONDS]
    opening_high = float(opening["mid"].max())
    opening_low = float(opening["mid"].min())
    levels["opening_range_high"] = np.nan
    levels["opening_range_low"] = np.nan
    if len(levels) > OPENING_RANGE_SECONDS:
        levels.iloc[OPENING_RANGE_SECONDS:, levels.columns.get_loc("opening_range_high")] = opening_high
        levels.iloc[OPENING_RANGE_SECONDS:, levels.columns.get_loc("opening_range_low")] = opening_low
    return levels


def executable_outcome(bars: pd.DataFrame, location: int, side: str) -> dict[str, float | int | None]:
    entry = bars.iloc[location]
    entry_price = float(entry["ask"] if side == "long" else entry["bid"])
    sign = 1.0 if side == "long" else -1.0
    executable = bars["bid"] if side == "long" else bars["ask"]
    result: dict[str, float | int | None] = {"entry_price": entry_price}
    for horizon in (5, 15, 60):
        future = executable.iloc[location + 1: location + horizon + 1]
        pnl_path = sign * (future - entry_price)
        result[f"mfe_{horizon}"] = float(pnl_path.max()) if len(pnl_path) else np.nan
        result[f"mae_{horizon}"] = float(max(0, -pnl_path.min())) if len(pnl_path) else np.nan
        result[f"pnl_{horizon}"] = float(pnl_path.iloc[-1]) if len(pnl_path) else np.nan
    cover_path = sign * (executable.iloc[location + 1: location + 16] - entry_price)
    covered = np.flatnonzero(cover_path.to_numpy() >= 0)
    result["cover_seconds"] = int(covered[0] + 1) if len(covered) else None
    return result


def event_rows(day: str, bars: pd.DataFrame, levels: pd.DataFrame) -> list[dict[str, object]]:
    events = structure_events(bars, pivot_seconds=PIVOT_SECONDS)
    rows: list[dict[str, object]] = []
    last_emitted = {"long": -COOLDOWN_SECONDS, "short": -COOLDOWN_SECONDS}
    level_names = list(levels.columns)
    for side, column in (("long", "bullish_structure_shift"), ("short", "bearish_structure_shift")):
        for location in np.flatnonzero(events[column].to_numpy()):
            if location - last_emitted[side] < COOLDOWN_SECONDS or location + 60 >= len(bars):
                continue
            if not bool(bars.iloc[location]["observed"]):
                continue
            last_emitted[side] = int(location)
            bar = bars.iloc[location]
            level_values = levels.iloc[location]
            radius = float(bar["zone_radius"])
            distances = (level_values - float(bar["mid"])).abs().dropna()
            near = distances[distances <= radius].sort_values()
            nearest_name = str(distances.idxmin()) if len(distances) else ""
            nearest_distance = float(distances.min()) if len(distances) else np.nan
            outcome = executable_outcome(bars, int(location), side)
            spread = float(bar["spread"])
            cover = outcome["cover_seconds"]
            excellent_proxy = bool(
                cover is not None and cover <= 5
                and float(outcome["mfe_15"]) >= 1.0
                and float(outcome["mae_15"]) <= max(spread + 0.25, 0.60)
            )
            rows.append({
                "day": day,
                "period": "later" if day >= LATER_PERIOD_START else "earlier",
                "timestamp_utc": bars.index[location].isoformat(),
                "tick_id": int(bar["id"]),
                "side": side,
                "bid": float(bar["bid"]),
                "ask": float(bar["ask"]),
                "spread": spread,
                "zone_radius": radius,
                "in_hot_zone": bool(len(near)),
                "near_zones": "|".join(map(str, near.index)),
                "confluence": int(len(near)),
                "nearest_zone": nearest_name,
                "nearest_distance": nearest_distance,
                **outcome,
                "covered_within_5s": bool(cover is not None and cover <= 5),
                "excellent_entry_proxy": excellent_proxy,
            })
    return rows


def zone_summary(events: pd.DataFrame) -> pd.DataFrame:
    exploded = events.assign(zone=events["near_zones"].str.split("|")).explode("zone")
    exploded = exploded[exploded["zone"].ne("")]
    baseline = events.assign(zone=np.where(events["in_hot_zone"], "any_hot_zone", "no_hot_zone"))
    data = pd.concat([exploded, baseline], ignore_index=True)
    return (
        data.groupby(["period", "zone"], dropna=False)
        .agg(
            events=("tick_id", "size"),
            cover_5s_rate=("covered_within_5s", "mean"),
            excellent_proxy_rate=("excellent_entry_proxy", "mean"),
            median_mfe_15=("mfe_15", "median"),
            median_mae_15=("mae_15", "median"),
            median_mfe_60=("mfe_60", "median"),
        )
        .reset_index()
        .sort_values(["period", "excellent_proxy_rate", "events"], ascending=[True, False, False])
    )


def confluence_summary(events: pd.DataFrame) -> pd.DataFrame:
    data = events.copy()
    data["zone_group"] = np.select(
        [data["confluence"].eq(0), data["confluence"].eq(1)],
        ["0 - no zone", "1 - single zone"],
        default="2+ - confluence",
    )
    return data.groupby(["period", "zone_group"]).agg(
        events=("tick_id", "size"),
        cover_5s_rate=("covered_within_5s", "mean"),
        excellent_proxy_rate=("excellent_entry_proxy", "mean"),
        median_mfe_15=("mfe_15", "median"),
        median_mae_15=("mae_15", "median"),
    ).reset_index()


def render_overview(zone_stats: pd.DataFrame, confluence: pd.DataFrame, output: Path) -> None:
    later = zone_stats[(zone_stats["period"] == "later") & (zone_stats["events"] >= 8)].copy()
    later = later.sort_values("excellent_proxy_rate", ascending=False).head(10)
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), facecolor="#081015")
    for ax in axes:
        ax.set_facecolor("#081015")
        ax.tick_params(colors="#9eb0bd")
        for spine in ax.spines.values():
            spine.set_color("#263640")
        ax.grid(axis="x", color="#263640", alpha=.65)
    axes[0].barh(later["zone"], 100 * later["excellent_proxy_rate"], color="#27c5d8")
    axes[0].invert_yaxis()
    axes[0].set_title("Later-period excellent-entry proxy by known zone", color="#e4edf2")
    axes[0].set_xlabel("rate (%)", color="#9eb0bd")
    later_conf = confluence[confluence["period"] == "later"]
    axes[1].bar(later_conf["zone_group"], 100 * later_conf["excellent_proxy_rate"], color="#f0aa3c")
    axes[1].set_title("Does level confluence help?", color="#e4edf2")
    axes[1].set_ylabel("excellent-entry proxy rate (%)", color="#9eb0bd")
    axes[1].tick_params(axis="x", rotation=15)
    fig.suptitle("XAUUSD causal hot-zone event study", color="#eef5f8", fontsize=16)
    fig.tight_layout()
    fig.savefig(output, dpi=150, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    paths = sorted(EXPORT_DIR.glob("ticks_XAUUSD_*.csv"))
    if len(paths) < 2:
        raise SystemExit("At least two daily exports are required")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cached_ticks: dict[str, pd.DataFrame] = {}
    all_rows: list[dict[str, object]] = []
    day_levels: list[dict[str, object]] = []
    for previous_path, current_path in zip(paths, paths[1:]):
        previous_day = previous_path.stem.rsplit("_", 1)[-1]
        day = current_path.stem.rsplit("_", 1)[-1]
        previous_ticks = cached_ticks.pop(previous_day, None)
        if previous_ticks is None:
            previous_ticks = load_ticks(previous_path)
        current_ticks = load_ticks(current_path)
        cached_ticks[day] = current_ticks
        prior = prior_day_levels(previous_ticks)
        bars = second_bars(current_ticks)
        levels = causal_level_frame(bars, prior)
        all_rows.extend(event_rows(day, bars, levels))
        day_levels.append({"day": day, **prior.__dict__})

    events = pd.DataFrame(all_rows).sort_values(["day", "timestamp_utc"])
    zone_stats = zone_summary(events)
    confluence = confluence_summary(events)
    day_stats = events.groupby(["day", "period", "in_hot_zone"]).agg(
        events=("tick_id", "size"),
        cover_5s_rate=("covered_within_5s", "mean"),
        excellent_proxy_rate=("excellent_entry_proxy", "mean"),
        median_mfe_15=("mfe_15", "median"),
        median_mae_15=("mae_15", "median"),
    ).reset_index()

    events.to_csv(OUTPUT_DIR / "hot_zone_events.csv", index=False)
    zone_stats.to_csv(OUTPUT_DIR / "hot_zone_summary.csv", index=False)
    confluence.to_csv(OUTPUT_DIR / "hot_zone_confluence.csv", index=False)
    day_stats.to_csv(OUTPUT_DIR / "hot_zone_daily.csv", index=False)
    pd.DataFrame(day_levels).to_csv(OUTPUT_DIR / "known_daily_levels.csv", index=False)
    render_overview(zone_stats, confluence, OUTPUT_DIR / "hot_zone_overview.png")

    overall = events.groupby(["period", "in_hot_zone"]).agg(
        events=("tick_id", "size"),
        cover_5s_rate=("covered_within_5s", "mean"),
        excellent_proxy_rate=("excellent_entry_proxy", "mean"),
        median_mfe_15=("mfe_15", "median"),
        median_mae_15=("mae_15", "median"),
    ).reset_index()
    report = {
        "schemaVersion": 1,
        "study": "causal hot-zone context around micro structure-shift entries",
        "days": [paths[1].stem.rsplit("_", 1)[-1], paths[-1].stem.rsplit("_", 1)[-1]],
        "daysAnalyzed": len(paths) - 1,
        "futureDataPolicy": {
            "zoneConstruction": "causal: previous broker day or expanding current-day data only",
            "entryTrigger": f"causal {PIVOT_SECONDS}-second pivot failure and structure break",
            "futureTicksUsedOnlyFor": "outcome measurement after the entry tick",
        },
        "vwapCaveat": "No traded volume exists in the exports. quote_average is quote-count weighted; time_average is a one-second time-weighted proxy.",
        "zoneRadius": "max(0.35, 2x trailing 5-minute median spread, 4x trailing median one-second move), capped at 1.25",
        "excellentEntryProxy": "spread covered within 5 seconds, 15-second MFE >= 1.00, and 15-second MAE <= max(spread + 0.25, 0.60)",
        "overall": overall.to_dict(orient="records"),
    }
    (OUTPUT_DIR / "hot_zone_study.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
