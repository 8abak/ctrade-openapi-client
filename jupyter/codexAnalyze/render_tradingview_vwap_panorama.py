from __future__ import annotations

import argparse
from datetime import timedelta, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import hot_zone_study as study


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
CHART_TIMEZONE = timezone(timedelta(hours=10))
BANDS = (1.28, 2.01, 2.51)


def render(day: str) -> Path:
    day_value = pd.Timestamp(day)
    start_utc = day_value.tz_localize("UTC")
    end_utc = start_utc + pd.Timedelta(days=1)
    required_days = (day_value, day_value + pd.Timedelta(days=1))
    ticks = pd.concat(
        [
            study.load_ticks(
                REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{value:%Y-%m-%d}.csv"
            )
            for value in required_days
        ],
        ignore_index=True,
    )
    ticks = ticks.loc[ticks["timestamp_utc"].ge(start_utc) & ticks["timestamp_utc"].lt(end_utc)].copy()
    ticks = ticks.sort_values(["timestamp_utc", "id"], kind="stable")
    if ticks.empty:
        raise ValueError(f"No ticks available for UTC session {start_utc} -> {end_utc}")

    # Pine defaults: cumulative sum(hl2 * volume) / sum(volume). For the local
    # tick model, each quote is one unit of volume and tick hl2 is its midpoint.
    source = ((ticks["bid"] + ticks["ask"]) / 2).to_numpy(float)
    count = np.arange(1, len(ticks) + 1, dtype=float)
    cumulative = np.cumsum(source)
    cumulative_square = np.cumsum(source * source)
    ticks["vwap"] = cumulative / count
    variance = np.maximum(cumulative_square / count - ticks["vwap"].to_numpy() ** 2, 0)
    ticks["dev"] = np.sqrt(variance)
    sampled = ticks.set_index("timestamp_utc")[["bid", "ask", "vwap", "dev"]].resample("20s").last()
    sampled.index = sampled.index.tz_convert(CHART_TIMEZONE)

    plt.style.use("dark_background")
    fig, axis = plt.subplots(figsize=(32, 8), constrained_layout=True)
    fig.patch.set_facecolor("#081015")
    axis.set_facecolor("#081015")
    axis.plot(sampled.index, sampled["bid"], color="#36d9f5", linewidth=.8, label="BID")
    axis.plot(sampled.index, sampled["ask"], color="#ffad42", linewidth=.8, label="ASK")
    axis.plot(sampled.index, sampled["vwap"], color="#e5eef4", linewidth=1.8, label="VWAP")

    upper = [sampled["vwap"] + multiplier * sampled["dev"] for multiplier in BANDS]
    lower = [sampled["vwap"] - multiplier * sampled["dev"] for multiplier in BANDS]
    axis.plot(sampled.index, upper[0], color="#9ea8ae", linewidth=1.1, label="±1.28σ")
    axis.plot(sampled.index, lower[0], color="#9ea8ae", linewidth=1.1)
    axis.plot(sampled.index, upper[1], color="#ff5d64", linewidth=1.15, label="±2.01σ")
    axis.plot(sampled.index, lower[1], color="#48d597", linewidth=1.15)
    axis.plot(sampled.index, upper[2], color="#ff5d64", linewidth=1.0, alpha=.75, label="±2.51σ")
    axis.plot(sampled.index, lower[2], color="#48d597", linewidth=1.0, alpha=.75)
    axis.fill_between(sampled.index, sampled["vwap"], upper[0], color="#9ea8ae", alpha=.08)
    axis.fill_between(sampled.index, sampled["vwap"], lower[0], color="#9ea8ae", alpha=.08)
    axis.fill_between(sampled.index, upper[0], upper[1], color="#ff5d64", alpha=.09)
    axis.fill_between(sampled.index, lower[0], lower[1], color="#48d597", alpha=.09)
    axis.fill_between(sampled.index, upper[1], upper[2], color="#ff5d64", alpha=.07)
    axis.fill_between(sampled.index, lower[1], lower[2], color="#48d597", alpha=.07)

    chart_start = start_utc.tz_convert(CHART_TIMEZONE)
    chart_end = end_utc.tz_convert(CHART_TIMEZONE)
    axis.set_xlim(chart_start, chart_end)
    axis.xaxis.set_major_locator(mdates.HourLocator(interval=1, tz=CHART_TIMEZONE))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=CHART_TIMEZONE))
    axis.grid(True, color="#20303a", linewidth=.7)
    axis.tick_params(colors="#8fa4b2", labelsize=10)
    axis.set_ylabel("XAUUSD", color="#8fa4b2")
    axis.set_title(
        f"XAUUSD · {day} UTC daily anchor · cumulative equal-tick VWAP · TradingView defaults\n"
        f"{chart_start:%d %b %H:%M} – {chart_end:%d %b %H:%M} · chart timezone UTC+10",
        loc="left", color="#e5eef4", fontsize=16,
    )
    for spine in axis.spines.values():
        spine.set_color("#2b3d47")
    axis.legend(loc="upper right", ncol=7, frameon=False, fontsize=10, labelcolor="#a9bbc6")
    output = PROJECT_DIR / "history_data" / "screenshots" / f"{day}_tradingview_default_vwap_panorama.png"
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=170, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Pine-default cumulative VWAP bands from equal-weight ticks.")
    parser.add_argument("--day", default="2026-02-18")
    args = parser.parse_args()
    print(render(args.day))


if __name__ == "__main__":
    main()
