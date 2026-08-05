from __future__ import annotations

import argparse
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import hot_zone_study as study
from rolling_tick_vwap_band_study import rolling_indicator


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
SYDNEY = ZoneInfo("Australia/Sydney")


def render(day: str) -> Path:
    ticks = study.load_ticks(REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{day}.csv")
    indicator = rolling_indicator(ticks, 30)
    quotes = ticks.set_index("timestamp_utc")[["bid", "ask"]].resample("20s").last()
    # Preserve empty resample buckets so matplotlib breaks the line across data outages.
    view = quotes.join(indicator, how="left")
    view.index = view.index.tz_convert(SYDNEY)

    start = pd.Timestamp(day, tz=SYDNEY) + pd.Timedelta(hours=8)
    panels = [(start + pd.Timedelta(hours=8 * i), start + pd.Timedelta(hours=8 * (i + 1))) for i in range(3)]
    plt.style.use("dark_background")
    fig, axes = plt.subplots(3, 1, figsize=(18, 15), constrained_layout=True)
    fig.patch.set_facecolor("#081015")
    fig.suptitle(
        f"XAUUSD · {day} broker day · 30-minute equal-tick VWAP · Sydney time",
        color="#e5eef4", fontsize=20, x=.055, ha="left",
    )

    for panel_number, (axis, (panel_start, panel_end)) in enumerate(zip(axes, panels), start=1):
        data = view.loc[panel_start:panel_end].copy()
        axis.set_facecolor("#081015")
        axis.plot(data.index, data["bid"], color="#36d9f5", linewidth=.8, label="BID")
        axis.plot(data.index, data["ask"], color="#ffad42", linewidth=.8, label="ASK")
        axis.plot(data.index, data["vwap"], color="#e5eef4", linewidth=1.8, label="30m TICK VWAP")
        for multiplier, width, alpha in ((1, 1.25, .95), (2, 1.0, .66), (3, .9, .45)):
            upper = data["vwap"] + multiplier * data["stdev"]
            lower = data["vwap"] - multiplier * data["stdev"]
            axis.plot(data.index, upper, color="#ff5d64", linewidth=width, alpha=alpha)
            axis.plot(data.index, lower, color="#48d597", linewidth=width, alpha=alpha)
        axis.plot([], [], color="#ff5d64", linewidth=1.2, label="UPPER 1σ / 2σ / 3σ")
        axis.plot([], [], color="#48d597", linewidth=1.2, label="LOWER 1σ / 2σ / 3σ")
        axis.set_xlim(panel_start, panel_end)
        axis.xaxis.set_major_locator(mdates.HourLocator(interval=1, tz=SYDNEY))
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=SYDNEY))
        axis.grid(True, color="#20303a", linewidth=.7)
        axis.tick_params(colors="#8fa4b2", labelsize=11)
        axis.set_ylabel("XAUUSD", color="#8fa4b2")
        axis.set_title(
            f"{panel_start:%d %b %H:%M} – {panel_end:%d %b %H:%M}",
            loc="left", color="#b9c9d3", fontsize=13,
        )
        for spine in axis.spines.values():
            spine.set_color("#2b3d47")
        if panel_number == 1:
            axis.legend(loc="upper right", ncol=5, frameon=False, fontsize=10, labelcolor="#a9bbc6")

    output = PROJECT_DIR / "history_data" / "screenshots" / f"{day}_whole_day_vwap.png"
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=170, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a phone-readable whole broker-day VWAP chart.")
    parser.add_argument("--day", default="2026-02-12")
    args = parser.parse_args()
    print(render(args.day))


if __name__ == "__main__":
    main()
