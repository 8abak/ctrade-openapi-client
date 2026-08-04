from __future__ import annotations

import argparse
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent


def render(
    day: str, sequence: int, minutes_before: int, minutes_after: int,
    override_tick_id: int | None = None, override_side: str | None = None,
    direct_tick_id: int | None = None, direct_side: str | None = None,
    study_label: str = "retrospective micro discovery",
    provenance_label: str = "hindsight-selected for pattern study",
    overlay_quote_average: bool = False,
) -> Path:
    csv_path = REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{day}.csv"
    ticks = pd.read_csv(csv_path, usecols=["id", "timestamp", "bid", "ask", "spread"])
    ticks["time"] = pd.to_datetime(ticks["timestamp"], format="mixed", utc=True)
    if overlay_quote_average:
        ticks["quote_average"] = ((ticks["bid"] + ticks["ask"]) / 2).expanding().mean()
    is_direct = direct_tick_id is not None
    if is_direct:
        if direct_side is None:
            raise ValueError("--direct-side is required with --direct-tick-id")
        payload = {"signals": []}
        signal = {"sequence": sequence, "side": direct_side}
    else:
        signal_path = PROJECT_DIR / f"history_data/signals_{day}.json"
        payload = json.loads(signal_path.read_text(encoding="utf-8"))
        signal = next(item for item in payload["signals"] if int(item["sequence"]) == sequence)
    selected_tick_id = direct_tick_id if is_direct else override_tick_id
    is_override = selected_tick_id is not None
    if is_override:
        row = ticks.loc[ticks["id"].eq(selected_tick_id)]
        if row.empty:
            raise ValueError(f"Tick ID {selected_tick_id} is not present in {day}")
        row = row.iloc[0]
        signal = dict(signal)
        signal.update({
            "tick_id": int(row["id"]), "timestamp_ms": int(row["time"].timestamp() * 1000),
            "bid": float(row["bid"]), "ask": float(row["ask"]),
            "side": direct_side if is_direct else (override_side or signal["side"]),
        })
        if is_direct:
            payload["signals"] = [signal]
    signal_time = pd.Timestamp(int(signal["timestamp_ms"]), unit="ms", tz="UTC")
    start = signal_time - pd.Timedelta(minutes=minutes_before)
    end = signal_time + pd.Timedelta(minutes=minutes_after)
    view = ticks.loc[ticks["time"].between(start, end)].copy()
    if view.empty:
        raise ValueError("No ticks in requested chart window")
    # Preserve extrema while reducing drawing overhead: last tick per 100 ms.
    view = view.set_index("time").resample("100ms").last().dropna().reset_index()
    local_tz = ZoneInfo("Australia/Sydney")
    view["local_time"] = view["time"].dt.tz_convert(local_tz)
    local_signal = signal_time.tz_convert(local_tz)

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, ax = plt.subplots(figsize=(16, 9), dpi=120)
    fig.patch.set_facecolor("#070c10")
    ax.set_facecolor("#070c10")
    ax.plot(view["local_time"], view["bid"], color="#29d6f2", linewidth=1.05, label="BID")
    ax.plot(view["local_time"], view["ask"], color="#ffb347", linewidth=1.05, label="ASK")
    if overlay_quote_average:
        ax.plot(
            view["local_time"], view["quote_average"], color="#d86cff", linewidth=1.35,
            linestyle=(0, (6, 3)), label="QUOTE AVG",
        )
    ax.axvline(local_signal, color="#91a4b2", linestyle=(0, (4, 4)), linewidth=1.1)
    if not is_override:
        break_level = float(signal["features"]["break_level"])
        ax.hlines(
            break_level,
            xmin=local_signal - pd.Timedelta(minutes=min(3, minutes_before)),
            xmax=local_signal,
            color="#58d68d" if signal["side"] == "long" else "#ff5c5c",
            linewidth=1.2,
            linestyle=(0, (5, 4)),
            alpha=.9,
        )
    marker_price = float(signal["ask"] if signal["side"] == "long" else signal["bid"])
    marker = "^" if signal["side"] == "long" else "v"
    color = "#58d68d" if signal["side"] == "long" else "#ff5c5c"
    ax.scatter([local_signal], [marker_price], marker=marker, s=65, color=color, zorder=5)
    ax.annotate(
        f"{'MICRO ' if is_override else ''}{signal['side'].upper()}  ID {signal['tick_id']}",
        xy=(local_signal, marker_price), xytext=(8, 8 if signal["side"] == "long" else -18),
        textcoords="offset points", color=color, fontsize=9, weight="bold",
    )
    ax.set_title(
        f"XAUUSD · {day} · Signal {sequence}/{len(payload['signals'])} · "
        f"{signal['side'].upper()} · {study_label if is_direct else ('failed-retest micro trigger' if is_override else 'first macro flow break')}",
        color="#dce7ef", fontsize=14, loc="left", pad=14,
    )
    detail = (
        (provenance_label if is_direct else "tick-level retest confirmation") if is_override
        else f"rank {signal['score']*100:.1f} · scales {'+'.join(map(str, signal['features']['scales']))}s"
    )
    ax.text(
        1, 1.018,
        f"Sydney {local_signal:%d %b %H:%M:%S}  ·  {detail}",
        transform=ax.transAxes, ha="right", va="bottom", color="#8296a5", fontsize=9,
    )
    ax.grid(True, color="#1b2a34", linewidth=.8)
    for spine in ax.spines.values():
        spine.set_color("#293943")
    ax.tick_params(colors="#8296a5", length=0, pad=8)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=7, integer=True))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=7, maxticks=11))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=local_tz))
    ax.legend(loc="upper right", frameon=False, labelcolor="#8296a5", fontsize=9)
    ax.margins(x=0, y=.08)
    fig.tight_layout(pad=2)

    output_dir = PROJECT_DIR / "history_data" / "screenshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = "study" if is_direct else ("micro" if is_override else "signal")
    output = output_dir / f"{day}_{suffix}_{sequence:02d}_{signal['side']}_{signal['tick_id']}.png"
    fig.savefig(output, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render one replay signal for chat review.")
    parser.add_argument("--day", required=True)
    parser.add_argument("--sequence", type=int, required=True)
    parser.add_argument("--minutes-before", type=int, default=5)
    parser.add_argument("--minutes-after", type=int, default=5)
    parser.add_argument("--override-tick-id", type=int)
    parser.add_argument("--override-side", choices=("long", "short"))
    parser.add_argument("--direct-tick-id", type=int)
    parser.add_argument("--direct-side", choices=("long", "short"))
    parser.add_argument("--study-label", default="retrospective micro discovery")
    parser.add_argument("--provenance-label", default="hindsight-selected for pattern study")
    parser.add_argument("--overlay-quote-average", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(render(
        args.day, args.sequence, args.minutes_before, args.minutes_after,
        args.override_tick_id, args.override_side,
        args.direct_tick_id, args.direct_side, args.study_label, args.provenance_label,
        args.overlay_quote_average,
    ))
