from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import hot_zone_study as study
from market_structure import structure_events


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
PIVOT_SECONDS = 3
COOLDOWN_SECONDS = 30
MAX_PRIOR_FIVE_SECOND_JUMP = 1.25
SPREAD_RATIO_LIMIT = 1.50


def outcome(ticks: pd.DataFrame, timestamp: pd.Timestamp, side: str) -> dict[str, float | int | None]:
    times = ticks["timestamp_utc"].astype("int64").to_numpy()
    location = int(np.searchsorted(times, timestamp.value, side="right")) - 1
    row = ticks.iloc[location]
    entry = float(row["ask"] if side == "long" else row["bid"])
    executable = ticks["bid"].to_numpy() if side == "long" else ticks["ask"].to_numpy()
    sign = 1.0 if side == "long" else -1.0
    result: dict[str, float | int | None] = {}
    for seconds in (5, 15, 60):
        end = int(np.searchsorted(times, times[location] + seconds * 1_000_000_000, side="right"))
        path = sign * (executable[location + 1:end] - entry)
        result[f"mfe{seconds}"] = round(float(path.max()), 4) if len(path) else None
        result[f"mae{seconds}"] = round(float(max(0, -path.min())), 4) if len(path) else None
        result[f"pnl{seconds}"] = round(float(path[-1]), 4) if len(path) else None
    end = int(np.searchsorted(times, times[location] + 15_000_000_000, side="right"))
    path = sign * (executable[location + 1:end] - entry)
    covered = np.flatnonzero(path >= 0)
    result["spreadCoverMs"] = (
        int((times[location + 1 + covered[0]] - times[location]) / 1_000_000) if len(covered) else None
    )
    return result


def causal_candidates(csv_path: Path, side: str = "long") -> tuple[pd.DataFrame, list[dict[str, object]]]:
    ticks = study.load_ticks(csv_path)
    bars = study.second_bars(ticks)
    events = structure_events(bars, pivot_seconds=PIVOT_SECONDS)
    mid = bars["mid"]
    quote_average = bars["quote_average"]
    distance = (mid - quote_average).abs()
    approach_gap = mid.shift(30) - quote_average.shift(30)
    spread_median = bars["spread"].rolling(300, min_periods=30).median()
    spread_ratio = bars["spread"] / spread_median.replace(0, np.nan)
    max_jump = mid.diff().abs().rolling(5, min_periods=5).max()
    radius = bars["zone_radius"]
    event_column = "bullish_structure_shift" if side == "long" else "bearish_structure_shift"
    reaction_approach = approach_gap.gt(radius) if side == "long" else approach_gap.lt(-radius)
    mask = (
        events[event_column]
        & bars["observed"]
        & distance.le(radius)
        & reaction_approach
        & spread_ratio.le(SPREAD_RATIO_LIMIT)
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
            "id": f"hot-zone-quote-reaction-{side}-{int(bar['id'])}",
            "tick_id": int(bar["id"]),
            "timestamp_ms": int(timestamp.value // 1_000_000),
            "side": side,
            "bid": float(bar["bid"]),
            "ask": float(bar["ask"]),
            "zone": "expanding_quote_average",
            "zone_price": float(quote_average.loc[timestamp]),
            "zone_distance": float(distance.loc[timestamp]),
            "zone_radius": float(radius.loc[timestamp]),
            "features": {
                "pivot_seconds": PIVOT_SECONDS,
                "spread_ratio": float(spread_ratio.loc[timestamp]),
                "maximum_prior_5s_jump": float(max_jump.loc[timestamp]),
                "interaction": "reaction",
            },
        })
    return ticks, selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate causal expanding-quote-average reaction candidates.")
    parser.add_argument("--day", default="2026-02-12")
    parser.add_argument("--side", choices=("long", "short"), default="long")
    args = parser.parse_args()
    csv_path = REPO_ROOT / f"logs/sql_exports/ticks_XAUUSD_{args.day}.csv"
    ticks, candidates = causal_candidates(csv_path, args.side)
    for candidate in candidates:
        timestamp = pd.Timestamp(int(candidate["timestamp_ms"]), unit="ms", tz="UTC")
        candidate["outcomeAudit"] = outcome(ticks, timestamp, args.side)
    payload = {
        "schemaVersion": 1,
        "day": args.day,
        "mode": "causal_quote_average_reaction_replay",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "selectionPolicy": {
            "futureTicksUsed": False,
            "side": args.side,
            "zone": "expanding quote-count average (not true volume VWAP)",
            "trigger": f"causal {PIVOT_SECONDS}-second HH/LL failure-and-break inside the zone after a reaction approach",
            "continuity": f"maximum prior five-second one-second jump <= {MAX_PRIOR_FIVE_SECOND_JUMP}",
            "spreadRatioLimit": SPREAD_RATIO_LIMIT,
            "cooldownSeconds": COOLDOWN_SECONDS,
        },
        "outcomePolicy": "outcomeAudit is appended only after the complete causal candidate list is frozen",
        "signals": candidates,
    }
    output = PROJECT_DIR / "history_data" / f"hot_zone_signals_{args.day}.json"
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(output)
    print(f"candidates={len(candidates)}")
    for item in candidates:
        print(item["sequence"], item["tick_id"], item["side"], item["outcomeAudit"])


if __name__ == "__main__":
    main()
