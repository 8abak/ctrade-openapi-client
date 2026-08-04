from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import hot_zone_study as study


def interaction(side: str, prior_gap: float, radius: float) -> str:
    if prior_gap > radius:
        return "reaction" if side == "long" else "breakout"
    if prior_gap < -radius:
        return "breakout" if side == "long" else "reaction"
    return "inside"


def accepted_entries() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in sorted(study.PROJECT_DIR.joinpath("history_data").glob("chat_learning_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        day = str(payload["day"])
        for decision in payload.get("decisions", []):
            if decision.get("rating") != "excellent" or not decision.get("userAccepted"):
                continue
            tick_id = next((
                decision.get(key) for key in (
                    "causalMicroTriggerTickId", "retrospectiveIdealEntryTickId", "correctedEntryTickId"
                ) if decision.get(key) is not None
            ), None)
            if tick_id is None:
                continue
            signal_id = str(decision.get("signalId", ""))
            side = "long" if "-long-" in signal_id else "short"
            rows.append({"day": day, "tick_id": int(tick_id), "side": side, "source": path.name})
    retrospective = study.PROJECT_DIR / "history_data" / "retrospective_candidates_2026-02-11.json"
    if retrospective.exists():
        payload = json.loads(retrospective.read_text(encoding="utf-8"))
        for candidate in payload.get("candidates", []):
            if candidate.get("rating") == "excellent" and candidate.get("userAccepted"):
                rows.append({
                    "day": str(payload["day"]), "tick_id": int(candidate["studyEntryTickId"]),
                    "side": str(candidate["side"]), "source": retrospective.name,
                })
    return rows


def context_at_tick(
    day: str, tick_id: int, side: str, bars: pd.DataFrame, levels: pd.DataFrame, ticks: pd.DataFrame
) -> dict[str, object]:
    tick_match = ticks.index[ticks["id"].eq(tick_id)]
    if not len(tick_match):
        return {"day": day, "tick_id": tick_id, "side": side, "error": "tick_not_found"}
    tick = ticks.loc[tick_match[0]]
    timestamp = tick["timestamp_utc"].floor("s")
    location = int(bars.index.searchsorted(timestamp, side="right")) - 1
    radius = float(bars.iloc[location]["zone_radius"])
    price = float(tick["mid"])
    values = levels.iloc[location].dropna()
    distances = (values - price).abs().sort_values()
    near = distances[distances <= radius]
    prior_location = max(0, location - 30)
    details: list[str] = []
    for name in near.index:
        prior_gap = float(bars.iloc[prior_location]["mid"] - levels.iloc[prior_location][name])
        details.append(f"{name}:{interaction(side, prior_gap, radius)}")
    return {
        "day": day, "tick_id": tick_id, "side": side, "timestamp_utc": tick["timestamp_utc"].isoformat(),
        "mid": price, "zone_radius": radius, "in_hot_zone": bool(len(near)),
        "near_zones": "|".join(map(str, near.index)),
        "zone_interactions": "|".join(details),
        "nearest_zone": str(distances.index[0]), "nearest_distance": float(distances.iloc[0]),
    }


def main() -> None:
    event_path = study.OUTPUT_DIR / "hot_zone_events.csv"
    events = pd.read_csv(event_path)
    events["zone_interactions"] = ""
    accepted = accepted_entries()
    accepted_rows: list[dict[str, object]] = []
    paths = sorted(study.EXPORT_DIR.glob("ticks_XAUUSD_*.csv"))
    path_by_day = {path.stem.rsplit("_", 1)[-1]: path for path in paths}
    days = sorted(set(events["day"]).union(str(row["day"]) for row in accepted))
    for day in days:
        current_path = path_by_day[day]
        current_index = paths.index(current_path)
        if current_index == 0:
            continue
        ticks = study.load_ticks(current_path)
        prior_ticks = study.load_ticks(paths[current_index - 1])
        bars = study.second_bars(ticks)
        levels = study.causal_level_frame(bars, study.prior_day_levels(prior_ticks))
        day_mask = events["day"].eq(day)
        for row_index, row in events[day_mask & events["in_hot_zone"]].iterrows():
            timestamp = pd.Timestamp(row["timestamp_utc"])
            location = int(bars.index.searchsorted(timestamp, side="right")) - 1
            prior_location = max(0, location - 30)
            radius = float(row["zone_radius"])
            labels: list[str] = []
            for name in str(row["near_zones"]).split("|"):
                prior_gap = float(bars.iloc[prior_location]["mid"] - levels.iloc[prior_location][name])
                labels.append(f"{name}:{interaction(str(row['side']), prior_gap, radius)}")
            events.at[row_index, "zone_interactions"] = "|".join(labels)
        for item in (row for row in accepted if row["day"] == day):
            accepted_rows.append({
                **item,
                **context_at_tick(day, int(item["tick_id"]), str(item["side"]), bars, levels, ticks),
            })

    events.to_csv(event_path, index=False)
    exploded = events.assign(interaction_zone=events["zone_interactions"].str.split("|")).explode("interaction_zone")
    exploded = exploded[exploded["interaction_zone"].ne("")].copy()
    exploded[["zone", "interaction"]] = exploded["interaction_zone"].str.rsplit(":", n=1, expand=True)
    summary = exploded.groupby(["period", "zone", "interaction"]).agg(
        events=("tick_id", "size"),
        cover_5s_rate=("covered_within_5s", "mean"),
        excellent_proxy_rate=("excellent_entry_proxy", "mean"),
        median_mfe_15=("mfe_15", "median"),
        median_mae_15=("mae_15", "median"),
        median_mfe_60=("mfe_60", "median"),
    ).reset_index()
    summary.to_csv(study.OUTPUT_DIR / "hot_zone_interactions.csv", index=False)
    accepted_frame = pd.DataFrame(accepted_rows)
    accepted_frame.to_csv(study.OUTPUT_DIR / "accepted_entry_zone_context.csv", index=False)
    print(summary[(summary["period"] == "later") & (summary["events"] >= 8)]
          .sort_values("excellent_proxy_rate", ascending=False).to_string(index=False))
    print("\nAccepted entries:\n", accepted_frame.to_string(index=False))


if __name__ == "__main__":
    main()
