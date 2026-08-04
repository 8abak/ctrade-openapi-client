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
    BAND_STDEV,
    MAX_PRIOR_FIVE_SECOND_JUMP,
    PIVOT_SECONDS,
    SPREAD_RATIO_LIMIT,
    WINDOW_MINUTES,
    causal_candidates,
)
from market_structure import first_flow_break_events
from rolling_tick_vwap_band_study import rolling_indicator


PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_DIR / "studies" / "one_time_reversal"
ACCEPTANCE_SECONDS = 3
STRUCTURE_MEMORY_SECONDS = 30
REVERSAL_SEARCH_SECONDS = 60
RECOVERY_SEARCH_SECONDS = 300


def _first_true(mask: pd.Series) -> pd.Timestamp | None:
    matches = mask.index[mask.fillna(False)]
    return matches[0] if len(matches) else None


def _simulate_recovery(
    ticks: pd.DataFrame,
    initial: dict[str, object],
    reversal_timestamp: pd.Timestamp,
    reversal_side: str,
) -> dict[str, object]:
    reversal_tick = ticks.loc[ticks["timestamp_utc"].le(reversal_timestamp)].iloc[-1]
    initial_entry = float(initial["ask"] if initial["side"] == "long" else initial["bid"])
    if initial["side"] == "long":
        initial_realized = float(reversal_tick["bid"]) - initial_entry
    else:
        initial_realized = initial_entry - float(reversal_tick["ask"])

    end = reversal_timestamp + pd.Timedelta(seconds=RECOVERY_SEARCH_SECONDS)
    path = ticks.loc[ticks["timestamp_utc"].between(reversal_timestamp, end)].copy()
    if reversal_side == "long":
        reversal_entry = float(reversal_tick["ask"])
        move = path["bid"] - reversal_entry
    else:
        reversal_entry = float(reversal_tick["bid"])
        move = reversal_entry - path["ask"]
    double_total = initial_realized + 2.0 * move
    single_total = initial_realized + move
    double_matches = path.loc[double_total.ge(0), "timestamp_utc"]
    single_matches = path.loc[single_total.ge(0), "timestamp_utc"]
    double_recovery = double_matches.iloc[0] if len(double_matches) else None
    single_recovery = single_matches.iloc[0] if len(single_matches) else None
    if double_recovery is not None:
        recovered_mask = path["timestamp_utc"].ge(double_recovery)
        recovery_location = path.index[recovered_mask][0]
        recovery_move = float(move.loc[recovery_location])
        scaled_total = double_total.copy()
        scaled_total.loc[recovered_mask] = initial_realized + recovery_move + move.loc[recovered_mask]
        risk_path = double_total.loc[~path["timestamp_utc"].gt(double_recovery)]
    else:
        scaled_total = double_total
        risk_path = double_total

    return {
        "reversal_tick_id": int(reversal_tick["id"]),
        "reversal_timestamp_utc": reversal_timestamp.isoformat(),
        "reversal_side": reversal_side,
        "initial_realized_loss": float(initial_realized),
        "reversal_entry": reversal_entry,
        "double_recovered": double_recovery is not None,
        "double_recovery_seconds": (
            float((double_recovery - reversal_timestamp).total_seconds())
            if double_recovery is not None else None
        ),
        "single_recovered": single_recovery is not None,
        "single_recovery_seconds": (
            float((single_recovery - reversal_timestamp).total_seconds())
            if single_recovery is not None else None
        ),
        "max_adverse_before_scale_down": float(risk_path.min()),
        "double_max_favorable": float(double_total.max()),
        "scaled_pnl_60": float(
            scaled_total.loc[path["timestamp_utc"].le(reversal_timestamp + pd.Timedelta(seconds=60))].iloc[-1]
        ),
        "scaled_pnl_300": float(scaled_total.iloc[-1]),
    }


def analyze_day(csv_path: Path) -> list[dict[str, object]]:
    ticks, candidates = causal_candidates(csv_path)
    bars = study.second_bars(ticks)
    indicator = rolling_indicator(ticks, WINDOW_MINUTES).reindex(bars.index).ffill()
    events = first_flow_break_events(bars, pivot_seconds=PIVOT_SECONDS)
    upper = indicator["vwap"] + BAND_STDEV * indicator["stdev"]
    lower = indicator["vwap"] - BAND_STDEV * indicator["stdev"]
    above = bars["mid"].gt(upper).rolling(ACCEPTANCE_SECONDS, min_periods=ACCEPTANCE_SECONDS).sum().eq(ACCEPTANCE_SECONDS)
    below = bars["mid"].lt(lower).rolling(ACCEPTANCE_SECONDS, min_periods=ACCEPTANCE_SECONDS).sum().eq(ACCEPTANCE_SECONDS)
    spread_median = bars["spread"].rolling(300, min_periods=30).median()
    spread_ok = (bars["spread"] / spread_median.replace(0, np.nan)).le(SPREAD_RATIO_LIMIT)
    jump_ok = bars["mid"].diff().abs().rolling(5, min_periods=5).max().le(MAX_PRIOR_FIVE_SECOND_JUMP)
    common = bars["observed"] & spread_ok & jump_ok
    recent_long_break = (
        events["long_first_flow_break"].rolling(STRUCTURE_MEMORY_SECONDS, min_periods=1).max().astype(bool)
    )
    recent_short_break = (
        events["short_first_flow_break"].rolling(STRUCTURE_MEMORY_SECONDS, min_periods=1).max().astype(bool)
    )

    rows: list[dict[str, object]] = []
    for initial in candidates:
        timestamp = pd.Timestamp(int(initial["timestamp_ms"]), unit="ms", tz="UTC")
        search = bars.index.to_series().between(
            timestamp + pd.Timedelta(seconds=1),
            timestamp + pd.Timedelta(seconds=REVERSAL_SEARCH_SECONDS),
        )
        if initial["side"] == "short":
            reversal_side = "long"
            trigger = search & common & above & recent_long_break
        else:
            reversal_side = "short"
            trigger = search & common & below & recent_short_break
        reversal_timestamp = _first_true(trigger)
        base = {
            "day": csv_path.stem.rsplit("_", 1)[-1],
            "initial_sequence": int(initial["sequence"]),
            "initial_tick_id": int(initial["tick_id"]),
            "initial_timestamp_utc": timestamp.isoformat(),
            "initial_side": str(initial["side"]),
            "reversal_triggered": reversal_timestamp is not None,
        }
        if reversal_timestamp is not None:
            result = _simulate_recovery(ticks, initial, reversal_timestamp, reversal_side)
            # Recovery is meaningful only when the first leg was closed at a loss.
            if float(result["initial_realized_loss"]) < 0:
                base.update(result)
            else:
                base["reversal_triggered"] = False
                base["rejection_reason"] = "initial leg was not losing at opposite confirmation"
        rows.append(base)
    return rows


def _segment_summary(triggered: pd.DataFrame) -> dict[str, object]:
    recovered = triggered.loc[triggered["double_recovered"]]
    return {
        "causal_reversals": int(len(triggered)),
        "double_recovered_300s": int(triggered["double_recovered"].sum()) if len(triggered) else 0,
        "double_recovery_rate_300s": float(triggered["double_recovered"].mean()) if len(triggered) else None,
        "single_recovered_300s": int(triggered["single_recovered"].sum()) if len(triggered) else 0,
        "single_recovery_rate_300s": float(triggered["single_recovered"].mean()) if len(triggered) else None,
        "median_double_recovery_seconds": float(recovered["double_recovery_seconds"].median()) if len(recovered) else None,
        "median_initial_loss": float(triggered["initial_realized_loss"].median()) if len(triggered) else None,
        "median_max_adverse_before_scale_down": float(triggered["max_adverse_before_scale_down"].median()) if len(triggered) else None,
        "worst_max_adverse_before_scale_down": float(triggered["max_adverse_before_scale_down"].min()) if len(triggered) else None,
        "mean_scaled_pnl_300": float(triggered["scaled_pnl_300"].mean()) if len(triggered) else None,
    }


def summarize(frame: pd.DataFrame) -> dict[str, object]:
    triggered = frame.loc[frame["reversal_triggered"] & frame["initial_realized_loss"].notna()].copy()
    result = {
        "initial_candidates": int(len(frame)),
        "causal_reversal_rate": float(len(triggered) / len(frame)) if len(frame) else None,
        **_segment_summary(triggered),
    }
    triggered["period"] = np.where(triggered["day"].ge("2026-02-06"), "later", "earlier")
    result["periods"] = {
        period: _segment_summary(group) for period, group in triggered.groupby("period", sort=True)
    }
    loss = -triggered["initial_realized_loss"]
    caps: dict[str, object] = {}
    for multiple in (3, 5, 10):
        recovered_before_cap = (
            triggered["double_recovered"]
            & triggered["max_adverse_before_scale_down"].ge(-multiple * loss)
        )
        caps[f"{multiple}x_initial_loss"] = {
            "recoveredBeforeCap": int(recovered_before_cap.sum()),
            "recoveryRate": float(recovered_before_cap.mean()) if len(triggered) else None,
            "stoppedOrNotRecovered": int((~recovered_before_cap).sum()),
        }
    result["riskCaps"] = caps
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Study a causal one-time 2x stop-and-reverse recovery rule.")
    parser.add_argument("--day")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    pattern = f"ticks_XAUUSD_{args.day}.csv" if args.day else "ticks_XAUUSD_2026-*.csv"
    paths = sorted((REPO_ROOT / "logs" / "sql_exports").glob(pattern))
    rows: list[dict[str, object]] = []
    if args.workers > 1 and len(paths) > 1:
        print(f"analyzing {len(paths)} days with {args.workers} workers", flush=True)
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            for path, day_rows in zip(paths, executor.map(analyze_day, paths)):
                print(f"completed {path.name}", flush=True)
                rows.extend(day_rows)
    else:
        for path in paths:
            print(f"analyzing {path.name}", flush=True)
            rows.extend(analyze_day(path))
    frame = pd.DataFrame(rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_output = args.output_dir / "one_time_reversal_events.csv"
    json_output = args.output_dir / "one_time_reversal_summary.json"
    frame.to_csv(csv_output, index=False)
    payload = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "rule": {
            "maximumReversals": 1,
            "secondLegSize": 2,
            "acceptanceSecondsBeyondBand": ACCEPTANCE_SECONDS,
            "oppositeConfirmation": (
                f"causal {PIVOT_SECONDS}-second first-flow break retained for "
                f"{STRUCTURE_MEMORY_SECONDS} seconds, then band acceptance"
            ),
            "reversalSearchSeconds": REVERSAL_SEARCH_SECONDS,
            "recoverySearchSeconds": RECOVERY_SEARCH_SECONDS,
            "costModel": "longs enter ask/exit bid; shorts enter bid/exit ask",
            "scaleDown": "close one recovery unit as soon as combined executable P&L reaches zero",
        },
        "summary": summarize(frame),
    }
    json_output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(csv_output)
    print(json_output)


if __name__ == "__main__":
    main()
