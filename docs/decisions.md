<!-- docs/decisions.md -->
## 2025-12-09 — [Structure] — Peer-Based Tick Importance (evals)

- Context:  
  Legacy structural layers (zones, pivots, swings, levels, etc.) for XAUUSD are being replaced by a peer-comparison importance ladder, driven purely by tick prices. Existing structure tables are no longer used in new ML/visualisation flows.

- Decision:  
  Introduce a new core table `evals` with 1:1 grain to `ticks.id` for XAUUSD.  
  - Phase A computes `base_sign`, `level` (0/1), and `signed_importance` per tick, with `mid` and `timestamp` denormalised from `ticks`.  
  - Phase B iteratively promotes ticks to higher levels using odd-sized, sign-consistent windows of peers, with monotone level increases and immutable sign.  
  - Legacy pivot / swing / zone / level tables for XAUUSD are dropped, and routes/views depending on them are removed. All structural overlays must come from `evals`.

- Consequences:  
  - `evals` becomes the single source of truth for structural importance for XAUUSD.  
  - Jobs are added to maintain `evals` in batch mode (`python -m jobs.build_evals`).  
  - A new backend route `/api/evals` exposes evals for charting and ML.  
  - Frontend overlays for structural points now query `/api/evals` and no longer use `zones`, `piv*`, `swg*`, or other legacy structure tables.


# Architectural and Structural Decisions — Segmeling / datavis.au

This file records every decision that affects the architecture, database schema, API surface, or development workflow.

Format:

- YYYY-MM-DD — [Area] — Title  
  - Context:  
  - Decision:  
  - Consequences:  

New decisions must be appended to this file in chronological order.

---

## 2025-12-06 — [DB] — Live Schema Snapshot System

- Context:  
  We require always-current documentation of all tables, columns, indexes, and constraints in the `trading` PostgreSQL database.

- Decision:  
  `docs/db-schema.txt` is regenerated on every deployment by running `python -m jobs.buildSchema` on the EC2 instance. This file is the canonical representation of the database schema.

- Consequences:  
  - All SQL and database-related code must be written to match `docs/db-schema.txt`.  
  - Custom GPTs must read `docs/db-schema.txt` before generating SQL or schema changes.  
  - Schema modifications must be followed by a rebuild of `docs/db-schema.txt`.

---

## 2025-12-06 — [API] — Route and DB Function Mapping

- Context:  
  It was difficult to track which API routes and DB helper functions existed, leading to unused endpoints and confusion.

- Decision:  
  `jobs/buildRoots.py` generates `docs/routes-and-db.txt`, listing all detected HTTP routes in `backend/main.py` and DB helper functions in `backend/db.py`.

- Consequences:  
  - Custom GPTs must read `docs/routes-and-db.txt` before adding, changing, or removing routes or DB helpers.  
  - Redundant or obsolete routes can be identified and removed in a controlled way.  
  - New routes must be consistent with the existing mapping.

---

## 2025-12-06 — [Frontend] — Centralized Chart Logic

- Context:  
  Chart behavior had been implemented in multiple JS files, making it hard to keep zoom, pan, and overlays consistent.

- Decision:  
  All chart behavior is consolidated into `frontend/chart-core.js`. Page-specific files (`tick-core.js`, `htick-core.js`, `review-core.js`) are responsible only for page logic and must use the public API of `chart-core.js`.

- Consequences:  
  - Any change to chart behavior (zoom, pan, tooltip, overlays) must be made in `chart-core.js`.  
  - Page files must not implement their own chart engines.  
  - Custom GPTs must preserve this separation and may not duplicate chart logic across files.

---

(Add new decisions below as architecture and workflow evolve.)

## 2026-03-01 — [DB/Jobs] — Daily Backtest Summary + Nightly Tuning Pipeline

- Context:  
  We need deterministic, day-bucketed backtesting over `ticks` and a safe runtime pipeline that tunes weekly parameters and prepares next-day live configuration without unsafe order execution by default.

- Decision:  
  Add a new table `backtest` (1 row per `symbol` + trading day) and implement jobs:
  - `jobs.backtest_week` for deterministic daily summaries.
  - `jobs.tune_week` for constrained grid search over last 7 days.
  - `jobs.nightly_retrain` for promotion gating.
  - `jobs.live_robot` as a paper-first execution skeleton.
  Trading-day buckets are computed in Australia/Sydney time using 08:00 → next-day 07:00 session boundaries.

- Consequences:  
  - Daily strategy metrics are persisted and queryable by day/symbol.
  - Runtime config is generated automatically under `runtime/configs/live_strategy.json`.
  - Live mode remains safe by default (paper execution only unless a real adapter is explicitly implemented and enabled).

---

## 2026-03-06 - [DB/Jobs] - Rolling 60s Flow Signal Engine

- Context:  
  We need a candle-free signal engine that tails `ticks` in id order, computes session aTVWAP plus rolling-60s flow metrics, and emits entry signals with explainable threshold context.

- Decision:  
  Add two additive tables:
  - `flow_state` to persist per-symbol streaming state (`last_tick_id`, session accumulators, rolling/EMA/zscore fields, and state-machine fields).
  - `flow_signals` to persist generated buy/sell entries with a compact JSON `reason`.
  Implement `python -m jobs.flow60sSignals` to:
  - tail ticks by id in batches,
  - compute rolling 60s hi/lo/range/tick-rate/ret,
  - maintain session reset behavior on configurable gap,
  - maintain time-decay EMAs and EWMA z-scores,
  - apply macro-bias + impulse/pullback/re-acceleration state logic,
  - write signals and upsert state once per batch.

- Consequences:  
  - Flow logic is isolated in `jobs/` and does not add heavy logic to API routes.
  - Signal generation is resumable from `flow_state.last_tick_id`.
  - New DB objects are additive and do not modify `ticks` schema.

---

## 2026-03-06 - [DB/Jobs] - Flow Signal Outcome Journal Evaluation

- Context:  
  We need a resumable backtest evaluator that replays `flow_signals` against future ticks and records first-hit TP/SL outcomes without duplicating previously evaluated signals.

- Decision:  
  Add an additive table `flow_signal_outcomes` keyed by `id` with uniqueness on `(symbol, signal_id)` plus entry-time and entry-tick indexes for efficient review queries.  
  Add `python -m jobs.evalFlowSignals` to process signals in configurable id order (default newest to oldest), evaluate first TP/SL/no-hit within a max-hold horizon, and upsert outcomes.

- Consequences:  
  - Evaluation is resumable by uniqueness on `(symbol, signal_id)` and optional `--force` recomputation.
  - Existing `ticks`, `flow_signals`, and API routes remain unchanged.
  - Outcome stats (`tp/sl/no_hit`, duration) are persisted for downstream reporting.

---

## 2026-03-20 - [DB/Jobs] - First Online Rule-Family Layer (`trulehit`)

- Context:  
  Structural layers now produce `tconfirm` rows with truth linkage, and we need the first explicit online rule-family layer that evaluates simple inspectable thresholds without introducing ML or live trading logic.

- Decision:  
  Add additive table `trulehit` and job `python -m jobs.buildTrulehit`. The first implemented slice is top-direction `StrictB` `v1`, evaluated 1:1 from `tconfirm` rows for a selected day and source build version with explicit pass/fail reason codes.

- Consequences:  
  - Rule hits become a persisted, inspectable layer keyed to `tconfirm`.
  - Rebuilds remain day-driven and resumable by deleting and rebuilding only the requested day/rule slice.
  - Additional families or directions can be added later without changing the current `tconfirm` contract.
