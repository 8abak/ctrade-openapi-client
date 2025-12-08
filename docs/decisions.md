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
