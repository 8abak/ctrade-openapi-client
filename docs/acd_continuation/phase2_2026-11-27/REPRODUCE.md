# Reproduce Phase 2 without touching the paper robot

The research branch commits its own calculation snapshot because the working repository contained extensive unrelated uncommitted code. Only research scripts, tests, reports and snapshots are included in the Phase 2 commit. The active source files and unrelated local work are not staged.

## Environment

Python3.13 was used for local tests/charts. The EC2 run reused `/home/ec2-user/venvs/datavis/bin/python`, PostgreSQL and the existing `datavis.db` read-only helper. For a standalone environment install `psycopg2-binary`, `matplotlib`, `pytest` and IANA timezone data (`tzdata` where the operating system lacks it). The strategy snapshot itself uses the standard library.

`frozen_source/datavis/acd_continuation.py`, `hlrtacd/acd.py` and `hlrtacd/models.py` are unmodified deployed calculation files; compare SHA256 with `status_before.json`. Minimal package exports and the environment-based `db.py` adapter are research-only and have no broker order API. No credentials are stored in the bundle.

## Run against the existing EC2 database

From the research checkout:

```sh
export PYTHONPATH="$PWD/docs/acd_continuation/phase2_2026-11-27/frozen_source"
# Supply ACD_RESEARCH_DB_DSN through your secure environment; do not put it in Git.
python scripts/acd_phase2_replay.py \
  --config docs/acd_continuation/phase2_2026-11-27/config.json \
  --output /path/to/a/new/research-output
python scripts/acd_phase2_charts.py /path/to/a/new/research-output
python -m pytest test_acd_phase2_replay.py -q
```

The connection is enforced read-only. The replay reads `public.ticks(id,timestamp,bid,ask,symbol)`, fixes the latest XAUUSD timestamp once, subtracts three **calendar months** and adds ten days of no-entry warmup. New data moves the end of a later run, so exact numerical reproduction also requires the same tick snapshot/window recorded in `summary.json`. Raw tick data stays in its existing database and is not deleted or rewritten. `data_gaps.csv` reports gaps as closure-or-missing-data, not invented holidays.

The recorded interval is three calendar months in UTC; Sydney display times differ by one wall-clock hour because daylight saving changes within the window. Reporting session dates always use New York's IANA clock.

Alternatively set `PYTHONPATH=/home/ec2-user/acd-paper-release` on EC2 to reuse the unchanged deployed modules and its existing database helper, as the recorded run did. Never pass the active paper SQLite path: the script has no parameter for it and always creates its own disposable temporary ledger.

## Semantics and output

All valid, strictly increasing timestamp ticks drive the unchanged `_process` method. Duplicate timestamps follow the deployed public method's skip behavior. No entry, stop, threshold, position size or cost parameter is tuned. A session-interval lookup cache and per-session diagnostic caches avoid repeated formatting/calendar work; the strategy's decisions are unchanged. Per-tick parity tests compare audit and production state.

The primary account starts once at500USD. When the configured455USD pre-limit liquidation threshold latches, the account never resumes. Later days retain causal signal observations with `risk_shutdown_latched` and no executable trades. This is signal diagnostics, not a hypothetical reset account or additional P&L.

Daily `net_pnl` is realized P&L on the exit's ACD session; `trade_net_pnl` is the full entry-day trade result. `closing_equity` includes executable floating P&L and configured charges. MAE/MFE use closing bid for longs and ask for shorts relative to the slipped entry, in quote dollars and USD at the fixed contract assumption, excluding commission. “Inside ACD” means inside A-down/A-up, not merely inside the un-offset opening range. Raw opposite-boundary breaks and buffered/held exit invalidations are distinguished.

An ACD session date is the reporting trading-day label. Scheduled weekdays, observed quote sessions and missing/partial sessions are counted separately. `preceding_observed_session` is a data-derived diagnostic; `calendar_predecessor` is the unchanged weekday clock. Neither constitutes a verified broker holiday calendar.

The daily decision observation is the actual entry timestamp on entry days; otherwise it is the first confirmed aligned breakout, if observed, or the first quote in the entry window. No-quote days have no decision timestamp. `today_direction` is causal at that selected timestamp. `no_trade_reason` is a whole-session rejection summary, and `reason_counts` retains all observed gate reasons; the summary reason need not be the gate at the first observation. This reporting convention does not create a new trading rule or retrospectively change an entry.

Chart prices/equity are sampled once per minute plus entry/exit points. Every trailing adjustment is in `trailing_movements.csv`. Entry, retracement, recovery and adverse/favorable extrema have exact observed timestamps. Tick-level decisions, extremes and maximum drawdown are never computed from the sampled charts.

Transaction costs remain the **unchanged deployed assumptions**: actual bid/ask spread,0.10 adverse quote slippage per side,0.14USD round-trip commission, zero modeled swap and100oz per lot. These have not been verified against the broker schedule. They are plausible paper inputs but cannot establish realistic all-in performance, particularly for overnight/weekend financing. No extra cost scenario or parameter optimization was substituted for the primary result.

Read `STRATEGY_AUDIT.md` before interpreting performance. There are known correctness/calendar gaps and unresolved owner definitions. No live activation is performed by any Phase 2 script.
