# Entry Research Loop

This subsystem adds an isolated, server-side, entry-only autonomous research loop under `datavis/research`.

It does not touch live order placement, trading routes, frontend pages, the SQL console, the auction page, or the existing collectors. The loop is deliberately small and calm:

- one worker job at a time
- bounded recent slices only
- bounded label horizons
- bounded hypothesis batches
- bounded feature families
- bounded search families
- compact supervisor briefings only
- DB state plus append-only JSONL journals

## Folder Layout

- `datavis/research/config.py`: env loading and runtime limits
- `datavis/research/db.py`: research DB connections with timeouts
- `datavis/research/entry.py`: executable entry labels, past-only features, bounded search families, contrast summaries
- `datavis/research/mutation.py`: bounded next-hypothesis generation, novelty filtering, stop guardrail evaluation
- `datavis/research/worker.py`: bounded worker job execution and persistence
- `datavis/research/orchestrator.py`: loop seeding, decision queueing, stop overrides, next-job insertion
- `datavis/research/supervisor.py`: pending decision polling and tolerant validation
- `datavis/research/supervisor_client.py`: OpenAI HTTP adapter with endpoint normalization and bounded JSON decisions
- `datavis/research/journal.py`: DB journals and JSONL artifact writing
- `datavis/research/state.py`: loop control state helpers

## DB Objects

Migration: `deploy/sql/20260418_entry_research_loop.sql`

This patch keeps the same schema and stores the new search-history, fingerprints, mutation rationale, and decision artifacts inside existing JSON columns plus filesystem artifacts. No new migration is required.

Objects created:

- `research.job`
- `research.run`
- `research.runsummary`
- `research.decision`
- `research.artifact`
- `research.state`
- `research.journal`
- `research.entry_label`
- `research.feature_snapshot`
- `research.candidate_result`
- `research.vw_loop_status`

## Services

Systemd unit files:

- `deploy/systemd/research-worker.service`
- `deploy/systemd/research-orchestrator.service`
- `deploy/systemd/research-supervisor.service`

Each service reads both `/etc/datavis.env` and `/etc/datavis-research.env` when present.

## Required Env Vars

See `deploy/env/datavis-research.env.example`.

Minimum useful set:

- `DATABASE_URL`
- `DATAVIS_RESEARCH_RUNTIME_DIR`
- `DATAVIS_RESEARCH_SYMBOL`
- `OPENAI_API_KEY`
- `DATAVIS_RESEARCH_OPENAI_MODEL`

Important optional knobs:

- `DATAVIS_RESEARCH_SEED_SLICE_ROWS`
- `DATAVIS_RESEARCH_MAX_SLICE_ROWS`
- `DATAVIS_RESEARCH_SLICE_LADDER`
- `DATAVIS_RESEARCH_MIN_RUNS_BEFORE_STOP`
- `DATAVIS_RESEARCH_MAX_NEXT_JOBS`
- `DATAVIS_RESEARCH_FAILED_DIRECTION_STOP_COUNT`
- `DATAVIS_RESEARCH_ITERATION_BUDGET`
- `DATAVIS_RESEARCH_WRITE_BATCH_ROWS`
- `DATAVIS_RESEARCH_CHUNK_SLEEP_SECONDS`
- `DATAVIS_RESEARCH_OPENAI_ENDPOINT`
- `DATAVIS_RESEARCH_OPENAI_API_STYLE`

## Startup Sequence

1. Apply `deploy/sql/20260418_entry_research_loop.sql`
2. Place env in `/etc/datavis-research.env`
3. Start `research-supervisor.service`
4. Start `research-worker.service`
5. Start `research-orchestrator.service`

The orchestrator seeds the first calm job automatically when `research.state.entry_loop_control` shows `seeded=false`.

After that, the loop is multi-pass by default:

- each completed run produces contrast summaries and bounded mutation proposals
- the supervisor chooses among `continue`, `refine`, `explore_new_family`, `increase_slice`, `split_by_pattern`, or `stop`
- the orchestrator only honors `stop` after policy guardrails pass
- otherwise it enqueues the next bounded jobs automatically and journals why

## Local Run Commands

```bash
python -m datavis.research.supervisor_runtime
python -m datavis.research.worker_runtime
python -m datavis.research.orchestrator_runtime
```

## Journals And Artifacts

Default local runtime paths:

- journals: `runtime/research/journals`
- artifacts: `runtime/research/artifacts`
- logs: `runtime/research/logs`

Recommended EC2 runtime paths are outside the repo, for example `/home/ec2-user/datavis-research/...`.

Tail JSONL journals:

```bash
tail -f /home/ec2-user/datavis-research/journals/worker.jsonl
tail -f /home/ec2-user/datavis-research/journals/orchestrator.jsonl
tail -f /home/ec2-user/datavis-research/journals/supervisor.jsonl
```

Tail systemd logs:

```bash
journalctl -u research-worker.service -f
journalctl -u research-orchestrator.service -f
journalctl -u research-supervisor.service -f
```

## Pause / Resume / Stop

Pause:

```sql
UPDATE research.state
SET value = jsonb_set(value, '{paused}', 'true'::jsonb, true), updated_at = NOW()
WHERE key = 'entry_loop_control';
```

Resume:

```sql
UPDATE research.state
SET value = jsonb_set(value, '{paused}', 'false'::jsonb, true), updated_at = NOW()
WHERE key = 'entry_loop_control';
```

Request stop:

```sql
UPDATE research.state
SET value = jsonb_set(value, '{stop_requested}', 'true'::jsonb, true), updated_at = NOW()
WHERE key = 'entry_loop_control';
```

## Inspect Current State

Quick status:

```sql
SELECT * FROM research.vw_loop_status;
```

Recent jobs:

```sql
SELECT id, status, job_type, requested_by, scheduled_at, started_at, finished_at, error_text
FROM research.job
ORDER BY id DESC
LIMIT 20;
```

Recent runs:

```sql
SELECT r.id, r.status, r.symbol, r.iteration, r.started_at, r.finished_at, rs.verdict_hint
FROM research.run r
LEFT JOIN research.runsummary rs ON rs.run_id = r.id
ORDER BY r.id DESC
LIMIT 20;
```

Recent decisions:

```sql
SELECT id, run_id, status, decision, stop_reason, requested_at, completed_at, applied_at
FROM research.decision
ORDER BY id DESC
LIMIT 20;
```

Recent journal events:

```sql
SELECT component, level, event_type, message, created_at
FROM research.journal
ORDER BY id DESC
LIMIT 50;
```

Artifacts:

```sql
SELECT run_id, artifact_type, path, created_at
FROM research.artifact
ORDER BY id DESC
LIMIT 20;
```

## Reading Final Verdicts

The durable final verdict lives in `research.state` under key `entry_loop_control`:

```sql
SELECT value->>'final_verdict' AS final_verdict, value->>'final_reason' AS final_reason
FROM research.state
WHERE key = 'entry_loop_control';
```

Run-level hints are also stored in `research.runsummary.verdict_hint`.

## Search Families

- `threshold_grid`: bounded single-threshold search
- `pair_combo`: bounded 2-predicate conjunctions
- `triad_combo`: bounded 3-predicate conjunctions
- `contrast_gate`: contrast-driven gates from positive vs false-positive deltas
- `regime_split`: same search but narrowed by spread/session regime
- `tighten_winner`: prior winner plus one or two contrast gates
- `slice_expand`: same promising regime on a larger slice or shifted slice
- `side_locked_refine`: long-only or short-only refinement when one side dominates

## Stop Guardrails

- `stop` is rejected before `DATAVIS_RESEARCH_MIN_RUNS_BEFORE_STOP`
- `stop` is accepted early only for a strong stable regime
- weak or inconclusive results are converted into new bounded jobs
- repeated failure across distinct search directions can stop the loop
- budget exhaustion, explicit stop requests, and hard technical failures still stop the loop
