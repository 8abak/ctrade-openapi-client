# Engineering Control Plane

This subsystem adds the missing bounded automation layer under `datavis/control`.

It manages the existing entry-only research loop without turning it into unrestricted self-modifying behavior. The control plane stays private, server-side, journaled, resumable, and bounded to approved services and files.

## Roles

- `datavis/control/api.py`: private FastAPI control surface for status, restart, reset, requeue, smoke tests, and patch history
- `datavis/control/orchestrator.py`: watches the research loop, opens one incident at a time, calls the engineering supervisor, validates bounded repairs, runs smoke tests, and resumes the loop
- `datavis/control/supervisor.py`: bounded OpenAI engineering supervisor client with structured repair decisions and heuristic fallback
- `datavis/control/executor.py`: bounded repair executor for env fixes, known code patches, permission repair, service restart, reset/requeue, and rollback
- `datavis/control/failure_detector.py`: compact diagnosis of failed jobs, rejected decisions, service failures, stuck jobs, and missing artifacts
- `datavis/control/store.py`: engineering incident/action/patch/smoketest/state persistence
- `datavis/control/journal.py`: append-only JSONL plus DB engineering journals

The runtime is intentionally collapsed into two services:

- `research-control.service`: private control API
- `engineering-orchestrator.service`: incident watcher and bounded repair loop

The engineering supervisor and repair executor remain separate classes and audit streams, but they do not need separate systemd units for the first safe version.

## Allowed Automatic Repair Classes

- Config repair:
  endpoint/style normalization, quoted env cleanup, bounded env writes in approved env files
- Permission repair:
  runtime directory creation, bounded chmod/touch recovery for journal/runtime paths
- Small code patch repair:
  approved templates only in `datavis/research` or `datavis/control`
- Service recovery:
  `reset-failed`, restart, and state recheck for approved research/control services
- Research state recovery:
  bounded pause/resume, soft/hard reset, failed-job requeue
- Smoke validation:
  import checks, control API boot, engineering supervisor schema, simulated patch roundtrip, optional DB/status probes

## Guardrails

- One active incident at a time
- Allowed repair decisions are enum-limited
- Allowed patch templates are enum-limited
- Patch targets are allowlisted to `datavis/research`, `datavis/control`, and configured env files
- Max changed files, max changed lines, and max changed bytes per patch
- Max restart budget per hour
- Max retries per incident
- Max rollbacks per incident
- Smoke tests must pass before an incident is marked resolved
- Exhausted retries escalate to manual review and keep the research loop paused
- No arbitrary command execution endpoint exists

## Control API

Default bind is loopback only: `127.0.0.1:8010`

Available endpoints:

- `GET /control/health`
- `GET /control/research/status`
- `GET /control/research/latest-run`
- `GET /control/research/latest-errors`
- `GET /control/research/journals`
- `POST /control/research/restart`
- `POST /control/research/reset`
- `POST /control/research/requeue`
- `POST /control/research/pause`
- `POST /control/research/resume`
- `POST /control/repair/run-smoke-tests`
- `POST /control/repair/apply-approved-patch`
- `GET /control/repair/history`
- `GET /control/repair/current-incident`

## DB Objects

Migration: `deploy/sql/20260418_engineering_control_plane.sql`

Tables:

- `research.engineering_incident`
- `research.engineering_action`
- `research.engineering_patch`
- `research.engineering_smoketest`
- `research.engineering_state`
- `research.engineering_journal`

## Journals And Artifacts

Default runtime paths:

- journals: `runtime/control/journals`
- artifacts: `runtime/control/artifacts`
- logs: `runtime/control/logs`

JSONL journals:

- `orchestrator.jsonl`
- `engineering-supervisor.jsonl`
- `repair-executor.jsonl`

Patch diffs, backups, rollback artifacts, and smoke results are stored under per-incident artifact folders.

## Startup Order

1. Apply `deploy/sql/20260418_engineering_control_plane.sql`
2. Place `/etc/datavis-control.env`
3. Start `research-control.service`
4. Start `engineering-orchestrator.service`
5. Start or keep the existing research services

## Inspect Incidents

```sql
SELECT id, status, incident_type, severity, summary, retry_count, max_retries, opened_at, updated_at
FROM research.engineering_incident
ORDER BY id DESC
LIMIT 20;
```

```sql
SELECT incident_id, action_type, status, rationale, started_at, finished_at
FROM research.engineering_action
ORDER BY id DESC
LIMIT 20;
```

```sql
SELECT incident_id, patch_type, status, target_files, diff_path, rollback_path, created_at
FROM research.engineering_patch
ORDER BY id DESC
LIMIT 20;
```

## Pause / Resume / Manual Takeover

Pause the engineering loop:

```sql
UPDATE research.engineering_state
SET value = jsonb_set(value, '{paused}', 'true'::jsonb, true), updated_at = NOW()
WHERE key = 'engineering_loop_control';
```

Resume the engineering loop:

```sql
UPDATE research.engineering_state
SET value = jsonb_set(value, '{paused}', 'false'::jsonb, true), updated_at = NOW()
WHERE key = 'engineering_loop_control';
```

Disable the engineering loop entirely:

```sql
UPDATE research.engineering_state
SET value = jsonb_set(value, '{enabled}', 'false'::jsonb, true), updated_at = NOW()
WHERE key = 'engineering_loop_control';
```
