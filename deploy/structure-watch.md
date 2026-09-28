# XAU Structure Watch prototype

This is an opt-in, read-only event detector. It does not place orders, send notifications,
call an AI model, or modify the tick collector. The thresholds are research defaults,
not calibrated trading signals.

## Data flow

`public.ticks` → watcher → `logs/structure_watch.json` (latest health/state) and
`logs/structure_watch.jsonl` (append-only confirmed events) →
`GET /api/structure-watch` → `/structure-watch` page.

The watcher starts at the latest tick on its first run, tracks a rolling five-minute
range after two minutes of observations, requires a break beyond the prior boundary,
ten seconds of acceptance, a retest, and rejection. It suppresses wide spreads,
out-of-order ticks, stale ticks, and events during a five-minute cooldown.
The event record includes evidence, range, boundary, invalidation reference,
spread, and an idempotent ID. "Confirmed" describes the observed sequence only.

## Running in a test environment

Use a PostgreSQL role with SELECT on `public.ticks`, set `DATABASE_URL` privately,
then run `python -m datavis.structure_watch --state logs/structure_watch.json`.
Run the existing FastAPI application separately and inspect `/structure-watch`.
The process is not installed or enabled as a service by this change.

Before any live alerting, replay historical ticks and measure event frequency,
false positives, lag, and spread at event time. Define a delivery channel and an
authenticated integration for context lookup. A ChatGPT conversation cannot be
invoked by arbitrary HTTP calls from EC2; an OpenAI API worker is a separate
credentialed service, and phone delivery needs a configured notification service.
Do not attach an API key to the public browser or commit it to the repository.
