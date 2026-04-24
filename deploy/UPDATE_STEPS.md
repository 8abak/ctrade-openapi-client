# Update Steps Workflow

`deploy/update_steps.json` remains the machine-readable manifest for EC2 deploys, but the current motion/trade-spots release now delegates to one journaled runner so SQL, backfill, restart, and validation all use the same environment-loading and DB-resolution path.

## One-command runner

After `git pull` on EC2, run:

```bash
cd /home/ec2-user/cTrade
bash deploy/scripts/apply-update-steps.sh
```

That script:

- changes to the repo root automatically
- writes a timestamped journal to `logs/update_journal/update_YYYYMMDD_HHMMSS.log`
- loads `/etc/datavis.env` only when it is readable
- resolves the database URL in this order: `DATABASE_URL`, then `DATAVIS_DB_URL`
- normalizes `postgresql+psycopg2://` to `postgresql://` before using `psql`
- fails clearly when neither env var is available
- tests the DB connection
- applies `deploy/sql/20260424_motion_trade_spots.sql` when the file exists
- runs `python -m datavis.motion_trade_spots backfill --last-broker-days 2`
- runs validation queries against `public.motionpoint` and `public.motionsignal`
- restarts `datavis.service`
- calls `http://127.0.0.1:8000/api/motion/signals/recent?limit=5` when the service is active

On success it prints:

```text
SUCCESS: update steps completed
```

On failure it prints:

```text
FAILED: see logs/update_journal/update_YYYYMMDD_HHMMSS.log
```

## Journal location

The detailed journal for each run is stored inside the repo:

```text
logs/update_journal/update_YYYYMMDD_HHMMSS.log
```

This is the first place to check when SQL, backfill, restart, or validation fails.

## Manifest wiring

Automated deploys still execute `deploy/scripts/run-update-steps.sh`, which validates `deploy/update_steps.json` and runs its actions in order. For this release, the manifest's required action is:

```bash
bash deploy/scripts/apply-update-steps.sh
```

That keeps manual EC2 runs and deploy-triggered runs on the same command path.

## Validation queries

The update runner records the output of these checks in the journal:

```sql
select windowsec, count(*), min(timestamp), max(timestamp)
from public.motionpoint
group by windowsec
order by windowsec;

select side, outcome, count(*), avg(score)
from public.motionsignal
group by side, outcome
order by side, outcome;

select *
from public.motionsignal
order by score desc
limit 20;
```

Those queries, plus the local API response from `/api/motion/signals/recent?limit=5`, are the proof that the motion migration and backfill completed successfully.

## Fingerprint workflow

For the fingerprint-based `motion_v3_best_fingerprints` rule, use this sequence:

1. Backfill or recreate the baseline v1 signals if needed:

```bash
python -m datavis.motion_trade_spots backfill --last-broker-days 2
# or, if motionpoint already exists and only v1 signals need rebuilding:
python -m datavis.motion_trade_spots recreate-signals --last-broker-days 2 --rule motion_v1_basic_acceleration
```

2. Analyze recent winners and store ranked fingerprints:

```bash
python -m datavis.motion_trade_spots analyze-winners --last-broker-days 2
```

3. Recreate the fingerprint-gated rule:

```bash
python -m datavis.motion_trade_spots recreate-signals --last-broker-days 2 --rule motion_v3_best_fingerprints
```

4. Run the comparison SQL:

```sql
select signalrule, side, outcome, count(*) as total
from public.motionsignal
where timestamp >= now() - interval '2 days'
group by signalrule, side, outcome
order by signalrule, side, outcome;
```
