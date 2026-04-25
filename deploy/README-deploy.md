# datavis deployment

Deployment runs automatically on every push to `main` and on manual `workflow_dispatch`.

## Source of truth

`deploy/scripts/apply-update-steps.sh` is the single deployment runner.

It:

- changes to the repo root
- safely loads `/etc/datavis.env` when readable on EC2
- prefers `DATABASE_URL` over `DATAVIS_DB_URL`
- normalizes `postgresql+psycopg2://` to `postgresql://`
- runs only the ordered actions in `deploy/update_steps.json`
- writes a timestamped detailed log under `logs/update_journal/`
- rewrites `deploy/updateJournal.md` on every run with the latest result only
- fails the deployment immediately when any step fails

## GitHub Actions flow

The deploy workflow does not duplicate deploy logic. It only SSHes into EC2 and runs:

```bash
cd /home/ec2-user/cTrade
git fetch origin main
git reset --hard origin/main
bash deploy/scripts/apply-update-steps.sh
cat deploy/updateJournal.md
```

Required GitHub repository secrets:

- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

Optional secret:

- `EC2_PORT` if SSH is not on port `22`

## Update manifest

Every release must replace the current update instructions instead of appending to them.

- human-readable current instructions: `deploy/UPDATE_STEPS.md`
- machine-readable current instructions: `deploy/update_steps.json`

For this release the current steps are only:

1. restart `datavis.service`
2. retry local `/api/health`

No SQL migration, backfill, or scenario rerun is part of this release.

## Journaling

Every deployment run refreshes:

- latest summary: `deploy/updateJournal.md`
- timestamped detailed logs: `logs/update_journal/update_YYYYMMDD_HHMMSS.log`

The summary includes the run date/time, commit hash, latest commit message, overall result, and per-step results with the last journal lines for each step.

## Manual EC2 fallback

If GitHub Actions cannot complete the deploy, run:

```bash
cd /home/ec2-user/cTrade
git fetch origin main
git reset --hard origin/main
bash deploy/scripts/apply-update-steps.sh
cat deploy/updateJournal.md
```
