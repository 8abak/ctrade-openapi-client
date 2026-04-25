# datavis deployment

Deployment runs automatically on every push to `main` and on manual `workflow_dispatch`.

## Source of truth

`deploy/scripts/apply-update-steps.sh` is the single deployment runner.

It:

- changes to the repo root
- creates a timestamped detailed log under `logs/update_journal/`
- restarts `datavis.service` with a literal `sudo systemctl restart datavis.service`
- sleeps 5 seconds
- runs a local `curl -fsS http://127.0.0.1:8000/api/health` check
- optionally rewrites `deploy/updateJournal.md` with a small latest-run summary
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

## Current steps

For this release the runner does only:

1. restart `datavis.service`
2. sleep 5 seconds
3. run local `/api/health`

No SQL migration, backfill, or scenario rerun is part of this release.

## Journaling

Every deployment run writes:

- timestamped detailed logs: `logs/update_journal/update_YYYYMMDD_HHMMSS.log`
- latest summary when writable: `deploy/updateJournal.md`

The summary is best-effort only; deployment does not depend on it.

## Manual EC2 fallback

If GitHub Actions cannot complete the deploy, run:

```bash
cd /home/ec2-user/cTrade
git fetch origin main
git reset --hard origin/main
bash deploy/scripts/apply-update-steps.sh
cat deploy/updateJournal.md
```
