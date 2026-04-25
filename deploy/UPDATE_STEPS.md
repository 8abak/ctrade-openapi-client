# Current Update Steps

This file is intentionally replaced for each release. It only describes the steps required for the current update.

## Current update

Update the SQL workbench CSV export flow and layout cleanup.

## Automatic deploy flow for this update

1. The GitHub Actions deploy workflow SSHes into EC2.
2. EC2 runs:

```bash
cd /home/ec2-user/cTrade
git fetch origin main
git reset --hard origin/main
bash deploy/scripts/apply-update-steps.sh
cat deploy/updateJournal.md
```

3. `deploy/scripts/apply-update-steps.sh` reads `deploy/update_steps.json` and runs only the current steps below.

## Current steps executed by apply-update-steps.sh

1. Restart `datavis.service`.

## Not required for this update

- No SQL migration.
- No backfill.
- No scenario rerun.
- No unrelated service restarts.

## Manual browser step

- Hard refresh the SQL page after deploy so the updated JS and CSS are loaded.

## Manual EC2 fallback

If the GitHub workflow fails, run the same commands directly on EC2:

```bash
cd /home/ec2-user/cTrade
git fetch origin main
git reset --hard origin/main
bash deploy/scripts/apply-update-steps.sh
cat deploy/updateJournal.md
```
