# Update Steps Workflow

This release adds server-side CSV export support to the SQL page.

- backend change: `datavis/app.py`
- frontend change: `frontend/sql.html`, `frontend/assets/sql.js`, and styling
- SQL migration: not required for this feature

## Manual EC2 steps

After `git pull` on EC2, run the typed update manifest:

```bash
cd /home/ec2-user/cTrade
source /home/ec2-user/venvs/datavis/bin/activate
bash deploy/scripts/run-update-steps.sh
```

For this release the manifest does two things:

- restarts `datavis.service`
- verifies local app health with `curl http://127.0.0.1:8000/api/health`

## Browser refresh

The SQL page frontend changed, so refresh the `/sql` browser tab after the deploy. A hard refresh is useful if the browser keeps an older cached script.

## CSV export runtime notes

- exported files are written under `logs/sql_exports/`
- the folder is created automatically on the first export
- exports only allow a single read-only `SELECT` or `WITH ... SELECT ...` query
- no SQL migration is needed unless a future change adds tracking tables or other schema objects

## Expected validation

After the restart, confirm the app is healthy:

```bash
curl --fail --silent http://127.0.0.1:8000/api/health
```

Then test a SQL export from the app or with `curl` against `/api/sql/export-csv`.
