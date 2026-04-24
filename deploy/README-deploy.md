# datavis deployment

`Deploy datavis to EC2` runs on every push to `main` and on manual `workflow_dispatch`.

Every change that requires a migration, service install, service restart, backfill, or other deploy-time update must update `deploy/update_steps.json` in the same push.

The workflow:
- opens an SSH session to the EC2 host with `appleboy/ssh-action`
- changes to `/home/ec2-user/cTrade`
- records the current checkout commit as a fallback previous deploy commit
- fetches `origin/main`, resets to that commit, and cleans untracked files
- runs `deploy/scripts/deploy-datavis.sh` with both the previous and new commit SHAs

Required GitHub repository secrets:
- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

Optional secret:
- `EC2_PORT` if SSH is not on port `22`

Runtime paths used by the deploy flow:
- repo checkout: `/home/ec2-user/cTrade`
- virtualenv: `/home/ec2-user/venvs/datavis`
- env file: `/etc/datavis.env`
- deploy state: `/home/ec2-user/.datavis/last_deployed_commit`

Systemd units installed by deploy:
- `datavis.service` runs `datavis.app:app` from `/home/ec2-user/venvs/datavis/bin/uvicorn`
- `tickcollector.service` runs `/home/ec2-user/cTrade/tickCollectorRawToDB.py` from `/home/ec2-user/venvs/datavis/bin/python`
- `backbone.service` runs `python -m datavis.backbone_runtime` from `/home/ec2-user/venvs/datavis/bin/python`

Removed services cleaned up by deploy:
- retired legacy services listed in `deploy/scripts/deploy-datavis.sh`

Deploy behavior:
- compares the previously deployed commit to the new commit
- logs the changed files before any restarts
- maps changed paths to service restarts explicitly
- applies only the changed SQL migration files for that deploy
- executes the typed update manifest in `deploy/update_steps.json`
- reloads `systemd` only when unit files changed or on a full deploy
- reloads nginx only when nginx-managed files changed or on a full deploy
- restarts only the affected services
- updates the stored successful deploy commit after the deploy passes

Typed update workflow:
- manifest file: `deploy/update_steps.json`
- runner entrypoint: `deploy/scripts/run-update-steps.sh`
- implementation: `deploy/scripts/run_update_steps.py`
- docs: `deploy/UPDATE_STEPS.md`
- persistent log: `/home/ec2-user/.datavis/update_steps.log`
- persistent state: `/home/ec2-user/.datavis/update_steps_state.json`

Current service/file mapping:
- `frontend/*`, `datavis/app.py`, `datavis/trading.py`, `datavis/smart_scalp.py`, `datavis/structure.py`, `datavis/rects.py` -> `datavis.service`
- `datavis/backbone.py`, `datavis/backbone_runtime.py`, `datavis/backbone_jobs.py`, `datavis/brokerday.py` -> `backbone.service` and `datavis.service`
- `tickCollectorRawToDB.py`, `datavis/tickcollector_runtime.py`, `ctrader_open_api/*`, `datavis/broker_creds.py`, `datavis/ctrader_auth.py` -> `tickcollector.service` and `datavis.service`
- `datavis/db.py`, `requirements.txt` -> `datavis.service`, `tickcollector.service`, `backbone.service`
- `deploy/systemd/*.service` -> `systemctl daemon-reload` plus restart of the matching installed service
- `deploy/nginx/*`, `deploy/scripts/recover-datavis-nginx.sh` -> nginx reload via `recover-datavis-nginx.sh`
- `deploy/sql/20260420_backbone.sql`, `deploy/sql/20260422_backbone_bigbones.sql`, `deploy/sql/20260422_retire_structure_family.sql` -> run migration, restart `datavis.service` and `backbone.service`
- `deploy/sql/20260411_layer_zero_rects.sql`, `deploy/sql/20260419_speed_cleanup.sql` -> run migration, restart `datavis.service` and `tickcollector.service`
- `deploy/sql/20260418_remove_legacy_structure_layer.sql`, `deploy/sql/20260421_drop_auction_layer_manual.sql` -> run migration, restart `datavis.service`

Nginx recovery managed by deploy:
- canonical site file: `/etc/nginx/conf.d/datavis.au.conf`
- source of truth in repo: `deploy/nginx/datavis.au.conf`
- `deploy/scripts/recover-datavis-nginx.sh` removes stale `server_name datavis.au` blocks from `/etc/nginx/nginx.conf`, installs the managed site file, runs `nginx -t`, and reloads nginx

Trading runtime env vars for `/etc/datavis.env`:
- `DATAVIS_TRADE_USERNAME` (default `babak`)
- `DATAVIS_TRADE_PASSWORD` (required to enable trade login)
- `DATAVIS_TRADE_SESSION_SECRET` (recommended; stable secret for signed login cookie)
- `DATAVIS_TRADE_COOKIE_SECURE` (`1` for HTTPS-only cookie in production)
- `DATAVIS_CTRADER_CLIENT_ID`
- `DATAVIS_CTRADER_CLIENT_SECRET`
- `DATAVIS_CTRADER_ACCOUNT_ID`
- `DATAVIS_CTRADER_ACCESS_TOKEN`
- `DATAVIS_CTRADER_REFRESH_TOKEN`
- `DATAVIS_CTRADER_SYMBOL` (default `XAUUSD`)
- `DATAVIS_CTRADER_SYMBOL_ID` (optional; autodetected when omitted)
- `DATAVIS_CTRADER_CONNECTION_TYPE` (`live` or `demo`)
- optional fallback: `DATAVIS_CTRADER_CREDS_FILE`

Operational checks:
- deploy on EC2: `cd /home/ec2-user/cTrade && bash deploy/scripts/deploy-datavis.sh`
- run update steps only: `cd /home/ec2-user/cTrade && source /home/ec2-user/venvs/datavis/bin/activate && set -a && source /etc/datavis.env && set +a && bash deploy/scripts/run-update-steps.sh`
- validate nginx: `sudo nginx -t`
- verify the app: `curl -I https://www.datavis.au/`
- export one broker day: `getCsv --day 14/04`
