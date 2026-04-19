# datavis deployment

`Deploy datavis to EC2` runs on every push to `main` and on manual `workflow_dispatch`.

The workflow:
- opens an SSH session to the EC2 host with `appleboy/ssh-action`
- changes to `/home/ec2-user/cTrade`
- runs `git fetch origin`, `git reset --hard origin/main`, and `git clean -fd`
- executes `deploy/scripts/deploy-datavis.sh`

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

Systemd units installed by deploy:
- `datavis.service` runs `datavis.app:app` from `/home/ec2-user/venvs/datavis/bin/uvicorn`
- `tickcollector.service` runs `/home/ec2-user/cTrade/tickCollectorRawToDB.py` from `/home/ec2-user/venvs/datavis/bin/python`
- `separation.service` runs `python -m datavis.separation_runtime` from `/home/ec2-user/venvs/datavis/bin/python`
- `tickcollector.service` also reads `/etc/datavis.env` when present so `DATAVIS_CTRADER_CREDS_FILE` and related runtime overrides apply to the collector too

Nginx recovery managed by deploy:
- canonical site file: `/etc/nginx/conf.d/datavis.au.conf`
- source of truth in repo: `deploy/nginx/datavis.au.conf`
- `deploy/scripts/recover-datavis-nginx.sh` removes any stale `server_name datavis.au` block from `/etc/nginx/nginx.conf`, installs the managed site file, runs `nginx -t`, and reloads nginx
- the managed site file redirects `/` to `/live`, proxies the live app to `127.0.0.1:8000`, and contains no `8501` upstream or static `frontend` root

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
- optional fallback: `DATAVIS_CTRADER_CREDS_FILE` (JSON path compatible with existing `creds.json`)

The deploy workflow resets and cleans the EC2 checkout, so do not store runtime-only files there.

The deploy script:
- activates `/home/ec2-user/venvs/datavis/bin/activate`
- runs `pip install -r requirements.txt`
- installs the `datavis`, `tickcollector`, and `separation` systemd units
- disables and removes retired legacy processor services
- applies `deploy/sql/20260418_remove_legacy_structure_layer.sql`
- applies `deploy/sql/20260411_layer_zero_rects.sql`
- applies `deploy/sql/20260416_separation.sql`
- applies `deploy/sql/20260419_speed_cleanup.sql`
- enables `datavis`, `tickcollector`, and `separation`
- restarts and verifies `datavis` and `separation`
- never restarts `tickcollector`
- performs a local health check at `http://127.0.0.1:8000/api/health` when `curl` is available

Legacy cleanup:
- `deploy/scripts/cleanup-layer0.sh` creates a public-schema backup before applying the same cleanup SQL
- the cleanup SQL drops old derived-layer tables, preserves the raw `ticks` table, and keeps the hot-path tick indexes
