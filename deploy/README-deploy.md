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

The deploy workflow resets and cleans the EC2 checkout, so do not store runtime-only files there.

The deploy script:
- activates `/home/ec2-user/venvs/datavis/bin/activate`
- runs `pip install -r requirements.txt`
- installs only the `datavis` and `tickcollector` systemd units
- disables and removes old processor services, including `fastzig` and `zonebuilder`
- applies `deploy/sql/20260408_layer_zero_structure.sql`
- enables `datavis` and `tickcollector`
- restarts and verifies `datavis`
- never restarts `tickcollector`
- performs a local health check at `http://127.0.0.1:8000/api/health` when `curl` is available

Layer Zero cleanup:
- `deploy/scripts/cleanup-layer0.sh` creates a public-schema backup before applying the same cleanup SQL
- the cleanup SQL drops old derived-layer tables and preserves the raw `ticks` table plus tick indexes
- the new structure layer is rebuilt by replaying raw ticks through the app's streaming engine, so no extra structure tables are required
