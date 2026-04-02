# datavis deployment

`Deploy datavis to EC2` runs on every push to `main` and on manual `workflow_dispatch`.

The workflow:
- opens an SSH session to the EC2 host with `appleboy/ssh-action`
- changes to `/home/ec2-user/cTrade`
- runs `git fetch origin`, `git reset --hard origin/main`, and `git clean -fd`
- executes the repo-managed script at `deploy/scripts/deploy-datavis.sh`

Required GitHub repository secrets:
- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

Optional secret:
- `EC2_PORT` if SSH is not on port `22`

Runtime paths used by the deploy flow:
- repo checkout: `/home/ec2-user/cTrade`
- repo-managed deploy script on EC2: `/home/ec2-user/cTrade/deploy/scripts/deploy-datavis.sh`
- virtualenv: `/home/ec2-user/venvs/datavis`
- env file: `/etc/datavis.env`

Server setup note:
- ensure the repo is cloned at `/home/ec2-user/cTrade`
- ensure `/home/ec2-user/venvs/datavis/bin/activate` exists
- ensure the `datavis` systemd service uses `/etc/datavis.env`

The deploy workflow runs `git reset --hard origin/main` and `git clean -fd`, so the EC2 checkout should not be used to store runtime-only files.

The deploy script:
- activates `/home/ec2-user/venvs/datavis/bin/activate`
- runs `pip install -r requirements.txt`
- installs the repo-managed systemd unit files for `datavis`, `tickcollector`, and `fastzig`
- runs `systemctl daemon-reload`
- loads `/etc/datavis.env` when present and applies `deploy/sql/20260403_fast_zig.sql`
- applies `deploy/sql/20260404_fast_zig_levels.sql` for level/state upgrades on partial fast-zig deployments
- disables and removes legacy processor services: `ottprocessor`, `envelopeprocessor`, `zigzag`, `envelopezigprocessor`, and `marketprofile`
- enables `datavis`, `tickcollector`, and `fastzig`
- restarts `datavis` and `fastzig`
- never restarts `tickcollector`
- verifies `datavis` and `fastzig` are active with `systemctl is-active --quiet`
- prints `systemctl status <service> --no-pager -l` on failure
- performs a local `curl` to `http://127.0.0.1:8000/api/health` when `curl` is available

Layer 0 cleanup:
- destructive database cleanup is intentionally not part of every deploy
- use `deploy/scripts/cleanup-layer0.sh` on EC2 for the one-time backup + public-schema cleanup
- that cleanup script backs up the current public schema before dropping derived app tables
