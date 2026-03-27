# datavis deployment

`Deploy datavis to EC2` runs on every push to `main` and on manual `workflow_dispatch`.

The workflow:
- runs that script over SSH on the EC2 host
- relies on `/home/ec2-user/bin/deploy-datavis.sh` to update `/home/ec2-user/cTrade` to `origin/main`
- relies on that script to repair `/home/ec2-user/venvs/datavis`, install `requirements.txt`, restart `datavis`, and fail on a bad health check

Required GitHub repository secrets:
- `EC2_HOST`
- `EC2_USER`
- `EC2_SSH_KEY`

Optional secret:
- `EC2_PORT` if SSH is not on port `22`

Runtime paths used by the deploy flow:
- repo checkout: `/home/ec2-user/cTrade`
- deploy script on EC2: `/home/ec2-user/bin/deploy-datavis.sh`
- virtualenv: `/home/ec2-user/venvs/datavis`
- env file: `/etc/datavis.env`

Server setup note:
- install or update the EC2 deploy script from `deploy/scripts/deploy-datavis.sh`
- ensure it is executable at `/home/ec2-user/bin/deploy-datavis.sh`

The deploy script runs `git reset --hard origin/main` and `git clean -fd`, so the EC2 checkout should not be used to store runtime-only files.

Later, service-specific restarts can be added by splitting deploy logic into separate scripts and using workflow path filters or conditional steps before the restart stage.
