# datavis deployment

`Deploy datavis to EC2` runs on every push to `main` and on manual `workflow_dispatch`.

The workflow:
- uploads `deploy/scripts/deploy-datavis.sh` to `/home/ec2-user/bin/deploy-datavis.sh`
- runs that script over SSH on the EC2 host
- updates `/home/ec2-user/cTrade` to `origin/main`
- recreates `/home/ec2-user/venvs/datavis` if it is missing or broken, then installs `requirements.txt`
- restarts `datavis.service`
- fails the Action if the service restart or `http://127.0.0.1:8000/api/health` check fails

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

The deploy script runs `git reset --hard origin/main` and `git clean -fd`, so the EC2 checkout should not be used to store runtime-only files.

Later, service-specific restarts can be added by splitting deploy logic into separate scripts and using workflow path filters or conditional steps before the restart stage.
