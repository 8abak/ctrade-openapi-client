# Update Steps Workflow

`deploy/update_steps.json` is the machine-readable release/update manifest for EC2 deploys. Every change that requires SQL, a new or changed systemd unit, service restarts, one-time commands, backfills, or post-deploy verification must update that file in the same push.

## Manifest shape

The manifest is JSON so it can be validated and executed without adding a YAML dependency. It contains:

- `version`: release/update identifier for the current manifest.
- `description`: human-readable summary of the release/update.
- `actions`: ordered list of typed deploy actions.

Each action must set:

- `id`
- `description`
- `type`
- `required`
- `safe_to_rerun`

Optional fields depend on the action type:

- `file` for `run_sql_file` and `install_systemd_unit`
- `service` for `install_systemd_unit`, `enable_service`, `start_service`, `restart_service`
- `command` for `run_command`, `backfill_command`, `verify_command`
- `timeout_seconds` when the action should not run forever

Supported action types:

- `run_sql_file`
- `install_systemd_unit`
- `daemon_reload`
- `restart_service`
- `enable_service`
- `start_service`
- `run_command`
- `backfill_command`
- `verify_command`

## Execution model

The EC2 deploy path still runs the existing deploy logic in `deploy/scripts/deploy-datavis.sh`, and now also executes the manifest through:

```bash
bash deploy/scripts/run-update-steps.sh
```

That wrapper runs `deploy/scripts/run_update_steps.py`, which:

- validates `deploy/update_steps.json`
- executes actions in manifest order
- stops immediately when a required action fails
- logs and continues when an optional action fails
- records persistent state in `/home/ec2-user/.datavis/update_steps_state.json`
- records logs in `/home/ec2-user/.datavis/update_steps.log`

`safe_to_rerun=false` actions are skipped automatically after a successful run for the same manifest `version`, unless they are forced manually. `safe_to_rerun=true` actions run again on repeated deploys of the same manifest version.

## Manual use

Normal deploy path:

```bash
cd /home/ec2-user/cTrade
bash deploy/scripts/deploy-datavis.sh
```

Manual manifest run only:

```bash
cd /home/ec2-user/cTrade
source /home/ec2-user/venvs/datavis/bin/activate
set -a
source /etc/datavis.env
set +a
bash deploy/scripts/run-update-steps.sh
```

Validate without executing:

```bash
bash deploy/scripts/run-update-steps.sh --dry-run
```

## Skip or rerun actions

Skip one action:

```bash
bash deploy/scripts/run-update-steps.sh --skip-action backfill_recent_mavg
```

Force rerun one action even when `safe_to_rerun=false`:

```bash
bash deploy/scripts/run-update-steps.sh --force-action some_action_id
```

Force rerun every action:

```bash
bash deploy/scripts/run-update-steps.sh --force-all
```

## Logs and state

Inspect logs:

```bash
tail -n 200 /home/ec2-user/.datavis/update_steps.log
```

Inspect recorded action state:

```bash
cat /home/ec2-user/.datavis/update_steps_state.json
```

## Current Motion Release

For the raw-mid motion/trade-spot research layer in this branch:

1. Run `deploy/sql/20260424_motion_trade_spots.sql`.
2. Restart `datavis.service` only if app/routes changed.
3. Run `python -m datavis.motion_trade_spots backfill --last-broker-days 2`.
4. Export:
   `logs/motionpoint_last2days.csv`
   `logs/motionsignal_last2days.csv`
5. Do not start any trading service unless that is added intentionally in a later release.

This branch does add `/api/motion/signals/recent`, so the current `deploy/update_steps.json` includes a `datavis.service` restart. No trading worker is introduced here.

## Authoring rules

When you add a push that changes deploy requirements:

1. Update `deploy/update_steps.json` in the same branch.
2. Prefer typed actions over generic shell commands.
3. Keep SQL idempotent with `IF NOT EXISTS` or conflict-safe inserts when possible.
4. Mark `safe_to_rerun` honestly.
5. Use `required=false` only for actions that should warn without failing the deploy.
6. Keep verification commands human-readable and cheap.

The current manifest is intentionally concrete for the motion research release: it applies `deploy/sql/20260424_motion_trade_spots.sql`, restarts `datavis.service` because the app routes changed, backfills the last two broker days, exports the motion research CSVs, and does not start any trading service.
