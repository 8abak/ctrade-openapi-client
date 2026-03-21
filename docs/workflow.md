# Workflow

## Live Tick Flow

1. `tickCollectorRawToDB.py` writes raw `ticks`.
2. `tickCalcFromDB.py` backfills derived `mid / spread / kal / k2` and existing pivot/day tables.
3. `unityFromDB.py` tails `ticks.id` in order, processes only new rows, and writes:
   - causal per-tick labels
   - bounded cleaned labels
   - causal pivot events
   - cleaned swing segments
   - signal candidates
   - one paper trade at a time
   - append-only trade journal events
4. `backend/main.py` exposes `/api/unity/recent` for quick inspection.

## UNITY Layers

### Causal Layer

- Uses only information available at the current tick.
- Noise unit is the rolling median of absolute `mid` change.
- Confirm threshold is `6.0 * noise + 0.25 * mean(spread)` with a floor.
- Pivots are confirmed only after opposite excursion past threshold.
- Causal state is derived from the active leg using notebook-style trend/yellow scoring.
- Only this layer can create paper signals and paper trades.

### Cleaned Layer

- Rebuilds only a bounded recent region.
- Compresses recent micro pivots into alternating swing pivots.
- Promotes swing pivots only when opposite travel exceeds `swingfactor * confirmthresh`.
- Paints red/green swing legs, then adds yellow transition buffers around swing turns.
- Dissolves small trapped islands when both neighbors agree and the island is weak.
- Never rewrites the full day repeatedly.

## Bounded Repaint Rule

- The cleaned layer only rewrites from the recent differentiating area.
- Current implementation rebuilds from:
  - the 24th most recent confirmed micro pivot when available, else
  - the 3rd most recent confirmed micro pivot, else
  - the earliest tick still kept in processor memory.
- This keeps causal history immutable while still allowing short backward cleanup around the most recent structural turns.

## Signal Rules

- A signal candidate is emitted once per new causal red/green leg when the causal state first becomes directional.
- Features include:
  - tick lag from leg start
  - price lag from last causal pivot
  - threshold multiple
  - leg efficiency
  - recent flip count
  - distance from previous decision
  - cleaned-state agreement
  - cleaned conviction
  - mature / too-early / too-late flags
- Every candidate is written to `unitysignal`, even if rejected or skipped.
- Only favored candidates may open a paper trade.
- If a favored candidate appears while a trade is already open, it is journaled as `skipped` with `skipreason='opentrade'`.

## Paper Trade Rules

- No real orders are placed.
- Long entry uses `ask` when available; short entry uses `bid`; fallback is `mid`.
- Stop uses the latest causal opposite pivot plus a buffer:
  - `max(tradebuffermin, tradenoisebuffer * noise, tradespreadbuffer * spread)`
- Target is `1R` from entry using the initial stop distance.
- When best unrealized profit reaches `+1.00`:
  - stop moves to entry
  - trailing is armed
- Trailing distance stays `1.00` behind the best favorable price.
- Exit reasons:
  - `tp`
  - `sl`
  - `breakeven`
  - `trail`
  - `regimechange`

## Commands

### Create Tables

```powershell
psql -d trading -f sql/2026-03-21-create-unity.sql
```

### Backfill Existing History

```powershell
python unityFromDB.py --mode backfill --symbol XAUUSD --fromid 35884041 --toid 999999999
```

### Rebuild From Scratch

```powershell
python unityFromDB.py --mode backfill --symbol XAUUSD --reset --fromid 35884041
```

### Run Continuously

```powershell
python unityFromDB.py --mode live --symbol XAUUSD
```

### Query Recent Output

```text
GET /api/unity/recent?symbol=XAUUSD&limit=100
```

## EC2 Service

- Systemd unit: `deploy/systemd/unity.service`
- Typical enable/start flow on EC2:

```bash
sudo cp deploy/systemd/unity.service /etc/systemd/system/unity.service
sudo systemctl daemon-reload
sudo systemctl enable unity.service
sudo systemctl start unity.service
```

## Notes

- `docs/db-schema.txt` still has to be regenerated on an environment where PostgreSQL is running:

```powershell
python -m jobs.buildSchema
python -m jobs.buildRoots
```
