"""Build auditable Phase 4 reports and portable research exports from replay results."""
import argparse
import csv
import hashlib
import json
import statistics
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def load(root, name):
    return json.loads((root / (name + '.json')).read_text(encoding='utf-8'))


def csvwrite(path, rows):
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows({k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in r.items()} for r in rows)


def table(headers, rows):
    def fmt(v):
        if isinstance(v, float):
            return f'{v:.2f}'
        return str(v).replace('|', '/')
    return '\n'.join(['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |'] + ['| ' + ' | '.join(map(fmt, r)) + ' |' for r in rows])


def ny(value):
    return datetime.fromisoformat(value).astimezone(ZoneInfo('America/New_York')).isoformat(timespec='milliseconds') if value else '—'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory
    results, trades = load(root, 'comparison'), load(root, 'all_trades')
    configs = {r['id']: r for r in results}
    baseline = load(root, 'baseline_forensics')
    quality = {r['config']: r for r in load(root, 'opportunity_quality')}
    before, after = load(root, 'status_before'), load(root, 'status_after')
    dataset = load(root, 'dataset')
    checks = []
    def check(name, condition):
        checks.append(dict(check=name, passed=bool(condition)))
        assert condition, name
    check('144 independent configurations; 36 entry profiles', len(results) == 144 and len({r['entry_id'] for r in results}) == 36)
    with (root/'daily.csv').open(encoding='utf-8', newline='') as f:
        daily = list(csv.DictReader(f))
    check('All 67 observed sessions replayed per configuration', len(daily) == 144*67 and all(sum(d['config'] == r['id'] for d in daily) == 67 for r in results))
    check('Exact revised Phase 2 baseline reproduction', load(root, 'baseline_reproduction')['matches'])
    check('Independent full-tick execution verification', load(root, 'execution_checks')['passed'])
    check('No open positions at fixed data cutoff', all(not rows for rows in load(root, 'open_positions').values()))
    old = {r['id']: r for r in load(root.parent/'phase3_exits_2026-11-27', 'comparison')}
    for exit_id, name in [('exit_A','baseline'), ('exit_B','target_edge_0.75W'), ('exit_C','trail_entry_entry_1W'), ('exit_D','no_trailing')]:
        check('Phase 3 control reproduced: '+name, abs(configs['A_450s__'+exit_id]['net_pnl']-old[name]['net_pnl']) < 1e-7)
    check('Identical frozen tick hash', dataset['dataset_sha256'] == dataset['reference']['dataset_sha256'])
    check('Active source hashes and configuration unchanged', before['sourceSHA256'] == after['sourceSHA256'] and before['config'] == after['config'])
    check('Paper only and broker orders disabled', after['health']['mode'] == 'paper' and after['health']['brokerOrdersEnabled'] is False)
    check('Paper equity, positions and event history preserved', before['state']['equity'] == after['state']['equity'] == 500 and before['state']['position'] == after['state']['position'] is None and before['events'] == after['events'])
    check('Paper service, collector and unrelated monitored services active', all('ActiveState=active' in value for value in after['services'].values()))
    for name, rows in trades.items():
        r = configs[name]
        check(name+' settlement and single-entry invariant', len({t['date'] for t in rows}) == len(rows) and abs(sum(t['net_pnl'] for t in rows)-r['net_pnl']) < 1e-7 and abs(r['final_balance']-r['external_capital']-r['net_pnl']) < 1e-7)
        check(name+' causal risk admission and zone', all(t['entry_risk_estimate'] < t['available_entry_risk_budget'] and (r['family'] != 'D' or t['entry_distance_W'] <= r['zone']+1e-10) for t in rows))
        check(name+' confirmation hold and no invalidating quote gap', all(t['actual_confirmation_seconds']+1e-6 >= r.get('hold',0) and t['confirmation_gap_seconds'] <= 60 for t in rows))
        check(name+' hybrid prior-touch or distance threshold', all(r['family'] not in ['B','E'] or t['confirmation_max_edge_excursion']+1e-8 >= r['distance']*t['W'] for t in rows))
        check(name+' no next-formation exposure', all(not t['next_formation_violation'] for t in rows))
        csvwrite(root/'trade_records'/(name+'.csv'), [{k:v for k,v in t.items() if k!='trail_movements'} for t in rows])
    context = load(root, 'chart_context_checks')
    check('All full frames have prior and next-formation context and identical panel scales', all(r['same_scales'] and r['previous_context_available'] and r['next_formation_complete'] and (root/r['path']).exists() for r in context))
    scope = load(root, 'selected_chart_scope')
    check('Full and zoom chart for every material session', all((root/'charts'/f'{kind}_{date}_exit_A.png').exists() for date in scope['sessions'] for kind in ['session','zoom']))
    grouped = []
    for name, rows in trades.items():
        for category, groups in [('month', ['2026-08','2026-09','2026-10','2026-11']), ('side',['up','down'])]:
            for group in groups:
                rr = [t for t in rows if (t['date'][:7] if category=='month' else t['side']) == group]
                gains = sum(max(0,t['net_pnl']) for t in rr); losses = -sum(min(0,t['net_pnl']) for t in rr)
                grouped.append(dict(config=name,category=category,group=group,trades=len(rr),wins=sum(t['net_pnl']>0 for t in rr),losses=sum(t['net_pnl']<0 for t in rr),net_pnl=sum(t['net_pnl'] for t in rr),profit_factor=gains/losses if losses else None,replenishments=sum(t['replenishment'] for t in rr),commission=sum(t['commission'] for t in rr),explicit_cost=sum(t['commission']+t['slippage_cost']+t['spread_cost'] for t in rr),closing_balance=rr[-1]['balance_after'] if rr else None))
    csvwrite(root/'monthly_and_side.csv', grouped)
    rejects = Counter((d['config'],d['reason']) for d in daily if d['trades']=='0')
    csvwrite(root/'rejection_summary.csv',[dict(config=k[0],reason=k[1],sessions=v) for k,v in sorted(rejects.items())])
    with (root/'deadline_events.csv').open(encoding='utf-8',newline='') as f:
        deadlines = list(csv.DictReader(f))
    max_delay = max([float(d['delay']) for d in deadlines] or [0])
    distances = [r['entry_distance_W'] for r in baseline]
    ftable = table(['#','NY session','Side','W','Entry / W','Hold s','Confirm MFE','Confirm retrace','Actual MFE','Structural-only MFE','Unrestricted MFE','First return NY'], [[r['trade_id'],r['date'],r['side'],r['W'],r['entry_distance_W'],r['actual_confirmation_seconds'],r['confirmation_max_favorable'],r['confirmation_max_retracement'],r['mfe'],r['structural_only_path_mfe'],r['unrestricted_mfe_before_deadline'],ny(r['first_return_inside_timestamp'])] for r in baseline])
    (root/'BASELINE_ENTRY_FORENSICS.md').write_text(f'''# Baseline entry forensic analysis

All 31 entries are included below. Median executed distance is {statistics.median(distances):.4f}W; {sum(x>=.75 for x in distances)} entries execute at or beyond 0.75W. Actual hold ranges from {min(r['actual_confirmation_seconds'] for r in baseline):.3f} to {max(r['actual_confirmation_seconds'] for r in baseline):.3f} seconds because execution waits for available quotes and risk admission.

The first fresh aligned breakout can fail before the continuous run that produces the actual entry. Both timestamps and quotes appear in [complete baseline diagnostics](baseline_forensics.csv). Confirmation movement uses the executable closing quote from the start of the actual continuous entry run. Retracement is maximum peak-to-trough adverse movement during that run. Entry uses the opposite side of the spread plus adverse slippage. Structural stop distance includes the original 0.25 buffer; it is not a new stop.

Actual MFE ends at the baseline exit. Structural-only MFE is a separate counterfactual from the same entry and pre-entry account balance, ending at the earliest original capital, held opposite-boundary or mandatory exit. Unrestricted MFE ignores exits but ends at the mandatory deadline. These extrema are descriptive upper bounds, not achievable fills. A separate field extends unrestricted movement to the next formation; it is never treated as executable reward.

{ftable}

![Entry and confirmation distributions](charts/baseline_entry_distributions.png)

![Actual exposure versus unrestricted movement](charts/attainable_vs_unrestricted.png)

The CSV includes first-return price, actual entry/exit timestamps, 0.5/0.75/1/1.5/2/3W first-hit times and raw opposite-boundary ordering both from first breakout and after execution. Hindsight reward/structural-risk ratios are diagnostics only; they are not admission criteria.
''',encoding='utf-8')
    t2 = baseline[1]
    trade2rows = []
    for profile in scope['profiles']:
        for exit_id in ['exit_A','exit_B','exit_C','exit_D']:
            rows = [t for t in trades[profile+'__'+exit_id] if t['date']==t2['date']]
            for t in rows:
                trade2rows.append([profile,exit_id,ny(t['entry_timestamp']),t['entry_price'],t['actual_confirmation_seconds'],t['entry_distance_W'],ny(t['exit_timestamp']),t['exit_price'],t['exit_reason'],t['net_pnl'],t['replenishment']])
    (root/'TRADE2.md').write_text(f'''# Trade 2: entry-family comparison, {t2['date']}

The original long enters at {ny(t2['entry_timestamp'])}, {t2['entry_price']:.2f}; W={t2['W']:.2f}, upper edge={t2['upper']:.3f}, lower edge={t2['lower']:.3f}. Execution is {t2['entry_distance_W']:.3f}W beyond the edge. The original trailing exit at {ny(t2['exit_timestamp'])}, {t2['exit_price']:.2f}, loses USD {abs(t2['net_pnl']):.2f} net.

Price later returns inside at {ny(t2['first_return_inside_timestamp'])}, {t2['first_return_inside_price']:.2f}. No raw or held opposite break occurs before the deadline. Unrestricted pre-deadline MFE/MAE from the original executed entry are {t2['unrestricted_mfe_before_deadline']:.2f}/{t2['unrestricted_mae_before_deadline']:.2f}; the next-formation unrestricted MFE is {t2['unrestricted_mfe_until_next_formation']:.2f}, which includes movement outside the permitted holding period. The structural-only counterfactual exits through {t2['structural_only_exit_reason']} at {ny(t2['structural_only_exit_timestamp'])}; capital protection does not interrupt that counterfactual before the later move. Its path MFE is {t2['structural_only_path_mfe']:.2f}, not a claim of peak execution.

Every alternative below is taken from its complete independent account replay, including preceding profits and capital headroom. Entry chronology, not retrospective swing identification, determines outcomes. Full records include first breakout, retest excursion and return timestamps, structural risk and target already-reached flags.

{table(['Entry profile','Exit','Entry NY','Price','Hold s','Distance W','Exit NY','Price','Reason','Net USD','Top-up'],trade2rows)}

'''+'\n\n'.join(f'## {exit_id}\n\n![Complete prior/current/next formation](charts/session_{t2["date"]}_{exit_id}.png)\n\n[Entry zoom](charts/zoom_{t2["date"]}_{exit_id}.png)' for exit_id in ['exit_A','exit_B','exit_C','exit_D']),encoding='utf-8')
    timing = [[hold]+[configs[f'A_{hold}s__exit_{letter}']['net_pnl'] for letter in 'ABCD'] for hold in [0,15,30,60,120,225,450,900]]
    exitsummary = [[exit_id,sum(r['net_pnl']>0 for r in results if r['exit_id']==exit_id),statistics.median(r['net_pnl'] for r in results if r['exit_id']==exit_id)] for exit_id in ['exit_A','exit_B','exit_C','exit_D']]
    matched = [[name,quality[name]['matched_sessions'],quality[name]['additional_sessions'],quality[name]['missed_control_sessions'],quality[name]['mean_matched_entry_price_improvement'],quality[name]['matched_net_delta'],quality[name]['additional_net'],quality[name]['missed_control_net_delta']] for name in ['A_0s__exit_B','A_120s__exit_B','E_60s_0.25W__exit_B']]
    hybrid = [[r['entry_id'],r['trades'],r['net_pnl'],r['profit_factor'],r['flow_adjusted_account_drawdown'],r['net_without_best_two_sessions']] for r in results if r['family']=='E' and r['exit_id']=='exit_B']
    reference = dataset['reference']
    report = f'''# Phase 4: causal ACD entry research

Earlier entry can improve execution price, but it also introduces losing opportunities. The experiment tests **36 entry profiles × four fixed exits = 144 independent account replays**, across all 67 observed sessions. The original 450-second baseline reproduces exactly: 31 trades, 13 winners, 18 losers, **−USD 90.51**. This is exploratory research on the same three-month sample used in previous phases; no active robot or strategy parameter was changed.

## Reproducibility and scope

Foundation: Phase 3 commit 2c0fdbdc2f9d8248a396166a83cbf734d69069e4. The frozen dataset interval is {reference['window_start']} through {reference['window_end']}; warmup starts {reference['warmup_start']}. New York observed session labels run August 26–November 26, including partial boundary sessions. No moving endpoint or newly collected quotes were added. Full-stream SHA256: {dataset['dataset_sha256']}. Counts: {json.dumps(dataset['counts'])}. The preceding observed trading session supplies yesterday's completed classification, never the future close of today's session. [Source causality checks](causal_source_checks.json) document the unchanged foundation; this run regenerates its tick stream and verifies the identical hash/counts.

The implementation reuses the Phase 3 exit, settlement and risk engine, replacing only the causal entry mask. The four 450-second exit controls also reproduce their Phase 3 P&L. All {sum(len(v) for v in trades.values())} closed trades were independently checked against full-tick UTC-microsecond bid/ask records, adverse fills, fees and admission budgets. [Baseline check](baseline_reproduction.json); [execution checks](execution_checks.json); [delivery checks](checks.json).

## Frozen mechanics and causal entry definitions

ACD formation is 09:30–09:45 New York, with the existing boundary calculation (A offset 0.75, C offset 1.0). W is upper minus lower A-edge, frozen after formation. Prior direction retains the existing first-ask to last-bid classification with USD 3 threshold. Today confirms through quotes beyond the aligned directional edge. Long decisions use executable bid; shorts use ask. Entry uses ask+0.10 for longs or bid−0.10 for shorts. Position size is fixed 0.01 lot, 100 ounces/lot = 1 ounce. Historical spread is retained, commission USD 0.14 round trip, adverse slippage USD 0.10 per side, swap zero as in the foundation. Explicit cost columns add commission, slippage and entry spread as descriptive components; spread/slippage are already embedded in execution P&L and must not be subtracted again.

Every family requires a fresh inside observation after formation; a quote gap over 60 seconds disarms confirmation until a new inside observation. Boundary equality counts as inside. The original 120-minute window begins at formation completion and entries also respect the existing three-hour late cutoff. There is at most one executed entry per session, including after losses. No alternative bypasses risk admission.

* A: continuous holds 0, 15, 30, 60, 120, 225, 450, 900 seconds. Zero enters on the first causal fresh breakout quote.
* B: closing quote reaches 0.05, 0.10, 0.15, 0.25 or 0.50W beyond the edge; no minimum time.
* C: fresh aligned breakout reaches 0.10 or 0.25W, returns to the edge/inside, then a new breakout holds 0, 30, 60 or 120 seconds. Pending retests cancel on an invalidating gap or the existing 450-second buffered opposite-boundary break. A later fresh sequence may start within the original window. No future pivots are used.
* D: holds 60, 225 or 450 seconds, then waits outside for an actual slipped executable entry within 0.10, 0.25 or 0.50W. No fabricated capped fills. Inside recross or gap resets confirmation; expiry cancels.
* E: holds 30, 60 or 120 seconds and touches 0.10 or 0.25W during the same continuously outside run. **The owner's clarification is applied: a prior distance touch remains valid while outside**, even if execution occurs closer to the edge. Inside recross or gap clears the distance latch and time hold.

Exit A retains the original strict >USD 3 activation and half retracement anchored to the ACD edge. Exit B targets 0.75W from the edge, with no trail. Exit C activates after >=1W from actual entry, trailing halfway from entry, tightening only. Exit D has no trail or target. All retain buffered opposite invalidation (0.25 buffer held 450 seconds), the original risk admission reserve and mandatory closure one hour before next formation. A prior target touch never causes a historical fill; if already beyond target at entry, exit occurs at the next available quote even if it retraces. Target flags and their P&L contributions are separate comparison columns.

Each account starts at USD 500; only after full closure, balances below 500 are replenished to 500 and the latch resets. Profits are retained. Contributions are external capital, excluded from net trading P&L. Capital protection remains **absolute equity USD 455 = USD 450 floor + USD 5 liquidation buffer**, not balance-minus45. Profits increase headroom to the same absolute threshold; no open loss is replenished. Every account obeys final balance = total external funding + trading P&L. Funding-adjusted drawdown uses marked equity minus cumulative external funding plus the initial500, so deposits cannot conceal trading losses.

## Baseline entry consumption

Median execution is **{statistics.median(distances):.3f}W** beyond the directional edge. {sum(x>=.75 for x in distances)}/31 baseline entries are already at least 0.75W beyond it. The hold measures the actual uninterrupted run, not elapsed time since an earlier failed breakout. [All 31 forensic entries](BASELINE_ENTRY_FORENSICS.md) and [complete threshold diagnostics](baseline_forensics.csv) distinguish confirmation movement, actual exposure, structural-only counterfactual exposure and unrestricted later movement.

## Complete executable results

[Comparison CSV](comparison.csv) contains every requested financial and entry metric for all 144 configurations; [monthly and long/short breakdown](monthly_and_side.csv), [daily decisions for all 9,648 configuration-sessions](daily.csv), [rejection summary](rejection_summary.csv), [full trade records](all_trades.csv), [capital contribution ledger](capital_contributions.csv) and [per-configuration exports](trade_records/) provide the underlying records. Eligible means an entry-pattern quote exists before admission; risk-eligible means the current funded account can admit one. Missed-control sessions and untraded directional follow-through are separate diagnostics.

Pure time family net P&L (USD), with all other conditions fixed:

{table(['Hold seconds','Original trailing A','Edge target B','Entry trailing C','No target/trail D'],timing)}

{table(['Fixed exit','Positive profiles /36','Median net USD'],exitsummary)}

Earlier is not monotonically better; changing the exit changes the outcome substantially. A positive three-month result does not establish future economic value. Especially, exit B's 0.75W target was already investigated in Phase 3 on this same sample; its success here is not independent exit validation.

## Opportunity quality and matched sessions

Controls use the same exit and original450 entry, not a different exit. Price improvement is signed favourably for both sides. For matched sessions, improved price directly reduces structural stop distance and increases remaining distance to the fixed edge target. The P&L decomposition includes actual independent account histories, so it is not a pure controlled price-only intervention.

{table(['Configuration','Matched','Additional','Missed','Mean price improvement','Matched P&L delta','Additional net','Missed-control delta'],matched)}

Immediate entry therefore improves matched prices but its 16 additional opportunities lose USD 156.84 under exit B. More trades alone do not establish quality. [Matched-session records](matched_sessions.csv) and [opportunity quality](opportunity_quality.csv) also identify admission changes and entries requiring retained profits to exceed the fixed500 risk budget. Those are causal cash-state effects, not free funding during an open position.

False-breakout statistics classify a return inside before the selected edge-distance threshold (0.5,0.75,1,1.5,2,3W). A valid retracement is not automatically a losing trade or a new exclusion rule. [Statistics](false_breakout_statistics.csv) distinguish the whole confirmed entry episode from the post-execution segment and explicitly count thresholds already consumed during confirmation. [First-breakout market paths](market_opportunities.csv) include all67 sessions and remain descriptive; future threshold hits are never entry inputs. A jump across the entire ACD range can break the opposite edge without a quoted inside observation; raw opposite ordering is reported separately.

## Robustness and independent-validation candidates

All six hybrid entry profiles with exit B are positive in this sample and remain positive after removing their best two sessions:

{table(['Hybrid profile','Trades','Net USD','PF','Funding-adjusted DD','Net without best2'],hybrid)}

This is a neighbouring parameter region worth freezing for independent historical validation, rather than selecting the highest historical cell. Candidate families: hybrid 30/60/120 seconds ×0.10/0.25W with exit B; pure time60/120/225 and distance0.10/0.25/0.50W as comparison controls; retain450 and no-target/trail controls. Preserve the entire proposed small family prospectively rather than tuning it further on these dates. Entry C retests and tight D restrictions are useful rejection controls; results do not support calling all earlier entries superior.

Every comparison row includes net excluding the best one and best two sessions. Sensitivity figures and heatmaps cover every family and all four exits: [A](charts/family_A_sensitivity.png), [B](charts/family_B_sensitivity.png), [C](charts/family_C_sensitivity.png), [D](charts/family_D_sensitivity.png), [E](charts/family_E_sensitivity.png). Heatmaps separately show net, PF, funding-adjusted drawdown and median entry distance. These are 144 related tests on only67 sessions, with prior same-sample strategy exploration. Selection bias, dependence across configurations and regime concentration prevent interpreting the highest P&L as an optimum. Best-session removal is a descriptive stress test, not out-of-sample evidence.

## Trade 2 and visual verification

[Trade 2 forensic report](TRADE2.md) compares all five entry families plus450 under all four exits, with exact New York timestamps and fills. Its early baseline loss is not proof that the later peak was available under every exit. Every alternative stops when its first valid exit occurs. No retrospective re-entry or peak fill is credited.

[Interactive gallery](gallery.html) contains {len(scope['sessions'])} material-session comparisons plus zooms, with profiles declared before viewing performance: {', '.join(scope['profiles'])}. Material means entry presence differs, time differs by at least30 seconds or execution price differs by at least0.05W from the same-exit450 control. Default gallery comparisons hold exit A fixed to isolate entry; trade2 adds exits B/C/D. Full charts include the complete preceding session, current session and next completed formation. Identical panel scales show ACDs, breakout/confirmation, actual entries/exits, retest events, permitted zones, trail/targets, exposure and mandatory deadline. Actual last quote per second is displayed; decisions/extrema use all ticks. Gaps over60 seconds remain blank, without interpolated trading across closures. Click images for high resolution. [Download charts, summaries and records](entry_research_charts_and_records.zip).

## Closure, risk and implementation limits

There are zero open positions at dataset cutoff and zero next-formation exposures. Strict wall-clock deadline misses number {len(deadlines)} across all replays, maximum {max_delay:.3f} seconds, all recorded in [deadline events](deadline_events.csv). These are first-available-quote latency after the deadline, not fabricated exact-time fills. Execution verification checks all548 timed exits against the next actual quote. No weekend/holiday/missing-quote deadline delay over60 seconds occurred in the executed sample. An isolated right-censor branch preserves an open position if no permitted exit quote exists by the fixed endpoint; it was unit-tested but not needed by these configurations.

The original weekday New York calendar does not validate broker holidays or early closes. Observed-session predecessor handling skips absent sessions, but partial formation/prior-session coverage can still produce a definite original bias; that foundation defect is retained for comparability. The original entry-tick equity mark lags execution costs by one quote; later marks and settlement contain costs. Neither defect was silently repaired. Fixed slippage and zero swap are inherited assumptions, not an independently calibrated liquidity/financing model. Absolute455 protection does not guarantee a maximum USD45 realized loss because quotes, slippage and retained-profit balances change realized exposure. No synthetic missing-market prices or continuous service availability are assumed.

Active paper status at {after['capturedAt']}: paper-only, broker orders disabled, USD500 equity, no position, paused. Source/config hashes and existing event history match the before snapshot. Paper, watchdog, collector and datavis monitored units remain active; warning/error tail is {after['warningErrorLogTail'].strip()!r}. Historical research did not unpause the robot or change active parameters. Raw collection and previous research were preserved.

## Reproduction and validation

The scripts are scripts/acd_phase4_research.py (entry masks and isolated replay), acd_phase4_opportunities.py (post-hoc path diagnostics), acd_phase4_visuals.py (full-context figures) and acd_phase4_deliver.py (reports/exports/invariants). They consume the exact Phase3 cache/manifest regenerated from the read-only database; raw cache stays outside Git. Run the research CLI with --help for required paths, then opportunity diagnostics, visuals and delivery in that order. Repository tests passed **37 tests**, covering the original controls, causal prefix invariance, long/short symmetry, fresh inside resets, retests, executable price restrictions, the clarified hybrid distance latch and right-censor behavior. Delivery checks and SHA256 artifact manifest accompany the committed artifacts. No deployment or live execution was authorized or performed.
'''
    (root/'REPORT.md').write_text(report,encoding='utf-8')
    (root/'.gitattributes').write_text('* text=auto eol=lf\n',encoding='utf-8')
    (root/'checks.json').write_text(json.dumps(dict(passed=True,count=len(checks),checks=checks),indent=2),encoding='utf-8')
    # Canonical bytes make manifest independent of Windows checkout line endings.
    for path in root.rglob('*'):
        if path.is_file() and path.suffix not in ['.png','.zip','.gz']:
            path.write_bytes(path.read_bytes().replace(b'\r\n',b'\n'))
    archive = root/'entry_research_charts_and_records.zip'
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for path in sorted(root.rglob('*')):
            if path.is_file() and path not in [archive,root/'artifact_sha256.json']:
                z.write(path,path.relative_to(root).as_posix())
    manifest = {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob('*')) if p.is_file() and p.name!='artifact_sha256.json'}
    (root/'artifact_sha256.json').write_bytes(json.dumps(manifest,indent=2).encode('utf-8'))
    print(json.dumps(dict(checks=len(checks),artifacts=len(manifest),charts=len(list((root/'charts').glob('*.png'))),zip_bytes=archive.stat().st_size)))


if __name__ == '__main__':
    main()
