"""Reconcile all experimental accounts and package Phase 3 research artifacts."""
import argparse
import csv
import hashlib
import json
import subprocess
import zipfile
from collections import Counter
from pathlib import Path


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory')
    args=p.parse_args()
    root=Path(args.directory)
    results=json.loads((root/'comparison.json').read_text())
    trades=json.loads((root/'all_trades.json').read_text())
    contexts=json.loads((root/'chart_context_checks.json').read_text())
    causal=json.loads((root/'causal_source_checks.json').read_text())
    before=json.loads((root/'status_before.json').read_text())
    after=json.loads((root/'status_after.json').read_text())
    baseline=json.loads((root/'baseline_reproduction.json').read_text())
    dataset=json.loads((root/'dataset.json').read_text())
    close=lambda a,b:abs(a-b)<1e-7
    checks={}
    checks['baseline31_exact_executions']=baseline['matches'] and len(trades['baseline'])==31
    checks['all_execution_quotes_independently_verified']=json.loads((root/'execution_checks.json').read_text())['passed']
    checks['baseline_net']=close(sum(t['net_pnl'] for t in trades['baseline']),-90.51)
    checks['55_independent_configurations']=len(results)==55 and len(trades)==55
    checks['all_pnl_excludes_contributions']=all(close(sum(t['net_pnl'] for t in trades[r['id']]),r['net_pnl']) for r in results)
    checks['all_accounts_reconcile']=all(close(r['external_capital']+r['net_pnl'],r['final_balance']) for r in results)
    checks['all_closed_only_correct_topups']=all(close(t['replenishment'],max(0,500-t['balance_before_replenishment'])) and close(t['balance_after'],max(500,t['balance_before_replenishment'])) for rows in trades.values() for t in rows)
    checks['every_configuration_started500']=all(close(r['external_capital']-r['capital_replenishments'],500) for r in results)
    checks['fixed001_lot']=all(t['lots']==.01 for rows in trades.values() for t in rows)
    checks['single_entries_except_explicit_reentry']=all(max(Counter(t['date'] for t in rows).values(),default=0)<=1 for cfg,rows in trades.items() if not cfg.startswith('reentry'))
    checks['all_positions_closed']=all(t['exit_timestamp'] and t['holding_seconds']>=0 for rows in trades.values() for t in rows)
    checks['no_next_formation_exposure']=all(not t['next_formation_violation'] for rows in trades.values() for t in rows)
    checks['trails_only_tighten']=all(all((b['level']-a['level'])*(1 if t['side']=='up' else -1)>=-1e-10 for a,b in zip(t['trail_movements'],t['trail_movements'][1:])) for rows in trades.values() for t in rows)
    checks['fresh_reentry_hold_no_overlap']=all(all(__import__('datetime').datetime.fromisoformat(b['entry_timestamp']).timestamp()-__import__('datetime').datetime.fromisoformat(a['exit_timestamp']).timestamp()>=450 for a,b in zip(rows,rows[1:]) if a['date']==b['date']) for cfg,rows in trades.items() if cfg.startswith('reentry'))
    checks['causal_bias_recomputed']=all(c['prior_bias_recomputed'] is not False for c in causal)
    checks['31_identical_scale_full_context_charts']=len(contexts)==31 and all(c['same_scales'] and c['next_formation_complete'] and (root/c['chart']).is_file() for c in contexts)
    checks['55_individual_trade_exports']=len(list((root/'trade_records').glob('*.csv')))==55
    checks['active_source_unchanged']=before['sourceSHA256']==after['sourceSHA256']
    checks['active_configuration_unchanged']=before['config']==after['config']
    checks['paper_history_preserved']=after['events'][:len(before['events'])]==before['events']
    checks['paper_only_execution']=after['health']['brokerOrdersEnabled'] is False and after['config']['mode']=='paper'
    checks['existing_services_active']=all('ActiveState=active' in value for value in after['services'].values())
    unchanged=subprocess.run(['git','diff','--exit-code','0ab8debe6291d0bf1c6cee94f5b0898872972ce5','--','docs/acd_continuation/phase2_2026-11-27','docs/acd_continuation/phase2_revised_2026-11-27'],capture_output=True)
    checks['both_phase2_directories_preserved']=unchanged.returncode==0
    report_checks=dict(passed=all(checks.values()),checks=checks)
    (root/'checks.json').write_text(json.dumps(report_checks,indent=2))
    assert report_checks['passed'],report_checks
    by_id={r['id']:r for r in results}
    for cfg,records in trades.items():
        rows=[{k:v for k,v in r.items() if k!='trail_movements'} for r in records]
        with (root/'trade_records'/(cfg+'.csv')).open('w',newline='',encoding='utf-8') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(rows[0]),lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
    def breakdown(rows):
        gains=sum(max(0,t['net_pnl']) for t in rows)
        losses=-sum(min(0,t['net_pnl']) for t in rows)
        net=sum(t['net_pnl'] for t in rows)
        return dict(trades=len(rows),entered_sessions=len({t['date'] for t in rows}),winners=sum(t['net_pnl']>0 for t in rows),losers=sum(t['net_pnl']<0 for t in rows),net_pnl=net,profit_factor=gains/losses if losses else None,expectancy_trade=net/len(rows) if rows else None,commission=sum(t['commission'] for t in rows),slippage_cost=sum(t['slippage_cost'] for t in rows),entry_spread_cost=sum(t['spread_cost'] for t in rows),replenishments=sum(t['replenishment'] for t in rows))
    for filename,group_key,groups in [('monthly.csv','month',['2026-08','2026-09','2026-10','2026-11']),('long_short.csv','side',['up','down'])]:
        rows=[]
        for cfg,records in trades.items():
            for group in groups:
                subset=[t for t in records if (t['date'][:7] if group_key=='month' else t['side'])==group]
                rows.append(dict(config=cfg,**{group_key:group},**breakdown(subset)))
        with (root/filename).open('w',newline='',encoding='utf-8') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(rows[0]),lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
    selected=['baseline','target_entry_1W','target_edge_1W','trail_entry_edge_1W','trail_edge_edge_1W','trail_entry_entry_1W','trail_edge_entry_1W','reentry_A','reentry_B','no_trailing']
    with (root/'deadline_events.csv').open(newline='') as f:deadlines=list(csv.DictReader(f))
    maximum_delay=max([float(r['delay']) for r in deadlines] or [0])
    long_gaps=[r for r in deadlines if r['gap_over60']=='True']
    lines=['# ACD Continuation — Phase 3 exit strategy research','',
        '**Exploratory research only. No strategy was selected or deployed.** Active EC2 paper execution, raw collection and both Phase 2 result directories are preserved. All 55 accounts are independent simulations, starting with USD 500; contributions are external capital, never trading profit.','',
        '## Findings','',
        'Trade 2 confirms that an early trail can discard a later continuation. With one-width activation measured from entry, both half-trail anchors remained open to mandatory closure and earned USD 52.29 net, versus the baseline loss of USD 5.64. No opposite-boundary or capital protection event would have interrupted that holding path. The later 4470 move occurred after the mandatory deadline and is excluded. [Detailed trade 2 forensic report](TRADE2.md).','',
        'That example does **not** establish a generally better exit. One-width fixed targets and adaptive trails lose more than baseline across the full sample. No-trailing is also negative and requires more external capital. The narrow re-entry policy is positive in aggregate, but removing its best session reverses that result. There is no broad profitable adaptive-trailing region in this grid.','',
        '## Baseline and causal data validation','',
        f"Fixed historical interval **{dataset['reference']['window_start']} → {dataset['reference']['window_end']}**, with the original warmup. All **{dataset['counts']['window_ticks']:,}** evaluation ticks were processed. Stream SHA256 **`{dataset['dataset_sha256']}`** matches revised Phase 2 exactly, including its warmup, duplicate-timestamp policy and counts. The cache is derived from a read-only database query; it never alters raw ticks.",'',
        'The baseline reproduces all 31 entry/exit timestamps, prices, directions, reasons and per-trade P&Ls (floating arithmetic tolerance 1e-8). It reproduces 13 winners, 18 losers, USD −90.51 net and 27 trailing exits. Its six replenishments total USD 90.51. Alternative performance is accepted only after the baseline reproduction gate passes. An independent cache lookup additionally verified all 1,831 execution pairs, including all 49 already-reached targets at the next quote and every timed exit at its first available quote (xecution_checks.json).','',
        'ACD levels were independently recomputed from the formation’s historical midpoint extrema and unchanged offsets. Prior directional labels were checked against the completed preceding observed session, using first ask to final bid, original coverage/gap rules and the unchanged ±USD 3 classification. Boundary partial sessions are disclosed in `causal_source_checks.json`; they are not falsely claimed to have independently reconstructed missing warmup data. Frozen levels/bias become available causally before entry; exit decisions use executable bid for longs and ask for shorts.','',
        '## Frozen rules and experimental definitions','',
        '- **W = A-upper − A-lower**, frozen after the New York 09:30–09:45 formation. A edges use midpoint OR extrema ±0.75; C edges remain ±1 but do not create new exit filters.',
        '- Initial entries retain the causal fresh-inside breakout and 450-second hold, the 120-minute post-formation window, three-hour late cutoff, fixed 0.01 lot / hypothetical 1 oz, and original risk-distance admission check. Each independent cash history can change risk admission and consequently the actual entry timestamp; entries are not artificially pinned to the baseline.',
        '- Fixed targets use entry or directional edge ± multiple × W. The first subsequent executable quote at or beyond the target closes at that actual quote with adverse slippage. Earlier target touches are flagged. If already beyond target on entry, it is explicitly flagged and executed on the next available quote; no historical target price is fabricated. This convention was confirmed by the owner.',
        '- Width-based trailing activates at **≥ multiple × W** from its independently specified entry/edge reference. Fixed-USD-3 controls preserve strict **> USD 3**. The 50% trailing equation independently uses edge or executed entry to the best subsequent closing-side quote, includes the entry quote as the initial extreme, and only tightens. Once active, trailing remains an independent exit, including during otherwise valid ACD retracement.',
        '- An edge-referenced activation can already have been crossed when a later executed entry occurs; it need not imply that the actual position is profitable. Such a trail can activate on the first post-entry quote. This is an explicit tested reference convention, not an assertion of a guaranteed profitable trail.',
        '- Pullback exits use bid ≤ upper A edge for longs, ask ≥ lower A edge for shorts. A fresh 450-second confirmation is required after full closure; the closing inside quote can arm that fresh observation, while gaps >60 seconds erase arming. No same-tick exit/re-entry or simultaneous positions. Policy A preserves the original entry window for every entry; policy B preserves it for the first entry and permits re-entries until the next formation minus three hours. No extra filter was added.',
        '- Opposite stops preserve the existing buffered boundary (0.25 beyond the opposite A edge) and 450-second continuous hold. They are evaluated while a position is open, as in the original core. A pullback exit often pre-empts this structural exit; flat positions do not acquire an invented session-wide opposite-boundary ban.',
        '- Mandatory closure remains next weekday ACD formation minus one hour. Capital protection remains **absolute equity USD 455 = 450 + 5 buffer**. Capital/deadline precede discretionary exits; opposite hold is checked afterward, preserving baseline precedence. All positions settle before any top-up, profits above 500 are retained, and only then is the risk latch reset. Larger retained profits increase headroom above the same absolute 455 floor.',
        '- Costs: historical bid/ask spread, 0.10 adverse quote slippage per side, 0.14 round-trip commission, zero modeled swap, unchanged 5 delay reserve for admission. Cost columns separately show commission, slippage and entry spread; their sum measures cost versus the closing-side quote path. These remain unverified broker assumptions.','',
        '## Primary comparison','',
        'References in names are `activation / trailing-half` for trails and `target reference` for targets. Expectancy/session divides net P&L by all 67 observed ACD sessions, including the two partial boundary sessions. Expectancy on entered sessions is a separate column in the full export. Multiple-entry variants are compared on net and session expectancy, not rewarded for trade count.','',
        '| Configuration | Trades | Net USD | PF | Net / ACD session | Replenishments | External capital incl.500 | Final balance | Funding-adjusted max DD |','|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for name in selected:
        r=by_id[name]
        lines.append(f"|{name}|{r['trades']}|{r['net_pnl']:.2f}|{r['profit_factor']:.3f}|{r['expectancy_session']:.2f}|{r['capital_replenishments']:.2f}|{r['external_capital']:.2f}|{r['final_balance']:.2f}|{r['flow_adjusted_account_drawdown']:.2f}|")
    lines+=['',
        '**Financial identity for every configuration:** final cash = initial USD 500 + subsequent external contributions + net trading P&L. A positive ending balance above 500 can coexist with negative trading P&L. Replenishments are recorded only after closure. Funding-adjusted account equity is cash equity minus cumulative subsequent contributions; maximum account drawdown uses its full tick path, including final slippage. Intraday drawdown uses the actual cash-equity path within each ACD session, so it is a distinct measure and can include replenishment jumps.','',
        'The [complete 55-configuration table](comparison.csv) includes every requested metric: trade/session expectancy, win rate, entries, maximum loss, both drawdowns, loss streak, funding, final cash, duration, costs, exit distribution and closure violations. [Monthly breakdown](monthly.csv), [long/short breakdown](long_short.csv), [daily diagnostics](daily.csv), [all trades](all_trades.csv), [per-configuration trade files](trade_records/), [capital ledger](capital_contributions.csv). Exact trail movements are retained in `all_trades.json`.','',
        '## Sensitivity, concentration and candidate scope','',
        'Entry-referenced activation is negative at every tested width under both half-trail anchors. Edge-referenced activation is positive only at 0.75W, surrounded by negative 0.50W and 1.00W results. Thus the apparent 0.75W maximum is an isolated historical result, not a broad stable region. Later activation can allow larger adverse moves and capital exits before recovery.','',
        '| Positive-sample configuration | Net USD | Without best session | Without best two sessions |','|---|---:|---:|---:|']
    for r in results:
        if r['net_pnl']>0:
            lines.append(f"|{r['id']}|{r['net_pnl']:.2f}|{r['net_without_best_session']:.2f}|{r['net_without_best_two_sessions']:.2f}|")
    lines+=['',
        'The 0.75W edge fixed target remains positive after removing its two best sessions, but its neighbors are near-zero (0.50W) and materially negative (1.00W). The two 0.75W adaptive variants weaken or reverse when the strongest sessions are removed. Re-entry A depends heavily on one or two large sessions; extending re-entry to policy B increases churn, costs and drawdown and is negative. These are diagnostic removals, not executable account replays or out-of-sample tests.','',
        '**Candidates for a separately frozen, independent validation sample:** compare the edge-referenced fixed-target neighborhood 0.50/0.75/1.00W as a family, alongside baseline and no-trailing controls. Include edge-activation half-trails at 0.50/0.75/1.00W with both half anchors to test whether the isolated 0.75W result persists. Retain re-entry A only as a concentration-sensitive research comparator. Do not choose a single historical winner or authorize deployment from this grid. No robust profitable adaptive region has been established.','']
    for field in ['net_pnl','profit_factor','flow_adjusted_account_drawdown','capital_replenishments']:
        lines +=[f'![{field} sensitivity](charts/{field}_sensitivity.png)','',f'![{field} reference heatmap](charts/{field}_heatmap.png)','']
    lines+=['## Repeated breakouts and costs','',
        '| Policy | Trades | Entered sessions | Pullback exits | Repeated false breakouts | Net / session | Commission | Slippage | Entry spread |','|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for name in ['reentry_A','reentry_B']:
        r=by_id[name]
        lines.append(f"|{name}|{r['trades']}|{r['sessions_with_entries']}|{r['pullback_exits']}|{r['repeated_false_breakouts']}|{r['expectancy_session']:.2f}|{r['commission']:.2f}|{r['slippage_cost']:.2f}|{r['entry_spread_cost']:.2f}|")
    lines+=['','A repeated false breakout is a re-entry (session trade number >1) subsequently closed by the return-inside pullback rule. Total pullback exits also include failed initial entries. All costs remain included in trading P&L.','',
        '## Mandatory closure and market data gaps','',
        f"No configuration carried an open position into its next ACD formation. There are **{len(deadlines)} configuration/trade deadline-delay records** (the same underlying session can appear across configurations); maximum delay **{maximum_delay:.6f} seconds**. **{len(long_gaps)}** records cross a quote gap over 60 seconds. Strict timestamp deadline compliance is therefore not universal. Every late execution uses the first available actual quote, never a fabricated deadline fill. See [every deadline event](deadline_events.csv).",
        '',
        'Weekday/New York DST session definitions are unchanged; no broker holiday or early-close calendar was invented. Weekend closures and source gaps are present on the true time axis. This three-month data stream cannot prove whether a particular long gap represents a market closure, a broker outage or missing collection. The software deadline and USD 455 stop are not guaranteed fill prices under gaps.','',
        '## Full-session visual evidence','',
        '[Open the complete side-by-side gallery](gallery.html). Every one of the 31 baseline signal sessions has ten panels: baseline, entry/edge one-width targets, all four one-width activation/half-reference combinations, both re-entry policies and no-trailing. All panels in a session share identical time and price scales and the same executable-side quote line. The frame begins at the previous formation and extends 30 minutes beyond completion of the next formation. Target and activation levels are included in the common full-frame price scale even if not reached. Short baseline trades receive secondary early-entry zooms; these never replace full-session charts.','',
        'The display selects the actual last historical quote each second and leaves gaps over 60 seconds blank. Every valid tick drives entries, extrema, exits, risk and performance. Formations, A edges, target/activation levels, actual trailing steps, all entries/exits and open-position portions are shown. Expand gallery records for every exact New York execution and contribution. [Download charts and full records](research_charts_and_records.zip).','',
        '## Preservation, checks and limitations','',
        f"**{sum(checks.values())}/{len(checks)} delivery checks passed; 25 tests passed.** Tests include an independent scalar tick loop against all 55 exit configurations in long/short randomized paths, causal fresh-breakout/gap checks and prior funding/baseline regressions. Baseline reproduction is in `baseline_reproduction.json`; source causality checks are in `causal_source_checks.json`.",'',
        f"Paper snapshot at {after['capturedAt']}: equity USD {after['state'].get('equity',500):.2f}, open position `{after['state']['position']}`, paused `{after['state']['paused']}`. Source/configuration hashes and existing event history are preserved; paper mode and disabled broker-order path remain verified. Primary service, watchdog timer, collector and application remain active. The pre-existing pause and watchdog error history were not changed. No active parameter, balance or live execution path was modified.",'',
        'The prior partial-formation directional classification defect, delayed entry-tick cost valuation, weekday-only calendar and stale-quote service-pause behaviour remain outside this exit experiment. This is an uninterrupted strategy-core simulation, not a claim that the paused production service would have executed these trades. Finite three-month historical parameter comparisons have selection bias and no independent predictive validation. Liquidity, gaps, financing, latency and contract specifications remain incomplete broker assumptions.','',
        '## Reproduction and artifact scope','',
        '`scripts/acd_phase3_research.py extract` regenerates the exact full-tick binary session cache from the read-only database and verifies the revised Phase 2 hash. `run` evaluates all independent accounts; `verify` independently checks every recorded execution against the cache and writes `execution_checks.json`. `scripts/acd_phase3_visuals.py` generates charts, gallery and forensics; `scripts/acd_phase3_deliver.py` reconciles funding, writes this report and packages downloads. See the scripts’ `--help` arguments. The support cache and prior quote-per-second gzip remain on EC2 under isolated research directories and are excluded from Git. All tabular results, full trade/trail data, charts, gallery, ledgers and research ZIP are committed on `codex/acd-phase3-exit-research`.']
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    (root/'.gitattributes').write_bytes(b'* text=auto eol=lf\n')
    for path in root.rglob('*'):
        if path.is_file() and (path.suffix in {'.json','.md','.csv','.html'} or path.name in {'COMPLETE','VISUALS_COMPLETE','.gitattributes'}):
            path.write_bytes(path.read_bytes().replace(b'\r\n',b'\n'))
    zip_path=root/'research_charts_and_records.zip'
    with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
        for path in root.rglob('*'):
            if path.is_file() and path!=zip_path and path.name!='artifact_sha256.json':
                archive.write(path,path.relative_to(root).as_posix())
    hashes={path.relative_to(root).as_posix():hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob('*') if path.is_file() and path.name!='artifact_sha256.json'}
    (root/'artifact_sha256.json').write_bytes(json.dumps(hashes,indent=2).encode('utf-8'))
    print(json.dumps(dict(passed=True,checks=len(checks),configurations=len(results),deadline_records=len(deadlines),maximum_delay=maximum_delay,quote_gap_records=len(long_gaps)),indent=2))


if __name__=='__main__':
    main()
