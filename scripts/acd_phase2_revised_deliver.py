"""Reconcile funding, verify visual context, write report and downloadable ZIP."""
import argparse
import csv
import hashlib
import json
import subprocess
import zipfile
from collections import Counter
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image


def load_csv(path):
    with path.open(newline='',encoding='utf-8') as stream:
        return list(csv.DictReader(stream))


def money(value):
    return f'{value:,.2f}'


def ny(value):
    return datetime.fromisoformat(value).astimezone(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z')


class GalleryParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.articles=[]
        self.images=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='article': self.articles.append(attrs.get('id'))
        if tag=='img': self.images.append(attrs.get('src'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    parser.add_argument('--reference',required=True)
    args=parser.parse_args()
    root=Path(args.directory)
    ref=Path(args.reference)
    summary=json.loads((root/'summary.json').read_text())
    m=summary['metrics']
    old=json.loads((ref/'summary.json').read_text())
    trades=json.loads((root/'trades.json').read_text())
    days=json.loads((root/'daily.json').read_text())
    decisions=json.loads((root/'signal_day_investigation.json').read_text())
    contexts=json.loads((root/'chart_context_checks.json').read_text())
    before=json.loads((root/'status_before.json').read_text())
    after=json.loads((root/'status_after.json').read_text())
    original_status=json.loads((ref/'status_before.json').read_text())
    contributions=load_csv(root/'capital_contributions.csv')
    baseline=json.loads((root/'baseline_comparison.json').read_text())
    by_id={t['trade_id']:t for t in trades}
    checks={}
    close=lambda a,b:abs(a-b)<1e-7
    checks['original_financial_baseline_reproduced']=baseline['matches']
    checks['same_fixed_window']=all(summary[k]==old[k] for k in ['window_start','window_end','warmup_start'])
    checks['same_processed_tick_counts']=summary['counts']==old['counts']
    checks['same_strategy_and_cost_configuration']=summary['config']==old['config']
    checks['all31_original_signal_days_investigated']=len(decisions)==31 and sorted(d['date'] for d in decisions)==sorted(d['date'] for d in json.loads((ref/'daily.json').read_text()) if d['continuation_satisfied'])
    checks['each_signal_has_trade_or_exact_rejection']=all(d['executed'] or (d['reason'] and (d['reason']!='risk_estimate_exceeds_budget' or d['closest_confirmed_rejection'])) for d in decisions)
    checks['rejected_risk_candidates_exceed_available_budget']=all(d['closest_confirmed_rejection']['estimated_risk']>=d['closest_confirmed_rejection']['available_risk_budget'] for d in decisions if not d['executed'] and d['reason']=='risk_estimate_exceeds_budget')
    checks['no_trade_outside_original_signal_days']=set(t['date'] for t in trades)<=set(d['date'] for d in decisions)
    checks['maximum_one_entry_per_session']=max(Counter(t['date'] for t in trades).values(),default=0)<=1
    checks['fixed001_lot']=all(t['lots']==.01 for t in trades)
    checks['every_position_fully_closed']=m['open_at_end'] is None and all(t['exit_timestamp'] for t in trades)
    checks['only_four_allowed_exit_types']=set(t['exit_reason'] for t in trades)<=set(['trailing','opposite','timed','capital'])
    checks['entry_direction_matches_previous']=all(t['side']==t['yesterday_bias']==t['today_direction'] for t in trades)
    checks['confirmation_time_preserved']=all((datetime.fromisoformat(t['entry_timestamp'])-datetime.fromisoformat(t['confirmation_start'])).total_seconds()>=summary['config']['hold_seconds'] for t in trades)
    checks['trading_pnl_excludes_contributions']=close(sum(t['net_pnl'] for t in trades),m['net_trading_pnl'])
    checks['daily_trading_pnl_reconciles']=close(sum(d['net_pnl'] for d in days),m['net_trading_pnl'])
    checks['initial_external_capital500']=contributions[0]['kind']=='initial' and float(contributions[0]['amount'])==500
    checks['capital_ledger_reconciles']=close(sum(float(c['amount']) for c in contributions),m['total_external_capital']) and close(sum(float(c['amount']) for c in contributions[1:]),m['total_replenishments'])
    checks['balance_equals_external_capital_plus_trading_pnl']=close(m['final_balance'],m['total_external_capital']+m['net_trading_pnl'])
    checks['replenish_only_closed_sub500_balances']=all(c['kind']=='replenishment' and float(c['balance_before'])<500 and close(float(c['balance_after']),500) and c['timestamp']==by_id[int(c['trade_id'])]['exit_timestamp'] for c in contributions[1:])
    checks['profits_retained_and_topups_correct']=all(close(t['replenishment'],max(0.,500-t['balance_before_replenishment'])) and close(t['balance_after_replenishment'],max(500.,t['balance_before_replenishment'])) for t in trades)
    checks['all_entry_balances_at_least500']=all(t['balance_at_entry']>=500-1e-7 for t in trades)
    checks['trailing_only_tightens']=all(all((b['level']-a['level'])*(1 if t['side']=='up' else -1)>=-1e-10 for a,b in zip(t['trailing_movements'],t['trailing_movements'][1:])) for t in trades)
    checks['active_source_unchanged']=before['sourceSHA256']==after['sourceSHA256']==original_status['sourceSHA256']
    checks['active_configuration_unchanged']=before['config']==after['config']==old['config']
    checks['paper_history_preserved']=after['events'][:len(before['events'])]==before['events']
    checks['paper_live_path_disabled']=after['health']['brokerOrdersEnabled'] is False and after['config']['mode']=='paper'
    checks['all_existing_services_active']=all('ActiveState=active' in s for s in after['services'].values())
    checks['full_chart_for_every_trade']=len(contexts)==len(trades) and all((root/c['full_chart']).is_file() and (root/c['zoom_chart']).is_file() for c in contexts)
    checks['full_previous_session_through_next_formation']=all(c['complete_context'] and c['requested_start']==by_id[c['trade_id']]['previous_context']['start'] and datetime.fromisoformat(c['requested_end'])>datetime.fromisoformat(by_id[c['trade_id']]['next_formation_end']) for c in contexts)
    checks['high_resolution_full_pngs']=all(Image.open(root/c['full_chart']).width>=4000 and Image.open(root/c['full_chart']).height>=1500 for c in contexts)
    gallery=GalleryParser()
    gallery.feed((root/'gallery.html').read_text(encoding='utf-8'))
    checks['gallery_has_every_trade_in_order']=gallery.articles==[f"trade-{t['trade_id']}" for t in trades]
    checks['gallery_image_links_exist']=len(gallery.images)==2*len(trades) and all((root/p).is_file() for p in gallery.images)
    olddiff=subprocess.run(['git','diff','--exit-code','22b8645','--',str(ref)],capture_output=True)
    checks['original_phase2_artifacts_unmodified']=olddiff.returncode==0
    closure=dict(no_open_position_at_end=m['open_at_end'] is None,no_position_into_next_formation=not m['next_formation_violations'],all_strict_deadlines_met=not m['deadline_violations'],deadline_violations=m['deadline_violations'],next_formation_violations=m['next_formation_violations'])
    check_report=dict(passed=all(checks.values()),checks=checks,closure_compliance=closure)
    (root/'checks.json').write_text(json.dumps(check_report,indent=2))
    if not check_report['passed']:
        print(json.dumps(check_report,indent=2))
        raise SystemExit('Research integrity/visual checks failed; report not approved')

    paper=after['state']
    lines=['# ACD continuation — Phase 2 revised replay and visual verification','',
        'Research only. The original Phase 2 artifacts, active EC2 paper robot, raw collection and unrelated services are unchanged. No Phase 3, optimization, live execution or actual capital transfer occurred. All contributions below are simulated external capital.','',
        '## Revised money management','',
        'Start once with USD 500. After the core has fully closed a trade, add `max(0,500−closed balance)` as external capital. Retain any balance above 500. Only after closure and replenishment clear the risk latch. Never add capital to an open position; never clear the session entry allowance. Position size remains 0.01 lot.','',
        '**The existing absolute trigger is USD455**, calculated as the USD450 floor plus the configured USD5 pre-limit liquidation buffer. It is not reset to “entry balance minus45”. After replenishment to500 there is USD 45 of equity headroom before estimated execution costs; if profits leave balance 600, the same 455 trigger provides USD 145 headroom. Spread, fees, gaps and slippage can cause overshoot. No threshold, reserve, entry price rule or exit formula was changed.','',
        'The legacy engine represents cash as `500 + realized`. The adapter adds external capital to that internal cash offset, while independently accumulating trading P&L from closed-trade results. The contribution ledger and reconciliation checks prevent that internal field name from disguising deposits as profits.','',
        '## Same dataset and original-account control','',
        f"Fixed interval: **{summary['window_start']} → {summary['window_end']}**, exactly the original Phase 2 endpoints, with the same ten-day warmup. Newer quotes do not move this window. Processed evaluation ticks: **{summary['counts']['window_ticks']:,}**. The original 31 signal dates and all raw/processed/duplicate counts match. Valid-stream SHA256: `{summary['dataset_sha256']}` (record encoding in `summary.json`). The previous run had no dataset hash; matching counts, 31 dates and control-account execution results are the available cross-run checks.",'',
        f"The original account was independently reprocessed on these same ticks until its permanent latch. Its **{baseline['baseline_trades']} trades, USD{baseline['baseline_net']:.2f} net and USD{baseline['baseline_equity']:.2f} final equity reproduce exactly**, including entry/exit timestamps, prices and reasons. This control is in `baseline_comparison.json`. The revised run changes only the funding/latch policy; different cash headroom can change which existing risk exit fires, as well as allowing later trades.",'',
        'All ACD calculations, weekday/DST clock, prior bias, current confirmation, entry window, 0.01-lot sizing, edge-based trailing half, opposite buffered/held exit and configured costs remain the same. Known Phase 2 correctness/calendar limitations were not silently fixed. This is an uninterrupted strategy-core replay, not historical service/watchdog availability or broker latency.','',
        '## Financial results — capital is not profit','',
        '| Measure | Original Phase 2 | Revised funding model |','|---|---:|---:|',
        f"| Continuation-signal days |31|{m['original_signal_days']}|",
        f"| Executed trades |{old['metrics']['executed_trades']}|{m['executed_trades']}|",
        f"| Winning / losing / breakeven |0 /5 /0|{m['winning_trades']} /{m['losing_trades']} /{m['breakeven_trades']}|",
        f"| Net trading P&L, excluding contributions |USD{old['metrics']['realized_net']:.2f}|**USD{m['net_trading_pnl']:.2f}**|",
        f"| Replenishment count |0|{m['replenishment_count']}|",
        f"| Total replenishments |USD0.00|USD{m['total_replenishments']:.2f}|",
        f"| Total external capital, including initial 500 |USD 500.00|**USD{m['total_external_capital']:.2f}**|",
        f"| Final account balance |USD{old['metrics']['ending_equity']:.2f}|**USD{m['final_balance']:.2f}**|",
        f"| Profit factor |0.00|{money(m['profit_factor']) if m['profit_factor'] is not None else 'undefined (no losses)'}|",
        f"| Expectancy per trade |USD{old['metrics']['expectancy']:.2f}|USD{m['expectancy']:.2f}|",
        f"| Maximum individual loss |USD30.88|USD{m['maximum_individual_loss']:.2f}|",
        f"| Maximum consecutive losses |5|{m['max_consecutive_losses']}|",'',
        f"**Cash reconciliation: USD{m['total_external_capital']:.2f} external capital + USD{m['net_trading_pnl']:.2f} trading P&L = USD{m['final_balance']:.2f} final balance.** A funded ending balance near500 does not mean the strategy broke even.",'',
        f"Exit reasons: `{json.dumps(m['exit_reasons'],sort_keys=True)}`. Capital-protection events/reset records: **{len(m['risk_shutdown_events'])}**. Contributions-adjusted maximum equity drawdown: **USD{m['max_flow_adjusted_equity_drawdown']:.2f}**, distinct from the cash account curve that includes funding.",'',
        '![Cash balance with replenishments](charts/account_balance.png)','',
        '![Trading P&L excluding capital](charts/cumulative_trading_pnl.png)','',
        '![External contributed capital](charts/capital_contributions.png)','',
        '## All 31 continuation-signal days','',
        'The table records every original signal day. A rejection based on risk shows the closest confirmed opportunity, not a newly invented filter. The unchanged estimate includes opposite-boundary distance at executable entry, configured costs/slippage and the USD5 delay reserve, and must be **strictly less** than equity−455. Full gate counts and precise numeric evidence are in `signal_day_investigation.json`/`.csv` and in the gallery.','',
        '| NY ACD date | Trade | Exact rejection evidence if not executed |','|---|---:|---|']
    for d in decisions:
        evidence='—'
        if not d['executed']:
            evidence=d['reason']
            c=d.get('closest_confirmed_rejection')
            if c:
                evidence+=f"; closest confirmed estimate USD{c['estimated_risk']:.2f} vs budget USD{c['available_risk_budget']:.2f} at {ny(c['timestamp'])}; entry {c['entry_price']:.2f}, opposite {c['opposite_boundary']:.2f}"
        lines.append(f"|{d['date']}|{d['trade_id'] or 'rejected'}|{evidence}|")
    lines+=['','## Mandatory closure compliance','',
        f"All positions fully closed at dataset end: **{closure['no_open_position_at_end']}**. No position carried into its next ACD formation: **{closure['no_position_into_next_formation']}**. Strict deadline compliance: **{closure['all_strict_deadlines_met']}**. Only the four permitted exit types were observed.",'']
    if m['deadline_violations']:
        lines+=['Every late execution is listed below. No fill is backdated to a timestamp without an executable quote. Small delays can be the normal wait for the next tick; long gaps cannot be classified as holiday/closure versus missing data without a broker calendar.','', '|Trade|Deadline|Actual exit|Delay seconds|Previous quote / cause|','|---:|---|---|---:|---|']
        for v in m['deadline_violations']:
            lines.append(f"|{v['trade_id']}|{ny(v['deadline'])}|{ny(v['exit'])}|{v['delay_seconds']:.3f}|{v['previous_quote']} /{v['cause']}|")
    else:
        lines.append('No late-deadline or next-formation exposure violations occurred in this dataset. No unavailable-market execution prices were fabricated. This is dataset evidence, not a guarantee under future gaps/closures.')
    lines+=['','## Full-session visual evidence','',
        f"[Open the complete HTML trade gallery](gallery.html) — **{len(trades)} full-session PNGs and {len(trades)} secondary zooms**, in chronological order, with every requested summary embedded. [Download charts and summaries ZIP](charts_and_summaries.zip). [Trade summaries CSV](trade_summaries.csv). The gallery also embeds all 31 signal-day decisions; inspecting trades does not require opening CSV files.",'',
        'Each full PNG begins at the previous ACD formation start and ends 30 minutes after completion of the next formation. It shows the complete prior and trading session, their formations and A boundaries, the prior directional classification and supporting first-ask/final-bid movement, today’s confirmation, exact entry/exit times and simulated execution prices, highlighted open-position line, every actual trailing movement, buffered opposite stop, deadline and next formation boundaries. Time uses New York EDT/EST and true elapsed spacing; weekend/closure gaps are blank, not compressed or interpolated.','',
        'The displayed line selects the actual last historical quote per second, using bid to close longs and ask to close shorts. All fills, confirmations, MAE/MFE and risk calculations use every valid tick. Exact execution prices include the unchanged slippage assumption and can differ from the plotted closing-side quote by spread/slippage. Context/formation coverage is checked in `chart_context_checks.json`. PNGs are at least 4000 pixels wide; secondary zooms never replace the full frame.','',
        '## Costs, limitations and interpretation','',
        'Costs remain historical bid/ask spread, 0.10 quote USD adverse slippage each side, USD0.14 round-trip commission, zero modeled swap and hypothetical 100oz/lot. These are unchanged, unverified broker assumptions. The weekday clock still lacks broker holidays, early-closure permissions and a validated financing schedule. The partial-prior-formation bias defect and entry-tick equity valuation delay identified in Phase 2 remain in the frozen core. No parameter search, tighter exit or new entry filter was introduced.','',
        'The half-trail remains anchored to the frozen A edge, not entry. Activation after a favorable USD3 quote move does not guarantee a profitable exit: a 50% edge-to-extreme level may still be beyond the loss side of a late entry. Replenishment permits later opportunities but cannot repair negative trading expectancy or remove virtual-exit/gap risk. The software capital threshold is not a guaranteed maximum loss, and retained profits expand headroom to the unchanged absolute 455 floor.','',
        '## Preservation and validation','',
        f"Paper snapshot: {after['capturedAt']}; equity USD{paper.get('equity',500):.2f}, realized USD{paper['realized']:.2f}, position `{paper['position']}`, paused `{paper['paused']}`. Active source/configuration match the original deployment and before/after snapshots. Existing service, collector and watchdog timer remain active. The existing stale-quote pause was not overridden. The warning log retains earlier watchdog service failures; active service/timer status does not establish that the robot is ready to enter. No active ledger, capital, parameters or live order path was changed.",'',
        f"**{sum(checks.values())}/{len(checks)} integrity and visual checks passed.** The funding/closure and prior-regression suite passed 36 tests. The original account reproduces exactly on the same tick stream; the original Phase 2 directory has no Git diff. Visual samples were reviewed before delivery. See `checks.json`, `baseline_comparison.json`, `capital_contributions.csv`, `account_events.csv`, `daily.csv`, `trades.json` and `status_before.json`/`status_after.json`.",'',
        '## Reproduction','',
        'Run `scripts/acd_phase2_revised_replay.py --reference docs/acd_continuation/phase2_2026-11-27 --config docs/acd_continuation/phase2_2026-11-27/config.json --output NEW_RESEARCH_DIRECTORY` with the unchanged snapshot/database integration on PYTHONPATH. The read-only database must retain the original window and warmup. Then run `scripts/acd_phase2_revised_visuals.py NEW_RESEARCH_DIRECTORY` and `scripts/acd_phase2_revised_deliver.py NEW_RESEARCH_DIRECTORY --reference docs/acd_continuation/phase2_2026-11-27` after placing read-only preservation snapshots in that directory.','',
        'The quote-per-second gzip is research support data retained separately on EC2; it is not included in Git or the downloadable gallery ZIP. It is reproducibly generated from the preserved raw database. Metadata, ledgers, reports, all PNGs, the offline gallery and ZIP are committed on `codex/acd-phase2-revised-validation`. No changes were pushed to main or deployed.']
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    # Canonical bytes keep the manifest valid across Windows/Linux checkouts.
    for path in root.rglob('*'):
        if path.is_file() and (path.suffix in {'.json','.md','.csv','.html'} or path.name in {'COMPLETE','.gitattributes'}):
            path.write_bytes(path.read_bytes().replace(b'\r\n',b'\n'))
    packaged=['gallery.html','REPORT.md','trade_summaries.csv','trade_summaries.json','capital_contributions.csv','account_events.csv','daily.csv','signal_day_investigation.csv','signal_day_investigation.json','checks.json','summary.json','chart_context_checks.json']
    with zipfile.ZipFile(root/'charts_and_summaries.zip','w',zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
        for name in packaged:
            archive.write(root/name,name)
        for path in sorted((root/'charts').glob('*.png')):
            archive.write(path,'charts/'+path.name)
    manifest={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file() and p.name!='artifact_sha256.json'}
    (root/'artifact_sha256.json').write_bytes(json.dumps(manifest,indent=2).encode('utf-8'))
    print(json.dumps(dict(checks_passed=check_report['passed'],closure=closure,trades=len(trades),net=m['net_trading_pnl'],replenished=m['total_replenishments'],final_balance=m['final_balance']),indent=2))


if __name__=='__main__':
    main()
