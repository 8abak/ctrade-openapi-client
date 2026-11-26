"""Validate completed research artifacts and preservation evidence."""
import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    args = parser.parse_args()
    root = Path(args.directory)
    summary = json.loads((root/'summary.json').read_text())
    trades = json.loads((root/'trades.json').read_text())
    daily = json.loads((root/'daily.json').read_text())
    before = json.loads((root/'status_before.json').read_text())
    after = json.loads((root/'status_after.json').read_text())
    counts = Counter(t['date'] for t in trades)
    checks = {}
    checks['maximum_one_entry_per_acd_day'] = all(v<=1 for v in counts.values())
    checks['daily_counts_match_trade_history'] = all(d['trade_count']==counts[d['date']] for d in daily)
    checks['summary_trade_count_matches'] = len(trades)==summary['metrics']['executed_trades']
    checks['net_pnl_reconciles'] = abs(sum((t.get('net_pnl') or 0) for t in trades)-summary['metrics']['realized_net'])<1e-7
    checks['daily_realized_pnl_reconciles'] = abs(sum(d['net_pnl'] for d in daily)-summary['metrics']['realized_net'])<1e-7
    checks['fixed_position_size'] = all(t['lots']==.01 for t in trades)
    checks['entry_direction_matches_yesterday'] = all(t['side']==t['yesterday_bias']==t['today_direction'] for t in trades)
    checks['causal_decision_is_entry_timestamp'] = all(t['decision_timestamp']==t['entry_timestamp'] for t in trades)
    checks['no_entry_after_loss_shutdown'] = all(datetime.fromisoformat(t['entry_timestamp'])<=datetime.fromisoformat(event['timestamp']) for event in summary['metrics']['risk_shutdown_events'] for t in trades)
    checks['maximum_one_permanent_latch'] = len(summary['metrics']['risk_shutdown_events'])<=1
    checks['entry_and_exit_chronological'] = all(t['exit_timestamp'] is None or datetime.fromisoformat(t['exit_timestamp'])>=datetime.fromisoformat(t['entry_timestamp']) for t in trades)
    checks['trailing_only_tightens'] = all(all((b['level']-a['level'])*(1 if t['side']=='up' else -1)>=-1e-10 for a,b in zip(t['trailing_movements'],t['trailing_movements'][1:])) for t in trades)
    checks['active_source_unchanged'] = before['sourceSHA256']==after['sourceSHA256']
    checks['active_configuration_unchanged'] = before['config']==after['config']
    checks['paper_history_preserved'] = after['events'][:len(before['events'])]==before['events']
    checks['live_execution_disabled'] = before['health']['brokerOrdersEnabled'] is False and after['health']['brokerOrdersEnabled'] is False and before['config']['mode']==after['config']['mode']=='paper'
    checks['unrelated_services_still_active'] = all('ActiveState=active' in after['services'][name] for name in ['datavis.service','tickcollector.service'])
    checks['paper_service_and_timer_still_active'] = all('ActiveState=active' in after['services'][name] for name in ['acd-paper.service','acd-paper-watchdog.timer'])
    checks['frozen_source_hashes_match_deployed'] = all(hashlib.sha256((root/'frozen_source'/p).read_bytes()).hexdigest()==before['sourceSHA256'][p] for p in ['datavis/acd_continuation.py','datavis/acd_paper_service.py','datavis/hlrtacd/acd.py','datavis/hlrtacd/models.py'])
    with (root/'equity_and_quotes.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    checks['chart_samples_chronological'] = all(datetime.fromisoformat(b['timestamp'])>=datetime.fromisoformat(a['timestamp']) for a,b in zip(rows,rows[1:]))
    checks['sampled_drawdown_not_above_tick_maximum'] = max((float(r['drawdown']) for r in rows),default=0)<=summary['metrics']['max_equity_drawdown']+1e-8
    for event in summary['metrics']['risk_shutdown_events']:
        later = [float(r['equity']) for r in rows if datetime.fromisoformat(r['timestamp'])>datetime.fromisoformat(event['timestamp'])]
        checks['equity_flat_after_shutdown'] = all(abs(v-summary['metrics']['ending_equity'])<1e-7 for v in later)
    report = dict(passed=all(checks.values()),checks=checks)
    (root/'checks.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
    if not report['passed']:
        raise SystemExit(1)


if __name__=='__main__':
    main()
