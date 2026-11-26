"""Assemble the measured Phase 2 report from completed research artifacts."""
import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def amount(value):
    return 'n/a' if value is None else f'{value:,.2f}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    args = parser.parse_args()
    root = Path(args.directory)
    summary = json.loads((root/'summary.json').read_text())
    m = summary['metrics']
    trades = json.loads((root/'trades.json').read_text())
    days = json.loads((root/'daily.json').read_text())
    examples = json.loads((root/'examples.json').read_text())
    checks = json.loads((root/'checks.json').read_text())
    status = json.loads((root/'status_after.json').read_text())
    state = status['state']
    when = datetime.fromisoformat(status['capturedAt']).astimezone(ZoneInfo('Australia/Sydney')).isoformat()
    start = datetime.fromisoformat(summary['window_start']).astimezone(ZoneInfo('Australia/Sydney')).isoformat()
    end = datetime.fromisoformat(summary['window_end']).astimezone(ZoneInfo('Australia/Sydney')).isoformat()
    lines = [
        '# ACD continuation robot — Phase 2 validation',
        '',
        '27 November 2026. Research only; the active EC2 robot, configuration, equity, ledger, raw tick collection and unrelated services were preserved. No live execution was enabled and no strategy optimization was performed.',
        '',
        '## Current paper robot',
        '',
        f"Snapshot: **{when}** (Sydney). Paper equity **USD {amount(state.get('equity'))}**, realized P&L **USD {amount(state.get('realized'))}**; **{len(status['events'])} ledger events**, **{sum(e['kind']=='ENTRY' for e in status['events'])} entries**; position **{'none' if state['position'] is None else json.dumps(state['position'])}**; paused **{state['paused']}**, risk latch **{state['latched']}**.",
        '',
        f"Last accepted quote: `{state['last']}`. Pending control: **{status['health']['controlPending']}**. The primary service, watchdog timer, raw collector and existing web service are running. The watchdog is logging stale-feed errors and has queued a close/pause; there is no exposure to liquidate. Calendar closure versus feed outage is unverified. The pause was not overridden.",
        '',
        'Isolation is verified by unchanged source hashes, paper-only configuration validation, the unit command and the call graph: the service imports the database reader and paper engine, with no broker order or authentication path. `status_before.json` and `status_after.json` preserve read-only evidence. No active restart/control/config write was performed in Phase 2.',
        '',
        '## Strategy compliance',
        '',
        'The implementation enforces one entry per New York ACD session, matches a causal confirmed A breakout to the frozen previous-session bias, permits range retracement and does not invalidate solely on re-entry. Low frequency and floating drawdown were preserved. Existing trailing, capital and timed exits remain independent overrides.',
        '',
        '**Opposite-edge ambiguity:** the deployed exit requires a 0.25 quote buffer plus a 450-second hold. It does not invalidate on the first boundary break. This matches the original break-and-hold mandate but may conflict with an immediate-break interpretation of the latest clarification. No active change was made. The owner has not yet accepted the candidate numeric bias/edge/confirmation definitions.',
        '',
        'Full recovered rules, causal proof, discrepancies and unresolved definitions: [STRATEGY_AUDIT.md](STRATEGY_AUDIT.md).',
        '',
        '## Primary historical account',
        '',
        f"Window: **{start} to {end}** (Sydney display) — three calendar months anchored in UTC, ending at the latest available quote when the run began. Ten preceding days were used for no-entry warmup. **{summary['counts'].get('window_ticks',0):,} full-resolution valid ticks** were processed in the window; **{summary['counts'].get('duplicate_timestamp_quotes_skipped',0):,} equal-timestamp quotes** were skipped over the full warmup+evaluation stream, following deployed semantics; **{summary['counts'].get('invalid_quotes',0)} invalid quotes**. Execution, excursions and maximum drawdown use full ticks; only chart rendering is downsampled.",
        '',
        'The primary starts once at USD 500, using the unchanged 0.01 lot sizing, ACD, entry/exit/trailing and risk rules. The actual configured pre-limit liquidation threshold is USD 455 (USD 450 floor plus USD 5 buffer). The loss latch is never reset during evaluation. Later sessions are signal diagnostics only, with no hypothetical account trades or P&L.',
        '',
        '**Scope:** this is an uninterrupted replay of the deployed strategy core, not a replay of service availability, watchdog control messages, restarts or execution latency. Core quote-gap timer resets are included. The active watchdog can pause trading through normal closures, so these historical trades must not be presented as trades the currently paused service would necessarily have taken.',
        '',
        '| Measure | Primary result |',
        '|---|---:|',
    ]
    measures = [
        ('Scheduled weekday sessions',m['scheduled_weekdays']),
        ('Observed quote sessions / trading days',m['observed_trading_days']),
        ('Weekdays without quotes',m['missing_weekdays']),
        ('Directional continuation days (including post-shutdown diagnostics)',m['directional_continuation_days']),
        ('Eligible signal days before account/risk gates',m['eligible_before_risk_days']),
        ('Executable eligible account days',m['eligible_executable_days']),
        ('Executed / closed trades',f"{m['executed_trades']} / {m['closed_trades']}"),
        ('Winning / losing / breakeven trades',f"{m['winning_trades']} / {m['losing_trades']} / {m['breakeven_trades']}"),
        ('Realized net P&L (configured costs)',f"USD {amount(m['realized_net'])}"),
        ('Ending marked equity',f"USD {amount(m['ending_equity'])}"),
        ('Net account change',f"USD {amount(m['mark_to_market_net'])}"),
        ('Win rate',f"{100*m['win_rate']:.2f}%" if m['win_rate'] is not None else 'n/a'),
        ('Profit factor',amount(m['profit_factor']) if m['profit_factor'] is not None else m['profit_factor_note']),
        ('Expectancy per closed trade',f"USD {amount(m['expectancy'])}"),
        ('Maximum tick-level equity drawdown',f"USD {amount(m['max_equity_drawdown'])}"),
        ('Maximum consecutive losses',m['max_consecutive_losses']),
        ('Average holding duration',f"{m['average_holding_seconds']/3600:.2f} hours" if m['average_holding_seconds'] is not None else 'n/a'),
        ('Maximum holding duration',f"{m['max_holding_seconds']/3600:.2f} hours" if m['max_holding_seconds'] is not None else 'n/a'),
        ('One-trade-per-day violations',len(m['day_trade_limit_violations'])),
    ]
    lines.extend(f'| {label} | {value} |' for label,value in measures)
    lines += ['', '### Loss shutdown', '']
    if m['risk_shutdown_events']:
        for event in m['risk_shutdown_events']:
            lines.append(f"- Permanent latch: **{event['timestamp']}**, post-exit equity **USD {amount(event['equity'])}**, configured trigger **USD {amount(event['threshold'])}**. No later account entry was allowed.")
        lines += ['', '**This early-terminated trading account is the primary result.** The remaining historical ticks are retained for daily signal diagnostics, not for a reset or unconstrained performance curve.']
    else:
        lines.append('No permanent risk shutdown occurred in the primary historical account.')
    lines += ['', '### Long and short performance', '', '| Side | Closed trades | Wins | Net USD |', '|---|---:|---:|---:|']
    for side,key in [('Long','up'),('Short','down')]:
        row = m['by_side'][key]
        lines.append(f"| {side} | {row['trades']} | {row['wins']} | {amount(row['net'])} |")
    lines += ['', '### Monthly realized performance', '', 'Boundary months are partial. Net P&L is assigned to the exit ACD session; ending equity also includes any floating P&L.', '', '| Month | Observed days | Entries | Net USD | Ending equity USD |','|---|---:|---:|---:|---:|']
    for month,row in m['monthly'].items():
        lines.append(f"| {month} | {row['trading_days']} | {row['trades']} | {amount(row['net_pnl'])} | {amount(row['ending_equity'])} |")
    lines += ['', '### Retracement, invalidation and trailing', '',
        f"Of {m['executed_trades']} primary trades, **{m['retracement_trades']} returned inside A-down/A-up**, **{m['retracement_recovery_trades']} subsequently recovered beyond their entry edge**, and **{m['profitable_retracement_trades']} retracement trades closed profitably**. Recovery is a price event, not a guaranteed win.", '',
        f"**{m['opposite_boundary_broken_trades']}** trades crossed the raw opposite A edge; **{m['buffered_opposite_broken_trades']}** crossed the buffered edge. These are separate from a held structural invalidation. Exit reasons: `{json.dumps(m['exit_reasons'],sort_keys=True)}`. Every actual trail movement is in `trailing_movements.csv`.", '',
        f"Trailing activation does not guarantee a profitable exit under the specified mathematics. The frozen A edge can be well beyond the actual entry after the 450-second confirmation; halfway from that edge to the favorable extreme can still be on the loss side of entry. **{sum(t.get('exit_reason')=='trailing' and (t.get('net_pnl') or 0)<0 for t in trades)} of {m['exit_reasons'].get('trailing',0)} trailing exits were net losses**, with **{sum(len(t['trailing_movements']) for t in trades)} recorded trail updates**. This is a consequence of the existing edge-based formula, not a tighter stop introduced by Phase 2. Retracement recovery means recrossing the breakout edge, not necessarily regaining the entry price or recovering account equity.", '',
        '## Costs, data and calendar limitations', '',
        f"Actual historical bid/ask spread is included (mean quote spread **{amount(summary['spread']['mean'])}**, maximum **{amount(summary['spread']['max'])}**). Fixed adverse slippage is 0.10 quote USD per side and commission is USD 0.14 round trip at a hypothetical 100 oz/lot. Zero modeled swap is retained from the active configuration. **Broker contract, financing and fee schedules are not verified; these are configured paper net results, not proven realistic all-in broker results.** No parameters or cost scenarios were optimized.", '',
        f"The weekday clock yields **{sum(d['partial_window'] for d in days)} partial boundary sessions** and **{sum(d['incomplete_session'] for d in days)} sessions with incomplete end/window coverage**. `data_gaps.csv` records all gaps above 60 seconds. Holidays, early closures and missing feed data cannot be distinguished reliably from this database alone. `daily.csv` exposes calendar versus preceding observed session labels; the unchanged strategy does not correctly resolve all broker-calendar cases.", '',
        'The observed primary result provides no positive economic justification for this candidate. It also cannot establish the intended strategy\'s profitability because definitions and implementation gaps remain. Further development should first resolve correctness, calendar and cost validation, then repeat validation without optimization; these results do not justify live execution.', '',
        '## Visual evidence', '',
        '![Primary account equity](charts/equity.png)', '',
        '![Drawdown](charts/drawdown.png)', '',
        '![Daily realized P&L](charts/daily_pnl.png)', '',
        '![Monthly realized performance](charts/monthly.png)', '',
        '## Representative examples', '',
    ]
    for category in ('successful_continuation','retracement_and_recovery','opposite_boundary_invalidation','opposite_boundary_breach','trailing_stop_exit','risk_shutdown_exit'):
        e = examples[category]
        label = category.replace('_',' ').capitalize()
        if e['available']:
            lines += [f"### {label}: trade {e['trade_id']} ({e['date']})", '',f"![{label}]({e['chart']})",'']
        else:
            lines += [f"**{label}:** not observed in the primary account. No fabricated or post-shutdown trade is substituted.",'']
    for e in examples['rejected_days']:
        lines += [f"### Code-consistent rejection: {e['date']}",'',f"Reason: `{e['reason']}`. This is consistent with the deployed candidate; unresolved owner definitions are not thereby validated.",'',f"![Rejected day]({e['chart']})",'']
    lines += [
        '## Defects, unresolved definitions and risk', '',
        'Reproduced defects: partial prior formation can produce a definite bias, and entry-tick equity omits immediate costs until the next quote. Calendar gaps affect prior-session identity and timed exits; closure-triggered pauses do not resume automatically. Opposite-break immediacy, the selected ACD variant, prior-move definition, trailing reference, entry window and trading-day meaning remain unresolved. See the full audit and regression fixtures. The active robot was not patched.', '',
        '**USD 50 is not a guaranteed maximum loss.** There is no broker stop, verified isolated live allocation, broker execution recovery or off-host liquidation watchdog. Gaps, unavailable quotes and execution failure can exceed the software threshold. No live activation was attempted.', '',
        '## Artifacts, tests and Git', '',
        '- `summary.json`: exact configuration, window, counts, tick-level statistics and source hash.',
        '- `daily.csv` / `daily.json`: every scheduled weekday, causal decision observation, ACD levels, gate reasons, entries, excursions, retracement, invalidation, exits and closing equity.',
        '- `trades.csv` / `trades.json`: full primary trade history, charges and exact excursion/retracement timestamps.',
        '- `trailing_movements.csv`: each trailing update; `equity_and_quotes.csv`: chart samples; `data_gaps.csv`: unresolved quote gaps.',
        '- `charts/` and `examples.json`: every primary trade chart plus representative rejected days and performance plots.',
        '- `status_before.json` / `status_after.json`: read-only forward-operation evidence; `frozen_source/`: unchanged calculation snapshot.',
        '- [REPRODUCE.md](REPRODUCE.md): isolated rerun commands and field definitions; [STRATEGY_AUDIT.md](STRATEGY_AUDIT.md): compliance and unresolved requirements.',
        '',
        f"Validation: `python -m pytest test_acd_phase2_replay.py test_acd_continuation.py test_hlrtacd_acd.py -q` — **30 passed** in the working environment. **{sum(checks['checks'].values())}/{len(checks['checks'])} artifact/preservation checks passed**, including trade counts, P&L, trailing, shutdown and unchanged active configuration/source. The committed Phase 2-specific suite is independently runnable from the frozen snapshot. Syntax compilation of all research scripts passed. Charts were rendered and visually inspected.",
        '',
        'Research branch: `codex/acd-phase2-validation`. The commit hash is returned with delivery; this report is part of that commit. Only research artifacts/scripts/tests were staged. No push to `main`, deployment or active parameter change was made.',
    ]
    (root/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(root/'REPORT.md')


if __name__=='__main__':
    main()
