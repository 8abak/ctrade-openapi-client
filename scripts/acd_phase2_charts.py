"""Render research charts and representative examples; no strategy changes."""
import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    args = parser.parse_args()
    root = Path(args.directory)
    charts = root/'charts'
    charts.mkdir(exist_ok=True)
    summary = json.loads((root/'summary.json').read_text())
    trades = json.loads((root/'trades.json').read_text())
    days = json.loads((root/'daily.json').read_text())
    with (root/'equity_and_quotes.csv').open(newline='') as stream:
        samples = list(csv.DictReader(stream))
    times = [datetime.fromisoformat(r['timestamp']) for r in samples]
    equity = [float(r['equity']) for r in samples]
    drawdown = [float(r['drawdown']) for r in samples]
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.facecolor':'white','axes.grid':True,'grid.alpha':.18})

    def finish(fig,path):
        fig.tight_layout()
        fig.savefig(charts/path,dpi=160,bbox_inches='tight')
        plt.close(fig)

    def quote_line(ax,rows,key,**kwargs):
        x,y = [],[]
        previous = None
        for row in rows:
            stamp = datetime.fromisoformat(row['timestamp'])
            if previous and (stamp-previous).total_seconds()>120:
                # Display missing/closed intervals as blank, never as an
                # interpolated executable price path across a weekend.
                x.append(stamp)
                y.append(float('nan'))
            x.append(stamp)
            y.append(float(row[key]))
            previous = stamp
        ax.plot(x,y,**kwargs)

    fig,ax = plt.subplots(figsize=(11,4))
    ax.plot(times,equity,color='#1565c0',lw=1.4)
    ax.axhline(500,color='#777',ls=':',label='USD 500 baseline')
    ax.axhline(455,color='#c62828',ls='--',label='Configured liquidation threshold USD 455')
    for event in summary['metrics']['risk_shutdown_events']:
        ax.axvline(datetime.fromisoformat(event['timestamp']),color='#c62828',ls=':',label='Permanent risk shutdown')
    ax.set(title='Primary account equity | exact deployed strategy',ylabel='USD')
    ax.legend(fontsize=8,loc='best')
    finish(fig,'equity.png')

    fig,ax = plt.subplots(figsize=(11,3.5))
    ax.fill_between(times,drawdown,color='#c62828',alpha=.28)
    ax.plot(times,drawdown,color='#c62828',lw=1)
    ax.set(title=f"Equity drawdown | tick-level maximum USD {summary['metrics']['max_equity_drawdown']:.2f}",ylabel='USD below running peak')
    finish(fig,'drawdown.png')

    fig,ax = plt.subplots(figsize=(11,3.5))
    dates = [datetime.fromisoformat(d['date']) for d in days]
    pnl = [d['net_pnl'] for d in days]
    ax.bar(dates,pnl,color=['#15803d' if p>=0 else '#b91c1c' for p in pnl],width=.8)
    ax.axhline(0,color='#777',lw=.7)
    ax.set(title='Daily realized net P&L | New York ACD session date',ylabel='USD')
    finish(fig,'daily_pnl.png')

    months = summary['metrics']['monthly']
    fig,ax = plt.subplots(figsize=(8,3.5))
    values = [m['net_pnl'] for m in months.values()]
    ax.bar(list(months),values,color=['#15803d' if p>=0 else '#b91c1c' for p in values],width=.55)
    ax.axhline(0,color='#777',lw=.7)
    ax.set(title='Monthly realized net P&L | partial boundary months',ylabel='USD')
    finish(fig,'monthly.png')

    categories = {
        'successful_continuation':lambda t:(t['net_pnl'] or 0)>0,
        'retracement_and_recovery':lambda t:t['returned_inside_acd'] and t['recovered_after_retracement'],
        'opposite_boundary_invalidation':lambda t:t.get('exit_reason')=='opposite',
        'opposite_boundary_breach':lambda t:t['opposite_boundary_broken'],
        'trailing_stop_exit':lambda t:t.get('exit_reason')=='trailing',
        'risk_shutdown_exit':lambda t:t.get('exit_reason')=='capital',
    }
    # Annotate every primary trade, not just the representative category picks.
    categories.update({f"trade_{t['trade_id']:03}":(lambda candidate,ident=t['trade_id']:candidate['trade_id']==ident) for t in trades})
    examples = {}
    charted = {}
    ny = ZoneInfo('America/New_York')
    for category,predicate in categories.items():
        trade = next((t for t in trades if predicate(t)),None)
        if trade is None:
            examples[category] = dict(available=False,reason='No matching trade in the primary unchanged-risk account; no synthetic or post-shutdown trade substituted')
            continue
        number = trade['trade_id']
        name = f'trade_{number:03}.png'
        examples[category] = dict(available=True,trade_id=number,date=trade['date'],chart='charts/'+name)
        if number in charted:
            continue
        charted[number] = True
        entry = datetime.fromisoformat(trade['entry_timestamp'])
        end = datetime.fromisoformat(trade['exit_timestamp']) if trade['exit_timestamp'] else times[-1]
        subset = [r for r in samples if r['session']==trade['date'] and entry-timedelta(minutes=60) <= datetime.fromisoformat(r['timestamp']) <= end+timedelta(minutes=30)]
        fig,ax = plt.subplots(figsize=(12,5))
        x = [datetime.fromisoformat(r['timestamp']) for r in subset]
        key = 'bid' if trade['side']=='up' else 'ask'
        quote_line(ax,subset,key,color='#334155',lw=1,label=f'Executable closing {key} (minute display)')
        ax.axhspan(trade['lower'],trade['upper'],color='#dbeafe',alpha=.5,label='Inside A-up/A-down boundaries')
        ax.axhline(trade['upper'],color='#2563eb',ls='--',lw=1,label='A-up / A-down')
        ax.axhline(trade['lower'],color='#2563eb',ls='--',lw=1)
        ax.axhline(trade['opposite_exit'],color='#dc2626',ls=':',label='Buffered opposite exit boundary')
        ax.scatter([entry],[trade['entry_price']],color='#15803d',marker='^',s=70,zorder=5,label='Entry including slippage')
        if trade['exit_timestamp']:
            ax.scatter([end],[trade['exit_price']],color='#b91c1c',marker='X',s=65,zorder=5,label='Exit: '+trade['exit_reason'])
        for stamp_key,price_key,label,color in (
            ('first_retracement_timestamp','first_retracement_price','First return inside','#d97706'),
            ('first_recovery_timestamp','first_recovery_price','Recovery beyond entry edge','#7c3aed'),
            ('mae_timestamp','mae_price','Tick-level MAE','#dc2626'),
            ('mfe_timestamp','mfe_price','Tick-level MFE','#0891b2')):
            if trade.get(stamp_key):
                stamp = datetime.fromisoformat(trade[stamp_key])
                ax.scatter([stamp],[trade[price_key]],s=28,color=color,zorder=6,label=label)
        moves = trade['trailing_movements']
        if moves:
            tx = [datetime.fromisoformat(m['timestamp']) for m in moves]+[end]
            ty = [m['level'] for m in moves]+[moves[-1]['level']]
            ax.step(tx,ty,where='post',lw=1.2,color='#9333ea',label='Virtual trail (every actual movement)')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M',tz=ny))
        net = trade.get('net_pnl')
        ax.set(title=f"Primary trade {number} | {trade['date']} | {'LONG' if trade['side']=='up' else 'SHORT'} | net USD {net:.2f}" if net is not None else f'Primary trade {number} | open',xlabel='New York time (DST aware)',ylabel='XAUUSD quote')
        ax.legend(fontsize=8,ncol=2,loc='best')
        finish(fig,name)

    rejected = []
    for reason in ('yesterday_uncertain','incomplete_formation_data','risk_estimate_exceeds_budget','confirmation_incomplete','no_aligned_breakout','risk_shutdown_latched'):
        day = next((d for d in days if not d['trade_executed'] and d['no_trade_reason']==reason and d['upper'] is not None),None)
        if day is None:
            continue
        subset = [r for r in samples if r['session']==day['date']]
        if not subset:
            continue
        fig,ax = plt.subplots(figsize=(11,4))
        x = [datetime.fromisoformat(r['timestamp']) for r in subset]
        quote_line(ax,subset,'bid',lw=.9,color='#334155',label='Bid (minute display)')
        quote_line(ax,subset,'ask',lw=.7,color='#94a3b8',label='Ask')
        ax.axhspan(day['lower'],day['upper'],color='#dbeafe',alpha=.5)
        ax.axhline(day['upper'],color='#2563eb',ls='--',label='A-up / A-down')
        ax.axhline(day['lower'],color='#2563eb',ls='--')
        opening_end = datetime.fromisoformat(day['session_start'])+timedelta(minutes=15)
        cutoff = min(opening_end+timedelta(minutes=summary['config']['entry_minutes']),datetime.fromisoformat(day['session_end'])-timedelta(hours=summary['config']['late_hours']))
        ax.axvspan(opening_end,cutoff,color='#bbf7d0',alpha=.22,label='Existing entry window')
        if day['decision_timestamp']:
            ax.axvline(datetime.fromisoformat(day['decision_timestamp']),color='#d97706',ls=':',label='Recorded causal decision observation')
        ax.set(title=f"No trade: {day['date']} | {reason} | prior bias: {day['yesterday_bias']}",xlabel='New York time',ylabel='XAUUSD quote')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M',tz=ny))
        ax.legend(fontsize=8,ncol=2)
        name = f"rejected_{day['date']}.png"
        finish(fig,name)
        rejected.append(dict(date=day['date'],reason=reason,chart='charts/'+name))
    examples['rejected_days'] = rejected
    (root/'examples.json').write_text(json.dumps(examples,indent=2))
    print(json.dumps(examples,indent=2))


if __name__=='__main__':
    main()
