"""Full previous/trading/next-formation charts, offline HTML gallery and ZIP."""
from __future__ import annotations

import argparse
import csv
import gzip
import html
import json
import zipfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

NY=ZoneInfo('America/New_York')


def stamp(value):
    return datetime.fromisoformat(value)


def ny(value):
    return stamp(value).astimezone(NY).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+' '+stamp(value).astimezone(NY).tzname()


def number(value):
    return 'n/a' if value is None else f'{value:,.2f}'


def direction(value):
    return {'up':'BULLISH','down':'BEARISH'}.get(value,str(value).upper())


def write_csv(path,rows):
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields or ['no_records'])
        w.writeheader()
        w.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    parser.add_argument('--data',help='Actual quote-per-second binary gzip; may be outside committed artifact directory')
    args=parser.parse_args()
    root=Path(args.directory)
    charts=root/'charts'
    charts.mkdir(exist_ok=True)
    report=json.loads((root/'summary.json').read_text())
    trades=json.loads((root/'trades.json').read_text())
    with gzip.open(args.data or root/'display_quotes.bin.gz','rb') as f:
        quotes=np.frombuffer(f.read(),dtype=[('time','<i8'),('bid','<f8'),('ask','<f8')])
    with (root/'account_events.csv').open(newline='') as f:
        events=list(csv.DictReader(f))
    plt.rcParams.update({'font.size':11,'axes.spines.top':False,'axes.spines.right':False,'axes.grid':True,'grid.alpha':.16,'figure.facecolor':'white','path.simplify':True})

    def date_axis(ax):
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=7,maxticks=12))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d\n%H:%M %Z',tz=NY))

    def finish(fig,path):
        fig.savefig(charts/path,dpi=220,bbox_inches='tight',facecolor='white')
        plt.close(fig)

    def line(ax,start,end,side,**kwargs):
        lo=np.searchsorted(quotes['time'],round(start.timestamp()*1e6))
        hi=np.searchsorted(quotes['time'],round(end.timestamp()*1e6),side='right')
        rows=quotes[lo:hi]
        x=rows['time']/86400e6
        y=rows['bid' if side=='up' else 'ask'].copy()
        gaps=np.flatnonzero(np.diff(rows['time'])>60e6)+1
        if len(gaps):
            x=np.insert(x,gaps,np.nan)
            y=np.insert(y,gaps,np.nan)
        ax.plot(x,y,**kwargs)
        return rows

    contexts=[]
    summaries=[]
    cards=[]
    for t in trades:
        i=t['trade_id']
        prev=t['previous_context']
        if not prev or not prev['levels']:
            raise ValueError(f'Trade{i}: previous ACD evidence missing')
        start=stamp(prev['start'])
        end=stamp(t['next_formation_end'])+timedelta(minutes=30)
        entry,exit_time=stamp(t['entry_timestamp']),stamp(t['exit_timestamp'])
        entry_session_start=stamp(t['formation_start'])
        entry_session_end=stamp(t['next_formation_start'])
        formation_end=stamp(t['formation_end'])
        deadline=stamp(t['mandatory_deadline'])
        next_end=stamp(t['next_formation_end'])
        full=f'trade_{i:03}_full_session.png'
        zoom=f'trade_{i:03}_zoom.png'
        fig,ax=plt.subplots(figsize=(20,9))
        rows=line(ax,start,end,t['side'],color='#374151',lw=.65,label='Actual closing bid' if t['side']=='up' else 'Actual closing ask')
        if not len(rows):
            raise ValueError('No chart context quotes')
        prior_levels=prev['levels']
        ax.axvspan(start,stamp(prev['formation_end']),color='#9ca3af',alpha=.45,label='Previous formation')
        ax.axvspan(entry_session_start,formation_end,color='#60a5fa',alpha=.38,label='Trading formation')
        ax.axvspan(entry_session_end,next_end,color='#c4b5fd',alpha=.55,label='Next formation')
        for value in (prior_levels['aUp'],prior_levels['aDown']):
            ax.hlines(value,stamp(prev['formation_end']),entry_session_start,color='#6b7280',ls='--',lw=1.2)
        ax.plot([],[],color='#6b7280',ls='--',label=f"Previous A {prior_levels['aDown']:.3f} / {prior_levels['aUp']:.3f}")
        for value in (t['upper'],t['lower']):
            ax.hlines(value,formation_end,entry_session_end,color='#2563eb',lw=1.25)
        ax.plot([],[],color='#2563eb',label=f"Trading A {t['lower']:.3f} / {t['upper']:.3f}")
        line(ax,entry,exit_time,t['side'],color='#0d9488',lw=1.6,label='Position open')
        ax.hlines(t['opposite_exit'],entry,entry_session_end,color='#dc2626',ls='--',lw=1.1,label=f"Opposite stop {t['opposite_exit']:.3f} (hold)")
        ax.axvline(deadline,color='#c2410c',ls=':',lw=1.4,label='Mandatory close deadline')
        ax.axvline(entry_session_end,color='#7c3aed',ls=':',lw=.8)
        ax.axvline(next_end,color='#7c3aed',ls=':',lw=.8)
        for name,when,price,marker,color in [('ENTRY',entry,t['entry_price'],'^','#15803d'),('EXIT',exit_time,t['exit_price'],'X','#b91c1c')]:
            ax.scatter([when],[price],marker=marker,color=color,s=80,zorder=7)
            ax.annotate(name,(when,price),xytext=(-15,24 if name=='ENTRY' else -38),textcoords='offset points',fontsize=10,fontweight='bold',color=color,arrowprops=dict(arrowstyle='-',color=color,lw=.7))
        ax.axvspan(stamp(t['confirmation_start']),entry,color='#fbbf24',alpha=.25,label='Current direction confirmation')
        moves=t['trailing_movements']
        if moves:
            tx=[stamp(r['timestamp']) for r in moves]+[exit_time]
            ty=[r['level'] for r in moves]+[moves[-1]['level']]
            ax.step(tx,ty,where='post',color='#9333ea',lw=1.1,label='Actual trailing half')
        ax.set_xlim(start,end)
        ax.set_ylabel('XAUUSD quote USD — linear price scale')
        date_axis(ax)
        ax.set_title(f"Trade {i:02} | NY ACD {t['date']} | {direction(t['side'])} | net USD {t['net_pnl']:+.2f}",loc='left',fontsize=17,pad=55)
        ax.text(0,1.075,f"Previous {prev['date']}: {direction(t['yesterday_bias'])} | first ask {prev['first_ask']:.2f} → final bid {prev['last_bid']:.2f} | movement {prev['movement']:+.2f}",transform=ax.transAxes,fontsize=12)
        ax.text(0,1.035,f"Today {direction(t['today_direction'])} confirmed {ny(t['entry_timestamp'])} | Entry {t['entry_price']:.2f} | Exit {ny(t['exit_timestamp'])} @ {t['exit_price']:.2f} ({t['exit_reason']})",transform=ax.transAxes,fontsize=11)
        ax.set_xlabel(f"New York time | next formation {ny(t['next_formation_start'])} → {ny(t['next_formation_end'])}\nPrice line selects actual last quote/second; gaps >60s left blank. All executions/MAE/MFE use full ticks.",fontsize=10)
        ax.legend(loc='upper center',bbox_to_anchor=(.5,-.15),ncol=5,fontsize=9,frameon=False)
        fig.subplots_adjust(top=.80,bottom=.24,left=.06,right=.98)
        finish(fig,full)

        # Secondary zoom always accompanies, never substitutes for, full context.
        fig,ax=plt.subplots(figsize=(14,6))
        line(ax,entry-timedelta(minutes=10),exit_time+timedelta(minutes=10),t['side'],color='#374151',lw=.9,label='Actual quote/second')
        line(ax,entry,exit_time,t['side'],color='#0d9488',lw=1.8,label='Position open')
        ax.axhline(t['upper'],color='#2563eb',ls='--',lw=.8,label='Trading A boundaries')
        ax.axhline(t['lower'],color='#2563eb',ls='--',lw=.8)
        ax.axhline(t['opposite_exit'],color='#dc2626',ls=':',label='Opposite stop')
        ax.scatter([entry,exit_time],[t['entry_price'],t['exit_price']],color=['#15803d','#b91c1c'],s=50,zorder=6)
        if moves:
            ax.step(tx,ty,where='post',color='#9333ea',label='Actual trailing half')
        for sk,pk,label,color in [('mae_timestamp','mae_price','Exact tick MAE','#dc2626'),('mfe_timestamp','mfe_price','Exact tick MFE','#0284c7')]:
            if t.get(sk):
                ax.scatter([stamp(t[sk])],[t[pk]],s=30,color=color,label=label,zorder=7)
        date_axis(ax)
        ax.set(title=f"Trade {i:02} secondary zoom | {ny(t['entry_timestamp'])} → {ny(t['exit_timestamp'])}",ylabel='XAUUSD quote USD',xlabel='New York time')
        ax.legend(fontsize=9,ncol=3,loc='best')
        fig.tight_layout()
        finish(fig,zoom)

        actual_first=datetime.fromtimestamp(int(rows['time'][0])/1e6,timezone.utc)
        actual_last=datetime.fromtimestamp(int(rows['time'][-1])/1e6,timezone.utc)
        prior_formation=quotes[(quotes['time']>=round(start.timestamp()*1e6))&(quotes['time']<round(stamp(prev['formation_end']).timestamp()*1e6))]
        next_formation=quotes[(quotes['time']>=round(entry_session_end.timestamp()*1e6))&(quotes['time']<round(next_end.timestamp()*1e6))]
        def covered(rows,left,right):
            return bool(len(rows) and rows['time'][0]-round(left.timestamp()*1e6)<=60e6 and round(right.timestamp()*1e6)-rows['time'][-1]<=60e6 and (len(rows)<2 or np.max(np.diff(rows['time']))<=60e6))
        previous_covered=covered(prior_formation,start,stamp(prev['formation_end']))
        next_covered=covered(next_formation,entry_session_end,next_end)
        complete=(actual_first-start).total_seconds()<=60 and actual_last>=next_end and previous_covered and next_covered
        contexts.append(dict(trade_id=i,date=t['date'],requested_start=start.isoformat(),requested_end=end.isoformat(),actual_first_quote=actual_first.isoformat(),actual_last_quote=actual_last.isoformat(),previous_formation_quotes=len(prior_formation),next_formation_quotes=len(next_formation),previous_formation_complete=previous_covered,next_formation_complete=next_covered,complete_context=bool(complete),full_chart='charts/'+full,zoom_chart='charts/'+zoom))
        fields=dict(trade_number=i,acd_session_date=t['date'],previous_session=prev['date'],previous_direction=direction(t['yesterday_bias']),previous_first_ask=prev['first_ask'],previous_final_bid=prev['last_bid'],previous_movement=prev['movement'],today_direction=direction(t['today_direction']),confirmation_start_ny=ny(t['confirmation_start']),entry_ny=ny(t['entry_timestamp']),entry_price=t['entry_price'],exit_ny=ny(t['exit_timestamp']),exit_price=t['exit_price'],lots=t['lots'],holding_minutes=t['holding_seconds']/60,mfe_usd=t['mfe_usd'],mae_usd=t['mae_usd'],exit_reason=t['exit_reason'],gross_pnl=t['gross_pnl'],net_pnl=t['net_pnl'],balance_before_replenishment=t['balance_before_replenishment'],replenishment=t['replenishment'],balance_after_replenishment=t['balance_after_replenishment'],mandatory_deadline_ny=ny(t['mandatory_deadline']),deadline_delay_seconds=t['deadline_delay_seconds'],open_into_next_formation=t['open_into_next_formation'],full_chart='charts/'+full,zoom_chart='charts/'+zoom)
        summaries.append(fields)
        fields.update(previous_upper_boundary=prior_levels['aUp'],previous_lower_boundary=prior_levels['aDown'],trading_upper_boundary=t['upper'],trading_lower_boundary=t['lower'],opposite_stop_boundary=t['opposite_exit'])
        cells=''.join(f'<div><dt>{html.escape(k.replace("_"," "))}</dt><dd>{html.escape((f"{v:,.3f}" if k.endswith("boundary") else number(v)) if isinstance(v,float) else str(v))}</dd></div>' for k,v in fields.items() if k not in ['full_chart','zoom_chart'])
        move_rows=''.join(f'<tr><td>{html.escape(ny(r["timestamp"]))}</td><td>{r["level"]:.3f}</td></tr>' for r in moves)
        move_table=f'<details><summary>All {len(moves)} actual trailing movements</summary><table><thead><tr><th>New York timestamp</th><th>Level</th></tr></thead><tbody>{move_rows}</tbody></table></details>' if moves else '<p>Trailing half was not activated during this trade.</p>'
        cards.append(f'<article id="trade-{i}" data-search="{t["date"]} {i} {direction(t["side"])} {t["exit_reason"]}"><h2>Trade {i:02} · NY {t["date"]} · {direction(t["side"])} · net ${t["net_pnl"]:+.2f}</h2><dl>{cells}</dl><a href="charts/{full}" target="_blank"><img loading="lazy" src="charts/{full}" alt="Trade {i} full previous and trading sessions and next formation"></a><details><summary>Secondary zoom — full-session chart remains above</summary><a href="charts/{zoom}" target="_blank"><img loading="lazy" src="charts/{zoom}" alt="Trade {i} secondary zoom"></a></details>{move_table}</article>')

    # Cash and contributed money are intentionally separate from trading results.
    for key,title,ylabel,name,color in [('balance','Account cash balance — contributions explicitly marked','USD cash balance','account_balance.png','#0369a1'),('trading_pnl','Cumulative NET trading P&L — external contributions excluded','USD trading P&L','cumulative_trading_pnl.png','#b91c1c'),('contributions','Cumulative external capital — includes initial USD 500','USD contributed','capital_contributions.png','#7c3aed')]:
        fig,ax=plt.subplots(figsize=(13,4.5))
        x=[stamp(r['timestamp']) for r in events]
        y=[float(r[key]) for r in events]
        x.append(stamp(report['window_end']))
        y.append(y[-1])
        ax.step(x,y,where='post',color=color,lw=1.3)
        if key=='balance':
            deposits=[r for r in events if r['kind']=='external_replenishment']
            ax.scatter([stamp(r['timestamp']) for r in deposits],[float(r['balance']) for r in deposits],facecolors='none',edgecolors='#d97706',s=45,label='External replenishment after closure')
            ax.axhline(500,color='#777',ls=':',lw=.8)
            ax.legend(fontsize=9)
        if key=='trading_pnl':
            ax.axhline(0,color='#777',lw=.8)
        ax.set(title=title,ylabel=ylabel,xlabel='New York dates')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d',tz=NY))
        fig.tight_layout()
        finish(fig,name)

    write_csv(root/'trade_summaries.csv',summaries)
    write_csv(root/'chart_context_checks.csv',contexts)
    (root/'trade_summaries.json').write_text(json.dumps(summaries,indent=2))
    (root/'chart_context_checks.json').write_text(json.dumps(contexts,indent=2))
    m=report['metrics']
    page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ACD Phase 2 revised — complete trade gallery</title><style>
body{font:16px system-ui,sans-serif;background:#f1f5f9;color:#172033;margin:0}header,main{max-width:1600px;margin:auto;padding:24px}header{position:relative;background:#fff}h1{margin:0 0 12px}p{line-height:1.55}a{color:#075985}nav{display:flex;gap:18px;flex-wrap:wrap;margin:16px 0}input{font:inherit;padding:10px;width:min(550px,90%);border:1px solid #94a3b8;border-radius:6px}article{background:white;padding:24px;margin:24px 0;border-radius:10px;box-shadow:0 1px 5px #0001}h2{font-size:22px}dl{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px;background:#f8fafc;padding:16px}dt{font-size:12px;color:#475569;text-transform:uppercase}dd{margin:4px 0;font-weight:600;overflow-wrap:anywhere}img{width:100%;height:auto;display:block}summary{cursor:pointer;padding:16px;background:#e2e8f0}small{color:#475569}.metrics{display:flex;gap:20px;flex-wrap:wrap}.metrics b{font-size:22px}.warning{border-left:4px solid #d97706;padding-left:14px}article[hidden]{display:none}
</style><header><h1>ACD continuation · Phase 2 revised</h1><p>Every executed trade in chronological New York ACD session order. The full chart starts at the previous formation and continues beyond completion of the next formation. Click a chart for full-resolution PNG. Secondary zooms supplement the full-session evidence.</p>'''
    page+=f'<div class="metrics"><span><b>{len(trades)}</b> trades</span><span><b>${m["net_trading_pnl"]:+.2f}</b> net trading P&L</span><span><b>${m["total_replenishments"]:.2f}</b> replenished</span><span><b>${m["total_external_capital"]:.2f}</b> total external capital</span><span><b>${m["final_balance"]:.2f}</b> final balance</span></div>'
    page+='<p class="warning">Replenishments are outside capital, never trading profit. The unchanged capital trigger is absolute USD455, not a new per-trade loss limit. Historical costs remain the original unverified paper assumptions. Prices are actual selected quotes; gaps longer than60 seconds are blank. Broker holiday/calendar limitations remain.</p><nav><a href="REPORT.md">Complete report</a><a href="charts_and_summaries.zip" download>Download charts and summaries ZIP</a><a href="trade_summaries.csv" download>Trade summaries CSV</a><a href="capital_contributions.csv" download>Capital ledger CSV</a><a href="signal_day_investigation.csv" download>All31 signal days</a></nav><label>Find date, trade number, side or exit reason <input id="filter" placeholder="Example: 2026-10, BULLISH, capital"></label><p><small>All timestamps are New York, explicitly labelled EDT/EST. The strategy runs on full ticks; context lines show actual last quote per second. No candles, synthetic fills, interpolation across closures or parameter optimization.</small></p></header><main>'
    investigation=json.loads((root/'signal_day_investigation.json').read_text())
    decisions=[]
    for r in investigation:
        if r['executed']:
            detail=f'<a href="#trade-{r["trade_id"]}">Executed trade {r["trade_id"]}</a>'
        else:
            c=r.get('closest_confirmed_rejection')
            detail=html.escape(str(r['reason']))
            if c:
                detail+=f' — closest confirmed risk ${c["estimated_risk"]:.2f} vs available ${c["available_risk_budget"]:.2f}; {html.escape(ny(c["timestamp"]))}'
        decisions.append(f'<tr><td>{r["date"]}</td><td>{detail}</td></tr>')
    page+='<details><summary>Inspect all31 original continuation-signal days and exact trade/rejection decisions</summary><table style="width:100%;border-collapse:collapse"><thead><tr><th style="text-align:left">New York date</th><th style="text-align:left">Decision</th></tr></thead><tbody>'+''.join(decisions)+'</tbody></table></details>'
    page+=''.join(cards)
    page+='</main><script>document.getElementById("filter").addEventListener("input",function(){const q=this.value.toLowerCase().trim();document.querySelectorAll("article").forEach(a=>a.hidden=!a.dataset.search.toLowerCase().includes(q));});</script></html>'
    (root/'gallery.html').write_text(page,encoding='utf-8')
    print(json.dumps(dict(charts=len(trades),all_contexts_complete=all(c['complete_context'] for c in contexts),incomplete=[c for c in contexts if not c['complete_context']]),indent=2))


if __name__=='__main__':
    main()
