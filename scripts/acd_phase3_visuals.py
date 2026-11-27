"""Identical-scale session comparisons, sensitivity figures and trade-2 forensics."""
import argparse
import csv
import gzip
import html
import json
import sys
import zipfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
from acd_phase3_research import held_beyond,seconds,iso,write_csv

SELECTED=['baseline','target_entry_1W','target_edge_1W','trail_entry_edge_1W','trail_edge_edge_1W','trail_entry_entry_1W','trail_edge_entry_1W','reentry_A','reentry_B','no_trailing']
NY=ZoneInfo('America/New_York')
EPOCH=mdates.date2num(datetime(1970,1,1,tzinfo=timezone.utc))


def number_time(value):
    return seconds(value)/86400+EPOCH


def ny(value):
    return datetime.fromisoformat(value).astimezone(NY).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+' '+datetime.fromisoformat(value).astimezone(NY).tzname()


def forensic(root,cache,trades,reference):
    baseline=trades['baseline'][1]
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    day=next(d for d in manifest['days'] if d['date']==baseline['date'])
    dtype=np.dtype([('id','<i8'),('time','<i8'),('bid','<f8'),('ask','<f8')])
    raw=np.memmap(cache/(day['date']+'.bin'),dtype=dtype,mode='r')
    t=raw['time']/1e6
    q=raw['bid'] if baseline['side']=='up' else raw['ask']
    sign=1 if baseline['side']=='up' else -1
    start=seconds(baseline['entry_timestamp'])
    deadline=seconds(baseline['mandatory_deadline'])
    selected=(t>=start)&(t<=deadline)
    tt,qq=t[selected],q[selected]
    def hit(condition):
        indexes=np.flatnonzero(condition)
        return dict(timestamp=iso(tt[indexes[0]]),price=float(qq[indexes[0]])) if len(indexes) else None
    favorable=(qq-baseline['entry_price'])*sign
    best=int(np.argmax(favorable))
    worst=int(np.argmin(favorable))
    result=dict(date=baseline['date'],entry=baseline['entry_price'],W=baseline['W'],deadline=baseline['mandatory_deadline'],returned_inside=hit((qq-baseline['edge'])*sign<=0),raw_opposite_touch=hit((qq-baseline['opposite'])*sign<0),held_opposite_invalidation=hit(held_beyond(tt,(qq-baseline['opposite'])*sign<0)),one_width_edge=hit((qq-baseline['edge'])*sign>=baseline['W']),one_width_entry=hit((qq-baseline['entry_price'])*sign>=baseline['W']),maximum_favorable=dict(movement=float(favorable[best]),timestamp=iso(tt[best]),price=float(qq[best])),maximum_adverse=dict(movement=float(max(0,-favorable[worst])),timestamp=iso(tt[worst]),price=float(qq[worst])),capital_first_if_held_from_baseline_entry=hit(baseline['balance_before']+favorable-.14<=455),variants={c:[r for r in trades[c] if r['date']==baseline['date']] for c in SELECTED})
    (root/'trade2_forensic.json').write_text(json.dumps(result,indent=2))
    lines=['# Trade 2 forensic investigation','',f"NY ACD session **{baseline['date']}**, {baseline['side']}; frozen W **{baseline['W']:.4f}**. Baseline entry **{ny(baseline['entry_timestamp'])} at {baseline['entry_price']:.2f}**. Mandatory deadline **{ny(baseline['mandatory_deadline'])}**.",'',
        'Events below use full executable quotes from entry until the deadline. A later favorable extreme is a path observation, not an achievable exit or a replacement for an earlier valid exit.','', '| Path event | First executable observation |','|---|---|']
    for key in ['returned_inside','raw_opposite_touch','held_opposite_invalidation','one_width_edge','one_width_entry','capital_first_if_held_from_baseline_entry']:
        value=result[key]
        lines.append(f"|{key}|{ny(value['timestamp'])+' at '+format(value['price'],'.2f') if value else 'Not reached before deadline'}|")
    lines+=['',f"Maximum favorable movement before closure deadline: **{result['maximum_favorable']['movement']:.2f}**, quote {result['maximum_favorable']['price']:.2f} at {ny(result['maximum_favorable']['timestamp'])}. Maximum adverse movement: **{result['maximum_adverse']['movement']:.2f}**, quote {result['maximum_adverse']['price']:.2f} at {ny(result['maximum_adverse']['timestamp'])}.",'',
        'The capital-first path uses the baseline entry balance and the unchanged absolute USD 455 equity trigger. Each alternative below instead follows its own independent account history and intervening exits. No later peak is credited after an earlier target, pullback, capital, opposite or timed exit.','', '| Configuration | Entry NY / price | Exit NY / price | Reason | Net USD | Capital before / after |','|---|---|---|---|---:|---|']
    for cfg,rows in result['variants'].items():
        for r in rows:
            lines.append(f"|{cfg} #{r['session_trade']}|{ny(r['entry_timestamp'])} / {r['entry_price']:.2f}|{ny(r['exit_timestamp'])} / {r['exit_price']:.2f}|{r['exit_reason']}|{r['net_pnl']:.2f}|{r['balance_before']:.2f} / {r['balance_after']:.2f}|")
    lines+=['','[Full-session comparison](charts/session_2026-09-02.png) · [Secondary zoom](charts/trade2_zoom.png) · [Complete raw result fields](trade2_forensic.json)','',
        'The previously cited 4470 movement occurs after the 08:30 NY mandatory deadline in the chart’s next-formation context. It cannot be credited to a position subject to that deadline. A software deadline can only execute at an available quote; exact delays are recorded for every configuration.']
    (root/'TRADE2.md').write_text('\n'.join(lines)+'\n')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    parser.add_argument('--cache',required=True)
    parser.add_argument('--reference',required=True)
    parser.add_argument('--quotes',required=True)
    args=parser.parse_args()
    root,cache,ref=Path(args.directory),Path(args.cache),Path(args.reference)
    charts=root/'charts'
    charts.mkdir(exist_ok=True)
    trades=json.loads((root/'all_trades.json').read_text())
    compare=json.loads((root/'comparison.json').read_text())
    assert json.loads((root/'baseline_reproduction.json').read_text())['matches']
    frozen=json.loads((ref/'trades.json').read_text())
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    days={d['date']:d for d in manifest['days']}
    quotes=np.frombuffer(gzip.open(args.quotes,'rb').read(),dtype=np.dtype([('time','<i8'),('bid','<f8'),('ask','<f8')]))
    all_x=quotes['time'].astype(np.float64)/1e6/86400+EPOCH
    contexts=[]
    def session_chart(original,zoom=False):
        date=original['date']
        prev=original['previous_context']
        left=number_time(prev['start']) if not zoom else number_time(original['entry_timestamp'])-5/1440
        right=number_time(original['next_formation_end'])+30/1440 if not zoom else number_time(original['exit_timestamp'])+5/1440
        use=(all_x>=left)&(all_x<=right)
        x=all_x[use]
        y=quotes['bid' if original['side']=='up' else 'ask'][use].copy()
        y[np.r_[False,np.diff(x)*86400>60]]=np.nan
        low,high=np.nanmin(y),np.nanmax(y)
        shared_levels=[original['upper'],original['lower'],original['opposite_exit']]+list(prev['levels'].values())
        for cfg in SELECTED:
            for r in trades[cfg]:
                if r['date']!=date:continue
                if r['target_level'] is not None:shared_levels.append(r['target_level'])
                if cfg=='baseline' or cfg.startswith('trail_'):
                    anchor=r['entry_price'] if cfg=='baseline' or cfg.startswith('trail_entry_') else r['edge']
                    shared_levels.append(anchor+(1 if r['side']=='up' else -1)*(3 if cfg=='baseline' else r['W']))
        if not zoom:
            low=min(low,min(shared_levels))
            high=max(high,max(shared_levels))
        padding=max(2,(high-low)*.04)
        fig,axes=plt.subplots(5,2,figsize=(30,25),sharex=True,sharey=True)
        for ax,cfg in zip(axes.flat,SELECTED):
            rows=[t for t in trades[cfg] if t['date']==date]
            ax.plot(x,y,color='#475569',lw=.55)
            for a,b,color in [(prev['start'],prev['formation_end'],'#94a3b8'),(original['formation_start'],original['formation_end'],'#60a5fa'),(original['next_formation_start'],original['next_formation_end'],'#c4b5fd')]:
                ax.axvspan(number_time(a),number_time(b),color=color,alpha=.30)
            for name in ['aUp','aDown']:
                ax.hlines(prev['levels'][name],number_time(prev['formation_end']),number_time(original['formation_start']),color='#64748b',ls='--',lw=.8)
            for edge in ['upper','lower']:
                ax.hlines(original[edge],number_time(original['formation_end']),number_time(original['next_formation_start']),color='#2563eb',lw=.8)
            ax.axvline(number_time(original['mandatory_deadline']),color='#ea580c',ls=':',lw=1)
            ax.hlines(original['opposite_exit'],number_time(original['formation_end']),number_time(original['next_formation_start']),color='#ef4444',ls=':',lw=.8)
            for r in rows:
                entry,exit=number_time(r['entry_timestamp']),number_time(r['exit_timestamp'])
                open_mask=(x>=entry)&(x<=exit)
                ax.plot(x[open_mask],y[open_mask],color='#0d9488',lw=1.4)
                ax.scatter([entry],[r['entry_price']],marker='^',s=22,color='#15803d',zorder=6)
                ax.scatter([exit],[r['exit_price']],marker='x',s=22,color='#b91c1c',zorder=6)
                ax.annotate(str(r['session_trade']),(entry,r['entry_price']),xytext=(2,7),textcoords='offset points',fontsize=7)
                if r['target_level'] is not None:
                    ax.hlines(r['target_level'],entry,exit,color='#d97706',ls='--',lw=1)
                if r['activation_timestamp']:
                    ax.axvline(number_time(r['activation_timestamp']),color='#16a34a',lw=.7,alpha=.7)
                if cfg=='baseline' or cfg.startswith('trail_'):
                    anchor=r['entry_price'] if cfg=='baseline' or cfg.startswith('trail_entry_') else r['edge']
                    activation_level=anchor+(1 if r['side']=='up' else -1)*(3 if cfg=='baseline' else r['W'])
                    ax.hlines(activation_level,entry,exit,color='#16a34a',ls=':',lw=.8)
                moves=r['trail_movements']
                if moves:
                    ax.step([number_time(m['timestamp']) for m in moves]+[exit],[m['level'] for m in moves]+[moves[-1]['level']],where='post',color='#9333ea',lw=1)
            total=sum(r['net_pnl'] for r in rows)
            ax.set_title(f"{cfg} | {len(rows)} trades | net USD {total:+.2f}",loc='left',fontsize=12)
            ax.set_xlim(left,right)
            ax.set_ylim(low-padding,high+padding)
            ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4,maxticks=7))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M %Z',tz=NY))
            ax.tick_params(labelbottom=True,labelleft=True,labelsize=8)
            ax.grid(alpha=.12)
        fig.suptitle(f"NY ACD {date} | prior {prev['date']} {original['yesterday_bias']} | W {original['upper']-original['lower']:.3f}\nIdentical time and price scales · gray/blue/purple: previous/trading/next formation · green entry, red exit · teal open · purple trail · green activation · orange target/deadline",fontsize=16,y=.99)
        fig.subplots_adjust(top=.95,bottom=.035,hspace=.35,wspace=.10,left=.045,right=.985)
        name=f'zoom_{date}.png' if zoom else f'session_{date}.png'
        fig.savefig(charts/name,dpi=155)
        plt.close(fig)
        if not zoom:
            contexts.append(dict(date=date,start=iso((left-EPOCH)*86400),end=iso((right-EPOCH)*86400),same_scales=True,quotes=len(x),next_formation_complete=bool(x[-1]>=number_time(original['next_formation_end'])),chart='charts/'+name))
    for original in frozen:
        session_chart(original)
        if original['holding_seconds']<1800 or original['trade_id']==2:
            session_chart(original,True)
        print('CHART',original['date'],flush=True)
    (charts/'trade2_zoom.png').write_bytes((charts/'zoom_2026-09-02.png').read_bytes())
    (root/'chart_context_checks.json').write_text(json.dumps(contexts,indent=2))
    # Four curves and four heatmaps; reference conventions remain separate.
    width_rows=[r for r in compare if r.get('activation')=='width']
    for field,label in [('net_pnl','Net trading P&L (USD)'),('profit_factor','Profit factor'),('flow_adjusted_account_drawdown','Funding-adjusted equity drawdown (USD)'),('capital_replenishments','Capital replenishments (USD)')]:
        fig,ax=plt.subplots(figsize=(10,5))
        groups=[]
        for activation_ref in ['entry','edge']:
            for trail_ref in ['edge','entry']:
                rows=sorted([r for r in width_rows if r['activation_ref']==activation_ref and r['trail_ref']==trail_ref],key=lambda r:r['multiple'])
                ax.plot([r['multiple'] for r in rows],[r[field] for r in rows],marker='o',label=f'activation {activation_ref}; half {trail_ref}')
                groups.append((f'{activation_ref} / {trail_ref}',rows))
        ax.set_xlabel('Activation distance / frozen ACD width W')
        ax.set_ylabel(label)
        ax.grid(alpha=.2)
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(charts/(field+'_sensitivity.png'),dpi=180)
        plt.close(fig)
        matrix=np.array([[r[field] if r[field] is not None else np.nan for r in rows] for _,rows in groups])
        fig,ax=plt.subplots(figsize=(11,4))
        im=ax.imshow(matrix,aspect='auto',cmap='viridis')
        ax.set_xticks(range(8),['.25','.50','.75','1','1.25','1.50','2','3'])
        ax.set_yticks(range(4),[name for name,_ in groups])
        ax.set_xlabel('Activation threshold (W)')
        ax.set_ylabel('Activation ref / trailing-half ref')
        ax.set_title(label)
        for i in range(4):
            for j in range(8):ax.text(j,i,f'{matrix[i,j]:.2f}',ha='center',va='center',fontsize=8,color='white')
        fig.colorbar(im,ax=ax)
        fig.tight_layout()
        fig.savefig(charts/(field+'_heatmap.png'),dpi=180)
        plt.close(fig)
    forensic(root,cache,trades,ref)
    # Independent per-configuration exports accompany the combined table.
    records=root/'trade_records'
    records.mkdir(exist_ok=True)
    for cfg,rows in trades.items():
        write_csv(records/(cfg+'.csv'),[{k:v for k,v in r.items() if k!='trail_movements'} for r in rows])
    page='''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>ACD Phase 3 exit comparisons</title><style>body{font:15px system-ui;margin:24px;color:#172033;background:#f8fafc}main{max-width:2200px;margin:auto}img{width:100%;background:white}article{margin:40px 0;border-top:3px solid #94a3b8}table{border-collapse:collapse;width:100%;font-size:12px}td,th{padding:6px;border:1px solid #cbd5e1;text-align:left}details{margin:14px 0}a{color:#0369a1}input{padding:10px;width:280px}</style><main><h1>ACD Phase 3 — exit research</h1><p>Exploratory same-sample comparisons. No configuration was selected or deployed. Every panel has identical time and price scales. Actual last quote per second is displayed; all valid ticks determine entries and exits. Gaps over 60 seconds are blank. Formation shading: previous gray, trading blue, next purple. Entry ▲ green; exit × red; open line teal; trailing purple; activation green; target/deadline orange.</p><p><a href="REPORT.md">Report</a> · <a href="TRADE2.md">Trade 2 forensic report</a> · <a href="comparison.csv">All 55 configurations</a> · <a href="all_trades.csv">All trades</a> · <a href="research_charts_and_records.zip">Download charts and records</a></p><input id="filter" placeholder="Filter session date"><p>Click a full-session chart for its full-resolution image. Expand records for every exact New York entry and exit.</p>'''
    for original in frozen:
        date=original['date']
        path=f'charts/session_{date}.png'
        page+=f'<article data-date="{date}"><h2>{date}</h2><a href="{path}"><img src="{path}" loading="lazy" alt="{date} ten identical-scale exit comparisons"></a><details><summary>Exact simulated trades and funding — all displayed configurations</summary><table><tr><th>Configuration</th><th>#</th><th>Entry NY</th><th>Entry</th><th>Exit NY</th><th>Exit</th><th>Reason</th><th>Net USD</th><th>Top-up</th><th>Balance</th></tr>'
        for cfg in SELECTED:
            for r in trades[cfg]:
                if r['date']==date:
                    page+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in [cfg,r['session_trade'],ny(r['entry_timestamp']),f"{r['entry_price']:.2f}",ny(r['exit_timestamp']),f"{r['exit_price']:.2f}",r['exit_reason'],f"{r['net_pnl']:.2f}",f"{r['replenishment']:.2f}",f"{r['balance_after']:.2f}"])+'</tr>'
        page+='</table></details>'
        if (charts/f'zoom_{date}.png').exists():page+=f'<details><summary>Secondary early-entry zoom (full-session comparison remains above)</summary><img src="charts/zoom_{date}.png" loading="lazy"></details>'
        page+='</article>'
    page+='</main><script>document.getElementById("filter").oninput=function(){document.querySelectorAll("article").forEach(a=>a.hidden=!a.dataset.date.includes(this.value.trim()))}</script>'
    (root/'gallery.html').write_text(page)
    (root/'VISUALS_COMPLETE').write_text('All full-context comparisons generated')


if __name__=='__main__':
    main()
