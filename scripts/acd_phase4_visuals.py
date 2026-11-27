"""Full-context entry comparisons, entry-process zooms and parameter surfaces."""
import argparse
import gzip
import html
import json
import sys
from datetime import datetime,timedelta,timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
import acd_phase4_research as study

SELECTED=['A_450s','A_60s','B_0.1W','C_0.1W_60s','D_225s_0.25W','E_60s_0.1W']
NY=ZoneInfo('America/New_York')
EPOCH=mdates.date2num(datetime(1970,1,1,tzinfo=timezone.utc))


def num(value):return study.core.seconds(value)/86400+EPOCH


def ny(value):
    if value is None:return 'OPEN AT DATA CUTOFF'
    return datetime.fromisoformat(value).astimezone(NY).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+' '+datetime.fromisoformat(value).astimezone(NY).tzname()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory')
    p.add_argument('--cache',required=True)
    p.add_argument('--quotes',required=True)
    args=p.parse_args()
    root,cache=Path(args.directory),Path(args.cache)
    charts=root/'charts'
    charts.mkdir(exist_ok=True)
    all_trades=json.loads((root/'all_trades.json').read_text())
    opens=json.loads((root/'open_positions.json').read_text())
    results=json.loads((root/'comparison.json').read_text())
    baseline=json.loads((root/'baseline_forensics.json').read_text())
    configs=json.loads((root/'configurations.json').read_text())
    config_map={r['id']:r for r in configs}
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    days=manifest['days']
    day_map={d['date']:d for d in days}
    dates=[d['date'] for d in days]
    records={cfg:{r['date']:r for r in rows+opens.get(cfg,[])} for cfg,rows in all_trades.items()}
    quotes=np.frombuffer(gzip.open(args.quotes,'rb').read(),dtype=np.dtype([('time','<i8'),('bid','<f8'),('ask','<f8')]))
    all_x=quotes['time']/1e6/86400+EPOCH
    selected_dates=[]
    for date in dates:
        base=records['A_450s__exit_A'].get(date)
        material=False
        for profile in SELECTED[1:]:
            other=records[profile+'__exit_A'].get(date)
            if bool(base)!=bool(other):material=True
            elif base and other:
                material|=abs(study.core.seconds(base['entry_timestamp'])-study.core.seconds(other['entry_timestamp']))>=30 or abs(base['entry_price']-other['entry_price'])>=.05*base['W']
        if material:selected_dates.append(date)
    contexts=[]
    def draw(date,exit_id='exit_A',zoom=False):
        day=day_map[date]
        previous=days[dates.index(date)-1]
        rowset=[records[profile+'__'+exit_id].get(date) for profile in SELECTED]
        base=records['A_450s__'+exit_id].get(date)
        previous_start=num(previous['session_start'])
        next_start=num(day['session_end'])
        full_end=next_start+45/1440
        left,right=previous_start,full_end
        entries=[num(r['entry_timestamp']) for r in rowset if r]
        if zoom:
            process_starts=[num(r[k]) for r in rowset if r for k in ['first_breakout_timestamp','entry_run_start','initial_excursion_timestamp'] if r.get(k)]
            left=(min(process_starts or entries) if entries else num(day['session_start'])+15/1440)-3/1440
            right=(max(entries) if entries else left)+10/1440
            right=min(right,num(day['session_start'])+150/1440)
        use=(all_x>=left)&(all_x<=right)
        x=all_x[use]
        key='bid' if day['yesterday_bias']=='up' else 'ask'
        y=quotes[key][use].copy()
        y[np.r_[False,np.diff(x)*86400>60]]=np.nan
        low,high=float(np.nanmin(y)),float(np.nanmax(y))
        levels=[day['upper'],day['lower'],previous['upper'],previous['lower']]
        for r in rowset:
            if r:
                levels.append(r['opposite'])
                if r.get('target_level') is not None:levels.append(r['target_level'])
        if not zoom:
            low=min(low,min(v for v in levels if v is not None))
            high=max(high,max(v for v in levels if v is not None))
        pad=max(1,(high-low)*.035)
        fig,axes=plt.subplots(3,2,figsize=(28,16),sharex=True,sharey=True)
        for ax,profile,r in zip(axes.flat,SELECTED,rowset):
            cfg=config_map[profile+'__'+exit_id]
            ax.plot(x,y,lw=.6,color='#475569')
            for a,b,color in [(previous_start,previous_start+15/1440,'#94a3b8'),(num(day['session_start']),num(day['session_start'])+15/1440,'#60a5fa'),(next_start,next_start+15/1440,'#c4b5fd')]:ax.axvspan(a,b,color=color,alpha=.3)
            for level in [previous['upper'],previous['lower']]:
                if level is not None:ax.hlines(level,previous_start+15/1440,num(day['session_start']),color='#64748b',ls='--',lw=.8)
            for level in [day['upper'],day['lower']]:
                if level is not None:ax.hlines(level,num(day['session_start'])+15/1440,next_start,color='#2563eb',lw=.8)
            ax.axvline(next_start-1/24,color='#ea580c',ls=':',lw=1)
            if base:
                ax.axvline(num(base['entry_timestamp']),color='black',ls='--',lw=.8)
                ax.scatter([num(base['entry_timestamp'])],[base['entry_price']],s=35,marker='o',facecolors='none',edgecolors='black',zorder=7)
            if r:
                entry=num(r['entry_timestamp'])
                end=num(r['exit_timestamp'] or r['last_quote_timestamp'])
                ax.axvspan(num(r['entry_run_start']),entry,color='#fbbf24',alpha=.15)
                if r.get('first_breakout_timestamp'):ax.axvline(num(r['first_breakout_timestamp']),color='#0891b2',ls=':',lw=.7)
                open_mask=(x>=entry)&(x<=end)
                ax.plot(x[open_mask],y[open_mask],color='#0d9488',lw=1.5)
                ax.scatter([entry],[r['entry_price']],marker='^',color='#15803d',s=35,zorder=8)
                if r['exit_timestamp']:ax.scatter([end],[r['exit_price']],marker='x',color='#b91c1c',s=35,zorder=8)
                if profile.startswith('C_'):
                    ax.axvline(num(r['initial_excursion_timestamp']),color='#db2777',ls=':',lw=.8)
                    ax.axvline(num(r['retest_timestamp']),color='#db2777',lw=1)
                if profile.startswith('D_'):
                    sign=1 if r['side']=='up' else -1
                    zone=r['edge']+sign*cfg['zone']*r['W']
                    ax.fill_between([num(day['session_start'])+15/1440,num(day['session_start'])+135/1440],r['edge'],zone,color='#22c55e',alpha=.12)
                if r.get('target_level') is not None:ax.hlines(r['target_level'],entry,end,color='#d97706',ls='--',lw=.8)
                moves=r['trail_movements']
                if moves:ax.step([num(m['timestamp']) for m in moves]+[end],[m['level'] for m in moves]+[moves[-1]['level']],where='post',color='#9333ea',lw=1)
                title=f"{profile} | entry {r['entry_price']:.2f} | {r['entry_distance_W']:.3f}W | net {r['net_pnl']:+.2f}" if r['net_pnl'] is not None else f'{profile} | OPEN AT DATA CUTOFF'
            else:title=profile+' | no executed entry'
            if x[-1]<right-1/1440:
                ax.axvline(x[-1],color='red',ls='--')
                ax.text(.98,.03,'DATA ENDS HERE — later context unavailable',ha='right',transform=ax.transAxes,color='#b91c1c',fontsize=8)
            ax.set_title(title,loc='left',fontsize=12)
            ax.set_xlim(left,right);ax.set_ylim(low-pad,high+pad)
            ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4,maxticks=7))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M %Z',tz=NY))
            ax.tick_params(labelbottom=True,labelleft=True,labelsize=9)
            ax.grid(alpha=.12)
        fig.suptitle(f"NY ACD {date} | {exit_id} held fixed | {'ENTRY PROCESS ZOOM' if zoom else 'FULL PREVIOUS / CURRENT / NEXT FORMATION'}\nSame time/price scales · cyan first breakout · gold confirmation · black original entry · green alternative entry · pink retest · green zone · teal position · red exit · orange deadline",fontsize=15,y=.99)
        fig.subplots_adjust(top=.94,bottom=.045,left=.045,right=.985,hspace=.32,wspace=.10)
        name=f"{'zoom' if zoom else 'session'}_{date}_{exit_id}.png"
        fig.savefig(charts/name,dpi=155)
        plt.close(fig)
        if not zoom:contexts.append(dict(date=date,exit_profile=exit_id,path='charts/'+name,start=study.core.iso((left-EPOCH)*86400),end=study.core.iso((right-EPOCH)*86400),same_scales=True,previous_context_available=bool(x[0]<=previous_start+1/1440),next_formation_complete=bool(x[-1]>=next_start+15/1440)))
    for date in selected_dates:
        draw(date)
        draw(date,zoom=True)
        print('CHART',date,flush=True)
    for exit_id in ['exit_B','exit_C','exit_D']:
        draw('2026-09-02',exit_id)
        draw('2026-09-02',exit_id,True)
    (root/'chart_context_checks.json').write_text(json.dumps(contexts,indent=2))
    (root/'selected_chart_scope.json').write_text(json.dumps(dict(profiles=SELECTED,fixed_exit='exit_A',material_definition='entry presence differs, >=30 seconds entry-time difference or >=0.05W executed-price difference versus same-exit 450s control',sessions=selected_dates),indent=2))
    # Parameter surfaces: all 144 configurations retain their fixed exit identity.
    for family in 'ABCDE':
        profiles=[e for e in study.entry_profiles() if e['family']==family]
        for metric,label in [('net_pnl','Net trading P&L (USD)'),('profit_factor','Profit factor'),('flow_adjusted_account_drawdown','Funding-adjusted max drawdown (USD)'),('entry_distance_W_median','Median executed entry distance (W)')]:
            matrix=np.array([[next(r for r in results if r['entry_id']==e['entry_id'] and r['exit_id']==x)[metric] or 0 for x in ['exit_A','exit_B','exit_C','exit_D']] for e in profiles])
            fig,ax=plt.subplots(figsize=(9,max(3.5,len(profiles)*.52)))
            im=ax.imshow(matrix,aspect='auto',cmap='RdYlGn' if metric=='net_pnl' else 'viridis')
            ax.set_yticks(range(len(profiles)),[e['entry_id'] for e in profiles])
            ax.set_xticks(range(4),['A: baseline trail','B: edge target .75W','C: entry 1W trail','D: no trail/target'])
            ax.tick_params(axis='x',labelsize=8)
            for i in range(len(profiles)):
                for j in range(4):ax.text(j,i,f'{matrix[i,j]:.2f}',ha='center',va='center',fontsize=8)
            ax.set_title(f'Entry family {family} — {label}')
            fig.colorbar(im,ax=ax)
            fig.tight_layout();fig.savefig(charts/f'family_{family}_{metric}_heatmap.png',dpi=170);plt.close(fig)
        fig,axes=plt.subplots(2,2,figsize=(13,8))
        for ax,exit_id in zip(axes.flat,['exit_A','exit_B','exit_C','exit_D']):
            rows=[next(r for r in results if r['entry_id']==e['entry_id'] and r['exit_id']==exit_id) for e in profiles]
            ax.plot(range(len(rows)),[r['net_pnl'] for r in rows],marker='o')
            ax.set_xticks(range(len(rows)),[r['entry_id'] for r in rows],rotation=70,fontsize=7)
            ax.axhline(0,color='gray',lw=.7);ax.grid(alpha=.15)
            ax.set_title(exit_id);ax.set_ylabel('Net trading P&L (USD)')
        fig.suptitle(f'Family {family}: listed parameter order; no interpolation between discrete setups')
        fig.tight_layout();fig.savefig(charts/f'family_{family}_sensitivity.png',dpi=170);plt.close(fig)
    fig,axes=plt.subplots(2,2,figsize=(14,9))
    specs=[('entry_distance_W','Entry distance beyond A edge / W'),('confirmation_max_favorable','Favorable movement during confirmation (quote USD)'),('confirmation_max_retracement','Maximum peak-to-trough retracement during confirmation'),('unrestricted_mfe_before_deadline','Unrestricted favorable movement after entry before deadline')]
    for ax,(key,label) in zip(axes.flat,specs):
        ax.hist([r[key] for r in baseline],bins=12,color='#0369a1',edgecolor='white')
        ax.set_xlabel(label);ax.set_ylabel('Baseline entries');ax.grid(alpha=.1)
    fig.suptitle('All 31 original entries — descriptive distributions, not new entry filters')
    fig.tight_layout();fig.savefig(charts/'baseline_entry_distributions.png',dpi=180);plt.close(fig)
    fig,ax=plt.subplots(figsize=(11,5))
    ax.scatter([r['entry_distance_W'] for r in baseline],[r['mfe']/r['W'] for r in baseline],label='Actual baseline exposure',alpha=.8)
    ax.scatter([r['entry_distance_W'] for r in baseline],[r['structural_only_path_mfe']/r['W'] for r in baseline],label='Structural/capital/deadline-only path upper bound',marker='x')
    ax.scatter([r['entry_distance_W'] for r in baseline],[r['unrestricted_mfe_before_deadline']/r['W'] for r in baseline],label='Unrestricted path before deadline',marker='+')
    ax.set_xlabel('Executed entry distance from directional edge (W)');ax.set_ylabel('Favorable excursion after entry (W)')
    ax.legend(fontsize=8);ax.grid(alpha=.15);fig.tight_layout();fig.savefig(charts/'attainable_vs_unrestricted.png',dpi=180);plt.close(fig)
    # Downloadable per-configuration exports include closed and censored positions.
    folder=root/'trade_records';folder.mkdir(exist_ok=True)
    for cfg,rows in all_trades.items():study.core.write_csv(folder/(cfg+'.csv'),[{k:v for k,v in r.items() if k!='trail_movements'} for r in rows+opens.get(cfg,[])])
    page='''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>ACD Phase 4 entry research</title><style>body{font:15px system-ui;margin:24px;background:#f8fafc;color:#172033}main{max-width:2200px;margin:auto}img{width:100%;background:white}article{border-top:3px solid #94a3b8;margin:35px 0}table{border-collapse:collapse;font-size:12px;width:100%}td,th{padding:7px;border:1px solid #cbd5e1;text-align:left}details{margin:15px 0}input{padding:10px;width:250px}a{color:#0369a1}</style><main><h1>ACD Phase 4 — entry research</h1><p>Exploratory same-sample research. No deployment or selected optimum. Six entry profiles were specified before viewing performance. Default comparisons hold exit A fixed; trade 2 additionally shows all four exits.</p><p>Full frames preserve the complete previous session through the next formation. Panels share price/time scales. Actual last quote per second is displayed; full ticks drive decisions. Gaps over 60 seconds are blank. Cyan: first fresh breakout; gold: actual confirmation interval; black: original 450s entry; green: alternative entry; pink: retest events; green shaded band: permitted price zone; teal: position open; red: exit; purple: trail; orange: target/deadline. Click images for full resolution and expand zooms/tables for inspection.</p><p><a href="REPORT.md">Report</a> · <a href="BASELINE_ENTRY_FORENSICS.md">Baseline entry forensics</a> · <a href="TRADE2.md">Trade 2</a> · <a href="comparison.csv">All 144 configurations</a> · <a href="entry_research_charts_and_records.zip">Download archive</a></p><input id="filter" placeholder="Filter NY session date">'''
    def table(date,exit_id):
        content='<table><tr><th>Entry</th><th>Entry NY</th><th>Price</th><th>Distance W</th><th>Confirm sec</th><th>Exit NY</th><th>Exit price</th><th>Reason</th><th>Net USD</th><th>Top-up</th></tr>'
        for profile in SELECTED:
            r=records[profile+'__'+exit_id].get(date)
            values=[profile,'No executed entry'] if not r else [profile,ny(r['entry_timestamp']),f"{r['entry_price']:.2f}",f"{r['entry_distance_W']:.3f}",f"{r['actual_confirmation_seconds']:.3f}",ny(r['exit_timestamp']),r['exit_price'],r['exit_reason'],r['net_pnl'],r['replenishment']]
            content+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in values)+'</tr>'
        return content+'</table>'
    for date in selected_dates:
        path=f'charts/session_{date}_exit_A.png'
        page+=f'<article data-date="{date}"><h2>{date} — fixed exit A</h2><a href="{path}"><img src="{path}" loading="lazy"></a><details><summary>Detailed entry-process zoom</summary><img src="charts/zoom_{date}_exit_A.png" loading="lazy"></details><details><summary>Exact entries, exits and funding</summary>'+table(date,'exit_A')+'</details>'
        if date=='2026-09-02':
            for exit_id in ['exit_B','exit_C','exit_D']:
                page+=f'<details><summary>Trade 2: same entry families with {exit_id}</summary><img src="charts/session_{date}_{exit_id}.png" loading="lazy"><details><summary>Entry-process zoom</summary><img src="charts/zoom_{date}_{exit_id}.png" loading="lazy"></details>'+table(date,exit_id)+'</details>'
        page+='</article>'
    page+='</main><script>document.getElementById("filter").oninput=function(){document.querySelectorAll("article").forEach(a=>a.hidden=!a.dataset.date.includes(this.value.trim()))}</script>'
    (root/'gallery.html').write_text(page)
    (root/'VISUALS_COMPLETE').write_text('Material-session charts, zooms, surfaces and gallery complete')


if __name__=='__main__':main()
