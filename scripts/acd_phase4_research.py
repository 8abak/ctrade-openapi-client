"""Causal entry-family research over the frozen Phase 3 full-tick foundation."""
import argparse
import inspect
import json
import sys
import textwrap
from collections import Counter
from pathlib import Path

import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
import acd_phase3_research as core

# Reuse the verified settlement/exit engine, changing only its signal-mask input.
_source=textwrap.dedent(inspect.getsource(core.replay_day))
_old='confirms=confirmation(t,q,edge,sign,opening_end,cutoff)'
assert _source.count(_old)==1
_namespace=dict(vars(core))
_source=_source.replace(_old,"confirms=cfg['_entry_mask']")
_source=_source.replace("if j>=len(t):break","if j>=len(t):return _censor(day,data,cfg,account,t,q,i,entry,edge,opposite,sign,width,deadline)")
_source=_source.replace("raise RuntimeError(f\"Unclosed position {day['date']} {cfg['id']}\")","return _censor(day,data,cfg,account,t,q,i,entry,edge,opposite,sign,width,deadline)")
exec(compile(_source,'<phase4-entry-mask-hook>','exec'),_namespace)
replay_with_mask=_namespace['replay_day']


def censor(day,data,cfg,account,t,q,i,entry,edge,opposite,sign,width,deadline):
    """Right-censor at the fixed dataset boundary, never invent a closing fill."""
    path=q[i:]
    marks=account['balance']+(path-entry)*sign-.14
    marks[0]=account['balance']
    flow=marks-account['contributed']+500
    peak=np.maximum.accumulate(np.r_[account['prior_flow_peak'],flow])[1:]
    account['drawdown']=max(account['drawdown'],float(np.max(peak-flow)))
    account['open_equity']=float(marks[-1])
    account['open_pnl']=float(marks[-1]-account['balance'])
    record=dict(config=cfg['id'],trade_id=account['trades']+1,date=day['date'],session_trade=1,side=day['yesterday_bias'],W=width,upper=day['upper'],lower=day['lower'],edge=edge,opposite=opposite,entry_timestamp=core.iso(t[i]),entry_price=entry,exit_timestamp=None,exit_price=None,exit_reason='open_at_data_cutoff',gross_pnl=None,net_pnl=None,commission=0.,slippage_cost=.1,spread_cost=float(data['ask'][i]-data['bid'][i]),holding_seconds=float(t[-1]-t[i]),mfe=float(max(0,np.max((path-entry)*sign))),mae=float(max(0,-np.min((path-entry)*sign))),balance_before=account['balance'],balance_before_replenishment=None,replenishment=0.,balance_after=account['balance'],mandatory_deadline=core.iso(deadline),deadline_delay=None,next_formation_violation=False,trail_movements=[],activation_timestamp=None,target_level=None,target_touched_before_entry=False,target_beyond_at_entry=False,lots=.01,last_quote_timestamp=core.iso(t[-1]),last_quote=float(q[-1]),unrealized_pnl=account['open_pnl'],mark_to_market_equity=account['open_equity'])
    cfg['_open_positions'].append(record)
    intraday=np.r_[np.full(i,account['balance']),marks]
    return [],float(np.max(np.maximum.accumulate(intraday)-intraday))


_namespace['_censor']=censor


def entry_profiles():
    rows=[]
    for hold in [0,15,30,60,120,225,450,900]:rows.append(dict(entry_id=f'A_{hold}s',family='A',hold=hold))
    for distance in [.05,.10,.15,.25,.50]:rows.append(dict(entry_id=f'B_{distance:g}W',family='B',distance=distance))
    for excursion in [.10,.25]:
        for hold in [0,30,60,120]:rows.append(dict(entry_id=f'C_{excursion:g}W_{hold}s',family='C',excursion=excursion,hold=hold))
    for hold in [60,225,450]:
        for zone in [.10,.25,.50]:rows.append(dict(entry_id=f'D_{hold}s_{zone:g}W',family='D',hold=hold,zone=zone))
    for hold in [30,60,120]:
        for distance in [.10,.25]:rows.append(dict(entry_id=f'E_{hold}s_{distance:g}W',family='E',hold=hold,distance=distance))
    return rows


def exit_profiles():
    return [dict(exit_id='exit_A',kind='trail',activation='fixed',multiple=3.,activation_ref='entry',trail_ref='edge'),dict(exit_id='exit_B',kind='target',multiple=.75,activation_ref='edge',trail_ref=None),dict(exit_id='exit_C',kind='trail',activation='width',multiple=1.,activation_ref='entry',trail_ref='entry'),dict(exit_id='exit_D',kind='none')]


def market_frame(day,data):
    t=data['time']/1e6
    start,end=core.seconds(day['session_start']),core.seconds(day['session_end'])
    sign=1 if day['yesterday_bias']=='up' else -1
    q=data['bid'] if sign==1 else data['ask']
    edge=day['upper'] if sign==1 else day['lower']
    W=day['upper']-day['lower'] if day['upper'] is not None else 0
    base=dict(t=t,q=q,sign=sign,edge=edge,W=W,start=start,end=end,valid=False,reason='yesterday_uncertain')
    if day['yesterday_bias'] not in ['up','down'] or edge is None:return base
    forming=(t>=start)&(t<start+900)
    ft=t[forming]
    if not len(ft) or ft[0]-start>60 or start+900-ft[-1]>60 or np.any(np.diff(ft)>60):
        base['reason']='incomplete_formation'
        return base
    begin,cutoff=start+900,min(start+900+7200,end-10800)
    valid=(t>=begin)&(t<cutoff)
    inside=(q-edge)*sign<=0
    outside=~inside&valid
    gap=np.r_[True,np.diff(t)>60]
    idx=np.arange(len(t))
    last_inside=np.maximum.accumulate(np.where(inside&valid,idx,-1))
    last_gap=np.maximum.accumulate(np.where(gap,idx,-1))
    armed=last_inside>=last_gap
    begins=outside&(np.r_[True,~outside[:-1]]|gap)
    runs=np.maximum.accumulate(np.where(begins,idx,0))
    elapsed=t-t[runs]
    favorable=(q-edge)*sign
    peak=favorable.copy()
    starts=np.flatnonzero(begins)
    for a,b in zip(starts,np.r_[starts[1:],len(t)]):peak[a:b]=np.maximum.accumulate(favorable[a:b])
    entry=(data['ask'] if sign==1 else data['bid'])+sign*.1
    opposite=day['lower']-.25 if sign==1 else day['upper']+.25
    invalidation=core.held_beyond(t,(q-opposite)*sign<0)
    first_break=core.first(outside&armed)
    base.update(valid=True,reason=None,window=valid,outside=outside,inside=inside,gap=gap,armed=armed,runs=runs,elapsed=elapsed,favorable=favorable,peak=peak,entry=entry,opposite=opposite,first_break=first_break,invalidation=invalidation)
    return base


def retest_state(frame,excursion):
    t,q=frame['t'],frame['q']
    n=len(t)
    since=np.full(n,np.nan)
    excursion_index=np.full(n,-1,dtype='i8')
    retest_index=np.full(n,-1,dtype='i8')
    first_run=np.full(n,-1,dtype='i8')
    stage=0
    armed=False
    run=None
    initial=None
    reached=returned=None
    events=[]
    for i in np.flatnonzero(frame['window']):
        if frame['gap'][i]:
            if stage:events.append(dict(timestamp=core.iso(t[i]),event='gap_cancel'))
            stage=0;armed=False;run=None;initial=None
        if frame['invalidation'][i]:
            if stage:events.append(dict(timestamp=core.iso(t[i]),event='opposite_cancel'))
            stage=0;run=None;initial=None
        if frame['inside'][i]:
            armed=True
            run=None
            if stage==1:
                stage=2;returned=i
                events.append(dict(timestamp=core.iso(t[i]),event='retest',price=float(q[i])))
            continue
        if not armed:continue
        if run is None:
            run=i
            if stage==0:initial=i
        if stage==0 and frame['favorable'][i]>=excursion*frame['W']:
            stage=1;reached=i
            events.append(dict(timestamp=core.iso(t[i]),event='initial_excursion',price=float(q[i])))
        if stage==2:
            since[i]=t[run]
            excursion_index[i]=reached
            retest_index[i]=returned
            first_run[i]=initial
    return dict(since=since,excursion_index=excursion_index,retest_index=retest_index,first_run=first_run,events=events)


def entry_mask(profile,frame,retests=None):
    n=len(frame['t'])
    if not frame['valid']:return np.zeros(n,dtype=bool),dict(reason=frame['reason'])
    base=frame['outside']&frame['armed']
    family=profile['family']
    if family=='C':
        state=retests[profile['excursion']]
        mask=np.isfinite(state['since'])&((frame['t']-state['since'])>=profile['hold'])
        return mask,dict(reason=None,retest_events=state['events'])
    timed=base&(frame['elapsed']>=profile.get('hold',0))
    if family=='A':mask=timed
    elif family=='B':mask=base&(frame['favorable']>=profile['distance']*frame['W'])
    elif family=='D':mask=timed&((frame['entry']-frame['edge'])*frame['sign']<=profile['zone']*frame['W'])
    elif family=='E':mask=timed&(frame['peak']>=profile['distance']*frame['W'])
    else:raise ValueError(family)
    reason='price_zone_unavailable' if family=='D' and timed.any() and not mask.any() else None
    return mask,dict(reason=reason,time_qualified=bool(timed.any()))


def path_quality(frame,day,entry_index,end_index,entry_price):
    t,q=frame['t'],frame['q']
    sign,W,edge=frame['sign'],frame['W'],frame['edge']
    stop=min(end_index,len(t))
    qq,tt=q[entry_index:stop],t[entry_index:stop]
    inside=np.flatnonzero((qq>=day['lower'])&(qq<=day['upper']))
    raw_opp=np.flatnonzero((qq-(day['lower'] if sign==1 else day['upper']))*sign<0)
    held_opp=np.flatnonzero(core.held_beyond(tt,(qq-frame['opposite'])*sign<0))
    first_inside=int(inside[0]) if len(inside) else None
    out=dict(first_return_inside_timestamp=core.iso(tt[first_inside]) if first_inside is not None else None,first_return_inside_price=float(qq[first_inside]) if first_inside is not None else None,raw_opposite_timestamp=core.iso(tt[raw_opp[0]]) if len(raw_opp) else None,held_opposite_timestamp=core.iso(tt[held_opp[0]]) if len(held_opp) else None,unrestricted_mfe_before_deadline=float(max(0,np.max((qq-entry_price)*sign))) if len(qq) else 0.,unrestricted_mae_before_deadline=float(max(0,-np.min((qq-entry_price)*sign))) if len(qq) else 0.)
    for multiple in [.5,.75,1.,1.5,2.,3.]:
        hits=np.flatnonzero((qq-edge)*sign>=multiple*W)
        k=int(hits[0]) if len(hits) else None
        tag=f'{multiple:g}W'
        out['reached_'+tag]=k is not None
        out['first_hit_'+tag]=core.iso(tt[k]) if k is not None else None
        out['false_before_'+tag]=bool(first_inside is not None and (k is None or first_inside<k))
        out['opposite_before_'+tag]=bool(len(raw_opp) and (k is None or raw_opp[0]<k))
    return out


def enrich(record,profile,frame,day,retests):
    t,q=frame['t'],frame['q']
    i=int(np.searchsorted(t,core.seconds(record['entry_timestamp'])))
    end=int(np.searchsorted(t,core.seconds(record['mandatory_deadline']),side='right'))
    begin=int(frame['runs'][i])
    extra={}
    if profile['family']=='C':
        state=retests[profile['excursion']]
        begin=int(np.searchsorted(t,state['since'][i]))
        extra=dict(initial_excursion_timestamp=core.iso(t[state['excursion_index'][i]]),retest_timestamp=core.iso(t[state['retest_index'][i]]),initial_breakout_timestamp=core.iso(t[state['first_run'][i]]))
    sign=frame['sign']
    path=(q[begin:i+1]-q[begin])*sign
    restriction=(record['entry_price']-frame['edge'])*sign
    record.update(entry_family=profile['family'],entry_profile=profile['entry_id'],entry_run_start=core.iso(t[begin]),actual_confirmation_seconds=float(t[i]-t[begin]),first_breakout_timestamp=core.iso(t[frame['first_break']]) if frame['first_break'] is not None else None,first_breakout_price=float(q[frame['first_break']]) if frame['first_break'] is not None else None,entry_distance=restriction,entry_distance_W=restriction/frame['W'],structural_stop_distance=abs(record['entry_price']-frame['opposite']),confirmation_max_favorable=float(max(0,np.max(path))),confirmation_max_retracement=float(max(0,np.max(np.maximum.accumulate(path)-path))),confirmation_max_edge_excursion=float(np.max((q[begin:i+1]-frame['edge'])*sign)),confirmation_gap_seconds=float(np.max(np.diff(t[begin:i+1]))) if i>begin else 0.,remaining_edge_target_0p75W=(frame['edge']+sign*.75*frame['W']-record['entry_price'])*sign,**extra)
    cache=frame.setdefault('quality_cache',{})
    cache_key=(i,end,record['entry_price'])
    if cache_key not in cache:
        cache[cache_key]=path_quality(frame,day,i,end,record['entry_price'])
        cache[cache_key]['unrestricted_mfe_until_next_formation']=float(max(0,np.max((q[i:]-record['entry_price'])*sign)))
    record.update(cache[cache_key])
    record['hindsight_reward_to_structural_risk']=record['unrestricted_mfe_before_deadline']/record['structural_stop_distance']
    # Full-session diagnostic is separately labelled; never used for decisions.


def account(initial):
    return dict(balance=500.,contributed=500.,net=0.,trades=0,contributions=[dict(timestamp=initial,trade_id=None,amount=500.,balance_before=0.,balance_after=500.)],flow_peak=500.,prior_flow_peak=500.,drawdown=0.,intraday_dd=0.)


def run(args):
    cache,root,ref=Path(args.cache),Path(args.output),Path(args.reference)
    root.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    entries,exits=entry_profiles(),exit_profiles()
    configs=[dict(id=e['entry_id']+'__'+x['exit_id'],**e,**x) for e in entries for x in exits]
    accounts={c['id']:account(manifest['reference']['window_start']) for c in configs}
    trades={c['id']:[] for c in configs}
    pending={c['id']:[] for c in configs}
    days=manifest['days']
    daily=[]
    baseline_forensics=[]
    dtype=np.dtype([('id','<i8'),('time','<i8'),('bid','<f8'),('ask','<f8')])
    baseline_ref=json.loads((ref/'trades.json').read_text())
    baseline_dates={r['date']:r for r in baseline_ref}
    for day in days:
        data=np.memmap(cache/(day['date']+'.bin'),dtype=dtype,mode='r')
        frame=market_frame(day,data)
        retests={m:retest_state(frame,m) for m in [.10,.25]} if frame['valid'] else {}
        prepared={e['entry_id']:entry_mask(e,frame,retests) for e in entries}
        for cfg in configs:
            a=accounts[cfg['id']]
            mask,diagnostic=prepared[cfg['entry_id']]
            candidates=np.flatnonzero(mask)
            eligible=bool(len(candidates))
            eligible500=False
            actual_admission=False
            if eligible:
                risk=np.abs(frame['entry'][candidates]-frame['opposite'])+.14+.10+5
                eligible500=bool(np.any(risk<45))
                actual_admission=bool(np.any(risk<a['balance']-455))
            was_open=len(pending[cfg['id']])
            records,dd=replay_with_mask(day,data,dict(cfg,_entry_mask=mask,_open_positions=pending[cfg['id']]),a)
            newly_open=pending[cfg['id']][was_open:]
            a['intraday_dd']=max(a['intraday_dd'],dd or 0.)
            for r in records+newly_open:
                enrich(r,cfg,frame,day,retests)
                r['exit_profile']=cfg['exit_id']
            trades[cfg['id']].extend(records)
            reason=None if records or newly_open else diagnostic.get('reason') or ('risk_admission_rejected' if eligible else 'no_qualifying_entry')
            if cfg['family']=='C' and not records and not newly_open and frame['valid']:
                events=diagnostic.get('retest_events',[])
                if any(x['event']=='opposite_cancel' for x in events):reason='pending_retest_opposite_cancel_or_no_later_setup'
            daily.append(dict(config=cfg['id'],entry_profile=cfg['entry_id'],exit_profile=cfg['exit_id'],date=day['date'],previous_bias=day['yesterday_bias'],eligible=eligible,eligible_with_fixed500=eligible500,risk_admission_eligible=actual_admission,trades=len(records)+len(newly_open),open_at_cutoff=len(newly_open),reason=reason,net_pnl=sum(r['net_pnl'] for r in records),replenishments=sum(r['replenishment'] for r in records),closing_balance=a['balance'],candidate_count=len(candidates),first_candidate=core.iso(frame['t'][candidates[0]]) if eligible else None,retest_opposite_cancels=sum(e['event']=='opposite_cancel' for e in diagnostic.get('retest_events',[]))))
            if cfg['id']=='A_450s__exit_A' and records:
                r=dict(records[0])
                entry=int(np.searchsorted(frame['t'],core.seconds(r['entry_timestamp'])))
                end=int(np.searchsorted(frame['t'],core.seconds(r['mandatory_deadline']),side='right'))
                first=frame['first_break']
                r['thresholds_since_first_breakout']=path_quality(frame,day,first,end,r['entry_price'])
                result,_=core.exit_path(frame['t'][entry+1:],frame['q'][entry+1:],r['entry_price'],r['edge'],r['opposite'],frame['sign'],r['W'],r['balance_before'],core.seconds(r['mandatory_deadline']),dict(kind='none'),initial_extreme=frame['q'][entry])
                k=entry+1+result[0]
                r['structural_only_exit_timestamp']=core.iso(frame['t'][k])
                r['structural_only_exit_reason']=result[1]
                r['structural_only_path_mfe']=float(max(0,np.max((frame['q'][entry:k+1]-r['entry_price'])*frame['sign'])))
                r['structural_only_path_reward_risk']=r['structural_only_path_mfe']/r['structural_stop_distance']
                baseline_forensics.append(r)
        print(day['date'],flush=True)
    observed=trades['A_450s__exit_A']
    errors=[]
    if len(observed)!=len(baseline_ref):errors.append('trade_count')
    for a,b in zip(observed,baseline_ref):
        for key in ['date','side','entry_price','exit_price','exit_reason','net_pnl']:
            if (abs(a[key]-b[key])>1e-8 if isinstance(a[key],float) else a[key]!=b[key]):errors.append([a['date'],key,a[key],b[key]])
        for key in ['entry_timestamp','exit_timestamp']:
            if core.seconds(a[key])!=core.seconds(b[key]):errors.append([a['date'],key])
    (root/'baseline_reproduction.json').write_text(json.dumps(dict(matches=not errors,errors=errors),indent=2))
    assert not errors,errors[:5]
    comparisons=[]
    for cfg in configs:
        rows=trades[cfg['id']]
        r=core.metrics(cfg,accounts[cfg['id']],rows,days)
        r.update(closed_trades=len(rows),open_at_cutoff=len(pending[cfg['id']]),executed_trades_including_open=len(rows)+len(pending[cfg['id']]),final_equity=accounts[cfg['id']].get('open_equity',accounts[cfg['id']]['balance']),unrealized_pnl=accounts[cfg['id']].get('open_pnl',0.),net_mark_to_market=r['net_pnl']+accounts[cfg['id']].get('open_pnl',0.))
        diagnostics=[d for d in daily if d['config']==cfg['id']]
        distances=[t['entry_distance_W'] for t in rows]
        r.update(eligible_sessions=sum(d['eligible'] for d in diagnostics),risk_eligible_sessions=sum(d['risk_admission_eligible'] for d in diagnostics),rejected_eligible_sessions=sum(d['eligible'] and not d['trades'] for d in diagnostics),entry_distance_W_median=float(np.median(distances)) if distances else None,entry_distance_W_p10=float(np.quantile(distances,.1)) if distances else None,entry_distance_W_p90=float(np.quantile(distances,.9)) if distances else None,mean_confirmation_seconds=float(np.mean([t['actual_confirmation_seconds'] for t in rows])) if rows else None,median_confirmation_seconds=float(np.median([t['actual_confirmation_seconds'] for t in rows])) if rows else None,maximum_confirmation_seconds=max([t['actual_confirmation_seconds'] for t in rows] or [0]),maximum_holding_seconds=max([t['holding_seconds'] for t in rows] or [0]),median_holding_seconds=float(np.median([t['holding_seconds'] for t in rows])) if rows else None,target_already_reached_trades=sum(t['target_beyond_at_entry'] for t in rows),target_already_reached_net=sum(t['net_pnl'] for t in rows if t['target_beyond_at_entry']),target_touched_before_entry_trades=sum(t['target_touched_before_entry'] for t in rows),target_touched_before_entry_net=sum(t['net_pnl'] for t in rows if t['target_touched_before_entry']))
        for m in [.5,.75,1.,1.5,2.,3.]:r[f'false_breakouts_before_{m:g}W']=sum(t[f'false_before_{m:g}W'] for t in rows)
        comparisons.append(r)
    (root/'comparison.json').write_text(json.dumps(comparisons,indent=2))
    (root/'all_trades.json').write_text(json.dumps(trades))
    (root/'open_positions.json').write_text(json.dumps(pending))
    (root/'configurations.json').write_text(json.dumps(configs,indent=2))
    (root/'baseline_forensics.json').write_text(json.dumps(baseline_forensics,indent=2))
    core.write_csv(root/'baseline_forensics.csv',[{k:v for k,v in r.items() if k not in ['trail_movements','thresholds_since_first_breakout']} for r in baseline_forensics])
    core.write_csv(root/'comparison.csv',comparisons)
    core.write_csv(root/'daily.csv',daily)
    core.write_csv(root/'all_trades.csv',[{k:v for k,v in r.items() if k!='trail_movements'} for cfg,rows in trades.items() for r in rows+pending[cfg]])
    core.write_csv(root/'capital_contributions.csv',[dict(config=cfg,**r) for cfg,a in accounts.items() for r in a['contributions']])
    core.write_csv(root/'deadline_events.csv',[dict(config=cfg,date=r['date'],deadline=r['mandatory_deadline'],exit=r['exit_timestamp'],delay=r['deadline_delay'],gap=r['quote_gap_before_exit']) for cfg,rows in trades.items() for r in rows if r['deadline_delay']>0])
    matched=[]
    for cfg,rows in trades.items():
        exit_id=cfg.split('__')[1]
        base={r['date']:r for r in trades['A_450s__'+exit_id]}
        alternative={r['date']:r for r in rows}
        for date in sorted(set(base)|set(alternative)):
            a,b=alternative.get(date),base.get(date)
            matched.append(dict(config=cfg,date=date,status='matched' if a and b else 'additional' if a else 'missed',net_delta=(a['net_pnl'] if a else 0)-(b['net_pnl'] if b else 0),entry_price_improvement=(b['entry_price']-a['entry_price'])*(1 if a['side']=='up' else -1) if a and b else None,entry_distance_W_delta=a['entry_distance_W']-b['entry_distance_W'] if a and b else None,structural_stop_distance_delta=a['structural_stop_distance']-b['structural_stop_distance'] if a and b else None,remaining_target_distance_delta=a['remaining_edge_target_0p75W']-b['remaining_edge_target_0p75W'] if a and b else None,confirmation_seconds_delta=a['actual_confirmation_seconds']-b['actual_confirmation_seconds'] if a and b else None))
    core.write_csv(root/'matched_sessions.csv',matched)
    (root/'dataset.json').write_text(json.dumps({k:v for k,v in manifest.items() if k!='days'},indent=2))
    core.verify(argparse.Namespace(output=str(root),cache=str(cache)))
    (root/'COMPLETE').write_text('Baseline reproduces; 144 entry/exit configurations complete and executions independently verified')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cache',required=True)
    p.add_argument('--reference',required=True)
    p.add_argument('--output',required=True)
    run(p.parse_args())
