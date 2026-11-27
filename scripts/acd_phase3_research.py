"""Isolated full-tick exit research; extraction never writes to the source DB."""
import argparse
import bisect
import csv
import hashlib
import json
import math
import struct
import sys
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path


def seconds(value):
    return datetime.fromisoformat(value).timestamp()


def iso(value):
    return datetime.fromtimestamp(float(value),timezone.utc).isoformat()


def write_csv(path,rows):
    rows=list(rows)
    keys=list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=keys or ['no_records'],lineterminator='\n')
        w.writeheader()
        for r in rows:
            w.writerow({k:json.dumps(v) if isinstance(v,(list,dict)) else v for k,v in r.items()})


def extract(args):
    from datavis.db import db_connection
    ref=Path(args.reference)
    root=Path(args.output)
    root.mkdir(parents=True,exist_ok=True)
    summary=json.loads((ref/'summary.json').read_text())
    days=json.loads((ref/'daily.json').read_text())
    starts=[seconds(d['session_start']) for d in days]
    streams=[(root/(d['date']+'.bin')).open('wb') for d in days]
    counts=Counter()
    previous=None
    digest=hashlib.sha256()
    with db_connection(readonly=True) as conn,conn.cursor(name='phase3_readonly_extract') as cur:
        cur.itersize=30000
        cur.execute('SELECT id,timestamp,bid,ask FROM public.ticks WHERE symbol=%s AND timestamp>=%s AND timestamp<=%s ORDER BY timestamp,id',('XAUUSD',datetime.fromisoformat(summary['warmup_start']),datetime.fromisoformat(summary['window_end'])))
        for ident,stamp,bid,ask in cur:
            counts['raw_ticks']+=1
            bid,ask=float(bid),float(ask)
            if not all(math.isfinite(v) and v>0 for v in (bid,ask)) or ask<bid:
                counts['invalid_quotes']+=1
                continue
            if previous is not None and stamp<=previous:
                counts['duplicate_timestamp_quotes_skipped']+=1
                continue
            epoch=stamp.timestamp()
            packed=struct.pack('<qqdd',ident,round(epoch*1e6),bid,ask)
            digest.update(packed)
            if epoch>=seconds(summary['window_start']):
                streams[bisect.bisect_right(starts,epoch)-1].write(packed)
                counts['window_ticks']+=1
            previous=stamp
            counts['processed_ticks']+=1
            if counts['processed_ticks']%1000000==0:
                print(json.dumps(dict(ticks=counts['processed_ticks'],timestamp=stamp.isoformat())),flush=True)
    for s in streams:s.close()
    assert dict(counts)==summary['counts'],(counts,summary['counts'])
    assert digest.hexdigest()==summary['dataset_sha256'],'Data stream changed'
    (root/'cache_manifest.json').write_text(json.dumps(dict(counts=dict(counts),dataset_sha256=digest.hexdigest(),days=days,reference=summary),indent=2))
    print('EXTRACT_COMPLETE',flush=True)


def configurations():
    configs=[dict(id='baseline',kind='trail',activation='fixed',multiple=3.,activation_ref='entry',trail_ref='edge')]
    widths=[.25,.5,.75,1.,1.25,1.5,2.,3.]
    for ref in ['entry','edge']:
        for m in widths:
            configs.append(dict(id=f'target_{ref}_{m:g}W',kind='target',multiple=m,activation_ref=ref,trail_ref=None))
        for trail in ['edge','entry']:
            for m in widths:
                configs.append(dict(id=f'trail_{ref}_{trail}_{m:g}W',kind='trail',activation='width',multiple=m,activation_ref=ref,trail_ref=trail))
            if not(ref=='entry' and trail=='edge'):
                configs.append(dict(id=f'trail_fixed3_{ref}_{trail}',kind='trail',activation='fixed',multiple=3.,activation_ref=ref,trail_ref=trail))
    configs += [dict(id='reentry_A',kind='pullback',policy='A'),dict(id='reentry_B',kind='pullback',policy='B'),dict(id='no_trailing',kind='none')]
    return configs


def first(mask):
    import numpy as np
    found=np.flatnonzero(mask)
    return int(found[0]) if len(found) else None


def confirmation(t,q,edge,sign,begin,end,initial_inside=False):
    """Causal fresh-inside observation, continuous hold and quote-gap reset."""
    import numpy as np
    n=len(t)
    indexes=np.arange(n)
    valid=(t>=begin)&(t<end)
    inside=(q-edge)*sign<=0
    gaps=np.r_[not initial_inside,np.diff(t)>60]
    # A gap erases arming; a same-tick inside observation can arm afresh.
    last_inside=np.maximum.accumulate(np.where(inside&valid,indexes,-1))
    if initial_inside:
        last_inside=np.maximum(last_inside,0)
    last_gap=np.maximum.accumulate(np.where(gaps,indexes,-1))
    armed=last_inside>=last_gap
    outside=~inside&valid
    begins=outside&(np.r_[True,~outside[:-1]]|gaps)
    run_start=np.maximum.accumulate(np.where(begins,indexes,0))
    elapsed=t-t[run_start]
    return outside&armed&(elapsed>=450)


def held_beyond(t,condition):
    import numpy as np
    idx=np.arange(len(t))
    begins=condition&(np.r_[True,~condition[:-1]]|np.r_[False,np.diff(t)>60])
    start=np.maximum.accumulate(np.where(begins,idx,0))
    return condition&((t-t[start])>=450)


def exit_path(t,q,entry,edge,opposite,sign,width,balance,deadline,cfg,initial_extreme=None):
    import numpy as np
    extreme=np.maximum.accumulate(q) if sign==1 else np.minimum.accumulate(q)
    if initial_extreme is not None:
        extreme=np.maximum(extreme,initial_extreme) if sign==1 else np.minimum(extreme,initial_extreme)
    pnl=(q-entry)*sign-.14
    capital=(balance+pnl)<=455
    timed=t>=deadline
    opposite_hit=held_beyond(t,(q-opposite)*sign<0)
    target=np.zeros(len(t),dtype=bool)
    trailing=np.zeros(len(t),dtype=bool)
    pullback=np.zeros(len(t),dtype=bool)
    levels=np.full(len(t),np.nan)
    active=np.zeros(len(t),dtype=bool)
    target_level=None
    if cfg['kind']=='target':
        anchor=entry if cfg['activation_ref']=='entry' else edge
        target_level=anchor+sign*cfg['multiple']*width
        target=(q-target_level)*sign>=0
    if cfg['kind']=='trail':
        anchor=entry if cfg['activation_ref']=='entry' else edge
        distance=cfg['multiple']*(width if cfg['activation']=='width' else 1.)
        active=(extreme-anchor)*sign>=distance if cfg['activation']=='width' else (extreme-anchor)*sign>distance
        trail_anchor=entry if cfg['trail_ref']=='entry' else edge
        levels=np.where(active,trail_anchor+.5*(extreme-trail_anchor),np.nan)
        trailing=active&((q-levels)*sign<=0)
    if cfg['kind']=='pullback':
        pullback=(q-edge)*sign<=0
    k=first(capital|timed|opposite_hit|target|trailing|pullback)
    if k is None:return None,dict(levels=levels,active=active,target=target_level)
    # Core precedence retained; target/pullback occupy the former trail slot.
    reason=next(name for name,mask in [('capital',capital),('timed',timed),('target',target),('trailing',trailing),('pullback',pullback),('opposite',opposite_hit)] if mask[k])
    return (k,reason),dict(levels=levels,active=active,target=target_level)


def replay_day(day,data,cfg,account):
    import numpy as np
    t=data['time'].astype(np.float64)/1e6
    bid,ask=data['bid'],data['ask']
    start,end=seconds(day['session_start']),seconds(day['session_end'])
    opening_end=start+900
    deadline=end-3600
    direction=day['yesterday_bias']
    if direction not in ['up','down'] or day['upper'] is None:return [],None
    upper,lower=day['upper'],day['lower']
    width=upper-lower
    sign=1 if direction=='up' else -1
    q=bid if sign==1 else ask
    opening=(t>=start)&(t<opening_end)
    times=t[opening]
    if not len(times) or times[0]-start>60 or opening_end-times[-1]>60 or np.any(np.diff(times)>60):return [],None
    edge=upper if sign==1 else lower
    opposite=lower-.25 if sign==1 else upper+.25
    cutoff=min(opening_end+7200,end-10800)
    confirms=confirmation(t,q,edge,sign,opening_end,cutoff)
    records=[]
    scan=0
    intraday_curve=np.full(len(t),account['balance'])
    for number in range(10000):
        if number and cfg['kind']!='pullback':break
        if number:
            cutoff=min(opening_end+7200,end-10800) if cfg['policy']=='A' else end-10800
            local=confirmation(t[scan:],q[scan:],edge,sign,t[scan],cutoff,initial_inside=bool((q[scan-1]-edge)*sign<=0 and t[scan]-t[scan-1]<=60))
            candidates=np.flatnonzero(local)+scan
        else:candidates=np.flatnonzero(confirms)
        candidates=candidates[candidates>=scan]
        if not len(candidates):break
        entry_quotes=(ask[candidates] if sign==1 else bid[candidates])+sign*.10
        risks=np.abs(entry_quotes-opposite)+.14+.10+5.
        fits=risks<account['balance']-455
        selected=first(fits)
        if selected is None:break
        i=int(candidates[selected])
        entry=float(entry_quotes[selected])
        # A newly opened position is not exited until a subsequent quote.
        j=i+1
        if j>=len(t):break
        result,details=exit_path(t[j:],q[j:],entry,edge,opposite,sign,width,account['balance'],deadline,cfg,initial_extreme=q[i])
        if result is None:
            raise RuntimeError(f"Unclosed position {day['date']} {cfg['id']}")
        offset,reason=result
        e=j+offset
        exit_price=float(q[e]-sign*.10)
        gross=(exit_price-entry)*sign
        net=gross-.14
        before=account['balance']+net
        topup=max(0.,500-before)
        tradeid=account['trades']+1
        account['trades']=tradeid
        account['net']+=net
        account['contributed']+=topup
        account['balance']=before+topup
        closing_q=q[i:e+1]
        excursion=(closing_q-entry)*sign
        marks=before-net+(closing_q-entry)*sign-.14
        # Match core entry-tick valuation convention, documented in Phase 2.
        marks[0]=before-net
        marks[-1]=before
        intraday_curve[i:e+1]=marks
        intraday_curve[e+1:]=account['balance']
        flow=np.r_[marks,before]-account['contributed']+topup+500
        account['flow_peak']=max(account['flow_peak'],float(np.max(flow)))
        running_peak=np.maximum.accumulate(np.r_[account['prior_flow_peak'],flow])[1:]
        account['drawdown']=max(account['drawdown'],float(np.max(running_peak-flow)))
        account['prior_flow_peak']=account['flow_peak']
        levels=details['levels'][:offset+1]
        movement_indexes=np.flatnonzero(np.isfinite(levels)&np.r_[True,np.diff(levels)!=0])
        trails=[dict(timestamp=iso(t[j+k]),level=float(levels[k])) for k in movement_indexes]
        activation=first(details['active'][:offset+1])
        target=details['target']
        record=dict(config=cfg['id'],trade_id=tradeid,date=day['date'],session_trade=number+1,side=direction,W=width,upper=upper,lower=lower,edge=edge,opposite=opposite,entry_timestamp=iso(t[i]),entry_price=entry,exit_timestamp=iso(t[e]),exit_price=exit_price,exit_reason=reason,gross_pnl=gross,net_pnl=net,commission=.14,slippage_cost=.2,spread_cost=float(ask[i]-bid[i]),holding_seconds=float(t[e]-t[i]),mfe=float(max(0,np.max(excursion))),mae=float(max(0,-np.min(excursion))),balance_before=before-net,balance_before_replenishment=before,replenishment=topup,balance_after=account['balance'],cumulative_pnl=account['net'],external_capital=account['contributed'],mandatory_deadline=iso(deadline),deadline_delay=max(0.,float(t[e]-deadline)),next_formation_violation=bool(t[e]>=end),trail_movements=trails,activation_timestamp=iso(t[j+activation]) if activation is not None else None,target_level=target,target_touched_before_entry=bool(target is not None and np.any((q[(t>=opening_end)&(t<t[i])]-target)*sign>=0)),target_beyond_at_entry=bool(target is not None and (q[i]-target)*sign>=0),returned_inside=bool(np.any((closing_q>=lower)&(closing_q<=upper))),lots=.01)
        records.append(record)
        record.update(previous_quote_before_exit=iso(t[e-1]),quote_gap_before_exit=float(t[e]-t[e-1]),deadline_gap_over60=bool(t[e]>deadline and t[e]-t[e-1]>60))
        account['contributions'].append(dict(timestamp=iso(t[e]),trade_id=tradeid,amount=topup,balance_before=before,balance_after=account['balance'])) if topup else None
        scan=e+1
        if reason=='timed':break
        if scan>=len(t):break
    if not records:return [],None
    peak=np.maximum.accumulate(intraday_curve)
    return records,float(np.max(peak-intraday_curve))


def metrics(cfg,account,trades,days):
    net=sum(t['net_pnl'] for t in trades)
    wins=sum(t['net_pnl']>0 for t in trades)
    gain=sum(max(0,t['net_pnl']) for t in trades)
    loss=-sum(min(0,t['net_pnl']) for t in trades)
    streak=maximum=0
    for t in trades:
        streak=streak+1 if t['net_pnl']<0 else 0
        maximum=max(maximum,streak)
    sessions=Counter(t['date'] for t in trades)
    contributions=account['contributed']-500
    session_pnl=Counter()
    for t in trades:session_pnl[t['date']]+=t['net_pnl']
    positive=sorted([v for v in session_pnl.values() if v>0],reverse=True)
    return dict(**cfg,net_pnl=net,profit_factor=gain/loss if loss else None,expectancy_trade=net/len(trades) if trades else None,expectancy_session=net/len(days),expectancy_entered_session=net/len(sessions) if sessions else None,win_rate=wins/len(trades) if trades else None,winners=wins,losers=sum(t['net_pnl']<0 for t in trades),trades=len(trades),sessions_with_entries=len(sessions),maximum_individual_loss=max([max(0,-t['net_pnl']) for t in trades] or [0]),maximum_intraday_drawdown=account['intraday_dd'],flow_adjusted_account_drawdown=account['drawdown'],maximum_consecutive_losses=maximum,capital_replenishments=contributions,replenishment_count=len(account['contributions'])-1,external_capital=account['contributed'],final_balance=account['balance'],average_holding_seconds=sum(t['holding_seconds'] for t in trades)/len(trades) if trades else None,commission=sum(t['commission'] for t in trades),slippage_cost=sum(t['slippage_cost'] for t in trades),entry_spread_cost=sum(t['spread_cost'] for t in trades),explicit_transaction_cost=sum(t['commission']+t['slippage_cost']+t['spread_cost'] for t in trades),exit_distribution=dict(Counter(t['exit_reason'] for t in trades)),deadline_violations=sum(t['deadline_delay']>0 for t in trades),next_formation_violations=sum(t['next_formation_violation'] for t in trades),pullback_exits=sum(t['exit_reason']=='pullback' for t in trades),repeated_false_breakouts=sum(t['session_trade']>1 and t['exit_reason']=='pullback' for t in trades),net_without_best_session=net-(positive[0] if positive else 0),net_without_best_two_sessions=net-sum(positive[:2]),largest_session_profit=positive[0] if positive else 0)


def run(args):
    import numpy as np
    cache=Path(args.cache)
    out=Path(args.output)
    out.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    days=manifest['days']
    configs=configurations()
    accounts={c['id']:dict(balance=500.,contributed=500.,net=0.,trades=0,contributions=[dict(timestamp=manifest['reference']['window_start'],trade_id=None,amount=500.,balance_before=0.,balance_after=500.)],flow_peak=500.,prior_flow_peak=500.,drawdown=0.,intraday_dd=0.) for c in configs}
    all_trades={c['id']:[] for c in configs}
    daily=[]
    dtype=np.dtype([('id','<i8'),('time','<i8'),('bid','<f8'),('ask','<f8')])
    source_checks=[]
    previous_day=None
    previous_data=None
    for day in days:
        data=np.memmap(cache/(day['date']+'.bin'),dtype=dtype,mode='r')
        epoch=data['time']/1e6
        opening=(epoch>=seconds(day['session_start']))&(epoch<seconds(day['session_start'])+900)
        if np.any(opening) and not day['partial_window']:
            mids=(data['bid'][opening]+data['ask'][opening])/2
            assert abs(float(np.max(mids))+.75-day['upper'])<1e-8
            assert abs(float(np.min(mids))-.75-day['lower'])<1e-8
        prior_match=None
        if previous_day is not None and not previous_day['partial_window']:
            prior_time=previous_data['time']/1e6
            forming=prior_time<seconds(previous_day['session_start'])+900
            gaps=np.diff(prior_time[forming])
            move=float(previous_data['bid'][-1]-previous_data['ask'][0])
            if (len(gaps) and np.max(gaps)>60) or seconds(previous_day['session_end'])-prior_time[-1]>60:
                move=0
            bias='up' if move>3 else 'down' if move< -3 else 'uncertain'
            prior_match=bias==day['yesterday_bias']
            assert prior_match,(day['date'],bias,day['yesterday_bias'])
        source_checks.append(dict(date=day['date'],formation_recomputed=bool(np.any(opening) and not day['partial_window']),prior_bias_recomputed=prior_match))
        for cfg in configs:
            a=accounts[cfg['id']]
            records,dd=replay_day(day,data,cfg,a)
            a['intraday_dd']=max(a['intraday_dd'],dd or 0)
            all_trades[cfg['id']].extend(records)
            daily.append(dict(config=cfg['id'],date=day['date'],previous_bias=day['yesterday_bias'],W=day['upper']-day['lower'] if day['upper'] is not None else None,trades=len(records),net_pnl=sum(r['net_pnl'] for r in records),replenishments=sum(r['replenishment'] for r in records),closing_balance=a['balance'],intraday_drawdown=dd or 0))
        print(day['date'],flush=True)
        previous_day,previous_data=day,data
    baseline=all_trades['baseline']
    expected=json.loads((Path(args.reference)/'trades.json').read_text())
    keys=['date','side','entry_price','exit_price','exit_reason','net_pnl']
    errors=[]
    if len(baseline)!=len(expected):errors.append(dict(counts=[len(baseline),len(expected)]))
    for a,b in zip(baseline,expected):
        differences={k:[a[k],b[k]] for k in keys if (abs(a[k]-b[k])>1e-8 if isinstance(a[k],float) else a[k]!=b[k])}
        for k in ['entry_timestamp','exit_timestamp']:
            if abs(seconds(a[k])-seconds(b[k]))>1e-5:differences[k]=[a[k],b[k]]
        if differences:errors.append(dict(date=a['date'],differences=differences))
    (out/'baseline_reproduction.json').write_text(json.dumps(dict(matches=not errors,errors=errors,compared_keys=keys+['entry_timestamp','exit_timestamp']),indent=2))
    (out/'causal_source_checks.json').write_text(json.dumps(source_checks,indent=2))
    # Always write diagnostics for review, but never approve failed reproduction.
    comparisons=[metrics(c,accounts[c['id']],all_trades[c['id']],days) for c in configs]
    (out/'configurations.json').write_text(json.dumps(configs,indent=2))
    (out/'comparison.json').write_text(json.dumps(comparisons,indent=2))
    write_csv(out/'comparison.csv',comparisons)
    write_csv(out/'daily.csv',daily)
    (out/'all_trades.json').write_text(json.dumps(all_trades))
    write_csv(out/'all_trades.csv',[{k:v for k,v in t.items() if k!='trail_movements'} for rows in all_trades.values() for t in rows])
    ledger=[dict(config=name,**row) for name,a in accounts.items() for row in a['contributions']]
    write_csv(out/'capital_contributions.csv',ledger)
    write_csv(out/'deadline_events.csv',[dict(config=cfg,date=t['date'],entry=t['entry_timestamp'],exit=t['exit_timestamp'],deadline=t['mandatory_deadline'],delay=t['deadline_delay'],previous_quote=t['previous_quote_before_exit'],quote_gap=t['quote_gap_before_exit'],gap_over60=t['deadline_gap_over60'],next_formation_violation=t['next_formation_violation']) for cfg,rows in all_trades.items() for t in rows if t['deadline_delay']>0])
    monthly=[]
    sides=[]
    for cfg in configs:
        trades=all_trades[cfg['id']]
        for month in sorted(set(d['date'][:7] for d in days)):
            rows=[t for t in trades if t['date'].startswith(month)]
            monthly.append(dict(config=cfg['id'],month=month,trades=len(rows),net_pnl=sum(t['net_pnl'] for t in rows),replenishments=sum(t['replenishment'] for t in rows)))
        for side in ['up','down']:
            rows=[t for t in trades if t['side']==side]
            sides.append(dict(config=cfg['id'],side=side,trades=len(rows),winners=sum(t['net_pnl']>0 for t in rows),net_pnl=sum(t['net_pnl'] for t in rows)))
    write_csv(out/'monthly.csv',monthly)
    write_csv(out/'long_short.csv',sides)
    (out/'dataset.json').write_text(json.dumps({k:v for k,v in manifest.items() if k!='days'},indent=2))
    if errors:raise RuntimeError(f'Baseline reproduction failed: {errors[:3]}')
    (out/'COMPLETE').write_text('Baseline matches; all configurations completed')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=['extract','run'])
    p.add_argument('--reference',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--cache')
    args=p.parse_args()
    (extract if args.mode=='extract' else run)(args)
