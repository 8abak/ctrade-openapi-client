"""Post-replay market-path and matched-session decomposition; never entry inputs."""
import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0,str(Path(__file__).resolve().parent))
import acd_phase4_research as study


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory')
    p.add_argument('--cache',required=True)
    args=p.parse_args()
    root,cache=Path(args.directory),Path(args.cache)
    manifest=json.loads((cache/'cache_manifest.json').read_text())
    trades=json.loads((root/'all_trades.json').read_text())
    configs=json.loads((root/'configurations.json').read_text())
    for rows in trades.values():
        for r in rows:
            r['entry_risk_estimate']=abs(r['entry_price']-r['opposite'])+.14+.10+5.
            r['available_entry_risk_budget']=r['balance_before']-455.
            r['entry_admissible_at500']=bool(r['entry_risk_estimate']<45.)
            for multiple in [.5,.75,1.,1.5,2.,3.]:
                tag=f'{multiple:g}W'
                consumed=r['confirmation_max_edge_excursion']>=multiple*r['W']
                r['threshold_reached_during_confirmation_'+tag]=bool(consumed)
                r['episode_false_before_'+tag]=bool(r['false_before_'+tag] and not consumed)
    with (root/'daily.csv').open(newline='') as f:daily=list(csv.DictReader(f))
    with (root/'matched_sessions.csv').open(newline='') as f:matched=list(csv.DictReader(f))
    paths=[]
    dtype=np.dtype([('id','<i8'),('time','<i8'),('bid','<f8'),('ask','<f8')])
    for day in manifest['days']:
        data=np.memmap(cache/(day['date']+'.bin'),dtype=dtype,mode='r')
        frame=study.market_frame(day,data)
        row=dict(date=day['date'],previous_bias=day['yesterday_bias'],has_fresh_aligned_breakout=False,reason=frame['reason'])
        if frame['valid'] and frame['first_break'] is not None:
            i=frame['first_break']
            end=int(np.searchsorted(frame['t'],frame['end']-3600,side='right'))
            row.update(has_fresh_aligned_breakout=True,first_breakout_timestamp=study.core.iso(frame['t'][i]),first_breakout_quote=float(frame['q'][i]),W=frame['W'],edge=frame['edge'],**study.path_quality(frame,day,i,end,float(frame['q'][i])))
        paths.append(row)
    path_map={r['date']:r for r in paths}
    study.core.write_csv(root/'market_opportunities.csv',paths)
    quality=[]
    for cfg in configs:
        name=cfg['id']
        m=[r for r in matched if r['config']==name]
        d=[r for r in daily if r['config']==name]
        both=[r for r in m if r['status']=='matched']
        avg=lambda key:float(np.mean([float(r[key]) for r in both])) if both else None
        row=dict(config=name,matched_sessions=len(both),additional_sessions=sum(r['status']=='additional' for r in m),missed_control_sessions=sum(r['status']=='missed' for r in m),matched_net_delta=sum(float(r['net_delta']) for r in both),additional_net=sum(float(r['net_delta']) for r in m if r['status']=='additional'),missed_control_net_delta=sum(float(r['net_delta']) for r in m if r['status']=='missed'),mean_matched_entry_price_improvement=avg('entry_price_improvement'),mean_matched_entry_distance_W_delta=avg('entry_distance_W_delta'),mean_matched_stop_distance_delta=avg('structural_stop_distance_delta'),mean_matched_remaining_target_distance_delta=avg('remaining_target_distance_delta'),mean_matched_confirmation_seconds_delta=avg('confirmation_seconds_delta'),admission_changed_by_retained_balance=sum(r['risk_admission_eligible']!=r['eligible_with_fixed500'] for r in d),eligible_but_risk_rejected=sum(r['eligible']=='True' and r['risk_admission_eligible']=='False' for r in d))
        row['executed_entries_requiring_retained_profits']=sum(not r['entry_admissible_at500'] for r in trades[name])
        for level in [.5,.75,1.,1.5,2.,3.]:
            tag=f'{level:g}W'
            row['untraded_market_followthrough_'+tag]=sum(r['trades']=='0' and path_map[r['date']].get('reached_'+tag,False) and not path_map[r['date']].get('opposite_before_'+tag,False) for r in d)
        quality.append(row)
    study.core.write_csv(root/'opportunity_quality.csv',quality)
    (root/'opportunity_quality.json').write_text(json.dumps(quality,indent=2))
    comparisons=json.loads((root/'comparison.json').read_text())
    false_rows=[]
    for r in comparisons:
        rows=trades[r['id']]
        r['executed_entries_requiring_retained_profits']=sum(not t['entry_admissible_at500'] for t in rows)
        for multiple in [.5,.75,1.,1.5,2.,3.]:
            tag=f'{multiple:g}W'
            episode=sum(t['episode_false_before_'+tag] for t in rows)
            consumed=sum(t['threshold_reached_during_confirmation_'+tag] for t in rows)
            r['episode_false_breakouts_before_'+tag]=episode
            r['threshold_consumed_during_confirmation_'+tag]=consumed
            false_rows.append(dict(config=r['id'],threshold=tag,trades=len(rows),episode_false_breakouts=episode,post_execution_false_breakouts=r['false_breakouts_before_'+tag],threshold_reached_before_or_at_entry=consumed))
    (root/'all_trades.json').write_text(json.dumps(trades))
    (root/'comparison.json').write_text(json.dumps(comparisons,indent=2))
    study.core.write_csv(root/'comparison.csv',comparisons)
    study.core.write_csv(root/'all_trades.csv',[{k:v for k,v in r.items() if k!='trail_movements'} for rows in trades.values() for r in rows])
    study.core.write_csv(root/'false_breakout_statistics.csv',false_rows)
    # Flatten baseline threshold diagnostics so every requested observation is in CSV.
    baseline=json.loads((root/'baseline_forensics.json').read_text())
    flattened=[]
    for r in baseline:
        row={k:v for k,v in r.items() if k not in ['trail_movements','thresholds_since_first_breakout']}
        for k,v in r['thresholds_since_first_breakout'].items():row['since_first_breakout_'+k]=v
        flattened.append(row)
    study.core.write_csv(root/'baseline_forensics.csv',flattened)
    (root/'OPPORTUNITIES_COMPLETE').write_text('Post-hoc diagnostics separated from causal account replay')


if __name__=='__main__':main()
