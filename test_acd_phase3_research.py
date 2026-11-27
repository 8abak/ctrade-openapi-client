import numpy as np
import pytest
from scripts.acd_phase3_research import confirmation,held_beyond,exit_path,configurations,replay_day,seconds


def test_grid_has_every_requested_reference_and_threshold():
    configs=configurations()
    assert len(configs)==55
    assert len({c['id'] for c in configs})==55
    assert sum(c['kind']=='target' for c in configs)==16
    assert sum(c.get('activation')=='width' for c in configs)==32
    assert sum(c.get('activation')=='fixed' for c in configs)==4


def test_fresh_inside_and450_second_causal_hold():
    t=np.arange(0.,502.)
    q=np.full(len(t),101.)
    q[0]=99.
    mask=confirmation(t,q,100.,1,0,502)
    assert np.flatnonzero(mask)[0]==451
    for cut in [20,200,450,499]:
        assert np.array_equal(mask[:cut],confirmation(t[:cut],q[:cut],100,1,0,502))
    assert not confirmation(t,np.full(len(t),101.),100,1,0,502).any()


def test_gap_clears_arming_until_new_inside_observation():
    t=np.r_[np.arange(0.,201.),np.arange(300.,1000.)]
    q=np.full(len(t),101.)
    q[0]=99
    assert not confirmation(t,q,100,1,0,1100).any()
    q[np.flatnonzero(t==400)[0]]=99
    assert t[np.flatnonzero(confirmation(t,q,100,1,0,1100))[0]]==851


def test_opposite_hold_resets_on_gaps_and_retracement():
    t=np.r_[np.arange(0.,201.),np.arange(300.,800.)]
    assert t[np.flatnonzero(held_beyond(t,np.ones(len(t),dtype=bool)))[0]]==750


@pytest.mark.parametrize('sign',[1,-1])
@pytest.mark.parametrize('trail_ref',['entry','edge'])
def test_activation_and_half_formula_are_separate(sign,trail_ref):
    cfg=dict(kind='trail',activation='width',multiple=1.,activation_ref='entry',trail_ref=trail_ref)
    t=np.array([1.,2.,3.,4.])
    q=100+sign*np.array([2.,4.,6.,2.])
    edge=100-sign*10
    outcome,details=exit_path(t,q,100,edge,100-sign*30,sign,5,500,1000,cfg)
    assert details['active'].tolist()==[False,False,True,True]
    assert details['levels'][2]==((100 if trail_ref=='entry' else edge)+.5*(q[2]-(100 if trail_ref=='entry' else edge)))
    if trail_ref=='entry':assert outcome==(3,'trailing')


def test_target_uses_actual_available_quote_not_prior_target_price():
    result,details=exit_path(np.array([1.,2.]),np.array([115.,116.]),113.,100.,80.,1,10.,500.,1000.,dict(kind='target',multiple=1.,activation_ref='edge'))
    assert result==(0,'target')
    assert details['target']==110


def test_already_reached_target_exits_next_quote_even_after_pullback():
    cfg=dict(kind='target',multiple=1.,activation_ref='edge')
    result,_=exit_path(np.array([1.,2.]),np.array([109.,115.]),113.,100.,80.,1,10.,500.,1000.,cfg,initial_extreme=114.)
    assert result==(0,'target')


def test_capital_and_deadline_override_other_exit_types():
    cfg=dict(kind='pullback')
    assert exit_path(np.array([100.]),np.array([40.]),100.,90.,80.,1,10.,500.,100.,cfg)[0]==(0,'capital')
    assert exit_path(np.array([100.]),np.array([89.]),100.,90.,80.,1,10.,500.,100.,cfg)[0]==(0,'timed')


def test_vector_exits_match_independent_tick_loop_across_every_configuration():
    rng=np.random.default_rng(20261127)
    for sign in [1,-1]:
        for cfg in configurations():
            for trial in range(5):
                t=np.cumsum(rng.choice([1.,10.,80.],size=150,p=[.7,.28,.02]))
                q=100+np.cumsum(rng.normal(0,2,150))
                entry,edge,opposite=100.,100-sign*10,100-sign*20
                expected=None
                extreme=100.
                trail=None
                opposite_since=None
                previous=None
                for k,(stamp,quote) in enumerate(zip(t,q)):
                    if previous is not None and stamp-previous>60:opposite_since=None
                    previous=stamp
                    extreme=max(extreme,quote) if sign==1 else min(extreme,quote)
                    if cfg['kind']=='trail':
                        anchor=entry if cfg['activation_ref']=='entry' else edge
                        distance=cfg['multiple']*(15 if cfg['activation']=='width' else 1)
                        activate=(extreme-anchor)*sign>=distance if cfg['activation']=='width' else (extreme-anchor)*sign>distance
                        if activate:
                            anchor=entry if cfg['trail_ref']=='entry' else edge
                            candidate=anchor+.5*(extreme-anchor)
                            trail=candidate if trail is None else max(trail,candidate) if sign==1 else min(trail,candidate)
                    beyond=(quote-opposite)*sign<0
                    opposite_since=(stamp if opposite_since is None else opposite_since) if beyond else None
                    target_hit=False
                    if cfg['kind']=='target':
                        anchor=entry if cfg['activation_ref']=='entry' else edge
                        target_level=anchor+sign*cfg['multiple']*15
                        target_hit=(quote-target_level)*sign>=0 or (100-target_level)*sign>=0
                    reason=None
                    if 500+(quote-entry)*sign-.14<=455:reason='capital'
                    elif stamp>=500:reason='timed'
                    elif target_hit:reason='target'
                    elif trail is not None and (quote-trail)*sign<=0:reason='trailing'
                    elif cfg['kind']=='pullback' and (quote-edge)*sign<=0:reason='pullback'
                    elif opposite_since is not None and stamp-opposite_since>=450:reason='opposite'
                    if reason:
                        expected=(k,reason)
                        break
                actual=exit_path(t,q,entry,edge,opposite,sign,15,500,500,cfg,initial_extreme=100)[0]
                assert actual==expected,(cfg,sign,trial,actual,expected)


def test_confirmation_matches_scalar_causal_loop_with_reentry_and_gaps():
    rng=np.random.default_rng(31)
    for initially_inside in [False,True]:
        for sign in [1,-1]:
            t=np.cumsum(rng.choice([1.,5.,80.],size=2000,p=[.7,.29,.01]))
            q=100+np.cumsum(rng.normal(0,.2,2000))
            armed=initially_inside
            since=None
            previous=None
            expected=[]
            for stamp,quote in zip(t,q):
                if (previous is None and not initially_inside) or (previous is not None and stamp-previous>60):
                    armed=False
                    since=None
                previous=stamp
                if not 0<=stamp<5000:
                    expected.append(False)
                    continue
                if (quote-100)*sign<=0:
                    armed=True
                    since=None
                    expected.append(False)
                elif armed:
                    since=stamp if since is None else since
                    expected.append(stamp-since>=450)
                else:expected.append(False)
            assert np.array_equal(confirmation(t,q,100,sign,0,5000,initial_inside=initially_inside),expected)


def test_reentry_can_resume_after_closed_capital_exit_and_topup():
    day=dict(date='2026-11-24',session_start='2026-11-24T14:30:00+00:00',session_end='2026-11-25T14:30:00+00:00',yesterday_bias='up',upper=100.,lower=99.)
    t=np.arange(3000.)
    q=np.full(len(t),99.5)
    q[901:1352]=101.
    q[1352]=50.
    q[1354:1805]=101.
    data=np.zeros(len(t),dtype=[('time','i8'),('bid','f8'),('ask','f8')])
    data['time']=np.rint((seconds(day['session_start'])+t)*1e6).astype('i8')
    data['bid'],data['ask']=q,q+.2
    account=dict(balance=500.,contributed=500.,net=0.,trades=0,contributions=[{}],flow_peak=500.,prior_flow_peak=500.,drawdown=0.)
    records,_=replay_day(day,data,dict(id='reentry_A',kind='pullback',policy='A'),account)
    assert len(records)==2
    assert records[0]['exit_reason']=='capital'
    assert records[0]['replenishment']>0
    assert records[0]['balance_after']==500
    assert records[1]['session_trade']==2
    assert seconds(records[1]['entry_timestamp'])-seconds(records[0]['exit_timestamp'])>=450
