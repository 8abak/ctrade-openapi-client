import numpy as np
from scripts import acd_phase4_research as study


def fixture(q=None,t=None):
    t=np.arange(2000.) if t is None else t
    q=np.full(len(t),99.5) if q is None else q
    data=np.zeros(len(t),dtype=[('id','i8'),('time','i8'),('bid','f8'),('ask','f8')])
    data['time']=np.rint((1764028800+t)*1e6).astype('i8')
    data['bid'],data['ask']=q,q+.2
    day=dict(date='2025-11-25',session_start=study.core.iso(1764028800),session_end=study.core.iso(1764028800+86400),yesterday_bias='up',upper=100.,lower=90.)
    return day,data


def mask(profile,q,t=None):
    day,data=fixture(q,t)
    frame=study.market_frame(day,data)
    retests={m:study.retest_state(frame,m) for m in [.1,.25]}
    return study.entry_mask(profile,frame,retests)[0],frame


def test_grid_exactly36_entries_and144_pairs():
    rows=study.entry_profiles()
    assert len(rows)==len({r['entry_id'] for r in rows})==36
    assert {f:sum(r['family']==f for r in rows) for f in 'ABCDE'}==dict(A=8,B=5,C=8,D=9,E=6)
    assert len(rows)*len(study.exit_profiles())==144


def test_zero_time_requires_inside_then_actual_outside_quote():
    q=np.full(2000,101.)
    observed,_=mask(dict(family='A',hold=0),q)
    assert not observed.any()
    q[920]=99.5
    observed,_=mask(dict(family='A',hold=0),q)
    assert np.flatnonzero(observed)[0]==921


def test450_mask_matches_original_control():
    q=np.full(2000,99.5)
    q[901:]=101.
    actual,frame=mask(dict(family='A',hold=450),q)
    expected=study.core.confirmation(frame['t'],frame['q'],100,1,frame['start']+900,frame['start']+8100)
    assert np.array_equal(actual,expected)
    assert np.flatnonzero(actual)[0]==1351


def test_distance_has_no_extra_time_hold():
    q=np.full(2000,99.5)
    q[901:]=100.2
    q[902:]=101.1
    actual,_=mask(dict(family='B',distance=.1),q)
    assert np.flatnonzero(actual)[0]==902


def test_hybrid_remembers_prior_touch_until_inside_recross():
    q=np.full(2000,99.5)
    q[901:]=100.5
    q[902]=102.
    actual,_=mask(dict(family='E',hold=60,distance=.1),q)
    assert np.flatnonzero(actual)[0]==961
    q[940]=99.5
    actual,_=mask(dict(family='E',hold=60,distance=.1),q)
    assert not actual.any()


def test_price_zone_waits_for_actual_achievable_fill_and_resets():
    q=np.full(2000,99.5)
    q[901:]=104.
    q[969:]=101.5
    actual,_=mask(dict(family='D',hold=60,zone=.25),q)
    assert np.flatnonzero(actual)[0]==969
    q[967]=99.5
    actual,_=mask(dict(family='D',hold=60,zone=.25),q)
    assert np.flatnonzero(actual)[0]==1028


def test_retest_requires_excursion_return_and_second_confirmed_break():
    q=np.full(2000,99.5)
    q[901:910]=101.1
    q[920:]=100.2
    actual,_=mask(dict(family='C',excursion=.1,hold=30),q)
    assert np.flatnonzero(actual)[0]==950
    q[901:910]=100.5
    actual,_=mask(dict(family='C',excursion=.1,hold=30),q)
    assert not actual.any()


def test_retest_opposite_hold_cancels_before_entry():
    q=np.full(2000,99.5)
    q[901:910]=101.1
    q[920:930]=100.2
    q[930:1385]=80.
    q[1385:]=101.1
    actual,frame=mask(dict(family='C',excursion=.1,hold=120),q)
    assert not actual.any()
    events=study.retest_state(frame,.1)['events']
    assert any(e['event']=='opposite_cancel' for e in events)


def test_all_entry_families_are_prefix_causal():
    q=np.full(2000,99.5)
    q[901:910]=103.
    q[920:1300]=101.2
    q[1350:]=104.
    for profile in study.entry_profiles():
        full,_=mask(profile,q)
        for cut in [950,1100,1450]:
            prefix,_=mask(profile,q[:cut],np.arange(float(cut)))
            assert np.array_equal(full[:cut],prefix),profile


def test_censored_position_has_no_fake_exit_or_open_topup():
    q=np.full(2000,99.5)
    q[901:]=101.
    day,data=fixture(q)
    frame=study.market_frame(day,data)
    profile=dict(family='A',hold=0)
    entry_mask=study.entry_mask(profile,frame,{})[0]
    a=study.account(day['session_start'])
    pending=[]
    records,_=study.replay_with_mask(day,data,dict(id='test',kind='none',_entry_mask=entry_mask,_open_positions=pending),a)
    assert not records and len(pending)==1
    assert pending[0]['exit_timestamp'] is None
    assert pending[0]['exit_price'] is None
    assert pending[0]['replenishment']==0
    assert len(a['contributions'])==1


def test_long_short_entry_masks_are_symmetric_for_every_profile():
    q=np.full(2000,99.5)
    q[901:910]=103.
    q[920:1300]=101.2
    q[1350:]=104.
    day,data=fixture(q)
    up=study.market_frame(day,data)
    short=data.copy()
    short['bid'],short['ask']=200-data['ask'],200-data['bid']
    down=study.market_frame(dict(day,yesterday_bias='down',upper=110.,lower=100.),short)
    for profile in study.entry_profiles():
        up_retests={m:study.retest_state(up,m) for m in [.1,.25]}
        down_retests={m:study.retest_state(down,m) for m in [.1,.25]}
        assert np.array_equal(study.entry_mask(profile,up,up_retests)[0],study.entry_mask(profile,down,down_retests)[0]),profile


def test_price_restriction_never_fills_after_original_window_expires():
    q=np.full(10000,99.5)
    q[7900:]=104.
    q[8101:]=100.5
    actual,frame=mask(dict(family='D',hold=60,zone=.1),q,np.arange(10000.))
    assert not actual.any()
    assert not frame['window'][8100:].any()
