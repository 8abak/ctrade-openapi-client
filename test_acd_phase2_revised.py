"""Closure-only contributions and the unchanged absolute risk threshold."""
import tempfile
from datetime import date, timedelta

from test_acd_phase2_replay import quotes
from datavis.acd_continuation import Config
from datavis.hlrtacd.acd import acd_session_for_date
from datavis.hlrtacd.models import Tick
from scripts.acd_phase2_revised_replay import ReplenishedAuditRobot


def entered(path,prior_profit=0):
    c=Config(hold_seconds=2)
    start=acd_session_for_date(date(2026,11,24)).start
    bot=ReplenishedAuditRobot(path,c,start.isoformat())
    bot.s['realized']=prior_profit
    bot.trading_pnl=prior_profit
    for t in quotes()[:33]:
        if t.timestamp>=acd_session_for_date(date(2026,11,24)).opening_end:
            bot.s['previous_move']='up'
        bot.process(t)
    assert bot.s['position'] is not None
    return bot,start


def tick(bot,start,seconds,bid):
    bot.observe_position(Tick(seconds,start+timedelta(seconds=seconds),bid,bid+.2,bid+.1))
    bot.process(Tick(seconds,start+timedelta(seconds=seconds),bid,bid+.2,bid+.1))


def test_no_open_position_replenishment():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger')
        try:
            tick(bot,start,904,100)
            assert bot.s['position'] is not None
            assert bot.s['equity']<500
            assert bot.total_topups==0
            assert len(bot.contributions)==1
        finally:
            bot.close()


def test_capital_exit_fully_closes_then_tops_up_and_clears_latch():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger')
        try:
            tick(bot,start,904,40)
            trade=bot.trades[0]
            assert bot.s['position'] is None
            assert trade['exit_reason']=='capital'
            assert trade['balance_before_replenishment']<455
            assert abs(bot.s['equity']-500)<1e-9
            assert not bot.s['latched']
            assert abs(bot.trading_pnl+bot.total_topups)<1e-8
            assert bot.trading_pnl<0
            assert bot.contributions[-1]['trade_id']==trade['trade_id']
            assert bot.contributions[-1]['timestamp']==trade['exit_timestamp']
        finally:
            bot.close()


def test_reset_does_not_rearm_used_session():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger')
        try:
            tick(bot,start,904,40)
            tick(bot,start,905,100)
            tick(bot,start,906,102)
            tick(bot,start,909,102)
            assert bot.s['position'] is None
            assert len(bot.trades)==1
            assert len(bot.s['used'])==1
        finally:
            bot.close()


def test_profits_retained_without_contribution():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger')
        try:
            tick(bot,start,904,106)
            tick(bot,start,905,103)
            assert bot.s['position'] is None
            assert bot.s['equity']>500
            assert bot.total_topups==0
            assert bot.trades[-1]['replenishment']==0
            assert abs(bot.s['equity']-500-bot.trading_pnl)<1e-8
        finally:
            bot.close()


def test_absolute455_threshold_is_not_balance_minus45():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger',prior_profit=20)
        try:
            tick(bot,start,904,50)
            assert bot.s['position'] is not None
            assert 455<bot.s['equity']<475  # loss exceeds45, absolute floor not hit
            tick(bot,start,905,37)
            assert bot.s['position'] is None
            assert bot.trades[-1]['exit_reason']=='capital'
            assert bot.s['equity']==500
            assert abs(500+bot.total_topups+bot.trading_pnl-bot.s['equity'])<1e-8
        finally:
            bot.close()


def test_missing_quotes_do_not_create_fictitious_deadline_fill():
    with tempfile.TemporaryDirectory() as tmp:
        bot,start=entered(tmp+'/ledger')
        try:
            next_start=acd_session_for_date(date(2026,11,24)).end
            actual_quote=next_start+timedelta(seconds=5)
            bot.process(Tick(1000,actual_quote,102.,102.2,102.1))
            trade=bot.trades[-1]
            assert trade['exit_reason']=='timed'
            assert trade['exit_timestamp']==actual_quote.isoformat()
            assert trade['open_into_next_formation']
            assert trade['deadline_delay_seconds']==3605
            assert trade['deadline_violation_cause']=='closure_or_missing_quotes_unverified'
            assert bot.s['position'] is None
        finally:
            bot.close()
