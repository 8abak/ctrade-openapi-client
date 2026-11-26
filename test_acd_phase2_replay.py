"""Telemetry must not change the strategy; clarified-rule discrepancy fixtures."""
import tempfile
import sys
from datetime import date, timedelta
from pathlib import Path

# Test the committed source snapshot, including from a clean research checkout.
sys.path.insert(0,str(Path(__file__).resolve().parent/'docs/acd_continuation/phase2_2026-11-27/frozen_source'))

from datavis.acd_continuation import Config, PaperRobot
from datavis.hlrtacd.acd import acd_session_for_date
from datavis.hlrtacd.models import Tick
from scripts.acd_phase2_replay import AuditRobot, month_before


def quotes():
    start = acd_session_for_date(date(2026,11,24)).start
    rows = [(s,100.) for s in range(0,900,30)]
    rows += [(900,100.),(901,102.),(903,102.),(904,101.),(905,99.),(906,100.),(907,106.),(908,103.)]
    return [Tick(i+1,start+timedelta(seconds=s),p,p+.2,p+.1) for i,(s,p) in enumerate(rows)]


def test_audit_exact_state_parity_per_tick():
    c = Config(hold_seconds=2)
    with tempfile.TemporaryDirectory() as tmp:
        reference = PaperRobot(tmp+'/reference.sqlite',c)
        audit = AuditRobot(tmp+'/audit.sqlite',c)
        try:
            for t in quotes():
                if t.timestamp >= acd_session_for_date(date(2026,11,24)).opening_end:
                    reference.s['previous_move'] = audit.s['previous_move'] = 'up'
                audit.observe_position(t)
                audit._process(t,None)
                audit.s['last'] = t.timestamp.isoformat()
                reference.process(t)
                assert audit.s == reference.s
            assert len(audit.trades)==1
            assert audit.trades[0]['returned_inside_acd']
            assert audit.trades[0]['recovered_after_retracement']
            assert audit.trades[0]['exit_reason']=='trailing'
        finally:
            reference.close()
            audit.close()


def test_inside_range_does_not_invalidate_and_opposite_break_waits():
    c = Config(hold_seconds=2)
    with tempfile.TemporaryDirectory() as tmp:
        bot = PaperRobot(tmp+'/paper.sqlite',c)
        try:
            for t in quotes()[:34]:
                if t.timestamp >= acd_session_for_date(date(2026,11,24)).opening_end:
                    bot.s['previous_move']='up'
                bot.process(t)
            assert bot.s['position'] is not None  # inside ACD retracement
            start = acd_session_for_date(date(2026,11,24)).start
            # One opposite boundary break does NOT immediately invalidate.
            bot.process(Tick(100,start+timedelta(seconds=905),98.,98.2,98.1))
            assert bot.s['position'] is not None
            bot.process(Tick(101,start+timedelta(seconds=907),98.,98.2,98.1))
            assert bot.s['position'] is None
        finally:
            bot.close()


def test_calendar_month_window_handles_short_month():
    from datetime import datetime, timezone
    assert month_before(datetime(2026,5,31,tzinfo=timezone.utc)).date()==date(2026,2,28)


def test_partial_previous_formation_can_produce_definite_bias_defect():
    c = Config(hold_seconds=2)
    session = acd_session_for_date(date(2026,11,24))
    with tempfile.TemporaryDirectory() as tmp:
        bot = PaperRobot(tmp+'/paper.sqlite',c)
        try:
            # Missing the first ten minutes of the prior opening range.
            rows = [(session.start+timedelta(seconds=s),100.) for s in range(600,900,30)]
            rows += [(session.end-timedelta(seconds=1),110.),(session.end,111.)]
            for i,(stamp,p) in enumerate(rows,1):
                bot.process(Tick(i,stamp,p,p+.2,p+.1))
            assert bot.s['previous_move']=='up'  # defect reproduction, not approval
        finally:
            bot.close()


def test_entry_quote_equity_is_not_marked_until_following_tick_defect():
    c = Config(hold_seconds=2)
    with tempfile.TemporaryDirectory() as tmp:
        bot = PaperRobot(tmp+'/paper.sqlite',c)
        try:
            for t in quotes()[:33]:
                if t.timestamp >= acd_session_for_date(date(2026,11,24)).opening_end:
                    bot.s['previous_move']='up'
                bot.process(t)
            assert bot.s['position'] is not None
            assert bot.s['equity']==500.  # spread/slippage/commission not yet marked
            assert bot.s['position']['entry']>102.
        finally:
            bot.close()
