"""Revised historical funding adapter; never opens the active service ledger.

Full bid/ask ticks drive the unchanged price/entry/exit core. The sole strategy
delta is replenishment and latch reset after full closure. The original account
is replayed independently on the same fixed tick window as a control.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import gzip
import json
import math
import sys
import struct
import tempfile
import time
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Fresh research checkouts can use the committed, hash-matched frozen engine.
# EC2 explicitly supplies its deployed source through PYTHONPATH instead.
import importlib.util
try:
    _strategy_spec = importlib.util.find_spec('datavis.acd_continuation')
except ModuleNotFoundError:
    _strategy_spec = None
if _strategy_spec is None:
    _bundle = Path(__file__).resolve().parents[1]/'docs/acd_continuation/phase2_2026-11-27/frozen_source'
    sys.path.insert(0,str(_bundle))
    # find_spec may already have loaded the repository's parent package.
    import datavis
    datavis.__path__.insert(0,str(_bundle/'datavis'))

import datavis.acd_continuation as strategy
from datavis.acd_continuation import Config, PaperRobot
from datavis.db import db_connection
from datavis.hlrtacd.acd import acd_session_for_date, acd_session_for_timestamp
from datavis.hlrtacd.models import Tick


def month_before(value, months=3):
    n = value.year*12+value.month-1-months
    year, month = divmod(n,12)
    return value.replace(year=year,month=month+1,day=min(value.day,calendar.monthrange(year,month+1)[1]))


def dump_csv(path, rows):
    rows = list(rows)
    fields = list(dict.fromkeys(k for row in rows for k in row))
    with open(path,'w',newline='',encoding='utf-8') as stream:
        writer = csv.DictWriter(stream,fieldnames=fields or ['no_records'])
        writer.writeheader()
        for row in rows:
            writer.writerow({k:json.dumps(v) if isinstance(v,(dict,list)) else v for k,v in row.items()})


class AuditRobot(PaperRobot):
    def __init__(self,path,config):
        super().__init__(path,config)
        self.trades = []
        self.current_trade = None
        self.observation = None
        self.trail_moves = []
        self.risk_events = []

    def event(self,t,kind,data):
        c = self.config
        if kind == 'ENTRY':
            p = data
            levels = dict(self.acd.levels)
            self.current_trade = dict(trade_id=len(self.trades)+1,date=self.s['session'],side=p['side'],entry_timestamp=t.timestamp.isoformat(),entry_price=p['entry'],entry_executable_quote=t.ask if p['sign']==1 else t.bid,lots=p['lots'],units=p['lots']*c.ounces_per_lot,upper=levels['aUp'],lower=levels['aDown'],opposite_exit=p['opposite'],yesterday_bias=self.s['previous_move'],today_direction=p['side'],decision_timestamp=t.timestamp.isoformat(),mae_quote=0.,mfe_quote=0.,returned_inside_acd=False,recovered_after_retracement=False,opposite_boundary_broken=False,buffered_opposite_broken=False,opposite_first_break=None,trailing_movements=[],exit_timestamp=None,net_pnl=None)
        elif kind == 'CLOSE':
            p = data['position']
            record = self.current_trade
            if record is None:
                raise RuntimeError('Close without audit entry')
            executable = t.bid if p['sign']==1 else t.ask
            exit_price = executable-p['sign']*c.slippage
            gross = (exit_price-p['entry'])*p['sign']*record['units']
            record.update(exit_timestamp=t.timestamp.isoformat(),exit_price=exit_price,exit_executable_quote=executable,exit_reason=data['reason'],gross_pnl=gross,net_pnl=data['net'],charges=gross-data['net'],holding_seconds=(t.timestamp-datetime.fromisoformat(p['opened'])).total_seconds(),exit_equity=self.s['equity'])
            self.trades.append(record)
            self.current_trade = None

    def observe_position(self,t):
        p, r = self.s['position'], self.current_trade
        if not p or r is None:
            return
        q = t.bid if p['sign']==1 else t.ask
        excursion = (q-p['entry'])*p['sign']
        if -excursion > r['mae_quote']:
            r['mae_timestamp'],r['mae_price'] = t.timestamp.isoformat(),q
        if excursion > r['mfe_quote']:
            r['mfe_timestamp'],r['mfe_price'] = t.timestamp.isoformat(),q
        r['mae_quote'] = max(r['mae_quote'],-excursion)
        r['mfe_quote'] = max(r['mfe_quote'],excursion)
        inside = r['lower'] <= q <= r['upper']
        if inside and not r['returned_inside_acd']:
            r['first_retracement_timestamp'],r['first_retracement_price'] = t.timestamp.isoformat(),q
        r['returned_inside_acd'] |= inside
        if r['returned_inside_acd'] and (q-p['edge'])*p['sign'] > 0:
            if not r['recovered_after_retracement']:
                r['first_recovery_timestamp'],r['first_recovery_price'] = t.timestamp.isoformat(),q
            r['recovered_after_retracement'] = True
        opposite = q < r['lower'] if p['sign']==1 else q > r['upper']
        r['opposite_boundary_broken'] |= opposite
        r['buffered_opposite_broken'] |= (q-p['opposite'])*p['sign'] < 0
        if opposite and r['opposite_first_break'] is None:
            r['opposite_first_break'] = t.timestamp.isoformat()


class ReplenishedAuditRobot(AuditRobot):
    """Only money-management delta: top up CLOSED balances, then clear latch.

    The core's fixed USD455 trigger and all price/entry/exit logic are unchanged.
    Its legacy `realized` field is used as a cash offset, including contributions;
    actual trading P&L is kept separately and never includes those contributions.
    """
    def __init__(self,path,config,initial_timestamp):
        super().__init__(path,config)
        self.contributions = [dict(sequence=1,timestamp=initial_timestamp,kind='initial',trade_id=None,amount=500.,balance_before=0.,balance_after=500.,total_contributed=500.)]
        self.total_topups = 0.
        self.trading_pnl = 0.
        self.previous_context = None
        self.rejected_candidates = {}
        self.account_events = [dict(timestamp=initial_timestamp,kind='initial',balance=500.,trading_pnl=0.,contributions=500.)]

    def event(self,t,kind,data):
        super().event(t,kind,data)
        if kind=='ENTRY':
            session = self.acd.session
            next_session = acd_session_for_timestamp(session.end)
            self.current_trade.update(previous_context=self.previous_context,formation_start=session.start.isoformat(),formation_end=session.opening_end.isoformat(),next_formation_start=session.end.isoformat(),next_formation_end=next_session.opening_end.isoformat(),mandatory_deadline=data['deadline'],confirmation_start=self.since[data['side']].isoformat(),balance_at_entry=500+self.s['realized'])
        elif kind=='CLOSE':
            r = self.trades[-1]
            deadline = datetime.fromisoformat(r['mandatory_deadline'])
            next_start = datetime.fromisoformat(r['next_formation_start'])
            late = max(0.,(t.timestamp-deadline).total_seconds())
            previous = self.before_tick_timestamp
            r.update(deadline_delay_seconds=late,open_into_next_formation=t.timestamp>=next_start,previous_quote_before_exit=previous.isoformat() if previous else None,quote_gap_before_exit_seconds=(t.timestamp-previous).total_seconds() if previous else None,deadline_violation_cause=('closure_or_missing_quotes_unverified' if previous and (t.timestamp-previous).total_seconds()>self.config.quote_gap_seconds else 'next_available_quote') if late else None)

    def _process(self,t,tradable_until):
        held = self.s['position'] is not None
        self.before_tick_timestamp = self.previous
        super()._process(t,tradable_until)
        if held and self.s['position'] is None:
            # The core has recorded the fill and cleared the position. Only now
            # may external money be added and the risk latch be reset.
            r = self.trades[-1]
            before = self.s['equity']
            self.trading_pnl += r['net_pnl']
            amount = max(0.,500.-before)
            self.account_events.append(dict(timestamp=t.timestamp.isoformat(),kind='trade_closed_before_replenishment',balance=before,trading_pnl=self.trading_pnl,contributions=500+self.total_topups,trade_id=r['trade_id']))
            if amount:
                self.total_topups += amount
                self.s['realized'] += amount
                self.s['equity'] += amount
                self.contributions.append(dict(sequence=len(self.contributions)+1,timestamp=t.timestamp.isoformat(),kind='replenishment',trade_id=r['trade_id'],amount=amount,balance_before=before,balance_after=self.s['equity'],total_contributed=500+self.total_topups))
                self.account_events.append(dict(timestamp=t.timestamp.isoformat(),kind='external_replenishment',balance=self.s['equity'],trading_pnl=self.trading_pnl,contributions=500+self.total_topups,trade_id=r['trade_id']))
            if self.s['latched']:
                self.risk_events.append(dict(timestamp=t.timestamp.isoformat(),equity_before_replenishment=before,threshold=450+self.config.liquidation_buffer,trade_id=r['trade_id'],reset_after_full_closure=True))
            self.s['latched'] = False
            r.update(balance_before_replenishment=before,replenishment=amount,balance_after_replenishment=self.s['equity'],cumulative_trading_pnl=self.trading_pnl,cumulative_external_contributions=500+self.total_topups)


def new_day(session,prior_equity):
    return dict(date=session.session_date.isoformat(),session_start=session.start.isoformat(),session_end=session.end.isoformat(),tick_count=0,first_quote=None,last_quote=None,quote_gaps_over_60s=0,max_quote_gap_seconds=0.,yesterday_bias='uncertain',preceding_observed_session=None,calendar_predecessor=None,today_direction='uncertain',decision_timestamp=None,upper=None,lower=None,continuation_satisfied=False,entry_signal_confirmed=False,eligible_before_risk=False,eligible_executable=False,trade_executed=False,trade_count=0,no_trade_reason=None,reason_counts={},opening_equity=prior_equity,closing_equity=prior_equity,net_pnl=0.,incomplete_session=False,partial_window=False)


def classify_reason(robot,t,session,levels,formed,cutoff=None):
    s,c = robot.s,robot.config
    if s['position']:
        return 'position_open'
    if s['latched']:
        return 'risk_shutdown_latched'
    if s['paused']:
        return 'paused'
    if s['session'] in s['used']:
        return 'day_allowance_used'
    if t.timestamp < session.opening_end:
        return 'acd_forming'
    if not levels or not formed:
        return 'incomplete_formation_data'
    cutoff = cutoff or min(session.opening_end+timedelta(minutes=c.entry_minutes),session.end-timedelta(hours=c.late_hours))
    if t.timestamp >= cutoff:
        return 'entry_window_closed'
    if s['previous_move']=='uncertain':
        return 'yesterday_uncertain'
    side = s['previous_move']
    if (side=='up' and t.bid <= levels['aUp']) or (side=='down' and t.ask >= levels['aDown']):
        return 'no_aligned_breakout'
    if not robot.armed[side]:
        return 'fresh_inside_observation_required'
    since = robot.since[side]
    if since is None or (t.timestamp-since).total_seconds()<c.hold_seconds:
        return 'confirmation_incomplete'
    return 'risk_estimate_exceeds_budget'


def summarize(trades,daily,drawdown,risk_events,end_equity):
    closed = [t for t in trades if t['exit_timestamp']]
    wins = [t for t in closed if t['net_pnl']>0]
    losses = [t for t in closed if t['net_pnl']<0]
    gp,gl = sum(t['net_pnl'] for t in wins),-sum(t['net_pnl'] for t in losses)
    longest = streak = 0
    for t in closed:
        streak = streak+1 if t['net_pnl']<0 else 0
        longest = max(longest,streak)
    sides = {}
    for side in ('up','down'):
        rows = [t for t in closed if t['side']==side]
        sides[side] = dict(trades=len(rows),net=sum(t['net_pnl'] for t in rows),wins=sum(t['net_pnl']>0 for t in rows))
    monthly = {}
    for d in daily:
        key = d['date'][:7]
        m = monthly.setdefault(key,dict(trading_days=0,trades=0,net_pnl=0.,ending_equity=500.))
        m['trading_days'] += d['tick_count']>0
        m['trades'] += d['trade_count']
        m['net_pnl'] += d['net_pnl']
        m['ending_equity'] = d['closing_equity']
    return dict(scheduled_weekdays=len(daily),observed_trading_days=sum(d['tick_count']>0 for d in daily),missing_weekdays=sum(d['tick_count']==0 for d in daily),directional_continuation_days=sum(d['continuation_satisfied'] for d in daily),eligible_before_risk_days=sum(d['eligible_before_risk'] for d in daily),eligible_executable_days=sum(d['eligible_executable'] for d in daily),executed_trades=len(trades),closed_trades=len(closed),winning_trades=len(wins),losing_trades=len(losses),breakeven_trades=len(closed)-len(wins)-len(losses),realized_net=gp-gl,ending_equity=end_equity,mark_to_market_net=end_equity-500,win_rate=len(wins)/len(closed) if closed else None,profit_factor=gp/gl if gl else None,profit_factor_note='undefined: no closed losses' if not gl else None,expectancy=(gp-gl)/len(closed) if closed else None,max_equity_drawdown=drawdown,max_consecutive_losses=longest,average_holding_seconds=sum(t['holding_seconds'] for t in closed)/len(closed) if closed else None,max_holding_seconds=max((t['holding_seconds'] for t in closed),default=None),by_side=sides,monthly=monthly,retracement_trades=sum(t['returned_inside_acd'] for t in trades),retracement_recovery_trades=sum(t['recovered_after_retracement'] for t in trades),profitable_retracement_trades=sum(t['returned_inside_acd'] and (t['net_pnl'] or 0)>0 for t in closed),opposite_boundary_broken_trades=sum(t['opposite_boundary_broken'] for t in trades),buffered_opposite_broken_trades=sum(t['buffered_opposite_broken'] for t in trades),exit_reasons=dict(Counter(t['exit_reason'] for t in closed)),risk_shutdown_events=risk_events,day_trade_limit_violations=[d['date'] for d in daily if d['trade_count']>1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--config',required=True)
    parser.add_argument('--reference',required=True,help='Original Phase 2 artifact directory; fixes window and 31 signal days')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True,exist_ok=True)
    config = Config(**json.loads(Path(args.config).read_text()))
    reference_root = Path(args.reference)
    reference = json.loads((reference_root/'summary.json').read_text())
    if asdict(config)!=reference['config']:
        raise ValueError('Strategy/cost configuration differs from original Phase 2')
    source = Path(strategy.__file__)
    hashes = {str(source):hashlib.sha256(source.read_bytes()).hexdigest()}
    begin_clock = time.monotonic()
    with db_connection(readonly=True) as conn, tempfile.TemporaryDirectory(prefix='acd_phase2_') as temp:
        with conn.cursor() as cur:
            cur.execute('SET statement_timeout = 0')
            cur.execute('SELECT MIN(timestamp),MAX(timestamp) FROM public.ticks WHERE symbol=%s',('XAUUSD',))
            available_start,available_end = cur.fetchone()
        start,end = datetime.fromisoformat(reference['window_start']),datetime.fromisoformat(reference['window_end'])
        warmup = datetime.fromisoformat(reference['warmup_start'])
        if available_end<end or available_start>warmup:
            raise ValueError('Original window/warmup not available; refusing to shift dataset')
        bot = ReplenishedAuditRobot(temp+'/disposable.sqlite',config,start.isoformat())
        bot.s['paused'] = True
        baseline = AuditRobot(temp+'/baseline.sqlite',config)
        baseline.s['paused'] = True
        display = gzip.open(output/'display_quotes.bin.gz','wb',compresslevel=3)
        display_second = None
        display_last = None
        dataset_hash = hashlib.sha256()
        # Exact interval cache: timezone conversion only at session rollover.
        # No indicator, confirmation, price or exit logic is changed.
        session_cache = [None]
        def session_for(t):
            value = session_cache[0]
            if value is None or not value.start <= t < value.end:
                value = acd_session_for_timestamp(t)
                session_cache[0] = value
            return value
        strategy.acd_session_for_timestamp = session_for
        daily = {}
        samples = []
        gaps = []
        previous = None
        previous_key = None
        observed_predecessor = None
        counts = Counter()
        peak,drawdown = 500.,0.
        flow_peak,flow_drawdown = 500.,0.
        signal_armed = dict(up=False,down=False)
        signal_since = dict(up=None,down=None)
        decision_set = set()
        formed = False
        last_minute = None
        spread_sum,spread_max,spread_count = 0.,0.,0
        window_started = False
        active_daily = None
        previous_equity = 500.
        formation_first = formation_last = None
        formation_gap = False
        formation_evaluated = False
        cutoff = None
        with conn.cursor(name='acd_phase2_readonly') as cur:
            cur.itersize = 30000
            cur.execute('SELECT id,timestamp,bid,ask FROM public.ticks WHERE symbol=%s AND timestamp>=%s AND timestamp<=%s ORDER BY timestamp,id',('XAUUSD',warmup,end))
            for ident,stamp,bid,ask in cur:
                counts['raw_ticks'] += 1
                bid,ask = float(bid),float(ask)
                if not all(math.isfinite(v) and v>0 for v in (bid,ask)) or ask<bid:
                    counts['invalid_quotes'] += 1
                    continue
                if previous is not None and stamp <= previous:
                    counts['duplicate_timestamp_quotes_skipped'] += 1
                    continue
                tick = Tick(ident,stamp,bid,ask,(bid+ask)/2)
                packed = struct.pack('<qqdd',int(ident),round(stamp.timestamp()*1_000_000),bid,ask)
                dataset_hash.update(packed)
                second = int(stamp.timestamp())
                if display_second is not None and second!=display_second:
                    display.write(struct.pack('<qdd',*display_last))
                display_second,display_last = second,(round(stamp.timestamp()*1_000_000),bid,ask)
                session = session_for(stamp)
                key = session.session_date.isoformat()
                in_window = stamp >= start
                if in_window and not window_started:
                    window_started = True
                    bot.s['paused'] = False
                    bot.s['realized'] = 0.
                    bot.s['equity'] = 500.
                    bot.s['latched'] = False
                    baseline.s.update(paused=False,realized=0.,equity=500.,latched=False)
                    # Warmup never entered orders; this starts the requested
                    # isolated account, not a reset of any running account.
                gap_seconds = (stamp-previous).total_seconds() if previous else 0.
                rollover = key != previous_key
                if rollover:
                    if bot.acd is not None and bot.s['history']:
                        old = bot.acd.session
                        history = bot.s['history']
                        bot.previous_context = dict(date=old.session_date.isoformat(),start=old.start.isoformat(),formation_end=old.opening_end.isoformat(),end=old.end.isoformat(),levels=dict(bot.acd.levels or {}),first_timestamp=history[0][1],first_ask=history[0][3],last_timestamp=history[-1][1],last_bid=history[-1][2],movement=history[-1][2]-history[0][3])
                    if previous_key:
                        observed_predecessor = previous_key
                    signal_armed = dict(up=False,down=False)
                    signal_since = dict(up=None,down=None)
                    formed = False
                    formation_first = formation_last = None
                    formation_gap = False
                    formation_evaluated = False
                    cutoff = min(session.opening_end+timedelta(minutes=config.entry_minutes),session.end-timedelta(hours=config.late_hours))
                if gap_seconds > config.quote_gap_seconds:
                    signal_armed = dict(up=False,down=False)
                    signal_since = dict(up=None,down=None)
                    if in_window:
                        gaps.append(dict(previous=previous.isoformat(),next=stamp.isoformat(),seconds=gap_seconds,session=key,classification='closure_or_missing_data_unverified'))
                if session.start <= stamp < session.opening_end:
                    if formation_last and (stamp-formation_last).total_seconds()>config.quote_gap_seconds:
                        formation_gap = True
                    formation_first = formation_first or stamp
                    formation_last = stamp
                if stamp >= session.opening_end and not formation_evaluated:
                    formed = bool(formation_first and formation_last and not formation_gap and (formation_first-session.start).total_seconds()<=config.quote_gap_seconds and (session.opening_end-formation_last).total_seconds()<=config.quote_gap_seconds)
                    formation_evaluated = True
                if in_window:
                    if key not in daily:
                        daily[key] = new_day(session,previous_equity)
                        daily[key]['partial_window'] = session.start < start or session.end > end
                    day = daily[key]
                    active_daily = day
                    day['tick_count'] += 1
                    day['first_quote'] = day['first_quote'] or stamp.isoformat()
                    day['last_quote'] = stamp.isoformat()
                    if gap_seconds>config.quote_gap_seconds:
                        day['quote_gaps_over_60s'] += 1
                        day['max_quote_gap_seconds'] = max(day['max_quote_gap_seconds'],gap_seconds)
                    day['preceding_observed_session'] = observed_predecessor
                    if day['calendar_predecessor'] is None:
                        predecessor = acd_session_for_timestamp(session.start-timedelta(microseconds=1))
                        day['calendar_predecessor'] = predecessor.session_date.isoformat()
                    spread_sum += ask-bid
                    spread_max = max(spread_max,ask-bid)
                    spread_count += 1
                old_position = bot.s['position']
                old_trail = old_position['trail'] if old_position else None
                old_trade = bot.current_trade
                old_latch = bot.s['latched']
                bot.observe_position(tick)
                if not baseline.s['latched']:
                    baseline._process(tick,None)
                    baseline.s['last'] = stamp.isoformat()
                elif baseline.s['position'] is not None:
                    raise RuntimeError('Baseline latched with unresolved exposure')
                bot._process(tick,None)
                bot.s['last'] = stamp.isoformat()
                p = bot.s['position']
                # Include entry tick executable spread in excursions.
                if p and not old_position:
                    bot.observe_position(tick)
                current_trail = p['trail'] if p else (old_position['trail'] if old_position else None)
                if old_position and current_trail != old_trail:
                    move = dict(trade_id=old_trade['trade_id'],timestamp=stamp.isoformat(),previous=old_trail,level=current_trail)
                    old_trade['trailing_movements'].append(move)
                    bot.trail_moves.append(move)
                if in_window and bot.s['latched'] and not old_latch:
                    bot.risk_events.append(dict(timestamp=stamp.isoformat(),equity=bot.s['equity'],threshold=450+config.liquidation_buffer))
                levels = bot.acd.levels
                if in_window:
                    day['yesterday_bias'] = bot.s['previous_move']
                    day['upper'] = levels['aUp'] if levels else day['upper']
                    day['lower'] = levels['aDown'] if levels else day['lower']
                    in_entry = session.opening_end <= stamp < cutoff
                    confirmed_side = None
                    if levels and in_entry:
                        for side,q,edge in (('up',bid,levels['aUp']),('down',ask,levels['aDown'])):
                            sign = 1 if side=='up' else -1
                            if (q-edge)*sign <= 0:
                                signal_armed[side] = True
                                signal_since[side] = None
                            elif signal_armed[side]:
                                signal_since[side] = signal_since[side] or stamp
                                if (stamp-signal_since[side]).total_seconds() >= config.hold_seconds:
                                    confirmed_side = side
                        state = confirmed_side or ('up_unconfirmed' if bid>levels['aUp'] else 'down_unconfirmed' if ask<levels['aDown'] else 'inside_acd')
                        if key not in decision_set:
                            day['today_direction'] = state
                            day['decision_timestamp'] = stamp.isoformat()
                            decision_set.add(key)
                        aligned = confirmed_side is not None and confirmed_side==bot.s['previous_move']
                        if aligned:
                            if not day['continuation_satisfied']:
                                day['today_direction'] = confirmed_side
                                day['decision_timestamp'] = stamp.isoformat()
                            day['continuation_satisfied'] = True
                            day['entry_signal_confirmed'] = True
                            day['eligible_before_risk'] |= formed
                            if formed and not bot.s['latched'] and not bot.s['paused'] and not old_position and key not in bot.s['used']:
                                sign = 1 if confirmed_side=='up' else -1
                                opposite = levels['aDown']-config.opposite_buffer if sign==1 else levels['aUp']+config.opposite_buffer
                                entry = (ask if sign==1 else bid)+sign*config.slippage
                                risk = abs(entry-opposite)*.01*config.ounces_per_lot+config.cost_per_trade+config.slippage*.01*config.ounces_per_lot+config.delay_reserve
                                day['eligible_executable'] |= risk < bot.s['equity']-450-config.liquidation_buffer
                    if p and not old_position:
                        day['trade_executed'] = True
                        day['trade_count'] += 1
                        day['eligible_executable'] = True
                        day['today_direction'] = p['side']
                        day['decision_timestamp'] = stamp.isoformat()
                        day['entry_timestamp'],day['entry_price'],day['lots'] = stamp.isoformat(),p['entry'],p['lots']
                    if old_position and not p:
                        day['net_pnl'] += bot.trades[-1]['net_pnl']
                    reason = classify_reason(bot,tick,session,levels,formed,cutoff)
                    if reason=='risk_estimate_exceeds_budget' and in_entry and levels:
                        side = bot.s['previous_move']
                        sign = 1 if side=='up' else -1
                        opposite = levels['aDown']-config.opposite_buffer if sign==1 else levels['aUp']+config.opposite_buffer
                        candidate_entry = (ask if sign==1 else bid)+sign*config.slippage
                        estimate = abs(candidate_entry-opposite)*.01*config.ounces_per_lot+config.cost_per_trade+config.slippage*.01*config.ounces_per_lot+config.delay_reserve
                        candidate = dict(timestamp=stamp.isoformat(),entry_price=candidate_entry,opposite_boundary=opposite,estimated_risk=estimate,available_risk_budget=bot.s['equity']-450-config.liquidation_buffer,balance=500+bot.s['realized'],rule='estimated_risk must be strictly less than available_risk_budget')
                        previous_candidate = bot.rejected_candidates.get(key)
                        if previous_candidate is None or estimate-candidate['available_risk_budget'] < previous_candidate['estimated_risk']-previous_candidate['available_risk_budget']:
                            bot.rejected_candidates[key] = candidate
                    day['reason_counts'][reason] = day['reason_counts'].get(reason,0)+1
                    day['closing_equity'] = bot.s.get('equity',500.)
                    previous_equity = day['closing_equity']
                    peak = max(peak,previous_equity)
                    drawdown = max(drawdown,peak-previous_equity)
                    flow_equity = previous_equity-bot.total_topups
                    flow_peak = max(flow_peak,flow_equity)
                    flow_drawdown = max(flow_drawdown,flow_peak-flow_equity)
                    minute = int(stamp.timestamp())//60
                    if minute != last_minute or p is not old_position or current_trail != old_trail:
                        sample = dict(timestamp=stamp.isoformat(),session=key,bid=bid,ask=ask,balance=500+bot.s['realized'],equity=previous_equity,cumulative_trading_pnl=bot.trading_pnl,cumulative_contributions=500+bot.total_topups,flow_adjusted_equity=flow_equity,flow_adjusted_drawdown=flow_peak-flow_equity,drawdown=peak-previous_equity,upper=levels['aUp'] if levels else None,lower=levels['aDown'] if levels else None,trail=current_trail,position_side=p['side'] if p else None)
                        if minute != last_minute or p is not old_position:
                            samples.append(sample)
                        # All trail changes are kept separately; chart price sampling
                        # never determines execution, extremes or account metrics.
                        last_minute = minute
                    counts['window_ticks'] += 1
                counts['processed_ticks'] += 1
                previous,previous_key = stamp,key
                if counts['processed_ticks'] % 500000 == 0:
                    progress = dict(ticks=counts['processed_ticks'],timestamp=stamp.isoformat(),equity=bot.s.get('equity',500),trades=len(bot.trades),latched=bot.s['latched'],elapsed_seconds=round(time.monotonic()-begin_clock,1))
                    (output/'progress.json').write_text(json.dumps(progress))
                    print(json.dumps(progress),flush=True)
        if display_last:
            display.write(struct.pack('<qdd',*display_last))
        display.close()
        # Explicit missing weekday rows; do not fabricate a holiday calendar.
        first_day = acd_session_for_timestamp(start).session_date
        last_day = acd_session_for_timestamp(end).session_date
        value = first_day
        while value <= last_day:
            if value.weekday()<5 and value.isoformat() not in daily:
                row = new_day(acd_session_for_date(value),500.)
                row.update(no_trade_reason='no_ticks_market_closure_or_missing_data',incomplete_session=True)
                daily[value.isoformat()] = row
            value += timedelta(days=1)
        ordered = [daily[k] for k in sorted(daily)]
        carry_equity = 500.
        for d in ordered:
            if not d['tick_count']:
                d['opening_equity'] = d['closing_equity'] = carry_equity
            carry_equity = d['closing_equity']
            d['incomplete_session'] = d['partial_window'] or not d['tick_count'] or (datetime.fromisoformat(d['session_end'])-datetime.fromisoformat(d['last_quote'])).total_seconds()>config.quote_gap_seconds if d['last_quote'] else True
            if d['trade_executed']:
                d['no_trade_reason'] = None
            elif d['no_trade_reason'] is None:
                reasons = d['reason_counts']
                priority = ['risk_shutdown_latched','incomplete_formation_data','yesterday_uncertain','risk_estimate_exceeds_budget','confirmation_incomplete','fresh_inside_observation_required','no_aligned_breakout','entry_window_closed','acd_forming','position_open']
                d['no_trade_reason'] = next((k for k in priority if reasons.get(k)), 'no_signal')
        trades = bot.trades+([bot.current_trade] if bot.current_trade else [])
        for t in trades:
            t['mae_usd'] = t['mae_quote']*t['units']
            t['mfe_usd'] = t['mfe_quote']*t['units']
            d = daily[t['date']]
            for key in ('mae_usd','mfe_usd','returned_inside_acd','recovered_after_retracement','opposite_boundary_broken','buffered_opposite_broken','exit_timestamp','exit_price','exit_reason','gross_pnl'):
                d[key] = t.get(key)
            d['trade_net_pnl'] = t.get('net_pnl')
            d['trailing_movement_count'] = len(t['trailing_movements'])
            d.update(balance_before_replenishment=t.get('balance_before_replenishment'),replenishment=t.get('replenishment'),balance_after_replenishment=t.get('balance_after_replenishment'),mandatory_deadline=t.get('mandatory_deadline'),deadline_delay_seconds=t.get('deadline_delay_seconds'),open_into_next_formation=t.get('open_into_next_formation'))
        metrics = summarize(trades,ordered,drawdown,bot.risk_events,bot.s.get('equity',500.))
        original_days = json.loads((reference_root/'daily.json').read_text())
        signal_days = [d['date'] for d in original_days if d['continuation_satisfied']]
        investigation = []
        for key in signal_days:
            d = daily[key]
            investigation.append(dict(date=key,executed=d['trade_executed'],trade_id=next((t['trade_id'] for t in trades if t['date']==key),None),reason=None if d['trade_executed'] else d['no_trade_reason'],closest_confirmed_rejection=bot.rejected_candidates.get(key),reason_counts=d['reason_counts']))
        metrics.update(net_trading_pnl=bot.trading_pnl,total_replenishments=bot.total_topups,replenishment_count=len(bot.contributions)-1,total_external_capital=500+bot.total_topups,final_balance=500+bot.s['realized'],mark_to_market_net=bot.s['equity']-500-bot.total_topups,max_flow_adjusted_equity_drawdown=flow_drawdown,maximum_individual_loss=max((max(0.,-t['net_pnl']) for t in bot.trades),default=0.),original_signal_days=len(signal_days),rejected_signal_days=[r for r in investigation if not r['executed']],open_at_end=bot.s['position'],deadline_violations=[dict(trade_id=t['trade_id'],date=t['date'],deadline=t['mandatory_deadline'],exit=t['exit_timestamp'],delay_seconds=t['deadline_delay_seconds'],cause=t['deadline_violation_cause'],previous_quote=t['previous_quote_before_exit'],quote_gap=t['quote_gap_before_exit_seconds']) for t in bot.trades if t['deadline_delay_seconds']>0],next_formation_violations=[t['trade_id'] for t in bot.trades if t['open_into_next_formation']])
        if counts['window_ticks']!=reference['counts']['window_ticks']:
            raise ValueError('Fixed-window processed tick count differs from original Phase 2')
        if sorted(d['date'] for d in ordered if d['continuation_satisfied'])!=signal_days:
            raise ValueError('Continuation signal dates differ from original Phase 2')
        original_trades = json.loads((reference_root/'trades.json').read_text())
        comparison_keys = ['date','side','entry_timestamp','entry_price','exit_timestamp','exit_price','exit_reason','net_pnl']
        financial_baseline_matches = len(baseline.trades)==len(original_trades) and all(all(a[k]==b[k] for k in comparison_keys) for a,b in zip(baseline.trades,original_trades)) and abs(baseline.s['equity']-reference['metrics']['ending_equity'])<1e-8
        if not financial_baseline_matches:
            raise ValueError('Original financial account does not reproduce on the same tick stream')
        report = dict(window_start=start.isoformat(),window_end=end.isoformat(),warmup_start=warmup.isoformat(),available_start=available_start.isoformat(),config=asdict(config),source_sha256=hashes,dataset_sha256=dataset_hash.hexdigest(),dataset_hash_encoding='valid increasing warmup+window records struct <qqdd: id, UTC microseconds, bid, ask',counts=dict(counts),spread=dict(mean=spread_sum/spread_count if spread_count else None,max=spread_max),metrics=metrics,original_metrics=reference['metrics'],elapsed_seconds=time.monotonic()-begin_clock,methodology=dict(account='Replenish to500 only after complete closure; retain profits; reset latch after settlement',risk_threshold='Unchanged absolute455 =450 floor +5 buffer; not balance-minus45',calendar='Unchanged weekday New York ACD; broker holidays unverified',costs='Unchanged historical spread and configured commission/slippage/swap',display='Actual last quote per second; all ticks drive execution and extrema; no synthetic closure prices'))
        (output/'summary.json').write_text(json.dumps(report,indent=2))
        (output/'trades.json').write_text(json.dumps(trades,indent=2))
        (output/'daily.json').write_text(json.dumps(ordered,indent=2))
        dump_csv(output/'daily.csv',ordered)
        dump_csv(output/'trades.csv',[{k:v for k,v in t.items() if k!='trailing_movements'} for t in trades])
        dump_csv(output/'trailing_movements.csv',bot.trail_moves)
        dump_csv(output/'equity_and_quotes.csv',samples)
        dump_csv(output/'data_gaps.csv',gaps)
        dump_csv(output/'capital_contributions.csv',bot.contributions)
        dump_csv(output/'account_events.csv',bot.account_events)
        dump_csv(output/'signal_day_investigation.csv',investigation)
        (output/'signal_day_investigation.json').write_text(json.dumps(investigation,indent=2))
        (output/'baseline_comparison.json').write_text(json.dumps(dict(matches=True,baseline_trades=len(baseline.trades),baseline_equity=baseline.s['equity'],baseline_net=sum(t['net_pnl'] for t in baseline.trades),compared_fields=comparison_keys,note='Baseline decisions replayed on identical ticks until its permanent latch; flat latched financial state then frozen'),indent=2))
        (output/'COMPLETE').write_text(datetime.now(timezone.utc).isoformat())
        bot.close()
        baseline.close()
        print(json.dumps(metrics,indent=2),flush=True)


if __name__ == '__main__':
    main()
