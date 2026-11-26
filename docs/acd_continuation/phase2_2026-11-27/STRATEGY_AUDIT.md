# Strategy compliance audit — 27 November 2026

This audit records the **unchanged deployed candidate**. The owner has confirmed low frequency, one trade per trading day, same-direction continuation, permissible ACD retracement and opposite-boundary invalidation. That clarification does not supply the missing numeric definitions listed below. No parameters, active source, service controls, equity or history were changed in Phase 2.

## Recovered definitions and evidence

Authoritative executable evidence: `frozen_source/datavis/acd_continuation.py`, `frozen_source/datavis/hlrtacd/acd.py`, `config.json` and the EC2 source hashes in `status_before.json`. The three frozen calculation files are byte-identical to the deployed files. Packaging and a portable research database adapter are separate research conveniences.

The original Phase 1 documents explicitly called these **candidate definitions requiring approval**, rather than recovered owner-approved settings. The repository also contains an incompatible Sydney ACD indicator. Exact owner intent cannot be recovered by silently choosing between them.

| Component | Exact deployed rule |
|---|---|
| Trading day | New York ACD session date; weekdays only; Friday runs until Monday 09:30; IANA `America/New_York` DST |
| Opening range | 09:30 inclusive to 09:45 exclusive; high/low of tick mid `(bid+ask)/2` |
| A edges | Opening high +0.75; opening low -0.75 quote USD |
| C edges | Opening high +1.00; opening low -1.00; calculated, not traded |
| Yesterday | Previous retained ACD session's first ask to final bid; >3 quote USD bullish, <-3 bearish, otherwise uncertain; frozen on rollover |
| Today | A causal fresh A-up break using bid, or A-down using ask, strictly outside for 450 seconds; inside observation required first; timer resets on recross/gap >60 seconds |
| Continuation | Confirmed break's direction must equal frozen prior bias; uncertain/opposing signals do not enter |
| Entry timing | After formation, before 120 minutes after formation and at least three hours before next formation; formation coverage checks and budget estimate are pre-existing gates |
| Sizing | Exactly 0.01 lot, hypothetical 100 oz/lot =1 oz exposure; no compounding, pyramiding or second entry in the session |
| Opposite exit | A-down minus 0.25 long; A-up plus 0.25 short; executable closing quote must remain strictly beyond it for 450 seconds; brief break/recross resets |
| Trailing | Activate only after favorable executable movement >3 quote USD from slipped entry; frozen edge E and favorable closing quote extreme H/L; E+0.5(H−E) long / E−0.5(E−L) short; tighten only; immediate closing-quote cross exit |
| Timed exit | One hour before next formation, at first available quote after deadline if no quote at the deadline; optional earlier `tradable_until` input exists but service does not supply it |
| Capital exit | Fixed USD 500 baseline; latch at equity<=455 (450 plus 5 pre-limit buffer); immediate exit request, never reset within the evaluation; no broker stop |
| Costs | Historical spread; 0.10 quote adverse slippage each side; USD 0.14 round-trip commission; configured swap 0; broker contract/fee schedule unverified |

The separate `datavis.app.load_acd_payload` uses Sydney08:00–08:30, min bid/max ask, A offsets half its range and C offsets one range. Existing `datavis.hlrtacd.market_moves` implements a different causal research detector. Neither is wired into the active continuation candidate. No screenshot supplied exact settings. Fisher's opening-range/time-confirmation rationale is documented in the Phase 1 runbook; it does not authenticate this candidate's XAUUSD parameters.

## Clarified requirements

| Requirement | Assessment |
|---|---|
| Maximum one trade per trading day | Enforced and durable **per New York ACD session date**; calendar/broker-day meaning remains unresolved. Historical counts are separately checked. |
| Today agrees with yesterday | Enforced using the above candidate definitions; no future price is used. Those definitions still require owner acceptance. |
| Inside-ACD retracement allowed | Compliant. No inside-range exit branch exists. A fixture enters, retraces inside and remains open. Floating drawdown alone does not invalidate the ACD signal. |
| Range re-entry is not invalidation | Compliant for the structural exit. Existing trailing, capital and timed exits can independently close during retracement, as required by the original mandate. |
| Opposite boundary break invalidates | **Ambiguous/conflicting if “break” now means immediate.** Current logic waits beyond a 0.25 buffer for 450 seconds, matching the original explicit break-and-hold instruction. A test proves a first break does not close immediately. No active change made. |
| Low trading frequency intentional | Preserved. No frequency target, tuning, tighter stop, additional filter or parameter search. |
| Floating drawdown expected | Measured without adding a retracement stop. Existing capital protection can still override an otherwise valid retracement. |

## Causality and no repainting

The opening band locks after its final formation tick; later prices do not alter it. Today's confirmation uses only current/prior executable quotes and elapsed observed time. Yesterday's bias is frozen from retained history at rollover. Entry edge/opposite edge are stored in the position; favorable extremes and trail levels use only seen quotes. Chronological `(timestamp,id)` replay skips duplicate timestamps exactly as the public processing method does. Warmup precedes the evaluation window with entries disabled; the account starts once at USD 500. No daily/reset/restart or hypothetical post-shutdown account is substituted.

The research reader calls the same `_process` decision method, bypassing production SQLite checkpoint overhead. A per-tick fixture compares its full strategy state against `PaperRobot.process`, including retracement and a trailing exit. The time-zone/session interval cache changes repeated lookup work only. Chart downsampling never determines fills, excursions or maximum drawdown. All execution decisions use full ticks.

## Reproduced defects and operational limitations

1. **Incomplete previous formation can produce a definite bias.** The prior-day check tests `formation_gap` and the final quote near expiry but omits the prior formation's start/end coverage checks. Starting ten minutes late within a prior opening range can still classify yesterday as bullish. This contradicts the Phase 1 documentation's broader “incomplete prior data is uncertain” claim. `test_partial_previous_formation_can_produce_definite_bias_defect` reproduces it.
2. **Entry equity is one quote late.** The entry branch computes equity before creating the position, leaving entry-tick equity at500 rather than marking spread, entry slippage and commission immediately. The next tick corrects valuation. The exact-strategy primary replay preserves this accounting behavior; tick-level drawdown may therefore omit the initial instantaneous cost drawdown. `test_entry_quote_equity_is_not_marked_until_following_tick_defect` reproduces it.
3. **Holiday/calendar mismatch.** Only weekends are skipped. There is no broker calendar for holidays, early closures, maintenance or DST-dependent broker hours. Prior bias requires a final quote within60 seconds of the scheduled ACD end; closures/missing data can force uncertainty or cause an incorrect preceding-session interpretation. Missing weekday/session rows and observed/calendar predecessors are reported, not silently repaired. Friday's session includes reopening quotes until Monday09:30; this is not automatically equivalent to the owner's “Friday” market move.
4. **Forward-validation pause can persist after a normal closure.** The watchdog treats any quote age>60 seconds as unhealthy, queues close/pause, and does not resume automatically. The service is currently running but paused with a pending close request and no exposure. No verified calendar distinguishes expected closure from an outage. Phase 2 deliberately leaves this state untouched.
5. **No calendar-aware early liquidation.** The core supports an earlier tradable-until timestamp, but the active service never supplies it. A timed exit during closure is executed only when a quote returns; the capital budget can be exceeded by gaps.
6. **Costs and contract remain assumptions.** Zero swap does not establish zero actual financing. No broker-confirmed fee/rollover/margin schedule or allocation isolation exists. Accordingly, “net after configured costs” is provisional, not verified all-in broker performance.

The first two are concrete implementation defects. Calendar handling and forward pause are concrete infrastructure gaps. Direction, edge and break/hold semantics are unresolved strategy definitions, rather than opportunities to optimize the rules. None has been patched in the active robot.

The historical account replays the uninterrupted strategy core, including its own quote-gap resets. It does not fabricate historical watchdog controls, service outages/restarts or broker latency. Thus it measures candidate strategy logic rather than the currently paused service's realized operational behavior. Forward validation remains limited until that operational gap is resolved with owner authorization.

## Definitions still requiring the owner

- Confirm New York mid-based A-up/A-down versus the Sydney indicator, and whether “range” means the opening band or offset A edges.
- Confirm the prior completed-session first-ask/final-bid ±3 rule versus a structural market-move definition; define the intended preceding trading session around holidays/weekend reopenings.
- Confirm today as the 450-second fresh A break, or supply the original alternative.
- Reconcile immediate opposite-edge invalidation with the original 0.25-buffer/450-second hold rule.
- Confirm the existing 120-minute entry window/three-hour cutoff and trailing activation from entry versus edge. No values were changed for this audit.
- Confirm “trading day” as the New York ACD session label rather than Sydney, UTC or broker day.

## Risk assessment

One-ounce hypothetical exposure on USD 500 can consume the USD 50 budget quickly. A valid retracement may coincide with capital liquidation; that is the existing override, not evidence of structural invalidation. The threshold is **not a guaranteed maximum loss**: quotes can gap, executable prices can disappear, and there is no broker stop or broker-capable independent watchdog. The primary result preserves the first permanent loss shutdown, if triggered. Later diagnostic signals do not count as account trades. Further development requires resolving correctness/calendar/cost issues before profitability or live-readiness conclusions.
