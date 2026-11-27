# Phase 4: causal ACD entry research

Earlier entry can improve execution price, but it also introduces losing opportunities. The experiment tests **36 entry profiles × four fixed exits = 144 independent account replays**, across all 67 observed sessions. The original 450-second baseline reproduces exactly: 31 trades, 13 winners, 18 losers, **−USD 90.51**. This is exploratory research on the same three-month sample used in previous phases; no active robot or strategy parameter was changed.

## Reproducibility and scope

Foundation: Phase 3 commit 2c0fdbdc2f9d8248a396166a83cbf734d69069e4. The frozen dataset interval is 2026-08-27T06:29:59.982000+11:00 through 2026-11-27T06:29:59.982000+11:00; warmup starts 2026-08-17T06:29:59.982000+11:00. New York observed session labels run August 26–November 26, including partial boundary sessions. No moving endpoint or newly collected quotes were added. Full-stream SHA256: e7ad2a5f2736f1deb90392af80d56891026b7832ca9bbd9976cea607ac12002b. Counts: {"raw_ticks": 18066705, "processed_ticks": 18065202, "duplicate_timestamp_quotes_skipped": 1503, "window_ticks": 16235560}. The preceding observed trading session supplies yesterday's completed classification, never the future close of today's session. [Source causality checks](causal_source_checks.json) document the unchanged foundation; this run regenerates its tick stream and verifies the identical hash/counts.

The implementation reuses the Phase 3 exit, settlement and risk engine, replacing only the causal entry mask. The four 450-second exit controls also reproduce their Phase 3 P&L. All 5160 closed trades were independently checked against full-tick UTC-microsecond bid/ask records, adverse fills, fees and admission budgets. [Baseline check](baseline_reproduction.json); [execution checks](execution_checks.json); [delivery checks](checks.json).

## Frozen mechanics and causal entry definitions

ACD formation is 09:30–09:45 New York, with the existing boundary calculation (A offset 0.75, C offset 1.0). W is upper minus lower A-edge, frozen after formation. Prior direction retains the existing first-ask to last-bid classification with USD 3 threshold. Today confirms through quotes beyond the aligned directional edge. Long decisions use executable bid; shorts use ask. Entry uses ask+0.10 for longs or bid−0.10 for shorts. Position size is fixed 0.01 lot, 100 ounces/lot = 1 ounce. Historical spread is retained, commission USD 0.14 round trip, adverse slippage USD 0.10 per side, swap zero as in the foundation. Explicit cost columns add commission, slippage and entry spread as descriptive components; spread/slippage are already embedded in execution P&L and must not be subtracted again.

Every family requires a fresh inside observation after formation; a quote gap over 60 seconds disarms confirmation until a new inside observation. Boundary equality counts as inside. The original 120-minute window begins at formation completion and entries also respect the existing three-hour late cutoff. There is at most one executed entry per session, including after losses. No alternative bypasses risk admission.

* A: continuous holds 0, 15, 30, 60, 120, 225, 450, 900 seconds. Zero enters on the first causal fresh breakout quote.
* B: closing quote reaches 0.05, 0.10, 0.15, 0.25 or 0.50W beyond the edge; no minimum time.
* C: fresh aligned breakout reaches 0.10 or 0.25W, returns to the edge/inside, then a new breakout holds 0, 30, 60 or 120 seconds. Pending retests cancel on an invalidating gap or the existing 450-second buffered opposite-boundary break. A later fresh sequence may start within the original window. No future pivots are used.
* D: holds 60, 225 or 450 seconds, then waits outside for an actual slipped executable entry within 0.10, 0.25 or 0.50W. No fabricated capped fills. Inside recross or gap resets confirmation; expiry cancels.
* E: holds 30, 60 or 120 seconds and touches 0.10 or 0.25W during the same continuously outside run. **The owner's clarification is applied: a prior distance touch remains valid while outside**, even if execution occurs closer to the edge. Inside recross or gap clears the distance latch and time hold.

Exit A retains the original strict >USD 3 activation and half retracement anchored to the ACD edge. Exit B targets 0.75W from the edge, with no trail. Exit C activates after >=1W from actual entry, trailing halfway from entry, tightening only. Exit D has no trail or target. All retain buffered opposite invalidation (0.25 buffer held 450 seconds), the original risk admission reserve and mandatory closure one hour before next formation. A prior target touch never causes a historical fill; if already beyond target at entry, exit occurs at the next available quote even if it retraces. Target flags and their P&L contributions are separate comparison columns.

Each account starts at USD 500; only after full closure, balances below 500 are replenished to 500 and the latch resets. Profits are retained. Contributions are external capital, excluded from net trading P&L. Capital protection remains **absolute equity USD 455 = USD 450 floor + USD 5 liquidation buffer**, not balance-minus45. Profits increase headroom to the same absolute threshold; no open loss is replenished. Every account obeys final balance = total external funding + trading P&L. Funding-adjusted drawdown uses marked equity minus cumulative external funding plus the initial500, so deposits cannot conceal trading losses.

## Baseline entry consumption

Median execution is **0.477W** beyond the directional edge. 6/31 baseline entries are already at least 0.75W beyond it. The hold measures the actual uninterrupted run, not elapsed time since an earlier failed breakout. [All 31 forensic entries](BASELINE_ENTRY_FORENSICS.md) and [complete threshold diagnostics](baseline_forensics.csv) distinguish confirmation movement, actual exposure, structural-only counterfactual exposure and unrestricted later movement.

## Complete executable results

[Comparison CSV](comparison.csv) contains every requested financial and entry metric for all 144 configurations; [monthly and long/short breakdown](monthly_and_side.csv), [daily decisions for all 9,648 configuration-sessions](daily.csv), [rejection summary](rejection_summary.csv), [full trade records](all_trades.csv), [capital contribution ledger](capital_contributions.csv) and [per-configuration exports](trade_records/) provide the underlying records. Eligible means an entry-pattern quote exists before admission; risk-eligible means the current funded account can admit one. Missed-control sessions and untraded directional follow-through are separate diagnostics.

Pure time family net P&L (USD), with all other conditions fixed:

| Hold seconds | Original trailing A | Edge target B | Entry trailing C | No target/trail D |
| --- | --- | --- | --- | --- |
| 0 | 37.66 | 124.86 | -3.67 | -75.10 |
| 15 | -20.64 | 72.59 | -26.70 | -95.48 |
| 30 | 36.54 | 79.00 | -48.33 | -125.01 |
| 60 | -23.01 | 59.73 | -68.64 | -151.80 |
| 120 | 44.23 | 140.54 | 17.08 | -74.08 |
| 225 | -8.51 | 99.16 | 48.09 | -93.05 |
| 450 | -90.51 | 63.64 | -236.59 | -92.17 |
| 900 | -129.07 | 16.61 | -189.42 | -83.52 |

| Fixed exit | Positive profiles /36 | Median net USD |
| --- | --- | --- |
| exit_A | 17 | -2.73 |
| exit_B | 35 | 74.48 |
| exit_C | 11 | -50.48 |
| exit_D | 5 | -83.98 |

Earlier is not monotonically better; changing the exit changes the outcome substantially. A positive three-month result does not establish future economic value. Especially, exit B's 0.75W target was already investigated in Phase 3 on this same sample; its success here is not independent exit validation.

## Opportunity quality and matched sessions

Controls use the same exit and original450 entry, not a different exit. Price improvement is signed favourably for both sides. For matched sessions, improved price directly reduces structural stop distance and increases remaining distance to the fixed edge target. The P&L decomposition includes actual independent account histories, so it is not a pure controlled price-only intervention.

| Configuration | Matched | Additional | Missed | Mean price improvement | Matched P&L delta | Additional net | Missed-control delta |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A_0s__exit_B | 31 | 16 | 0 | 8.00 | 218.06 | -156.84 | 0 |
| A_120s__exit_B | 31 | 8 | 0 | 4.17 | 104.24 | -27.34 | 0 |
| E_60s_0.25W__exit_B | 30 | 7 | 1 | 3.60 | 79.75 | -10.52 | 28.81 |

Immediate entry therefore improves matched prices but its 16 additional opportunities lose USD 156.84 under exit B. More trades alone do not establish quality. [Matched-session records](matched_sessions.csv) and [opportunity quality](opportunity_quality.csv) also identify admission changes and entries requiring retained profits to exceed the fixed500 risk budget. Those are causal cash-state effects, not free funding during an open position.

False-breakout statistics classify a return inside before the selected edge-distance threshold (0.5,0.75,1,1.5,2,3W). A valid retracement is not automatically a losing trade or a new exclusion rule. [Statistics](false_breakout_statistics.csv) distinguish the whole confirmed entry episode from the post-execution segment and explicitly count thresholds already consumed during confirmation. [First-breakout market paths](market_opportunities.csv) include all67 sessions and remain descriptive; future threshold hits are never entry inputs. A jump across the entire ACD range can break the opposite edge without a quoted inside observation; raw opposite ordering is reported separately.

## Robustness and independent-validation candidates

All six hybrid entry profiles with exit B are positive in this sample and remain positive after removing their best two sessions:

| Hybrid profile | Trades | Net USD | PF | Funding-adjusted DD | Net without best2 |
| --- | --- | --- | --- | --- | --- |
| E_30s_0.1W | 42 | 76.37 | 1.31 | 150.85 | 41.82 |
| E_30s_0.25W | 38 | 125.25 | 1.94 | 97.83 | 99.82 |
| E_60s_0.1W | 42 | 57.96 | 1.23 | 156.62 | 25.05 |
| E_60s_0.25W | 37 | 161.68 | 2.64 | 97.83 | 136.18 |
| E_120s_0.1W | 39 | 140.54 | 1.93 | 93.44 | 107.49 |
| E_120s_0.25W | 37 | 153.07 | 2.58 | 97.18 | 126.71 |

This is a neighbouring parameter region worth freezing for independent historical validation, rather than selecting the highest historical cell. Candidate families: hybrid 30/60/120 seconds ×0.10/0.25W with exit B; pure time60/120/225 and distance0.10/0.25/0.50W as comparison controls; retain450 and no-target/trail controls. Preserve the entire proposed small family prospectively rather than tuning it further on these dates. Entry C retests and tight D restrictions are useful rejection controls; results do not support calling all earlier entries superior.

Every comparison row includes net excluding the best one and best two sessions. Sensitivity figures and heatmaps cover every family and all four exits: [A](charts/family_A_sensitivity.png), [B](charts/family_B_sensitivity.png), [C](charts/family_C_sensitivity.png), [D](charts/family_D_sensitivity.png), [E](charts/family_E_sensitivity.png). Heatmaps separately show net, PF, funding-adjusted drawdown and median entry distance. These are 144 related tests on only67 sessions, with prior same-sample strategy exploration. Selection bias, dependence across configurations and regime concentration prevent interpreting the highest P&L as an optimum. Best-session removal is a descriptive stress test, not out-of-sample evidence.

## Trade 2 and visual verification

[Trade 2 forensic report](TRADE2.md) compares all five entry families plus450 under all four exits, with exact New York timestamps and fills. Its early baseline loss is not proof that the later peak was available under every exit. Every alternative stops when its first valid exit occurs. No retrospective re-entry or peak fill is credited.

[Interactive gallery](gallery.html) contains 44 material-session comparisons plus zooms, with profiles declared before viewing performance: A_450s, A_60s, B_0.1W, C_0.1W_60s, D_225s_0.25W, E_60s_0.1W. Material means entry presence differs, time differs by at least30 seconds or execution price differs by at least0.05W from the same-exit450 control. Default gallery comparisons hold exit A fixed to isolate entry; trade2 adds exits B/C/D. Full charts include the complete preceding session, current session and next completed formation. Identical panel scales show ACDs, breakout/confirmation, actual entries/exits, retest events, permitted zones, trail/targets, exposure and mandatory deadline. Actual last quote per second is displayed; decisions/extrema use all ticks. Gaps over60 seconds remain blank, without interpolated trading across closures. Click images for high resolution. [Download charts, summaries and records](entry_research_charts_and_records.zip).

## Closure, risk and implementation limits

There are zero open positions at dataset cutoff and zero next-formation exposures. Strict wall-clock deadline misses number 465 across all replays, maximum 0.298 seconds, all recorded in [deadline events](deadline_events.csv). These are first-available-quote latency after the deadline, not fabricated exact-time fills. Execution verification checks all548 timed exits against the next actual quote. No weekend/holiday/missing-quote deadline delay over60 seconds occurred in the executed sample. An isolated right-censor branch preserves an open position if no permitted exit quote exists by the fixed endpoint; it was unit-tested but not needed by these configurations.

The original weekday New York calendar does not validate broker holidays or early closes. Observed-session predecessor handling skips absent sessions, but partial formation/prior-session coverage can still produce a definite original bias; that foundation defect is retained for comparability. The original entry-tick equity mark lags execution costs by one quote; later marks and settlement contain costs. Neither defect was silently repaired. Fixed slippage and zero swap are inherited assumptions, not an independently calibrated liquidity/financing model. Absolute455 protection does not guarantee a maximum USD45 realized loss because quotes, slippage and retained-profit balances change realized exposure. No synthetic missing-market prices or continuous service availability are assumed.

Active paper status at 2026-11-27T01:46:20.627121+00:00: paper-only, broker orders disabled, USD500 equity, no position, paused. Source/config hashes and existing event history match the before snapshot. Paper, watchdog, collector and datavis monitored units remain active; warning/error tail is '-- No entries --'. Historical research did not unpause the robot or change active parameters. Raw collection and previous research were preserved.

## Reproduction and validation

The scripts are scripts/acd_phase4_research.py (entry masks and isolated replay), acd_phase4_opportunities.py (post-hoc path diagnostics), acd_phase4_visuals.py (full-context figures) and acd_phase4_deliver.py (reports/exports/invariants). They consume the exact Phase3 cache/manifest regenerated from the read-only database; raw cache stays outside Git. Run the research CLI with --help for required paths, then opportunity diagnostics, visuals and delivery in that order. Repository tests passed **37 tests**, covering the original controls, causal prefix invariance, long/short symmetry, fresh inside resets, retests, executable price restrictions, the clarified hybrid distance latch and right-censor behavior. Delivery checks and SHA256 artifact manifest accompany the committed artifacts. No deployment or live execution was authorized or performed.
