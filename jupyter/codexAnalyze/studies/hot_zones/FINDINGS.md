# Causal hot-zone study

## Scope

- XAUUSD broker days from 20 January through 12 February 2026 (18 evaluable days).
- Levels use only information available at the signal time: previous-day H/L/C, classic pivot, R1/S1, previous midrange, session open, opening-range H/L, expanding quote-count average, and expanding one-second time average.
- The exports contain no traded volume. `quote_average` is therefore a quote-count-weighted VWAP proxy; `time_average` is a one-second time-weighted proxy.
- Future ticks are used only to score executable bid/ask outcomes after an entry.

## Main findings

1. A hot zone is useful as an attention gate, but not as an entry by itself. In the later period, causal structure shifts near any known level produced a 12.8% strict Excellent proxy rate versus 11.1% away from levels. The lift is real but too small for an independent signal.
2. More levels did not mean more edge. Later-period single-zone events scored 14.0%, while two-or-more-level confluence scored only 5.1%.
3. Classic pivot was weak by itself. It should not receive special status without a live tick confirmation.
4. The most repeatable interaction was a rejection/bounce at the expanding quote-count average: 20.6% Excellent proxy in the earlier period and 22.9% later, versus period baselines of 15.2% and 11.1%. The 95% interval remains wide (later: 13.3%–36.5%), and 12 February produced zero successes from 15 attempts, so this is a research setup rather than a production rule.
5. Long quote-average reactions were stronger in the later period (6/17, 35.3%) than shorts (5/31, 16.1%). This may be regime-specific and must not be assumed permanent.
6. Only two of eight manually accepted exact entries were inside the tested zones: 9 February near the time average, and 11 February near the opening-range high. The other six depended on local structure away from these reference levels. Hot zones therefore complement, rather than replace, micro-pattern hunting.

## The approved 11 February entry

- Ideal training entry: long ID 31335923 at 11:17:51.299 Sydney.
- It was 0.565 below the known opening-range high (5033.045), inside the causal 0.68 attention radius.
- The actual boundary break occurred shortly afterward. A live break-and-hold entry at ID 31335934 (11:17:54.602 Sydney) would have entered at ask 5033.26, covered spread in 1.425 seconds, had 0.58 maximum adverse movement, and reached 7.58 maximum favorable movement within 15 seconds.
- A generic three-quote break-and-hold detector was still too noisy: opening-range-high longs scored only 8.0% in the later period. The known level can arm the detector, but an additional local tick-quality filter is required.

## Recommended next experiment

Keep two independent entry families:

1. The existing strict failed-retest micro setup, unchanged.
2. A hot-zone watcher that arms only near the expanding quote average, previous high/low, or opening-range boundary. It should not enter on contact. It should wait for a local higher-low/lower-high, synchronized quote movement, normal spread, continuous prices, and then a first break or reclaim.

Train and score the final confirmation pattern separately for each zone interaction (`reaction`, `breakout`, `inside`) because their results differ materially.
