# Trade 2 forensic investigation

NY ACD session **2026-09-02**, up; frozen W **24.0200**. Baseline entry **2026-09-02 10:09:50.463 EDT at 4393.99**. Mandatory deadline **2026-09-03 08:30:00.000 EDT**.

Events below use full executable quotes from entry until the deadline. A later favorable extreme is a path observation, not an achievable exit or a replacement for an earlier valid exit.

| Path event | First executable observation |
|---|---|
|returned_inside|2026-09-02 10:47:02.000 EDT at 4380.40|
|raw_opposite_touch|Not reached before deadline|
|held_opposite_invalidation|Not reached before deadline|
|one_width_edge|2026-09-02 21:10:04.739 EDT at 4404.51|
|one_width_entry|2026-09-02 22:37:34.354 EDT at 4418.08|
|capital_first_if_held_from_baseline_entry|Not reached before deadline|

Maximum favorable movement before closure deadline: **56.15**, quote 4450.14 at 2026-09-03 08:10:36.821 EDT. Maximum adverse movement: **29.57**, quote 4364.42 at 2026-09-02 12:46:02.836 EDT.

The capital-first path uses the baseline entry balance and the unchanged absolute USD 455 equity trigger. Each alternative below instead follows its own independent account history and intervening exits. No later peak is credited after an earlier target, pullback, capital, opposite or timed exit.

| Configuration | Entry NY / price | Exit NY / price | Reason | Net USD | Capital before / after |
|---|---|---|---|---:|---|
|baseline #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 10:13:04.682 EDT / 4388.49|trailing|-5.64|500.00 / 500.00|
|target_entry_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 22:37:34.354 EDT / 4417.98|target|23.85|500.00 / 523.85|
|target_edge_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 21:10:04.739 EDT / 4404.41|target|10.28|500.00 / 510.28|
|trail_entry_edge_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-03 08:30:00.000 EDT / 4446.42|timed|52.29|500.00 / 552.29|
|trail_edge_edge_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 21:26:32.284 EDT / 4394.35|trailing|0.22|500.00 / 500.22|
|trail_entry_entry_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-03 08:30:00.000 EDT / 4446.42|timed|52.29|500.00 / 552.29|
|trail_edge_entry_1W #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 21:14:32.679 EDT / 4401.50|trailing|7.37|500.00 / 507.37|
|reentry_A #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 10:47:02.000 EDT / 4380.30|pullback|-13.83|500.00 / 500.00|
|reentry_B #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-02 10:47:02.000 EDT / 4380.30|pullback|-13.83|556.91 / 543.08|
|reentry_B #2|2026-09-02 15:33:10.874 EDT / 4382.74|2026-09-03 08:30:00.000 EDT / 4446.42|timed|63.54|543.08 / 606.62|
|no_trailing #1|2026-09-02 10:09:50.463 EDT / 4393.99|2026-09-03 08:30:00.000 EDT / 4446.42|timed|52.29|500.00 / 552.29|

[Full-session comparison](charts/session_2026-09-02.png) · [Secondary zoom](charts/trade2_zoom.png) · [Complete raw result fields](trade2_forensic.json)

The previously cited 4470 movement occurs after the 08:30 NY mandatory deadline in the chart’s next-formation context. It cannot be credited to a position subject to that deadline. A software deadline can only execute at an available quote; exact delays are recorded for every configuration.
