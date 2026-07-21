# Final Fresh-Holdout Preregistration

## Status

This specification was frozen after the Day 43 robustness audit and before
collecting or evaluating the final holdout batch.

## Final training data

Strict-quality runs from:

- `weekday_active_tue_day28`
- `weekday_active_wed_day29`
- `weekday_active_thu_day30`
- `weekday_active_fri_day39`

Friday is development data and is not an independent test for this model.

## Final holdout batch

- Batch label: `weekday_active_final_holdout_day44`
- Target: 36 runs
- Requested duration: 300 seconds per run
- Collection window: next eligible weekday active-hours window
- Symbol: BTCUSDT USD-M perpetual futures

## Technical quality rules

- collection status is success;
- collection duration is at least 295 seconds;
- depth events are at least 2500;
- trade events are at least 1;
- processed rows are at least 2500;
- top-of-book reconstruction succeeds.

No run may be excluded because of PnL, volatility, prediction quality or model
outcomes.

## Frozen MOVE stage

- Logistic regression
- `C = 0.03`
- class weighting: balanced
- features:

- `trade_count`
- `buy_trade_count`
- `sell_trade_count`
- `trade_volume`
- `buy_trade_volume`
- `sell_trade_volume`
- `signed_trade_volume`
- `avg_trade_size`
- `trade_imbalance`
- `trade_intensity_per_second`
- `trade_count_rolling_sum_5e`
- `trade_count_rolling_sum_20e`
- `trade_count_rolling_sum_50e`
- `trade_volume_rolling_sum_5e`
- `trade_volume_rolling_sum_20e`
- `trade_volume_rolling_sum_50e`
- `signed_trade_volume_rolling_sum_5e`
- `signed_trade_volume_rolling_sum_20e`
- `signed_trade_volume_rolling_sum_50e`
- `trade_imbalance_rolling_5e`
- `trade_imbalance_rolling_20e`
- `trade_imbalance_rolling_50e`
- `trade_intensity_rolling_mean_20e`
- `trade_intensity_rolling_mean_50e`

## Frozen direction stage

- Logistic regression
- `C = 1.0`
- class weighting: balanced
- features:

- `spread_bps`
- `event_gap_ms`
- `best_bid_qty`
- `best_ask_qty`
- `imbalance_1`
- `imbalance_5`
- `microprice_deviation_bps`
- `quote_changed`

## Frozen deployment rule

- Horizon: 50 book events
- MOVE target dead zone: 1.0 bps
- MOVE threshold: 0.65
- Direction threshold: 0.65
- Entry: first eligible observation
- Cooldown: 50 events
- Round-trip cost assumption: 1.0 bps

No threshold, feature, hyperparameter, horizon, cost assumption, entry rule or
quality rule may be changed after the holdout data are inspected.

## Primary evaluation

The primary estimand is mean net signed h50 return per selected signal after a
1 bps round-trip cost.

The report must include signal count, signal-bearing runs, directional
precision, gross and net returns, LONG/SHORT results, run-level robustness,
leave-one-run-out results, run-cluster bootstrap and cost sensitivity.

The final holdout is evaluated once. No post-hoc replacement model or threshold
is permitted.
