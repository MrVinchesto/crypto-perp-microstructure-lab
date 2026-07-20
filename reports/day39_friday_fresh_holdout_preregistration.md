# Day 39 Friday Fresh-Holdout Preregistration

## 1. Purpose

This document freezes the model architecture, feature sets, hyperparameters,
thresholds, entry rule, transaction-cost assumption, data-quality protocol,
and reporting requirements before inspecting the Friday fresh holdout.

The fresh holdout must be evaluated without changing any decision specified
below after observing Friday model predictions, signals, or returns.

## 2. Fresh-holdout batch

- Collection batch: `weekday_active_fri_day39`
- Collection date: `2026-07-17`
- Instrument: `BTCUSDT`
- Market: Binance USD-M Futures
- Planned active-hours window:
  - `14:00–17:00 UTC`
  - `15:00–18:00 London time`
  - `16:00–19:00 Berlin time`
- Planned runs: `36`
- Collection duration per run: `300 seconds`
- Prediction horizon: `50 events`
- Signal cooldown: `50 events`
- MOVE dead zone: `1.0 bps`
- Primary round-trip cost assumption: `1.0 bps`

## 3. Development and fitting data

### Development data already inspected

- Tuesday Day 28
- Wednesday Day 29
- Thursday Day 30

Thursday was used during architecture development and diagnostic analysis.
It is therefore not treated as a fresh holdout and is not used by the Day 38
threshold-stability selection procedure.

### Final fitting data before Friday evaluation

The frozen primary and sensitivity models will be fitted using:

- strict Tuesday Day 28 runs;
- strict Wednesday Day 29 runs.

No Friday observation may be used in model fitting, feature selection,
hyperparameter selection, threshold selection, calibration, or data-quality
rule design.

## 4. Fixed direction stage

- Model: standardized logistic regression
- Feature set: reduced book
- Number of features: `8`
- Regularization parameter: `C = 1.0`
- Target: direction conditional on
  `|future return over 50 events| > 1.0 bps`

### Reduced-book features

1. `spread_bps`
2. `event_gap_ms`
3. `best_bid_qty`
4. `best_ask_qty`
5. `imbalance_1`
6. `imbalance_5`
7. `microprice_deviation_bps`
8. `quote_changed`

## 5. Confirmatory primary specification

### MOVE stage

- Feature set: `medium_trade_flow`
- Number of features: `24`
- Model: standardized logistic regression
- Regularization parameter: `C = 0.03`
- MOVE threshold: `0.75`

### Direction and execution

- Direction threshold: `0.65`
- Entry rule: `first_eligible`
- Cooldown: `50 events`
- Cost: `1.0 bps` round trip

### Medium trade-flow features

1. `trade_count`
2. `buy_trade_count`
3. `sell_trade_count`
4. `trade_volume`
5. `buy_trade_volume`
6. `sell_trade_volume`
7. `signed_trade_volume`
8. `avg_trade_size`
9. `trade_imbalance`
10. `trade_intensity_per_second`
11. `trade_count_rolling_sum_5e`
12. `trade_count_rolling_sum_20e`
13. `trade_count_rolling_sum_50e`
14. `trade_volume_rolling_sum_5e`
15. `trade_volume_rolling_sum_20e`
16. `trade_volume_rolling_sum_50e`
17. `signed_trade_volume_rolling_sum_5e`
18. `signed_trade_volume_rolling_sum_20e`
19. `signed_trade_volume_rolling_sum_50e`
20. `trade_imbalance_rolling_5e`
21. `trade_imbalance_rolling_20e`
22. `trade_imbalance_rolling_50e`
23. `trade_intensity_rolling_mean_20e`
24. `trade_intensity_rolling_mean_50e`

### Selection rationale

The primary threshold policy follows a stability-first rule.

The medium `0.75 / 0.65` specification produced:

- positive mean net return in all three pre-Thursday chronological folds;
- positive worst-fold mean net return;
- low cross-fold dispersion.

It was preferred over the higher-coverage medium `0.65 / 0.65`
specification, which produced substantially more signals but failed materially
in the late-Wednesday fold.

The lower signal count of the selected specification is accepted in exchange
for stronger temporal stability. This choice was made before inspecting the
Friday holdout.

## 6. High-coverage sensitivity specification

The following specification must be reported separately:

- MOVE feature set: `medium_trade_flow`
- MOVE model: standardized logistic regression
- MOVE `C = 0.03`
- MOVE threshold: `0.65`
- Direction feature set: reduced book
- Direction `C = 1.0`
- Direction threshold: `0.65`
- Entry rule: `first_eligible`
- Cooldown: `50 events`

This is a sensitivity analysis, not the confirmatory primary specification.

Its Friday result cannot replace the primary result solely because it performs
better on the Friday holdout.

## 7. Exploratory reduced-model benchmark

The reduced trade-flow model may be evaluated as an exploratory benchmark.

- Feature set: `reduced_trade_flow`
- Number of features: `14`
- MOVE model: standardized logistic regression
- MOVE `C = 0.03`
- Direction stage: the same frozen reduced-book model

No confirmatory economic threshold is preregistered for the reduced model.

Permitted reporting:

- Friday MOVE ROC-AUC;
- average precision;
- Brier score;
- clearly labelled exploratory threshold-based diagnostics.

Prohibited interpretation:

- the reduced model may not replace the primary model based on one Friday
  result;
- post-hoc Friday threshold selection may not be described as out-of-sample
  evidence.

## 8. Frozen entry rule

The only confirmatory entry rule is `first_eligible`.

For every run:

1. calculate frozen MOVE and direction probabilities;
2. take the first row that satisfies both frozen thresholds;
3. assign LONG when the frozen UP probability is at least the direction
   threshold;
4. assign SHORT when the frozen UP probability is at most
   `1 - direction threshold`;
5. suppress subsequent entries for the next `50 events`;
6. resume the search after the cooldown.

The following post-hoc rules are prohibited for confirmatory evaluation:

- maximum future MOVE probability inside an episode;
- maximum future joint confidence inside an episode;
- best entry selected after observing future returns;
- any entry delay chosen from Friday results.

## 9. Frozen data-quality protocol

Friday exclusions must be based only on technical integrity, not model
performance.

Permitted exclusion reasons include:

- failed collection;
- materially incomplete collection duration;
- missing snapshot;
- unrecoverable order-book synchronization failure;
- missing or empty required stream caused by a technical error;
- failed top-of-book reconstruction;
- a quality rule already defined before Friday prediction evaluation.

Prohibited exclusions include:

- negative Friday return;
- low model confidence after inspecting results;
- an unfavorable signal direction;
- poor run-level PnL;
- disagreement with the research hypothesis.

Every excluded run must be listed with a technical reason.

## 10. Frozen evaluation procedure

After raw-data validation:

1. reconstruct top of book for all technically valid Friday runs;
2. build basic features;
3. build trade-flow features;
4. apply the frozen technical quality rules;
5. fit the frozen models on strict Tuesday and Wednesday data only;
6. produce Friday predictions once;
7. apply the confirmatory primary specification;
8. apply the high-coverage sensitivity specification separately;
9. report the exploratory reduced-model metrics separately;
10. do not refit or retune after seeing Friday results.

## 11. Required Friday outputs

### Data quality

1. number of planned raw runs;
2. number of successful raw runs;
3. number of successfully reconstructed runs;
4. number of strict evaluation runs;
5. list of excluded runs and technical reasons;
6. event-count and activity summary.

### Classification

1. direction ROC-AUC;
2. direction balanced accuracy;
3. MOVE ROC-AUC;
4. MOVE average precision;
5. MOVE Brier score.

### Confirmatory primary deployment

1. number of signals;
2. number of signal-bearing runs;
3. coverage;
4. directional precision;
5. mean and median signed return;
6. mean and median net return after `1.0 bps`;
7. total net return in bps;
8. LONG/SHORT breakdown;
9. per-run breakdown;
10. result without the best signal;
11. result without the best run;
12. leave-one-run-out range;
13. run-cluster bootstrap interval.

### Cost sensitivity

Report the frozen primary signals under:

- `0.5 bps`;
- `1.0 bps`;
- `1.5 bps`;
- `2.0 bps`.

Signal selection must not be recalculated separately for each cost.

## 12. Interpretation rules

### Confirmatory conclusion

The primary conclusion must be based on the medium `0.75 / 0.65`
specification.

### Sensitivity conclusion

The medium `0.65 / 0.65` result must be labelled as a higher-coverage
sensitivity analysis.

### Exploratory conclusion

All reduced-model threshold results must be labelled exploratory.

### No post-holdout promotion

A sensitivity or exploratory model cannot replace the primary model based only
on Friday performance. Promotion requires confirmation on another independent
holdout collected after a new preregistration.

## 13. Decisions frozen before Friday predictions

The following may not be changed after Friday prediction evaluation begins:

- primary feature set;
- sensitivity feature set;
- model type;
- `C` values;
- MOVE threshold;
- direction threshold;
- horizon;
- dead zone;
- cooldown;
- entry rule;
- transaction-cost assumptions;
- technical quality criteria;
- primary/sensitivity/exploratory labels;
- required output metrics.

## 14. Preregistration status

Status: **FROZEN BEFORE FRIDAY MODEL EVALUATION**

Confirmatory primary:

```text
medium_trade_flow
MOVE C = 0.03
MOVE threshold = 0.75
reduced_book direction C = 1.0
direction threshold = 0.65
first_eligible
50-event horizon and cooldown
1.0 bps primary round-trip cost
```

High-coverage sensitivity:

```text
medium_trade_flow
MOVE threshold = 0.65
direction threshold = 0.65
```

Exploratory benchmark:

```text
reduced_trade_flow
no confirmatory economic threshold
```