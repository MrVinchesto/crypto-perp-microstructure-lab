# Final Research Summary

## Research question

Can short-horizon BTCUSDT perpetual-futures order-book and trade-flow features predict economically meaningful price movement and direction strongly enough to survive a preregistered 1 bps round-trip transaction-cost assumption?

## Data and research design

- Instrument: BTCUSDT perpetual futures.
- Inputs: Binance depth snapshots, depth updates and aggregate trades collected locally.
- Primary regime: weekday active hours, approximately 14:00-17:00 UTC.
- Observation unit: reconstructed top-of-book event.
- Final horizon: 50 events.
- Meaningful-move dead zone: 1 bps.
- Validation: chronological folds with no random shuffling.
- Final confirmation: one frozen holdout collected after Day 43 preregistration.

## Frozen specification

- MOVE model: logistic regression, 24 medium trade-flow features, C=0.03, threshold=0.65.
- Direction model: logistic regression, 8 reduced book features, C=1.0, threshold=0.65.
- Entry: first eligible signal.
- Cooldown: 50 events.
- Directions: LONG and SHORT retained.
- Primary round-trip cost: 1 bps.
- No post-hoc model, threshold, feature, timing or directional changes after freeze.

## Chronological development evidence

| fold                      | n_signals | n_signal_runs | directional_precision | mean_net_return_bps | median_net_return_bps | total_net_return_bps |
| ------------------------- | --------- | ------------- | --------------------- | ------------------- | --------------------- | -------------------- |
| fold_1_tue_to_wed         | 46        | 17            | 56.5%                 | 0.102               | 0.153                 | 4.703                |
| fold_2_tue_wed_to_thu     | 61        | 20            | 68.9%                 | 0.723               | 0.383                 | 44.080               |
| fold_3_tue_wed_thu_to_fri | 118       | 31            | 63.6%                 | -0.214              | -0.335                | -25.302              |

Pooled Day 43 candidate:

- Signals: 225
- Signal-bearing runs: 68
- Directional precision: 63.6%
- Mean gross return: 1.104 bps
- Mean net return after 1 bps: 0.104 bps
- Median net return: -0.212 bps
- Run-cluster bootstrap 95% CI: [-0.327, 0.571] bps
- Positive bootstrap means: 68.4%

Interpretation: the candidate had a slightly positive development-stage point estimate, but evidence was fold-dependent and uncertain. This justified one final independent holdout, not a profitability claim.

## Final holdout quality

- Raw attempts: 36
- Strict eligible runs: 36
- Technical exclusions: 0
- All runs reconstructed and exceeded the preregistered 2,500 processed-row threshold.

## Final classifier diagnostics

| stage     | feature_set       | balanced_accuracy | roc_auc | average_precision | brier_score |
| --------- | ----------------- | ----------------- | ------- | ----------------- | ----------- |
| move      | medium_trade_flow | 0.538             | 0.593   | 0.438             | 0.235       |
| direction | reduced_book      | 0.663             | 0.716   | 0.694             | 0.221       |

The reduced-book direction model retained meaningful directional predictive power. The trade-flow MOVE model remained weaker but above random by ROC-AUC.

## Final frozen deployment result

- Signals: 37
- Signal-bearing runs: 17
- Run coverage: 47.2%
- Directional precision: 67.6%
- Mean gross return: 0.716 bps
- Mean net return after 1 bps: -0.284 bps
- Median net return: -0.430 bps
- Total net return: -10.513 bps
- Positive-net signal share: 35.1%
- Break-even cost: 0.716 bps

## Outcome decomposition

| outcome_bucket               | n_signals | n_runs | share | mean_net_bps | total_net_bps |
| ---------------------------- | --------- | ------ | ----- | ------------ | ------------- |
| correct_direction_below_cost | 12        | 9      | 32.4% | -0.436       | -5.237        |
| profitable_after_cost        | 13        | 10     | 35.1% | 1.634        | 21.242        |
| wrong_direction              | 12        | 11     | 32.4% | -2.210       | -26.517       |

Correct directional forecasts were frequent, but many moves were too small to cover costs, while wrong-direction signals generated sufficiently large adverse losses to eliminate the profitable tail.

## Direction decomposition

| direction_label | n_signals | n_runs | directional_precision | mean_gross_bps | mean_net_bps | total_net_bps |
| --------------- | --------- | ------ | --------------------- | -------------- | ------------ | ------------- |
| DOWN_SHORT      | 23        | 12     | 60.9%                 | 0.358          | -0.642       | -14.766       |
| UP_LONG         | 14        | 8      | 78.6%                 | 1.304          | 0.304        | 4.253         |

The directional asymmetry reversed relative to development: SHORT was negative and LONG positive on the holdout. This is evidence against a post-hoc direction-only policy.

## Holdout robustness

- Mean net without best signal: -0.400 bps
- Mean net without best run: -0.400 bps
- Leave-one-run-out range: [-0.401, -0.196] bps
- Run-cluster bootstrap 95% CI: [-0.737, 0.191] bps
- Positive bootstrap means: 11.6%

All leave-one-run-out point estimates remained negative, so the result was not driven by one adverse run.

## Cost sensitivity

| cost_bps | mean_net_bps | median_net_bps | total_net_bps | positive_net_signal_share |
| -------- | ------------ | -------------- | ------------- | ------------------------- |
| 0.500    | 0.216        | 0.070          | 7.987         | 54.1%                     |
| 1.000    | -0.284       | -0.430         | -10.513       | 35.1%                     |
| 1.500    | -0.784       | -0.930         | -29.013       | 29.7%                     |
| 2.000    | -1.284       | -1.430         | -47.513       | 24.3%                     |

The point estimate was positive at 0.5 bps but negative at the preregistered 1 bps cost. The primary conclusion therefore remains negative at 1 bps.

## Final decision

- Point-estimate status: `NON_POSITIVE_POINT_ESTIMATE`
- Evidence status: `INSUFFICIENT_ROBUST_EVIDENCE`
- Model status: `FINAL_FROZEN_HOLDOUT_EVALUATED`

## Main conclusion

The project found transferable short-horizon predictive information in the order book and weaker predictive information in trade flow. However, the frozen execution policy did not select moves large enough to overcome the preregistered 1 bps round-trip cost on the independent holdout.

> Predictive signal transferred, but robust economic profitability after transaction costs was not demonstrated.

## Limitations

- The final holdout contains one future active-hours weekday rather than many independent days.
- The 1 bps cost assumption does not separately model fees, spread crossing, latency, queue position and market impact.
- Event-time horizons vary in clock time with market activity.
- Logistic models primarily capture linear relationships after scaling.
- The execution rule is evaluated on reconstructed data rather than a live exchange simulator.
- Results are specific to BTCUSDT perpetual futures and the sampled regimes.

## Future work

Future work must begin as a separate, preregistered research cycle. Scientifically appropriate directions include magnitude-aware targets, explicit execution-cost modelling, multi-day rolling retraining, predefined regime variables and live paper trading. None of these directions changes the conclusion of the completed frozen cycle.
