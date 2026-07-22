# Day 45 Final Frozen Holdout Evaluation

## Research status

This is the one-time evaluation of the specification frozen and preregistered
on Day 43. No threshold, feature, model, horizon, quality rule, cost assumption,
entry rule or cooldown rule was changed after the holdout was inspected.

## Frozen specification

- Training batches: Tuesday, Wednesday, Thursday and Friday active-hours data
- Holdout batch: `weekday_active_final_holdout_day44`
- MOVE model: medium trade-flow logistic regression, C=0.03
- MOVE threshold: 0.65
- Direction model: reduced-book logistic regression, C=1.0
- Direction threshold: 0.65
- Horizon: 50 book events
- Dead zone: 1.0 bps
- Entry: first eligible
- Cooldown: 50 events
- Primary cost: 1.0 bps

## Strict technical sample

- Raw holdout attempts: 36
- Strict eligible holdout runs: 36
- Excluded holdout runs: 0

## Classifier diagnostics

| stage     | feature_set       |   n_observations |   positive_class_share |   balanced_accuracy |   precision_positive |   recall_positive |   brier_score |   roc_auc |   average_precision |
|:----------|:------------------|-----------------:|-----------------------:|--------------------:|---------------------:|------------------:|--------------:|----------:|--------------------:|
| move      | medium_trade_flow |           103128 |               0.357565 |            0.538243 |             0.470416 |          0.204827 |      0.234864 |  0.593484 |            0.437807 |
| direction | reduced_book      |            36875 |               0.496298 |            0.662954 |             0.666629 |          0.642479 |      0.221024 |  0.715783 |            0.693841 |

## Primary deployment result

| holdout_batch                      |   eligible_runs |   n_signals |   n_signal_runs |   signal_run_coverage |   directional_precision |   positive_net_signal_share |   mean_gross_bps |   median_gross_bps |   mean_net_bps |   median_net_bps |   total_net_bps |   break_even_cost_bps |
|:-----------------------------------|----------------:|------------:|----------------:|----------------------:|------------------------:|----------------------------:|-----------------:|-------------------:|---------------:|-----------------:|----------------:|----------------------:|
| weekday_active_final_holdout_day44 |              36 |          37 |              17 |              0.472222 |                0.675676 |                    0.351351 |         0.715878 |           0.570214 |      -0.284122 |        -0.429786 |        -10.5125 |              0.715878 |

## Direction decomposition

| direction_label   |   n_signals |   n_runs |   directional_precision |   positive_net_signal_share |   mean_gross_bps |   median_gross_bps |   mean_net_bps |   median_net_bps |   total_net_bps |
|:------------------|------------:|---------:|------------------------:|----------------------------:|-----------------:|-------------------:|---------------:|-----------------:|----------------:|
| DOWN_SHORT        |          23 |       12 |                0.608696 |                    0.304348 |         0.358003 |           0.46402  |      -0.641997 |        -0.53598  |       -14.7659  |
| UP_LONG           |          14 |        8 |                0.785714 |                    0.428571 |         1.30382  |           0.786992 |       0.303816 |        -0.213008 |         4.25342 |

## Outcome decomposition

| outcome_bucket               |   n_signals |   n_runs |   mean_gross_bps |   mean_net_bps |   total_net_bps |    share |
|:-----------------------------|------------:|---------:|-----------------:|---------------:|----------------:|---------:|
| correct_direction_below_cost |          12 |        9 |         0.563594 |      -0.436406 |        -5.23688 | 0.324324 |
| profitable_after_cost        |          13 |       10 |         2.63399  |       1.63399  |        21.2418  | 0.351351 |
| wrong_direction              |          12 |       11 |        -1.20979  |      -2.20979  |       -26.5175  | 0.324324 |

## Robustness

| holdout_batch                      |   n_signals |   n_signal_runs |   best_signal_net_bps |   worst_signal_net_bps |   mean_net_without_best_signal_bps |   mean_net_without_worst_signal_bps |   best_run_name |   worst_run_name |   mean_net_without_best_run_bps |   mean_net_without_worst_run_bps |   leave_one_run_out_min_mean_net_bps |   leave_one_run_out_median_mean_net_bps |   leave_one_run_out_max_mean_net_bps |   bootstrap_mean_net_bps |   bootstrap_ci_lower_2_5 |   bootstrap_ci_upper_97_5 |   bootstrap_share_mean_net_positive |
|:-----------------------------------|------------:|----------------:|----------------------:|-----------------------:|-----------------------------------:|------------------------------------:|----------------:|-----------------:|--------------------------------:|---------------------------------:|-------------------------------------:|----------------------------------------:|-------------------------------------:|-------------------------:|-------------------------:|--------------------------:|------------------------------------:|
| weekday_active_final_holdout_day44 |          37 |              17 |               3.90002 |               -4.74211 |                          -0.400348 |                           -0.160289 | 20260721_192612 |  20260721_191028 |                       -0.400348 |                        -0.196191 |                             -0.40057 |                               -0.272959 |                            -0.196191 |                -0.282234 |                -0.737148 |                  0.190674 |                              0.1159 |

## Cost sensitivity

|   cost_bps |   n_signals |   mean_net_bps |   median_net_bps |   total_net_bps |   positive_net_signal_share |
|-----------:|------------:|---------------:|-----------------:|----------------:|----------------------------:|
|        0.5 |          37 |       0.215878 |         0.070214 |         7.98749 |                    0.540541 |
|        1   |          37 |      -0.284122 |        -0.429786 |       -10.5125  |                    0.351351 |
|        1.5 |          37 |      -0.784122 |        -0.929786 |       -29.0125  |                    0.297297 |
|        2   |          37 |      -1.28412  |        -1.42979  |       -47.5125  |                    0.243243 |

## Final interpretation

- Point-estimate status: `NON_POSITIVE_POINT_ESTIMATE`
- Evidence status: `INSUFFICIENT_ROBUST_EVIDENCE`

A positive point estimate does not by itself establish robust profitability.
The conclusion must reflect run-level concentration, bootstrap uncertainty,
transaction-cost sensitivity and consistency with the earlier chronological
folds. This holdout is evaluated once; no post-hoc replacement policy is allowed.
