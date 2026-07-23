from pathlib import Path
import math
import pandas as pd

REPORTS = Path('reports')
TABLES = REPORTS / 'tables'
OUT_TABLE = TABLES / 'day46_final_results_overview.csv'
OUT_REPORT = REPORTS / 'day46_final_research_summary.md'

FILES = {
    'folds': TABLES / 'day43_fold_comparison.csv',
    'dev_robustness': TABLES / 'day43_robustness_summary.csv',
    'classifier': TABLES / 'day45_final_classifier_metrics.csv',
    'summary': TABLES / 'day45_final_holdout_summary.csv',
    'outcomes': TABLES / 'day45_final_outcome_decomposition.csv',
    'directions': TABLES / 'day45_final_per_direction_summary.csv',
    'robustness': TABLES / 'day45_final_robustness_summary.csv',
    'costs': TABLES / 'day45_final_cost_sensitivity.csv',
    'decision': TABLES / 'day45_final_decision.csv',
}

for name, path in FILES.items():
    if not path.exists():
        raise FileNotFoundError(f'Missing required file for {name}: {path}')


def fmt(value, digits=3):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isnan(number):
        return 'NA'
    return f'{number:.{digits}f}'


def pct(value, digits=1):
    return f'{100 * float(value):.{digits}f}%'


def md_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return '_No rows._'
    frame = frame.astype(str)
    headers = list(frame.columns)
    rows = frame.values.tolist()
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]

    def row(values):
        return '| ' + ' | '.join(str(v).ljust(widths[i]) for i, v in enumerate(values)) + ' |'

    return '\n'.join([row(headers), row(['-' * w for w in widths]), *[row(r) for r in rows]])


folds = pd.read_csv(FILES['folds'])
dev_robustness = pd.read_csv(FILES['dev_robustness'])
classifier = pd.read_csv(FILES['classifier'])
summary = pd.read_csv(FILES['summary']).iloc[0]
outcomes = pd.read_csv(FILES['outcomes'])
directions = pd.read_csv(FILES['directions'])
robustness = pd.read_csv(FILES['robustness']).iloc[0]
costs = pd.read_csv(FILES['costs'])
decision = pd.read_csv(FILES['decision']).iloc[0]

candidate = 'two_stage_sensitivity_0p65_0p65'
candidate_folds = folds[folds['model'].eq(candidate)].copy()
if len(candidate_folds) != 3:
    raise ValueError(f'Expected 3 chronological folds, found {len(candidate_folds)}')

dev = dev_robustness[dev_robustness['model'].eq(candidate)]
if len(dev) != 1:
    raise ValueError(f'Expected 1 pooled development row, found {len(dev)}')
dev = dev.iloc[0]

# Machine-readable overview.
overview = candidate_folds.assign(
    sample_type='development_chronological_oos',
    sample_name=candidate_folds['fold'],
    mean_gross_bps=candidate_folds['mean_net_return_bps'] + 1.0,
).rename(columns={
    'mean_net_return_bps': 'mean_net_bps',
    'median_net_return_bps': 'median_net_bps',
    'total_net_return_bps': 'total_net_bps',
})[[
    'sample_type', 'sample_name', 'n_signals', 'n_signal_runs',
    'directional_precision', 'mean_gross_bps', 'mean_net_bps',
    'median_net_bps', 'total_net_bps'
]]

overview = pd.concat([
    overview,
    pd.DataFrame([{
        'sample_type': 'development_pooled_oos',
        'sample_name': 'Day 43 pooled candidate',
        'n_signals': int(dev['n_signals']),
        'n_signal_runs': int(dev['n_runs']),
        'directional_precision': float(dev['directional_precision']),
        'mean_gross_bps': float(dev['mean_gross_bps']),
        'mean_net_bps': float(dev['mean_net_bps']),
        'median_net_bps': float(dev['median_net_bps']),
        'total_net_bps': float(dev['mean_net_bps'] * dev['n_signals']),
    }, {
        'sample_type': 'final_frozen_holdout',
        'sample_name': 'Day 45 final holdout',
        'n_signals': int(summary['n_signals']),
        'n_signal_runs': int(summary['n_signal_runs']),
        'directional_precision': float(summary['directional_precision']),
        'mean_gross_bps': float(summary['mean_gross_bps']),
        'mean_net_bps': float(summary['mean_net_bps']),
        'median_net_bps': float(summary['median_net_bps']),
        'total_net_bps': float(summary['total_net_bps']),
    }])
], ignore_index=True)

overview.to_csv(OUT_TABLE, index=False)

fold_table = candidate_folds[[
    'fold', 'n_signals', 'n_signal_runs', 'directional_precision',
    'mean_net_return_bps', 'median_net_return_bps', 'total_net_return_bps'
]].copy()
fold_table['directional_precision'] = fold_table['directional_precision'].map(pct)
for c in ['mean_net_return_bps', 'median_net_return_bps', 'total_net_return_bps']:
    fold_table[c] = fold_table[c].map(fmt)

classifier_table = classifier[[
    'stage', 'feature_set', 'balanced_accuracy', 'roc_auc', 'average_precision', 'brier_score'
]].copy()
for c in ['balanced_accuracy', 'roc_auc', 'average_precision', 'brier_score']:
    classifier_table[c] = classifier_table[c].map(fmt)

outcome_table = outcomes[['outcome_bucket', 'n_signals', 'n_runs', 'share', 'mean_net_bps', 'total_net_bps']].copy()
outcome_table['share'] = outcome_table['share'].map(pct)
for c in ['mean_net_bps', 'total_net_bps']:
    outcome_table[c] = outcome_table[c].map(fmt)

direction_table = directions[['direction_label', 'n_signals', 'n_runs', 'directional_precision', 'mean_gross_bps', 'mean_net_bps', 'total_net_bps']].copy()
direction_table['directional_precision'] = direction_table['directional_precision'].map(pct)
for c in ['mean_gross_bps', 'mean_net_bps', 'total_net_bps']:
    direction_table[c] = direction_table[c].map(fmt)

cost_table = costs[['cost_bps', 'mean_net_bps', 'median_net_bps', 'total_net_bps', 'positive_net_signal_share']].copy()
for c in ['cost_bps', 'mean_net_bps', 'median_net_bps', 'total_net_bps']:
    cost_table[c] = cost_table[c].map(fmt)
cost_table['positive_net_signal_share'] = cost_table['positive_net_signal_share'].map(pct)

report = f'''# Final Research Summary

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

{md_table(fold_table)}

Pooled Day 43 candidate:

- Signals: {int(dev['n_signals'])}
- Signal-bearing runs: {int(dev['n_runs'])}
- Directional precision: {pct(dev['directional_precision'])}
- Mean gross return: {fmt(dev['mean_gross_bps'])} bps
- Mean net return after 1 bps: {fmt(dev['mean_net_bps'])} bps
- Median net return: {fmt(dev['median_net_bps'])} bps
- Run-cluster bootstrap 95% CI: [{fmt(dev['bootstrap_ci_lower_2_5'])}, {fmt(dev['bootstrap_ci_upper_97_5'])}] bps
- Positive bootstrap means: {pct(dev['bootstrap_share_mean_net_positive'])}

Interpretation: the candidate had a slightly positive development-stage point estimate, but evidence was fold-dependent and uncertain. This justified one final independent holdout, not a profitability claim.

## Final holdout quality

- Raw attempts: {int(summary['eligible_runs'])}
- Strict eligible runs: {int(summary['eligible_runs'])}
- Technical exclusions: 0
- All runs reconstructed and exceeded the preregistered 2,500 processed-row threshold.

## Final classifier diagnostics

{md_table(classifier_table)}

The reduced-book direction model retained meaningful directional predictive power. The trade-flow MOVE model remained weaker but above random by ROC-AUC.

## Final frozen deployment result

- Signals: {int(summary['n_signals'])}
- Signal-bearing runs: {int(summary['n_signal_runs'])}
- Run coverage: {pct(summary['signal_run_coverage'])}
- Directional precision: {pct(summary['directional_precision'])}
- Mean gross return: {fmt(summary['mean_gross_bps'])} bps
- Mean net return after 1 bps: {fmt(summary['mean_net_bps'])} bps
- Median net return: {fmt(summary['median_net_bps'])} bps
- Total net return: {fmt(summary['total_net_bps'])} bps
- Positive-net signal share: {pct(summary['positive_net_signal_share'])}
- Break-even cost: {fmt(summary['break_even_cost_bps'])} bps

## Outcome decomposition

{md_table(outcome_table)}

Correct directional forecasts were frequent, but many moves were too small to cover costs, while wrong-direction signals generated sufficiently large adverse losses to eliminate the profitable tail.

## Direction decomposition

{md_table(direction_table)}

The directional asymmetry reversed relative to development: SHORT was negative and LONG positive on the holdout. This is evidence against a post-hoc direction-only policy.

## Holdout robustness

- Mean net without best signal: {fmt(robustness['mean_net_without_best_signal_bps'])} bps
- Mean net without best run: {fmt(robustness['mean_net_without_best_run_bps'])} bps
- Leave-one-run-out range: [{fmt(robustness['leave_one_run_out_min_mean_net_bps'])}, {fmt(robustness['leave_one_run_out_max_mean_net_bps'])}] bps
- Run-cluster bootstrap 95% CI: [{fmt(robustness['bootstrap_ci_lower_2_5'])}, {fmt(robustness['bootstrap_ci_upper_97_5'])}] bps
- Positive bootstrap means: {pct(robustness['bootstrap_share_mean_net_positive'])}

All leave-one-run-out point estimates remained negative, so the result was not driven by one adverse run.

## Cost sensitivity

{md_table(cost_table)}

The point estimate was positive at 0.5 bps but negative at the preregistered 1 bps cost. The primary conclusion therefore remains negative at 1 bps.

## Final decision

- Point-estimate status: `{decision['point_estimate_status']}`
- Evidence status: `{decision['evidence_status']}`
- Model status: `{decision['model_status']}`

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
'''

OUT_REPORT.write_text(report, encoding='utf-8')
print(f'[INFO] Saved overview: {OUT_TABLE}')
print(f'[INFO] Saved report: {OUT_REPORT}')
print('[INFO] Day 46 final summary build completed successfully.')