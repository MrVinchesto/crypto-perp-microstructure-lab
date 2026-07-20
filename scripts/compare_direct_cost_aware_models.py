from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, f1_score, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DATA_PATH = Path('data/processed/trade_flow_features.csv')
LOG_PATH = Path('reports/tables/fresh_trade_collection_log.csv')
TABLES_DIR = Path('reports/tables')
FIGURES_DIR = Path('reports/figures')
REPORT_PATH = Path('reports/day42_direct_cost_aware_model_summary.md')
TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TUE = 'weekday_active_tue_day28'
WED = 'weekday_active_wed_day29'
THU = 'weekday_active_thu_day30'
FRI = 'weekday_active_fri_day39'
FOLDS = [
    ('fold_1_tue_to_wed', [TUE], WED),
    ('fold_2_tue_wed_to_thu', [TUE, WED], THU),
    ('fold_3_tue_wed_thu_to_fri', [TUE, WED, THU], FRI),
]

HORIZON = 50
COST_BPS = 1.0
COOLDOWN = 50
MOVE_C = 0.03
DIRECTION_C = 1.0
DIRECT_C = 0.03
RANDOM_STATE = 42

BOOK_FEATURES = [
    'spread_bps', 'event_gap_ms', 'best_bid_qty', 'best_ask_qty',
    'imbalance_1', 'imbalance_5', 'microprice_deviation_bps', 'quote_changed',
]
MOVE_FEATURES = [
    'trade_count', 'buy_trade_count', 'sell_trade_count',
    'trade_volume', 'buy_trade_volume', 'sell_trade_volume',
    'signed_trade_volume', 'avg_trade_size', 'trade_imbalance',
    'trade_intensity_per_second',
    'trade_count_rolling_sum_5e', 'trade_count_rolling_sum_20e',
    'trade_count_rolling_sum_50e', 'trade_volume_rolling_sum_5e',
    'trade_volume_rolling_sum_20e', 'trade_volume_rolling_sum_50e',
    'signed_trade_volume_rolling_sum_5e',
    'signed_trade_volume_rolling_sum_20e',
    'signed_trade_volume_rolling_sum_50e',
    'trade_imbalance_rolling_5e', 'trade_imbalance_rolling_20e',
    'trade_imbalance_rolling_50e',
    'trade_intensity_rolling_mean_20e', 'trade_intensity_rolling_mean_50e',
]
BASE_FEATURES = list(dict.fromkeys(BOOK_FEATURES + MOVE_FEATURES))
TEMPORAL_FEATURES = [
    'mid_return_5e_bps', 'mid_return_20e_bps',
    'imbalance_1_delta_5e', 'imbalance_1_delta_20e',
    'imbalance_5_delta_5e', 'imbalance_5_delta_20e',
    'microprice_deviation_bps_delta_5e',
    'microprice_deviation_bps_delta_20e',
    'spread_bps_delta_5e', 'trade_imbalance_delta_5e',
    'trade_imbalance_delta_20e', 'trade_intensity_delta_20e',
]
TEMPORAL_MODEL_FEATURES = BASE_FEATURES + TEMPORAL_FEATURES


def make_model(c_value, multiclass=False):
    return Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()),
        ('model', LogisticRegression(
            C=c_value,
            class_weight='balanced',
            solver='lbfgs' if multiclass else 'liblinear',
            max_iter=1000 if multiclass else 2000,
            random_state=RANDOM_STATE,
        )),
    ])


def add_temporal_features(frame):
    result = frame.sort_values(['run_name', 'row_in_run']).copy()
    grouped = result.groupby('run_name', sort=False)
    result['mid_return_5e_bps'] = (result['mid_price'] / grouped['mid_price'].shift(5) - 1) * 10000
    result['mid_return_20e_bps'] = (result['mid_price'] / grouped['mid_price'].shift(20) - 1) * 10000
    for feature in ['imbalance_1', 'imbalance_5']:
        result[f'{feature}_delta_5e'] = result[feature] - grouped[feature].shift(5)
        result[f'{feature}_delta_20e'] = result[feature] - grouped[feature].shift(20)
    result['microprice_deviation_bps_delta_5e'] = result['microprice_deviation_bps'] - grouped['microprice_deviation_bps'].shift(5)
    result['microprice_deviation_bps_delta_20e'] = result['microprice_deviation_bps'] - grouped['microprice_deviation_bps'].shift(20)
    result['spread_bps_delta_5e'] = result['spread_bps'] - grouped['spread_bps'].shift(5)
    result['trade_imbalance_delta_5e'] = result['trade_imbalance'] - grouped['trade_imbalance'].shift(5)
    result['trade_imbalance_delta_20e'] = result['trade_imbalance'] - grouped['trade_imbalance'].shift(20)
    result['trade_intensity_delta_20e'] = result['trade_intensity_per_second'] - grouped['trade_intensity_per_second'].shift(20)
    return result


def first_eligible(candidates):
    candidates = candidates[candidates['signal_direction'] != 0].copy()
    selected = []
    for _, run_frame in candidates.groupby('run_name', sort=False):
        last_row = None
        for idx, row in run_frame.sort_values('row_in_run').iterrows():
            current = int(row['row_in_run'])
            if last_row is None or current > last_row + COOLDOWN:
                selected.append(idx)
                last_row = current
    return candidates.loc[selected].copy()


def finish_signals(signals, fold_name, model_name):
    signals = signals.copy()
    signals['fold'] = fold_name
    signals['model'] = model_name
    signals['signed_return_bps'] = signals['signal_direction'] * signals['future_return_bps']
    signals['net_return_bps'] = signals['signed_return_bps'] - COST_BPS
    signals['direction_correct'] = signals['signed_return_bps'] > 0
    signals['net_profitable'] = signals['net_return_bps'] > 0
    return signals


def summarize(signals, n_runs):
    if signals.empty:
        return dict(n_signals=0, n_signal_runs=0, signal_run_coverage=0.0,
                    directional_precision=np.nan, positive_net_signal_share=np.nan,
                    mean_signed_return_bps=np.nan, median_signed_return_bps=np.nan,
                    mean_net_return_bps=np.nan, median_net_return_bps=np.nan,
                    total_net_return_bps=0.0)
    return dict(
        n_signals=len(signals),
        n_signal_runs=signals['run_name'].nunique(),
        signal_run_coverage=signals['run_name'].nunique() / n_runs,
        directional_precision=signals['direction_correct'].mean(),
        positive_net_signal_share=signals['net_profitable'].mean(),
        mean_signed_return_bps=signals['signed_return_bps'].mean(),
        median_signed_return_bps=signals['signed_return_bps'].median(),
        mean_net_return_bps=signals['net_return_bps'].mean(),
        median_net_return_bps=signals['net_return_bps'].median(),
        total_net_return_bps=signals['net_return_bps'].sum(),
    )

print('[INFO] Loading collection log and processed data...')
log = pd.read_csv(LOG_PATH)
relevant_log = log[log['collection_batch'].isin([TUE, WED, THU, FRI])].copy()
relevant_log['run_name'] = relevant_log['run_name'].astype(str)
relevant_runs = set(relevant_log['run_name'])

required = list(dict.fromkeys(['run_name', 'row_in_run', 'mid_price'] + BASE_FEATURES))
available = pd.read_csv(DATA_PATH, nrows=0).columns.tolist()
missing = [column for column in required if column not in available]
if missing:
    raise ValueError(f'Missing columns in trade_flow_features.csv: {missing}')

data = pd.read_csv(DATA_PATH, usecols=required)
data['run_name'] = data['run_name'].astype(str)
data = data[data['run_name'].isin(relevant_runs)].copy()
data = data.sort_values(['run_name', 'row_in_run']).reset_index(drop=True)
data[BASE_FEATURES] = data[BASE_FEATURES].replace([np.inf, -np.inf], np.nan)

processed_rows = data.groupby('run_name').size()
quality = relevant_log.drop_duplicates('run_name', keep='last').copy()
quality['processed_rows'] = quality['run_name'].map(processed_rows).fillna(0).astype(int)
quality['status_ok'] = quality['status'].astype(str).str.strip().str.lower().eq('success')
duration_source = quality['collection_seconds_meta'] if 'collection_seconds_meta' in quality.columns else quality['collection_seconds_requested']
quality['duration_ok'] = pd.to_numeric(duration_source, errors='coerce') >= 295
quality['depth_ok'] = pd.to_numeric(quality['depth_events'], errors='coerce') >= 2500
quality['trades_ok'] = pd.to_numeric(quality['trade_events'], errors='coerce') >= 1
quality['processed_ok'] = quality['processed_rows'] >= 2500
quality['strict_quality_ok'] = (
    quality['status_ok'] & quality['duration_ok'] & quality['depth_ok']
    & quality['trades_ok'] & quality['processed_ok']
)

def exclusion_reason(row):
    reasons = []
    if not row['status_ok']:
        reasons.append('status_not_success')
    if not row['duration_ok']:
        reasons.append('duration_below_295')
    if not row['depth_ok']:
        reasons.append('depth_below_2500')
    if not row['trades_ok']:
        reasons.append('no_trades')
    if not row['processed_ok']:
        reasons.append('processed_rows_below_2500_or_missing')
    return ';'.join(reasons)

quality['exclusion_reason'] = quality.apply(exclusion_reason, axis=1)
quality.to_csv(TABLES_DIR / 'day42_run_quality_audit.csv', index=False)
quality[~quality['strict_quality_ok']].to_csv(TABLES_DIR / 'day42_excluded_runs.csv', index=False)

quality_summary = quality.groupby('collection_batch', as_index=False).agg(
    log_runs=('run_name', 'nunique'),
    strict_runs=('strict_quality_ok', 'sum'),
)
quality_summary['excluded_runs'] = quality_summary['log_runs'] - quality_summary['strict_runs']
quality_summary.to_csv(TABLES_DIR / 'day42_quality_summary.csv', index=False)

run_to_batch = dict(zip(quality['run_name'], quality['collection_batch']))
strict_runs = set(quality.loc[quality['strict_quality_ok'], 'run_name'])
data = data[data['run_name'].isin(strict_runs)].copy()
data['collection_batch'] = data['run_name'].map(run_to_batch)

print('[INFO] Building temporal features and direct cost-aware target...')
data = add_temporal_features(data)
data[TEMPORAL_MODEL_FEATURES] = data[TEMPORAL_MODEL_FEATURES].replace([np.inf, -np.inf], np.nan)
future_mid = data.groupby('run_name', sort=False)['mid_price'].shift(-HORIZON)
data['future_return_bps'] = (future_mid / data['mid_price'] - 1) * 10000

data['move_target'] = np.where(
    data['future_return_bps'].notna(),
    (data['future_return_bps'].abs() > COST_BPS).astype(float),
    np.nan,
)
data['direction_target'] = np.where(
    data['future_return_bps'] > COST_BPS,
    1.0,
    np.where(data['future_return_bps'] < -COST_BPS, 0.0, np.nan),
)
data['direct_target'] = np.where(
    data['future_return_bps'] > COST_BPS,
    1,
    np.where(data['future_return_bps'] < -COST_BPS, -1, 0),
)
data.loc[data['future_return_bps'].isna(), 'direct_target'] = np.nan
model_data = data[data['future_return_bps'].notna()].copy()

pd.DataFrame({
    'feature': BASE_FEATURES + TEMPORAL_FEATURES,
    'feature_set': ['base'] * len(BASE_FEATURES) + ['temporal_addition'] * len(TEMPORAL_FEATURES),
}).to_csv(TABLES_DIR / 'day42_feature_specification.csv', index=False)

metric_rows = []
deployment_rows = []
confusion_rows = []
all_signals = []

for fold_name, train_batches, validation_batch in FOLDS:
    print(f'[INFO] Running {fold_name}...')
    train = model_data[model_data['collection_batch'].isin(train_batches)].copy()
    validation = model_data[model_data['collection_batch'].eq(validation_batch)].copy()
    train_direction = train[train['direction_target'].notna()].copy()

    move_model = make_model(MOVE_C)
    move_model.fit(train[MOVE_FEATURES], train['move_target'].astype(int))
    direction_model = make_model(DIRECTION_C)
    direction_model.fit(
        train_direction[BOOK_FEATURES],
        train_direction['direction_target'].astype(int),
    )

    move_probability = move_model.predict_proba(validation[MOVE_FEATURES])[:, 1]
    direction_probability = direction_model.predict_proba(validation[BOOK_FEATURES])[:, 1]

    for model_name, move_threshold, direction_threshold in [
        ('two_stage_primary_0p75_0p65', 0.75, 0.65),
        ('two_stage_sensitivity_0p65_0p65', 0.65, 0.65),
    ]:
        candidates = validation[['run_name', 'row_in_run', 'future_return_bps']].copy()
        candidates['signal_direction'] = np.where(
            (move_probability >= move_threshold) & (direction_probability >= direction_threshold),
            1,
            np.where(
                (move_probability >= move_threshold)
                & (direction_probability <= 1 - direction_threshold),
                -1,
                0,
            ),
        )
        signals = finish_signals(first_eligible(candidates), fold_name, model_name)
        all_signals.append(signals)
        deployment_rows.append({
            'fold': fold_name,
            'validation_batch': validation_batch,
            'model': model_name,
            'model_family': 'two_stage',
            **summarize(signals, validation['run_name'].nunique()),
        })

    for model_name, features in [
        ('direct_multiclass_base', BASE_FEATURES),
        ('direct_multiclass_temporal', TEMPORAL_MODEL_FEATURES),
    ]:
        model = make_model(DIRECT_C, multiclass=True)
        model.fit(train[features], train['direct_target'].astype(int))
        predicted = model.predict(validation[features]).astype(int)
        probabilities = model.predict_proba(validation[features])
        true_class = validation['direct_target'].astype(int)
        classes = model.named_steps['model'].classes_

        metric_rows.append({
            'fold': fold_name,
            'validation_batch': validation_batch,
            'model': model_name,
            'n_features': len(features),
            'n_observations': len(validation),
            'balanced_accuracy': balanced_accuracy_score(true_class, predicted),
            'macro_f1': f1_score(true_class, predicted, average='macro', zero_division=0),
            'multiclass_log_loss': log_loss(true_class, probabilities, labels=classes),
            'predicted_short_share': (predicted == -1).mean(),
            'predicted_no_trade_share': (predicted == 0).mean(),
            'predicted_long_share': (predicted == 1).mean(),
        })

        matrix = confusion_matrix(true_class, predicted, labels=[-1, 0, 1])
        for i, true_label in enumerate([-1, 0, 1]):
            for j, predicted_label in enumerate([-1, 0, 1]):
                confusion_rows.append({
                    'fold': fold_name,
                    'model': model_name,
                    'true_class': true_label,
                    'predicted_class': predicted_label,
                    'count': int(matrix[i, j]),
                })

        candidates = validation[['run_name', 'row_in_run', 'future_return_bps']].copy()
        candidates['signal_direction'] = predicted
        signals = finish_signals(first_eligible(candidates), fold_name, model_name)
        all_signals.append(signals)
        deployment_rows.append({
            'fold': fold_name,
            'validation_batch': validation_batch,
            'model': model_name,
            'model_family': 'direct_multiclass',
            **summarize(signals, validation['run_name'].nunique()),
        })

metrics = pd.DataFrame(metric_rows)
deployment = pd.DataFrame(deployment_rows)
confusion = pd.DataFrame(confusion_rows)
signals = pd.concat(all_signals, ignore_index=True)

metrics.to_csv(TABLES_DIR / 'day42_direct_classifier_metrics.csv', index=False)
deployment.to_csv(TABLES_DIR / 'day42_fold_deployment_results.csv', index=False)
confusion.to_csv(TABLES_DIR / 'day42_direct_confusion_matrix.csv', index=False)
signals.to_csv(TABLES_DIR / 'day42_selected_signals.csv', index=False)

summary_rows = []
for model_name, frame in deployment.groupby('model', sort=False):
    model_signals = signals[signals['model'].eq(model_name)]
    fold_net = frame['mean_net_return_bps'].dropna()
    summary_rows.append({
        'model': model_name,
        'model_family': frame['model_family'].iloc[0],
        'positive_folds': int((frame['mean_net_return_bps'] > 0).sum()),
        'total_signals': int(frame['n_signals'].sum()),
        'total_signal_runs': int(frame['n_signal_runs'].sum()),
        'median_fold_mean_net_bps': fold_net.median(),
        'mean_fold_mean_net_bps': fold_net.mean(),
        'worst_fold_mean_net_bps': fold_net.min(),
        'best_fold_mean_net_bps': fold_net.max(),
        'fold_net_std_bps': fold_net.std(ddof=0),
        'weighted_mean_net_bps': model_signals['net_return_bps'].mean(),
        'aggregate_directional_precision': model_signals['direction_correct'].mean(),
        'aggregate_positive_net_share': model_signals['net_profitable'].mean(),
    })

model_summary = pd.DataFrame(summary_rows).sort_values(
    ['positive_folds', 'median_fold_mean_net_bps', 'worst_fold_mean_net_bps'],
    ascending=[False, False, False],
)
model_summary.to_csv(TABLES_DIR / 'day42_model_comparison_summary.csv', index=False)

plot_order = [
    'two_stage_primary_0p75_0p65',
    'two_stage_sensitivity_0p65_0p65',
    'direct_multiclass_base',
    'direct_multiclass_temporal',
]
fold_order = [fold[0] for fold in FOLDS]
pivot = deployment.pivot(
    index='fold', columns='model', values='mean_net_return_bps'
).reindex(index=fold_order, columns=plot_order)

plt.figure(figsize=(12, 7))
pivot.plot(kind='bar', ax=plt.gca())
plt.axhline(0, linewidth=1)
plt.ylabel('Mean net return after 1 bps, bps')
plt.xlabel('Chronological fold')
plt.title('Day 42 chronological model comparison')
plt.xticks(rotation=20, ha='right')
plt.grid(axis='y', alpha=0.3)
plt.legend(fontsize=8)
plt.tight_layout()
plt.savefig(FIGURES_DIR / 'day42_fold_mean_net_comparison.png', dpi=150)
plt.close()

report_lines = [
    '# Day 42 Direct Cost-Aware Model Development',
    '',
    'Tuesday-Friday are development data. This is not a fresh-holdout test.',
    '',
    '## Fixed expanding folds',
    '',
]
for fold_name, train_batches, validation_batch in FOLDS:
    report_lines.append(
        f'- {fold_name}: train {train_batches} -> validate {validation_batch}'
    )
report_lines.extend([
    '',
    '## Compared models',
    '',
    '- Two-stage primary: MOVE 0.75 + direction 0.65',
    '- Two-stage sensitivity: MOVE 0.65 + direction 0.65',
    '- Direct multiclass base: SHORT / NO TRADE / LONG',
    '- Direct multiclass temporal: base plus 12 fixed delta features',
    '',
    '## Direct target',
    '',
    '- SHORT: future h50 return < -1 bps',
    '- NO TRADE: future h50 return between -1 and +1 bps',
    '- LONG: future h50 return > +1 bps',
    '',
    '## Interpretation boundary',
    '',
    'Day 42 may nominate one candidate for Day 43 robustness analysis.',
    'Any final candidate must be frozen before another independent holdout.',
    '',
    'See reports/tables/day42_model_comparison_summary.csv.',
])
REPORT_PATH.write_text('\n'.join(report_lines), encoding='utf-8')

print()
print('DAY 42 QUALITY SUMMARY')
print(quality_summary.to_string(index=False))
print()
print('DAY 42 DIRECT CLASSIFIER METRICS')
print(metrics.to_string(index=False))
print()
print('DAY 42 DEPLOYMENT BY FOLD')
print(deployment[[
    'fold', 'model', 'n_signals', 'n_signal_runs',
    'directional_precision', 'mean_signed_return_bps',
    'mean_net_return_bps', 'median_net_return_bps',
    'total_net_return_bps',
]].to_string(index=False))
print()
print('DAY 42 MODEL SUMMARY')
print(model_summary.to_string(index=False))
print()
print('[INFO] Day 42 completed successfully.')