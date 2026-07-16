from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# =============================================================================
# Configuration
# =============================================================================

DATA_PATH = Path("data/processed/trade_flow_features.csv")
STRICT_RUNS_PATH = Path("reports/tables/day31_activity_by_run.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")
REPORTS_DIR = Path("reports")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

TUESDAY_BATCH = "weekday_active_tue_day28"
WEDNESDAY_BATCH = "weekday_active_wed_day29"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
ROUND_TRIP_COST_BPS = 1.0
MOVE_C = 0.03
DIRECTION_C = 1.0
RANDOM_STATE = 42

MOVE_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]
DIRECTION_THRESHOLDS = [0.55, 0.60, 0.65, 0.70]

MIN_SIGNALS_PER_FOLD = 5
MIN_SIGNAL_RUNS_PER_FOLD = 3
MIN_TOTAL_SIGNALS = 25
MIN_POSITIVE_FOLDS = 2
MIN_NEIGHBOR_COUNT = 4
MIN_NEIGHBOR_POSITIVE_SHARE = 0.60

PRIMARY_MODEL = "medium_trade_flow"
CHALLENGER_MODEL = "reduced_trade_flow"


# =============================================================================
# Feature sets
# =============================================================================

REDUCED_BOOK_FEATURES = [
    "spread_bps",
    "event_gap_ms",
    "best_bid_qty",
    "best_ask_qty",
    "imbalance_1",
    "imbalance_5",
    "microprice_deviation_bps",
    "quote_changed",
]

MEDIUM_TRADE_FLOW_FEATURES = [
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "trade_volume",
    "buy_trade_volume",
    "sell_trade_volume",
    "signed_trade_volume",
    "avg_trade_size",
    "trade_imbalance",
    "trade_intensity_per_second",
    "trade_count_rolling_sum_5e",
    "trade_count_rolling_sum_20e",
    "trade_count_rolling_sum_50e",
    "trade_volume_rolling_sum_5e",
    "trade_volume_rolling_sum_20e",
    "trade_volume_rolling_sum_50e",
    "signed_trade_volume_rolling_sum_5e",
    "signed_trade_volume_rolling_sum_20e",
    "signed_trade_volume_rolling_sum_50e",
    "trade_imbalance_rolling_5e",
    "trade_imbalance_rolling_20e",
    "trade_imbalance_rolling_50e",
    "trade_intensity_rolling_mean_20e",
    "trade_intensity_rolling_mean_50e",
]

REDUCED_TRADE_FLOW_FEATURES = [
    "trade_count",
    "trade_volume",
    "signed_trade_volume",
    "trade_imbalance",
    "trade_intensity_per_second",
    "trade_count_rolling_sum_10e",
    "trade_count_rolling_sum_20e",
    "trade_count_rolling_sum_50e",
    "trade_volume_rolling_sum_10e",
    "trade_volume_rolling_sum_20e",
    "trade_volume_rolling_sum_50e",
    "signed_trade_volume_rolling_sum_10e",
    "signed_trade_volume_rolling_sum_20e",
    "signed_trade_volume_rolling_sum_50e",
]

MOVE_FEATURE_SETS = {
    PRIMARY_MODEL: MEDIUM_TRADE_FLOW_FEATURES,
    CHALLENGER_MODEL: REDUCED_TRADE_FLOW_FEATURES,
}


@dataclass(frozen=True)
class ChronologicalFold:
    fold_name: str
    train_runs: tuple[str, ...]
    evaluation_runs: tuple[str, ...]
    train_description: str
    evaluation_description: str


def require_columns(
    available_columns: list[str],
    required_columns: list[str],
    source_name: str,
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in available_columns
    ]
    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {missing}"
        )


def make_model(C: float) -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    C=C,
                    class_weight="balanced",
                    solver="liblinear",
                    max_iter=2_000,
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )


def safe_roc_auc(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> float:
    if y_true.nunique() < 2:
        return float("nan")
    return float(roc_auc_score(y_true, probabilities))


def evaluate_classifier(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> dict[str, float | int]:
    predictions = (probabilities >= 0.50).astype(int)
    return {
        "n_observations": int(len(y_true)),
        "positive_class_share": float(y_true.mean()),
        "balanced_accuracy": float(
            balanced_accuracy_score(y_true, predictions)
        ),
        "precision_positive": float(
            precision_score(y_true, predictions, zero_division=0)
        ),
        "recall_positive": float(
            recall_score(y_true, predictions, zero_division=0)
        ),
        "roc_auc": safe_roc_auc(y_true, probabilities),
        "average_precision": float(
            average_precision_score(y_true, probabilities)
        ),
        "brier_score": float(
            brier_score_loss(y_true, probabilities)
        ),
    }


def build_first_eligible_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    move_threshold: float,
    direction_threshold: float,
) -> pd.DataFrame:
    candidates = evaluation_data[
        ["run_name", "row_in_run", "future_return_bps"]
    ].copy()

    candidates["move_probability"] = move_probabilities
    candidates["direction_probability_up"] = direction_probabilities

    move_filter = candidates["move_probability"] >= move_threshold
    long_filter = (
        candidates["direction_probability_up"] >= direction_threshold
    )
    short_filter = (
        candidates["direction_probability_up"]
        <= (1.0 - direction_threshold)
    )

    candidates["signal_direction"] = np.where(
        move_filter & long_filter,
        1,
        np.where(move_filter & short_filter, -1, 0),
    )
    candidates = candidates[
        candidates["signal_direction"] != 0
    ].copy()

    selected_parts: list[pd.DataFrame] = []

    for _, run_candidates in candidates.groupby(
        "run_name", sort=False
    ):
        run_candidates = run_candidates.sort_values("row_in_run")
        selected_indices: list[int] = []
        last_selected_row: int | None = None

        for row_index, row in run_candidates.iterrows():
            current_row = int(row["row_in_run"])
            if (
                last_selected_row is None
                or current_row > last_selected_row + HORIZON
            ):
                selected_indices.append(row_index)
                last_selected_row = current_row

        if selected_indices:
            selected_parts.append(run_candidates.loc[selected_indices])

    if not selected_parts:
        empty = candidates.iloc[0:0].copy()
        empty["signed_return_bps"] = pd.Series(dtype=float)
        empty["net_return_bps"] = pd.Series(dtype=float)
        return empty

    signals = pd.concat(selected_parts, ignore_index=True)
    signals["signed_return_bps"] = (
        signals["signal_direction"] * signals["future_return_bps"]
    )
    signals["net_return_bps"] = (
        signals["signed_return_bps"] - ROUND_TRIP_COST_BPS
    )
    return signals


def evaluate_signals(
    signals: pd.DataFrame,
) -> dict[str, float | int]:
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
            "directional_precision": float("nan"),
            "mean_signed_return_bps": float("nan"),
            "median_signed_return_bps": float("nan"),
            "mean_net_return_bps": float("nan"),
            "median_net_return_bps": float("nan"),
            "trimmed_mean_net_return_bps": float("nan"),
        }

    net_returns = signals["net_return_bps"].to_numpy()
    if len(net_returns) >= 10:
        trim_count = max(1, int(len(net_returns) * 0.10))
        sorted_returns = np.sort(net_returns)
        if len(sorted_returns) - 2 * trim_count > 0:
            trimmed_mean = float(
                sorted_returns[trim_count:-trim_count].mean()
            )
        else:
            trimmed_mean = float(sorted_returns.mean())
    else:
        trimmed_mean = float(net_returns.mean())

    return {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(signals["run_name"].nunique()),
        "directional_precision": float(
            signals["signed_return_bps"].gt(0).mean()
        ),
        "mean_signed_return_bps": float(
            signals["signed_return_bps"].mean()
        ),
        "median_signed_return_bps": float(
            signals["signed_return_bps"].median()
        ),
        "mean_net_return_bps": float(
            signals["net_return_bps"].mean()
        ),
        "median_net_return_bps": float(
            signals["net_return_bps"].median()
        ),
        "trimmed_mean_net_return_bps": trimmed_mean,
    }


def split_runs_in_half(
    runs: list[str],
) -> tuple[list[str], list[str]]:
    midpoint = len(runs) // 2
    return runs[:midpoint], runs[midpoint:]


def create_chronological_folds(
    tuesday_runs: list[str],
    wednesday_runs: list[str],
) -> list[ChronologicalFold]:
    tue_early, tue_late = split_runs_in_half(tuesday_runs)
    wed_early, wed_late = split_runs_in_half(wednesday_runs)

    return [
        ChronologicalFold(
            fold_name="fold_1_tuesday_late",
            train_runs=tuple(tue_early),
            evaluation_runs=tuple(tue_late),
            train_description="first half of strict Tuesday runs",
            evaluation_description="second half of strict Tuesday runs",
        ),
        ChronologicalFold(
            fold_name="fold_2_wednesday_early",
            train_runs=tuple(tuesday_runs),
            evaluation_runs=tuple(wed_early),
            train_description="all strict Tuesday runs",
            evaluation_description="first half of strict Wednesday runs",
        ),
        ChronologicalFold(
            fold_name="fold_3_wednesday_late",
            train_runs=tuple(tuesday_runs + wed_early),
            evaluation_runs=tuple(wed_late),
            train_description=(
                "all Tuesday plus first half of Wednesday"
            ),
            evaluation_description=(
                "second half of strict Wednesday runs"
            ),
        ),
    ]


print("[INFO] Loading strict run metadata...")
strict_metadata = pd.read_csv(
    STRICT_RUNS_PATH,
    usecols=["run_name", "collection_batch", "regime"],
)
strict_metadata["run_name"] = strict_metadata["run_name"].astype(str)
strict_metadata = strict_metadata[
    strict_metadata["collection_batch"].isin(
        [TUESDAY_BATCH, WEDNESDAY_BATCH]
    )
].drop_duplicates(subset=["run_name"], keep="last")

tuesday_runs = sorted(
    strict_metadata.loc[
        strict_metadata["collection_batch"] == TUESDAY_BATCH,
        "run_name",
    ].tolist()
)
wednesday_runs = sorted(
    strict_metadata.loc[
        strict_metadata["collection_batch"] == WEDNESDAY_BATCH,
        "run_name",
    ].tolist()
)

if len(tuesday_runs) < 10 or len(wednesday_runs) < 10:
    raise ValueError(
        "Too few strict Tuesday or Wednesday runs for calibration folds."
    )

folds = create_chronological_folds(tuesday_runs, wednesday_runs)
fold_definitions = pd.DataFrame(
    [
        {
            "fold_name": fold.fold_name,
            "train_description": fold.train_description,
            "evaluation_description": fold.evaluation_description,
            "n_train_runs": len(fold.train_runs),
            "n_evaluation_runs": len(fold.evaluation_runs),
            "first_train_run": fold.train_runs[0],
            "last_train_run": fold.train_runs[-1],
            "first_evaluation_run": fold.evaluation_runs[0],
            "last_evaluation_run": fold.evaluation_runs[-1],
        }
        for fold in folds
    ]
)

print("[INFO] Loading modeling data...")
available_columns = pd.read_csv(DATA_PATH, nrows=0).columns.tolist()
required_features = list(
    dict.fromkeys(
        REDUCED_BOOK_FEATURES
        + MEDIUM_TRADE_FLOW_FEATURES
        + REDUCED_TRADE_FLOW_FEATURES
    )
)
require_columns(
    available_columns,
    ["run_name", "row_in_run", "mid_price"] + required_features,
    "trade_flow_features.csv",
)

data = pd.read_csv(
    DATA_PATH,
    usecols=["run_name", "row_in_run", "mid_price"]
    + required_features,
)
data["run_name"] = data["run_name"].astype(str)
data = data[
    data["run_name"].isin(set(tuesday_runs + wednesday_runs))
].copy()
data = data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)
data[required_features] = data[required_features].replace(
    [np.inf, -np.inf], np.nan
)

future_mid_price = data.groupby(
    "run_name", sort=False
)["mid_price"].shift(-HORIZON)
data["future_return_bps"] = (
    future_mid_price / data["mid_price"] - 1.0
) * 10_000.0

data["move_target"] = np.where(
    data["future_return_bps"].notna(),
    data["future_return_bps"]
    .abs()
    .gt(DEAD_ZONE_BPS)
    .astype(float),
    np.nan,
)
data["direction_target"] = np.where(
    data["future_return_bps"] > DEAD_ZONE_BPS,
    1.0,
    np.where(
        data["future_return_bps"] < -DEAD_ZONE_BPS,
        0.0,
        np.nan,
    ),
)

print("[INFO] Running chronological calibration folds...")
classifier_metric_rows: list[dict[str, object]] = []
threshold_fold_rows: list[dict[str, object]] = []

for fold in folds:
    print(
        f"[INFO] {fold.fold_name}: "
        f"{len(fold.train_runs)} train runs, "
        f"{len(fold.evaluation_runs)} evaluation runs"
    )

    train_data = data[
        data["run_name"].isin(fold.train_runs)
        & data["future_return_bps"].notna()
    ].copy()
    evaluation_data = data[
        data["run_name"].isin(fold.evaluation_runs)
        & data["future_return_bps"].notna()
    ].copy()

    direction_train = train_data[
        train_data["direction_target"].notna()
    ].copy()
    direction_evaluation = evaluation_data[
        evaluation_data["direction_target"].notna()
    ].copy()

    direction_model = make_model(DIRECTION_C)
    direction_model.fit(
        direction_train[REDUCED_BOOK_FEATURES],
        direction_train["direction_target"].astype(int),
    )

    direction_probabilities_all = direction_model.predict_proba(
        evaluation_data[REDUCED_BOOK_FEATURES]
    )[:, 1]
    direction_probabilities_nonflat = direction_model.predict_proba(
        direction_evaluation[REDUCED_BOOK_FEATURES]
    )[:, 1]

    classifier_metric_rows.append(
        {
            "fold_name": fold.fold_name,
            "model_stage": "direction",
            "feature_set": "reduced_book",
            "n_features": len(REDUCED_BOOK_FEATURES),
            "C": DIRECTION_C,
            **evaluate_classifier(
                direction_evaluation["direction_target"].astype(int),
                direction_probabilities_nonflat,
            ),
        }
    )

    for move_feature_set, move_features in MOVE_FEATURE_SETS.items():
        move_model = make_model(MOVE_C)
        move_model.fit(
            train_data[move_features],
            train_data["move_target"].astype(int),
        )
        move_probabilities = move_model.predict_proba(
            evaluation_data[move_features]
        )[:, 1]

        classifier_metric_rows.append(
            {
                "fold_name": fold.fold_name,
                "model_stage": "move",
                "feature_set": move_feature_set,
                "n_features": len(move_features),
                "C": MOVE_C,
                **evaluate_classifier(
                    evaluation_data["move_target"].astype(int),
                    move_probabilities,
                ),
            }
        )

        for move_threshold in MOVE_THRESHOLDS:
            for direction_threshold in DIRECTION_THRESHOLDS:
                signals = build_first_eligible_signals(
                    evaluation_data=evaluation_data,
                    move_probabilities=move_probabilities,
                    direction_probabilities=direction_probabilities_all,
                    move_threshold=move_threshold,
                    direction_threshold=direction_threshold,
                )
                threshold_fold_rows.append(
                    {
                        "fold_name": fold.fold_name,
                        "move_feature_set": move_feature_set,
                        "move_threshold": move_threshold,
                        "direction_threshold": direction_threshold,
                        **evaluate_signals(signals),
                    }
                )

classifier_metrics = pd.DataFrame(classifier_metric_rows)
threshold_fold_results = pd.DataFrame(threshold_fold_rows)

print("[INFO] Aggregating threshold stability...")
support_flags = threshold_fold_results.assign(
    fold_support_ok=lambda frame: (
        frame["n_signals"] >= MIN_SIGNALS_PER_FOLD
    )
    & (frame["n_signal_runs"] >= MIN_SIGNAL_RUNS_PER_FOLD)
)

threshold_summary = (
    support_flags.groupby(
        [
            "move_feature_set",
            "move_threshold",
            "direction_threshold",
        ],
        as_index=False,
    )
    .agg(
        n_folds=("fold_name", "nunique"),
        supported_folds=("fold_support_ok", "sum"),
        positive_mean_net_folds=(
            "mean_net_return_bps",
            lambda values: (values > 0).sum(),
        ),
        total_signals=("n_signals", "sum"),
        minimum_fold_signals=("n_signals", "min"),
        total_signal_runs=("n_signal_runs", "sum"),
        minimum_fold_signal_runs=("n_signal_runs", "min"),
        mean_fold_mean_net_bps=("mean_net_return_bps", "mean"),
        median_fold_mean_net_bps=("mean_net_return_bps", "median"),
        minimum_fold_mean_net_bps=("mean_net_return_bps", "min"),
        maximum_fold_mean_net_bps=("mean_net_return_bps", "max"),
        std_fold_mean_net_bps=("mean_net_return_bps", "std"),
        median_fold_median_net_bps=("median_net_return_bps", "median"),
        median_fold_trimmed_mean_net_bps=(
            "trimmed_mean_net_return_bps",
            "median",
        ),
        mean_directional_precision=("directional_precision", "mean"),
    )
)

threshold_summary["base_eligible"] = (
    threshold_summary["supported_folds"].eq(len(folds))
    & (threshold_summary["total_signals"] >= MIN_TOTAL_SIGNALS)
    & (
        threshold_summary["positive_mean_net_folds"]
        >= MIN_POSITIVE_FOLDS
    )
)


def add_neighborhood_statistics(
    model_summary: pd.DataFrame,
) -> pd.DataFrame:
    model_summary = model_summary.copy()
    counts: list[int] = []
    positive_shares: list[float] = []
    medians: list[float] = []
    minimums: list[float] = []

    for _, row in model_summary.iterrows():
        neighborhood = model_summary[
            (
                model_summary["move_threshold"]
                .sub(row["move_threshold"])
                .abs()
                <= 0.0500001
            )
            & (
                model_summary["direction_threshold"]
                .sub(row["direction_threshold"])
                .abs()
                <= 0.0500001
            )
        ]
        neighborhood = neighborhood[neighborhood["base_eligible"]]
        count = int(len(neighborhood))
        counts.append(count)

        if count == 0:
            positive_shares.append(float("nan"))
            medians.append(float("nan"))
            minimums.append(float("nan"))
        else:
            values = neighborhood["median_fold_mean_net_bps"]
            positive_shares.append(float(values.gt(0).mean()))
            medians.append(float(values.median()))
            minimums.append(float(values.min()))

    model_summary["eligible_neighbor_count"] = counts
    model_summary["neighbor_positive_share"] = positive_shares
    model_summary["neighbor_median_net_bps"] = medians
    model_summary["neighbor_minimum_net_bps"] = minimums
    model_summary["plateau_eligible"] = (
        model_summary["base_eligible"]
        & (
            model_summary["eligible_neighbor_count"]
            >= MIN_NEIGHBOR_COUNT
        )
        & (
            model_summary["neighbor_positive_share"]
            >= MIN_NEIGHBOR_POSITIVE_SHARE
        )
    )
    return model_summary


threshold_summary = pd.concat(
    [
        add_neighborhood_statistics(group)
        for _, group in threshold_summary.groupby(
            "move_feature_set", sort=False
        )
    ],
    ignore_index=True,
)

print("[INFO] Selecting stable thresholds...")
selected_rows: list[pd.Series] = []

for move_feature_set, model_summary in threshold_summary.groupby(
    "move_feature_set", sort=False
):
    plateau_candidates = model_summary[
        model_summary["plateau_eligible"]
    ].copy()

    if not plateau_candidates.empty:
        candidate_pool = plateau_candidates
        selection_source = "plateau_eligible"
    else:
        candidate_pool = model_summary[
            model_summary["base_eligible"]
        ].copy()
        selection_source = "base_eligible_fallback"

    if candidate_pool.empty:
        raise ValueError(
            f"No stable threshold candidate for {move_feature_set}."
        )

    candidate_pool = candidate_pool.sort_values(
        [
            "median_fold_mean_net_bps",
            "minimum_fold_mean_net_bps",
            "neighbor_positive_share",
            "total_signals",
            "move_threshold",
            "direction_threshold",
        ],
        ascending=[False, False, False, False, True, True],
    )

    selected_row = candidate_pool.iloc[0].copy()
    selected_row["selection_source"] = selection_source
    selected_rows.append(selected_row)

selected_thresholds = pd.DataFrame(selected_rows).reset_index(drop=True)
selected_thresholds["model_role"] = np.where(
    selected_thresholds["move_feature_set"].eq(PRIMARY_MODEL),
    "PRIMARY",
    "CHALLENGER",
)

fixed_common_summary = threshold_summary[
    threshold_summary["move_threshold"].eq(0.65)
    & threshold_summary["direction_threshold"].eq(0.65)
].copy()
fixed_common_summary["policy_name"] = "fixed_common_0p65_0p65"

fold_definitions.to_csv(
    TABLES_DIR / "day38_chronological_fold_definitions.csv",
    index=False,
)
classifier_metrics.to_csv(
    TABLES_DIR / "day38_fold_classifier_metrics.csv",
    index=False,
)
threshold_fold_results.to_csv(
    TABLES_DIR / "day38_threshold_fold_results.csv",
    index=False,
)
threshold_summary.to_csv(
    TABLES_DIR / "day38_threshold_stability_summary.csv",
    index=False,
)
selected_thresholds.to_csv(
    TABLES_DIR / "day38_selected_thresholds.csv",
    index=False,
)
fixed_common_summary.to_csv(
    TABLES_DIR / "day38_fixed_common_threshold_summary.csv",
    index=False,
)

selected_lookup = selected_thresholds.set_index(
    "move_feature_set"
).to_dict("index")
primary_selection = selected_lookup[PRIMARY_MODEL]
challenger_selection = selected_lookup[CHALLENGER_MODEL]

preregistration_text = f"""# Day 38 Fresh-Holdout Preregistration

## Purpose

This document freezes the model architecture, threshold-selection rule,
entry rule, cost assumption, and quality protocol before inspecting a new
weekday active-hours holdout batch.

Thursday Day 30 data were not used by the Day 38 threshold-stability script.

## Data used for threshold stability

- Tuesday batch: `{TUESDAY_BATCH}`
- Wednesday batch: `{WEDNESDAY_BATCH}`
- Three expanding chronological calibration folds
- Prediction horizon: {HORIZON} events
- MOVE dead zone: {DEAD_ZONE_BPS:.1f} bps

## Direction stage

- Model: standardized logistic regression
- Features: reduced book set, {len(REDUCED_BOOK_FEATURES)} features
- C: {DIRECTION_C}
- Target: direction conditional on `|future return| > {DEAD_ZONE_BPS:.1f} bps`

## Primary MOVE stage

- Feature set: `{PRIMARY_MODEL}`
- Features: {len(MEDIUM_TRADE_FLOW_FEATURES)}
- C: {MOVE_C}
- MOVE threshold: {primary_selection['move_threshold']:.2f}
- Direction threshold: {primary_selection['direction_threshold']:.2f}
- Selection source: `{primary_selection['selection_source']}`
- Median fold mean net: {primary_selection['median_fold_mean_net_bps']:.6f} bps
- Minimum fold mean net: {primary_selection['minimum_fold_mean_net_bps']:.6f} bps
- Positive folds: {int(primary_selection['positive_mean_net_folds'])} of {len(folds)}

## Challenger MOVE stage

- Feature set: `{CHALLENGER_MODEL}`
- Features: {len(REDUCED_TRADE_FLOW_FEATURES)}
- C: {MOVE_C}
- MOVE threshold: {challenger_selection['move_threshold']:.2f}
- Direction threshold: {challenger_selection['direction_threshold']:.2f}
- Selection source: `{challenger_selection['selection_source']}`
- Median fold mean net: {challenger_selection['median_fold_mean_net_bps']:.6f} bps
- Minimum fold mean net: {challenger_selection['minimum_fold_mean_net_bps']:.6f} bps
- Positive folds: {int(challenger_selection['positive_mean_net_folds'])} of {len(folds)}

## Deployment rule

- Entry rule: first eligible signal
- Cooldown: {HORIZON} events after an entry
- Alternative post-hoc maximum-confidence rules are prohibited
- Round-trip cost assumption: {ROUND_TRIP_COST_BPS:.1f} bps per signal

## Final fitting before the fresh holdout

- Fit primary and challenger models using strict Tuesday and Wednesday runs
- Do not alter features, C values, thresholds, horizon, dead zone, cooldown,
  or cost after seeing fresh-holdout results
- Thursday remains a development diagnostic and is not used by this
  threshold-selection script

## Required fresh-holdout outputs

1. Strict raw and processed run counts
2. MOVE and direction classification metrics
3. Primary and challenger signal counts
4. Signal-bearing runs
5. Directional precision
6. Mean and median signed return
7. Mean and median net return after 1 bps
8. LONG/SHORT breakdown
9. Per-run results
10. Leave-one-run-out and run-cluster bootstrap
11. Cost sensitivity at 0.5, 1.0, 1.5, and 2.0 bps

## Decision rule

The primary model remains the official confirmatory model. The challenger is
reported separately. The challenger cannot replace the primary based only on
the fresh holdout unless the result is explicitly labeled exploratory and is
confirmed on another independent day.
"""

preregistration_path = (
    REPORTS_DIR / "day38_fresh_holdout_preregistration.md"
)
preregistration_path.write_text(
    preregistration_text,
    encoding="utf-8",
)

for model_name in MOVE_FEATURE_SETS:
    plot_data = threshold_summary[
        threshold_summary["move_feature_set"].eq(model_name)
    ].copy()
    pivot = plot_data.pivot(
        index="direction_threshold",
        columns="move_threshold",
        values="median_fold_mean_net_bps",
    )

    plt.figure(figsize=(9, 5))
    image = plt.imshow(
        pivot.to_numpy(),
        aspect="auto",
        origin="lower",
    )
    plt.colorbar(
        image,
        label="Median fold mean net return, bps",
    )
    plt.xticks(
        range(len(pivot.columns)),
        [f"{value:.2f}" for value in pivot.columns],
    )
    plt.yticks(
        range(len(pivot.index)),
        [f"{value:.2f}" for value in pivot.index],
    )
    plt.xlabel("MOVE threshold")
    plt.ylabel("Direction threshold")
    plt.title(f"Day 38 threshold stability: {model_name}")
    plt.tight_layout()
    plt.savefig(
        FIGURES_DIR
        / f"day38_threshold_stability_{model_name}.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()

print()
print("=" * 88)
print("[INFO] Chronological calibration folds")
print("=" * 88)
print(fold_definitions.to_string(index=False))

print()
print("=" * 88)
print("[INFO] Fold classifier metrics")
print("=" * 88)
print(
    classifier_metrics[
        [
            "fold_name",
            "model_stage",
            "feature_set",
            "n_observations",
            "roc_auc",
            "average_precision",
            "brier_score",
        ]
    ].to_string(index=False)
)

print()
print("=" * 88)
print("[INFO] Selected stable thresholds")
print("=" * 88)
print(
    selected_thresholds[
        [
            "model_role",
            "move_feature_set",
            "move_threshold",
            "direction_threshold",
            "selection_source",
            "positive_mean_net_folds",
            "total_signals",
            "median_fold_mean_net_bps",
            "minimum_fold_mean_net_bps",
            "std_fold_mean_net_bps",
            "eligible_neighbor_count",
            "neighbor_positive_share",
            "neighbor_median_net_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 88)
print("[INFO] Fixed common 0.65 / 0.65 pre-Thursday summary")
print("=" * 88)
print(
    fixed_common_summary[
        [
            "move_feature_set",
            "positive_mean_net_folds",
            "total_signals",
            "median_fold_mean_net_bps",
            "minimum_fold_mean_net_bps",
            "std_fold_mean_net_bps",
            "base_eligible",
            "plateau_eligible",
        ]
    ].to_string(index=False)
)

print()
print(
    "[INFO] Preregistration written to: "
    f"{preregistration_path}"
)
print(
    "[INFO] Day 38 threshold-stability and "
    "fresh-holdout preregistration completed successfully."
)