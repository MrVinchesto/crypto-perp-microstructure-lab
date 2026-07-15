from __future__ import annotations

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

DATA_PATH = Path(
    "data/processed/trade_flow_features.csv"
)

STRICT_RUNS_PATH = Path(
    "reports/tables/day31_activity_by_run.csv"
)

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")

TABLES_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

FIGURES_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


TRAIN_BATCH = "weekday_active_tue_day28"
VALIDATION_BATCH = "weekday_active_wed_day29"
TEST_BATCH = "weekday_active_thu_day30"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
ROUND_TRIP_COST_BPS = 1.0

MIN_VALIDATION_COOLDOWN_SIGNALS = 30
MIN_VALIDATION_SIGNAL_RUNS = 10

BOOTSTRAP_ITERATIONS = 2_000
RANDOM_STATE = 42


MOVE_THRESHOLDS = [
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
]

DIRECTION_THRESHOLDS = [
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
]


# =============================================================================
# Full and reduced feature sets
# =============================================================================

FULL_BOOK_FEATURES = [
    "spread_bps",
    "event_gap_ms",
    "transaction_lag_ms",
    "best_bid_qty",
    "best_ask_qty",
    "bid_depth_5",
    "ask_depth_5",
    "bid_depth_10",
    "ask_depth_10",
    "imbalance_1",
    "imbalance_5",
    "imbalance_10",
    "microprice_deviation_bps",
    "quote_changed",
]


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


TRADE_FLOW_BASE_CANDIDATES = [
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "trade_volume",
    "buy_trade_volume",
    "sell_trade_volume",
    "signed_trade_volume",
    "trade_notional",
    "buy_trade_notional",
    "sell_trade_notional",
    "signed_trade_notional",
    "avg_trade_size",
    "avg_trade_notional",
    "trade_imbalance",
    "notional_imbalance",
    "trade_intensity_per_second",
]


TRADE_FLOW_ROLLING_PREFIXES = [
    "trade_count_rolling_sum_",
    "trade_volume_rolling_sum_",
    "signed_trade_volume_rolling_sum_",
    "trade_notional_rolling_sum_",
    "signed_trade_notional_rolling_sum_",
    "trade_imbalance_rolling_",
    "notional_imbalance_rolling_",
    "trade_intensity_rolling_mean_",
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


# =============================================================================
# General helper functions
# =============================================================================

def require_columns(
    available_columns: list[str],
    required_columns: list[str],
    source_name: str,
) -> None:
    """Raise a readable error when expected columns are absent."""
    missing_columns = [
        column
        for column in required_columns
        if column not in available_columns
    ]

    if missing_columns:
        raise ValueError(
            f"{source_name} is missing columns: "
            f"{missing_columns}"
        )


def select_full_trade_flow_features(
    available_columns: list[str],
) -> list[str]:
    """
    Reproduce the full Day 33 trade-flow feature selection.
    """
    selected_features: list[str] = []

    for column in TRADE_FLOW_BASE_CANDIDATES:
        if column in available_columns:
            selected_features.append(column)

    for column in available_columns:
        if any(
            column.startswith(prefix)
            for prefix in TRADE_FLOW_ROLLING_PREFIXES
        ):
            selected_features.append(column)

    return list(
        dict.fromkeys(selected_features)
    )


def make_model() -> Pipeline:
    """
    Create the same regularized logistic regression pipeline
    used during Days 32 and 33.
    """
    return Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="median",
                ),
            ),
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "model",
                LogisticRegression(
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
    """Return NaN if ROC-AUC cannot be calculated."""
    if y_true.nunique() < 2:
        return float("nan")

    return float(
        roc_auc_score(
            y_true,
            probabilities,
        )
    )


def evaluate_classifier(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> dict[str, float | int]:
    """
    Calculate standard binary-classification metrics.

    A probability threshold of 0.50 is used only for metrics such
    as balanced accuracy, precision and recall.
    """
    predictions = (
        probabilities >= 0.50
    ).astype(int)

    return {
        "n_observations": int(
            len(y_true)
        ),
        "positive_class_share": float(
            y_true.mean()
        ),
        "predicted_positive_share": float(
            predictions.mean()
        ),
        "balanced_accuracy": float(
            balanced_accuracy_score(
                y_true,
                predictions,
            )
        ),
        "precision_positive": float(
            precision_score(
                y_true,
                predictions,
                pos_label=1,
                zero_division=0,
            )
        ),
        "recall_positive": float(
            recall_score(
                y_true,
                predictions,
                pos_label=1,
                zero_division=0,
            )
        ),
        "precision_negative": float(
            precision_score(
                y_true,
                predictions,
                pos_label=0,
                zero_division=0,
            )
        ),
        "recall_negative": float(
            recall_score(
                y_true,
                predictions,
                pos_label=0,
                zero_division=0,
            )
        ),
        "roc_auc": safe_roc_auc(
            y_true,
            probabilities,
        ),
        "average_precision": float(
            average_precision_score(
                y_true,
                probabilities,
            )
        ),
        "brier_score": float(
            brier_score_loss(
                y_true,
                probabilities,
            )
        ),
    }


# =============================================================================
# Trading signal functions
# =============================================================================

def build_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    return_column: str,
    move_threshold: float,
    direction_threshold: float,
) -> pd.DataFrame:
    """
    Combine MOVE and DIRECTION probabilities.

    Long signal:
        P(MOVE) >= move threshold
        P(UP | MOVE) >= direction threshold

    Short signal:
        P(MOVE) >= move threshold
        P(UP | MOVE) <= 1 - direction threshold
    """
    signals = evaluation_data[
        [
            "run_name",
            "row_in_run",
            "split",
            return_column,
        ]
    ].copy()

    signals = signals.rename(
        columns={
            return_column: "future_return_bps",
        }
    )

    signals["move_probability"] = (
        move_probabilities
    )

    signals["direction_probability_up"] = (
        direction_probabilities
    )

    move_filter = (
        signals["move_probability"]
        >= move_threshold
    )

    long_filter = (
        signals["direction_probability_up"]
        >= direction_threshold
    )

    short_filter = (
        signals["direction_probability_up"]
        <= (1.0 - direction_threshold)
    )

    signals["signal_direction"] = np.where(
        move_filter & long_filter,
        1,
        np.where(
            move_filter & short_filter,
            -1,
            0,
        ),
    )

    signals = signals[
        signals["signal_direction"] != 0
    ].copy()

    signals["signed_return_bps"] = (
        signals["signal_direction"]
        * signals["future_return_bps"]
    )

    signals["net_return_bps_1bps_cost"] = (
        signals["signed_return_bps"]
        - ROUND_TRIP_COST_BPS
    )

    signals["move_threshold"] = (
        move_threshold
    )

    signals["direction_threshold"] = (
        direction_threshold
    )

    return signals


def apply_cooldown(
    signals: pd.DataFrame,
    cooldown_events: int,
) -> pd.DataFrame:
    """
    Keep only the first signal in an overlapping signal episode.

    Once a signal is accepted at row t, another signal from the
    same run is accepted only after t + cooldown_events.
    """
    if signals.empty:
        return signals.copy()

    ordered_signals = signals.sort_values(
        [
            "run_name",
            "row_in_run",
        ]
    )

    selected_indices: list[int] = []

    for _, run_signals in ordered_signals.groupby(
        "run_name",
        sort=False,
    ):
        last_selected_row: int | None = None

        for index, row in run_signals.iterrows():
            current_row = int(
                row["row_in_run"]
            )

            if last_selected_row is None:
                selected_indices.append(index)
                last_selected_row = current_row
                continue

            if current_row > (
                last_selected_row
                + cooldown_events
            ):
                selected_indices.append(index)
                last_selected_row = current_row

    return ordered_signals.loc[
        selected_indices
    ].copy()


def evaluate_deployment(
    signals: pd.DataFrame,
    n_evaluation_rows: int,
    cost_bps: float,
) -> dict[str, float | int]:
    """Evaluate trading signals on all chronological rows."""
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
            "coverage": 0.0,
            "directional_precision": float("nan"),
            "mean_signed_return_bps": float("nan"),
            "median_signed_return_bps": float("nan"),
            "mean_net_return_bps": float("nan"),
            "median_net_return_bps": float("nan"),
            "share_positive_signed_return": float("nan"),
            "share_signed_return_ge_1bps": float("nan"),
            "up_signal_share": float("nan"),
            "down_signal_share": float("nan"),
        }

    signed_returns = (
        signals["signed_return_bps"]
    )

    net_returns = (
        signed_returns - cost_bps
    )

    return {
        "n_signals": int(
            len(signals)
        ),
        "n_signal_runs": int(
            signals["run_name"].nunique()
        ),
        "coverage": float(
            len(signals)
            / n_evaluation_rows
        ),
        "directional_precision": float(
            signed_returns.gt(0).mean()
        ),
        "mean_signed_return_bps": float(
            signed_returns.mean()
        ),
        "median_signed_return_bps": float(
            signed_returns.median()
        ),
        "mean_net_return_bps": float(
            net_returns.mean()
        ),
        "median_net_return_bps": float(
            net_returns.median()
        ),
        "share_positive_signed_return": float(
            signed_returns.gt(0).mean()
        ),
        "share_signed_return_ge_1bps": float(
            signed_returns.ge(1.0).mean()
        ),
        "up_signal_share": float(
            signals["signal_direction"]
            .eq(1)
            .mean()
        ),
        "down_signal_share": float(
            signals["signal_direction"]
            .eq(-1)
            .mean()
        ),
    }


# =============================================================================
# Feature redundancy diagnostics
# =============================================================================

def calculate_correlation_diagnostics(
    data: pd.DataFrame,
    features: list[str],
    feature_set_name: str,
) -> tuple[
    dict[str, float | int | str],
    pd.DataFrame,
]:
    """
    Measure pairwise feature correlation in the Tuesday training data.
    """
    correlation_matrix = (
        data[features]
        .corr()
        .abs()
    )

    upper_triangle_mask = np.triu(
        np.ones(
            correlation_matrix.shape,
            dtype=bool,
        ),
        k=1,
    )

    correlation_pairs = (
        correlation_matrix
        .where(upper_triangle_mask)
        .stack()
        .reset_index()
    )

    correlation_pairs.columns = [
        "feature_1",
        "feature_2",
        "absolute_correlation",
    ]

    correlation_pairs[
        "feature_set"
    ] = feature_set_name

    correlation_pairs = (
        correlation_pairs.sort_values(
            "absolute_correlation",
            ascending=False,
        )
    )

    correlations = correlation_pairs[
        "absolute_correlation"
    ]

    summary = {
        "feature_set": feature_set_name,
        "n_features": len(features),
        "n_feature_pairs": len(
            correlation_pairs
        ),
        "mean_absolute_correlation": float(
            correlations.mean()
        ),
        "median_absolute_correlation": float(
            correlations.median()
        ),
        "maximum_absolute_correlation": float(
            correlations.max()
        ),
        "pairs_ge_0_80": int(
            correlations.ge(0.80).sum()
        ),
        "pairs_ge_0_90": int(
            correlations.ge(0.90).sum()
        ),
        "pairs_ge_0_95": int(
            correlations.ge(0.95).sum()
        ),
        "pairs_ge_0_99": int(
            correlations.ge(0.99).sum()
        ),
    }

    return summary, correlation_pairs


# =============================================================================
# Robustness diagnostics
# =============================================================================

def trimmed_mean(
    values: np.ndarray,
    trim_fraction: float = 0.10,
) -> float:
    """
    Remove the same fraction of extreme observations from both tails.
    """
    sorted_values = np.sort(values)

    trim_count = int(
        len(sorted_values) * trim_fraction
    )

    if trim_count == 0:
        return float(
            sorted_values.mean()
        )

    if (
        len(sorted_values)
        - 2 * trim_count
        <= 0
    ):
        return float(
            sorted_values.mean()
        )

    trimmed_values = sorted_values[
        trim_count:-trim_count
    ]

    return float(
        trimmed_values.mean()
    )


def run_cluster_bootstrap(
    signals: pd.DataFrame,
    n_iterations: int,
    random_state: int,
) -> dict[str, float]:
    """
    Resample entire runs rather than individual signals.

    This preserves clustering of signals within the same five-minute run.
    """
    if signals.empty:
        return {
            "bootstrap_mean_net_bps": float("nan"),
            "bootstrap_median_net_bps": float("nan"),
            "bootstrap_ci_lower_2_5": float("nan"),
            "bootstrap_ci_upper_97_5": float("nan"),
            "bootstrap_share_mean_net_positive": float("nan"),
        }

    run_returns = {
        run_name: (
            run_signals[
                "net_return_bps_1bps_cost"
            ].to_numpy()
        )
        for run_name, run_signals
        in signals.groupby(
            "run_name",
            sort=False,
        )
    }

    run_names = np.array(
        list(run_returns)
    )

    random_generator = (
        np.random.default_rng(
            random_state
        )
    )

    bootstrap_means = np.empty(
        n_iterations,
        dtype=float,
    )

    for iteration in range(
        n_iterations
    ):
        sampled_runs = (
            random_generator.choice(
                run_names,
                size=len(run_names),
                replace=True,
            )
        )

        sampled_returns = np.concatenate(
            [
                run_returns[run_name]
                for run_name
                in sampled_runs
            ]
        )

        bootstrap_means[
            iteration
        ] = sampled_returns.mean()

    return {
        "bootstrap_mean_net_bps": float(
            bootstrap_means.mean()
        ),
        "bootstrap_median_net_bps": float(
            np.median(bootstrap_means)
        ),
        "bootstrap_ci_lower_2_5": float(
            np.quantile(
                bootstrap_means,
                0.025,
            )
        ),
        "bootstrap_ci_upper_97_5": float(
            np.quantile(
                bootstrap_means,
                0.975,
            )
        ),
        "bootstrap_share_mean_net_positive": float(
            (
                bootstrap_means > 0
            ).mean()
        ),
    }


def calculate_robustness_summary(
    signals: pd.DataFrame,
    model_variant: str,
) -> dict[str, object]:
    """
    Examine sensitivity to individual signals and entire runs.
    """
    if signals.empty:
        return {
            "model_variant": model_variant,
            "n_signals": 0,
            "n_signal_runs": 0,
        }

    signed_returns = (
        signals["signed_return_bps"]
    )

    net_returns = (
        signals[
            "net_return_bps_1bps_cost"
        ]
    )

    signed_array = (
        signed_returns.to_numpy()
    )

    net_array = (
        net_returns.to_numpy()
    )

    result: dict[str, object] = {
        "model_variant": model_variant,
        "n_signals": len(signals),
        "n_signal_runs": (
            signals["run_name"].nunique()
        ),
        "mean_signed_return_bps": (
            signed_returns.mean()
        ),
        "median_signed_return_bps": (
            signed_returns.median()
        ),
        "trimmed_mean_signed_return_bps": (
            trimmed_mean(
                signed_array,
                trim_fraction=0.10,
            )
        ),
        "std_signed_return_bps": (
            signed_returns.std(ddof=1)
        ),
        "minimum_signed_return_bps": (
            signed_returns.min()
        ),
        "quantile_10_signed_return_bps": (
            signed_returns.quantile(0.10)
        ),
        "quantile_25_signed_return_bps": (
            signed_returns.quantile(0.25)
        ),
        "quantile_75_signed_return_bps": (
            signed_returns.quantile(0.75)
        ),
        "quantile_90_signed_return_bps": (
            signed_returns.quantile(0.90)
        ),
        "maximum_signed_return_bps": (
            signed_returns.max()
        ),
        "mean_net_return_bps": (
            net_returns.mean()
        ),
        "median_net_return_bps": (
            net_returns.median()
        ),
        "trimmed_mean_net_return_bps": (
            trimmed_mean(
                net_array,
                trim_fraction=0.10,
            )
        ),
    }

    if len(signals) > 1:
        best_signal_index = (
            signed_returns.idxmax()
        )

        worst_signal_index = (
            signed_returns.idxmin()
        )

        result[
            "mean_net_without_best_signal_bps"
        ] = float(
            net_returns.drop(
                best_signal_index
            ).mean()
        )

        result[
            "mean_net_without_worst_signal_bps"
        ] = float(
            net_returns.drop(
                worst_signal_index
            ).mean()
        )

        total_net_return = (
            net_array.sum()
        )

        leave_one_signal_out_means = (
            total_net_return
            - net_array
        ) / (
            len(net_array) - 1
        )

        result[
            "leave_one_signal_out_min_mean_net_bps"
        ] = float(
            leave_one_signal_out_means.min()
        )

        result[
            "leave_one_signal_out_median_mean_net_bps"
        ] = float(
            np.median(
                leave_one_signal_out_means
            )
        )

        result[
            "leave_one_signal_out_max_mean_net_bps"
        ] = float(
            leave_one_signal_out_means.max()
        )

    run_summary = (
        signals.assign(
            net_return_bps=net_returns
        )
        .groupby("run_name")
        .agg(
            run_net_sum=(
                "net_return_bps",
                "sum",
            ),
            run_signal_count=(
                "net_return_bps",
                "size",
            ),
        )
    )

    if len(run_summary) > 1:
        total_net_return = (
            run_summary[
                "run_net_sum"
            ].sum()
        )

        total_signal_count = (
            run_summary[
                "run_signal_count"
            ].sum()
        )

        leave_one_run_out_means = (
            (
                total_net_return
                - run_summary[
                    "run_net_sum"
                ]
            )
            /
            (
                total_signal_count
                - run_summary[
                    "run_signal_count"
                ]
            )
        )

        best_run = (
            run_summary[
                "run_net_sum"
            ].idxmax()
        )

        worst_run = (
            run_summary[
                "run_net_sum"
            ].idxmin()
        )

        result[
            "best_run_name"
        ] = best_run

        result[
            "worst_run_name"
        ] = worst_run

        result[
            "mean_net_without_best_run_bps"
        ] = float(
            leave_one_run_out_means.loc[
                best_run
            ]
        )

        result[
            "mean_net_without_worst_run_bps"
        ] = float(
            leave_one_run_out_means.loc[
                worst_run
            ]
        )

        result[
            "leave_one_run_out_min_mean_net_bps"
        ] = float(
            leave_one_run_out_means.min()
        )

        result[
            "leave_one_run_out_median_mean_net_bps"
        ] = float(
            leave_one_run_out_means.median()
        )

        result[
            "leave_one_run_out_max_mean_net_bps"
        ] = float(
            leave_one_run_out_means.max()
        )

    result.update(
        run_cluster_bootstrap(
            signals=signals,
            n_iterations=(
                BOOTSTRAP_ITERATIONS
            ),
            random_state=(
                RANDOM_STATE
            ),
        )
    )

    return result


# =============================================================================
# Read available columns and define feature sets
# =============================================================================

print(
    "[INFO] Reading available dataset columns..."
)

available_columns = pd.read_csv(
    DATA_PATH,
    nrows=0,
).columns.tolist()


require_columns(
    available_columns,
    [
        "run_name",
        "row_in_run",
        "mid_price",
    ]
    + FULL_BOOK_FEATURES
    + REDUCED_TRADE_FLOW_FEATURES,
    "trade_flow_features.csv",
)


FULL_TRADE_FLOW_FEATURES = (
    select_full_trade_flow_features(
        available_columns
    )
)


if len(FULL_TRADE_FLOW_FEATURES) < 20:
    raise ValueError(
        "Too few full trade-flow features "
        f"were found: {FULL_TRADE_FLOW_FEATURES}"
    )


MOVE_FEATURE_SETS = {
    "full_trade_flow": (
        FULL_TRADE_FLOW_FEATURES
    ),
    "reduced_trade_flow": (
        REDUCED_TRADE_FLOW_FEATURES
    ),
}


DIRECTION_FEATURE_SETS = {
    "full_book": FULL_BOOK_FEATURES,
    "reduced_book": (
        REDUCED_BOOK_FEATURES
    ),
}


print(
    "[INFO] Full book feature count: "
    f"{len(FULL_BOOK_FEATURES)}"
)

print(
    "[INFO] Reduced book feature count: "
    f"{len(REDUCED_BOOK_FEATURES)}"
)

print(
    "[INFO] Full trade-flow feature count: "
    f"{len(FULL_TRADE_FLOW_FEATURES)}"
)

print(
    "[INFO] Reduced trade-flow feature count: "
    f"{len(REDUCED_TRADE_FLOW_FEATURES)}"
)


# =============================================================================
# Load strict Tuesday, Wednesday and Thursday runs
# =============================================================================

print(
    "[INFO] Loading strict run metadata..."
)

strict_metadata = pd.read_csv(
    STRICT_RUNS_PATH,
    usecols=[
        "run_name",
        "collection_batch",
        "regime",
    ],
)

strict_metadata["run_name"] = (
    strict_metadata[
        "run_name"
    ].astype(str)
)

strict_metadata = strict_metadata[
    strict_metadata[
        "collection_batch"
    ].isin(
        [
            TRAIN_BATCH,
            VALIDATION_BATCH,
            TEST_BATCH,
        ]
    )
].drop_duplicates(
    subset=["run_name"],
    keep="last",
)


batch_to_split = {
    TRAIN_BATCH: "train",
    VALIDATION_BATCH: "validation",
    TEST_BATCH: "test",
}


strict_metadata["split"] = (
    strict_metadata[
        "collection_batch"
    ].map(batch_to_split)
)


run_to_split = (
    strict_metadata
    .set_index("run_name")["split"]
    .to_dict()
)


strict_runs = set(
    strict_metadata["run_name"]
)


# =============================================================================
# Load modeling data
# =============================================================================

required_columns = list(
    dict.fromkeys(
        [
            "run_name",
            "row_in_run",
            "mid_price",
        ]
        + FULL_BOOK_FEATURES
        + FULL_TRADE_FLOW_FEATURES
    )
)


print(
    "[INFO] Loading modeling data..."
)

data = pd.read_csv(
    DATA_PATH,
    usecols=required_columns,
)


data["run_name"] = (
    data["run_name"].astype(str)
)


data = data[
    data["run_name"].isin(
        strict_runs
    )
].copy()


data["split"] = (
    data["run_name"].map(
        run_to_split
    )
)


data = data.sort_values(
    [
        "run_name",
        "row_in_run",
    ]
).reset_index(drop=True)


all_features = list(
    dict.fromkeys(
        FULL_BOOK_FEATURES
        + FULL_TRADE_FLOW_FEATURES
    )
)


data[all_features] = (
    data[all_features]
    .replace(
        [np.inf, -np.inf],
        np.nan,
    )
)


# =============================================================================
# Build h50 MOVE and DIRECTION targets
# =============================================================================

print(
    "[INFO] Building h50 targets..."
)


future_mid_price = (
    data
    .groupby(
        "run_name",
        sort=False,
    )["mid_price"]
    .shift(-HORIZON)
)


RETURN_COLUMN = (
    f"future_mid_return_bps_h{HORIZON}"
)


data[RETURN_COLUMN] = (
    (
        future_mid_price
        / data["mid_price"]
    )
    - 1.0
) * 10_000.0


data["move_target"] = np.where(
    data[RETURN_COLUMN].notna(),
    data[RETURN_COLUMN]
    .abs()
    .gt(DEAD_ZONE_BPS)
    .astype(float),
    np.nan,
)


data["direction_target"] = np.where(
    data[RETURN_COLUMN] > DEAD_ZONE_BPS,
    1.0,
    np.where(
        data[RETURN_COLUMN]
        < -DEAD_ZONE_BPS,
        0.0,
        np.nan,
    ),
)


train_all = data[
    data["split"].eq("train")
    & data[RETURN_COLUMN].notna()
].copy()


validation_all = data[
    data["split"].eq(
        "validation"
    )
    & data[RETURN_COLUMN].notna()
].copy()


test_all = data[
    data["split"].eq("test")
    & data[RETURN_COLUMN].notna()
].copy()


train_direction = train_all[
    train_all[
        "direction_target"
    ].notna()
].copy()


validation_direction = validation_all[
    validation_all[
        "direction_target"
    ].notna()
].copy()


test_direction = test_all[
    test_all[
        "direction_target"
    ].notna()
].copy()


# =============================================================================
# Feature redundancy analysis
# =============================================================================

print(
    "[INFO] Calculating feature correlations..."
)


redundancy_summary_rows: list[
    dict[str, object]
] = []


correlation_pair_tables: list[
    pd.DataFrame
] = []


redundancy_feature_sets = {
    "full_book": FULL_BOOK_FEATURES,
    "reduced_book": (
        REDUCED_BOOK_FEATURES
    ),
    "full_trade_flow": (
        FULL_TRADE_FLOW_FEATURES
    ),
    "reduced_trade_flow": (
        REDUCED_TRADE_FLOW_FEATURES
    ),
}


for (
    feature_set_name,
    feature_columns,
) in redundancy_feature_sets.items():
    (
        redundancy_summary,
        correlation_pairs,
    ) = calculate_correlation_diagnostics(
        data=train_all,
        features=feature_columns,
        feature_set_name=(
            feature_set_name
        ),
    )

    redundancy_summary_rows.append(
        redundancy_summary
    )

    correlation_pair_tables.append(
        correlation_pairs.head(30)
    )


feature_redundancy_summary = (
    pd.DataFrame(
        redundancy_summary_rows
    )
)


top_correlated_pairs = pd.concat(
    correlation_pair_tables,
    ignore_index=True,
)


# =============================================================================
# Train MOVE models
# =============================================================================

print(
    "[INFO] Training full and reduced MOVE models..."
)


move_models: dict[str, Pipeline] = {}

move_probability_predictions: dict[
    str,
    dict[str, np.ndarray],
] = {
    "validation": {},
    "test": {},
}


move_metric_rows: list[
    dict[str, object]
] = []


for (
    feature_set_name,
    feature_columns,
) in MOVE_FEATURE_SETS.items():
    model = make_model()

    model.fit(
        train_all[feature_columns],
        train_all[
            "move_target"
        ].astype(int),
    )

    move_models[
        feature_set_name
    ] = model

    for split_name, split_data in [
        (
            "validation",
            validation_all,
        ),
        (
            "test",
            test_all,
        ),
    ]:
        probabilities = (
            model.predict_proba(
                split_data[
                    feature_columns
                ]
            )[:, 1]
        )

        move_probability_predictions[
            split_name
        ][feature_set_name] = (
            probabilities
        )

        move_metric_rows.append(
            {
                "feature_set": (
                    feature_set_name
                ),
                "n_features": len(
                    feature_columns
                ),
                "split": split_name,
                **evaluate_classifier(
                    split_data[
                        "move_target"
                    ].astype(int),
                    probabilities,
                ),
            }
        )


move_metrics = pd.DataFrame(
    move_metric_rows
)


# =============================================================================
# Train DIRECTION models
# =============================================================================

print(
    "[INFO] Training full and reduced direction models..."
)


direction_models: dict[
    str,
    Pipeline,
] = {}


direction_probability_predictions: dict[
    str,
    dict[str, np.ndarray],
] = {
    "validation_all": {},
    "test_all": {},
}


direction_metric_rows: list[
    dict[str, object]
] = []


for (
    feature_set_name,
    feature_columns,
) in DIRECTION_FEATURE_SETS.items():
    model = make_model()

    model.fit(
        train_direction[
            feature_columns
        ],
        train_direction[
            "direction_target"
        ].astype(int),
    )

    direction_models[
        feature_set_name
    ] = model

    for split_name, split_data in [
        (
            "validation",
            validation_direction,
        ),
        (
            "test",
            test_direction,
        ),
    ]:
        probabilities = (
            model.predict_proba(
                split_data[
                    feature_columns
                ]
            )[:, 1]
        )

        direction_metric_rows.append(
            {
                "feature_set": (
                    feature_set_name
                ),
                "n_features": len(
                    feature_columns
                ),
                "split": split_name,
                **evaluate_classifier(
                    split_data[
                        "direction_target"
                    ].astype(int),
                    probabilities,
                ),
            }
        )

    direction_probability_predictions[
        "validation_all"
    ][feature_set_name] = (
        model.predict_proba(
            validation_all[
                feature_columns
            ]
        )[:, 1]
    )

    direction_probability_predictions[
        "test_all"
    ][feature_set_name] = (
        model.predict_proba(
            test_all[
                feature_columns
            ]
        )[:, 1]
    )


direction_metrics = pd.DataFrame(
    direction_metric_rows
)


# =============================================================================
# Define model variants
# =============================================================================

MODEL_VARIANTS = [
    {
        "model_variant": (
            "direction_only_full_book"
        ),
        "move_feature_set": None,
        "direction_feature_set": (
            "full_book"
        ),
    },
    {
        "model_variant": (
            "direction_only_reduced_book"
        ),
        "move_feature_set": None,
        "direction_feature_set": (
            "reduced_book"
        ),
    },
    {
        "model_variant": (
            "two_stage_full_move_full_direction"
        ),
        "move_feature_set": (
            "full_trade_flow"
        ),
        "direction_feature_set": (
            "full_book"
        ),
    },
    {
        "model_variant": (
            "two_stage_reduced_move_full_direction"
        ),
        "move_feature_set": (
            "reduced_trade_flow"
        ),
        "direction_feature_set": (
            "full_book"
        ),
    },
    {
        "model_variant": (
            "two_stage_full_move_reduced_direction"
        ),
        "move_feature_set": (
            "full_trade_flow"
        ),
        "direction_feature_set": (
            "reduced_book"
        ),
    },
    {
        "model_variant": (
            "two_stage_reduced_move_reduced_direction"
        ),
        "move_feature_set": (
            "reduced_trade_flow"
        ),
        "direction_feature_set": (
            "reduced_book"
        ),
    },
]


# =============================================================================
# Validation threshold search
# =============================================================================

print(
    "[INFO] Selecting thresholds on Wednesday..."
)


validation_threshold_rows: list[
    dict[str, object]
] = []


for variant in MODEL_VARIANTS:
    model_variant = variant[
        "model_variant"
    ]

    move_feature_set = variant[
        "move_feature_set"
    ]

    direction_feature_set = variant[
        "direction_feature_set"
    ]

    direction_probabilities = (
        direction_probability_predictions[
            "validation_all"
        ][direction_feature_set]
    )

    if move_feature_set is None:
        move_probabilities = np.ones(
            len(validation_all)
        )

        move_threshold_values = [
            0.0
        ]
    else:
        move_probabilities = (
            move_probability_predictions[
                "validation"
            ][move_feature_set]
        )

        move_threshold_values = (
            MOVE_THRESHOLDS
        )

    for move_threshold in (
        move_threshold_values
    ):
        for direction_threshold in (
            DIRECTION_THRESHOLDS
        ):
            raw_signals = build_signals(
                evaluation_data=(
                    validation_all
                ),
                move_probabilities=(
                    move_probabilities
                ),
                direction_probabilities=(
                    direction_probabilities
                ),
                return_column=(
                    RETURN_COLUMN
                ),
                move_threshold=(
                    move_threshold
                ),
                direction_threshold=(
                    direction_threshold
                ),
            )

            cooldown_signals = (
                apply_cooldown(
                    raw_signals,
                    cooldown_events=(
                        HORIZON
                    ),
                )
            )

            metrics = evaluate_deployment(
                signals=cooldown_signals,
                n_evaluation_rows=len(
                    validation_all
                ),
                cost_bps=(
                    ROUND_TRIP_COST_BPS
                ),
            )

            threshold_eligible = (
                metrics["n_signals"]
                >= MIN_VALIDATION_COOLDOWN_SIGNALS
                and metrics[
                    "n_signal_runs"
                ]
                >= MIN_VALIDATION_SIGNAL_RUNS
            )

            validation_threshold_rows.append(
                {
                    "model_variant": (
                        model_variant
                    ),
                    "move_feature_set": (
                        move_feature_set
                        if move_feature_set
                        is not None
                        else "none"
                    ),
                    "direction_feature_set": (
                        direction_feature_set
                    ),
                    "move_threshold": (
                        move_threshold
                    ),
                    "direction_threshold": (
                        direction_threshold
                    ),
                    "threshold_eligible": (
                        threshold_eligible
                    ),
                    **metrics,
                }
            )


validation_threshold_grid = (
    pd.DataFrame(
        validation_threshold_rows
    )
)


selected_threshold_rows: list[
    dict[str, object]
] = []


for (
    model_variant,
    model_grid,
) in validation_threshold_grid.groupby(
    "model_variant",
    sort=False,
):
    eligible_grid = model_grid[
        model_grid[
            "threshold_eligible"
        ]
    ].copy()

    if eligible_grid.empty:
        print(
            "[WARNING] No eligible threshold "
            f"for {model_variant}."
        )

        candidate_grid = (
            model_grid.copy()
        )
    else:
        candidate_grid = (
            eligible_grid
        )

    candidate_grid = (
        candidate_grid.sort_values(
            [
                "mean_net_return_bps",
                "n_signal_runs",
                "n_signals",
                "move_threshold",
                "direction_threshold",
            ],
            ascending=[
                False,
                False,
                False,
                True,
                True,
            ],
        )
    )

    selected_threshold_rows.append(
        candidate_grid.iloc[
            0
        ].to_dict()
    )


selected_thresholds = pd.DataFrame(
    selected_threshold_rows
)


# =============================================================================
# Apply selected thresholds to Thursday
# =============================================================================

print(
    "[INFO] Evaluating selected models on Thursday..."
)


test_summary_rows: list[
    dict[str, object]
] = []


selected_test_signals: dict[
    str,
    pd.DataFrame,
] = {}


for _, selected in (
    selected_thresholds.iterrows()
):
    model_variant = str(
        selected["model_variant"]
    )

    move_feature_set = str(
        selected["move_feature_set"]
    )

    direction_feature_set = str(
        selected[
            "direction_feature_set"
        ]
    )

    move_threshold = float(
        selected["move_threshold"]
    )

    direction_threshold = float(
        selected[
            "direction_threshold"
        ]
    )

    direction_probabilities = (
        direction_probability_predictions[
            "test_all"
        ][direction_feature_set]
    )

    if move_feature_set == "none":
        move_probabilities = np.ones(
            len(test_all)
        )
    else:
        move_probabilities = (
            move_probability_predictions[
                "test"
            ][move_feature_set]
        )

    raw_signals = build_signals(
        evaluation_data=test_all,
        move_probabilities=(
            move_probabilities
        ),
        direction_probabilities=(
            direction_probabilities
        ),
        return_column=RETURN_COLUMN,
        move_threshold=move_threshold,
        direction_threshold=(
            direction_threshold
        ),
    )

    cooldown_signals = apply_cooldown(
        raw_signals,
        cooldown_events=HORIZON,
    )

    cooldown_signals[
        "model_variant"
    ] = model_variant

    cooldown_signals[
        "move_feature_set"
    ] = move_feature_set

    cooldown_signals[
        "direction_feature_set"
    ] = direction_feature_set

    selected_test_signals[
        model_variant
    ] = cooldown_signals

    test_metrics = evaluate_deployment(
        signals=cooldown_signals,
        n_evaluation_rows=len(
            test_all
        ),
        cost_bps=(
            ROUND_TRIP_COST_BPS
        ),
    )

    test_summary_rows.append(
        {
            "model_variant": (
                model_variant
            ),
            "move_feature_set": (
                move_feature_set
            ),
            "direction_feature_set": (
                direction_feature_set
            ),
            "selected_move_threshold": (
                move_threshold
            ),
            "selected_direction_threshold": (
                direction_threshold
            ),
            **test_metrics,
        }
    )


test_summary = pd.DataFrame(
    test_summary_rows
)


baseline_row = test_summary[
    test_summary[
        "model_variant"
    ].eq(
        "direction_only_full_book"
    )
].iloc[0]


test_comparison = (
    test_summary.copy()
)


test_comparison[
    "delta_n_signals_vs_full_direction_only"
] = (
    test_comparison["n_signals"]
    - baseline_row["n_signals"]
)


test_comparison[
    "delta_directional_precision_vs_full_direction_only"
] = (
    test_comparison[
        "directional_precision"
    ]
    - baseline_row[
        "directional_precision"
    ]
)


test_comparison[
    "delta_gross_bps_vs_full_direction_only"
] = (
    test_comparison[
        "mean_signed_return_bps"
    ]
    - baseline_row[
        "mean_signed_return_bps"
    ]
)


test_comparison[
    "delta_net_bps_vs_full_direction_only"
] = (
    test_comparison[
        "mean_net_return_bps"
    ]
    - baseline_row[
        "mean_net_return_bps"
    ]
)


# =============================================================================
# Robustness audit
# =============================================================================

print(
    "[INFO] Running signal robustness audit..."
)


robustness_rows: list[
    dict[str, object]
] = []


for (
    model_variant,
    signals,
) in selected_test_signals.items():
    robustness_rows.append(
        calculate_robustness_summary(
            signals=signals,
            model_variant=model_variant,
        )
    )


robustness_summary = pd.DataFrame(
    robustness_rows
)


# =============================================================================
# Thursday threshold-neighborhood diagnostic
# =============================================================================

print(
    "[INFO] Evaluating neighboring thresholds..."
)


def get_neighboring_values(
    selected_value: float,
    allowed_values: list[float],
) -> list[float]:
    """
    Return the selected threshold and one adjacent value on each side.
    """
    selected_index = min(
        range(len(allowed_values)),
        key=lambda index: abs(
            allowed_values[index]
            - selected_value
        ),
    )

    neighboring_indices = {
        selected_index,
        max(
            0,
            selected_index - 1,
        ),
        min(
            len(allowed_values) - 1,
            selected_index + 1,
        ),
    }

    return [
        allowed_values[index]
        for index
        in sorted(neighboring_indices)
    ]


neighborhood_rows: list[
    dict[str, object]
] = []


for _, selected in (
    selected_thresholds.iterrows()
):
    model_variant = str(
        selected["model_variant"]
    )

    move_feature_set = str(
        selected["move_feature_set"]
    )

    direction_feature_set = str(
        selected[
            "direction_feature_set"
        ]
    )

    selected_move_threshold = float(
        selected["move_threshold"]
    )

    selected_direction_threshold = float(
        selected[
            "direction_threshold"
        ]
    )

    if move_feature_set == "none":
        move_threshold_values = [
            0.0
        ]

        move_probabilities = np.ones(
            len(test_all)
        )
    else:
        move_threshold_values = (
            get_neighboring_values(
                selected_value=(
                    selected_move_threshold
                ),
                allowed_values=(
                    MOVE_THRESHOLDS
                ),
            )
        )

        move_probabilities = (
            move_probability_predictions[
                "test"
            ][move_feature_set]
        )

    direction_threshold_values = (
        get_neighboring_values(
            selected_value=(
                selected_direction_threshold
            ),
            allowed_values=(
                DIRECTION_THRESHOLDS
            ),
        )
    )

    direction_probabilities = (
        direction_probability_predictions[
            "test_all"
        ][direction_feature_set]
    )

    for move_threshold in (
        move_threshold_values
    ):
        for direction_threshold in (
            direction_threshold_values
        ):
            raw_signals = build_signals(
                evaluation_data=test_all,
                move_probabilities=(
                    move_probabilities
                ),
                direction_probabilities=(
                    direction_probabilities
                ),
                return_column=(
                    RETURN_COLUMN
                ),
                move_threshold=(
                    move_threshold
                ),
                direction_threshold=(
                    direction_threshold
                ),
            )

            cooldown_signals = (
                apply_cooldown(
                    raw_signals,
                    cooldown_events=(
                        HORIZON
                    ),
                )
            )

            metrics = evaluate_deployment(
                signals=cooldown_signals,
                n_evaluation_rows=len(
                    test_all
                ),
                cost_bps=(
                    ROUND_TRIP_COST_BPS
                ),
            )

            neighborhood_rows.append(
                {
                    "model_variant": (
                        model_variant
                    ),
                    "move_threshold": (
                        move_threshold
                    ),
                    "direction_threshold": (
                        direction_threshold
                    ),
                    "is_validation_selected_combination": (
                        np.isclose(
                            move_threshold,
                            selected_move_threshold,
                        )
                        and np.isclose(
                            direction_threshold,
                            selected_direction_threshold,
                        )
                    ),
                    **metrics,
                }
            )


threshold_neighborhood = pd.DataFrame(
    neighborhood_rows
)


neighborhood_summary = (
    threshold_neighborhood
    .groupby(
        "model_variant",
        as_index=False,
    )
    .agg(
        n_neighbor_combinations=(
            "mean_net_return_bps",
            "size",
        ),
        positive_net_combinations=(
            "mean_net_return_bps",
            lambda values: (
                values > 0
            ).sum(),
        ),
        share_positive_net_combinations=(
            "mean_net_return_bps",
            lambda values: (
                values > 0
            ).mean(),
        ),
        minimum_neighbor_net_bps=(
            "mean_net_return_bps",
            "min",
        ),
        median_neighbor_net_bps=(
            "mean_net_return_bps",
            "median",
        ),
        maximum_neighbor_net_bps=(
            "mean_net_return_bps",
            "max",
        ),
        minimum_neighbor_signals=(
            "n_signals",
            "min",
        ),
        maximum_neighbor_signals=(
            "n_signals",
            "max",
        ),
    )
)


# =============================================================================
# Save standardized coefficients
# =============================================================================

coefficient_rows: list[
    dict[str, object]
] = []


for (
    feature_set_name,
    model,
) in move_models.items():
    feature_columns = (
        MOVE_FEATURE_SETS[
            feature_set_name
        ]
    )

    coefficients = (
        model
        .named_steps["model"]
        .coef_[0]
    )

    for feature, coefficient in zip(
        feature_columns,
        coefficients,
    ):
        coefficient_rows.append(
            {
                "model_type": "move",
                "feature_set": (
                    feature_set_name
                ),
                "feature": feature,
                "coefficient": (
                    coefficient
                ),
                "absolute_coefficient": (
                    abs(coefficient)
                ),
            }
        )


for (
    feature_set_name,
    model,
) in direction_models.items():
    feature_columns = (
        DIRECTION_FEATURE_SETS[
            feature_set_name
        ]
    )

    coefficients = (
        model
        .named_steps["model"]
        .coef_[0]
    )

    for feature, coefficient in zip(
        feature_columns,
        coefficients,
    ):
        coefficient_rows.append(
            {
                "model_type": (
                    "direction"
                ),
                "feature_set": (
                    feature_set_name
                ),
                "feature": feature,
                "coefficient": (
                    coefficient
                ),
                "absolute_coefficient": (
                    abs(coefficient)
                ),
            }
        )


model_coefficients = (
    pd.DataFrame(
        coefficient_rows
    )
    .sort_values(
        [
            "model_type",
            "feature_set",
            "absolute_coefficient",
        ],
        ascending=[
            True,
            True,
            False,
        ],
    )
)


# =============================================================================
# Save tables
# =============================================================================

feature_redundancy_summary.to_csv(
    TABLES_DIR
    / "day34_feature_redundancy_summary.csv",
    index=False,
)


top_correlated_pairs.to_csv(
    TABLES_DIR
    / "day34_top_correlated_feature_pairs.csv",
    index=False,
)


move_metrics.to_csv(
    TABLES_DIR
    / "day34_move_model_metrics.csv",
    index=False,
)


direction_metrics.to_csv(
    TABLES_DIR
    / "day34_direction_model_metrics.csv",
    index=False,
)


validation_threshold_grid.to_csv(
    TABLES_DIR
    / "day34_validation_threshold_grid.csv",
    index=False,
)


selected_thresholds.to_csv(
    TABLES_DIR
    / "day34_selected_thresholds.csv",
    index=False,
)


test_summary.to_csv(
    TABLES_DIR
    / "day34_test_deployment_summary.csv",
    index=False,
)


test_comparison.to_csv(
    TABLES_DIR
    / "day34_test_model_comparison.csv",
    index=False,
)


robustness_summary.to_csv(
    TABLES_DIR
    / "day34_robustness_summary.csv",
    index=False,
)


threshold_neighborhood.to_csv(
    TABLES_DIR
    / "day34_test_threshold_neighborhood.csv",
    index=False,
)


neighborhood_summary.to_csv(
    TABLES_DIR
    / "day34_threshold_neighborhood_summary.csv",
    index=False,
)


model_coefficients.to_csv(
    TABLES_DIR
    / "day34_model_coefficients.csv",
    index=False,
)


all_selected_signals = pd.concat(
    list(
        selected_test_signals.values()
    ),
    ignore_index=True,
)


all_selected_signals.to_csv(
    TABLES_DIR
    / "day34_selected_test_signals.csv",
    index=False,
)


# =============================================================================
# Create figures
# =============================================================================

move_plot = move_metrics.copy()

move_plot["label"] = (
    move_plot["feature_set"]
    + "_"
    + move_plot["split"]
)


plt.figure(
    figsize=(10, 6)
)

plt.bar(
    move_plot["label"],
    move_plot["roc_auc"],
)

plt.axhline(
    0.50,
    linewidth=1,
)

plt.ylabel("MOVE ROC-AUC")
plt.title(
    "Full versus reduced MOVE models"
)
plt.xticks(rotation=25)
plt.grid(
    axis="y",
    alpha=0.3,
)
plt.tight_layout()


move_figure_path = (
    FIGURES_DIR
    / "day34_move_full_vs_reduced_roc_auc.png"
)


plt.savefig(
    move_figure_path,
    dpi=150,
    bbox_inches="tight",
)

plt.close()


plt.figure(
    figsize=(13, 6)
)

plt.bar(
    test_summary["model_variant"],
    test_summary[
        "mean_net_return_bps"
    ],
)

plt.axhline(
    0.0,
    linewidth=1,
)

plt.ylabel(
    "Thursday mean net return, bps"
)

plt.title(
    "Full versus reduced two-stage models"
)

plt.xticks(
    rotation=30,
    ha="right",
)

plt.grid(
    axis="y",
    alpha=0.3,
)

plt.tight_layout()


test_figure_path = (
    FIGURES_DIR
    / "day34_test_net_full_vs_reduced.png"
)


plt.savefig(
    test_figure_path,
    dpi=150,
    bbox_inches="tight",
)

plt.close()


# =============================================================================
# Console output
# =============================================================================

print()
print("=" * 80)
print(
    "[INFO] Feature redundancy summary"
)
print("=" * 80)

print(
    feature_redundancy_summary
    .to_string(index=False)
)


print()
print("=" * 80)
print(
    "[INFO] Full versus reduced MOVE metrics"
)
print("=" * 80)

print(
    move_metrics[
        [
            "feature_set",
            "n_features",
            "split",
            "balanced_accuracy",
            "precision_positive",
            "recall_positive",
            "roc_auc",
            "average_precision",
            "brier_score",
        ]
    ].to_string(index=False)
)


print()
print("=" * 80)
print(
    "[INFO] Full versus reduced DIRECTION metrics"
)
print("=" * 80)

print(
    direction_metrics[
        [
            "feature_set",
            "n_features",
            "split",
            "balanced_accuracy",
            "precision_positive",
            "recall_positive",
            "roc_auc",
        ]
    ].to_string(index=False)
)


print()
print("=" * 80)
print(
    "[INFO] Selected Wednesday thresholds"
)
print("=" * 80)

print(
    selected_thresholds[
        [
            "model_variant",
            "move_threshold",
            "direction_threshold",
            "n_signals",
            "n_signal_runs",
            "mean_signed_return_bps",
            "mean_net_return_bps",
        ]
    ].to_string(index=False)
)


print()
print("=" * 80)
print(
    "[INFO] Thursday model comparison"
)
print("=" * 80)

print(
    test_comparison.to_string(
        index=False
    )
)


print()
print("=" * 80)
print(
    "[INFO] Thursday robustness summary"
)
print("=" * 80)

print(
    robustness_summary[
        [
            "model_variant",
            "n_signals",
            "n_signal_runs",
            "mean_net_return_bps",
            "median_net_return_bps",
            "trimmed_mean_net_return_bps",
            "mean_net_without_best_signal_bps",
            "mean_net_without_best_run_bps",
            "bootstrap_ci_lower_2_5",
            "bootstrap_ci_upper_97_5",
            "bootstrap_share_mean_net_positive",
        ]
    ].to_string(index=False)
)


print()
print("=" * 80)
print(
    "[INFO] Threshold neighborhood summary"
)
print("=" * 80)

print(
    neighborhood_summary.to_string(
        index=False
    )
)


print()
print(
    "[INFO] Day 34 robustness audit "
    "completed successfully."
)