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

DATA_PATH = Path("data/processed/trade_flow_features.csv")
STRICT_RUNS_PATH = Path("reports/tables/day31_activity_by_run.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_BATCH = "weekday_active_tue_day28"
VALIDATION_BATCH = "weekday_active_wed_day29"
TEST_BATCH = "weekday_active_thu_day30"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
ROUND_TRIP_COST_BPS = 1.0
RANDOM_STATE = 42

MIN_VALIDATION_COOLDOWN_SIGNALS = 30
MIN_VALIDATION_SIGNAL_RUNS = 10

C_VALUES = [
    0.01,
    0.03,
    0.10,
    0.30,
    1.00,
    3.00,
    10.00,
]

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

TAIL_FRACTIONS = [
    0.10,
    0.05,
    0.02,
    0.01,
]


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
# Helper functions
# =============================================================================

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


def select_full_trade_flow_features(
    available_columns: list[str],
) -> list[str]:
    selected: list[str] = []

    for column in TRADE_FLOW_BASE_CANDIDATES:
        if column in available_columns:
            selected.append(column)

    for column in available_columns:
        if any(
            column.startswith(prefix)
            for prefix in TRADE_FLOW_ROLLING_PREFIXES
        ):
            selected.append(column)

    return list(dict.fromkeys(selected))


def make_model(c_value: float) -> Pipeline:
    """Smaller C means stronger L2 regularization."""
    return Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(strategy="median"),
            ),
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "model",
                LogisticRegression(
                    penalty="l2",
                    C=c_value,
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
    predictions = (
        probabilities >= 0.50
    ).astype(int)

    return {
        "n_observations": int(len(y_true)),
        "positive_class_share": float(y_true.mean()),
        "predicted_positive_share": float(predictions.mean()),
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


def build_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    return_column: str,
    move_threshold: float,
    direction_threshold: float,
) -> pd.DataFrame:
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

    signals["move_probability"] = move_probabilities
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

    return signals


def apply_cooldown(
    signals: pd.DataFrame,
    cooldown_events: int,
) -> pd.DataFrame:
    if signals.empty:
        return signals.copy()

    ordered = signals.sort_values(
        [
            "run_name",
            "row_in_run",
        ]
    )

    selected_indices: list[int] = []

    for _, run_signals in ordered.groupby(
        "run_name",
        sort=False,
    ):
        last_selected_row: int | None = None

        for index, row in run_signals.iterrows():
            current_row = int(row["row_in_run"])

            if last_selected_row is None:
                selected_indices.append(index)
                last_selected_row = current_row
                continue

            if current_row > (
                last_selected_row + cooldown_events
            ):
                selected_indices.append(index)
                last_selected_row = current_row

    return ordered.loc[selected_indices].copy()


def evaluate_deployment(
    signals: pd.DataFrame,
    n_evaluation_rows: int,
) -> dict[str, float | int]:
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
        }

    signed_returns = signals["signed_return_bps"]
    net_returns = (
        signals["net_return_bps_1bps_cost"]
    )

    return {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(
            signals["run_name"].nunique()
        ),
        "coverage": float(
            len(signals) / n_evaluation_rows
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
    }


def choose_validation_threshold(
    candidate_grid: pd.DataFrame,
) -> pd.Series:
    eligible = candidate_grid[
        candidate_grid["threshold_eligible"]
    ].copy()

    if eligible.empty:
        eligible = candidate_grid.copy()

    eligible["selection_score"] = (
        eligible["mean_net_return_bps"]
        .fillna(-np.inf)
    )

    eligible = eligible.sort_values(
        [
            "selection_score",
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

    return eligible.iloc[0]


def probability_summary(
    probabilities: np.ndarray,
) -> dict[str, float]:
    return {
        "probability_mean": float(np.mean(probabilities)),
        "probability_std": float(
            np.std(probabilities, ddof=1)
        ),
        "probability_p90": float(
            np.quantile(probabilities, 0.90)
        ),
        "probability_p95": float(
            np.quantile(probabilities, 0.95)
        ),
        "probability_p99": float(
            np.quantile(probabilities, 0.99)
        ),
        "probability_max": float(
            np.max(probabilities)
        ),
    }


def tail_summary(
    split_data: pd.DataFrame,
    probabilities: np.ndarray,
    return_column: str,
    tail_fraction: float,
) -> dict[str, float | int]:
    n_tail = max(
        1,
        int(
            np.ceil(
                len(split_data)
                * tail_fraction
            )
        ),
    )

    tail_indices = np.argsort(
        probabilities
    )[-n_tail:]

    tail_data = split_data.iloc[
        tail_indices
    ]

    tail_move_share = float(
        tail_data["move_target"].mean()
    )

    baseline_move_share = float(
        split_data["move_target"].mean()
    )

    return {
        "tail_fraction": tail_fraction,
        "tail_n_rows": n_tail,
        "tail_probability_min": float(
            probabilities[tail_indices].min()
        ),
        "tail_move_share": tail_move_share,
        "baseline_move_share": baseline_move_share,
        "move_share_lift": float(
            tail_move_share
            / baseline_move_share
        ),
        "tail_mean_abs_future_return_bps": float(
            tail_data[return_column]
            .abs()
            .mean()
        ),
        "tail_median_abs_future_return_bps": float(
            tail_data[return_column]
            .abs()
            .median()
        ),
    }


def c_label(c_value: float) -> str:
    return f"{c_value:g}".replace(".", "p")


# =============================================================================
# Load data and build targets
# =============================================================================

print("[INFO] Reading dataset columns...")

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
    + REDUCED_BOOK_FEATURES
    + REDUCED_TRADE_FLOW_FEATURES,
    "trade_flow_features.csv",
)

FULL_TRADE_FLOW_FEATURES = (
    select_full_trade_flow_features(
        available_columns
    )
)

MOVE_FEATURE_SETS = {
    "full_trade_flow": (
        FULL_TRADE_FLOW_FEATURES
    ),
    "reduced_trade_flow": (
        REDUCED_TRADE_FLOW_FEATURES
    ),
}

print(
    "[INFO] Full trade-flow features: "
    f"{len(FULL_TRADE_FLOW_FEATURES)}"
)

print(
    "[INFO] Reduced trade-flow features: "
    f"{len(REDUCED_TRADE_FLOW_FEATURES)}"
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
    strict_metadata["run_name"].astype(str)
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

required_columns = list(
    dict.fromkeys(
        [
            "run_name",
            "row_in_run",
            "mid_price",
        ]
        + REDUCED_BOOK_FEATURES
        + FULL_TRADE_FLOW_FEATURES
    )
)

print("[INFO] Loading modeling data...")

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
        REDUCED_BOOK_FEATURES
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

future_mid = (
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
        future_mid
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
    data["split"].eq("validation")
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
# Fixed reduced-book direction model
# =============================================================================

print(
    "[INFO] Training fixed reduced-book direction model..."
)

DIRECTION_C = 1.0

direction_model = make_model(
    c_value=DIRECTION_C
)

direction_model.fit(
    train_direction[
        REDUCED_BOOK_FEATURES
    ],
    train_direction[
        "direction_target"
    ].astype(int),
)

direction_metric_rows = []

for split_name, split_data in [
    ("validation", validation_direction),
    ("test", test_direction),
]:
    probabilities = (
        direction_model.predict_proba(
            split_data[
                REDUCED_BOOK_FEATURES
            ]
        )[:, 1]
    )

    direction_metric_rows.append(
        {
            "split": split_name,
            "n_features": len(
                REDUCED_BOOK_FEATURES
            ),
            "C": DIRECTION_C,
            **evaluate_classifier(
                split_data[
                    "direction_target"
                ].astype(int),
                probabilities,
            ),
        }
    )

direction_metrics = pd.DataFrame(
    direction_metric_rows
)

validation_direction_probabilities = (
    direction_model.predict_proba(
        validation_all[
            REDUCED_BOOK_FEATURES
        ]
    )[:, 1]
)

test_direction_probabilities = (
    direction_model.predict_proba(
        test_all[
            REDUCED_BOOK_FEATURES
        ]
    )[:, 1]
)


# =============================================================================
# MOVE regularization grid
# =============================================================================

move_metric_rows = []
coefficient_rows = []
probability_rows = []
tail_rows = []
threshold_rows = []
test_rows = []

candidate_predictions: dict[
    str,
    dict[str, np.ndarray],
] = {}

print(
    "[INFO] Training full and reduced MOVE models across C values..."
)

for feature_set_name, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    for c_value in C_VALUES:
        candidate_id = (
            f"{feature_set_name}_C_{c_label(c_value)}"
        )

        print(
            f"[INFO] Fitting {candidate_id}"
        )

        model = make_model(
            c_value=c_value
        )

        model.fit(
            train_all[feature_columns],
            train_all[
                "move_target"
            ].astype(int),
        )

        coefficients = (
            model
            .named_steps["model"]
            .coef_[0]
        )

        coefficient_rows.append(
            {
                "candidate_id": candidate_id,
                "move_feature_set": (
                    feature_set_name
                ),
                "n_features": len(
                    feature_columns
                ),
                "C": c_value,
                "coefficient_l1_norm": float(
                    np.abs(
                        coefficients
                    ).sum()
                ),
                "coefficient_l2_norm": float(
                    np.sqrt(
                        np.square(
                            coefficients
                        ).sum()
                    )
                ),
                "maximum_absolute_coefficient": float(
                    np.abs(
                        coefficients
                    ).max()
                ),
            }
        )

        candidate_predictions[
            candidate_id
        ] = {}

        for split_name, split_data in [
            ("validation", validation_all),
            ("test", test_all),
        ]:
            probabilities = (
                model.predict_proba(
                    split_data[
                        feature_columns
                    ]
                )[:, 1]
            )

            candidate_predictions[
                candidate_id
            ][split_name] = probabilities

            move_metric_rows.append(
                {
                    "candidate_id": candidate_id,
                    "move_feature_set": (
                        feature_set_name
                    ),
                    "n_features": len(
                        feature_columns
                    ),
                    "C": c_value,
                    "split": split_name,
                    **evaluate_classifier(
                        split_data[
                            "move_target"
                        ].astype(int),
                        probabilities,
                    ),
                }
            )

            probability_rows.append(
                {
                    "candidate_id": candidate_id,
                    "move_feature_set": (
                        feature_set_name
                    ),
                    "C": c_value,
                    "split": split_name,
                    **probability_summary(
                        probabilities
                    ),
                }
            )

        validation_move_probabilities = (
            candidate_predictions[
                candidate_id
            ]["validation"]
        )

        candidate_threshold_rows = []

        for move_threshold in (
            MOVE_THRESHOLDS
        ):
            for direction_threshold in (
                DIRECTION_THRESHOLDS
            ):
                raw_signals = build_signals(
                    evaluation_data=(
                        validation_all
                    ),
                    move_probabilities=(
                        validation_move_probabilities
                    ),
                    direction_probabilities=(
                        validation_direction_probabilities
                    ),
                    return_column=RETURN_COLUMN,
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
                    signals=(
                        cooldown_signals
                    ),
                    n_evaluation_rows=len(
                        validation_all
                    ),
                )

                row = {
                    "candidate_id": candidate_id,
                    "move_feature_set": (
                        feature_set_name
                    ),
                    "n_features": len(
                        feature_columns
                    ),
                    "C": c_value,
                    "move_threshold": (
                        move_threshold
                    ),
                    "direction_threshold": (
                        direction_threshold
                    ),
                    "threshold_eligible": (
                        metrics["n_signals"]
                        >= MIN_VALIDATION_COOLDOWN_SIGNALS
                        and metrics[
                            "n_signal_runs"
                        ]
                        >= MIN_VALIDATION_SIGNAL_RUNS
                    ),
                    **metrics,
                }

                threshold_rows.append(row)
                candidate_threshold_rows.append(row)

        selected_threshold = (
            choose_validation_threshold(
                pd.DataFrame(
                    candidate_threshold_rows
                )
            )
        )

        test_raw_signals = build_signals(
            evaluation_data=test_all,
            move_probabilities=(
                candidate_predictions[
                    candidate_id
                ]["test"]
            ),
            direction_probabilities=(
                test_direction_probabilities
            ),
            return_column=RETURN_COLUMN,
            move_threshold=float(
                selected_threshold[
                    "move_threshold"
                ]
            ),
            direction_threshold=float(
                selected_threshold[
                    "direction_threshold"
                ]
            ),
        )

        test_cooldown_signals = (
            apply_cooldown(
                test_raw_signals,
                cooldown_events=HORIZON,
            )
        )

        test_metrics = evaluate_deployment(
            signals=test_cooldown_signals,
            n_evaluation_rows=len(
                test_all
            ),
        )

        test_rows.append(
            {
                "candidate_id": candidate_id,
                "move_feature_set": (
                    feature_set_name
                ),
                "n_features": len(
                    feature_columns
                ),
                "C": c_value,
                "selected_move_threshold": float(
                    selected_threshold[
                        "move_threshold"
                    ]
                ),
                "selected_direction_threshold": float(
                    selected_threshold[
                        "direction_threshold"
                    ]
                ),
                "validation_n_signals": int(
                    selected_threshold[
                        "n_signals"
                    ]
                ),
                "validation_n_signal_runs": int(
                    selected_threshold[
                        "n_signal_runs"
                    ]
                ),
                "validation_mean_net_return_bps": float(
                    selected_threshold[
                        "mean_net_return_bps"
                    ]
                ),
                **test_metrics,
            }
        )

move_metrics = pd.DataFrame(
    move_metric_rows
)

coefficient_norms = pd.DataFrame(
    coefficient_rows
)

probability_distributions = pd.DataFrame(
    probability_rows
)

validation_threshold_grid = pd.DataFrame(
    threshold_rows
)

test_all_c_summary = pd.DataFrame(
    test_rows
)


# =============================================================================
# Select C using Wednesday only
# =============================================================================

selected_c_rows = []

for feature_set_name, feature_set_grid in (
    test_all_c_summary.groupby(
        "move_feature_set",
        sort=False,
    )
):
    selection_grid = (
        feature_set_grid.copy()
    )

    selection_grid["selection_score"] = (
        selection_grid[
            "validation_mean_net_return_bps"
        ].fillna(-np.inf)
    )

    selection_grid = selection_grid.sort_values(
        [
            "selection_score",
            "validation_n_signal_runs",
            "validation_n_signals",
            "C",
        ],
        ascending=[
            False,
            False,
            False,
            True,
        ],
    )

    selected_c_rows.append(
        selection_grid.iloc[0].to_dict()
    )

selected_regularization = pd.DataFrame(
    selected_c_rows
)

primary_candidate_ids = (
    selected_regularization[
        "candidate_id"
    ].tolist()
)

primary_test_summary = (
    test_all_c_summary[
        test_all_c_summary[
            "candidate_id"
        ].isin(primary_candidate_ids)
    ]
    .copy()
    .sort_values(
        "move_feature_set"
    )
)


# =============================================================================
# Probability-tail audit for the selected C values
# =============================================================================

for candidate_id in primary_candidate_ids:
    candidate_row = (
        selected_regularization[
            selected_regularization[
                "candidate_id"
            ].eq(candidate_id)
        ].iloc[0]
    )

    for split_name, split_data in [
        ("validation", validation_all),
        ("test", test_all),
    ]:
        probabilities = (
            candidate_predictions[
                candidate_id
            ][split_name]
        )

        for tail_fraction in (
            TAIL_FRACTIONS
        ):
            tail_rows.append(
                {
                    "candidate_id": candidate_id,
                    "move_feature_set": (
                        candidate_row[
                            "move_feature_set"
                        ]
                    ),
                    "C": float(
                        candidate_row["C"]
                    ),
                    "split": split_name,
                    **tail_summary(
                        split_data=split_data,
                        probabilities=(
                            probabilities
                        ),
                        return_column=RETURN_COLUMN,
                        tail_fraction=(
                            tail_fraction
                        ),
                    ),
                }
            )

selected_tail_summary = pd.DataFrame(
    tail_rows
)


# =============================================================================
# Save outputs
# =============================================================================

direction_metrics.to_csv(
    TABLES_DIR
    / "day35_direction_sanity_metrics.csv",
    index=False,
)

move_metrics.to_csv(
    TABLES_DIR
    / "day35_move_regularization_metrics.csv",
    index=False,
)

coefficient_norms.to_csv(
    TABLES_DIR
    / "day35_move_coefficient_norms.csv",
    index=False,
)

probability_distributions.to_csv(
    TABLES_DIR
    / "day35_probability_distribution_summary.csv",
    index=False,
)

validation_threshold_grid.to_csv(
    TABLES_DIR
    / "day35_validation_threshold_grid.csv",
    index=False,
)

test_all_c_summary.to_csv(
    TABLES_DIR
    / "day35_test_all_c_summary.csv",
    index=False,
)

selected_regularization.to_csv(
    TABLES_DIR
    / "day35_selected_regularization.csv",
    index=False,
)

primary_test_summary.to_csv(
    TABLES_DIR
    / "day35_primary_test_summary.csv",
    index=False,
)

selected_tail_summary.to_csv(
    TABLES_DIR
    / "day35_selected_tail_summary.csv",
    index=False,
)


# =============================================================================
# Figures
# =============================================================================

plt.figure(figsize=(10, 6))

for (
    feature_set_name,
    feature_set_data,
) in move_metrics.groupby(
    "move_feature_set"
):
    for split_name in [
        "validation",
        "test",
    ]:
        plot_data = feature_set_data[
            feature_set_data[
                "split"
            ].eq(split_name)
        ].sort_values("C")

        plt.plot(
            plot_data["C"],
            plot_data["roc_auc"],
            marker="o",
            label=(
                f"{feature_set_name} "
                f"{split_name}"
            ),
        )

plt.xscale("log")
plt.xlabel("Logistic regression C")
plt.ylabel("MOVE ROC-AUC")
plt.title(
    "MOVE model ranking across regularization strengths"
)
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()

plt.savefig(
    FIGURES_DIR
    / "day35_move_auc_vs_c.png",
    dpi=150,
    bbox_inches="tight",
)

plt.close()


plt.figure(figsize=(10, 6))

for (
    feature_set_name,
    feature_set_data,
) in test_all_c_summary.groupby(
    "move_feature_set"
):
    plot_data = feature_set_data.sort_values(
        "C"
    )

    plt.plot(
        plot_data["C"],
        plot_data[
            "mean_net_return_bps"
        ],
        marker="o",
        label=feature_set_name,
    )

plt.axhline(
    0.0,
    linewidth=1,
)
plt.xscale("log")
plt.xlabel("Logistic regression C")
plt.ylabel(
    "Thursday mean net return, bps"
)
plt.title(
    "Thursday deployment after Wednesday threshold selection"
)
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()

plt.savefig(
    FIGURES_DIR
    / "day35_test_net_vs_c.png",
    dpi=150,
    bbox_inches="tight",
)

plt.close()


# =============================================================================
# Console summary
# =============================================================================

print()
print("=" * 80)
print("[INFO] Direction sanity check")
print("=" * 80)
print(
    direction_metrics[
        [
            "split",
            "n_features",
            "C",
            "balanced_accuracy",
            "precision_positive",
            "recall_positive",
            "roc_auc",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] MOVE metrics across C")
print("=" * 80)
print(
    move_metrics[
        [
            "move_feature_set",
            "n_features",
            "C",
            "split",
            "balanced_accuracy",
            "roc_auc",
            "average_precision",
            "brier_score",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] C selected on Wednesday")
print("=" * 80)
print(
    selected_regularization[
        [
            "candidate_id",
            "move_feature_set",
            "n_features",
            "C",
            "selected_move_threshold",
            "selected_direction_threshold",
            "validation_n_signals",
            "validation_n_signal_runs",
            "validation_mean_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Primary Thursday results")
print("=" * 80)
print(
    primary_test_summary[
        [
            "candidate_id",
            "move_feature_set",
            "n_features",
            "C",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_signed_return_bps",
            "mean_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Selected-model probability tails")
print("=" * 80)
print(
    selected_tail_summary[
        [
            "candidate_id",
            "split",
            "tail_fraction",
            "tail_n_rows",
            "tail_move_share",
            "baseline_move_share",
            "move_share_lift",
            "tail_mean_abs_future_return_bps",
        ]
    ].to_string(index=False)
)

print()
print(
    "[INFO] Day 35 regularization and probability-tail audit "
    "completed successfully."
)