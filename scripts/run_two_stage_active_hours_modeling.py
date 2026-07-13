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

# Minimum validation support required before a threshold combination
# can be selected.
MIN_VALIDATION_COOLDOWN_SIGNALS = 30
MIN_VALIDATION_SIGNAL_RUNS = 10

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

COST_LEVELS_BPS = [
    0.0,
    0.5,
    1.0,
    2.0,
    3.0,
]


BOOK_FEATURES = [
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


# =============================================================================
# Helper functions
# =============================================================================

def require_columns(
    columns: list[str],
    required: list[str],
    source_name: str,
) -> None:
    """Raise a clear error if required columns are missing."""
    missing = [
        column
        for column in required
        if column not in columns
    ]

    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {missing}"
        )


def choose_trade_flow_features(
    columns: list[str],
) -> list[str]:
    """
    Select known trade-flow features that really exist in the dataset.
    """
    selected: list[str] = []

    for column in TRADE_FLOW_BASE_CANDIDATES:
        if column in columns:
            selected.append(column)

    for column in columns:
        if any(
            column.startswith(prefix)
            for prefix in TRADE_FLOW_ROLLING_PREFIXES
        ):
            selected.append(column)

    return list(dict.fromkeys(selected))


def make_model() -> Pipeline:
    """
    Create a regularized logistic-regression pipeline.

    The imputer and scaler are fitted only on the training data because
    they are inside the sklearn Pipeline.
    """
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
                    class_weight="balanced",
                    solver="liblinear",
                    max_iter=2_000,
                    random_state=42,
                ),
            ),
        ]
    )


def safe_roc_auc(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> float:
    """ROC-AUC is undefined when only one class is present."""
    if y_true.nunique() < 2:
        return float("nan")

    return float(
        roc_auc_score(
            y_true,
            probabilities,
        )
    )


def evaluate_binary_model(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> dict[str, float | int]:
    """
    Evaluate a binary classifier at the default probability threshold
    of 0.50.
    """
    predictions = (
        probabilities >= 0.50
    ).astype(int)

    return {
        "n_observations": int(len(y_true)),
        "positive_class_share": float(y_true.mean()),
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


def build_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    return_column: str,
    move_threshold: float,
    direction_threshold: float,
) -> pd.DataFrame:
    """
    Create trading signals from the two-stage model.

    A signal is generated only if:

    1. P(MOVE) >= move_threshold
    2. the direction model is sufficiently confident

    Long:
        P(UP | MOVE) >= direction_threshold

    Short:
        P(UP | MOVE) <= 1 - direction_threshold
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

    signals["move_probability"] = move_probabilities
    signals["direction_probability_up"] = (
        direction_probabilities
    )

    move_filter = (
        signals["move_probability"] >= move_threshold
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

    signals["move_threshold"] = move_threshold
    signals["direction_threshold"] = direction_threshold

    return signals


def apply_cooldown(
    signals: pd.DataFrame,
    cooldown_events: int,
) -> pd.DataFrame:
    """
    Keep only the first signal in each overlapping signal episode.

    After selecting a signal, the next signal from the same run can only
    be selected after the prediction horizon has passed.
    """
    if signals.empty:
        return signals.copy()

    selected_indices: list[int] = []

    ordered = signals.sort_values(
        [
            "run_name",
            "row_in_run",
        ]
    )

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
    cost_bps: float,
) -> dict[str, float | int]:
    """
    Evaluate selected signals on the full chronological sample,
    including flat and sub-cost future movements.
    """
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
            "coverage": 0.0,
            "directional_precision": float("nan"),
            "mean_signed_return_bps": float("nan"),
            "median_signed_return_bps": float("nan"),
            "mean_net_return_bps": float("nan"),
            "share_signed_return_ge_1bps": float("nan"),
            "up_signal_share": float("nan"),
            "down_signal_share": float("nan"),
        }

    signed_returns = signals["signed_return_bps"]

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
            (
                signed_returns
                - cost_bps
            ).mean()
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


def summarize_by_run(
    signals: pd.DataFrame,
    model_variant: str,
) -> list[dict[str, object]]:
    """Create one result row per run containing signals."""
    rows: list[dict[str, object]] = []

    for run_name, run_signals in signals.groupby(
        "run_name",
        sort=False,
    ):
        signed_returns = run_signals[
            "signed_return_bps"
        ]

        rows.append(
            {
                "model_variant": model_variant,
                "run_name": run_name,
                "n_signals": len(run_signals),
                "directional_precision": (
                    signed_returns.gt(0).mean()
                ),
                "mean_signed_return_bps": (
                    signed_returns.mean()
                ),
                "mean_net_return_bps_1bps_cost": (
                    (
                        signed_returns
                        - ROUND_TRIP_COST_BPS
                    ).mean()
                ),
                "up_signal_share": (
                    run_signals[
                        "signal_direction"
                    ].eq(1).mean()
                ),
                "down_signal_share": (
                    run_signals[
                        "signal_direction"
                    ].eq(-1).mean()
                ),
            }
        )

    return rows


def summarize_by_direction(
    signals: pd.DataFrame,
    model_variant: str,
) -> list[dict[str, object]]:
    """Evaluate long and short signals separately."""
    rows: list[dict[str, object]] = []

    direction_names = {
        1: "UP",
        -1: "DOWN",
    }

    for direction, direction_name in (
        direction_names.items()
    ):
        subset = signals[
            signals["signal_direction"].eq(direction)
        ]

        if subset.empty:
            continue

        signed_returns = subset["signed_return_bps"]

        rows.append(
            {
                "model_variant": model_variant,
                "direction": direction_name,
                "n_signals": len(subset),
                "n_runs": subset[
                    "run_name"
                ].nunique(),
                "directional_precision": (
                    signed_returns.gt(0).mean()
                ),
                "mean_signed_return_bps": (
                    signed_returns.mean()
                ),
                "mean_net_return_bps_1bps_cost": (
                    (
                        signed_returns
                        - ROUND_TRIP_COST_BPS
                    ).mean()
                ),
            }
        )

    return rows


# =============================================================================
# Inspect available dataset columns
# =============================================================================

print("[INFO] Reading dataset columns...")

all_columns = pd.read_csv(
    DATA_PATH,
    nrows=0,
).columns.tolist()

require_columns(
    all_columns,
    [
        "run_name",
        "row_in_run",
        "mid_price",
    ] + BOOK_FEATURES,
    "trade_flow_features.csv",
)

TRADE_FLOW_FEATURES = choose_trade_flow_features(
    all_columns
)

if len(TRADE_FLOW_FEATURES) < 10:
    raise ValueError(
        "Too few trade-flow features were found. "
        f"Found: {TRADE_FLOW_FEATURES}"
    )


MOVE_FEATURE_SETS = {
    "book_only": BOOK_FEATURES,
    "trade_flow_only": TRADE_FLOW_FEATURES,
    "combined": (
        BOOK_FEATURES
        + TRADE_FLOW_FEATURES
    ),
}

# Day 32 showed that book-only was the most stable directional model.
DIRECTION_FEATURES = BOOK_FEATURES


print(
    f"[INFO] Book features: {len(BOOK_FEATURES)}"
)
print(
    f"[INFO] Trade-flow features: "
    f"{len(TRADE_FLOW_FEATURES)}"
)


# =============================================================================
# Load strict run metadata from Day 31
# =============================================================================

print("[INFO] Loading strict run metadata...")

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
    strict_metadata["collection_batch"].isin(
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
    strict_metadata["collection_batch"]
    .map(batch_to_split)
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
        + BOOK_FEATURES
        + TRADE_FLOW_FEATURES
    )
)

print("[INFO] Loading modeling data...")

data = pd.read_csv(
    DATA_PATH,
    usecols=required_columns,
)

data["run_name"] = data["run_name"].astype(str)

data = data[
    data["run_name"].isin(strict_runs)
].copy()

data["split"] = (
    data["run_name"].map(run_to_split)
)

data = data.sort_values(
    [
        "run_name",
        "row_in_run",
    ]
).reset_index(drop=True)

all_feature_columns = list(
    dict.fromkeys(
        BOOK_FEATURES
        + TRADE_FLOW_FEATURES
    )
)

data[all_feature_columns] = (
    data[all_feature_columns]
    .replace(
        [np.inf, -np.inf],
        np.nan,
    )
)


# =============================================================================
# Build h50 future return and both targets
# =============================================================================

print("[INFO] Building h50 targets...")

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


# Stage 1 target:
# 1 = future movement exceeds 1 bps in either direction
# 0 = future movement is no larger than 1 bps
data["move_target"] = np.where(
    data[RETURN_COLUMN].notna(),
    data[RETURN_COLUMN]
    .abs()
    .gt(DEAD_ZONE_BPS)
    .astype(float),
    np.nan,
)


# Stage 2 target:
# 1 = UP move larger than 1 bps
# 0 = DOWN move larger than 1 bps
# NaN = flat/sub-cost observation
data["direction_target"] = np.where(
    data[RETURN_COLUMN] > DEAD_ZONE_BPS,
    1.0,
    np.where(
        data[RETURN_COLUMN] < -DEAD_ZONE_BPS,
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
    train_all["direction_target"].notna()
].copy()

validation_direction = validation_all[
    validation_all["direction_target"].notna()
].copy()

test_direction = test_all[
    test_all["direction_target"].notna()
].copy()


# =============================================================================
# Split summary
# =============================================================================

split_summary = pd.DataFrame(
    [
        {
            "horizon_events": HORIZON,
            "train_runs": train_all[
                "run_name"
            ].nunique(),
            "validation_runs": validation_all[
                "run_name"
            ].nunique(),
            "test_runs": test_all[
                "run_name"
            ].nunique(),
            "train_all_rows": len(train_all),
            "validation_all_rows": len(
                validation_all
            ),
            "test_all_rows": len(test_all),
            "train_move_rows": int(
                train_all["move_target"].sum()
            ),
            "validation_move_rows": int(
                validation_all[
                    "move_target"
                ].sum()
            ),
            "test_move_rows": int(
                test_all["move_target"].sum()
            ),
            "train_move_share": (
                train_all["move_target"].mean()
            ),
            "validation_move_share": (
                validation_all[
                    "move_target"
                ].mean()
            ),
            "test_move_share": (
                test_all["move_target"].mean()
            ),
            "train_up_share_among_moves": (
                train_direction[
                    "direction_target"
                ].mean()
            ),
            "validation_up_share_among_moves": (
                validation_direction[
                    "direction_target"
                ].mean()
            ),
            "test_up_share_among_moves": (
                test_direction[
                    "direction_target"
                ].mean()
            ),
        }
    ]
)


# =============================================================================
# Train Stage 2 direction model
# =============================================================================

print("[INFO] Training book-only direction model...")

direction_model = make_model()

direction_model.fit(
    train_direction[DIRECTION_FEATURES],
    train_direction[
        "direction_target"
    ].astype(int),
)


direction_metric_rows: list[
    dict[str, object]
] = []


# Majority baseline for direction
direction_majority_class = int(
    train_direction[
        "direction_target"
    ]
    .value_counts()
    .idxmax()
)

direction_majority_probability = (
    1.0
    if direction_majority_class == 1
    else 0.0
)


for split_name, split_data in [
    ("validation", validation_direction),
    ("test", test_direction),
]:
    baseline_probabilities = np.full(
        len(split_data),
        direction_majority_probability,
    )

    direction_metric_rows.append(
        {
            "model_name": "majority_baseline",
            "split": split_name,
            **evaluate_binary_model(
                split_data[
                    "direction_target"
                ].astype(int),
                baseline_probabilities,
            ),
        }
    )

    model_probabilities = (
        direction_model.predict_proba(
            split_data[DIRECTION_FEATURES]
        )[:, 1]
    )

    direction_metric_rows.append(
        {
            "model_name": (
                "logit_book_only_direction"
            ),
            "split": split_name,
            **evaluate_binary_model(
                split_data[
                    "direction_target"
                ].astype(int),
                model_probabilities,
            ),
        }
    )


direction_metrics = pd.DataFrame(
    direction_metric_rows
)


# Direction probabilities are applied to ALL rows during deployment.
validation_direction_probabilities = (
    direction_model.predict_proba(
        validation_all[DIRECTION_FEATURES]
    )[:, 1]
)

test_direction_probabilities = (
    direction_model.predict_proba(
        test_all[DIRECTION_FEATURES]
    )[:, 1]
)


# =============================================================================
# Train Stage 1 move models
# =============================================================================

print("[INFO] Training move/no-move models...")

move_metric_rows: list[dict[str, object]] = []
move_models: dict[str, Pipeline] = {}

validation_move_probabilities: dict[
    str,
    np.ndarray,
] = {}

test_move_probabilities: dict[
    str,
    np.ndarray,
] = {}


# Probability baseline equal to Tuesday move prevalence.
train_move_prevalence = float(
    train_all["move_target"].mean()
)

for split_name, split_data in [
    ("validation", validation_all),
    ("test", test_all),
]:
    baseline_probabilities = np.full(
        len(split_data),
        train_move_prevalence,
    )

    move_metric_rows.append(
        {
            "model_name": "prevalence_baseline",
            "feature_set": "none",
            "split": split_name,
            **evaluate_binary_model(
                split_data[
                    "move_target"
                ].astype(int),
                baseline_probabilities,
            ),
        }
    )


for feature_set, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    model_name = (
        f"logit_move_{feature_set}"
    )

    print(
        f"[INFO] Fitting {model_name} "
        f"with {len(feature_columns)} features"
    )

    model = make_model()

    model.fit(
        train_all[feature_columns],
        train_all["move_target"].astype(int),
    )

    move_models[feature_set] = model

    for split_name, split_data in [
        ("validation", validation_all),
        ("test", test_all),
    ]:
        probabilities = model.predict_proba(
            split_data[feature_columns]
        )[:, 1]

        move_metric_rows.append(
            {
                "model_name": model_name,
                "feature_set": feature_set,
                "split": split_name,
                **evaluate_binary_model(
                    split_data[
                        "move_target"
                    ].astype(int),
                    probabilities,
                ),
            }
        )

    validation_move_probabilities[
        feature_set
    ] = model.predict_proba(
        validation_all[feature_columns]
    )[:, 1]

    test_move_probabilities[
        feature_set
    ] = model.predict_proba(
        test_all[feature_columns]
    )[:, 1]


move_metrics = pd.DataFrame(
    move_metric_rows
)


# =============================================================================
# Validation threshold search
# =============================================================================

print("[INFO] Searching validation thresholds...")

threshold_grid_rows: list[dict[str, object]] = []
selected_threshold_rows: list[
    dict[str, object]
] = []

test_summary_rows: list[dict[str, object]] = []
test_by_run_rows: list[dict[str, object]] = []
test_by_direction_rows: list[
    dict[str, object]
] = []
cost_sanity_rows: list[dict[str, object]] = []

all_test_signals: list[pd.DataFrame] = []


def evaluate_validation_candidate(
    model_variant: str,
    move_feature_set: str,
    move_probabilities: np.ndarray,
    move_threshold: float,
    direction_threshold: float,
) -> dict[str, object]:
    """Build and evaluate one validation threshold combination."""
    raw_signals = build_signals(
        evaluation_data=validation_all,
        move_probabilities=move_probabilities,
        direction_probabilities=(
            validation_direction_probabilities
        ),
        return_column=RETURN_COLUMN,
        move_threshold=move_threshold,
        direction_threshold=direction_threshold,
    )

    cooldown_signals = apply_cooldown(
        raw_signals,
        cooldown_events=HORIZON,
    )

    raw_metrics = evaluate_deployment(
        raw_signals,
        n_evaluation_rows=len(validation_all),
        cost_bps=ROUND_TRIP_COST_BPS,
    )

    cooldown_metrics = evaluate_deployment(
        cooldown_signals,
        n_evaluation_rows=len(validation_all),
        cost_bps=ROUND_TRIP_COST_BPS,
    )

    row: dict[str, object] = {
        "model_variant": model_variant,
        "move_feature_set": move_feature_set,
        "move_threshold": move_threshold,
        "direction_threshold": direction_threshold,
    }

    row.update(
        {
            f"raw_{key}": value
            for key, value in raw_metrics.items()
        }
    )

    row.update(
        {
            f"cooldown_{key}": value
            for key, value in cooldown_metrics.items()
        }
    )

    row["threshold_eligible"] = (
        cooldown_metrics["n_signals"]
        >= MIN_VALIDATION_COOLDOWN_SIGNALS
        and cooldown_metrics["n_signal_runs"]
        >= MIN_VALIDATION_SIGNAL_RUNS
    )

    return row


# Direction-only baseline:
# move probabilities are set to one, so no move filter is applied.
direction_only_move_probabilities = np.ones(
    len(validation_all)
)

for direction_threshold in DIRECTION_THRESHOLDS:
    row = evaluate_validation_candidate(
        model_variant="direction_only_book",
        move_feature_set="none",
        move_probabilities=(
            direction_only_move_probabilities
        ),
        move_threshold=0.0,
        direction_threshold=direction_threshold,
    )

    threshold_grid_rows.append(row)


# Two-stage candidates
for feature_set in MOVE_FEATURE_SETS:
    model_variant = (
        f"two_stage_move_{feature_set}"
    )

    move_probabilities = (
        validation_move_probabilities[
            feature_set
        ]
    )

    for move_threshold in MOVE_THRESHOLDS:
        for direction_threshold in (
            DIRECTION_THRESHOLDS
        ):
            row = evaluate_validation_candidate(
                model_variant=model_variant,
                move_feature_set=feature_set,
                move_probabilities=(
                    move_probabilities
                ),
                move_threshold=move_threshold,
                direction_threshold=(
                    direction_threshold
                ),
            )

            threshold_grid_rows.append(row)


threshold_grid = pd.DataFrame(
    threshold_grid_rows
)


# Select one validation threshold combination per model variant.
for model_variant, model_grid in (
    threshold_grid.groupby(
        "model_variant",
        sort=False,
    )
):
    eligible = model_grid[
        model_grid["threshold_eligible"]
    ].copy()

    if eligible.empty:
        print(
            f"[WARNING] No eligible threshold for "
            f"{model_variant}. Selecting from all "
            f"threshold combinations."
        )

        candidates = model_grid.copy()
    else:
        candidates = eligible

    candidates = candidates.sort_values(
        [
            "cooldown_mean_net_return_bps",
            "cooldown_n_signal_runs",
            "cooldown_n_signals",
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

    selected = candidates.iloc[0].to_dict()

    selected_threshold_rows.append(selected)


selected_thresholds = pd.DataFrame(
    selected_threshold_rows
)


# =============================================================================
# Apply selected thresholds to Thursday evaluation
# =============================================================================

print("[INFO] Evaluating selected models on Thursday...")

for _, selected in selected_thresholds.iterrows():
    model_variant = str(
        selected["model_variant"]
    )

    move_feature_set = str(
        selected["move_feature_set"]
    )

    move_threshold = float(
        selected["move_threshold"]
    )

    direction_threshold = float(
        selected["direction_threshold"]
    )

    if model_variant == "direction_only_book":
        test_move_probability = np.ones(
            len(test_all)
        )
    else:
        test_move_probability = (
            test_move_probabilities[
                move_feature_set
            ]
        )

    raw_signals = build_signals(
        evaluation_data=test_all,
        move_probabilities=test_move_probability,
        direction_probabilities=(
            test_direction_probabilities
        ),
        return_column=RETURN_COLUMN,
        move_threshold=move_threshold,
        direction_threshold=direction_threshold,
    )

    cooldown_signals = apply_cooldown(
        raw_signals,
        cooldown_events=HORIZON,
    )

    for selection_method, signals in [
        ("raw_selected_signal", raw_signals),
        (
            "cooldown_first_signal",
            cooldown_signals,
        ),
    ]:
        metrics = evaluate_deployment(
            signals,
            n_evaluation_rows=len(test_all),
            cost_bps=ROUND_TRIP_COST_BPS,
        )

        test_summary_rows.append(
            {
                "model_variant": model_variant,
                "move_feature_set": (
                    move_feature_set
                ),
                "horizon_events": HORIZON,
                "selected_move_threshold": (
                    move_threshold
                ),
                "selected_direction_threshold": (
                    direction_threshold
                ),
                "selection_method": (
                    selection_method
                ),
                **metrics,
            }
        )

        if selection_method == (
            "cooldown_first_signal"
        ):
            test_by_run_rows.extend(
                summarize_by_run(
                    signals,
                    model_variant=model_variant,
                )
            )

            test_by_direction_rows.extend(
                summarize_by_direction(
                    signals,
                    model_variant=model_variant,
                )
            )

    for cost_bps in COST_LEVELS_BPS:
        metrics = evaluate_deployment(
            cooldown_signals,
            n_evaluation_rows=len(test_all),
            cost_bps=cost_bps,
        )

        cost_sanity_rows.append(
            {
                "model_variant": model_variant,
                "move_feature_set": (
                    move_feature_set
                ),
                "selected_move_threshold": (
                    move_threshold
                ),
                "selected_direction_threshold": (
                    direction_threshold
                ),
                "round_trip_cost_bps": cost_bps,
                **metrics,
            }
        )

    if not cooldown_signals.empty:
        saved_signals = cooldown_signals.copy()

        saved_signals["model_variant"] = (
            model_variant
        )

        saved_signals[
            "move_feature_set"
        ] = move_feature_set

        all_test_signals.append(
            saved_signals
        )


test_summary = pd.DataFrame(
    test_summary_rows
)

test_by_run = pd.DataFrame(
    test_by_run_rows
)

test_by_direction = pd.DataFrame(
    test_by_direction_rows
)

cost_sanity = pd.DataFrame(
    cost_sanity_rows
)


# =============================================================================
# Compare each two-stage model with the direction-only baseline
# =============================================================================

cooldown_summary = test_summary[
    test_summary["selection_method"].eq(
        "cooldown_first_signal"
    )
].copy()

baseline_row = cooldown_summary[
    cooldown_summary["model_variant"].eq(
        "direction_only_book"
    )
].iloc[0]

comparison_rows: list[dict[str, object]] = []

for _, row in cooldown_summary.iterrows():
    comparison_rows.append(
        {
            "model_variant": row[
                "model_variant"
            ],
            "n_signals": row["n_signals"],
            "n_signal_runs": row[
                "n_signal_runs"
            ],
            "directional_precision": row[
                "directional_precision"
            ],
            "mean_signed_return_bps": row[
                "mean_signed_return_bps"
            ],
            "mean_net_return_bps": row[
                "mean_net_return_bps"
            ],
            "delta_n_signals_vs_direction_only": (
                row["n_signals"]
                - baseline_row["n_signals"]
            ),
            "delta_directional_precision_vs_direction_only": (
                row["directional_precision"]
                - baseline_row[
                    "directional_precision"
                ]
            ),
            "delta_gross_bps_vs_direction_only": (
                row["mean_signed_return_bps"]
                - baseline_row[
                    "mean_signed_return_bps"
                ]
            ),
            "delta_net_bps_vs_direction_only": (
                row["mean_net_return_bps"]
                - baseline_row[
                    "mean_net_return_bps"
                ]
            ),
        }
    )


test_comparison = pd.DataFrame(
    comparison_rows
)


# =============================================================================
# Save standardized model coefficients
# =============================================================================

coefficient_rows: list[dict[str, object]] = []


direction_coefficients = (
    direction_model
    .named_steps["model"]
    .coef_[0]
)

for feature, coefficient in zip(
    DIRECTION_FEATURES,
    direction_coefficients,
):
    coefficient_rows.append(
        {
            "model_type": "direction",
            "feature_set": "book_only",
            "feature": feature,
            "coefficient": coefficient,
            "absolute_coefficient": abs(
                coefficient
            ),
        }
    )


for feature_set, model in move_models.items():
    feature_columns = MOVE_FEATURE_SETS[
        feature_set
    ]

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
                "feature_set": feature_set,
                "feature": feature,
                "coefficient": coefficient,
                "absolute_coefficient": abs(
                    coefficient
                ),
            }
        )


coefficients = pd.DataFrame(
    coefficient_rows
).sort_values(
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


# =============================================================================
# Save tables
# =============================================================================

split_summary.to_csv(
    TABLES_DIR / "day33_split_summary.csv",
    index=False,
)

move_metrics.to_csv(
    TABLES_DIR / "day33_move_model_metrics.csv",
    index=False,
)

direction_metrics.to_csv(
    TABLES_DIR / "day33_direction_model_metrics.csv",
    index=False,
)

threshold_grid.to_csv(
    TABLES_DIR / "day33_validation_threshold_grid.csv",
    index=False,
)

selected_thresholds.to_csv(
    TABLES_DIR / "day33_selected_thresholds.csv",
    index=False,
)

test_summary.to_csv(
    TABLES_DIR / "day33_test_deployment_summary.csv",
    index=False,
)

test_comparison.to_csv(
    TABLES_DIR / "day33_test_model_comparison.csv",
    index=False,
)

test_by_run.to_csv(
    TABLES_DIR / "day33_test_deployment_by_run.csv",
    index=False,
)

test_by_direction.to_csv(
    TABLES_DIR / "day33_test_deployment_by_direction.csv",
    index=False,
)

cost_sanity.to_csv(
    TABLES_DIR / "day33_test_cost_sanity.csv",
    index=False,
)

coefficients.to_csv(
    TABLES_DIR / "day33_model_coefficients.csv",
    index=False,
)


if all_test_signals:
    test_signals = pd.concat(
        all_test_signals,
        ignore_index=True,
    )

    test_signals.to_csv(
        TABLES_DIR
        / "day33_test_cooldown_signals.csv",
        index=False,
    )


# =============================================================================
# Figures
# =============================================================================

move_model_plot = move_metrics[
    move_metrics["model_name"].ne(
        "prevalence_baseline"
    )
].copy()

move_model_plot["label"] = (
    move_model_plot["feature_set"]
    + "_"
    + move_model_plot["split"]
)

plt.figure(figsize=(11, 6))

plt.bar(
    move_model_plot["label"],
    move_model_plot["roc_auc"],
)

plt.axhline(
    0.5,
    linewidth=1,
)

plt.xticks(rotation=30)
plt.ylabel("MOVE model ROC-AUC")
plt.title(
    "Move/no-move classification by feature set"
)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

move_figure_path = (
    FIGURES_DIR
    / "day33_move_model_roc_auc.png"
)

plt.savefig(
    move_figure_path,
    dpi=150,
    bbox_inches="tight",
)

plt.close()

print(
    f"[INFO] Saved figure: {move_figure_path}"
)


plt.figure(figsize=(11, 6))

plt.bar(
    cooldown_summary["model_variant"],
    cooldown_summary["mean_net_return_bps"],
)

plt.axhline(
    0.0,
    linewidth=1,
)

plt.xticks(rotation=25)
plt.ylabel(
    "Thursday cooldown mean net return, bps"
)
plt.title(
    "Two-stage versus direction-only performance"
)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

test_figure_path = (
    FIGURES_DIR
    / "day33_test_net_after_1bps.png"
)

plt.savefig(
    test_figure_path,
    dpi=150,
    bbox_inches="tight",
)

plt.close()

print(
    f"[INFO] Saved figure: {test_figure_path}"
)


# =============================================================================
# Console summary
# =============================================================================

print()
print("=" * 80)
print("[INFO] Day 33 split summary")
print("=" * 80)
print(
    split_summary.to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Move-model validation/test metrics")
print("=" * 80)

print(
    move_metrics[
        [
            "model_name",
            "feature_set",
            "split",
            "positive_class_share",
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
print("[INFO] Selected validation thresholds")
print("=" * 80)

print(
    selected_thresholds[
        [
            "model_variant",
            "move_feature_set",
            "move_threshold",
            "direction_threshold",
            "cooldown_n_signals",
            "cooldown_n_signal_runs",
            "cooldown_directional_precision",
            "cooldown_mean_signed_return_bps",
            "cooldown_mean_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Thursday cooldown comparison")
print("=" * 80)

print(
    test_comparison.to_string(index=False)
)

print()
print(
    "[INFO] Day 33 two-stage modeling "
    "completed successfully."
)