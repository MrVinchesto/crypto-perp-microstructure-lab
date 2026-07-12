from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
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

# This Day 31 table contains only strict quality-filtered runs.
STRICT_RUNS_PATH = Path("reports/tables/day31_activity_by_run.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)


TRAIN_BATCH = "weekday_active_tue_day28"
VALIDATION_BATCH = "weekday_active_wed_day29"
TEST_BATCH = "weekday_active_thu_day30"

HORIZONS = [50, 100]

DEAD_ZONE_BPS = 1.0
ROUND_TRIP_COST_BPS = 1.0

THRESHOLDS = [
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
]

# A validation threshold should not be selected from only a few isolated signals.
MIN_VALIDATION_COOLDOWN_SIGNALS = 20
MIN_VALIDATION_SIGNAL_RUNS = 5

COST_LEVELS_BPS = [0.0, 0.5, 1.0, 2.0, 3.0, 5.0]


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
    """Raise a clear error when a required column is missing."""
    missing = [column for column in required if column not in columns]

    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {missing}"
        )


def choose_trade_flow_features(columns: list[str]) -> list[str]:
    """
    Select known trade-flow columns while avoiding accidental inclusion
    of targets or metadata.
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

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(selected))


def make_model() -> Pipeline:
    """
    Create the same regularized logistic-regression pipeline for every
    feature set, so the comparison is fair.
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
    """ROC-AUC is undefined if only one class is present."""
    if y_true.nunique() < 2:
        return float("nan")

    return float(roc_auc_score(y_true, probabilities))


def evaluate_conditional_direction(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> dict[str, float | int]:
    """
    Evaluate direction only among economically meaningful UP/DOWN moves.
    This is a classification diagnostic, not a deployment result.
    """
    predictions = (probabilities >= 0.50).astype(int)

    return {
        "n_observations": int(len(y_true)),
        "up_share": float(y_true.mean()),
        "accuracy": float(
            accuracy_score(y_true, predictions)
        ),
        "balanced_accuracy": float(
            balanced_accuracy_score(y_true, predictions)
        ),
        "precision_up": float(
            precision_score(
                y_true,
                predictions,
                pos_label=1,
                zero_division=0,
            )
        ),
        "recall_up": float(
            recall_score(
                y_true,
                predictions,
                pos_label=1,
                zero_division=0,
            )
        ),
        "precision_down": float(
            precision_score(
                y_true,
                predictions,
                pos_label=0,
                zero_division=0,
            )
        ),
        "recall_down": float(
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
    }


def build_selected_signals(
    evaluation_data: pd.DataFrame,
    probabilities: np.ndarray,
    return_column: str,
    threshold: float,
) -> pd.DataFrame:
    """
    Apply a symmetric confidence threshold to all rows.

    p(up) >= threshold       -> long / UP signal
    p(up) <= 1 - threshold   -> short / DOWN signal
    otherwise                -> no signal
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

    signals["probability_up"] = probabilities

    signals["signal_direction"] = np.where(
        signals["probability_up"] >= threshold,
        1,
        np.where(
            signals["probability_up"] <= (1.0 - threshold),
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

    signals["threshold"] = threshold

    return signals


def apply_cooldown(
    signals: pd.DataFrame,
    cooldown_events: int,
) -> pd.DataFrame:
    """
    De-cluster overlapping signals.

    After selecting one signal, ignore every later signal within the
    next `cooldown_events` rows of the same run.
    """
    if signals.empty:
        return signals.copy()

    selected_indices: list[int] = []

    ordered = signals.sort_values(
        ["run_name", "row_in_run"]
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

            if current_row > last_selected_row + cooldown_events:
                selected_indices.append(index)
                last_selected_row = current_row

    return ordered.loc[selected_indices].copy()


def evaluate_deployment(
    signals: pd.DataFrame,
    n_evaluation_rows: int,
    cost_bps: float,
) -> dict[str, float | int]:
    """
    Evaluate selected signals on all available rows, including future
    flat and sub-cost moves.
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
        "n_signal_runs": int(signals["run_name"].nunique()),
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
            (signed_returns - cost_bps).mean()
        ),
        "share_signed_return_ge_1bps": float(
            signed_returns.ge(1.0).mean()
        ),
        "up_signal_share": float(
            signals["signal_direction"].eq(1).mean()
        ),
        "down_signal_share": float(
            signals["signal_direction"].eq(-1).mean()
        ),
    }


def summarize_signals_by_run(
    signals: pd.DataFrame,
    model_name: str,
    feature_set: str,
    horizon: int,
    method: str,
) -> list[dict[str, object]]:
    """Return one deployment summary row per run."""
    rows: list[dict[str, object]] = []

    for run_name, run_signals in signals.groupby(
        "run_name",
        sort=False,
    ):
        signed_returns = run_signals["signed_return_bps"]

        rows.append(
            {
                "model_name": model_name,
                "feature_set": feature_set,
                "horizon_events": horizon,
                "selection_method": method,
                "run_name": run_name,
                "n_signals": len(run_signals),
                "directional_precision": signed_returns.gt(0).mean(),
                "mean_signed_return_bps": signed_returns.mean(),
                "mean_net_return_bps_1bps_cost": (
                    signed_returns - ROUND_TRIP_COST_BPS
                ).mean(),
                "up_signal_share": (
                    run_signals["signal_direction"].eq(1).mean()
                ),
                "down_signal_share": (
                    run_signals["signal_direction"].eq(-1).mean()
                ),
            }
        )

    return rows


def summarize_signals_by_direction(
    signals: pd.DataFrame,
    model_name: str,
    feature_set: str,
    horizon: int,
    method: str,
) -> list[dict[str, object]]:
    """Check whether results depend only on long or short signals."""
    rows: list[dict[str, object]] = []

    direction_names = {
        1: "UP",
        -1: "DOWN",
    }

    for direction, direction_name in direction_names.items():
        subset = signals[
            signals["signal_direction"].eq(direction)
        ]

        if subset.empty:
            continue

        signed_returns = subset["signed_return_bps"]

        rows.append(
            {
                "model_name": model_name,
                "feature_set": feature_set,
                "horizon_events": horizon,
                "selection_method": method,
                "direction": direction_name,
                "n_signals": len(subset),
                "n_runs": subset["run_name"].nunique(),
                "directional_precision": signed_returns.gt(0).mean(),
                "mean_signed_return_bps": signed_returns.mean(),
                "mean_net_return_bps_1bps_cost": (
                    signed_returns - ROUND_TRIP_COST_BPS
                ).mean(),
            }
        )

    return rows


# =============================================================================
# Inspect available columns and define feature sets
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
        f"Found only {len(TRADE_FLOW_FEATURES)}: "
        f"{TRADE_FLOW_FEATURES}"
    )

FEATURE_SETS = {
    "book_only": BOOK_FEATURES,
    "trade_flow_only": TRADE_FLOW_FEATURES,
    "combined": (
        BOOK_FEATURES
        + TRADE_FLOW_FEATURES
    ),
}

print(
    f"[INFO] Book features: {len(BOOK_FEATURES)}"
)
print(
    f"[INFO] Trade-flow features: {len(TRADE_FLOW_FEATURES)}"
)
print(
    f"[INFO] Combined features: "
    f"{len(FEATURE_SETS['combined'])}"
)


# =============================================================================
# Load strict run metadata from Day 31
# =============================================================================

print("[INFO] Loading strict Day 31 run list...")

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

run_to_batch = (
    strict_metadata
    .set_index("run_name")["collection_batch"]
    .to_dict()
)

strict_runs = set(strict_metadata["run_name"])


# =============================================================================
# Load only required data columns
# =============================================================================

required_data_columns = list(
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
    usecols=required_data_columns,
)

data["run_name"] = data["run_name"].astype(str)

data = data[
    data["run_name"].isin(strict_runs)
].copy()

data["split"] = data["run_name"].map(run_to_split)
data["collection_batch"] = data["run_name"].map(run_to_batch)

data = data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)

all_feature_columns = list(
    dict.fromkeys(
        BOOK_FEATURES + TRADE_FLOW_FEATURES
    )
)

data[all_feature_columns] = (
    data[all_feature_columns]
    .replace([np.inf, -np.inf], np.nan)
)


# =============================================================================
# Build future returns and cost-aware direction labels within each run
# =============================================================================

print("[INFO] Building cost-aware targets...")

for horizon in HORIZONS:
    future_mid = (
        data
        .groupby("run_name", sort=False)["mid_price"]
        .shift(-horizon)
    )

    return_column = (
        f"future_mid_return_bps_h{horizon}"
    )

    target_column = (
        f"cost_aware_direction_h{horizon}"
    )

    data[return_column] = (
        (future_mid / data["mid_price"]) - 1.0
    ) * 10_000.0

    data[target_column] = np.where(
        data[return_column] > DEAD_ZONE_BPS,
        1.0,
        np.where(
            data[return_column] < -DEAD_ZONE_BPS,
            0.0,
            np.nan,
        ),
    )


# =============================================================================
# Containers for outputs
# =============================================================================

split_rows: list[dict[str, object]] = []
conditional_metric_rows: list[dict[str, object]] = []
threshold_grid_rows: list[dict[str, object]] = []
selected_threshold_rows: list[dict[str, object]] = []

test_summary_rows: list[dict[str, object]] = []
test_by_run_rows: list[dict[str, object]] = []
test_by_direction_rows: list[dict[str, object]] = []
cost_sanity_rows: list[dict[str, object]] = []
coefficient_rows: list[dict[str, object]] = []

all_test_cooldown_signals: list[pd.DataFrame] = []


# =============================================================================
# Modeling loop
# =============================================================================

for horizon in HORIZONS:
    print()
    print("=" * 80)
    print(f"[INFO] Processing horizon h{horizon}")
    print("=" * 80)

    return_column = (
        f"future_mid_return_bps_h{horizon}"
    )

    target_column = (
        f"cost_aware_direction_h{horizon}"
    )

    train_all = data[
        data["split"].eq("train")
        & data[return_column].notna()
    ].copy()

    validation_all = data[
        data["split"].eq("validation")
        & data[return_column].notna()
    ].copy()

    test_all = data[
        data["split"].eq("test")
        & data[return_column].notna()
    ].copy()

    train_nonflat = train_all[
        train_all[target_column].notna()
    ].copy()

    validation_nonflat = validation_all[
        validation_all[target_column].notna()
    ].copy()

    test_nonflat = test_all[
        test_all[target_column].notna()
    ].copy()

    split_rows.append(
        {
            "horizon_events": horizon,
            "train_runs": train_all["run_name"].nunique(),
            "validation_runs": validation_all["run_name"].nunique(),
            "test_runs": test_all["run_name"].nunique(),
            "train_all_rows": len(train_all),
            "validation_all_rows": len(validation_all),
            "test_all_rows": len(test_all),
            "train_nonflat_rows": len(train_nonflat),
            "validation_nonflat_rows": len(validation_nonflat),
            "test_nonflat_rows": len(test_nonflat),
            "train_nonflat_share": (
                len(train_nonflat) / len(train_all)
            ),
            "validation_nonflat_share": (
                len(validation_nonflat) / len(validation_all)
            ),
            "test_nonflat_share": (
                len(test_nonflat) / len(test_all)
            ),
            "train_up_share_nonflat": (
                train_nonflat[target_column].mean()
            ),
            "validation_up_share_nonflat": (
                validation_nonflat[target_column].mean()
            ),
            "test_up_share_nonflat": (
                test_nonflat[target_column].mean()
            ),
        }
    )

    # -------------------------------------------------------------------------
    # Majority baseline for the conditional classification problem
    # -------------------------------------------------------------------------

    majority_class = int(
        train_nonflat[target_column]
        .value_counts()
        .idxmax()
    )

    constant_probability = (
        1.0 if majority_class == 1 else 0.0
    )

    for split_name, split_nonflat in [
        ("validation", validation_nonflat),
        ("test", test_nonflat),
    ]:
        baseline_probabilities = np.full(
            len(split_nonflat),
            constant_probability,
        )

        baseline_metrics = (
            evaluate_conditional_direction(
                split_nonflat[target_column].astype(int),
                baseline_probabilities,
            )
        )

        conditional_metric_rows.append(
            {
                "model_name": "majority_baseline",
                "feature_set": "none",
                "horizon_events": horizon,
                "split": split_name,
                **baseline_metrics,
            }
        )

    # -------------------------------------------------------------------------
    # Feature-set models
    # -------------------------------------------------------------------------

    for feature_set, feature_columns in FEATURE_SETS.items():
        model_name = f"logit_{feature_set}"

        print(
            f"[INFO] Fitting {model_name} "
            f"with {len(feature_columns)} features"
        )

        model = make_model()

        model.fit(
            train_nonflat[feature_columns],
            train_nonflat[target_column].astype(int),
        )

        # ---------------------------------------------------------------------
        # Conditional direction metrics
        # ---------------------------------------------------------------------

        for split_name, split_nonflat in [
            ("validation", validation_nonflat),
            ("test", test_nonflat),
        ]:
            probabilities = model.predict_proba(
                split_nonflat[feature_columns]
            )[:, 1]

            metrics = evaluate_conditional_direction(
                split_nonflat[target_column].astype(int),
                probabilities,
            )

            conditional_metric_rows.append(
                {
                    "model_name": model_name,
                    "feature_set": feature_set,
                    "horizon_events": horizon,
                    "split": split_name,
                    **metrics,
                }
            )

        # ---------------------------------------------------------------------
        # Validation threshold search on ALL validation rows
        # ---------------------------------------------------------------------

        validation_probabilities = model.predict_proba(
            validation_all[feature_columns]
        )[:, 1]

        model_threshold_rows: list[dict[str, object]] = []

        for threshold in THRESHOLDS:
            raw_signals = build_selected_signals(
                evaluation_data=validation_all,
                probabilities=validation_probabilities,
                return_column=return_column,
                threshold=threshold,
            )

            cooldown_signals = apply_cooldown(
                raw_signals,
                cooldown_events=horizon,
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

            threshold_row = {
                "model_name": model_name,
                "feature_set": feature_set,
                "horizon_events": horizon,
                "threshold": threshold,
                **{
                    f"raw_{key}": value
                    for key, value in raw_metrics.items()
                },
                **{
                    f"cooldown_{key}": value
                    for key, value in cooldown_metrics.items()
                },
            }

            threshold_row["threshold_eligible"] = (
                cooldown_metrics["n_signals"]
                >= MIN_VALIDATION_COOLDOWN_SIGNALS
                and cooldown_metrics["n_signal_runs"]
                >= MIN_VALIDATION_SIGNAL_RUNS
            )

            model_threshold_rows.append(threshold_row)
            threshold_grid_rows.append(threshold_row)

        model_threshold_table = pd.DataFrame(
            model_threshold_rows
        )

        eligible_thresholds = model_threshold_table[
            model_threshold_table["threshold_eligible"]
        ].copy()

        if eligible_thresholds.empty:
            print(
                f"[WARNING] No threshold met the minimum "
                f"signal-count rule for {model_name}, h{horizon}. "
                f"Selecting from all thresholds."
            )

            threshold_candidates = (
                model_threshold_table.copy()
            )
        else:
            threshold_candidates = eligible_thresholds

        threshold_candidates = (
            threshold_candidates
            .sort_values(
                [
                    "cooldown_mean_net_return_bps",
                    "cooldown_n_signal_runs",
                    "cooldown_n_signals",
                    "threshold",
                ],
                ascending=[
                    False,
                    False,
                    False,
                    True,
                ],
            )
        )

        selected_threshold_row = (
            threshold_candidates.iloc[0]
        )

        selected_threshold = float(
            selected_threshold_row["threshold"]
        )

        selected_threshold_rows.append(
            selected_threshold_row.to_dict()
        )

        print(
            f"[INFO] Selected threshold for "
            f"{model_name}, h{horizon}: "
            f"{selected_threshold:.2f}"
        )

        # ---------------------------------------------------------------------
        # Completely untouched Thursday test deployment
        # ---------------------------------------------------------------------

        test_probabilities = model.predict_proba(
            test_all[feature_columns]
        )[:, 1]

        test_raw_signals = build_selected_signals(
            evaluation_data=test_all,
            probabilities=test_probabilities,
            return_column=return_column,
            threshold=selected_threshold,
        )

        test_cooldown_signals = apply_cooldown(
            test_raw_signals,
            cooldown_events=horizon,
        )

        for selection_method, selected_signals in [
            ("raw_selected_signal", test_raw_signals),
            ("cooldown_first_signal", test_cooldown_signals),
        ]:
            deployment_metrics = evaluate_deployment(
                selected_signals,
                n_evaluation_rows=len(test_all),
                cost_bps=ROUND_TRIP_COST_BPS,
            )

            test_summary_rows.append(
                {
                    "model_name": model_name,
                    "feature_set": feature_set,
                    "horizon_events": horizon,
                    "selected_threshold": selected_threshold,
                    "selection_method": selection_method,
                    **deployment_metrics,
                }
            )

            test_by_run_rows.extend(
                summarize_signals_by_run(
                    selected_signals,
                    model_name=model_name,
                    feature_set=feature_set,
                    horizon=horizon,
                    method=selection_method,
                )
            )

            test_by_direction_rows.extend(
                summarize_signals_by_direction(
                    selected_signals,
                    model_name=model_name,
                    feature_set=feature_set,
                    horizon=horizon,
                    method=selection_method,
                )
            )

        # Cost sanity is performed on de-clustered test signals.
        for cost_bps in COST_LEVELS_BPS:
            cost_metrics = evaluate_deployment(
                test_cooldown_signals,
                n_evaluation_rows=len(test_all),
                cost_bps=cost_bps,
            )

            cost_sanity_rows.append(
                {
                    "model_name": model_name,
                    "feature_set": feature_set,
                    "horizon_events": horizon,
                    "selected_threshold": selected_threshold,
                    "round_trip_cost_bps": cost_bps,
                    **cost_metrics,
                }
            )

        if not test_cooldown_signals.empty:
            saved_signals = test_cooldown_signals.copy()

            saved_signals["model_name"] = model_name
            saved_signals["feature_set"] = feature_set
            saved_signals["horizon_events"] = horizon
            saved_signals["selected_threshold"] = (
                selected_threshold
            )

            all_test_cooldown_signals.append(
                saved_signals
            )

        # ---------------------------------------------------------------------
        # Standardized logistic-regression coefficients
        # ---------------------------------------------------------------------

        coefficients = (
            model.named_steps["model"]
            .coef_[0]
        )

        for feature_name, coefficient in zip(
            feature_columns,
            coefficients,
        ):
            coefficient_rows.append(
                {
                    "model_name": model_name,
                    "feature_set": feature_set,
                    "horizon_events": horizon,
                    "feature": feature_name,
                    "coefficient": coefficient,
                    "absolute_coefficient": abs(coefficient),
                }
            )


# =============================================================================
# Save tables
# =============================================================================

split_summary = pd.DataFrame(split_rows)

conditional_metrics = pd.DataFrame(
    conditional_metric_rows
)

threshold_grid = pd.DataFrame(
    threshold_grid_rows
)

selected_thresholds = pd.DataFrame(
    selected_threshold_rows
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

coefficients = pd.DataFrame(
    coefficient_rows
)

if not coefficients.empty:
    coefficients = coefficients.sort_values(
        [
            "model_name",
            "horizon_events",
            "absolute_coefficient",
        ],
        ascending=[
            True,
            True,
            False,
        ],
    )


split_summary.to_csv(
    TABLES_DIR / "day32_split_summary.csv",
    index=False,
)

conditional_metrics.to_csv(
    TABLES_DIR / "day32_conditional_metrics.csv",
    index=False,
)

threshold_grid.to_csv(
    TABLES_DIR / "day32_validation_threshold_grid.csv",
    index=False,
)

selected_thresholds.to_csv(
    TABLES_DIR / "day32_selected_thresholds.csv",
    index=False,
)

test_summary.to_csv(
    TABLES_DIR / "day32_test_deployment_summary.csv",
    index=False,
)

test_by_run.to_csv(
    TABLES_DIR / "day32_test_deployment_by_run.csv",
    index=False,
)

test_by_direction.to_csv(
    TABLES_DIR / "day32_test_deployment_by_direction.csv",
    index=False,
)

cost_sanity.to_csv(
    TABLES_DIR / "day32_test_cost_sanity.csv",
    index=False,
)

coefficients.to_csv(
    TABLES_DIR / "day32_logit_coefficients.csv",
    index=False,
)


if all_test_cooldown_signals:
    test_cooldown_signals = pd.concat(
        all_test_cooldown_signals,
        ignore_index=True,
    )

    test_cooldown_signals.to_csv(
        TABLES_DIR / "day32_test_cooldown_signals.csv",
        index=False,
    )


# =============================================================================
# Figures
# =============================================================================

plt.figure(figsize=(11, 6))

for (
    model_name,
    horizon,
), group in threshold_grid.groupby(
    ["model_name", "horizon_events"],
    sort=False,
):
    plt.plot(
        group["threshold"],
        group["cooldown_mean_net_return_bps"],
        marker="o",
        label=f"{model_name}, h{horizon}",
    )

plt.axhline(
    0.0,
    linewidth=1,
)

plt.xlabel("Confidence threshold")
plt.ylabel("Validation cooldown mean net return, bps")
plt.title("Validation threshold selection after 1 bps cost")
plt.legend(fontsize=8)
plt.grid(alpha=0.3)
plt.tight_layout()

validation_figure_path = (
    FIGURES_DIR
    / "day32_validation_net_by_threshold.png"
)

plt.savefig(
    validation_figure_path,
    dpi=150,
    bbox_inches="tight",
)

plt.close()

print(
    f"[INFO] Saved figure: {validation_figure_path}"
)


test_cooldown_summary = test_summary[
    test_summary["selection_method"].eq(
        "cooldown_first_signal"
    )
].copy()

test_cooldown_summary["label"] = (
    test_cooldown_summary["feature_set"]
    + "_h"
    + test_cooldown_summary["horizon_events"].astype(str)
)

plt.figure(figsize=(11, 6))

plt.bar(
    test_cooldown_summary["label"],
    test_cooldown_summary["mean_net_return_bps"],
)

plt.axhline(
    0.0,
    linewidth=1,
)

plt.xticks(rotation=30)
plt.ylabel("Thursday cooldown mean net return, bps")
plt.title("Strict Thursday test performance after 1 bps cost")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

test_figure_path = (
    FIGURES_DIR
    / "day32_test_net_after_1bps.png"
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
print("[INFO] Day 32 split summary")
print("=" * 80)
print(split_summary.to_string(index=False))

print()
print("=" * 80)
print("[INFO] Selected validation thresholds")
print("=" * 80)

print(
    selected_thresholds[
        [
            "model_name",
            "feature_set",
            "horizon_events",
            "threshold",
            "cooldown_n_signals",
            "cooldown_n_signal_runs",
            "cooldown_mean_signed_return_bps",
            "cooldown_mean_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Strict Thursday cooldown test summary")
print("=" * 80)

print(
    test_cooldown_summary[
        [
            "model_name",
            "feature_set",
            "horizon_events",
            "selected_threshold",
            "n_signals",
            "n_signal_runs",
            "coverage",
            "directional_precision",
            "mean_signed_return_bps",
            "mean_net_return_bps",
            "up_signal_share",
            "down_signal_share",
        ]
    ].to_string(index=False)
)

print()
print(
    "[INFO] Day 32 cost-aware active-hours modeling "
    "completed successfully."
)