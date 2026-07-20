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


DATA_PATH = Path("data/processed/trade_flow_features.csv")
COLLECTION_LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")
TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")
TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_BATCHES = [
    "weekday_active_tue_day28",
    "weekday_active_wed_day29",
]
TEST_BATCH = "weekday_active_fri_day39"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
PRIMARY_COST_BPS = 1.0
COST_GRID_BPS = [0.5, 1.0, 1.5, 2.0]

MOVE_C = 0.03
DIRECTION_C = 1.0

PRIMARY_MOVE_THRESHOLD = 0.75
PRIMARY_DIRECTION_THRESHOLD = 0.65
SENSITIVITY_MOVE_THRESHOLD = 0.65
SENSITIVITY_DIRECTION_THRESHOLD = 0.65

COOLDOWN_EVENTS = 50

MIN_COLLECTION_SECONDS = 295
MIN_DEPTH_EVENTS = 2_500
MIN_TRADE_EVENTS = 1
MIN_PROCESSED_ROWS = 2_500

BOOTSTRAP_ITERATIONS = 5_000
RANDOM_STATE = 42


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
        "predicted_positive_share": float(predictions.mean()),
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


def normalize_status(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def build_quality_table(
    log: pd.DataFrame,
    processed_rows_by_run: pd.Series,
) -> pd.DataFrame:
    quality = log.copy()
    quality["run_name"] = quality["run_name"].astype(str)
    quality["processed_rows"] = (
        quality["run_name"]
        .map(processed_rows_by_run)
        .fillna(0)
        .astype(int)
    )
    quality["status_ok"] = normalize_status(
        quality["status"]
    ).eq("success")

    if "collection_seconds_meta" in quality.columns:
        duration_source = quality["collection_seconds_meta"]
    else:
        duration_source = quality[
            "collection_seconds_requested"
        ]

    quality["duration_seconds_used"] = pd.to_numeric(
        duration_source,
        errors="coerce",
    )
    quality["duration_ok"] = (
        quality["duration_seconds_used"]
        >= MIN_COLLECTION_SECONDS
    )
    quality["depth_ok"] = (
        pd.to_numeric(
            quality["depth_events"],
            errors="coerce",
        )
        >= MIN_DEPTH_EVENTS
    )
    quality["trades_ok"] = (
        pd.to_numeric(
            quality["trade_events"],
            errors="coerce",
        )
        >= MIN_TRADE_EVENTS
    )
    quality["processed_ok"] = (
        quality["processed_rows"]
        >= MIN_PROCESSED_ROWS
    )
    quality["strict_quality_ok"] = (
        quality["status_ok"]
        & quality["duration_ok"]
        & quality["depth_ok"]
        & quality["trades_ok"]
        & quality["processed_ok"]
    )

    def exclusion_reason(row: pd.Series) -> str:
        reasons: list[str] = []
        if not bool(row["status_ok"]):
            reasons.append("collection_status_not_success")
        if not bool(row["duration_ok"]):
            reasons.append(
                "collection_duration_below_295_seconds"
            )
        if not bool(row["depth_ok"]):
            reasons.append("depth_events_below_2500")
        if not bool(row["trades_ok"]):
            reasons.append("no_trade_events")
        if not bool(row["processed_ok"]):
            reasons.append(
                "processed_rows_below_2500_or_reconstruction_failed"
            )
        return ";".join(reasons)

    quality["exclusion_reason"] = quality.apply(
        exclusion_reason,
        axis=1,
    )
    return quality


def build_first_eligible_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    move_threshold: float,
    direction_threshold: float,
    specification_name: str,
) -> pd.DataFrame:
    candidates = evaluation_data[
        [
            "run_name",
            "row_in_run",
            "future_return_bps",
        ]
    ].copy()

    candidates["move_probability"] = move_probabilities
    candidates[
        "direction_probability_up"
    ] = direction_probabilities
    candidates["direction_confidence"] = np.maximum(
        direction_probabilities,
        1.0 - direction_probabilities,
    )

    move_filter = (
        candidates["move_probability"]
        >= move_threshold
    )
    long_filter = (
        candidates["direction_probability_up"]
        >= direction_threshold
    )
    short_filter = (
        candidates["direction_probability_up"]
        <= 1.0 - direction_threshold
    )

    candidates["signal_direction"] = np.where(
        move_filter & long_filter,
        1,
        np.where(
            move_filter & short_filter,
            -1,
            0,
        ),
    )
    candidates = candidates[
        candidates["signal_direction"] != 0
    ].copy()

    selected_parts: list[pd.DataFrame] = []

    for _, run_candidates in candidates.groupby(
        "run_name",
        sort=False,
    ):
        run_candidates = run_candidates.sort_values(
            "row_in_run"
        )
        selected_indices: list[int] = []
        last_selected_row: int | None = None

        for row_index, row in run_candidates.iterrows():
            current_row = int(row["row_in_run"])
            if (
                last_selected_row is None
                or current_row
                > last_selected_row + COOLDOWN_EVENTS
            ):
                selected_indices.append(row_index)
                last_selected_row = current_row

        if selected_indices:
            selected_parts.append(
                run_candidates.loc[selected_indices]
            )

    if selected_parts:
        signals = pd.concat(
            selected_parts,
            ignore_index=True,
        )
    else:
        signals = candidates.iloc[0:0].copy()

    signals["signed_return_bps"] = (
        signals["signal_direction"]
        * signals["future_return_bps"]
    )
    signals["net_return_bps_1bps_cost"] = (
        signals["signed_return_bps"]
        - PRIMARY_COST_BPS
    )
    signals["specification"] = specification_name
    signals["move_threshold"] = move_threshold
    signals["direction_threshold"] = direction_threshold
    return signals


def evaluate_signals(
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
            "total_net_return_bps": 0.0,
            "long_signal_share": float("nan"),
        }

    return {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(
            signals["run_name"].nunique()
        ),
        "coverage": float(
            len(signals) / n_evaluation_rows
        ),
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
            signals[
                "net_return_bps_1bps_cost"
            ].mean()
        ),
        "median_net_return_bps": float(
            signals[
                "net_return_bps_1bps_cost"
            ].median()
        ),
        "total_net_return_bps": float(
            signals[
                "net_return_bps_1bps_cost"
            ].sum()
        ),
        "long_signal_share": float(
            signals["signal_direction"].eq(1).mean()
        ),
    }


def run_cluster_bootstrap(
    signals: pd.DataFrame,
) -> dict[str, float]:
    if signals.empty:
        return {
            "bootstrap_mean_net_bps": float("nan"),
            "bootstrap_ci_lower_2_5": float("nan"),
            "bootstrap_ci_upper_97_5": float("nan"),
            "bootstrap_share_mean_net_positive": float("nan"),
        }

    run_returns = {
        run_name: run_signals[
            "net_return_bps_1bps_cost"
        ].to_numpy()
        for run_name, run_signals
        in signals.groupby("run_name", sort=False)
    }

    run_names = np.array(list(run_returns))
    rng = np.random.default_rng(RANDOM_STATE)
    bootstrap_means = np.empty(
        BOOTSTRAP_ITERATIONS,
        dtype=float,
    )

    for iteration in range(BOOTSTRAP_ITERATIONS):
        sampled_runs = rng.choice(
            run_names,
            size=len(run_names),
            replace=True,
        )
        sampled_returns = np.concatenate(
            [
                run_returns[run_name]
                for run_name in sampled_runs
            ]
        )
        bootstrap_means[
            iteration
        ] = sampled_returns.mean()

    return {
        "bootstrap_mean_net_bps": float(
            bootstrap_means.mean()
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
            (bootstrap_means > 0).mean()
        ),
    }


def calculate_primary_robustness(
    signals: pd.DataFrame,
) -> dict[str, object]:
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
        }

    net_returns = signals[
        "net_return_bps_1bps_cost"
    ]
    result: dict[str, object] = {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(
            signals["run_name"].nunique()
        ),
        "mean_net_return_bps": float(
            net_returns.mean()
        ),
        "median_net_return_bps": float(
            net_returns.median()
        ),
    }

    if len(signals) > 1:
        best_signal_index = net_returns.idxmax()
        worst_signal_index = net_returns.idxmin()
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

    run_summary = (
        signals.groupby("run_name")
        .agg(
            run_net_sum=(
                "net_return_bps_1bps_cost",
                "sum",
            ),
            run_signal_count=(
                "net_return_bps_1bps_cost",
                "size",
            ),
        )
    )

    if len(run_summary) > 1:
        total_net = float(
            run_summary["run_net_sum"].sum()
        )
        total_signals = int(
            run_summary["run_signal_count"].sum()
        )
        leave_one_run_out = (
            total_net
            - run_summary["run_net_sum"]
        ) / (
            total_signals
            - run_summary["run_signal_count"]
        )

        best_run = run_summary[
            "run_net_sum"
        ].idxmax()
        worst_run = run_summary[
            "run_net_sum"
        ].idxmin()

        result["best_run_name"] = best_run
        result["worst_run_name"] = worst_run
        result[
            "mean_net_without_best_run_bps"
        ] = float(
            leave_one_run_out.loc[best_run]
        )
        result[
            "mean_net_without_worst_run_bps"
        ] = float(
            leave_one_run_out.loc[worst_run]
        )
        result[
            "leave_one_run_out_min_mean_net_bps"
        ] = float(leave_one_run_out.min())
        result[
            "leave_one_run_out_median_mean_net_bps"
        ] = float(leave_one_run_out.median())
        result[
            "leave_one_run_out_max_mean_net_bps"
        ] = float(leave_one_run_out.max())

    result.update(run_cluster_bootstrap(signals))
    return result


print("[INFO] Loading collection log...")
log = pd.read_csv(COLLECTION_LOG_PATH)

require_columns(
    log.columns.tolist(),
    [
        "collection_batch",
        "status",
        "run_name",
        "collection_seconds_requested",
        "depth_events",
        "trade_events",
    ],
    "fresh_trade_collection_log.csv",
)

log["run_name"] = log["run_name"].astype(str)
relevant_log = log[
    log["collection_batch"].isin(
        TRAIN_BATCHES + [TEST_BATCH]
    )
].copy()

print("[INFO] Loading processed trade-flow features...")
available_columns = pd.read_csv(
    DATA_PATH,
    nrows=0,
).columns.tolist()

required_features = list(
    dict.fromkeys(
        REDUCED_BOOK_FEATURES
        + MEDIUM_TRADE_FLOW_FEATURES
        + REDUCED_TRADE_FLOW_FEATURES
    )
)

require_columns(
    available_columns,
    [
        "run_name",
        "row_in_run",
        "mid_price",
    ]
    + required_features,
    "trade_flow_features.csv",
)

relevant_runs = set(relevant_log["run_name"])
data = pd.read_csv(
    DATA_PATH,
    usecols=[
        "run_name",
        "row_in_run",
        "mid_price",
    ]
    + required_features,
)

data["run_name"] = data["run_name"].astype(str)
data = data[
    data["run_name"].isin(relevant_runs)
].copy()
data = data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)
data[required_features] = data[
    required_features
].replace(
    [np.inf, -np.inf],
    np.nan,
)

processed_rows_by_run = (
    data.groupby("run_name").size()
)

quality = build_quality_table(
    log=relevant_log,
    processed_rows_by_run=processed_rows_by_run,
)

quality.to_csv(
    TABLES_DIR / "day40_run_quality_audit.csv",
    index=False,
)
quality[
    ~quality["strict_quality_ok"]
].to_csv(
    TABLES_DIR / "day40_excluded_runs.csv",
    index=False,
)

strict_train_runs = set(
    quality.loc[
        quality["collection_batch"].isin(
            TRAIN_BATCHES
        )
        & quality["strict_quality_ok"],
        "run_name",
    ]
)
strict_test_runs = set(
    quality.loc[
        quality["collection_batch"].eq(
            TEST_BATCH
        )
        & quality["strict_quality_ok"],
        "run_name",
    ]
)

quality_summary = pd.DataFrame(
    [
        {
            "sample": "training_tuesday_wednesday",
            "planned_log_rows": int(
                quality[
                    "collection_batch"
                ].isin(TRAIN_BATCHES).sum()
            ),
            "strict_runs": int(
                len(strict_train_runs)
            ),
            "excluded_runs": int(
                quality[
                    "collection_batch"
                ].isin(TRAIN_BATCHES).sum()
                - len(strict_train_runs)
            ),
        },
        {
            "sample": "friday_fresh_holdout",
            "planned_log_rows": int(
                quality[
                    "collection_batch"
                ].eq(TEST_BATCH).sum()
            ),
            "strict_runs": int(
                len(strict_test_runs)
            ),
            "excluded_runs": int(
                quality[
                    "collection_batch"
                ].eq(TEST_BATCH).sum()
                - len(strict_test_runs)
            ),
        },
    ]
)
quality_summary.to_csv(
    TABLES_DIR / "day40_quality_summary.csv",
    index=False,
)

if not strict_test_runs:
    raise ValueError(
        "No strict Friday runs remain after frozen quality checks."
    )

strict_data = data[
    data["run_name"].isin(
        strict_train_runs | strict_test_runs
    )
].copy()

future_mid_price = (
    strict_data.groupby(
        "run_name",
        sort=False,
    )["mid_price"]
    .shift(-HORIZON)
)

strict_data["future_return_bps"] = (
    future_mid_price
    / strict_data["mid_price"]
    - 1.0
) * 10_000.0

strict_data["move_target"] = np.where(
    strict_data["future_return_bps"].notna(),
    strict_data["future_return_bps"]
    .abs()
    .gt(DEAD_ZONE_BPS)
    .astype(float),
    np.nan,
)

strict_data["direction_target"] = np.where(
    strict_data["future_return_bps"]
    > DEAD_ZONE_BPS,
    1.0,
    np.where(
        strict_data["future_return_bps"]
        < -DEAD_ZONE_BPS,
        0.0,
        np.nan,
    ),
)

train_data = strict_data[
    strict_data["run_name"].isin(
        strict_train_runs
    )
    & strict_data["future_return_bps"].notna()
].copy()

test_data = strict_data[
    strict_data["run_name"].isin(
        strict_test_runs
    )
    & strict_data["future_return_bps"].notna()
].copy()

train_direction = train_data[
    train_data["direction_target"].notna()
].copy()
test_direction = test_data[
    test_data["direction_target"].notna()
].copy()

print("[INFO] Fitting frozen direction model on Tuesday + Wednesday...")
direction_model = make_model(DIRECTION_C)
direction_model.fit(
    train_direction[REDUCED_BOOK_FEATURES],
    train_direction["direction_target"].astype(int),
)
direction_probabilities_all = (
    direction_model.predict_proba(
        test_data[REDUCED_BOOK_FEATURES]
    )[:, 1]
)
direction_probabilities_nonflat = (
    direction_model.predict_proba(
        test_direction[REDUCED_BOOK_FEATURES]
    )[:, 1]
)

print("[INFO] Fitting frozen medium MOVE model...")
medium_move_model = make_model(MOVE_C)
medium_move_model.fit(
    train_data[MEDIUM_TRADE_FLOW_FEATURES],
    train_data["move_target"].astype(int),
)
medium_move_probabilities = (
    medium_move_model.predict_proba(
        test_data[MEDIUM_TRADE_FLOW_FEATURES]
    )[:, 1]
)

print("[INFO] Fitting exploratory reduced MOVE model...")
reduced_move_model = make_model(MOVE_C)
reduced_move_model.fit(
    train_data[REDUCED_TRADE_FLOW_FEATURES],
    train_data["move_target"].astype(int),
)
reduced_move_probabilities = (
    reduced_move_model.predict_proba(
        test_data[REDUCED_TRADE_FLOW_FEATURES]
    )[:, 1]
)

classifier_metrics = pd.DataFrame(
    [
        {
            "model_stage": "direction",
            "feature_set": "reduced_book",
            "model_role": "confirmatory_shared_stage",
            "n_features": len(REDUCED_BOOK_FEATURES),
            "C": DIRECTION_C,
            **evaluate_classifier(
                test_direction["direction_target"].astype(int),
                direction_probabilities_nonflat,
            ),
        },
        {
            "model_stage": "move",
            "feature_set": "medium_trade_flow",
            "model_role": "confirmatory_primary",
            "n_features": len(
                MEDIUM_TRADE_FLOW_FEATURES
            ),
            "C": MOVE_C,
            **evaluate_classifier(
                test_data["move_target"].astype(int),
                medium_move_probabilities,
            ),
        },
        {
            "model_stage": "move",
            "feature_set": "reduced_trade_flow",
            "model_role": "exploratory_classification_benchmark",
            "n_features": len(
                REDUCED_TRADE_FLOW_FEATURES
            ),
            "C": MOVE_C,
            **evaluate_classifier(
                test_data["move_target"].astype(int),
                reduced_move_probabilities,
            ),
        },
    ]
)
classifier_metrics.to_csv(
    TABLES_DIR / "day40_classifier_metrics.csv",
    index=False,
)

primary_signals = build_first_eligible_signals(
    evaluation_data=test_data,
    move_probabilities=medium_move_probabilities,
    direction_probabilities=direction_probabilities_all,
    move_threshold=PRIMARY_MOVE_THRESHOLD,
    direction_threshold=PRIMARY_DIRECTION_THRESHOLD,
    specification_name=(
        "confirmatory_primary_medium_0p75_0p65"
    ),
)

sensitivity_signals = build_first_eligible_signals(
    evaluation_data=test_data,
    move_probabilities=medium_move_probabilities,
    direction_probabilities=direction_probabilities_all,
    move_threshold=SENSITIVITY_MOVE_THRESHOLD,
    direction_threshold=SENSITIVITY_DIRECTION_THRESHOLD,
    specification_name=(
        "sensitivity_medium_0p65_0p65"
    ),
)

deployment_summary = pd.DataFrame(
    [
        {
            "specification": (
                "confirmatory_primary_medium_0p75_0p65"
            ),
            "model_role": "CONFIRMATORY_PRIMARY",
            "move_feature_set": "medium_trade_flow",
            "move_threshold": PRIMARY_MOVE_THRESHOLD,
            "direction_threshold": (
                PRIMARY_DIRECTION_THRESHOLD
            ),
            "round_trip_cost_bps": PRIMARY_COST_BPS,
            **evaluate_signals(
                primary_signals,
                len(test_data),
            ),
        },
        {
            "specification": (
                "sensitivity_medium_0p65_0p65"
            ),
            "model_role": (
                "HIGH_COVERAGE_SENSITIVITY"
            ),
            "move_feature_set": "medium_trade_flow",
            "move_threshold": (
                SENSITIVITY_MOVE_THRESHOLD
            ),
            "direction_threshold": (
                SENSITIVITY_DIRECTION_THRESHOLD
            ),
            "round_trip_cost_bps": PRIMARY_COST_BPS,
            **evaluate_signals(
                sensitivity_signals,
                len(test_data),
            ),
        },
    ]
)
deployment_summary.to_csv(
    TABLES_DIR / "day40_deployment_summary.csv",
    index=False,
)

all_signals = pd.concat(
    [primary_signals, sensitivity_signals],
    ignore_index=True,
)
all_signals.to_csv(
    TABLES_DIR / "day40_selected_signals.csv",
    index=False,
)

cost_rows: list[dict[str, object]] = []

for cost_bps in COST_GRID_BPS:
    if primary_signals.empty:
        cost_rows.append(
            {
                "cost_bps": cost_bps,
                "n_signals": 0,
                "n_signal_runs": 0,
                "mean_net_return_bps": float("nan"),
                "median_net_return_bps": float("nan"),
                "total_net_return_bps": 0.0,
                "positive_net_signal_share": float("nan"),
            }
        )
        continue

    net_returns = (
        primary_signals["signed_return_bps"]
        - cost_bps
    )
    cost_rows.append(
        {
            "cost_bps": cost_bps,
            "n_signals": int(len(primary_signals)),
            "n_signal_runs": int(
                primary_signals[
                    "run_name"
                ].nunique()
            ),
            "mean_net_return_bps": float(
                net_returns.mean()
            ),
            "median_net_return_bps": float(
                net_returns.median()
            ),
            "total_net_return_bps": float(
                net_returns.sum()
            ),
            "positive_net_signal_share": float(
                net_returns.gt(0).mean()
            ),
        }
    )

cost_sensitivity = pd.DataFrame(cost_rows)
cost_sensitivity.to_csv(
    TABLES_DIR / "day40_primary_cost_sensitivity.csv",
    index=False,
)

if primary_signals.empty:
    primary_per_direction = pd.DataFrame()
    primary_per_run = pd.DataFrame()
else:
    primary_per_direction = (
        primary_signals.assign(
            direction_label=np.where(
                primary_signals[
                    "signal_direction"
                ].eq(1),
                "UP_LONG",
                "DOWN_SHORT",
            )
        )
        .groupby(
            "direction_label",
            as_index=False,
        )
        .agg(
            n_signals=("run_name", "size"),
            n_signal_runs=(
                "run_name",
                "nunique",
            ),
            directional_precision=(
                "signed_return_bps",
                lambda values: (
                    values > 0
                ).mean(),
            ),
            mean_signed_return_bps=(
                "signed_return_bps",
                "mean",
            ),
            median_signed_return_bps=(
                "signed_return_bps",
                "median",
            ),
            mean_net_return_bps=(
                "net_return_bps_1bps_cost",
                "mean",
            ),
            median_net_return_bps=(
                "net_return_bps_1bps_cost",
                "median",
            ),
            total_net_return_bps=(
                "net_return_bps_1bps_cost",
                "sum",
            ),
        )
    )

    primary_per_run = (
        primary_signals.groupby(
            "run_name",
            as_index=False,
        )
        .agg(
            n_signals=("run_name", "size"),
            mean_signed_return_bps=(
                "signed_return_bps",
                "mean",
            ),
            median_signed_return_bps=(
                "signed_return_bps",
                "median",
            ),
            mean_net_return_bps=(
                "net_return_bps_1bps_cost",
                "mean",
            ),
            median_net_return_bps=(
                "net_return_bps_1bps_cost",
                "median",
            ),
            total_net_return_bps=(
                "net_return_bps_1bps_cost",
                "sum",
            ),
        )
        .sort_values(
            "total_net_return_bps",
            ascending=False,
        )
    )

primary_per_direction.to_csv(
    TABLES_DIR / "day40_primary_per_direction.csv",
    index=False,
)
primary_per_run.to_csv(
    TABLES_DIR / "day40_primary_per_run.csv",
    index=False,
)

primary_robustness = pd.DataFrame(
    [
        calculate_primary_robustness(
            primary_signals
        )
    ]
)
primary_robustness.to_csv(
    TABLES_DIR / "day40_primary_robustness.csv",
    index=False,
)

if not primary_per_run.empty:
    plt.figure(figsize=(11, 6))
    plt.bar(
        primary_per_run["run_name"],
        primary_per_run[
            "total_net_return_bps"
        ],
    )
    plt.axhline(0.0, linewidth=1)
    plt.ylabel(
        "Total primary net return in run, bps"
    )
    plt.title(
        "Day 40 Friday confirmatory primary result by run"
    )
    plt.xticks(rotation=70, ha="right")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(
        FIGURES_DIR
        / "day40_primary_net_by_run.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()

plt.figure(figsize=(8, 5))
plt.plot(
    cost_sensitivity["cost_bps"],
    cost_sensitivity[
        "mean_net_return_bps"
    ],
    marker="o",
)
plt.axhline(0.0, linewidth=1)
plt.xlabel("Round-trip cost, bps")
plt.ylabel("Mean primary net return, bps")
plt.title(
    "Day 40 Friday primary cost sensitivity"
)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day40_primary_cost_sensitivity.png",
    dpi=150,
    bbox_inches="tight",
)
plt.close()

print()
print("=" * 88)
print("[INFO] Quality summary")
print("=" * 88)
print(quality_summary.to_string(index=False))

print()
print("=" * 88)
print("[INFO] Friday classifier metrics")
print("=" * 88)
print(
    classifier_metrics[
        [
            "model_stage",
            "feature_set",
            "model_role",
            "n_observations",
            "positive_class_share",
            "balanced_accuracy",
            "roc_auc",
            "average_precision",
            "brier_score",
        ]
    ].to_string(index=False)
)

print()
print("=" * 88)
print("[INFO] Frozen deployment results")
print("=" * 88)
print(
    deployment_summary[
        [
            "model_role",
            "move_threshold",
            "direction_threshold",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_signed_return_bps",
            "mean_net_return_bps",
            "median_net_return_bps",
            "total_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 88)
print("[INFO] Primary cost sensitivity")
print("=" * 88)
print(cost_sensitivity.to_string(index=False))

print()
print("=" * 88)
print("[INFO] Primary robustness")
print("=" * 88)
print(primary_robustness.to_string(index=False))

print()
print(
    "[INFO] Day 40 frozen Friday holdout evaluation completed successfully."
)