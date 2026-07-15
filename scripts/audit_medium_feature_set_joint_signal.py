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

# Day 35 selected C = 0.03 for both full and reduced MOVE models.
# We keep the same C for all Day 36 MOVE models so that the feature set is the
# only component that changes.
MOVE_C = 0.03
DIRECTION_C = 1.0

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

DIRECTION_CONFIDENCE_LEVELS = [
    0.60,
    0.65,
    0.70,
    0.75,
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

# Day 34 parsimonious benchmark.
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

# Medium set: remove all notional duplicates, but retain economically distinct
# information that was discarded by the 14-feature reduced set.
#
# Current state (10):
#   trade count and buy/sell decomposition,
#   total and buy/sell volume,
#   signed volume,
#   average trade size,
#   trade imbalance,
#   trade intensity.
#
# Rolling state (14):
#   count, volume, signed volume, imbalance at fast/medium/slow windows,
#   plus rolling intensity at medium/slow windows.
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


# =============================================================================
# General helper functions
# =============================================================================


def require_columns(
    available_columns: list[str],
    required_columns: list[str],
    source_name: str,
) -> None:
    """Raise a readable error when expected columns are absent."""
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
    """Reproduce the full trade-flow feature selection from Days 33-35."""
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
    """Create a standardized L2-regularized logistic regression pipeline."""
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
    """Return NaN if the subset contains only one target class."""
    if y_true.nunique() < 2:
        return float("nan")

    return float(
        roc_auc_score(
            y_true,
            probabilities,
        )
    )


def safe_average_precision(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> float:
    """Return NaN if the subset does not contain both classes."""
    if y_true.nunique() < 2:
        return float("nan")

    return float(
        average_precision_score(
            y_true,
            probabilities,
        )
    )


def evaluate_classifier(
    y_true: pd.Series,
    probabilities: np.ndarray,
) -> dict[str, float | int]:
    """Calculate standard binary-classification diagnostics."""
    predictions = (probabilities >= 0.50).astype(int)

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
        "average_precision": safe_average_precision(
            y_true,
            probabilities,
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
    """Combine MOVE and DIRECTION probabilities into long/short signals."""
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
    signals["direction_probability_up"] = direction_probabilities
    signals["direction_confidence"] = np.maximum(
        direction_probabilities,
        1.0 - direction_probabilities,
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

    signals["move_threshold"] = move_threshold
    signals["direction_threshold"] = direction_threshold

    return signals


def apply_cooldown(
    signals: pd.DataFrame,
    cooldown_events: int,
) -> pd.DataFrame:
    """Keep only the first signal in an overlapping signal episode."""
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
    """Evaluate chronological cooldown signals after a fixed 1 bps cost."""
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
    net_returns = signals["net_return_bps_1bps_cost"]

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
    """Select thresholds on Wednesday only."""
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


def joint_subset_metrics(
    split_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    direction_confidence_threshold: float,
    return_column: str,
) -> dict[str, float | int]:
    """
    Evaluate MOVE ranking only where the direction model is sufficiently
    confident. This is the subset that can contribute to two-stage signals.
    """
    direction_confidence = np.maximum(
        direction_probabilities,
        1.0 - direction_probabilities,
    )

    mask = (
        direction_confidence
        >= direction_confidence_threshold
    )

    subset = split_data.loc[mask]
    subset_probabilities = move_probabilities[mask]

    if subset.empty:
        return {
            "n_rows": 0,
            "row_share": 0.0,
            "move_share": float("nan"),
            "mean_abs_future_return_bps": float("nan"),
            "roc_auc": float("nan"),
            "average_precision": float("nan"),
            "brier_score": float("nan"),
        }

    y_true = subset["move_target"].astype(int)

    return {
        "n_rows": int(len(subset)),
        "row_share": float(
            len(subset) / len(split_data)
        ),
        "move_share": float(y_true.mean()),
        "mean_abs_future_return_bps": float(
            subset[return_column].abs().mean()
        ),
        "roc_auc": safe_roc_auc(
            y_true,
            subset_probabilities,
        ),
        "average_precision": safe_average_precision(
            y_true,
            subset_probabilities,
        ),
        "brier_score": float(
            brier_score_loss(
                y_true,
                subset_probabilities,
            )
        ),
    }


def joint_tail_summary(
    split_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    direction_confidence_threshold: float,
    tail_fraction: float,
    return_column: str,
) -> dict[str, float | int]:
    """
    Inspect the highest MOVE probabilities inside a direction-confident subset.
    This is more relevant to two-stage trading than an unconditional MOVE tail.
    """
    direction_confidence = np.maximum(
        direction_probabilities,
        1.0 - direction_probabilities,
    )

    direction_sign = np.where(
        direction_probabilities >= 0.50,
        1,
        -1,
    )

    mask = (
        direction_confidence
        >= direction_confidence_threshold
    )

    subset = split_data.loc[mask].copy()
    subset_probabilities = move_probabilities[mask]
    subset_direction_sign = direction_sign[mask]

    if subset.empty:
        return {
            "direction_confident_rows": 0,
            "tail_n_rows": 0,
            "tail_probability_min": float("nan"),
            "tail_move_share": float("nan"),
            "subset_move_share": float("nan"),
            "move_share_lift": float("nan"),
            "tail_mean_abs_future_return_bps": float("nan"),
            "tail_median_abs_future_return_bps": float("nan"),
            "tail_directional_precision": float("nan"),
            "tail_mean_direction_signed_return_bps": float("nan"),
            "tail_median_direction_signed_return_bps": float("nan"),
        }

    n_tail = max(
        1,
        int(np.ceil(len(subset) * tail_fraction)),
    )

    local_tail_indices = np.argsort(
        subset_probabilities
    )[-n_tail:]

    tail_data = subset.iloc[
        local_tail_indices
    ].copy()

    tail_direction_sign = subset_direction_sign[
        local_tail_indices
    ]

    tail_signed_returns = (
        tail_direction_sign
        * tail_data[return_column].to_numpy()
    )

    subset_move_share = float(
        subset["move_target"].mean()
    )

    tail_move_share = float(
        tail_data["move_target"].mean()
    )

    return {
        "direction_confident_rows": int(len(subset)),
        "tail_n_rows": int(n_tail),
        "tail_probability_min": float(
            subset_probabilities[
                local_tail_indices
            ].min()
        ),
        "tail_move_share": tail_move_share,
        "subset_move_share": subset_move_share,
        "move_share_lift": float(
            tail_move_share / subset_move_share
        ) if subset_move_share > 0 else float("nan"),
        "tail_mean_abs_future_return_bps": float(
            tail_data[return_column].abs().mean()
        ),
        "tail_median_abs_future_return_bps": float(
            tail_data[return_column].abs().median()
        ),
        "tail_directional_precision": float(
            np.mean(tail_signed_returns > 0)
        ),
        "tail_mean_direction_signed_return_bps": float(
            np.mean(tail_signed_returns)
        ),
        "tail_median_direction_signed_return_bps": float(
            np.median(tail_signed_returns)
        ),
    }


def signal_key_set(
    signals: pd.DataFrame,
) -> set[tuple[str, int, int]]:
    """Create exact signal keys for overlap calculations."""
    return set(
        zip(
            signals["run_name"].astype(str),
            signals["row_in_run"].astype(int),
            signals["signal_direction"].astype(int),
        )
    )


def nearest_episode_matches(
    source_signals: pd.DataFrame,
    target_signals: pd.DataFrame,
    source_name: str,
    target_name: str,
    max_row_distance: int,
) -> pd.DataFrame:
    """
    For every source signal, find the nearest target signal with the same run
    and direction. A match within one h50 window is treated as the same episode.
    """
    rows: list[dict[str, object]] = []

    grouped_target = {
        (str(run_name), int(direction)): group[
            "row_in_run"
        ].astype(int).to_numpy()
        for (run_name, direction), group
        in target_signals.groupby(
            ["run_name", "signal_direction"],
            sort=False,
        )
    }

    for _, signal in source_signals.iterrows():
        run_name = str(signal["run_name"])
        direction = int(signal["signal_direction"])
        source_row = int(signal["row_in_run"])

        target_rows = grouped_target.get(
            (run_name, direction)
        )

        if target_rows is None or len(target_rows) == 0:
            nearest_distance = float("nan")
            matched = False
        else:
            nearest_distance = int(
                np.min(
                    np.abs(
                        target_rows - source_row
                    )
                )
            )
            matched = (
                nearest_distance
                <= max_row_distance
            )

        rows.append(
            {
                "source_model": source_name,
                "target_model": target_name,
                "run_name": run_name,
                "source_row_in_run": source_row,
                "signal_direction": direction,
                "nearest_row_distance": nearest_distance,
                "episode_match": matched,
            }
        )

    return pd.DataFrame(rows)


def calculate_pairwise_overlap(
    signals_a: pd.DataFrame,
    signals_b: pd.DataFrame,
    model_a: str,
    model_b: str,
    max_row_distance: int,
) -> tuple[dict[str, object], pd.DataFrame]:
    """Calculate exact and same-episode overlap between two model variants."""
    keys_a = signal_key_set(signals_a)
    keys_b = signal_key_set(signals_b)

    exact_intersection = keys_a.intersection(keys_b)
    exact_union = keys_a.union(keys_b)

    details_a_to_b = nearest_episode_matches(
        source_signals=signals_a,
        target_signals=signals_b,
        source_name=model_a,
        target_name=model_b,
        max_row_distance=max_row_distance,
    )

    details_b_to_a = nearest_episode_matches(
        source_signals=signals_b,
        target_signals=signals_a,
        source_name=model_b,
        target_name=model_a,
        max_row_distance=max_row_distance,
    )

    details = pd.concat(
        [
            details_a_to_b,
            details_b_to_a,
        ],
        ignore_index=True,
    )

    a_episode_matches = int(
        details_a_to_b["episode_match"].sum()
    )
    b_episode_matches = int(
        details_b_to_a["episode_match"].sum()
    )

    finite_distances = details.loc[
        details["episode_match"],
        "nearest_row_distance",
    ]

    summary = {
        "model_a": model_a,
        "model_b": model_b,
        "n_signals_a": int(len(signals_a)),
        "n_signals_b": int(len(signals_b)),
        "exact_overlap_n": int(len(exact_intersection)),
        "exact_jaccard": float(
            len(exact_intersection) / len(exact_union)
        ) if exact_union else float("nan"),
        "a_episode_matches": a_episode_matches,
        "a_episode_match_share": float(
            a_episode_matches / len(signals_a)
        ) if len(signals_a) else float("nan"),
        "b_episode_matches": b_episode_matches,
        "b_episode_match_share": float(
            b_episode_matches / len(signals_b)
        ) if len(signals_b) else float("nan"),
        "symmetric_episode_overlap": float(
            (a_episode_matches + b_episode_matches)
            / (len(signals_a) + len(signals_b))
        ) if (len(signals_a) + len(signals_b)) else float("nan"),
        "median_matched_row_distance": float(
            finite_distances.median()
        ) if not finite_distances.empty else float("nan"),
        "maximum_episode_distance": max_row_distance,
    }

    return summary, details


# =============================================================================
# Read columns and define feature sets
# =============================================================================

print("[INFO] Reading available dataset columns...")

available_columns = pd.read_csv(
    DATA_PATH,
    nrows=0,
).columns.tolist()

FULL_TRADE_FLOW_FEATURES = (
    select_full_trade_flow_features(
        available_columns
    )
)

require_columns(
    available_columns,
    [
        "run_name",
        "row_in_run",
        "mid_price",
    ]
    + REDUCED_BOOK_FEATURES
    + FULL_TRADE_FLOW_FEATURES
    + MEDIUM_TRADE_FLOW_FEATURES
    + REDUCED_TRADE_FLOW_FEATURES,
    "trade_flow_features.csv",
)

MOVE_FEATURE_SETS = {
    "full_trade_flow": FULL_TRADE_FLOW_FEATURES,
    "medium_trade_flow": MEDIUM_TRADE_FLOW_FEATURES,
    "reduced_trade_flow": REDUCED_TRADE_FLOW_FEATURES,
}

print(
    "[INFO] Full trade-flow features: "
    f"{len(FULL_TRADE_FLOW_FEATURES)}"
)
print(
    "[INFO] Medium trade-flow features: "
    f"{len(MEDIUM_TRADE_FLOW_FEATURES)}"
)
print(
    "[INFO] Reduced trade-flow features: "
    f"{len(REDUCED_TRADE_FLOW_FEATURES)}"
)


# =============================================================================
# Load strict Tuesday, Wednesday and Thursday data
# =============================================================================

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

data["run_name"] = data["run_name"].astype(str)

data = data[
    data["run_name"].isin(strict_runs)
].copy()

data["split"] = data["run_name"].map(
    run_to_split
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

data[all_features] = data[all_features].replace(
    [np.inf, -np.inf],
    np.nan,
)


# =============================================================================
# Build h50 MOVE and DIRECTION targets
# =============================================================================

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
        future_mid / data["mid_price"]
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
# Fixed reduced-book direction model
# =============================================================================

print("[INFO] Training fixed reduced-book direction model...")

direction_model = make_model(
    c_value=DIRECTION_C
)

direction_model.fit(
    train_direction[REDUCED_BOOK_FEATURES],
    train_direction["direction_target"].astype(int),
)

direction_metric_rows: list[dict[str, object]] = []

for split_name, split_data in [
    ("validation", validation_direction),
    ("test", test_direction),
]:
    probabilities = direction_model.predict_proba(
        split_data[REDUCED_BOOK_FEATURES]
    )[:, 1]

    direction_metric_rows.append(
        {
            "split": split_name,
            "n_features": len(REDUCED_BOOK_FEATURES),
            "C": DIRECTION_C,
            **evaluate_classifier(
                split_data["direction_target"].astype(int),
                probabilities,
            ),
        }
    )

direction_metrics = pd.DataFrame(
    direction_metric_rows
)

validation_direction_probabilities = (
    direction_model.predict_proba(
        validation_all[REDUCED_BOOK_FEATURES]
    )[:, 1]
)

test_direction_probabilities = (
    direction_model.predict_proba(
        test_all[REDUCED_BOOK_FEATURES]
    )[:, 1]
)


# =============================================================================
# Train full, medium and reduced MOVE models
# =============================================================================

print("[INFO] Training full, medium and reduced MOVE models...")

move_models: dict[str, Pipeline] = {}
move_predictions: dict[
    str,
    dict[str, np.ndarray],
] = {
    "validation": {},
    "test": {},
}

move_metric_rows: list[dict[str, object]] = []
coefficient_rows: list[dict[str, object]] = []
joint_subset_rows: list[dict[str, object]] = []
joint_tail_rows: list[dict[str, object]] = []

for feature_set_name, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    print(
        "[INFO] Fitting "
        f"{feature_set_name} with C={MOVE_C}"
    )

    model = make_model(c_value=MOVE_C)

    model.fit(
        train_all[feature_columns],
        train_all["move_target"].astype(int),
    )

    move_models[feature_set_name] = model

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
                "move_feature_set": feature_set_name,
                "n_features": len(feature_columns),
                "C": MOVE_C,
                "feature": feature,
                "coefficient": float(coefficient),
                "absolute_coefficient": float(
                    abs(coefficient)
                ),
            }
        )

    for split_name, split_data, direction_probabilities in [
        (
            "validation",
            validation_all,
            validation_direction_probabilities,
        ),
        (
            "test",
            test_all,
            test_direction_probabilities,
        ),
    ]:
        probabilities = model.predict_proba(
            split_data[feature_columns]
        )[:, 1]

        move_predictions[split_name][
            feature_set_name
        ] = probabilities

        move_metric_rows.append(
            {
                "move_feature_set": feature_set_name,
                "n_features": len(feature_columns),
                "C": MOVE_C,
                "split": split_name,
                **evaluate_classifier(
                    split_data["move_target"].astype(int),
                    probabilities,
                ),
            }
        )

        for confidence_threshold in (
            DIRECTION_CONFIDENCE_LEVELS
        ):
            joint_subset_rows.append(
                {
                    "move_feature_set": feature_set_name,
                    "n_features": len(feature_columns),
                    "C": MOVE_C,
                    "split": split_name,
                    "direction_confidence_threshold": (
                        confidence_threshold
                    ),
                    **joint_subset_metrics(
                        split_data=split_data,
                        move_probabilities=probabilities,
                        direction_probabilities=(
                            direction_probabilities
                        ),
                        direction_confidence_threshold=(
                            confidence_threshold
                        ),
                        return_column=RETURN_COLUMN,
                    ),
                }
            )

            for tail_fraction in TAIL_FRACTIONS:
                joint_tail_rows.append(
                    {
                        "move_feature_set": feature_set_name,
                        "n_features": len(feature_columns),
                        "C": MOVE_C,
                        "split": split_name,
                        "direction_confidence_threshold": (
                            confidence_threshold
                        ),
                        "tail_fraction": tail_fraction,
                        **joint_tail_summary(
                            split_data=split_data,
                            move_probabilities=probabilities,
                            direction_probabilities=(
                                direction_probabilities
                            ),
                            direction_confidence_threshold=(
                                confidence_threshold
                            ),
                            tail_fraction=tail_fraction,
                            return_column=RETURN_COLUMN,
                        ),
                    }
                )

move_metrics = pd.DataFrame(
    move_metric_rows
)

model_coefficients = (
    pd.DataFrame(coefficient_rows)
    .sort_values(
        [
            "move_feature_set",
            "absolute_coefficient",
        ],
        ascending=[True, False],
    )
)

joint_direction_confident_metrics = pd.DataFrame(
    joint_subset_rows
)

joint_tail_summary_table = pd.DataFrame(
    joint_tail_rows
)


# =============================================================================
# Wednesday threshold selection and Thursday deployment
# =============================================================================

print("[INFO] Selecting thresholds on Wednesday...")

threshold_rows: list[dict[str, object]] = []
selected_threshold_rows: list[dict[str, object]] = []
test_summary_rows: list[dict[str, object]] = []
selected_test_signals: dict[str, pd.DataFrame] = {}

for feature_set_name, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    candidate_rows: list[dict[str, object]] = []

    validation_move_probabilities = (
        move_predictions["validation"][
            feature_set_name
        ]
    )

    for move_threshold in MOVE_THRESHOLDS:
        for direction_threshold in (
            DIRECTION_THRESHOLDS
        ):
            raw_signals = build_signals(
                evaluation_data=validation_all,
                move_probabilities=(
                    validation_move_probabilities
                ),
                direction_probabilities=(
                    validation_direction_probabilities
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

            metrics = evaluate_deployment(
                signals=cooldown_signals,
                n_evaluation_rows=len(
                    validation_all
                ),
            )

            row = {
                "move_feature_set": feature_set_name,
                "n_features": len(feature_columns),
                "C": MOVE_C,
                "move_threshold": move_threshold,
                "direction_threshold": (
                    direction_threshold
                ),
                "threshold_eligible": (
                    metrics["n_signals"]
                    >= MIN_VALIDATION_COOLDOWN_SIGNALS
                    and metrics["n_signal_runs"]
                    >= MIN_VALIDATION_SIGNAL_RUNS
                ),
                **metrics,
            }

            threshold_rows.append(row)
            candidate_rows.append(row)

    selected = choose_validation_threshold(
        pd.DataFrame(candidate_rows)
    )

    selected_threshold_rows.append(
        selected.to_dict()
    )

    test_raw_signals = build_signals(
        evaluation_data=test_all,
        move_probabilities=(
            move_predictions["test"][
                feature_set_name
            ]
        ),
        direction_probabilities=(
            test_direction_probabilities
        ),
        return_column=RETURN_COLUMN,
        move_threshold=float(
            selected["move_threshold"]
        ),
        direction_threshold=float(
            selected["direction_threshold"]
        ),
    )

    test_cooldown_signals = apply_cooldown(
        test_raw_signals,
        cooldown_events=HORIZON,
    )

    test_cooldown_signals[
        "move_feature_set"
    ] = feature_set_name

    selected_test_signals[
        feature_set_name
    ] = test_cooldown_signals

    test_metrics = evaluate_deployment(
        signals=test_cooldown_signals,
        n_evaluation_rows=len(test_all),
    )

    test_summary_rows.append(
        {
            "move_feature_set": feature_set_name,
            "n_features": len(feature_columns),
            "C": MOVE_C,
            "selected_move_threshold": float(
                selected["move_threshold"]
            ),
            "selected_direction_threshold": float(
                selected["direction_threshold"]
            ),
            "validation_n_signals": int(
                selected["n_signals"]
            ),
            "validation_n_signal_runs": int(
                selected["n_signal_runs"]
            ),
            "validation_mean_signed_return_bps": float(
                selected["mean_signed_return_bps"]
            ),
            "validation_mean_net_return_bps": float(
                selected["mean_net_return_bps"]
            ),
            **test_metrics,
        }
    )

validation_threshold_grid = pd.DataFrame(
    threshold_rows
)

selected_thresholds = pd.DataFrame(
    selected_threshold_rows
)

test_deployment_summary = pd.DataFrame(
    test_summary_rows
)

all_selected_test_signals = pd.concat(
    list(selected_test_signals.values()),
    ignore_index=True,
)


# =============================================================================
# Signal-overlap audit
# =============================================================================

print("[INFO] Calculating signal overlap...")

model_pairs = [
    ("full_trade_flow", "medium_trade_flow"),
    ("full_trade_flow", "reduced_trade_flow"),
    ("medium_trade_flow", "reduced_trade_flow"),
]

overlap_summary_rows: list[dict[str, object]] = []
overlap_detail_tables: list[pd.DataFrame] = []

for model_a, model_b in model_pairs:
    summary, details = calculate_pairwise_overlap(
        signals_a=selected_test_signals[model_a],
        signals_b=selected_test_signals[model_b],
        model_a=model_a,
        model_b=model_b,
        max_row_distance=HORIZON,
    )

    overlap_summary_rows.append(summary)
    overlap_detail_tables.append(details)

signal_overlap_summary = pd.DataFrame(
    overlap_summary_rows
)

signal_overlap_details = pd.concat(
    overlap_detail_tables,
    ignore_index=True,
)


# =============================================================================
# Feature inventory
# =============================================================================

feature_inventory_rows: list[dict[str, object]] = []

for feature_set_name, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    for feature_position, feature in enumerate(
        feature_columns,
        start=1,
    ):
        feature_inventory_rows.append(
            {
                "move_feature_set": feature_set_name,
                "n_features": len(feature_columns),
                "feature_position": feature_position,
                "feature": feature,
                "contains_notional": (
                    "notional" in feature
                ),
            }
        )

feature_inventory = pd.DataFrame(
    feature_inventory_rows
)


# =============================================================================
# Save tables
# =============================================================================

feature_inventory.to_csv(
    TABLES_DIR
    / "day36_feature_set_inventory.csv",
    index=False,
)

direction_metrics.to_csv(
    TABLES_DIR
    / "day36_direction_sanity_metrics.csv",
    index=False,
)

move_metrics.to_csv(
    TABLES_DIR
    / "day36_move_model_metrics.csv",
    index=False,
)

model_coefficients.to_csv(
    TABLES_DIR
    / "day36_move_model_coefficients.csv",
    index=False,
)

joint_direction_confident_metrics.to_csv(
    TABLES_DIR
    / "day36_joint_direction_confident_metrics.csv",
    index=False,
)

joint_tail_summary_table.to_csv(
    TABLES_DIR
    / "day36_joint_tail_summary.csv",
    index=False,
)

validation_threshold_grid.to_csv(
    TABLES_DIR
    / "day36_validation_threshold_grid.csv",
    index=False,
)

selected_thresholds.to_csv(
    TABLES_DIR
    / "day36_selected_thresholds.csv",
    index=False,
)

test_deployment_summary.to_csv(
    TABLES_DIR
    / "day36_test_deployment_summary.csv",
    index=False,
)

all_selected_test_signals.to_csv(
    TABLES_DIR
    / "day36_selected_test_signals.csv",
    index=False,
)

signal_overlap_summary.to_csv(
    TABLES_DIR
    / "day36_signal_overlap_summary.csv",
    index=False,
)

signal_overlap_details.to_csv(
    TABLES_DIR
    / "day36_signal_overlap_details.csv",
    index=False,
)


# =============================================================================
# Figures
# =============================================================================

auc_plot = move_metrics.pivot(
    index="move_feature_set",
    columns="split",
    values="roc_auc",
)

auc_plot.plot(
    kind="bar",
    figsize=(10, 6),
)

plt.axhline(0.50, linewidth=1)
plt.ylabel("MOVE ROC-AUC")
plt.xlabel("MOVE feature set")
plt.title("Full, medium and reduced MOVE ranking")
plt.xticks(rotation=20)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    FIGURES_DIR
    / "day36_move_auc_feature_sets.png",
    dpi=150,
    bbox_inches="tight",
)

plt.close()

plt.figure(figsize=(10, 6))

plt.bar(
    test_deployment_summary[
        "move_feature_set"
    ],
    test_deployment_summary[
        "mean_net_return_bps"
    ],
)

plt.axhline(0.0, linewidth=1)
plt.ylabel("Thursday mean net return, bps")
plt.xlabel("MOVE feature set")
plt.title(
    "Thursday deployment after Wednesday threshold selection"
)
plt.xticks(rotation=20)
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig(
    FIGURES_DIR
    / "day36_test_net_feature_sets.png",
    dpi=150,
    bbox_inches="tight",
)

plt.close()

joint_auc_plot = (
    joint_direction_confident_metrics[
        joint_direction_confident_metrics[
            "split"
        ].eq("test")
    ]
    .pivot(
        index="direction_confidence_threshold",
        columns="move_feature_set",
        values="roc_auc",
    )
)

joint_auc_plot.plot(
    marker="o",
    figsize=(10, 6),
)

plt.axhline(0.50, linewidth=1)
plt.ylabel("Thursday MOVE ROC-AUC")
plt.xlabel("Direction confidence threshold")
plt.title(
    "MOVE ranking inside direction-confident observations"
)
plt.grid(alpha=0.3)
plt.tight_layout()

plt.savefig(
    FIGURES_DIR
    / "day36_joint_subset_auc.png",
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
print("[INFO] Full, medium and reduced MOVE metrics")
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
print("[INFO] Selected Wednesday thresholds")
print("=" * 80)
print(
    selected_thresholds[
        [
            "move_feature_set",
            "n_features",
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
print("[INFO] Thursday deployment")
print("=" * 80)
print(
    test_deployment_summary.to_string(
        index=False
    )
)

print()
print("=" * 80)
print("[INFO] Signal overlap")
print("=" * 80)
print(
    signal_overlap_summary.to_string(
        index=False
    )
)

print()
print(
    "[INFO] Day 36 medium feature-set and joint-signal audit "
    "completed successfully."
)