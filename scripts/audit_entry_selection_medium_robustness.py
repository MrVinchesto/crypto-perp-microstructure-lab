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
DAY36_THRESHOLDS_PATH = Path("reports/tables/day36_selected_thresholds.csv")

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

MOVE_C = 0.03
DIRECTION_C = 1.0

COMMON_MOVE_THRESHOLD = 0.65
COMMON_DIRECTION_THRESHOLD = 0.65

BOOTSTRAP_ITERATIONS = 5_000
RANDOM_STATE = 42


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


# =============================================================================
# General helpers
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
            f"{source_name} is missing required columns: {missing_columns}"
        )


def select_full_trade_flow_features(
    available_columns: list[str],
) -> list[str]:
    """Reproduce the full trade-flow feature selection from earlier days."""
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

    return list(dict.fromkeys(selected_features))


def make_model(C: float) -> Pipeline:
    """Create a standardized L2 logistic-regression pipeline."""
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
        "roc_auc": safe_roc_auc(y_true, probabilities),
        "average_precision": float(
            average_precision_score(y_true, probabilities)
        ),
        "brier_score": float(
            brier_score_loss(y_true, probabilities)
        ),
    }


# =============================================================================
# Signal construction and entry rules
# =============================================================================

def build_candidate_signals(
    evaluation_data: pd.DataFrame,
    move_probabilities: np.ndarray,
    direction_probabilities: np.ndarray,
    return_column: str,
    move_threshold: float,
    direction_threshold: float,
) -> pd.DataFrame:
    """Create every row that satisfies both MOVE and direction filters."""
    candidates = evaluation_data[
        [
            "run_name",
            "row_in_run",
            "split",
            return_column,
        ]
    ].copy()

    candidates = candidates.rename(
        columns={return_column: "future_return_bps"}
    )

    candidates["move_probability"] = move_probabilities
    candidates["direction_probability_up"] = direction_probabilities
    candidates["direction_confidence"] = np.maximum(
        direction_probabilities,
        1.0 - direction_probabilities,
    )
    candidates["joint_confidence"] = (
        candidates["move_probability"]
        * candidates["direction_confidence"]
    )

    move_filter = (
        candidates["move_probability"] >= move_threshold
    )
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
        np.where(
            move_filter & short_filter,
            -1,
            0,
        ),
    )

    candidates = candidates[
        candidates["signal_direction"] != 0
    ].copy()

    candidates["signed_return_bps"] = (
        candidates["signal_direction"]
        * candidates["future_return_bps"]
    )
    candidates["net_return_bps_1bps_cost"] = (
        candidates["signed_return_bps"]
        - ROUND_TRIP_COST_BPS
    )
    candidates["move_threshold"] = move_threshold
    candidates["direction_threshold"] = direction_threshold

    return candidates


def assign_first_hit_anchored_episodes(
    candidates: pd.DataFrame,
    episode_window_events: int,
) -> pd.DataFrame:
    """
    Build episodes anchored to the first eligible row.

    For each run:
    - the first candidate starts an episode;
    - every candidate up to first_row + episode_window_events belongs to it;
    - the next episode starts at the first later candidate.

    Under this definition, selecting the first row exactly reproduces the
    current first-hit cooldown rule. Selecting a later row is diagnostic and
    uses future candidates inside the episode, so it is not deployable live.
    """
    if candidates.empty:
        result = candidates.copy()
        result["episode_id"] = pd.Series(dtype="int64")
        result["episode_anchor_row"] = pd.Series(dtype="int64")
        return result

    episode_parts: list[pd.DataFrame] = []
    global_episode_id = 0

    ordered = candidates.sort_values(
        ["run_name", "row_in_run"]
    )

    for _, run_candidates in ordered.groupby(
        "run_name",
        sort=False,
    ):
        run_candidates = run_candidates.copy()
        row_values = run_candidates["row_in_run"].to_numpy()
        start_position = 0

        while start_position < len(run_candidates):
            anchor_row = int(row_values[start_position])
            end_position = start_position

            while (
                end_position + 1 < len(run_candidates)
                and int(row_values[end_position + 1])
                <= anchor_row + episode_window_events
            ):
                end_position += 1

            episode = run_candidates.iloc[
                start_position:end_position + 1
            ].copy()

            episode["episode_id"] = global_episode_id
            episode["episode_anchor_row"] = anchor_row
            episode_parts.append(episode)

            global_episode_id += 1
            start_position = end_position + 1

    return pd.concat(
        episode_parts,
        ignore_index=True,
    )


def select_entries_from_episodes(
    episode_candidates: pd.DataFrame,
    entry_rule: str,
) -> pd.DataFrame:
    """Select one candidate from every pre-defined episode."""
    if episode_candidates.empty:
        selected = episode_candidates.copy()
        selected["entry_rule"] = entry_rule
        return selected

    selected_rows: list[pd.Series] = []

    for _, episode in episode_candidates.groupby(
        "episode_id",
        sort=False,
    ):
        episode = episode.sort_values("row_in_run")

        if entry_rule == "first_eligible":
            selected_row = episode.iloc[0]

        elif entry_rule == "max_move_probability":
            selected_row = (
                episode.sort_values(
                    [
                        "move_probability",
                        "row_in_run",
                    ],
                    ascending=[
                        False,
                        True,
                    ],
                )
                .iloc[0]
            )

        elif entry_rule == "max_joint_confidence":
            selected_row = (
                episode.sort_values(
                    [
                        "joint_confidence",
                        "row_in_run",
                    ],
                    ascending=[
                        False,
                        True,
                    ],
                )
                .iloc[0]
            )

        else:
            raise ValueError(
                f"Unknown entry rule: {entry_rule}"
            )

        selected_rows.append(selected_row)

    selected = pd.DataFrame(selected_rows).reset_index(drop=True)
    selected["entry_rule"] = entry_rule
    selected["entry_delay_from_anchor_events"] = (
        selected["row_in_run"]
        - selected["episode_anchor_row"]
    )

    return selected


def evaluate_deployment(
    signals: pd.DataFrame,
    n_evaluation_rows: int,
) -> dict[str, float | int]:
    """Evaluate one selected signal per episode."""
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
            "up_signal_share": float("nan"),
            "mean_entry_delay_events": float("nan"),
            "median_entry_delay_events": float("nan"),
        }

    return {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(signals["run_name"].nunique()),
        "coverage": float(len(signals) / n_evaluation_rows),
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
            signals["net_return_bps_1bps_cost"].mean()
        ),
        "median_net_return_bps": float(
            signals["net_return_bps_1bps_cost"].median()
        ),
        "total_net_return_bps": float(
            signals["net_return_bps_1bps_cost"].sum()
        ),
        "up_signal_share": float(
            signals["signal_direction"].eq(1).mean()
        ),
        "mean_entry_delay_events": float(
            signals["entry_delay_from_anchor_events"].mean()
        ),
        "median_entry_delay_events": float(
            signals["entry_delay_from_anchor_events"].median()
        ),
    }


# =============================================================================
# Robustness helpers
# =============================================================================

def trimmed_mean(
    values: np.ndarray,
    trim_fraction: float = 0.10,
) -> float:
    sorted_values = np.sort(values)
    trim_count = int(len(sorted_values) * trim_fraction)

    if trim_count == 0:
        return float(sorted_values.mean())

    if len(sorted_values) - 2 * trim_count <= 0:
        return float(sorted_values.mean())

    return float(
        sorted_values[
            trim_count:-trim_count
        ].mean()
    )


def run_cluster_bootstrap(
    signals: pd.DataFrame,
    n_iterations: int,
    random_state: int,
) -> dict[str, float]:
    """Resample whole signal-bearing runs with replacement."""
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
    random_generator = np.random.default_rng(random_state)
    bootstrap_means = np.empty(n_iterations, dtype=float)

    for iteration in range(n_iterations):
        sampled_runs = random_generator.choice(
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

        bootstrap_means[iteration] = sampled_returns.mean()

    return {
        "bootstrap_mean_net_bps": float(bootstrap_means.mean()),
        "bootstrap_ci_lower_2_5": float(
            np.quantile(bootstrap_means, 0.025)
        ),
        "bootstrap_ci_upper_97_5": float(
            np.quantile(bootstrap_means, 0.975)
        ),
        "bootstrap_share_mean_net_positive": float(
            (bootstrap_means > 0).mean()
        ),
    }


def calculate_robustness_summary(
    signals: pd.DataFrame,
) -> dict[str, object]:
    """Calculate signal-level and run-level sensitivity."""
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
        }

    net_returns = signals[
        "net_return_bps_1bps_cost"
    ]
    signed_returns = signals["signed_return_bps"]

    result: dict[str, object] = {
        "n_signals": int(len(signals)),
        "n_signal_runs": int(
            signals["run_name"].nunique()
        ),
        "mean_signed_return_bps": float(
            signed_returns.mean()
        ),
        "median_signed_return_bps": float(
            signed_returns.median()
        ),
        "trimmed_mean_signed_return_bps": trimmed_mean(
            signed_returns.to_numpy()
        ),
        "mean_net_return_bps": float(
            net_returns.mean()
        ),
        "median_net_return_bps": float(
            net_returns.median()
        ),
        "trimmed_mean_net_return_bps": trimmed_mean(
            net_returns.to_numpy()
        ),
    }

    if len(signals) > 1:
        best_signal_index = net_returns.idxmax()
        worst_signal_index = net_returns.idxmin()

        result["mean_net_without_best_signal_bps"] = float(
            net_returns.drop(best_signal_index).mean()
        )
        result["mean_net_without_worst_signal_bps"] = float(
            net_returns.drop(worst_signal_index).mean()
        )

        leave_one_signal_out_means = (
            net_returns.sum() - net_returns.to_numpy()
        ) / (len(net_returns) - 1)

        result[
            "leave_one_signal_out_min_mean_net_bps"
        ] = float(leave_one_signal_out_means.min())
        result[
            "leave_one_signal_out_median_mean_net_bps"
        ] = float(np.median(leave_one_signal_out_means))
        result[
            "leave_one_signal_out_max_mean_net_bps"
        ] = float(leave_one_signal_out_means.max())

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
        total_net_return = float(
            run_summary["run_net_sum"].sum()
        )
        total_signal_count = int(
            run_summary["run_signal_count"].sum()
        )

        leave_one_run_out_means = (
            total_net_return
            - run_summary["run_net_sum"]
        ) / (
            total_signal_count
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
        result["mean_net_without_best_run_bps"] = float(
            leave_one_run_out_means.loc[best_run]
        )
        result["mean_net_without_worst_run_bps"] = float(
            leave_one_run_out_means.loc[worst_run]
        )
        result[
            "leave_one_run_out_min_mean_net_bps"
        ] = float(leave_one_run_out_means.min())
        result[
            "leave_one_run_out_median_mean_net_bps"
        ] = float(leave_one_run_out_means.median())
        result[
            "leave_one_run_out_max_mean_net_bps"
        ] = float(leave_one_run_out_means.max())

    result.update(
        run_cluster_bootstrap(
            signals=signals,
            n_iterations=BOOTSTRAP_ITERATIONS,
            random_state=RANDOM_STATE,
        )
    )

    return result


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
    "[INFO] Full / medium / reduced feature counts: "
    f"{len(FULL_TRADE_FLOW_FEATURES)} / "
    f"{len(MEDIUM_TRADE_FLOW_FEATURES)} / "
    f"{len(REDUCED_TRADE_FLOW_FEATURES)}"
)


# =============================================================================
# Load strict run metadata
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

strict_runs = set(strict_metadata["run_name"])


# =============================================================================
# Load data and build targets
# =============================================================================

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
data["split"] = data["run_name"].map(run_to_split)

data = data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)

all_features = list(
    dict.fromkeys(
        REDUCED_BOOK_FEATURES
        + FULL_TRADE_FLOW_FEATURES
    )
)

data[all_features] = data[
    all_features
].replace(
    [np.inf, -np.inf],
    np.nan,
)

future_mid_price = (
    data.groupby(
        "run_name",
        sort=False,
    )["mid_price"]
    .shift(-HORIZON)
)

RETURN_COLUMN = (
    f"future_mid_return_bps_h{HORIZON}"
)

data[RETURN_COLUMN] = (
    future_mid_price
    / data["mid_price"]
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
# Train fixed direction model
# =============================================================================

print("[INFO] Training fixed reduced-book direction model...")

direction_model = make_model(DIRECTION_C)
direction_model.fit(
    train_direction[REDUCED_BOOK_FEATURES],
    train_direction[
        "direction_target"
    ].astype(int),
)

direction_probabilities = {
    "validation": direction_model.predict_proba(
        validation_all[REDUCED_BOOK_FEATURES]
    )[:, 1],
    "test": direction_model.predict_proba(
        test_all[REDUCED_BOOK_FEATURES]
    )[:, 1],
}

direction_sanity_rows: list[dict[str, object]] = []

for split_name, split_data in [
    ("validation", validation_direction),
    ("test", test_direction),
]:
    probabilities = direction_model.predict_proba(
        split_data[REDUCED_BOOK_FEATURES]
    )[:, 1]

    direction_sanity_rows.append(
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

direction_sanity = pd.DataFrame(
    direction_sanity_rows
)


# =============================================================================
# Train MOVE models
# =============================================================================

print("[INFO] Training MOVE models...")

move_probabilities: dict[
    str,
    dict[str, np.ndarray],
] = {
    "validation": {},
    "test": {},
}

move_metric_rows: list[dict[str, object]] = []

for feature_set_name, feature_columns in (
    MOVE_FEATURE_SETS.items()
):
    print(
        f"[INFO] Fitting {feature_set_name} with C={MOVE_C}"
    )

    model = make_model(MOVE_C)
    model.fit(
        train_all[feature_columns],
        train_all["move_target"].astype(int),
    )

    for split_name, split_data in [
        ("validation", validation_all),
        ("test", test_all),
    ]:
        probabilities = model.predict_proba(
            split_data[feature_columns]
        )[:, 1]

        move_probabilities[
            split_name
        ][feature_set_name] = probabilities

        move_metric_rows.append(
            {
                "move_feature_set": feature_set_name,
                "n_features": len(feature_columns),
                "C": MOVE_C,
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
# Load model-specific thresholds selected on Wednesday in Day 36
# =============================================================================

print("[INFO] Loading Day 36 thresholds...")

day36_thresholds = pd.read_csv(
    DAY36_THRESHOLDS_PATH
)

require_columns(
    day36_thresholds.columns.tolist(),
    [
        "move_feature_set",
        "move_threshold",
        "direction_threshold",
    ],
    "day36_selected_thresholds.csv",
)

model_specific_thresholds = (
    day36_thresholds
    .set_index("move_feature_set")
    [
        [
            "move_threshold",
            "direction_threshold",
        ]
    ]
    .to_dict("index")
)

for feature_set_name in MOVE_FEATURE_SETS:
    if feature_set_name not in model_specific_thresholds:
        raise ValueError(
            "No Day 36 threshold found for "
            f"{feature_set_name}"
        )


THRESHOLD_MODES = {
    "model_specific": model_specific_thresholds,
    "fixed_common_0p65_0p65": {
        feature_set_name: {
            "move_threshold": COMMON_MOVE_THRESHOLD,
            "direction_threshold": COMMON_DIRECTION_THRESHOLD,
        }
        for feature_set_name in MOVE_FEATURE_SETS
    },
}

ENTRY_RULES = [
    "first_eligible",
    "max_move_probability",
    "max_joint_confidence",
]


# =============================================================================
# Evaluate all models, threshold modes and entry rules
# =============================================================================

print("[INFO] Evaluating entry-selection rules...")

entry_summary_rows: list[dict[str, object]] = []
all_signal_tables: list[pd.DataFrame] = []
timing_comparison_rows: list[dict[str, object]] = []

for threshold_mode, thresholds_by_model in (
    THRESHOLD_MODES.items()
):
    for feature_set_name in MOVE_FEATURE_SETS:
        move_threshold = float(
            thresholds_by_model[
                feature_set_name
            ]["move_threshold"]
        )
        direction_threshold = float(
            thresholds_by_model[
                feature_set_name
            ]["direction_threshold"]
        )

        for split_name, split_data in [
            ("validation", validation_all),
            ("test", test_all),
        ]:
            candidates = build_candidate_signals(
                evaluation_data=split_data,
                move_probabilities=(
                    move_probabilities[
                        split_name
                    ][feature_set_name]
                ),
                direction_probabilities=(
                    direction_probabilities[
                        split_name
                    ]
                ),
                return_column=RETURN_COLUMN,
                move_threshold=move_threshold,
                direction_threshold=(
                    direction_threshold
                ),
            )

            episodes = assign_first_hit_anchored_episodes(
                candidates=candidates,
                episode_window_events=HORIZON,
            )

            selected_by_rule: dict[
                str,
                pd.DataFrame,
            ] = {}

            for entry_rule in ENTRY_RULES:
                selected = select_entries_from_episodes(
                    episode_candidates=episodes,
                    entry_rule=entry_rule,
                )

                selected[
                    "move_feature_set"
                ] = feature_set_name
                selected[
                    "threshold_mode"
                ] = threshold_mode
                selected["split_name"] = split_name

                selected_by_rule[
                    entry_rule
                ] = selected

                all_signal_tables.append(selected)

                entry_summary_rows.append(
                    {
                        "move_feature_set": (
                            feature_set_name
                        ),
                        "n_features": len(
                            MOVE_FEATURE_SETS[
                                feature_set_name
                            ]
                        ),
                        "threshold_mode": (
                            threshold_mode
                        ),
                        "split": split_name,
                        "move_threshold": (
                            move_threshold
                        ),
                        "direction_threshold": (
                            direction_threshold
                        ),
                        "entry_rule": entry_rule,
                        "entry_rule_is_live_deployable": (
                            entry_rule
                            == "first_eligible"
                        ),
                        **evaluate_deployment(
                            signals=selected,
                            n_evaluation_rows=len(
                                split_data
                            ),
                        ),
                    }
                )

            first_entries = selected_by_rule[
                "first_eligible"
            ][
                [
                    "episode_id",
                    "run_name",
                    "row_in_run",
                    "signal_direction",
                    "signed_return_bps",
                    "net_return_bps_1bps_cost",
                ]
            ].rename(
                columns={
                    "row_in_run": (
                        "first_row_in_run"
                    ),
                    "signal_direction": (
                        "first_signal_direction"
                    ),
                    "signed_return_bps": (
                        "first_signed_return_bps"
                    ),
                    "net_return_bps_1bps_cost": (
                        "first_net_return_bps"
                    ),
                }
            )

            for diagnostic_rule in [
                "max_move_probability",
                "max_joint_confidence",
            ]:
                diagnostic_entries = selected_by_rule[
                    diagnostic_rule
                ][
                    [
                        "episode_id",
                        "run_name",
                        "row_in_run",
                        "signal_direction",
                        "signed_return_bps",
                        "net_return_bps_1bps_cost",
                        "move_probability",
                        "direction_confidence",
                        "joint_confidence",
                    ]
                ].rename(
                    columns={
                        "row_in_run": (
                            "diagnostic_row_in_run"
                        ),
                        "signal_direction": (
                            "diagnostic_signal_direction"
                        ),
                        "signed_return_bps": (
                            "diagnostic_signed_return_bps"
                        ),
                        "net_return_bps_1bps_cost": (
                            "diagnostic_net_return_bps"
                        ),
                    }
                )

                comparison = first_entries.merge(
                    diagnostic_entries,
                    on=[
                        "episode_id",
                        "run_name",
                    ],
                    how="inner",
                    validate="one_to_one",
                )

                comparison[
                    "row_shift_events"
                ] = (
                    comparison[
                        "diagnostic_row_in_run"
                    ]
                    - comparison[
                        "first_row_in_run"
                    ]
                )

                comparison[
                    "same_signal_direction"
                ] = (
                    comparison[
                        "diagnostic_signal_direction"
                    ]
                    == comparison[
                        "first_signal_direction"
                    ]
                )

                comparison[
                    "net_improvement_vs_first_bps"
                ] = (
                    comparison[
                        "diagnostic_net_return_bps"
                    ]
                    - comparison[
                        "first_net_return_bps"
                    ]
                )

                timing_comparison_rows.append(
                    {
                        "move_feature_set": (
                            feature_set_name
                        ),
                        "threshold_mode": (
                            threshold_mode
                        ),
                        "split": split_name,
                        "diagnostic_entry_rule": (
                            diagnostic_rule
                        ),
                        "n_episodes": int(
                            len(comparison)
                        ),
                        "share_same_row_as_first": float(
                            comparison[
                                "row_shift_events"
                            ].eq(0).mean()
                        ),
                        "share_same_direction_as_first": float(
                            comparison[
                                "same_signal_direction"
                            ].mean()
                        ),
                        "mean_row_shift_events": float(
                            comparison[
                                "row_shift_events"
                            ].mean()
                        ),
                        "median_row_shift_events": float(
                            comparison[
                                "row_shift_events"
                            ].median()
                        ),
                        "mean_net_improvement_vs_first_bps": float(
                            comparison[
                                "net_improvement_vs_first_bps"
                            ].mean()
                        ),
                        "median_net_improvement_vs_first_bps": float(
                            comparison[
                                "net_improvement_vs_first_bps"
                            ].median()
                        ),
                        "share_net_improvement_positive": float(
                            comparison[
                                "net_improvement_vs_first_bps"
                            ].gt(0).mean()
                        ),
                    }
                )


entry_rule_summary = pd.DataFrame(
    entry_summary_rows
)

entry_rule_signals = pd.concat(
    all_signal_tables,
    ignore_index=True,
)

timing_comparison = pd.DataFrame(
    timing_comparison_rows
)


# =============================================================================
# Primary medium-model robustness
# =============================================================================

print("[INFO] Running primary medium-model robustness audit...")

primary_medium_signals = entry_rule_signals[
    entry_rule_signals[
        "move_feature_set"
    ].eq("medium_trade_flow")
    & entry_rule_signals[
        "threshold_mode"
    ].eq("model_specific")
    & entry_rule_signals[
        "split_name"
    ].eq("test")
    & entry_rule_signals[
        "entry_rule"
    ].eq("first_eligible")
].copy()

medium_robustness_summary = pd.DataFrame(
    [
        {
            "move_feature_set": (
                "medium_trade_flow"
            ),
            "threshold_mode": (
                "model_specific"
            ),
            "entry_rule": (
                "first_eligible"
            ),
            **calculate_robustness_summary(
                primary_medium_signals
            ),
        }
    ]
)


medium_per_direction = (
    primary_medium_signals
    .assign(
        direction_label=np.where(
            primary_medium_signals[
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
        n_signals=(
            "net_return_bps_1bps_cost",
            "size",
        ),
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


medium_per_run = (
    primary_medium_signals
    .groupby(
        "run_name",
        as_index=False,
    )
    .agg(
        n_signals=(
            "net_return_bps_1bps_cost",
            "size",
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
    .sort_values(
        "total_net_return_bps",
        ascending=False,
    )
)


# =============================================================================
# Save tables
# =============================================================================

direction_sanity.to_csv(
    TABLES_DIR
    / "day37_direction_sanity_metrics.csv",
    index=False,
)

move_metrics.to_csv(
    TABLES_DIR
    / "day37_move_model_metrics.csv",
    index=False,
)

entry_rule_summary.to_csv(
    TABLES_DIR
    / "day37_entry_rule_summary.csv",
    index=False,
)

entry_rule_signals.to_csv(
    TABLES_DIR
    / "day37_entry_rule_signals.csv",
    index=False,
)

timing_comparison.to_csv(
    TABLES_DIR
    / "day37_entry_timing_comparison.csv",
    index=False,
)

medium_robustness_summary.to_csv(
    TABLES_DIR
    / "day37_medium_robustness_summary.csv",
    index=False,
)

medium_per_direction.to_csv(
    TABLES_DIR
    / "day37_medium_per_direction.csv",
    index=False,
)

medium_per_run.to_csv(
    TABLES_DIR
    / "day37_medium_per_run.csv",
    index=False,
)


# =============================================================================
# Create figures
# =============================================================================

test_entry_plot = entry_rule_summary[
    entry_rule_summary["split"].eq("test")
    & entry_rule_summary[
        "threshold_mode"
    ].eq("model_specific")
].copy()

test_entry_plot["label"] = (
    test_entry_plot["move_feature_set"]
    + "\n"
    + test_entry_plot["entry_rule"]
)

plt.figure(figsize=(13, 6))
plt.bar(
    test_entry_plot["label"],
    test_entry_plot["mean_net_return_bps"],
)
plt.axhline(0.0, linewidth=1)
plt.ylabel("Thursday mean net return, bps")
plt.title(
    "Day 37: entry-selection rule comparison"
)
plt.xticks(rotation=35, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day37_entry_rule_net_comparison.png",
    dpi=150,
    bbox_inches="tight",
)
plt.close()


plt.figure(figsize=(11, 6))
plt.bar(
    medium_per_run["run_name"],
    medium_per_run["total_net_return_bps"],
)
plt.axhline(0.0, linewidth=1)
plt.ylabel("Total net return in run, bps")
plt.title(
    "Day 37: primary medium-model net return by run"
)
plt.xticks(rotation=70, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day37_medium_net_by_run.png",
    dpi=150,
    bbox_inches="tight",
)
plt.close()


# =============================================================================
# Console output
# =============================================================================

print()
print("=" * 80)
print("[INFO] Direction sanity check")
print("=" * 80)
print(
    direction_sanity[
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
print("[INFO] First-eligible: model-specific thresholds")
print("=" * 80)
print(
    entry_rule_summary[
        entry_rule_summary[
            "threshold_mode"
        ].eq("model_specific")
        & entry_rule_summary[
            "entry_rule"
        ].eq("first_eligible")
    ][
        [
            "move_feature_set",
            "split",
            "move_threshold",
            "direction_threshold",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_net_return_bps",
            "median_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Thursday entry-rule comparison")
print("=" * 80)
print(
    entry_rule_summary[
        entry_rule_summary["split"].eq("test")
        & entry_rule_summary[
            "threshold_mode"
        ].eq("model_specific")
    ][
        [
            "move_feature_set",
            "entry_rule",
            "entry_rule_is_live_deployable",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_net_return_bps",
            "median_net_return_bps",
            "mean_entry_delay_events",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Fixed common thresholds, first eligible")
print("=" * 80)
print(
    entry_rule_summary[
        entry_rule_summary["split"].eq("test")
        & entry_rule_summary[
            "threshold_mode"
        ].eq(
            "fixed_common_0p65_0p65"
        )
        & entry_rule_summary[
            "entry_rule"
        ].eq("first_eligible")
    ][
        [
            "move_feature_set",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_net_return_bps",
            "median_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 80)
print("[INFO] Primary medium robustness")
print("=" * 80)
print(
    medium_robustness_summary.to_string(
        index=False
    )
)

print()
print("=" * 80)
print("[INFO] Primary medium by direction")
print("=" * 80)
print(
    medium_per_direction.to_string(
        index=False
    )
)

print()
print(
    "[INFO] Day 37 entry-selection and "
    "medium robustness audit completed successfully."
)