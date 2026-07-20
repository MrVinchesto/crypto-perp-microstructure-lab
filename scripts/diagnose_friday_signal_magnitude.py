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


# Day 41 is diagnostic only.
# Do not use these results to choose a new Friday threshold.

DATA_PATH = Path("data/processed/trade_flow_features.csv")
LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")
QUALITY_PATH = Path("reports/tables/day40_run_quality_audit.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")
REPORT_PATH = Path("reports/day41_friday_diagnostic_summary.md")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_BATCHES = [
    "weekday_active_tue_day28",
    "weekday_active_wed_day29",
]
TEST_BATCH = "weekday_active_fri_day39"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
COST_BPS = 1.0
COOLDOWN = 50

MOVE_C = 0.03
DIRECTION_C = 1.0

PRIMARY_MOVE_THRESHOLD = 0.75
PRIMARY_DIRECTION_THRESHOLD = 0.65
SENSITIVITY_MOVE_THRESHOLD = 0.65
SENSITIVITY_DIRECTION_THRESHOLD = 0.65

BOOK_FEATURES = [
    "spread_bps",
    "event_gap_ms",
    "best_bid_qty",
    "best_ask_qty",
    "imbalance_1",
    "imbalance_5",
    "microprice_deviation_bps",
    "quote_changed",
]

MOVE_FEATURES = [
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


def parse_bool(series):
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)

    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin(["true", "1", "yes"])
    )


def make_model(c_value):
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    C=c_value,
                    class_weight="balanced",
                    solver="liblinear",
                    max_iter=2000,
                    random_state=42,
                ),
            ),
        ]
    )


def safe_auc(y_true, probability):
    if pd.Series(y_true).nunique() < 2:
        return np.nan

    return roc_auc_score(y_true, probability)


def metrics(y_true, probability):
    prediction = (probability >= 0.50).astype(int)

    return {
        "n_observations": len(y_true),
        "positive_class_share": np.mean(y_true),
        "balanced_accuracy": balanced_accuracy_score(
            y_true,
            prediction,
        ),
        "precision_positive": precision_score(
            y_true,
            prediction,
            zero_division=0,
        ),
        "recall_positive": recall_score(
            y_true,
            prediction,
            zero_division=0,
        ),
        "roc_auc": safe_auc(
            y_true,
            probability,
        ),
        "average_precision": average_precision_score(
            y_true,
            probability,
        ),
        "brier_score": brier_score_loss(
            y_true,
            probability,
        ),
    }


def select_first_eligible(
    frame,
    move_threshold,
    direction_threshold,
    specification,
):
    candidates = frame[
        [
            "run_name",
            "row_in_run",
            "time_segment",
            "future_return_bps",
            "future_abs_return_bps",
            "move_probability",
            "direction_probability_up",
            "direction_confidence",
        ]
    ].copy()

    is_long = (
        candidates["direction_probability_up"]
        >= direction_threshold
    )
    is_short = (
        candidates["direction_probability_up"]
        <= 1.0 - direction_threshold
    )
    is_move = (
        candidates["move_probability"]
        >= move_threshold
    )

    candidates["signal_direction"] = np.where(
        is_move & is_long,
        1,
        np.where(
            is_move & is_short,
            -1,
            0,
        ),
    )

    candidates = candidates[
        candidates["signal_direction"] != 0
    ].copy()

    selected = []

    for _, run_frame in candidates.groupby(
        "run_name",
        sort=False,
    ):
        run_frame = run_frame.sort_values(
            "row_in_run"
        )
        last_row = None

        for index, row in run_frame.iterrows():
            current_row = int(row["row_in_run"])

            if (
                last_row is None
                or current_row > last_row + COOLDOWN
            ):
                selected.append(index)
                last_row = current_row

    signals = candidates.loc[selected].copy()

    signals["signed_return_bps"] = (
        signals["signal_direction"]
        * signals["future_return_bps"]
    )
    signals["net_return_bps"] = (
        signals["signed_return_bps"]
        - COST_BPS
    )
    signals["direction_correct"] = (
        signals["signed_return_bps"] > 0
    )
    signals["net_profitable"] = (
        signals["net_return_bps"] > 0
    )

    signals["outcome_bucket"] = np.select(
        [
            signals["signed_return_bps"] <= 0,
            (
                signals["signed_return_bps"] > 0
            )
            & (
                signals["signed_return_bps"] <= COST_BPS
            ),
            signals["signed_return_bps"] > COST_BPS,
        ],
        [
            "wrong_direction",
            "correct_direction_below_cost",
            "profitable_after_cost",
        ],
        default="unclassified",
    )
    
    signals["specification"] = specification

    return signals


def signal_summary(
    signals,
    n_rows,
    n_runs,
):
    if signals.empty:
        return {
            "n_signals": 0,
            "n_signal_runs": 0,
            "signal_rate_per_row": 0.0,
            "signal_run_coverage": 0.0,
            "directional_precision": np.nan,
            "positive_net_signal_share": np.nan,
            "mean_signed_return_bps": np.nan,
            "mean_net_return_bps": np.nan,
            "median_net_return_bps": np.nan,
        }

    return {
        "n_signals": len(signals),
        "n_signal_runs": signals[
            "run_name"
        ].nunique(),
        "signal_rate_per_row": len(signals) / n_rows,
        "signal_run_coverage": (
            signals["run_name"].nunique()
            / n_runs
        ),
        "directional_precision": signals[
            "direction_correct"
        ].mean(),
        "positive_net_signal_share": signals[
            "net_profitable"
        ].mean(),
        "mean_signed_return_bps": signals[
            "signed_return_bps"
        ].mean(),
        "mean_net_return_bps": signals[
            "net_return_bps"
        ].mean(),
        "median_net_return_bps": signals[
            "net_return_bps"
        ].median(),
    }


quality = pd.read_csv(QUALITY_PATH)
quality["run_name"] = quality["run_name"].astype(str)
quality["strict_quality_ok"] = parse_bool(
    quality["strict_quality_ok"]
)

train_runs = set(
    quality.loc[
        quality["collection_batch"].isin(
            TRAIN_BATCHES
        )
        & quality["strict_quality_ok"],
        "run_name",
    ]
)

test_runs = set(
    quality.loc[
        quality["collection_batch"].eq(
            TEST_BATCH
        )
        & quality["strict_quality_ok"],
        "run_name",
    ]
)

log = pd.read_csv(LOG_PATH)
log["run_name"] = log["run_name"].astype(str)

friday_log = log[
    log["run_name"].isin(test_runs)
].copy()

friday_log["started_at_utc"] = pd.to_datetime(
    friday_log["started_at_utc"],
    utc=True,
    errors="coerce",
)
friday_log = friday_log.sort_values(
    ["started_at_utc", "run_name"]
)

segment_names = [
    "early_first_12_runs",
    "middle_second_12_runs",
    "late_final_12_runs",
]

run_to_segment = {}

for segment, run_group in zip(
    segment_names,
    np.array_split(
        friday_log["run_name"].tolist(),
        3,
    ),
):
    for run_name in run_group:
        run_to_segment[str(run_name)] = segment


required_columns = (
    ["run_name", "row_in_run", "mid_price"]
    + BOOK_FEATURES
    + MOVE_FEATURES
)

available_columns = pd.read_csv(
    DATA_PATH,
    nrows=0,
).columns.tolist()

missing_columns = [
    column
    for column in required_columns
    if column not in available_columns
]

if missing_columns:
    raise ValueError(
        f"Missing columns: {missing_columns}"
    )

data = pd.read_csv(
    DATA_PATH,
    usecols=required_columns,
)

data["run_name"] = data["run_name"].astype(str)

data = data[
    data["run_name"].isin(
        train_runs | test_runs
    )
].copy()

data = data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)

data[
    BOOK_FEATURES + MOVE_FEATURES
] = data[
    BOOK_FEATURES + MOVE_FEATURES
].replace(
    [np.inf, -np.inf],
    np.nan,
)

data["future_mid_price"] = (
    data.groupby(
        "run_name",
        sort=False,
    )["mid_price"]
    .shift(-HORIZON)
)

data["future_return_bps"] = (
    data["future_mid_price"]
    / data["mid_price"]
    - 1.0
) * 10000.0

data["future_abs_return_bps"] = (
    data["future_return_bps"].abs()
)

data["move_target"] = np.where(
    data["future_return_bps"].notna(),
    (
        data["future_abs_return_bps"]
        > DEAD_ZONE_BPS
    ).astype(float),
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

train = data[
    data["run_name"].isin(train_runs)
    & data["future_return_bps"].notna()
].copy()

test = data[
    data["run_name"].isin(test_runs)
    & data["future_return_bps"].notna()
].copy()

test["time_segment"] = (
    test["run_name"].map(run_to_segment)
)

train_direction = train[
    train["direction_target"].notna()
].copy()

move_model = make_model(MOVE_C)
move_model.fit(
    train[MOVE_FEATURES],
    train["move_target"].astype(int),
)

direction_model = make_model(DIRECTION_C)
direction_model.fit(
    train_direction[BOOK_FEATURES],
    train_direction[
        "direction_target"
    ].astype(int),
)

test["move_probability"] = (
    move_model.predict_proba(
        test[MOVE_FEATURES]
    )[:, 1]
)

test["direction_probability_up"] = (
    direction_model.predict_proba(
        test[BOOK_FEATURES]
    )[:, 1]
)

test["direction_confidence"] = np.maximum(
    test["direction_probability_up"],
    1.0 - test["direction_probability_up"],
)

test["predicted_direction"] = np.where(
    test["direction_probability_up"] >= 0.50,
    1,
    -1,
)

test["hypothetical_signed_return_bps"] = (
    test["predicted_direction"]
    * test["future_return_bps"]
)

test["direction_correct"] = (
    test["hypothetical_signed_return_bps"] > 0
)


# 1. MOVE calibration and magnitude

move_bins = [
    0.00,
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    1.000001,
]

test["move_probability_bin"] = pd.cut(
    test["move_probability"],
    bins=move_bins,
    right=False,
    include_lowest=True,
)

move_summary = (
    test.groupby(
        "move_probability_bin",
        observed=False,
    )
    .agg(
        n_rows=("run_name", "size"),
        n_runs=("run_name", "nunique"),
        mean_move_probability=(
            "move_probability",
            "mean",
        ),
        realized_move_rate_gt_1bps=(
            "move_target",
            "mean",
        ),
        mean_abs_return_bps=(
            "future_abs_return_bps",
            "mean",
        ),
        median_abs_return_bps=(
            "future_abs_return_bps",
            "median",
        ),
        p90_abs_return_bps=(
            "future_abs_return_bps",
            lambda values: values.quantile(0.90),
        ),
        share_abs_return_gt_1p5bps=(
            "future_abs_return_bps",
            lambda values: (
                values > 1.5
            ).mean(),
        ),
        share_abs_return_gt_2bps=(
            "future_abs_return_bps",
            lambda values: (
                values > 2.0
            ).mean(),
        ),
    )
    .reset_index()
)

move_summary["calibration_gap"] = (
    move_summary[
        "realized_move_rate_gt_1bps"
    ]
    - move_summary[
        "mean_move_probability"
    ]
)

move_summary["move_probability_bin"] = (
    move_summary[
        "move_probability_bin"
    ].astype(str)
)

move_summary.to_csv(
    TABLES_DIR
    / "day41_move_probability_bins.csv",
    index=False,
)


# 2. Direction confidence

direction_bins = [
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    1.000001,
]

direction_outputs = []

for subset_name, subset in [
    ("all_rows", test),
    (
        "realized_move_gt_1bps",
        test[test["move_target"] == 1],
    ),
]:
    subset = subset.copy()

    subset["direction_confidence_bin"] = pd.cut(
        subset["direction_confidence"],
        bins=direction_bins,
        right=False,
        include_lowest=True,
    )

    summary = (
        subset.groupby(
            "direction_confidence_bin",
            observed=False,
        )
        .agg(
            n_rows=("run_name", "size"),
            n_runs=("run_name", "nunique"),
            mean_direction_confidence=(
                "direction_confidence",
                "mean",
            ),
            directional_precision=(
                "direction_correct",
                "mean",
            ),
            mean_signed_return_bps=(
                "hypothetical_signed_return_bps",
                "mean",
            ),
            mean_abs_return_bps=(
                "future_abs_return_bps",
                "mean",
            ),
        )
        .reset_index()
    )

    summary["subset"] = subset_name
    direction_outputs.append(summary)

direction_summary = pd.concat(
    direction_outputs,
    ignore_index=True,
)

direction_summary[
    "direction_confidence_bin"
] = direction_summary[
    "direction_confidence_bin"
].astype(str)

direction_summary.to_csv(
    TABLES_DIR
    / "day41_direction_confidence_bins.csv",
    index=False,
)


# 3. Joint coarse grid

test["move_tier"] = pd.cut(
    test["move_probability"],
    bins=[-np.inf, 0.65, 0.75, np.inf],
    labels=[
        "move_below_0p65",
        "move_0p65_to_0p75",
        "move_at_least_0p75",
    ],
    right=False,
)

test["direction_tier"] = pd.cut(
    test["direction_confidence"],
    bins=[-np.inf, 0.65, 0.70, np.inf],
    labels=[
        "direction_below_0p65",
        "direction_0p65_to_0p70",
        "direction_at_least_0p70",
    ],
    right=False,
)

joint_summary = (
    test.groupby(
        ["move_tier", "direction_tier"],
        observed=False,
    )
    .agg(
        n_rows=("run_name", "size"),
        n_runs=("run_name", "nunique"),
        mean_move_probability=(
            "move_probability",
            "mean",
        ),
        mean_direction_confidence=(
            "direction_confidence",
            "mean",
        ),
        realized_move_rate_gt_1bps=(
            "move_target",
            "mean",
        ),
        directional_precision=(
            "direction_correct",
            "mean",
        ),
        mean_abs_return_bps=(
            "future_abs_return_bps",
            "mean",
        ),
        mean_signed_return_bps=(
            "hypothetical_signed_return_bps",
            "mean",
        ),
    )
    .reset_index()
)

joint_summary["mean_net_after_1bps"] = (
    joint_summary["mean_signed_return_bps"]
    - COST_BPS
)

joint_summary.to_csv(
    TABLES_DIR
    / "day41_joint_confidence_grid.csv",
    index=False,
)


# 4. Frozen signal decomposition

primary = select_first_eligible(
    test,
    PRIMARY_MOVE_THRESHOLD,
    PRIMARY_DIRECTION_THRESHOLD,
    "primary_0p75_0p65",
)

sensitivity = select_first_eligible(
    test,
    SENSITIVITY_MOVE_THRESHOLD,
    SENSITIVITY_DIRECTION_THRESHOLD,
    "sensitivity_0p65_0p65",
)

all_signals = pd.concat(
    [primary, sensitivity],
    ignore_index=True,
)

outcome_summary = (
    all_signals.groupby(
        ["specification", "outcome_bucket"],
        as_index=False,
    )
    .agg(
        n_signals=("run_name", "size"),
        n_runs=("run_name", "nunique"),
        mean_move_probability=(
            "move_probability",
            "mean",
        ),
        mean_direction_confidence=(
            "direction_confidence",
            "mean",
        ),
        mean_abs_return_bps=(
            "future_abs_return_bps",
            "mean",
        ),
        mean_signed_return_bps=(
            "signed_return_bps",
            "mean",
        ),
        mean_net_return_bps=(
            "net_return_bps",
            "mean",
        ),
        total_net_return_bps=(
            "net_return_bps",
            "sum",
        ),
    )
)

specification_counts = (
    all_signals.groupby(
        "specification"
    ).size()
)

outcome_summary[
    "share_within_specification"
] = (
    outcome_summary["n_signals"]
    / outcome_summary[
        "specification"
    ].map(specification_counts)
)

outcome_summary.to_csv(
    TABLES_DIR
    / "day41_signal_outcome_decomposition.csv",
    index=False,
)


# 5. Early / middle / late diagnostics

segment_classifier_rows = []
segment_deployment_rows = []

for segment in segment_names:
    segment_data = test[
        test["time_segment"] == segment
    ]

    segment_direction = segment_data[
        segment_data["direction_target"].notna()
    ]

    segment_classifier_rows.append(
        {
            "time_segment": segment,
            "model_stage": "move",
            **metrics(
                segment_data[
                    "move_target"
                ].astype(int),
                segment_data[
                    "move_probability"
                ].to_numpy(),
            ),
        }
    )

    segment_classifier_rows.append(
        {
            "time_segment": segment,
            "model_stage": "direction",
            **metrics(
                segment_direction[
                    "direction_target"
                ].astype(int),
                segment_direction[
                    "direction_probability_up"
                ].to_numpy(),
            ),
        }
    )

    for specification, signal_frame in [
        ("primary_0p75_0p65", primary),
        ("sensitivity_0p65_0p65", sensitivity),
    ]:
        selected = signal_frame[
            signal_frame["time_segment"]
            == segment
        ]

        segment_deployment_rows.append(
            {
                "time_segment": segment,
                "specification": specification,
                **signal_summary(
                    selected,
                    len(segment_data),
                    segment_data[
                        "run_name"
                    ].nunique(),
                ),
            }
        )

segment_classifier = pd.DataFrame(
    segment_classifier_rows
)
segment_deployment = pd.DataFrame(
    segment_deployment_rows
)

segment_classifier.to_csv(
    TABLES_DIR
    / "day41_time_segment_classifier_metrics.csv",
    index=False,
)

segment_deployment.to_csv(
    TABLES_DIR
    / "day41_time_segment_deployment.csv",
    index=False,
)


# 6. Primary/sensitivity overlap

primary_keys = primary[
    ["run_name", "row_in_run"]
].drop_duplicates()

sensitivity_keys = sensitivity[
    ["run_name", "row_in_run"]
].drop_duplicates()

overlap = primary_keys.merge(
    sensitivity_keys,
    on=["run_name", "row_in_run"],
    how="inner",
)

overlap_summary = pd.DataFrame(
    [
        {
            "primary_signals": len(primary_keys),
            "sensitivity_signals": len(
                sensitivity_keys
            ),
            "exact_signal_overlap": len(overlap),
            "primary_overlap_share": (
                len(overlap) / len(primary_keys)
                if len(primary_keys)
                else np.nan
            ),
            "sensitivity_overlap_share": (
                len(overlap)
                / len(sensitivity_keys)
                if len(sensitivity_keys)
                else np.nan
            ),
        }
    ]
)

overlap_summary.to_csv(
    TABLES_DIR
    / "day41_primary_sensitivity_overlap.csv",
    index=False,
)


# 7. Figures

move_plot = move_summary[
    move_summary["n_rows"] > 0
].copy()

plt.figure(figsize=(10, 6))
plt.plot(
    move_plot["move_probability_bin"],
    move_plot["mean_move_probability"],
    marker="o",
    label="Mean predicted probability",
)
plt.plot(
    move_plot["move_probability_bin"],
    move_plot[
        "realized_move_rate_gt_1bps"
    ],
    marker="o",
    label="Realized move rate",
)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Probability / realized rate")
plt.xlabel("MOVE probability bin")
plt.title("Friday MOVE calibration")
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day41_move_probability_calibration.png",
    dpi=150,
)
plt.close()

plt.figure(figsize=(10, 6))
plt.plot(
    move_plot["move_probability_bin"],
    move_plot["mean_abs_return_bps"],
    marker="o",
)
plt.axhline(
    COST_BPS,
    linewidth=1,
    label="1 bps cost hurdle",
)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Mean absolute h50 return, bps")
plt.xlabel("MOVE probability bin")
plt.title("Friday move magnitude by MOVE probability")
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day41_move_probability_vs_magnitude.png",
    dpi=150,
)
plt.close()

direction_plot = direction_summary[
    (
        direction_summary["subset"]
        == "realized_move_gt_1bps"
    )
    & (
        direction_summary["n_rows"] > 0
    )
]

plt.figure(figsize=(10, 6))
plt.plot(
    direction_plot[
        "direction_confidence_bin"
    ],
    direction_plot[
        "directional_precision"
    ],
    marker="o",
)
plt.axhline(0.50, linewidth=1)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Directional precision")
plt.xlabel("Direction confidence bin")
plt.title(
    "Friday direction precision on realized >1 bps moves"
)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day41_direction_confidence_precision.png",
    dpi=150,
)
plt.close()

segment_plot = segment_deployment.pivot(
    index="time_segment",
    columns="specification",
    values="mean_net_return_bps",
).reindex(segment_names)

plt.figure(figsize=(10, 6))
segment_plot.plot(
    kind="bar",
    ax=plt.gca(),
)
plt.axhline(0.0, linewidth=1)
plt.ylabel("Mean net return after 1 bps")
plt.xlabel("Fixed Friday segment")
plt.title("Frozen results by Friday time segment")
plt.xticks(rotation=20, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(
    FIGURES_DIR
    / "day41_time_segment_net_returns.png",
    dpi=150,
)
plt.close()


primary_result = signal_summary(
    primary,
    len(test),
    test["run_name"].nunique(),
)

sensitivity_result = signal_summary(
    sensitivity,
    len(test),
    test["run_name"].nunique(),
)

report = f"""# Day 41 Friday Diagnostic Summary

This is a post-holdout diagnostic analysis. It is not a new confirmatory test
and must not be used to select a new Friday threshold.

## Frozen primary reproduced

- Signals: {primary_result["n_signals"]}
- Signal-bearing runs: {primary_result["n_signal_runs"]}
- Run coverage: {primary_result["signal_run_coverage"]:.3f}
- Directional precision: {primary_result["directional_precision"]:.3f}
- Positive-net signal share: {primary_result["positive_net_signal_share"]:.3f}
- Mean signed return: {primary_result["mean_signed_return_bps"]:.6f} bps
- Mean net return: {primary_result["mean_net_return_bps"]:.6f} bps

## Frozen sensitivity reproduced

- Signals: {sensitivity_result["n_signals"]}
- Signal-bearing runs: {sensitivity_result["n_signal_runs"]}
- Run coverage: {sensitivity_result["signal_run_coverage"]:.3f}
- Directional precision: {sensitivity_result["directional_precision"]:.3f}
- Positive-net signal share: {sensitivity_result["positive_net_signal_share"]:.3f}
- Mean signed return: {sensitivity_result["mean_signed_return_bps"]:.6f} bps
- Mean net return: {sensitivity_result["mean_net_return_bps"]:.6f} bps

Any new model motivated by these diagnostics must be frozen before another
independent holdout.
"""

REPORT_PATH.write_text(
    report,
    encoding="utf-8",
)

print()
print("DAY 41 MOVE BINS")
print(move_summary.to_string(index=False))

print()
print("DAY 41 SIGNAL OUTCOME DECOMPOSITION")
print(outcome_summary.to_string(index=False))

print()
print("DAY 41 TIME SEGMENTS")
print(segment_deployment.to_string(index=False))

print()
print("DAY 41 OVERLAP")
print(overlap_summary.to_string(index=False))

print()
print("[INFO] Day 41 diagnostics completed.")