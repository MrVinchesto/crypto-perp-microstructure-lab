from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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
from sklearn.utils.class_weight import compute_sample_weight


warnings.filterwarnings("ignore", category=UserWarning)


FEATURES_PATH = Path("data/processed/trade_flow_features.csv")
COLLECTION_LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

COLLECTION_BATCH = "fresh_trades_large_day25"

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5

HORIZONS = [10, 20, 50]

TRAIN_RUNS = 14
VALIDATION_RUNS = 5
TEST_RUNS = 5

THRESHOLD_GRID = [
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

MIN_VALIDATION_COOLDOWN_TRADES = 10

COST_SCENARIOS = [
    {"cost_scenario": "no_cost", "round_trip_cost_bps": 0.0},
    {"cost_scenario": "very_low_cost", "round_trip_cost_bps": 1.0},
    {"cost_scenario": "low_cost", "round_trip_cost_bps": 2.0},
    {"cost_scenario": "medium_cost", "round_trip_cost_bps": 3.0},
    {"cost_scenario": "high_cost", "round_trip_cost_bps": 4.0},
    {"cost_scenario": "expensive_taker_like", "round_trip_cost_bps": 5.0},
    {"cost_scenario": "very_expensive_round_trip", "round_trip_cost_bps": 9.0},
]


BOOK_ONLY_FEATURES = [
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


TRADE_FLOW_ONLY_FEATURES = [
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
    "trade_imbalance",
    "notional_imbalance",
    "trade_intensity_per_second",

    "trade_count_rolling_sum_5e",
    "trade_count_rolling_sum_10e",
    "trade_count_rolling_sum_20e",
    "trade_count_rolling_sum_50e",

    "trade_volume_rolling_sum_5e",
    "trade_volume_rolling_sum_10e",
    "trade_volume_rolling_sum_20e",
    "trade_volume_rolling_sum_50e",

    "signed_trade_volume_rolling_sum_5e",
    "signed_trade_volume_rolling_sum_10e",
    "signed_trade_volume_rolling_sum_20e",
    "signed_trade_volume_rolling_sum_50e",

    "trade_notional_rolling_sum_5e",
    "trade_notional_rolling_sum_10e",
    "trade_notional_rolling_sum_20e",
    "trade_notional_rolling_sum_50e",

    "signed_trade_notional_rolling_sum_5e",
    "signed_trade_notional_rolling_sum_10e",
    "signed_trade_notional_rolling_sum_20e",
    "signed_trade_notional_rolling_sum_50e",

    "trade_imbalance_rolling_5e",
    "trade_imbalance_rolling_10e",
    "trade_imbalance_rolling_20e",
    "trade_imbalance_rolling_50e",

    "notional_imbalance_rolling_5e",
    "notional_imbalance_rolling_10e",
    "notional_imbalance_rolling_20e",
    "notional_imbalance_rolling_50e",

    "trade_intensity_rolling_mean_5e",
    "trade_intensity_rolling_mean_10e",
    "trade_intensity_rolling_mean_20e",
    "trade_intensity_rolling_mean_50e",
]


FEATURE_SETS = {
    "book_only": BOOK_ONLY_FEATURES,
    "trade_flow_only": TRADE_FLOW_ONLY_FEATURES,
    "combined": BOOK_ONLY_FEATURES + TRADE_FLOW_ONLY_FEATURES,
}


SPLIT_INFO_PATH = TABLES_DIR / "trade_flow_validation_split_info.csv"
MODEL_METRICS_PATH = TABLES_DIR / "trade_flow_model_metrics.csv"
VALIDATION_THRESHOLD_GRID_PATH = TABLES_DIR / "trade_flow_validation_threshold_grid.csv"
SELECTED_THRESHOLDS_PATH = TABLES_DIR / "trade_flow_selected_thresholds.csv"
TEST_RAW_SIGNALS_PATH = TABLES_DIR / "trade_flow_test_raw_signals.csv"
TEST_COOLDOWN_SIGNALS_PATH = TABLES_DIR / "trade_flow_test_cooldown_signals.csv"
TEST_SUMMARY_PATH = TABLES_DIR / "trade_flow_test_summary.csv"
TEST_PER_RUN_PATH = TABLES_DIR / "trade_flow_test_per_run.csv"
TEST_DIRECTION_PATH = TABLES_DIR / "trade_flow_test_direction.csv"
COST_SANITY_PATH = TABLES_DIR / "trade_flow_cost_sanity.csv"
COEFFICIENTS_PATH = TABLES_DIR / "trade_flow_logit_coefficients.csv"


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_large_trade_runs() -> list[str]:
    if not COLLECTION_LOG_PATH.exists():
        raise FileNotFoundError(f"Collection log not found: {COLLECTION_LOG_PATH}")

    log = pd.read_csv(COLLECTION_LOG_PATH)

    required_cols = [
        "collection_batch",
        "collection_seconds_requested",
        "status",
        "run_number",
        "run_name",
    ]

    missing = [col for col in required_cols if col not in log.columns]

    if missing:
        raise ValueError(f"fresh_trade_collection_log.csv missing columns: {missing}")

    large = log[
        (log["collection_batch"] == COLLECTION_BATCH) &
        (log["collection_seconds_requested"] == 300) &
        (log["status"] == "success")
    ].copy()

    large = large.sort_values("run_number")

    runs = large["run_name"].dropna().astype(str).tolist()

    if len(runs) < TRAIN_RUNS + VALIDATION_RUNS + TEST_RUNS:
        raise ValueError(
            f"Not enough runs. Need at least {TRAIN_RUNS + VALIDATION_RUNS + TEST_RUNS}, "
            f"found {len(runs)}."
        )

    return runs


def load_features(runs: list[str]) -> pd.DataFrame:
    if not FEATURES_PATH.exists():
        raise FileNotFoundError(f"Features file not found: {FEATURES_PATH}")

    df = pd.read_csv(FEATURES_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"trade_flow_features.csv missing columns: {missing}")

    df = df[df["run_name"].isin(runs)].copy()

    run_order = {run_name: idx for idx, run_name in enumerate(runs)}
    df["run_order"] = df["run_name"].map(run_order)

    df = df.sort_values(["run_order", "row_in_run", "event_time"]).reset_index(drop=True)

    if df.empty:
        raise ValueError("Filtered feature dataset is empty.")

    return df


def label_from_tick_change(tick_change: float) -> str:
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > DEAD_ZONE_TICKS:
        return "up"

    if tick_change < -DEAD_ZONE_TICKS:
        return "down"

    return "flat"


def add_labels(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    frames = []

    for run_name, group in df.groupby("run_name", sort=False):
        temp = group.copy()
        temp = temp.sort_values("row_in_run").reset_index(drop=True)

        temp["target_horizon"] = horizon
        temp["future_mid_price"] = temp["mid_price"].shift(-horizon)
        temp["future_mid_change"] = temp["future_mid_price"] - temp["mid_price"]
        temp["future_mid_change_ticks"] = temp["future_mid_change"] / PRICE_TICK
        temp["future_mid_return"] = temp["future_mid_price"] / temp["mid_price"] - 1.0
        temp["future_mid_return_bps"] = temp["future_mid_return"] * 10000.0

        temp["target_label"] = temp["future_mid_change_ticks"].apply(label_from_tick_change)

        frames.append(temp)

    labeled = pd.concat(frames, ignore_index=True)

    labeled = labeled[labeled["target_label"].isin(["down", "up"])].copy()
    labeled["target"] = labeled["target_label"].map({"down": 0, "up": 1}).astype(int)

    return labeled


def make_split_info(runs: list[str]) -> pd.DataFrame:
    train_runs = runs[:TRAIN_RUNS]
    validation_runs = runs[TRAIN_RUNS:TRAIN_RUNS + VALIDATION_RUNS]
    test_runs = runs[TRAIN_RUNS + VALIDATION_RUNS:TRAIN_RUNS + VALIDATION_RUNS + TEST_RUNS]

    rows = []

    for run_name in train_runs:
        rows.append({"split": "train", "run_name": run_name})

    for run_name in validation_runs:
        rows.append({"split": "validation", "run_name": run_name})

    for run_name in test_runs:
        rows.append({"split": "test", "run_name": run_name})

    return pd.DataFrame(rows)


def build_model() -> Pipeline:
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "logistic_regression",
                LogisticRegression(
                    solver="liblinear",
                    random_state=42,
                ),
            ),
        ]
    )


def ordinary_model_metrics(
    model_name: str,
    feature_set: str,
    horizon: int,
    split_name: str,
    y_true: pd.Series,
    p_up: np.ndarray,
) -> dict:
    y_pred = (p_up >= 0.5).astype(int)

    if len(np.unique(y_true)) > 1:
        roc_auc = roc_auc_score(y_true, p_up)
    else:
        roc_auc = np.nan

    return {
        "model": model_name,
        "feature_set": feature_set,
        "horizon": horizon,
        "split": split_name,
        "n": len(y_true),
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "precision_up": precision_score(y_true, y_pred, zero_division=0),
        "recall_up": recall_score(y_true, y_pred, zero_division=0),
        "roc_auc": roc_auc,
        "up_share_true": y_true.mean(),
        "up_share_predicted": y_pred.mean(),
    }


def assign_signal(p_up: float, threshold: float) -> str:
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def make_selected_signals(
    scored: pd.DataFrame,
    model_name: str,
    feature_set: str,
    horizon: int,
    threshold: float,
    evaluation_method: str,
) -> pd.DataFrame:
    temp = scored.copy()
    temp["signal"] = temp["predicted_proba_up"].apply(
        lambda p: assign_signal(p, threshold)
    )

    selected = temp[temp["signal"].isin(["up", "down"])].copy()

    if selected.empty:
        return selected

    selected["model"] = model_name
    selected["feature_set"] = feature_set
    selected["horizon"] = horizon
    selected["threshold"] = threshold
    selected["evaluation_method"] = evaluation_method

    selected["is_correct_signal"] = (
        ((selected["signal"] == "up") & (selected["target_label"] == "up")) |
        ((selected["signal"] == "down") & (selected["target_label"] == "down"))
    )

    selected["signed_return_bps"] = np.where(
        selected["signal"] == "up",
        selected["future_mid_return_bps"],
        -selected["future_mid_return_bps"],
    )

    selected["is_positive_signed_return"] = selected["signed_return_bps"] > 0

    keep_cols = [
        "evaluation_method",
        "model",
        "feature_set",
        "horizon",
        "threshold",
        "run_name",
        "row_in_run",
        "event_time",
        "target_label",
        "signal",
        "predicted_proba_up",
        "future_mid_return_bps",
        "signed_return_bps",
        "is_correct_signal",
        "is_positive_signed_return",
    ]

    return selected[keep_cols].copy()


def apply_cooldown(raw_signals: pd.DataFrame, horizon: int) -> pd.DataFrame:
    rows = []

    if raw_signals.empty:
        return raw_signals.copy()

    cooldown_events = horizon

    for (model_name, feature_set, run_name), group in raw_signals.groupby(
        ["model", "feature_set", "run_name"],
        sort=True,
    ):
        group = group.sort_values("row_in_run").reset_index(drop=True)

        last_selected_row = None
        cooldown_trade_id = 0

        for _, row in group.iterrows():
            row_number = int(row["row_in_run"])

            if last_selected_row is not None:
                if row_number <= last_selected_row + cooldown_events:
                    continue

            cooldown_trade_id += 1
            last_selected_row = row_number

            item = row.to_dict()
            item["evaluation_method"] = "cooldown_first_signal"
            item["cooldown_events"] = cooldown_events
            item["cooldown_trade_id"] = cooldown_trade_id

            rows.append(item)

    result = pd.DataFrame(rows)

    if result.empty:
        return result

    result = result.sort_values(
        ["model", "feature_set", "horizon", "run_name", "row_in_run"]
    ).reset_index(drop=True)

    return result


def evaluate_threshold_grid(
    validation_scored: pd.DataFrame,
    model_name: str,
    feature_set: str,
    horizon: int,
) -> tuple[pd.DataFrame, dict]:
    rows = []

    for threshold in THRESHOLD_GRID:
        raw = make_selected_signals(
            scored=validation_scored,
            model_name=model_name,
            feature_set=feature_set,
            horizon=horizon,
            threshold=threshold,
            evaluation_method="validation_raw_selected_signal",
        )

        cooldown = apply_cooldown(raw, horizon=horizon)

        if cooldown.empty:
            rows.append(
                {
                    "model": model_name,
                    "feature_set": feature_set,
                    "horizon": horizon,
                    "threshold": threshold,
                    "validation_raw_signals": len(raw),
                    "validation_cooldown_trades": 0,
                    "validation_precision": np.nan,
                    "validation_mean_signed_return_bps": np.nan,
                    "validation_positive_signed_return_share": np.nan,
                    "meets_min_trades": False,
                }
            )
            continue

        rows.append(
            {
                "model": model_name,
                "feature_set": feature_set,
                "horizon": horizon,
                "threshold": threshold,
                "validation_raw_signals": len(raw),
                "validation_cooldown_trades": len(cooldown),
                "validation_precision": cooldown["is_correct_signal"].mean(),
                "validation_mean_signed_return_bps": cooldown["signed_return_bps"].mean(),
                "validation_positive_signed_return_share": cooldown["is_positive_signed_return"].mean(),
                "meets_min_trades": len(cooldown) >= MIN_VALIDATION_COOLDOWN_TRADES,
            }
        )

    grid = pd.DataFrame(rows)

    eligible = grid[
        grid["meets_min_trades"] &
        grid["validation_mean_signed_return_bps"].notna()
    ].copy()

    if eligible.empty:
        fallback = grid[grid["validation_mean_signed_return_bps"].notna()].copy()

        if fallback.empty:
            selected = grid.iloc[0].to_dict()
            selected["selection_reason"] = "fallback_no_validation_signals"
            return grid, selected

        selected = fallback.sort_values(
            [
                "validation_mean_signed_return_bps",
                "validation_precision",
                "validation_cooldown_trades",
            ],
            ascending=[False, False, False],
        ).iloc[0].to_dict()

        selected["selection_reason"] = "fallback_best_available"
        return grid, selected

    selected = eligible.sort_values(
        [
            "validation_mean_signed_return_bps",
            "validation_precision",
            "validation_cooldown_trades",
        ],
        ascending=[False, False, False],
    ).iloc[0].to_dict()

    selected["selection_reason"] = "best_validation_edge_with_min_trades"

    return grid, selected


def summarize_signals(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if evaluation.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon), group in evaluation.groupby(
        ["evaluation_method", "feature_set", "horizon"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "feature_set": feature_set,
                "horizon": horizon,
                "n_signals_or_trades": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_share": (group["signal"] == "up").mean(),
                "down_share": (group["signal"] == "down").mean(),
                "break_even_round_trip_cost_bps": group["signed_return_bps"].mean(),
                "runs_with_signals": group["run_name"].nunique(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "horizon", "break_even_round_trip_cost_bps"],
        ascending=[True, True, False],
    ).reset_index(drop=True)


def summarize_per_run(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if evaluation.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon, run_name), group in evaluation.groupby(
        ["evaluation_method", "feature_set", "horizon", "run_name"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "feature_set": feature_set,
                "horizon": horizon,
                "run_name": run_name,
                "n_signals_or_trades": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_count": int((group["signal"] == "up").sum()),
                "down_count": int((group["signal"] == "down").sum()),
            }
        )

    return pd.DataFrame(rows)


def summarize_direction(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if evaluation.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon, signal), group in evaluation.groupby(
        ["evaluation_method", "feature_set", "horizon", "signal"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "feature_set": feature_set,
                "horizon": horizon,
                "signal": signal,
                "n_signals_or_trades": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "mean_future_return_bps": group["future_mid_return_bps"].mean(),
            }
        )

    return pd.DataFrame(rows)


def summarize_cost_sanity(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if evaluation.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon), group in evaluation.groupby(
        ["evaluation_method", "feature_set", "horizon"],
        sort=True,
    ):
        for scenario in COST_SCENARIOS:
            cost_name = scenario["cost_scenario"]
            cost_bps = scenario["round_trip_cost_bps"]

            net_returns = group["signed_return_bps"] - cost_bps

            rows.append(
                {
                    "evaluation_method": method,
                    "feature_set": feature_set,
                    "horizon": horizon,
                    "cost_scenario": cost_name,
                    "round_trip_cost_bps": cost_bps,
                    "n_signals_or_trades": len(group),
                    "gross_mean_signed_return_bps": group["signed_return_bps"].mean(),
                    "net_mean_signed_return_bps": net_returns.mean(),
                    "net_median_signed_return_bps": net_returns.median(),
                    "net_positive_share": (net_returns > 0).mean(),
                }
            )

    return pd.DataFrame(rows)


def extract_logit_coefficients(
    model: Pipeline,
    feature_set: str,
    horizon: int,
    feature_cols: list[str],
) -> pd.DataFrame:
    coefficients = model.named_steps["logistic_regression"].coef_[0]

    result = pd.DataFrame(
        {
            "feature_set": feature_set,
            "horizon": horizon,
            "feature": feature_cols,
            "standardized_coefficient": coefficients,
            "abs_standardized_coefficient": np.abs(coefficients),
        }
    )

    result = result.sort_values(
        "abs_standardized_coefficient",
        ascending=False,
    ).reset_index(drop=True)

    return result


def save_summary_figure(summary: pd.DataFrame) -> None:
    cooldown = summary[summary["evaluation_method"] == "cooldown_first_signal"].copy()

    if cooldown.empty:
        return

    for horizon, group in cooldown.groupby("horizon", sort=True):
        group = group.sort_values("break_even_round_trip_cost_bps", ascending=False)

        plt.figure(figsize=(9, 5))
        plt.bar(group["feature_set"], group["break_even_round_trip_cost_bps"])
        plt.axhline(0)
        plt.title(f"Trade-flow validation: cooldown break-even, h{horizon}")
        plt.ylabel("Break-even round-trip cost, bps")
        plt.xlabel("Feature set")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"trade_flow_validation_break_even_h{horizon}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved figure: {out_path}")


def save_cost_figure(cost: pd.DataFrame) -> None:
    cooldown = cost[cost["evaluation_method"] == "cooldown_first_signal"].copy()

    if cooldown.empty:
        return

    for horizon, horizon_df in cooldown.groupby("horizon", sort=True):
        plt.figure(figsize=(9, 5))

        for feature_set, group in horizon_df.groupby("feature_set", sort=True):
            group = group.sort_values("round_trip_cost_bps")

            plt.plot(
                group["round_trip_cost_bps"],
                group["net_mean_signed_return_bps"],
                marker="o",
                label=feature_set,
            )

        plt.axhline(0)
        plt.title(f"Trade-flow validation: cost sanity, h{horizon}")
        plt.xlabel("Round-trip cost, bps")
        plt.ylabel("Net mean signed return, bps")
        plt.legend()
        plt.tight_layout()

        out_path = FIGURES_DIR / f"trade_flow_validation_cost_sanity_h{horizon}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved figure: {out_path}")


def main() -> None:
    ensure_output_dirs()

    runs = load_large_trade_runs()
    split_info = make_split_info(runs)

    split_info.to_csv(SPLIT_INFO_PATH, index=False)

    print("[INFO] Large trade runs:")
    for idx, run_name in enumerate(runs, start=1):
        print(f"  {idx}: {run_name}")

    print("\n[INFO] Split:")
    print(split_info)

    features = load_features(runs)

    print(f"\n[INFO] Filtered feature rows: {len(features)}")
    print(f"[INFO] Runs in feature dataset: {features['run_name'].nunique()}")

    train_runs = split_info[split_info["split"] == "train"]["run_name"].tolist()
    validation_runs = split_info[split_info["split"] == "validation"]["run_name"].tolist()
    test_runs = split_info[split_info["split"] == "test"]["run_name"].tolist()

    all_model_metrics = []
    all_threshold_grids = []
    selected_threshold_rows = []
    test_raw_frames = []
    test_cooldown_frames = []
    coefficient_frames = []

    for horizon in HORIZONS:
        print("\n" + "=" * 80)
        print(f"[INFO] Horizon h{horizon}")
        print("=" * 80)

        labeled = add_labels(features, horizon=horizon)

        print(f"[INFO] Non-flat rows for h{horizon}: {len(labeled)}")
        print("[INFO] Target distribution:")
        print(labeled["target_label"].value_counts())

        train = labeled[labeled["run_name"].isin(train_runs)].copy()
        validation = labeled[labeled["run_name"].isin(validation_runs)].copy()
        test = labeled[labeled["run_name"].isin(test_runs)].copy()

        print(f"[INFO] Train rows: {len(train)}")
        print(f"[INFO] Validation rows: {len(validation)}")
        print(f"[INFO] Test rows: {len(test)}")

        for feature_set, feature_cols in FEATURE_SETS.items():
            model_name = f"logit_{feature_set}_h{horizon}"

            print("\n" + "-" * 80)
            print(f"[INFO] Model: {model_name}")
            print(f"[INFO] Feature count: {len(feature_cols)}")

            missing = [col for col in feature_cols if col not in labeled.columns]

            if missing:
                raise ValueError(f"Missing feature columns for {feature_set}: {missing}")

            train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
            validation_clean = validation.dropna(subset=feature_cols + ["target"]).copy()
            test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

            if train_clean.empty or validation_clean.empty or test_clean.empty:
                print(f"[WARNING] Empty split for {model_name}; skipping.")
                continue

            if train_clean["target"].nunique() < 2:
                print(f"[WARNING] Train split has only one class for {model_name}; skipping.")
                continue

            X_train = train_clean[feature_cols]
            y_train = train_clean["target"]

            X_validation = validation_clean[feature_cols]
            y_validation = validation_clean["target"]

            X_test = test_clean[feature_cols]
            y_test = test_clean["target"]

            sample_weight = compute_sample_weight(
                class_weight="balanced",
                y=y_train,
            )

            model = build_model()
            model.fit(
                X_train,
                y_train,
                logistic_regression__sample_weight=sample_weight,
            )

            validation_p_up = model.predict_proba(X_validation)[:, 1]
            test_p_up = model.predict_proba(X_test)[:, 1]

            all_model_metrics.append(
                ordinary_model_metrics(
                    model_name=model_name,
                    feature_set=feature_set,
                    horizon=horizon,
                    split_name="validation",
                    y_true=y_validation,
                    p_up=validation_p_up,
                )
            )

            all_model_metrics.append(
                ordinary_model_metrics(
                    model_name=model_name,
                    feature_set=feature_set,
                    horizon=horizon,
                    split_name="test",
                    y_true=y_test,
                    p_up=test_p_up,
                )
            )

            validation_scored = validation_clean.copy()
            validation_scored["predicted_proba_up"] = validation_p_up

            threshold_grid, selected_threshold = evaluate_threshold_grid(
                validation_scored=validation_scored,
                model_name=model_name,
                feature_set=feature_set,
                horizon=horizon,
            )

            all_threshold_grids.append(threshold_grid)
            selected_threshold_rows.append(selected_threshold)

            threshold = float(selected_threshold["threshold"])

            print(f"[INFO] Selected threshold: {threshold:.2f}")
            print(f"[INFO] Selection reason: {selected_threshold['selection_reason']}")
            print(
                "[INFO] Validation cooldown mean signed return: "
                f"{selected_threshold['validation_mean_signed_return_bps']}"
            )

            test_scored = test_clean.copy()
            test_scored["predicted_proba_up"] = test_p_up

            test_raw = make_selected_signals(
                scored=test_scored,
                model_name=model_name,
                feature_set=feature_set,
                horizon=horizon,
                threshold=threshold,
                evaluation_method="raw_selected_signal",
            )

            test_cooldown = apply_cooldown(test_raw, horizon=horizon)

            if not test_raw.empty:
                test_raw_frames.append(test_raw)

            if not test_cooldown.empty:
                test_cooldown_frames.append(test_cooldown)

            coefficient_frames.append(
                extract_logit_coefficients(
                    model=model,
                    feature_set=feature_set,
                    horizon=horizon,
                    feature_cols=feature_cols,
                )
            )

            print(f"[INFO] Test raw signals: {len(test_raw)}")
            if not test_raw.empty:
                print(f"[INFO] Test raw mean signed return: {test_raw['signed_return_bps'].mean():.4f} bps")

            print(f"[INFO] Test cooldown trades: {len(test_cooldown)}")
            if not test_cooldown.empty:
                print(f"[INFO] Test cooldown mean signed return: {test_cooldown['signed_return_bps'].mean():.4f} bps")

    metrics = pd.DataFrame(all_model_metrics)
    threshold_grids = pd.concat(all_threshold_grids, ignore_index=True)
    selected_thresholds = pd.DataFrame(selected_threshold_rows)
    coefficients = pd.concat(coefficient_frames, ignore_index=True)

    if test_raw_frames:
        test_raw_all = pd.concat(test_raw_frames, ignore_index=True)
    else:
        test_raw_all = pd.DataFrame()

    if test_cooldown_frames:
        test_cooldown_all = pd.concat(test_cooldown_frames, ignore_index=True)
    else:
        test_cooldown_all = pd.DataFrame()

    if not test_raw_all.empty:
        test_raw_all.to_csv(TEST_RAW_SIGNALS_PATH, index=False)

    if not test_cooldown_all.empty:
        test_cooldown_all.to_csv(TEST_COOLDOWN_SIGNALS_PATH, index=False)

    evaluation = pd.concat(
        [test_raw_all, test_cooldown_all],
        ignore_index=True,
        sort=False,
    )

    summary = summarize_signals(evaluation)
    per_run = summarize_per_run(evaluation)
    direction = summarize_direction(evaluation)
    cost = summarize_cost_sanity(evaluation)

    metrics.to_csv(MODEL_METRICS_PATH, index=False)
    threshold_grids.to_csv(VALIDATION_THRESHOLD_GRID_PATH, index=False)
    selected_thresholds.to_csv(SELECTED_THRESHOLDS_PATH, index=False)
    coefficients.to_csv(COEFFICIENTS_PATH, index=False)

    summary.to_csv(TEST_SUMMARY_PATH, index=False)
    per_run.to_csv(TEST_PER_RUN_PATH, index=False)
    direction.to_csv(TEST_DIRECTION_PATH, index=False)
    cost.to_csv(COST_SANITY_PATH, index=False)

    save_summary_figure(summary)
    save_cost_figure(cost)

    print("\n" + "=" * 80)
    print("[INFO] Saved outputs")
    print("=" * 80)
    print(f"[INFO] Split info: {SPLIT_INFO_PATH}")
    print(f"[INFO] Model metrics: {MODEL_METRICS_PATH}")
    print(f"[INFO] Validation threshold grid: {VALIDATION_THRESHOLD_GRID_PATH}")
    print(f"[INFO] Selected thresholds: {SELECTED_THRESHOLDS_PATH}")
    print(f"[INFO] Test raw signals: {TEST_RAW_SIGNALS_PATH}")
    print(f"[INFO] Test cooldown signals: {TEST_COOLDOWN_SIGNALS_PATH}")
    print(f"[INFO] Test summary: {TEST_SUMMARY_PATH}")
    print(f"[INFO] Test per run: {TEST_PER_RUN_PATH}")
    print(f"[INFO] Test direction: {TEST_DIRECTION_PATH}")
    print(f"[INFO] Cost sanity: {COST_SANITY_PATH}")
    print(f"[INFO] Coefficients: {COEFFICIENTS_PATH}")

    print("\n[INFO] Test summary:")
    print(summary)

    print("\n[INFO] Trade-flow predictive power validation completed successfully.")


if __name__ == "__main__":
    main()