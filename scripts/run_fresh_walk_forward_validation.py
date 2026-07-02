from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.utils.class_weight import compute_sample_weight


try:
    from xgboost import XGBClassifier
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBClassifier = None
    XGBOOST_AVAILABLE = False


try:
    from lightgbm import LGBMClassifier
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LGBMClassifier = None
    LIGHTGBM_AVAILABLE = False


warnings.filterwarnings("ignore", category=UserWarning)


FEATURES_PATH = Path("data/processed/basic_features_all.csv")
FRESH_LOG_PATH = Path("reports/tables/fresh_oos_collection_log.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

TARGET_HORIZON = 50
PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5
COOLDOWN_EVENTS = 50

TRAIN_RUNS = 4
VALIDATION_RUNS = 2
TEST_RUNS = 1
STEP_RUNS = 1

THRESHOLD_GRID = [
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    0.95,
]

MIN_VALIDATION_COOLDOWN_TRADES = 5

SPLIT_PLAN_PATH = TABLES_DIR / "fresh_walk_forward_split_plan_h50.csv"
THRESHOLD_SELECTION_PATH = TABLES_DIR / "fresh_walk_forward_threshold_selection_h50.csv"
TEST_RAW_SIGNALS_PATH = TABLES_DIR / "fresh_walk_forward_test_raw_signals_h50.csv"
TEST_COOLDOWN_SIGNALS_PATH = TABLES_DIR / "fresh_walk_forward_test_cooldown_signals_h50.csv"
SUMMARY_PATH = TABLES_DIR / "fresh_walk_forward_summary_h50.csv"
PER_FOLD_PATH = TABLES_DIR / "fresh_walk_forward_per_fold_h50.csv"
PER_RUN_PATH = TABLES_DIR / "fresh_walk_forward_per_run_h50.csv"
DIRECTION_PATH = TABLES_DIR / "fresh_walk_forward_direction_h50.csv"
COST_PATH = TABLES_DIR / "fresh_walk_forward_cost_sanity_h50.csv"

COST_SCENARIOS = [
    {"cost_scenario": "no_cost", "round_trip_cost_bps": 0.0},
    {"cost_scenario": "very_low_cost", "round_trip_cost_bps": 1.0},
    {"cost_scenario": "low_cost", "round_trip_cost_bps": 2.0},
    {"cost_scenario": "medium_cost", "round_trip_cost_bps": 3.0},
    {"cost_scenario": "high_cost", "round_trip_cost_bps": 4.0},
    {"cost_scenario": "expensive_taker_like", "round_trip_cost_bps": 5.0},
    {"cost_scenario": "very_expensive_round_trip", "round_trip_cost_bps": 9.0},
]


STATIC_SIGNAL_FEATURES = [
    "imbalance_5",
    "microprice_deviation_bps",
]

STATIC_FULL_FEATURES = [
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

DYNAMIC_FEATURES = [
    "imbalance_5_change_1e",
    "imbalance_5_change_5e",
    "imbalance_5_change_10e",
    "imbalance_5_rolling_mean_5e",
    "imbalance_5_rolling_mean_10e",

    "microprice_deviation_bps_change_1e",
    "microprice_deviation_bps_change_5e",
    "microprice_deviation_bps_change_10e",
    "microprice_deviation_bps_rolling_mean_5e",
    "microprice_deviation_bps_rolling_mean_10e",

    "mid_return_bps_lag_1e",
    "mid_return_bps_lag_2e",
    "mid_return_bps_lag_5e",
    "mid_return_bps_lag_10e",
    "mid_return_bps_rolling_mean_5e",
    "mid_return_bps_rolling_mean_10e",
    "mid_return_bps_rolling_std_5e",
    "mid_return_bps_rolling_std_10e",

    "quote_changed_rolling_sum_5e",
    "quote_changed_rolling_sum_10e",

    "spread_bps_change_1e",
    "spread_bps_change_5e",
    "spread_bps_rolling_mean_5e",
    "spread_bps_rolling_mean_10e",

    "best_bid_qty_change_1e",
    "best_ask_qty_change_1e",
    "bid_depth_5_change_1e",
    "ask_depth_5_change_1e",
]

FEATURE_SETS = {
    "dynamic_compact": STATIC_SIGNAL_FEATURES + DYNAMIC_FEATURES,
    "dynamic_full": STATIC_FULL_FEATURES + DYNAMIC_FEATURES,
}


MODEL_CONFIGS = [
    {
        "model": "logit_dynamic_compact",
        "algorithm": "logit",
        "feature_set": "dynamic_compact",
    },
    {
        "model": "lightgbm_dynamic_compact",
        "algorithm": "lightgbm",
        "feature_set": "dynamic_compact",
    },
    {
        "model": "xgboost_dynamic_full",
        "algorithm": "xgboost",
        "feature_set": "dynamic_full",
    },
]


def ensure_output_dirs():
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_fresh_runs():
    if not FRESH_LOG_PATH.exists():
        raise FileNotFoundError(f"Fresh log not found: {FRESH_LOG_PATH}")

    log = pd.read_csv(FRESH_LOG_PATH)

    required_cols = ["collection_batch", "status", "run_name", "run_number"]
    missing = [col for col in required_cols if col not in log.columns]

    if missing:
        raise ValueError(f"Fresh log missing columns: {missing}")

    fresh = log[
        (log["collection_batch"] == "fresh_oos_day20") &
        (log["status"] == "success")
    ].copy()

    fresh = fresh.sort_values("run_number")

    fresh_runs = fresh["run_name"].dropna().astype(str).tolist()

    if len(fresh_runs) < TRAIN_RUNS + VALIDATION_RUNS + TEST_RUNS:
        raise ValueError(
            "Not enough fresh runs for requested walk-forward design."
        )

    return fresh_runs


def load_features():
    if not FEATURES_PATH.exists():
        raise FileNotFoundError(f"Features file not found: {FEATURES_PATH}")

    df = pd.read_csv(FEATURES_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
        "quote_changed",
        "mid_return_bps",
        "spread_bps",
        "imbalance_5",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Features file missing required columns: {missing}")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def add_microprice_deviation_if_needed(df):
    df = df.copy()

    if "microprice_deviation_bps" in df.columns:
        return df

    if "microprice" not in df.columns:
        raise ValueError(
            "microprice_deviation_bps is missing and microprice is unavailable."
        )

    df["microprice_deviation_bps"] = (
        df["microprice"] / df["mid_price"] - 1.0
    ) * 10000.0

    return df


def add_dynamic_features(df):
    df = add_microprice_deviation_if_needed(df)

    frames = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        temp = run_df.copy()
        temp = temp.sort_values("event_time").reset_index(drop=True)

        temp["imbalance_5_change_1e"] = temp["imbalance_5"] - temp["imbalance_5"].shift(1)
        temp["imbalance_5_change_5e"] = temp["imbalance_5"] - temp["imbalance_5"].shift(5)
        temp["imbalance_5_change_10e"] = temp["imbalance_5"] - temp["imbalance_5"].shift(10)
        temp["imbalance_5_rolling_mean_5e"] = temp["imbalance_5"].rolling(5, min_periods=1).mean()
        temp["imbalance_5_rolling_mean_10e"] = temp["imbalance_5"].rolling(10, min_periods=1).mean()

        temp["microprice_deviation_bps_change_1e"] = (
            temp["microprice_deviation_bps"] -
            temp["microprice_deviation_bps"].shift(1)
        )
        temp["microprice_deviation_bps_change_5e"] = (
            temp["microprice_deviation_bps"] -
            temp["microprice_deviation_bps"].shift(5)
        )
        temp["microprice_deviation_bps_change_10e"] = (
            temp["microprice_deviation_bps"] -
            temp["microprice_deviation_bps"].shift(10)
        )
        temp["microprice_deviation_bps_rolling_mean_5e"] = (
            temp["microprice_deviation_bps"].rolling(5, min_periods=1).mean()
        )
        temp["microprice_deviation_bps_rolling_mean_10e"] = (
            temp["microprice_deviation_bps"].rolling(10, min_periods=1).mean()
        )

        temp["mid_return_bps_lag_1e"] = temp["mid_return_bps"].shift(1)
        temp["mid_return_bps_lag_2e"] = temp["mid_return_bps"].shift(2)
        temp["mid_return_bps_lag_5e"] = temp["mid_return_bps"].shift(5)
        temp["mid_return_bps_lag_10e"] = temp["mid_return_bps"].shift(10)
        temp["mid_return_bps_rolling_mean_5e"] = temp["mid_return_bps"].rolling(5, min_periods=1).mean()
        temp["mid_return_bps_rolling_mean_10e"] = temp["mid_return_bps"].rolling(10, min_periods=1).mean()
        temp["mid_return_bps_rolling_std_5e"] = temp["mid_return_bps"].rolling(5, min_periods=2).std()
        temp["mid_return_bps_rolling_std_10e"] = temp["mid_return_bps"].rolling(10, min_periods=2).std()

        temp["quote_changed_rolling_sum_5e"] = temp["quote_changed"].rolling(5, min_periods=1).sum()
        temp["quote_changed_rolling_sum_10e"] = temp["quote_changed"].rolling(10, min_periods=1).sum()

        temp["spread_bps_change_1e"] = temp["spread_bps"] - temp["spread_bps"].shift(1)
        temp["spread_bps_change_5e"] = temp["spread_bps"] - temp["spread_bps"].shift(5)
        temp["spread_bps_rolling_mean_5e"] = temp["spread_bps"].rolling(5, min_periods=1).mean()
        temp["spread_bps_rolling_mean_10e"] = temp["spread_bps"].rolling(10, min_periods=1).mean()

        temp["best_bid_qty_change_1e"] = temp["best_bid_qty"] - temp["best_bid_qty"].shift(1)
        temp["best_ask_qty_change_1e"] = temp["best_ask_qty"] - temp["best_ask_qty"].shift(1)
        temp["bid_depth_5_change_1e"] = temp["bid_depth_5"] - temp["bid_depth_5"].shift(1)
        temp["ask_depth_5_change_1e"] = temp["ask_depth_5"] - temp["ask_depth_5"].shift(1)

        frames.append(temp)

    return pd.concat(frames, ignore_index=True)


def label_from_tick_change(tick_change):
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > DEAD_ZONE_TICKS:
        return "up"

    if tick_change < -DEAD_ZONE_TICKS:
        return "down"

    return "flat"


def build_dataset_for_horizon(df, horizon):
    frames = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        temp = run_df.copy()
        temp = temp.sort_values("event_time").reset_index(drop=True)

        temp["target_horizon"] = horizon
        temp["future_mid_price"] = temp["mid_price"].shift(-horizon)
        temp["future_mid_change"] = temp["future_mid_price"] - temp["mid_price"]
        temp["future_mid_change_ticks"] = temp["future_mid_change"] / PRICE_TICK
        temp["future_mid_return"] = temp["future_mid_price"] / temp["mid_price"] - 1.0
        temp["future_mid_return_bps"] = temp["future_mid_return"] * 10000.0

        temp["target_label"] = temp["future_mid_change_ticks"].apply(
            label_from_tick_change
        )

        frames.append(temp)

    labeled = pd.concat(frames, ignore_index=True)

    labeled = labeled[labeled["target_label"].isin(["down", "up"])].copy()
    labeled["target"] = labeled["target_label"].map({"down": 0, "up": 1}).astype(int)

    return labeled


def build_walk_forward_plan(fresh_runs):
    rows = []
    fold_number = 0

    window_size = TRAIN_RUNS + VALIDATION_RUNS + TEST_RUNS

    for start_idx in range(0, len(fresh_runs) - window_size + 1, STEP_RUNS):
        fold_number += 1

        train_runs = fresh_runs[start_idx:start_idx + TRAIN_RUNS]
        validation_runs = fresh_runs[
            start_idx + TRAIN_RUNS:
            start_idx + TRAIN_RUNS + VALIDATION_RUNS
        ]
        test_runs = fresh_runs[
            start_idx + TRAIN_RUNS + VALIDATION_RUNS:
            start_idx + window_size
        ]

        for run_name in train_runs:
            rows.append(
                {
                    "fold": fold_number,
                    "split": "train",
                    "run_name": run_name,
                }
            )

        for run_name in validation_runs:
            rows.append(
                {
                    "fold": fold_number,
                    "split": "validation",
                    "run_name": run_name,
                }
            )

        for run_name in test_runs:
            rows.append(
                {
                    "fold": fold_number,
                    "split": "test",
                    "run_name": run_name,
                }
            )

    plan = pd.DataFrame(rows)

    if plan.empty:
        raise ValueError("Walk-forward plan is empty.")

    return plan


def build_model(algorithm):
    if algorithm == "logit":
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

    if algorithm == "lightgbm":
        if not LIGHTGBM_AVAILABLE:
            return None

        return LGBMClassifier(
            n_estimators=150,
            max_depth=2,
            learning_rate=0.05,
            num_leaves=4,
            subsample=0.8,
            subsample_freq=1,
            colsample_bytree=0.8,
            reg_lambda=5.0,
            objective="binary",
            random_state=42,
            n_jobs=1,
            verbose=-1,
        )

    if algorithm == "xgboost":
        if not XGBOOST_AVAILABLE:
            return None

        return XGBClassifier(
            n_estimators=150,
            max_depth=2,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=5.0,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=42,
            n_jobs=1,
            tree_method="hist",
        )

    raise ValueError(f"Unknown algorithm: {algorithm}")


def fit_model(algorithm, model, X_train, y_train, sample_weight):
    if algorithm == "logit":
        model.fit(
            X_train,
            y_train,
            logistic_regression__sample_weight=sample_weight,
        )
    else:
        model.fit(
            X_train,
            y_train,
            sample_weight=sample_weight,
        )

    return model


def assign_signal(p_up, threshold):
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def add_signal_columns(df, model_name, algorithm, feature_set, threshold, fold):
    selected = df[df["signal"].isin(["up", "down"])].copy()

    if selected.empty:
        return selected

    selected["evaluation_method"] = "raw_selected_signal"
    selected["model"] = model_name
    selected["algorithm"] = algorithm
    selected["feature_set"] = feature_set
    selected["threshold"] = threshold
    selected["fold"] = fold

    selected["is_correct_signal"] = (
        ((selected["signal"] == "up") & (selected["target_label"] == "up")) |
        ((selected["signal"] == "down") & (selected["target_label"] == "down"))
    )

    selected["signed_return_bps"] = None

    up_mask = selected["signal"] == "up"
    down_mask = selected["signal"] == "down"

    selected.loc[up_mask, "signed_return_bps"] = selected.loc[
        up_mask,
        "future_mid_return_bps",
    ]

    selected.loc[down_mask, "signed_return_bps"] = -selected.loc[
        down_mask,
        "future_mid_return_bps",
    ]

    selected["signed_return_bps"] = pd.to_numeric(
        selected["signed_return_bps"],
        errors="coerce",
    )

    selected["is_positive_signed_return"] = selected["signed_return_bps"] > 0

    keep_cols = [
        "evaluation_method",
        "fold",
        "model",
        "algorithm",
        "feature_set",
        "threshold",
        "run_name",
        "row_in_run",
        "event_time",
        "target_horizon",
        "target_label",
        "signal",
        "predicted_proba_up",
        "future_mid_return_bps",
        "signed_return_bps",
        "is_correct_signal",
        "is_positive_signed_return",
    ]

    return selected[keep_cols].copy()


def apply_cooldown(raw_signals, method_name="cooldown_first_signal"):
    rows = []

    if raw_signals.empty:
        return raw_signals.copy()

    group_cols = ["fold", "model", "run_name"]

    for (fold, model_name, run_name), group in raw_signals.groupby(group_cols, sort=True):
        group = group.sort_values("row_in_run").reset_index(drop=True)

        last_selected_row = None
        trade_id = 0

        for _, row in group.iterrows():
            row_number = int(row["row_in_run"])

            if last_selected_row is not None:
                if row_number <= last_selected_row + COOLDOWN_EVENTS:
                    continue

            trade_id += 1
            last_selected_row = row_number

            item = row.to_dict()
            item["evaluation_method"] = method_name
            item["cooldown_trade_id"] = trade_id

            rows.append(item)

    cooldown = pd.DataFrame(rows)

    if cooldown.empty:
        return cooldown

    cooldown = cooldown.sort_values(
        ["fold", "model", "run_name", "row_in_run"]
    ).reset_index(drop=True)

    return cooldown


def evaluate_thresholds_on_validation(
    validation_scored,
    model_name,
    algorithm,
    feature_set,
    fold,
):
    rows = []

    for threshold in THRESHOLD_GRID:
        temp = validation_scored.copy()
        temp["signal"] = temp["predicted_proba_up"].apply(
            lambda p: assign_signal(p, threshold)
        )

        raw = add_signal_columns(
            df=temp,
            model_name=model_name,
            algorithm=algorithm,
            feature_set=feature_set,
            threshold=threshold,
            fold=fold,
        )

        if raw.empty:
            rows.append(
                {
                    "fold": fold,
                    "model": model_name,
                    "algorithm": algorithm,
                    "feature_set": feature_set,
                    "threshold": threshold,
                    "validation_raw_signals": 0,
                    "validation_cooldown_trades": 0,
                    "validation_precision": None,
                    "validation_mean_signed_return_bps": None,
                    "validation_positive_signed_return_share": None,
                    "meets_min_trades": False,
                }
            )
            continue

        cooldown = apply_cooldown(raw, method_name="validation_cooldown")

        rows.append(
            {
                "fold": fold,
                "model": model_name,
                "algorithm": algorithm,
                "feature_set": feature_set,
                "threshold": threshold,
                "validation_raw_signals": len(raw),
                "validation_cooldown_trades": len(cooldown),
                "validation_precision": cooldown["is_correct_signal"].mean() if len(cooldown) else None,
                "validation_mean_signed_return_bps": cooldown["signed_return_bps"].mean() if len(cooldown) else None,
                "validation_positive_signed_return_share": cooldown["is_positive_signed_return"].mean() if len(cooldown) else None,
                "meets_min_trades": len(cooldown) >= MIN_VALIDATION_COOLDOWN_TRADES,
            }
        )

    result = pd.DataFrame(rows)

    eligible = result[
        result["meets_min_trades"] &
        result["validation_mean_signed_return_bps"].notna()
    ].copy()

    if eligible.empty:
        fallback = result[
            result["validation_mean_signed_return_bps"].notna()
        ].copy()

        if fallback.empty:
            selected = result.iloc[0].copy()
            selected["selection_reason"] = "fallback_no_validation_signals"
            return result, selected

        selected = fallback.sort_values(
            [
                "validation_mean_signed_return_bps",
                "validation_precision",
                "validation_cooldown_trades",
            ],
            ascending=[False, False, False],
        ).iloc[0].copy()
        selected["selection_reason"] = "fallback_best_available"
        return result, selected

    selected = eligible.sort_values(
        [
            "validation_mean_signed_return_bps",
            "validation_precision",
            "validation_cooldown_trades",
        ],
        ascending=[False, False, False],
    ).iloc[0].copy()
    selected["selection_reason"] = "best_validation_edge_with_min_trades"

    return result, selected


def summarize_overall(evaluation):
    rows = []

    for (method, model_name), group in evaluation.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "algorithm": group["algorithm"].iloc[0],
                "feature_set": group["feature_set"].iloc[0],
                "n_signals_or_trades": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_share": (group["signal"] == "up").mean(),
                "down_share": (group["signal"] == "down").mean(),
                "break_even_round_trip_cost_bps": group["signed_return_bps"].mean(),
                "folds_with_signals": group["fold"].nunique(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "break_even_round_trip_cost_bps", "precision"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def summarize_per_fold(evaluation):
    rows = []

    for (method, model_name, fold), group in evaluation.groupby(
        ["evaluation_method", "model", "fold"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "fold": fold,
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


def summarize_per_run(evaluation):
    rows = []

    for (method, model_name, run_name), group in evaluation.groupby(
        ["evaluation_method", "model", "run_name"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
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


def summarize_direction(evaluation):
    rows = []

    for (method, model_name, signal), group in evaluation.groupby(
        ["evaluation_method", "model", "signal"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
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


def summarize_cost_scenarios(evaluation):
    rows = []

    for (method, model_name), group in evaluation.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        for scenario in COST_SCENARIOS:
            cost_name = scenario["cost_scenario"]
            cost_bps = scenario["round_trip_cost_bps"]

            temp = group.copy()
            temp["net_signed_return_bps"] = temp["signed_return_bps"] - cost_bps
            temp["is_net_positive"] = temp["net_signed_return_bps"] > 0

            rows.append(
                {
                    "evaluation_method": method,
                    "model": model_name,
                    "cost_scenario": cost_name,
                    "round_trip_cost_bps": cost_bps,
                    "n_signals_or_trades": len(temp),
                    "gross_mean_signed_return_bps": temp["signed_return_bps"].mean(),
                    "net_mean_signed_return_bps": temp["net_signed_return_bps"].mean(),
                    "net_median_signed_return_bps": temp["net_signed_return_bps"].median(),
                    "net_positive_share": temp["is_net_positive"].mean(),
                }
            )

    return pd.DataFrame(rows)


def save_summary_plot(summary):
    for method, group in summary.groupby("evaluation_method", sort=True):
        group = group.sort_values("break_even_round_trip_cost_bps", ascending=False)

        plt.figure(figsize=(10, 6))
        plt.bar(
            group["model"],
            group["break_even_round_trip_cost_bps"],
        )
        plt.axhline(0)
        plt.xticks(rotation=75, ha="right")
        plt.title(f"Fresh walk-forward break-even: {method}")
        plt.ylabel("Break-even round-trip cost, bps")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"fresh_walk_forward_summary_{method}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved summary plot to: {out_path}")


def save_cost_plot(cost):
    for method, method_df in cost.groupby("evaluation_method", sort=True):
        plt.figure(figsize=(10, 6))

        for model_name, group in method_df.groupby("model", sort=True):
            group = group.sort_values("round_trip_cost_bps")

            plt.plot(
                group["round_trip_cost_bps"],
                group["net_mean_signed_return_bps"],
                marker="o",
                label=model_name,
            )

        plt.axhline(0)
        plt.title(f"Fresh walk-forward cost sanity: {method}")
        plt.xlabel("Round-trip cost, bps")
        plt.ylabel("Net mean signed return, bps")
        plt.legend(fontsize=7)
        plt.tight_layout()

        out_path = FIGURES_DIR / f"fresh_walk_forward_cost_sanity_{method}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved cost plot to: {out_path}")


def save_all_plots(summary, cost):
    save_summary_plot(summary)
    save_cost_plot(cost)


def main():
    ensure_output_dirs()

    print(f"[INFO] XGBoost available: {XGBOOST_AVAILABLE}")
    print(f"[INFO] LightGBM available: {LIGHTGBM_AVAILABLE}")

    fresh_runs = load_fresh_runs()

    print(f"[INFO] Fresh runs available: {len(fresh_runs)}")
    for idx, run_name in enumerate(fresh_runs, start=1):
        print(f"  {idx}: {run_name}")

    features = load_features()
    features = features[features["run_name"].isin(fresh_runs)].copy()

    print(f"[INFO] Fresh-only feature rows: {len(features)}")
    print(f"[INFO] Fresh-only runs: {features['run_name'].nunique()}")

    features = add_dynamic_features(features)

    dataset = build_dataset_for_horizon(features, horizon=TARGET_HORIZON)

    print(f"[INFO] Fresh-only non-flat labeled rows: {len(dataset)}")
    print("[INFO] Target distribution:")
    print(dataset["target_label"].value_counts())

    split_plan = build_walk_forward_plan(fresh_runs)
    split_plan.to_csv(SPLIT_PLAN_PATH, index=False)

    print(f"[INFO] Walk-forward split plan saved to: {SPLIT_PLAN_PATH}")
    print(f"[INFO] Number of folds: {split_plan['fold'].nunique()}")

    threshold_evaluation_frames = []
    threshold_selection_rows = []
    test_raw_frames = []

    for fold in sorted(split_plan["fold"].unique()):
        fold_plan = split_plan[split_plan["fold"] == fold]

        train_runs = fold_plan[fold_plan["split"] == "train"]["run_name"].tolist()
        validation_runs = fold_plan[fold_plan["split"] == "validation"]["run_name"].tolist()
        test_runs = fold_plan[fold_plan["split"] == "test"]["run_name"].tolist()

        print("\n" + "=" * 80)
        print(f"[INFO] Fold {fold}")
        print(f"[INFO] Train runs: {train_runs}")
        print(f"[INFO] Validation runs: {validation_runs}")
        print(f"[INFO] Test runs: {test_runs}")
        print("=" * 80)

        train = dataset[dataset["run_name"].isin(train_runs)].copy()
        validation = dataset[dataset["run_name"].isin(validation_runs)].copy()
        test = dataset[dataset["run_name"].isin(test_runs)].copy()

        print(f"[INFO] Train rows: {len(train)}")
        print(f"[INFO] Validation rows: {len(validation)}")
        print(f"[INFO] Test rows: {len(test)}")

        for config in MODEL_CONFIGS:
            model_name = config["model"]
            algorithm = config["algorithm"]
            feature_set = config["feature_set"]
            feature_cols = FEATURE_SETS[feature_set]

            print(f"\n[INFO] Model: {model_name}")

            missing = [col for col in feature_cols if col not in dataset.columns]
            if missing:
                raise ValueError(f"Missing feature columns for {model_name}: {missing}")

            model = build_model(algorithm)

            if model is None:
                print(f"[WARNING] Skipping {model_name}: package unavailable.")
                continue

            train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
            validation_clean = validation.dropna(subset=feature_cols + ["target"]).copy()
            test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

            if train_clean.empty or validation_clean.empty or test_clean.empty:
                print(f"[WARNING] Skipping {model_name}: empty split after dropna.")
                continue

            if train_clean["target"].nunique() < 2:
                print(f"[WARNING] Skipping {model_name}: train has only one class.")
                continue

            X_train = train_clean[feature_cols]
            y_train = train_clean["target"]

            X_validation = validation_clean[feature_cols]
            X_test = test_clean[feature_cols]

            sample_weight = compute_sample_weight(
                class_weight="balanced",
                y=y_train,
            )

            model = fit_model(
                algorithm=algorithm,
                model=model,
                X_train=X_train,
                y_train=y_train,
                sample_weight=sample_weight,
            )

            validation_scored = validation_clean.copy()
            validation_scored["predicted_proba_up"] = model.predict_proba(X_validation)[:, 1]

            threshold_eval, selected_threshold_row = evaluate_thresholds_on_validation(
                validation_scored=validation_scored,
                model_name=model_name,
                algorithm=algorithm,
                feature_set=feature_set,
                fold=fold,
            )

            threshold_evaluation_frames.append(threshold_eval)

            selected_threshold = float(selected_threshold_row["threshold"])

            selection_record = selected_threshold_row.to_dict()
            threshold_selection_rows.append(selection_record)

            print(f"[INFO] Selected threshold: {selected_threshold:.2f}")
            print(f"[INFO] Selection reason: {selection_record.get('selection_reason')}")
            print(
                "[INFO] Validation cooldown mean signed return: "
                f"{selection_record.get('validation_mean_signed_return_bps')}"
            )

            test_scored = test_clean.copy()
            test_scored["predicted_proba_up"] = model.predict_proba(X_test)[:, 1]
            test_scored["signal"] = test_scored["predicted_proba_up"].apply(
                lambda p: assign_signal(p, selected_threshold)
            )

            test_raw = add_signal_columns(
                df=test_scored,
                model_name=model_name,
                algorithm=algorithm,
                feature_set=feature_set,
                threshold=selected_threshold,
                fold=fold,
            )

            if test_raw.empty:
                print(f"[INFO] No test raw signals for {model_name}.")
                continue

            test_raw_frames.append(test_raw)

            print(f"[INFO] Test raw signals: {len(test_raw)}")
            print(f"[INFO] Test raw precision: {test_raw['is_correct_signal'].mean():.4f}")
            print(f"[INFO] Test raw mean signed return: {test_raw['signed_return_bps'].mean():.4f} bps")

    if not threshold_evaluation_frames:
        raise ValueError("No threshold evaluations were produced.")

    threshold_eval_all = pd.concat(threshold_evaluation_frames, ignore_index=True)
    threshold_selection = pd.DataFrame(threshold_selection_rows)

    threshold_eval_all.to_csv(THRESHOLD_SELECTION_PATH, index=False)

    if not test_raw_frames:
        raise ValueError("No test raw signals were produced.")

    test_raw_all = pd.concat(test_raw_frames, ignore_index=True)
    test_cooldown_all = apply_cooldown(
        test_raw_all,
        method_name="cooldown_first_signal",
    )

    test_raw_all.to_csv(TEST_RAW_SIGNALS_PATH, index=False)
    test_cooldown_all.to_csv(TEST_COOLDOWN_SIGNALS_PATH, index=False)

    evaluation_all = pd.concat(
        [test_raw_all, test_cooldown_all],
        ignore_index=True,
        sort=False,
    )

    summary = summarize_overall(evaluation_all)
    per_fold = summarize_per_fold(evaluation_all)
    per_run = summarize_per_run(evaluation_all)
    direction = summarize_direction(evaluation_all)
    cost = summarize_cost_scenarios(evaluation_all)

    threshold_selection.to_csv(
        TABLES_DIR / "fresh_walk_forward_selected_thresholds_h50.csv",
        index=False,
    )
    summary.to_csv(SUMMARY_PATH, index=False)
    per_fold.to_csv(PER_FOLD_PATH, index=False)
    per_run.to_csv(PER_RUN_PATH, index=False)
    direction.to_csv(DIRECTION_PATH, index=False)
    cost.to_csv(COST_PATH, index=False)

    save_all_plots(summary=summary, cost=cost)

    print(f"\n[INFO] Saved split plan to: {SPLIT_PLAN_PATH}")
    print(f"[INFO] Saved threshold evaluation to: {THRESHOLD_SELECTION_PATH}")
    print(f"[INFO] Saved selected thresholds to: reports/tables/fresh_walk_forward_selected_thresholds_h50.csv")
    print(f"[INFO] Saved test raw signals to: {TEST_RAW_SIGNALS_PATH}")
    print(f"[INFO] Saved test cooldown signals to: {TEST_COOLDOWN_SIGNALS_PATH}")
    print(f"[INFO] Saved summary to: {SUMMARY_PATH}")
    print(f"[INFO] Saved per-fold report to: {PER_FOLD_PATH}")
    print(f"[INFO] Saved per-run report to: {PER_RUN_PATH}")
    print(f"[INFO] Saved direction report to: {DIRECTION_PATH}")
    print(f"[INFO] Saved cost sanity to: {COST_PATH}")

    print("\n[INFO] Fresh walk-forward summary:")
    print(
        summary[
            [
                "evaluation_method",
                "model",
                "n_signals_or_trades",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
                "up_share",
                "down_share",
                "folds_with_signals",
            ]
        ]
    )

    print("\n[INFO] Fresh-only walk-forward validation completed successfully.")


if __name__ == "__main__":
    main()