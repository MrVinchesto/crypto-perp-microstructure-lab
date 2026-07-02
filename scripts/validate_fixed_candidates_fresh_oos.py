from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import pandas as pd

from sklearn.ensemble import HistGradientBoostingClassifier
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

RAW_SIGNALS_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_raw_signals_h50.csv"
COOLDOWN_SIGNALS_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_cooldown_signals_h50.csv"
SUMMARY_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_summary_h50.csv"
PER_RUN_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_per_run_h50.csv"
DIRECTION_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_direction_h50.csv"
COST_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_cost_sanity_h50.csv"
CONCENTRATION_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_concentration_h50.csv"
SPLIT_INFO_PATH = TABLES_DIR / "fresh_oos_fixed_candidate_split_info_h50.csv"

COST_SCENARIOS = [
    {"cost_scenario": "no_cost", "round_trip_cost_bps": 0.0},
    {"cost_scenario": "very_low_cost", "round_trip_cost_bps": 1.0},
    {"cost_scenario": "low_cost", "round_trip_cost_bps": 2.0},
    {"cost_scenario": "medium_cost", "round_trip_cost_bps": 3.0},
    {"cost_scenario": "high_cost", "round_trip_cost_bps": 4.0},
    {"cost_scenario": "expensive_taker_like", "round_trip_cost_bps": 5.0},
    {"cost_scenario": "very_expensive_round_trip", "round_trip_cost_bps": 9.0},
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

STATIC_SIGNAL_FEATURES = [
    "imbalance_5",
    "microprice_deviation_bps",
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


FIXED_CANDIDATES = [
    {
        "model": "lightgbm_dynamic_compact",
        "algorithm": "lightgbm",
        "feature_set": "dynamic_compact",
        "threshold": 0.75,
        "role": "candidate_a",
    },
    {
        "model": "xgboost_dynamic_full",
        "algorithm": "xgboost",
        "feature_set": "dynamic_full",
        "threshold": 0.75,
        "role": "candidate_b",
    },
    {
        "model": "hist_gradient_boosting_dynamic_full",
        "algorithm": "hist_gradient_boosting",
        "feature_set": "dynamic_full",
        "threshold": 0.85,
        "role": "candidate_c",
    },
    {
        "model": "logit_dynamic_compact",
        "algorithm": "logit",
        "feature_set": "dynamic_compact",
        "threshold": 0.65,
        "role": "reference_baseline",
    },
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_fresh_runs() -> list[str]:
    if not FRESH_LOG_PATH.exists():
        raise FileNotFoundError(f"Fresh log not found: {FRESH_LOG_PATH}")

    log = pd.read_csv(FRESH_LOG_PATH)

    required_cols = ["collection_batch", "status", "run_name"]

    missing = [col for col in required_cols if col not in log.columns]

    if missing:
        raise ValueError(f"Fresh log missing columns: {missing}")

    fresh = log[
        (log["collection_batch"] == "fresh_oos_day20") &
        (log["status"] == "success")
    ].copy()

    fresh_runs = fresh["run_name"].dropna().astype(str).tolist()

    if not fresh_runs:
        raise ValueError("No successful fresh OOS runs found in fresh log.")

    return fresh_runs


def load_features() -> pd.DataFrame:
    if not FEATURES_PATH.exists():
        raise FileNotFoundError(f"Features file not found: {FEATURES_PATH}")

    df = pd.read_csv(FEATURES_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
        "best_bid",
        "best_ask",
        "quote_changed",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Features file missing required columns: {missing}")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def add_microprice_deviation_if_needed(df: pd.DataFrame) -> pd.DataFrame:
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


def add_dynamic_features(df: pd.DataFrame) -> pd.DataFrame:
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

    result = pd.concat(frames, ignore_index=True)

    return result


def label_from_tick_change(tick_change: float) -> str:
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > DEAD_ZONE_TICKS:
        return "up"

    if tick_change < -DEAD_ZONE_TICKS:
        return "down"

    return "flat"


def build_dataset_for_horizon(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
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


def validate_feature_columns(df: pd.DataFrame, feature_cols: list[str]) -> None:
    missing = [col for col in feature_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing feature columns: {missing}")


def build_model(algorithm: str):
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

    if algorithm == "hist_gradient_boosting":
        return HistGradientBoostingClassifier(
            max_iter=150,
            max_leaf_nodes=7,
            learning_rate=0.05,
            l2_regularization=1.0,
            random_state=42,
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

    raise ValueError(f"Unknown algorithm: {algorithm}")


def fit_model(algorithm: str, model, X_train, y_train, sample_weight):
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


def assign_signal(p_up: float, threshold: float) -> str:
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def train_and_select_raw_signals(
    config: dict,
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> pd.DataFrame | None:
    model_name = config["model"]
    algorithm = config["algorithm"]
    feature_set = config["feature_set"]
    threshold = config["threshold"]
    role = config["role"]

    feature_cols = FEATURE_SETS[feature_set]

    validate_feature_columns(train, feature_cols)
    validate_feature_columns(test, feature_cols)

    model = build_model(algorithm)

    if model is None:
        print(f"[WARNING] Skipping {model_name}: package unavailable.")
        return None

    train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
    test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

    if train_clean.empty or test_clean.empty:
        print(f"[WARNING] Skipping {model_name}: empty train/test after dropna.")
        return None

    if train_clean["target"].nunique() < 2:
        print(f"[WARNING] Skipping {model_name}: train has only one class.")
        return None

    if test_clean["target"].nunique() < 2:
        print(f"[WARNING] Skipping {model_name}: test has only one class.")
        return None

    X_train = train_clean[feature_cols]
    y_train = train_clean["target"]

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

    test_clean = test_clean.copy()
    test_clean["predicted_proba_up"] = model.predict_proba(X_test)[:, 1]
    test_clean["signal"] = test_clean["predicted_proba_up"].apply(
        lambda p: assign_signal(p, threshold)
    )

    selected = test_clean[test_clean["signal"].isin(["up", "down"])].copy()

    if selected.empty:
        print(f"[INFO] No raw selected signals for {model_name}.")
        return selected

    selected["evaluation_method"] = "raw_selected_signal"
    selected["model"] = model_name
    selected["algorithm"] = algorithm
    selected["feature_set"] = feature_set
    selected["threshold"] = threshold
    selected["role"] = role
    selected["n_features"] = len(feature_cols)

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
        "model",
        "algorithm",
        "feature_set",
        "threshold",
        "role",
        "n_features",
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


def apply_cooldown(raw_signals: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if raw_signals.empty:
        return raw_signals.copy()

    group_cols = ["model", "run_name"]

    for (model_name, run_name), group in raw_signals.groupby(group_cols, sort=True):
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
            item["evaluation_method"] = "cooldown_first_signal"
            item["cooldown_trade_id"] = trade_id

            rows.append(item)

    cooldown = pd.DataFrame(rows)

    if cooldown.empty:
        return cooldown

    cooldown = cooldown.sort_values(
        ["model", "run_name", "row_in_run"]
    ).reset_index(drop=True)

    return cooldown


def summarize_overall(evaluation: pd.DataFrame) -> pd.DataFrame:
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
                "threshold": group["threshold"].iloc[0],
                "role": group["role"].iloc[0],
                "n_signals_or_trades": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_share": (group["signal"] == "up").mean(),
                "down_share": (group["signal"] == "down").mean(),
                "break_even_round_trip_cost_bps": group["signed_return_bps"].mean(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "break_even_round_trip_cost_bps", "precision"],
        ascending=[True, False, False],
    ).reset_index(drop=True)


def summarize_per_run(evaluation: pd.DataFrame) -> pd.DataFrame:
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
                "total_signed_return_bps": group["signed_return_bps"].sum(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "run_name"]
    ).reset_index(drop=True)


def summarize_direction(evaluation: pd.DataFrame) -> pd.DataFrame:
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

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "signal"]
    ).reset_index(drop=True)


def summarize_cost_scenarios(evaluation: pd.DataFrame) -> pd.DataFrame:
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

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "round_trip_cost_bps"]
    ).reset_index(drop=True)


def summarize_concentration(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name), group in per_run.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        total = group["n_signals_or_trades"].sum()

        if total == 0:
            continue

        temp = group.copy()
        temp["run_share"] = temp["n_signals_or_trades"] / total

        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "test_runs_with_signals_or_trades": int(
                    (temp["n_signals_or_trades"] > 0).sum()
                ),
                "max_run_share": temp["run_share"].max(),
                "positive_run_share": (temp["mean_signed_return_bps"] > 0).mean(),
                "min_run_mean_signed_return_bps": temp["mean_signed_return_bps"].min(),
                "max_run_mean_signed_return_bps": temp["mean_signed_return_bps"].max(),
                "all_runs_positive": bool((temp["mean_signed_return_bps"] > 0).all()),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        [
            "evaluation_method",
            "all_runs_positive",
            "positive_run_share",
            "min_run_mean_signed_return_bps",
        ],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)


def save_summary_plot(summary: pd.DataFrame) -> None:
    for method, group in summary.groupby("evaluation_method", sort=True):
        group = group.sort_values("break_even_round_trip_cost_bps", ascending=False)

        plt.figure(figsize=(10, 6))
        plt.bar(
            group["model"],
            group["break_even_round_trip_cost_bps"],
        )
        plt.axhline(0)
        plt.xticks(rotation=75, ha="right")
        plt.title(f"Fresh OOS fixed-candidate break-even: {method}")
        plt.ylabel("Break-even round-trip cost, bps")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"fresh_oos_fixed_candidate_summary_{method}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved summary plot to: {out_path}")


def save_cost_plot(cost: pd.DataFrame) -> None:
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
        plt.title(f"Fresh OOS cost sanity: {method}")
        plt.xlabel("Round-trip cost, bps")
        plt.ylabel("Net mean signed return, bps")
        plt.legend(fontsize=7)
        plt.tight_layout()

        out_path = FIGURES_DIR / f"fresh_oos_fixed_candidate_cost_sanity_{method}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved cost plot to: {out_path}")


def save_all_plots(summary: pd.DataFrame, cost: pd.DataFrame) -> None:
    save_summary_plot(summary)
    save_cost_plot(cost)


def main() -> None:
    ensure_output_dirs()

    print(f"[INFO] XGBoost available: {XGBOOST_AVAILABLE}")
    print(f"[INFO] LightGBM available: {LIGHTGBM_AVAILABLE}")

    fresh_runs = load_fresh_runs()
    print(f"[INFO] Fresh OOS runs from Day 20: {len(fresh_runs)}")
    for run in fresh_runs:
        print(f"  {run}")

    features = load_features()
    features = add_dynamic_features(features)

    all_runs = sorted(features["run_name"].unique().tolist())

    discovery_runs = [run for run in all_runs if run not in fresh_runs]

    if not discovery_runs:
        raise ValueError("No discovery/training runs found.")

    split_info_rows = []

    for run in discovery_runs:
        split_info_rows.append(
            {
                "run_name": run,
                "split": "discovery_train",
            }
        )

    for run in fresh_runs:
        split_info_rows.append(
            {
                "run_name": run,
                "split": "fresh_oos_test",
            }
        )

    split_info = pd.DataFrame(split_info_rows)
    split_info.to_csv(SPLIT_INFO_PATH, index=False)

    print(f"\n[INFO] Discovery train runs: {len(discovery_runs)}")
    print(f"[INFO] Fresh OOS test runs: {len(fresh_runs)}")
    print(f"[INFO] Split info saved to: {SPLIT_INFO_PATH}")

    dataset = build_dataset_for_horizon(features, horizon=TARGET_HORIZON)

    train = dataset[dataset["run_name"].isin(discovery_runs)].copy()
    test = dataset[dataset["run_name"].isin(fresh_runs)].copy()

    print(f"\n[INFO] Horizon: {TARGET_HORIZON}")
    print(f"[INFO] Train rows: {len(train)}")
    print(f"[INFO] Test rows: {len(test)}")

    print("\n[INFO] Train target distribution:")
    print(train["target_label"].value_counts(dropna=False))

    print("\n[INFO] Fresh OOS target distribution:")
    print(test["target_label"].value_counts(dropna=False))

    raw_frames = []

    for config in FIXED_CANDIDATES:
        print("\n" + "=" * 80)
        print(f"[INFO] Fixed candidate: {config['model']}")
        print(f"[INFO] Algorithm: {config['algorithm']}")
        print(f"[INFO] Feature set: {config['feature_set']}")
        print(f"[INFO] Threshold: {config['threshold']}")
        print(f"[INFO] Role: {config['role']}")
        print("=" * 80)

        raw = train_and_select_raw_signals(
            config=config,
            train=train,
            test=test,
        )

        if raw is None or raw.empty:
            print(f"[INFO] No raw signals for {config['model']}.")
            continue

        raw_frames.append(raw)

        print(f"[INFO] Raw selected signals: {len(raw)}")
        print(f"[INFO] Raw precision: {raw['is_correct_signal'].mean():.4f}")
        print(f"[INFO] Raw mean signed return: {raw['signed_return_bps'].mean():.4f} bps")

    if not raw_frames:
        raise ValueError("No raw signals produced by fixed candidates on fresh OOS data.")

    raw_all = pd.concat(raw_frames, ignore_index=True)
    cooldown_all = apply_cooldown(raw_all)

    raw_all.to_csv(RAW_SIGNALS_PATH, index=False)
    cooldown_all.to_csv(COOLDOWN_SIGNALS_PATH, index=False)

    evaluation_all = pd.concat(
        [raw_all, cooldown_all],
        ignore_index=True,
        sort=False,
    )

    summary = summarize_overall(evaluation_all)
    per_run = summarize_per_run(evaluation_all)
    direction = summarize_direction(evaluation_all)
    cost = summarize_cost_scenarios(evaluation_all)
    concentration = summarize_concentration(per_run)

    summary.to_csv(SUMMARY_PATH, index=False)
    per_run.to_csv(PER_RUN_PATH, index=False)
    direction.to_csv(DIRECTION_PATH, index=False)
    cost.to_csv(COST_PATH, index=False)
    concentration.to_csv(CONCENTRATION_PATH, index=False)

    save_all_plots(summary=summary, cost=cost)

    print(f"\n[INFO] Saved raw signals to: {RAW_SIGNALS_PATH}")
    print(f"[INFO] Saved cooldown signals to: {COOLDOWN_SIGNALS_PATH}")
    print(f"[INFO] Saved summary to: {SUMMARY_PATH}")
    print(f"[INFO] Saved per-run to: {PER_RUN_PATH}")
    print(f"[INFO] Saved direction to: {DIRECTION_PATH}")
    print(f"[INFO] Saved cost sanity to: {COST_PATH}")
    print(f"[INFO] Saved concentration to: {CONCENTRATION_PATH}")

    print("\n[INFO] Fresh OOS fixed-candidate summary:")
    print(
        summary[
            [
                "evaluation_method",
                "model",
                "role",
                "n_signals_or_trades",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
                "up_share",
                "down_share",
            ]
        ]
    )

    print("\n[INFO] Fresh OOS fixed-candidate validation completed successfully.")


if __name__ == "__main__":
    main()