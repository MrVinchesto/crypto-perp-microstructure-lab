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


INPUT_PATH = Path("data/processed/dynamic_features_all.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

TARGET_HORIZON = 50
PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5
TRAIN_RUN_FRACTION = 0.70

SELECTED_SIGNALS_PATH = TABLES_DIR / "multirun_advanced_ml_validation_selected_signals_h50.csv"
PER_RUN_PATH = TABLES_DIR / "multirun_advanced_ml_validation_per_run_h50.csv"
DIRECTION_PATH = TABLES_DIR / "multirun_advanced_ml_validation_direction_h50.csv"
COST_SANITY_PATH = TABLES_DIR / "multirun_advanced_ml_validation_cost_sanity_h50.csv"
CONCENTRATION_PATH = TABLES_DIR / "multirun_advanced_ml_validation_concentration_h50.csv"
SUMMARY_PATH = TABLES_DIR / "multirun_advanced_ml_validation_summary_h50.csv"

COST_SCENARIOS = [
    {"cost_scenario": "no_cost", "round_trip_cost_bps": 0.0},
    {"cost_scenario": "very_low_cost", "round_trip_cost_bps": 1.0},
    {"cost_scenario": "low_cost", "round_trip_cost_bps": 2.0},
    {"cost_scenario": "medium_cost", "round_trip_cost_bps": 3.0},
    {"cost_scenario": "near_current_best_edge", "round_trip_cost_bps": 3.5},
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
    "static_full": STATIC_FULL_FEATURES,
    "dynamic_compact": STATIC_SIGNAL_FEATURES + DYNAMIC_FEATURES,
    "dynamic_full": STATIC_FULL_FEATURES + DYNAMIC_FEATURES,
}


SELECTED_CONFIGS = [
    {
        "model": "xgboost_dynamic_full",
        "algorithm": "xgboost",
        "feature_set": "dynamic_full",
        "threshold": 0.75,
    },
    {
        "model": "xgboost_dynamic_compact",
        "algorithm": "xgboost",
        "feature_set": "dynamic_compact",
        "threshold": 0.75,
    },
    {
        "model": "lightgbm_dynamic_compact",
        "algorithm": "lightgbm",
        "feature_set": "dynamic_compact",
        "threshold": 0.75,
    },
    {
        "model": "lightgbm_dynamic_full",
        "algorithm": "lightgbm",
        "feature_set": "dynamic_full",
        "threshold": 0.75,
    },
    {
        "model": "hist_gradient_boosting_static_full",
        "algorithm": "hist_gradient_boosting",
        "feature_set": "static_full",
        "threshold": 0.90,
    },
    {
        "model": "hist_gradient_boosting_dynamic_full",
        "algorithm": "hist_gradient_boosting",
        "feature_set": "dynamic_full",
        "threshold": 0.85,
    },
    {
        "model": "logit_dynamic_compact",
        "algorithm": "logit",
        "feature_set": "dynamic_compact",
        "threshold": 0.65,
    },
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_features() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/run_dynamic_feature_sweep.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def get_ordered_runs(df: pd.DataFrame) -> list[str]:
    return (
        df.groupby("run_name")["event_time"]
        .min()
        .sort_values()
        .index
        .tolist()
    )


def split_runs(ordered_runs: list[str]) -> tuple[list[str], list[str]]:
    split_idx = int(len(ordered_runs) * TRAIN_RUN_FRACTION)

    if split_idx <= 0 or split_idx >= len(ordered_runs):
        raise ValueError("Invalid train/test split. Need more runs.")

    return ordered_runs[:split_idx], ordered_runs[split_idx:]


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
        temp["future_mid_return"] = temp["future_mid_price"] / temp["mid_price"] - 1
        temp["future_mid_return_bps"] = temp["future_mid_return"] * 10000

        temp["target_label"] = temp["future_mid_change_ticks"].apply(label_from_tick_change)

        frames.append(temp)

    labeled = pd.concat(frames, ignore_index=True)

    labeled = labeled[labeled["target_label"].isin(["down", "up"])].copy()
    labeled["target"] = labeled["target_label"].map({"down": 0, "up": 1}).astype(int)

    return labeled


def validate_features(df: pd.DataFrame, feature_cols: list[str]) -> None:
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


def assign_threshold_signal(p_up: float, threshold: float) -> str:
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def train_predict_select_signals(
    config: dict,
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> pd.DataFrame | None:
    model_name = config["model"]
    algorithm = config["algorithm"]
    feature_set = config["feature_set"]
    threshold = config["threshold"]

    feature_cols = FEATURE_SETS[feature_set]

    validate_features(train, feature_cols)
    validate_features(test, feature_cols)

    model = build_model(algorithm)

    if model is None:
        print(f"[WARNING] Skipping {model_name}: package is not available.")
        return None

    train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
    test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

    if train_clean.empty or test_clean.empty:
        print(f"[WARNING] Skipping {model_name}: empty train or test set.")
        return None

    if train_clean["target"].nunique() < 2:
        print(f"[WARNING] Skipping {model_name}: train set contains only one class.")
        return None

    if test_clean["target"].nunique() < 2:
        print(f"[WARNING] Skipping {model_name}: test set contains only one class.")
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
        lambda p: assign_threshold_signal(p, threshold)
    )

    selected = test_clean[test_clean["signal"].isin(["up", "down"])].copy()

    if selected.empty:
        print(f"[WARNING] No selected signals for {model_name}.")
        return selected

    selected["model"] = model_name
    selected["algorithm"] = algorithm
    selected["feature_set"] = feature_set
    selected["threshold"] = threshold
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
        "model",
        "algorithm",
        "feature_set",
        "threshold",
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


def summarize_overall(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for model_name, group in selected.groupby("model", sort=True):
        rows.append(
            {
                "model": model_name,
                "algorithm": group["algorithm"].iloc[0],
                "feature_set": group["feature_set"].iloc[0],
                "threshold": group["threshold"].iloc[0],
                "n_features": group["n_features"].iloc[0],
                "n_signals": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_signal_share": (group["signal"] == "up").mean(),
                "down_signal_share": (group["signal"] == "down").mean(),
                "break_even_round_trip_cost_bps": group["signed_return_bps"].mean(),
            }
        )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        ["break_even_round_trip_cost_bps", "precision", "n_signals"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    return result


def summarize_by_run(selected: pd.DataFrame, test_runs: list[str]) -> pd.DataFrame:
    rows = []

    for model_name, model_df in selected.groupby("model", sort=True):
        for run_name in test_runs:
            subset = model_df[model_df["run_name"] == run_name].copy()

            if subset.empty:
                rows.append(
                    {
                        "model": model_name,
                        "run_name": run_name,
                        "n_signals": 0,
                        "signal_share_within_model": 0.0,
                        "precision": None,
                        "mean_signed_return_bps": None,
                        "median_signed_return_bps": None,
                        "positive_signed_return_share": None,
                        "up_signals": 0,
                        "down_signals": 0,
                        "total_signed_return_bps": 0.0,
                    }
                )
                continue

            rows.append(
                {
                    "model": model_name,
                    "run_name": run_name,
                    "n_signals": len(subset),
                    "signal_share_within_model": len(subset) / len(model_df),
                    "precision": subset["is_correct_signal"].mean(),
                    "mean_signed_return_bps": subset["signed_return_bps"].mean(),
                    "median_signed_return_bps": subset["signed_return_bps"].median(),
                    "positive_signed_return_share": subset["is_positive_signed_return"].mean(),
                    "up_signals": int((subset["signal"] == "up").sum()),
                    "down_signals": int((subset["signal"] == "down").sum()),
                    "total_signed_return_bps": subset["signed_return_bps"].sum(),
                }
            )

    result = pd.DataFrame(rows)

    return result.sort_values(["model", "run_name"]).reset_index(drop=True)


def summarize_by_direction(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (model_name, signal), subset in selected.groupby(["model", "signal"], sort=True):
        rows.append(
            {
                "model": model_name,
                "signal": signal,
                "n_signals": len(subset),
                "precision": subset["is_correct_signal"].mean(),
                "mean_signed_return_bps": subset["signed_return_bps"].mean(),
                "median_signed_return_bps": subset["signed_return_bps"].median(),
                "positive_signed_return_share": subset["is_positive_signed_return"].mean(),
                "mean_future_return_bps": subset["future_mid_return_bps"].mean(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(["model", "signal"]).reset_index(drop=True)


def summarize_cost_scenarios(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for model_name, group in selected.groupby("model", sort=True):
        for scenario in COST_SCENARIOS:
            cost_name = scenario["cost_scenario"]
            cost_bps = scenario["round_trip_cost_bps"]

            temp = group.copy()
            temp["net_signed_return_bps"] = temp["signed_return_bps"] - cost_bps
            temp["is_net_positive"] = temp["net_signed_return_bps"] > 0

            rows.append(
                {
                    "model": model_name,
                    "cost_scenario": cost_name,
                    "round_trip_cost_bps": cost_bps,
                    "n_signals": len(temp),
                    "gross_mean_signed_return_bps": temp["signed_return_bps"].mean(),
                    "net_mean_signed_return_bps": temp["net_signed_return_bps"].mean(),
                    "net_median_signed_return_bps": temp["net_signed_return_bps"].median(),
                    "net_positive_share": temp["is_net_positive"].mean(),
                }
            )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["model", "round_trip_cost_bps"]
    ).reset_index(drop=True)


def summarize_concentration(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for model_name, group in per_run.groupby("model", sort=True):
        nonzero = group[group["n_signals"] > 0].copy()

        if nonzero.empty:
            rows.append(
                {
                    "model": model_name,
                    "test_runs_with_signals": 0,
                    "max_run_signal_share": None,
                    "positive_run_share": None,
                    "min_run_mean_signed_return_bps": None,
                    "max_run_mean_signed_return_bps": None,
                    "all_runs_positive": False,
                }
            )
            continue

        positive_run_share = (nonzero["mean_signed_return_bps"] > 0).mean()

        rows.append(
            {
                "model": model_name,
                "test_runs_with_signals": len(nonzero),
                "max_run_signal_share": nonzero["signal_share_within_model"].max(),
                "positive_run_share": positive_run_share,
                "min_run_mean_signed_return_bps": nonzero["mean_signed_return_bps"].min(),
                "max_run_mean_signed_return_bps": nonzero["mean_signed_return_bps"].max(),
                "all_runs_positive": bool((nonzero["mean_signed_return_bps"] > 0).all()),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["all_runs_positive", "positive_run_share", "min_run_mean_signed_return_bps"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def save_cost_plot(cost_sanity: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 6))

    for model_name, group in cost_sanity.groupby("model", sort=True):
        group = group.sort_values("round_trip_cost_bps")
        plt.plot(
            group["round_trip_cost_bps"],
            group["net_mean_signed_return_bps"],
            marker="o",
            label=model_name,
        )

    plt.axhline(0)
    plt.title("Net mean signed return under cost scenarios, h=50")
    plt.xlabel("Round-trip cost, bps")
    plt.ylabel("Net mean signed return, bps")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / "multirun_advanced_ml_validation_cost_sanity_h50.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved cost plot to: {out_path}")


def save_per_run_plot(per_run: pd.DataFrame) -> None:
    models = per_run["model"].unique().tolist()

    for model_name in models:
        subset = per_run[per_run["model"] == model_name].copy()

        plt.figure(figsize=(8, 5))
        plt.bar(
            subset["run_name"],
            subset["mean_signed_return_bps"].fillna(0.0),
        )
        plt.axhline(0)
        plt.xticks(rotation=45, ha="right")
        plt.title(f"Per-run mean signed return: {model_name}")
        plt.xlabel("Test run")
        plt.ylabel("Mean signed return, bps")
        plt.tight_layout()

        safe_model_name = model_name.replace("/", "_")
        out_path = FIGURES_DIR / f"multirun_advanced_ml_validation_per_run_{safe_model_name}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved per-run plot to: {out_path}")


def save_signal_count_plot(per_run: pd.DataFrame) -> None:
    models = per_run["model"].unique().tolist()

    for model_name in models:
        subset = per_run[per_run["model"] == model_name].copy()

        plt.figure(figsize=(8, 5))
        plt.bar(
            subset["run_name"],
            subset["n_signals"],
        )
        plt.xticks(rotation=45, ha="right")
        plt.title(f"Per-run signal count: {model_name}")
        plt.xlabel("Test run")
        plt.ylabel("Number of signals")
        plt.tight_layout()

        safe_model_name = model_name.replace("/", "_")
        out_path = FIGURES_DIR / f"multirun_advanced_ml_validation_signal_count_{safe_model_name}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved signal-count plot to: {out_path}")


def save_all_plots(cost_sanity: pd.DataFrame, per_run: pd.DataFrame) -> None:
    save_cost_plot(cost_sanity)
    save_per_run_plot(per_run)
    save_signal_count_plot(per_run)


def print_split_info(train_runs: list[str], test_runs: list[str]) -> None:
    print("\n[INFO] Train runs:")
    for run in train_runs:
        print(f"  {run}")

    print("\n[INFO] Test runs:")
    for run in test_runs:
        print(f"  {run}")


def main() -> None:
    ensure_output_dirs()

    print(f"[INFO] XGBoost available: {XGBOOST_AVAILABLE}")
    print(f"[INFO] LightGBM available: {LIGHTGBM_AVAILABLE}")

    features = load_features()
    ordered_runs = get_ordered_runs(features)
    train_runs, test_runs = split_runs(ordered_runs)

    print_split_info(train_runs, test_runs)

    dataset = build_dataset_for_horizon(features, horizon=TARGET_HORIZON)

    train = dataset[dataset["run_name"].isin(train_runs)].copy()
    test = dataset[dataset["run_name"].isin(test_runs)].copy()

    print(f"\n[INFO] Horizon: {TARGET_HORIZON}")
    print(f"[INFO] Dataset rows: {len(dataset)}")
    print(f"[INFO] Train rows: {len(train)}")
    print(f"[INFO] Test rows: {len(test)}")

    print("\n[INFO] Train class distribution:")
    print(train["target_label"].value_counts(dropna=False))

    print("\n[INFO] Test class distribution:")
    print(test["target_label"].value_counts(dropna=False))

    selected_frames = []

    for config in SELECTED_CONFIGS:
        print("\n" + "=" * 80)
        print(f"[INFO] Validating {config['model']}")
        print(f"[INFO] Algorithm: {config['algorithm']}")
        print(f"[INFO] Feature set: {config['feature_set']}")
        print(f"[INFO] Threshold: {config['threshold']}")
        print("=" * 80)

        selected = train_predict_select_signals(
            config=config,
            train=train,
            test=test,
        )

        if selected is None or selected.empty:
            print(f"[WARNING] No output for {config['model']}")
            continue

        selected_frames.append(selected)

        print(
            selected[
                [
                    "run_name",
                    "signal",
                    "target_label",
                    "predicted_proba_up",
                    "signed_return_bps",
                ]
            ].head()
        )

        print(f"[INFO] Selected signals: {len(selected)}")
        print(f"[INFO] Precision: {selected['is_correct_signal'].mean():.4f}")
        print(f"[INFO] Mean signed return: {selected['signed_return_bps'].mean():.4f} bps")

    if not selected_frames:
        raise ValueError("No selected signals were produced.")

    selected_all = pd.concat(selected_frames, ignore_index=True)

    summary = summarize_overall(selected_all)
    per_run = summarize_by_run(selected_all, test_runs=test_runs)
    direction = summarize_by_direction(selected_all)
    cost_sanity = summarize_cost_scenarios(selected_all)
    concentration = summarize_concentration(per_run)

    selected_all.to_csv(SELECTED_SIGNALS_PATH, index=False)
    summary.to_csv(SUMMARY_PATH, index=False)
    per_run.to_csv(PER_RUN_PATH, index=False)
    direction.to_csv(DIRECTION_PATH, index=False)
    cost_sanity.to_csv(COST_SANITY_PATH, index=False)
    concentration.to_csv(CONCENTRATION_PATH, index=False)

    save_all_plots(
        cost_sanity=cost_sanity,
        per_run=per_run,
    )

    print(f"\n[INFO] Saved selected signals to: {SELECTED_SIGNALS_PATH}")
    print(f"[INFO] Saved summary to: {SUMMARY_PATH}")
    print(f"[INFO] Saved per-run validation to: {PER_RUN_PATH}")
    print(f"[INFO] Saved direction validation to: {DIRECTION_PATH}")
    print(f"[INFO] Saved cost sanity to: {COST_SANITY_PATH}")
    print(f"[INFO] Saved concentration report to: {CONCENTRATION_PATH}")

    print("\n[INFO] Overall summary:")
    print(
        summary[
            [
                "model",
                "n_signals",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
                "up_signal_share",
                "down_signal_share",
            ]
        ]
    )

    print("\n[INFO] Concentration summary:")
    print(concentration)

    print("\n[INFO] Cost sanity, first rows:")
    print(
        cost_sanity[
            [
                "model",
                "cost_scenario",
                "round_trip_cost_bps",
                "gross_mean_signed_return_bps",
                "net_mean_signed_return_bps",
                "net_positive_share",
            ]
        ].head(30)
    )

    print("\n[INFO] Advanced ML validation completed successfully.")


if __name__ == "__main__":
    main()