from pathlib import Path
import warnings

import matplotlib.pyplot as plt
import pandas as pd

from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
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

METRICS_PATH = TABLES_DIR / "multirun_advanced_ml_model_metrics_h50.csv"
SWEEP_PATH = TABLES_DIR / "multirun_advanced_ml_threshold_sweep_h50.csv"
TOP_CANDIDATES_PATH = TABLES_DIR / "multirun_advanced_ml_top_candidates_h50.csv"
FEATURE_IMPORTANCE_PATH = TABLES_DIR / "multirun_advanced_ml_feature_importance_h50.csv"

TARGET_HORIZON = 50

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5

TRAIN_RUN_FRACTION = 0.70

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
    0.95,
]

MIN_SIGNALS_FOR_TOP = 30
MIN_SIGNALS_FOR_RELIABLE = 50


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
    ordered_runs = (
        df.groupby("run_name")["event_time"]
        .min()
        .sort_values()
        .index
        .tolist()
    )

    return ordered_runs


def split_runs(ordered_runs: list[str]) -> tuple[list[str], list[str]]:
    split_idx = int(len(ordered_runs) * TRAIN_RUN_FRACTION)

    if split_idx <= 0 or split_idx >= len(ordered_runs):
        raise ValueError("Invalid train/test split. Need more runs.")

    train_runs = ordered_runs[:split_idx]
    test_runs = ordered_runs[split_idx:]

    return train_runs, test_runs


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


def build_model_registry() -> dict:
    registry = {
        "logit": {
            "available": True,
            "model": Pipeline(
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
            ),
        },
        "random_forest": {
            "available": True,
            "model": RandomForestClassifier(
                n_estimators=300,
                max_depth=3,
                min_samples_leaf=20,
                max_features="sqrt",
                random_state=42,
                n_jobs=-1,
            ),
        },
        "hist_gradient_boosting": {
            "available": True,
            "model": HistGradientBoostingClassifier(
                max_iter=150,
                max_leaf_nodes=7,
                learning_rate=0.05,
                l2_regularization=1.0,
                random_state=42,
            ),
        },
    }

    if XGBOOST_AVAILABLE:
        registry["xgboost"] = {
            "available": True,
            "model": XGBClassifier(
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
            ),
        }
    else:
        registry["xgboost"] = {
            "available": False,
            "model": None,
        }

    if LIGHTGBM_AVAILABLE:
        registry["lightgbm"] = {
            "available": True,
            "model": LGBMClassifier(
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
            ),
        }
    else:
        registry["lightgbm"] = {
            "available": False,
            "model": None,
        }

    return registry


def fit_model(
    algorithm: str,
    model,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    sample_weight,
):
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


def compute_classification_metrics(
    y_true,
    y_pred,
    y_score=None,
) -> dict:
    result = {
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "precision_up": precision_score(y_true, y_pred, pos_label=1, zero_division=0),
        "recall_up": recall_score(y_true, y_pred, pos_label=1, zero_division=0),
        "f1_up": f1_score(y_true, y_pred, pos_label=1, zero_division=0),
        "precision_down": precision_score(y_true, y_pred, pos_label=0, zero_division=0),
        "recall_down": recall_score(y_true, y_pred, pos_label=0, zero_division=0),
        "f1_down": f1_score(y_true, y_pred, pos_label=0, zero_division=0),
        "n_obs": len(y_true),
        "up_share": float(pd.Series(y_true).mean()),
    }

    if y_score is not None and len(set(y_true)) == 2:
        result["roc_auc"] = roc_auc_score(y_true, y_score)
    else:
        result["roc_auc"] = None

    return result


def train_and_predict(
    algorithm: str,
    model,
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: list[str],
) -> tuple[pd.DataFrame | None, dict | None, str | None]:
    validate_features(train, feature_cols)
    validate_features(test, feature_cols)

    train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
    test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

    if train_clean.empty or test_clean.empty:
        return None, None, "Empty train or test set after dropping missing values."

    if train_clean["target"].nunique() < 2:
        return None, None, "Train set contains only one class."

    if test_clean["target"].nunique() < 2:
        return None, None, "Test set contains only one class."

    X_train = train_clean[feature_cols]
    y_train = train_clean["target"]

    X_test = test_clean[feature_cols]
    y_test = test_clean["target"]

    sample_weight = compute_sample_weight(
        class_weight="balanced",
        y=y_train,
    )

    fitted_model = fit_model(
        algorithm=algorithm,
        model=model,
        X_train=X_train,
        y_train=y_train,
        sample_weight=sample_weight,
    )

    y_score = fitted_model.predict_proba(X_test)[:, 1]
    y_pred = (y_score >= 0.50).astype(int)

    predictions = test_clean[
        [
            "run_name",
            "row_in_run",
            "event_time",
            "target_horizon",
            "target_label",
            "target",
            "future_mid_return_bps",
        ]
    ].copy()

    predictions["predicted_proba_up"] = y_score
    predictions["y_pred_050"] = y_pred
    predictions["predicted_label_050"] = pd.Series(y_pred).map({0: "down", 1: "up"}).values

    metrics = compute_classification_metrics(
        y_true=y_test,
        y_pred=y_pred,
        y_score=y_score,
    )

    return predictions, metrics, None


def assign_threshold_signal(p_up: float, threshold: float) -> str:
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def summarize_thresholds(
    predictions: pd.DataFrame,
    model_name: str,
    algorithm: str,
    feature_set_name: str,
    n_features: int,
    total_model_test_obs: int,
) -> pd.DataFrame:
    rows = []

    for threshold in THRESHOLDS:
        temp = predictions.copy()
        temp["threshold"] = threshold

        temp["signal"] = temp["predicted_proba_up"].apply(
            lambda p: assign_threshold_signal(p, threshold)
        )

        temp["has_signal"] = temp["signal"].isin(["up", "down"])

        temp["is_correct_signal"] = (
            ((temp["signal"] == "up") & (temp["target_label"] == "up")) |
            ((temp["signal"] == "down") & (temp["target_label"] == "down"))
        )

        temp["signed_return_bps"] = None

        up_mask = temp["signal"] == "up"
        down_mask = temp["signal"] == "down"

        temp.loc[up_mask, "signed_return_bps"] = temp.loc[
            up_mask,
            "future_mid_return_bps",
        ]

        temp.loc[down_mask, "signed_return_bps"] = -temp.loc[
            down_mask,
            "future_mid_return_bps",
        ]

        temp["signed_return_bps"] = pd.to_numeric(
            temp["signed_return_bps"],
            errors="coerce",
        )

        for signal_group_name, subset in [
            ("up", temp[temp["signal"] == "up"]),
            ("down", temp[temp["signal"] == "down"]),
            ("both", temp[temp["has_signal"]]),
        ]:
            n_signals = len(subset)

            if n_signals == 0:
                rows.append(
                    {
                        "model": model_name,
                        "algorithm": algorithm,
                        "feature_set": feature_set_name,
                        "n_features": n_features,
                        "horizon": TARGET_HORIZON,
                        "threshold": threshold,
                        "signal_group": signal_group_name,
                        "n_signals": 0,
                        "coverage": 0.0,
                        "precision": None,
                        "mean_future_return_bps": None,
                        "median_future_return_bps": None,
                        "mean_signed_return_bps": None,
                        "median_signed_return_bps": None,
                        "positive_signed_return_share": None,
                        "break_even_round_trip_cost_bps": None,
                    }
                )
                continue

            rows.append(
                {
                    "model": model_name,
                    "algorithm": algorithm,
                    "feature_set": feature_set_name,
                    "n_features": n_features,
                    "horizon": TARGET_HORIZON,
                    "threshold": threshold,
                    "signal_group": signal_group_name,
                    "n_signals": n_signals,
                    "coverage": n_signals / total_model_test_obs,
                    "precision": subset["is_correct_signal"].mean(),
                    "mean_future_return_bps": subset["future_mid_return_bps"].mean(),
                    "median_future_return_bps": subset["future_mid_return_bps"].median(),
                    "mean_signed_return_bps": subset["signed_return_bps"].mean(),
                    "median_signed_return_bps": subset["signed_return_bps"].median(),
                    "positive_signed_return_share": (subset["signed_return_bps"] > 0).mean(),
                    "break_even_round_trip_cost_bps": subset["signed_return_bps"].mean(),
                }
            )

    return pd.DataFrame(rows)


def extract_feature_importance(
    model_name: str,
    algorithm: str,
    feature_set_name: str,
    feature_cols: list[str],
    fitted_model,
) -> list[dict]:
    rows = []

    if algorithm == "logit":
        lr = fitted_model.named_steps["logistic_regression"]
        values = lr.coef_[0]
        importance_type = "coefficient"

        for feature, value in zip(feature_cols, values):
            rows.append(
                {
                    "model": model_name,
                    "algorithm": algorithm,
                    "feature_set": feature_set_name,
                    "feature": feature,
                    "importance": value,
                    "abs_importance": abs(value),
                    "importance_type": importance_type,
                }
            )

        return rows

    if hasattr(fitted_model, "feature_importances_"):
        values = fitted_model.feature_importances_
        importance_type = "feature_importance"

        for feature, value in zip(feature_cols, values):
            rows.append(
                {
                    "model": model_name,
                    "algorithm": algorithm,
                    "feature_set": feature_set_name,
                    "feature": feature,
                    "importance": value,
                    "abs_importance": abs(value),
                    "importance_type": importance_type,
                }
            )

        return rows

    rows.append(
        {
            "model": model_name,
            "algorithm": algorithm,
            "feature_set": feature_set_name,
            "feature": None,
            "importance": None,
            "abs_importance": None,
            "importance_type": "not_available",
        }
    )

    return rows


def save_bar_plot_top_candidates(top_candidates: pd.DataFrame) -> None:
    if top_candidates.empty:
        return

    plot_df = top_candidates.head(15).copy()
    plot_df["label"] = (
        plot_df["algorithm"] + "\n" +
        plot_df["feature_set"] + "\n" +
        "thr=" + plot_df["threshold"].astype(str)
    )

    plt.figure(figsize=(10, 6))
    plt.bar(
        plot_df["label"],
        plot_df["break_even_round_trip_cost_bps"],
    )
    plt.xticks(rotation=75, ha="right")
    plt.ylabel("Break-even round-trip cost, bps")
    plt.title("Top advanced ML candidates, h=50")
    plt.tight_layout()

    out_path = FIGURES_DIR / "multirun_advanced_ml_top_candidates_h50.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_threshold_plot(sweep: pd.DataFrame, metric: str, ylabel: str) -> None:
    subset = sweep[
        (sweep["signal_group"] == "both") &
        (sweep["n_signals"] >= MIN_SIGNALS_FOR_TOP)
    ].copy()

    if subset.empty:
        return

    plt.figure(figsize=(10, 6))

    for model_name, model_df in subset.groupby("model", sort=True):
        model_df = model_df.sort_values("threshold")
        plt.plot(
            model_df["threshold"],
            model_df[metric],
            marker="o",
            label=model_name,
        )

    plt.title(f"{metric} by threshold, h={TARGET_HORIZON}")
    plt.xlabel("Threshold")
    plt.ylabel(ylabel)
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_advanced_ml_{metric}_by_threshold_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_all_plots(sweep: pd.DataFrame, top_candidates: pd.DataFrame) -> None:
    save_bar_plot_top_candidates(top_candidates)

    save_threshold_plot(
        sweep=sweep,
        metric="break_even_round_trip_cost_bps",
        ylabel="Break-even round-trip cost, bps",
    )

    save_threshold_plot(
        sweep=sweep,
        metric="precision",
        ylabel="Precision / hit rate",
    )

    save_threshold_plot(
        sweep=sweep,
        metric="coverage",
        ylabel="Coverage",
    )


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

    model_registry = build_model_registry()

    all_metrics_rows = []
    all_sweep_frames = []
    all_importance_rows = []

    for feature_set_name, feature_cols in FEATURE_SETS.items():
        print("\n" + "=" * 80)
        print(f"[INFO] Feature set: {feature_set_name}")
        print(f"[INFO] Number of features: {len(feature_cols)}")
        print("=" * 80)

        for algorithm, registry_item in model_registry.items():
            model_name = f"{algorithm}_{feature_set_name}"

            if not registry_item["available"]:
                print(f"[WARNING] Skipping {model_name}: package is not available.")

                all_metrics_rows.append(
                    {
                        "model": model_name,
                        "algorithm": algorithm,
                        "feature_set": feature_set_name,
                        "n_features": len(feature_cols),
                        "horizon": TARGET_HORIZON,
                        "status": "skipped",
                        "error": "package_not_available",
                    }
                )
                continue

            print("\n" + "-" * 80)
            print(f"[INFO] Training model: {model_name}")
            print("-" * 80)

            model = registry_item["model"]

            try:
                predictions, metrics, error = train_and_predict(
                    algorithm=algorithm,
                    model=model,
                    train=train,
                    test=test,
                    feature_cols=feature_cols,
                )

                if error is not None:
                    print(f"[WARNING] Failed {model_name}: {error}")

                    all_metrics_rows.append(
                        {
                            "model": model_name,
                            "algorithm": algorithm,
                            "feature_set": feature_set_name,
                            "n_features": len(feature_cols),
                            "horizon": TARGET_HORIZON,
                            "status": "failed",
                            "error": error,
                        }
                    )
                    continue

                all_metrics_rows.append(
                    {
                        "model": model_name,
                        "algorithm": algorithm,
                        "feature_set": feature_set_name,
                        "n_features": len(feature_cols),
                        "horizon": TARGET_HORIZON,
                        "status": "success",
                        "error": None,
                        **metrics,
                    }
                )

                sweep = summarize_thresholds(
                    predictions=predictions,
                    model_name=model_name,
                    algorithm=algorithm,
                    feature_set_name=feature_set_name,
                    n_features=len(feature_cols),
                    total_model_test_obs=len(predictions),
                )

                all_sweep_frames.append(sweep)

                importance_rows = extract_feature_importance(
                    model_name=model_name,
                    algorithm=algorithm,
                    feature_set_name=feature_set_name,
                    feature_cols=feature_cols,
                    fitted_model=model,
                )

                all_importance_rows.extend(importance_rows)

                print("[INFO] Threshold summary, both signals:")
                print(
                    sweep[sweep["signal_group"] == "both"][
                        [
                            "threshold",
                            "n_signals",
                            "coverage",
                            "precision",
                            "mean_signed_return_bps",
                            "break_even_round_trip_cost_bps",
                        ]
                    ]
                )

            except Exception as exc:
                print(f"[WARNING] Exception while training {model_name}: {exc}")

                all_metrics_rows.append(
                    {
                        "model": model_name,
                        "algorithm": algorithm,
                        "feature_set": feature_set_name,
                        "n_features": len(feature_cols),
                        "horizon": TARGET_HORIZON,
                        "status": "failed",
                        "error": str(exc),
                    }
                )

    metrics_df = pd.DataFrame(all_metrics_rows)
    metrics_df.to_csv(METRICS_PATH, index=False)

    if not all_sweep_frames:
        raise ValueError("No successful model results were produced.")

    sweep_df = pd.concat(all_sweep_frames, ignore_index=True)
    sweep_df.to_csv(SWEEP_PATH, index=False)

    importance_df = pd.DataFrame(all_importance_rows)

    if not importance_df.empty:
        importance_df = importance_df.sort_values(
            ["model", "abs_importance"],
            ascending=[True, False],
        )

    importance_df.to_csv(FEATURE_IMPORTANCE_PATH, index=False)

    top_candidates = (
        sweep_df[
            (sweep_df["signal_group"] == "both") &
            (sweep_df["n_signals"] >= MIN_SIGNALS_FOR_TOP)
        ]
        .sort_values(
            ["break_even_round_trip_cost_bps", "precision", "n_signals"],
            ascending=[False, False, False],
        )
        .head(50)
        .copy()
    )

    top_candidates.to_csv(TOP_CANDIDATES_PATH, index=False)

    save_all_plots(
        sweep=sweep_df,
        top_candidates=top_candidates,
    )

    print(f"\n[INFO] Saved model metrics to: {METRICS_PATH}")
    print(f"[INFO] Saved threshold sweep to: {SWEEP_PATH}")
    print(f"[INFO] Saved top candidates to: {TOP_CANDIDATES_PATH}")
    print(f"[INFO] Saved feature importance to: {FEATURE_IMPORTANCE_PATH}")

    print("\n[INFO] Top candidates:")
    print(
        top_candidates[
            [
                "model",
                "algorithm",
                "feature_set",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
            ]
        ]
    )

    reliable = top_candidates[
        top_candidates["n_signals"] >= MIN_SIGNALS_FOR_RELIABLE
    ].copy()

    print("\n[INFO] Reliable candidates, n_signals >= 50:")
    print(
        reliable[
            [
                "model",
                "algorithm",
                "feature_set",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "break_even_round_trip_cost_bps",
            ]
        ]
    )

    print("\n[INFO] Advanced ML benchmark completed successfully.")


if __name__ == "__main__":
    main()