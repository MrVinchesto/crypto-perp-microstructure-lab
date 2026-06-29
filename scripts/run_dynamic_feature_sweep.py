from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

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


INPUT_PATH = Path("data/processed/basic_features_all.csv")
DYNAMIC_FEATURES_PATH = Path("data/processed/dynamic_features_all.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

METRICS_PATH = TABLES_DIR / "multirun_dynamic_feature_model_metrics.csv"
SWEEP_PATH = TABLES_DIR / "multirun_dynamic_feature_threshold_sweep.csv"
TOP_CANDIDATES_PATH = TABLES_DIR / "multirun_dynamic_feature_top_candidates.csv"
BEST_BY_HORIZON_PATH = TABLES_DIR / "multirun_dynamic_feature_best_by_horizon.csv"
FEATURE_MISSINGNESS_PATH = TABLES_DIR / "multirun_dynamic_feature_missingness.csv"

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5

HORIZONS = [5, 10, 20, 50, 100]
THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]

TRAIN_RUN_FRACTION = 0.70
MIN_SIGNALS_FOR_TOP = 30
MIN_SIGNALS_FOR_BEST_BY_HORIZON = 50


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
    "logit_signal_static": STATIC_SIGNAL_FEATURES,
    "logit_full_static": STATIC_FULL_FEATURES,
    "logit_dynamic_compact": STATIC_SIGNAL_FEATURES + DYNAMIC_FEATURES,
    "logit_dynamic_full": STATIC_FULL_FEATURES + DYNAMIC_FEATURES,
}


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    DYNAMIC_FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_base_features() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_basic_features_all_runs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
        "spread_bps",
        "best_bid_qty",
        "best_ask_qty",
        "bid_depth_5",
        "ask_depth_5",
        "imbalance_5",
        "microprice_deviation_bps",
        "mid_return_bps",
        "quote_changed",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns in base features: {missing}")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def add_change_features(
    run_df: pd.DataFrame,
    column: str,
    lags: list[int],
) -> pd.DataFrame:
    for lag in lags:
        run_df[f"{column}_change_{lag}e"] = run_df[column] - run_df[column].shift(lag)

    return run_df


def add_lag_features(
    run_df: pd.DataFrame,
    column: str,
    lags: list[int],
) -> pd.DataFrame:
    for lag in lags:
        run_df[f"{column}_lag_{lag}e"] = run_df[column].shift(lag)

    return run_df


def add_rolling_features(
    run_df: pd.DataFrame,
    column: str,
    windows: list[int],
    add_mean: bool = True,
    add_std: bool = False,
    add_sum: bool = False,
) -> pd.DataFrame:
    for window in windows:
        rolling_obj = run_df[column].rolling(window=window, min_periods=window)

        if add_mean:
            run_df[f"{column}_rolling_mean_{window}e"] = rolling_obj.mean()

        if add_std:
            run_df[f"{column}_rolling_std_{window}e"] = rolling_obj.std()

        if add_sum:
            run_df[f"{column}_rolling_sum_{window}e"] = rolling_obj.sum()

    return run_df


def add_dynamic_features(df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        temp = run_df.copy()
        temp = temp.sort_values("event_time").reset_index(drop=True)

        temp = add_change_features(
            temp,
            column="imbalance_5",
            lags=[1, 5, 10],
        )

        temp = add_rolling_features(
            temp,
            column="imbalance_5",
            windows=[5, 10],
            add_mean=True,
            add_std=False,
            add_sum=False,
        )

        temp = add_change_features(
            temp,
            column="microprice_deviation_bps",
            lags=[1, 5, 10],
        )

        temp = add_rolling_features(
            temp,
            column="microprice_deviation_bps",
            windows=[5, 10],
            add_mean=True,
            add_std=False,
            add_sum=False,
        )

        temp = add_lag_features(
            temp,
            column="mid_return_bps",
            lags=[1, 2, 5, 10],
        )

        temp = add_rolling_features(
            temp,
            column="mid_return_bps",
            windows=[5, 10],
            add_mean=True,
            add_std=True,
            add_sum=False,
        )

        temp = add_rolling_features(
            temp,
            column="quote_changed",
            windows=[5, 10],
            add_mean=False,
            add_std=False,
            add_sum=True,
        )

        temp = add_change_features(
            temp,
            column="spread_bps",
            lags=[1, 5],
        )

        temp = add_rolling_features(
            temp,
            column="spread_bps",
            windows=[5, 10],
            add_mean=True,
            add_std=False,
            add_sum=False,
        )

        temp = add_change_features(
            temp,
            column="best_bid_qty",
            lags=[1],
        )

        temp = add_change_features(
            temp,
            column="best_ask_qty",
            lags=[1],
        )

        temp = add_change_features(
            temp,
            column="bid_depth_5",
            lags=[1],
        )

        temp = add_change_features(
            temp,
            column="ask_depth_5",
            lags=[1],
        )

        frames.append(temp)

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return result


def save_feature_missingness(df: pd.DataFrame) -> None:
    rows = []

    for feature in DYNAMIC_FEATURES:
        if feature not in df.columns:
            rows.append(
                {
                    "feature": feature,
                    "missing_count": None,
                    "missing_share": None,
                    "status": "missing_column",
                }
            )
            continue

        missing_count = int(df[feature].isna().sum())
        missing_share = float(df[feature].isna().mean())

        rows.append(
            {
                "feature": feature,
                "missing_count": missing_count,
                "missing_share": missing_share,
                "status": "ok",
            }
        )

    result = pd.DataFrame(rows)
    result.to_csv(FEATURE_MISSINGNESS_PATH, index=False)

    print(f"[INFO] Saved feature missingness report to: {FEATURE_MISSINGNESS_PATH}")


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
        raise ValueError("Invalid train/test split. Need more valid runs.")

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


def train_logistic_model(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: list[str],
) -> tuple[pd.DataFrame | None, str | None]:
    validate_features(train, feature_cols)
    validate_features(test, feature_cols)

    train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
    test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

    if train_clean.empty or test_clean.empty:
        return None, "Empty train or test set after dropping missing values."

    if train_clean["target"].nunique() < 2:
        return None, "Train set contains only one class."

    if test_clean["target"].nunique() < 2:
        return None, "Test set contains only one class."

    X_train = train_clean[feature_cols]
    y_train = train_clean["target"]

    X_test = test_clean[feature_cols]

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "logistic_regression",
                LogisticRegression(
                    class_weight="balanced",
                    solver="liblinear",
                    random_state=42,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    y_score = model.predict_proba(X_test)[:, 1]
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

    return predictions, None


def assign_threshold_signal(p_up: float, threshold: float) -> str:
    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def summarize_thresholds(
    predictions: pd.DataFrame,
    model_name: str,
    horizon: int,
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
                        "horizon": horizon,
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
                    "horizon": horizon,
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

    result = pd.DataFrame(rows)

    return result


def save_metric_plot(
    sweep: pd.DataFrame,
    metric: str,
    ylabel: str,
) -> None:
    subset = sweep[sweep["signal_group"] == "both"].copy()

    plt.figure()

    for model_name, model_df in subset.groupby("model", sort=True):
        reliable = model_df[model_df["n_signals"] >= MIN_SIGNALS_FOR_TOP].copy()

        if reliable.empty:
            continue

        best_by_threshold = (
            reliable
            .sort_values(metric, ascending=False)
            .groupby("threshold")
            .head(1)
            .sort_values("threshold")
        )

        plt.plot(
            best_by_threshold["threshold"],
            best_by_threshold[metric],
            marker="o",
            label=model_name,
        )

    plt.title(f"Best {metric} by threshold across horizons")
    plt.xlabel("Threshold")
    plt.ylabel(ylabel)
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_dynamic_feature_best_{metric}_by_threshold.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_horizon_plot(
    sweep: pd.DataFrame,
    model_name: str,
    metric: str,
    ylabel: str,
) -> None:
    subset = sweep[
        (sweep["model"] == model_name) &
        (sweep["signal_group"] == "both")
    ].copy()

    plt.figure()

    for horizon, horizon_df in subset.groupby("horizon", sort=True):
        horizon_df = horizon_df.sort_values("threshold")
        plt.plot(
            horizon_df["threshold"],
            horizon_df[metric],
            marker="o",
            label=f"h={horizon}",
        )

    plt.title(f"{metric} by threshold and horizon: {model_name}")
    plt.xlabel("Threshold")
    plt.ylabel(ylabel)
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_dynamic_feature_{metric}_{model_name}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_all_plots(sweep: pd.DataFrame) -> None:
    save_metric_plot(
        sweep=sweep,
        metric="break_even_round_trip_cost_bps",
        ylabel="Break-even round-trip cost, bps",
    )

    save_metric_plot(
        sweep=sweep,
        metric="mean_signed_return_bps",
        ylabel="Mean signed return, bps",
    )

    save_metric_plot(
        sweep=sweep,
        metric="precision",
        ylabel="Precision / hit rate",
    )

    for model_name in FEATURE_SETS.keys():
        save_horizon_plot(
            sweep=sweep,
            model_name=model_name,
            metric="break_even_round_trip_cost_bps",
            ylabel="Break-even round-trip cost, bps",
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

    base_features = load_base_features()

    print(f"[INFO] Loaded base features: {INPUT_PATH}")
    print(f"[INFO] Base shape: {base_features.shape}")

    dynamic_features = add_dynamic_features(base_features)

    dynamic_features.to_csv(DYNAMIC_FEATURES_PATH, index=False)
    print(f"[INFO] Saved dynamic features to: {DYNAMIC_FEATURES_PATH}")
    print(f"[INFO] Dynamic shape: {dynamic_features.shape}")

    save_feature_missingness(dynamic_features)

    ordered_runs = get_ordered_runs(dynamic_features)
    train_runs, test_runs = split_runs(ordered_runs)

    print_split_info(train_runs, test_runs)

    all_metrics_rows = []
    all_sweep_frames = []

    for horizon in HORIZONS:
        print("\n" + "=" * 80)
        print(f"[INFO] Processing horizon: {horizon} events")
        print("=" * 80)

        horizon_df = build_dataset_for_horizon(dynamic_features, horizon=horizon)

        train = horizon_df[horizon_df["run_name"].isin(train_runs)].copy()
        test = horizon_df[horizon_df["run_name"].isin(test_runs)].copy()

        print(f"[INFO] Horizon dataset rows: {len(horizon_df)}")
        print(f"[INFO] Train rows: {len(train)}")
        print(f"[INFO] Test rows: {len(test)}")

        print("[INFO] Train class distribution:")
        print(train["target_label"].value_counts(dropna=False))

        print("[INFO] Test class distribution:")
        print(test["target_label"].value_counts(dropna=False))

        for model_name, feature_cols in FEATURE_SETS.items():
            print("\n" + "-" * 80)
            print(f"[INFO] Training {model_name}, horizon={horizon}")
            print("-" * 80)

            predictions, error = train_logistic_model(
                train=train,
                test=test,
                feature_cols=feature_cols,
            )

            if error is not None:
                print(f"[WARNING] Skipped {model_name}, horizon={horizon}: {error}")

                all_metrics_rows.append(
                    {
                        "model": model_name,
                        "horizon": horizon,
                        "feature_set": ",".join(feature_cols),
                        "n_features": len(feature_cols),
                        "status": "failed",
                        "error": error,
                    }
                )
                continue

            y_true = predictions["target"]
            y_pred = predictions["y_pred_050"]
            y_score = predictions["predicted_proba_up"]

            metrics = compute_classification_metrics(
                y_true=y_true,
                y_pred=y_pred,
                y_score=y_score,
            )

            all_metrics_rows.append(
                {
                    "model": model_name,
                    "horizon": horizon,
                    "feature_set": ",".join(feature_cols),
                    "n_features": len(feature_cols),
                    "status": "success",
                    "error": None,
                    **metrics,
                }
            )

            sweep = summarize_thresholds(
                predictions=predictions,
                model_name=model_name,
                horizon=horizon,
                total_model_test_obs=len(predictions),
            )

            all_sweep_frames.append(sweep)

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

    metrics_df = pd.DataFrame(all_metrics_rows)
    metrics_df.to_csv(METRICS_PATH, index=False)

    if not all_sweep_frames:
        raise ValueError("No threshold sweep results were produced.")

    sweep_df = pd.concat(all_sweep_frames, ignore_index=True)
    sweep_df.to_csv(SWEEP_PATH, index=False)

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

    best_by_horizon = (
        sweep_df[
            (sweep_df["signal_group"] == "both") &
            (sweep_df["n_signals"] >= MIN_SIGNALS_FOR_BEST_BY_HORIZON)
        ]
        .sort_values(
            ["break_even_round_trip_cost_bps", "precision"],
            ascending=[False, False],
        )
        .groupby(["model", "horizon"])
        .head(1)
        .reset_index(drop=True)
    )

    best_by_horizon.to_csv(BEST_BY_HORIZON_PATH, index=False)

    save_all_plots(sweep_df)

    print(f"\n[INFO] Saved model metrics to: {METRICS_PATH}")
    print(f"[INFO] Saved threshold sweep to: {SWEEP_PATH}")
    print(f"[INFO] Saved top candidates to: {TOP_CANDIDATES_PATH}")
    print(f"[INFO] Saved best-by-horizon table to: {BEST_BY_HORIZON_PATH}")

    print("\n[INFO] Top candidates:")
    print(
        top_candidates[
            [
                "model",
                "horizon",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
            ]
        ]
    )

    print("\n[INFO] Best by model and horizon, requiring enough signals:")
    print(
        best_by_horizon[
            [
                "model",
                "horizon",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
            ]
        ]
    )

    print("\n[INFO] Dynamic feature sweep completed successfully.")


if __name__ == "__main__":
    main()