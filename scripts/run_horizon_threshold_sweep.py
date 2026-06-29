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

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

METRICS_PATH = TABLES_DIR / "multirun_horizon_model_metrics.csv"
SWEEP_PATH = TABLES_DIR / "multirun_horizon_threshold_sweep.csv"
TOP_CANDIDATES_PATH = TABLES_DIR / "multirun_horizon_threshold_top_candidates.csv"

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5

HORIZONS = [5, 10, 20, 50, 100]
THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]

TRAIN_RUN_FRACTION = 0.70

FEATURE_SETS = {
    "logit_signal": [
        "imbalance_5",
        "microprice_deviation_bps",
    ],
    "logit_full": [
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
    ],
}


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_features() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_basic_features_all_runs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    if "run_name" not in df.columns:
        raise ValueError("Column 'run_name' is missing.")

    required_cols = [
        "run_name",
        "event_time",
        "mid_price",
        "imbalance_5",
        "microprice_deviation_bps",
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
        raise ValueError("Invalid run split. Need more runs.")

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


def compute_classification_metrics(y_true, y_pred, y_score=None) -> dict:
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
) -> tuple[pd.DataFrame | None, Pipeline | None, str | None]:
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

    return predictions, model, None


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


def build_majority_metrics(
    train: pd.DataFrame,
    test: pd.DataFrame,
    horizon: int,
) -> dict | None:
    if train.empty or test.empty:
        return None

    if train["target"].nunique() < 1 or test["target"].nunique() < 2:
        return None

    majority_class = int(train["target"].value_counts().idxmax())

    y_true = test["target"]
    y_pred = pd.Series([majority_class] * len(test), index=test.index)

    metrics = compute_classification_metrics(y_true, y_pred)

    return {
        "model": "majority_baseline",
        "horizon": horizon,
        "feature_set": "none",
        "status": "success",
        "error": None,
        **metrics,
    }


def save_metric_plot(sweep: pd.DataFrame, model_name: str, metric: str, ylabel: str) -> None:
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
    plt.legend()
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_horizon_threshold_{metric}_{model_name}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_all_plots(sweep: pd.DataFrame) -> None:
    for model_name in FEATURE_SETS.keys():
        save_metric_plot(
            sweep=sweep,
            model_name=model_name,
            metric="coverage",
            ylabel="Coverage",
        )

        save_metric_plot(
            sweep=sweep,
            model_name=model_name,
            metric="precision",
            ylabel="Precision / hit rate",
        )

        save_metric_plot(
            sweep=sweep,
            model_name=model_name,
            metric="mean_signed_return_bps",
            ylabel="Mean signed return, bps",
        )

        save_metric_plot(
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

    features = load_features()

    ordered_runs = get_ordered_runs(features)
    train_runs, test_runs = split_runs(ordered_runs)

    print_split_info(train_runs, test_runs)

    all_metrics_rows = []
    all_sweep_frames = []

    for horizon in HORIZONS:
        print("\n" + "=" * 80)
        print(f"[INFO] Processing horizon: {horizon} events")
        print("=" * 80)

        horizon_df = build_dataset_for_horizon(features, horizon=horizon)

        train = horizon_df[horizon_df["run_name"].isin(train_runs)].copy()
        test = horizon_df[horizon_df["run_name"].isin(test_runs)].copy()

        print(f"[INFO] Horizon dataset rows: {len(horizon_df)}")
        print(f"[INFO] Train rows: {len(train)}")
        print(f"[INFO] Test rows: {len(test)}")

        print("[INFO] Train class distribution:")
        print(train["target_label"].value_counts(dropna=False))

        print("[INFO] Test class distribution:")
        print(test["target_label"].value_counts(dropna=False))

        majority_metrics = build_majority_metrics(
            train=train,
            test=test,
            horizon=horizon,
        )

        if majority_metrics is not None:
            all_metrics_rows.append(majority_metrics)

        for model_name, feature_cols in FEATURE_SETS.items():
            print("\n" + "-" * 80)
            print(f"[INFO] Training {model_name}, horizon={horizon}")
            print("-" * 80)

            predictions, model, error = train_logistic_model(
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
            (sweep_df["n_signals"] >= 30)
        ]
        .sort_values(
            ["break_even_round_trip_cost_bps", "precision"],
            ascending=[False, False],
        )
        .head(30)
        .copy()
    )

    top_candidates.to_csv(TOP_CANDIDATES_PATH, index=False)

    save_all_plots(sweep_df)

    print(f"\n[INFO] Saved model metrics to: {METRICS_PATH}")
    print(f"[INFO] Saved horizon-threshold sweep to: {SWEEP_PATH}")
    print(f"[INFO] Saved top candidates to: {TOP_CANDIDATES_PATH}")

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

    print("\n[INFO] Horizon + threshold sweep completed successfully.")


if __name__ == "__main__":
    main()