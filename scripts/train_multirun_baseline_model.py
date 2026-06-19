from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


INPUT_PATH = Path("data/processed/modeling_dataset_nonflat_all.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

TARGET_HORIZON = 10
TRAIN_RUN_FRACTION = 0.70

METRICS_PATH = TABLES_DIR / f"multirun_baseline_model_metrics_h{TARGET_HORIZON}.csv"
CONFUSION_PATH = TABLES_DIR / f"multirun_baseline_model_confusion_matrices_h{TARGET_HORIZON}.csv"
COEFFICIENTS_PATH = TABLES_DIR / f"multirun_baseline_model_coefficients_h{TARGET_HORIZON}.csv"
PREDICTIONS_PATH = TABLES_DIR / f"multirun_baseline_model_predictions_h{TARGET_HORIZON}.csv"


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


def load_dataset() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_labels_all_runs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    df = df[df["target_horizon"] == TARGET_HORIZON].copy()
    df = df[df["target_label"].isin(["down", "up"])].copy()

    if df.empty:
        raise ValueError(f"No data found for target_horizon={TARGET_HORIZON}")

    df["target"] = df["target_label"].map({"down": 0, "up": 1}).astype(int)

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def get_ordered_runs(df: pd.DataFrame) -> list[str]:
    run_order = (
        df.groupby("run_name")["event_time"]
        .min()
        .sort_values()
        .index
        .tolist()
    )

    return run_order


def split_by_runs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    ordered_runs = get_ordered_runs(df)

    split_idx = int(len(ordered_runs) * TRAIN_RUN_FRACTION)

    if split_idx <= 0 or split_idx >= len(ordered_runs):
        raise ValueError("Invalid run split. Need more runs.")

    train_runs = ordered_runs[:split_idx]
    test_runs = ordered_runs[split_idx:]

    train = df[df["run_name"].isin(train_runs)].copy()
    test = df[df["run_name"].isin(test_runs)].copy()

    return train, test, train_runs, test_runs


def validate_feature_columns(df: pd.DataFrame, feature_cols: list[str]) -> None:
    required = feature_cols + ["target", "target_label", "run_name", "event_time"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")


def compute_metrics(y_true, y_pred, y_score=None) -> dict:
    metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "precision_up": precision_score(y_true, y_pred, pos_label=1, zero_division=0),
        "recall_up": recall_score(y_true, y_pred, pos_label=1, zero_division=0),
        "f1_up": f1_score(y_true, y_pred, pos_label=1, zero_division=0),
        "precision_down": precision_score(y_true, y_pred, pos_label=0, zero_division=0),
        "recall_down": recall_score(y_true, y_pred, pos_label=0, zero_division=0),
        "f1_down": f1_score(y_true, y_pred, pos_label=0, zero_division=0),
        "n_test": len(y_true),
        "up_share_test": float(pd.Series(y_true).mean()),
    }

    if y_score is not None and len(set(y_true)) == 2:
        metrics["roc_auc"] = roc_auc_score(y_true, y_score)
    else:
        metrics["roc_auc"] = None

    return metrics


def make_majority_baseline(train: pd.DataFrame, test: pd.DataFrame) -> tuple[pd.Series, None]:
    majority_class = int(train["target"].value_counts().idxmax())

    y_pred = pd.Series(
        [majority_class] * len(test),
        index=test.index,
    )

    return y_pred, None


def train_logistic_model(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: list[str],
) -> tuple[pd.Series, pd.Series, Pipeline]:
    validate_feature_columns(train, feature_cols)
    validate_feature_columns(test, feature_cols)

    train_clean = train.dropna(subset=feature_cols + ["target"]).copy()
    test_clean = test.dropna(subset=feature_cols + ["target"]).copy()

    if train_clean["target"].nunique() < 2:
        raise ValueError("Train set contains only one class.")

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

    y_pred = pd.Series(
        model.predict(X_test),
        index=test_clean.index,
    )

    y_score = pd.Series(
        model.predict_proba(X_test)[:, 1],
        index=test_clean.index,
    )

    return y_pred, y_score, model


def save_confusion_matrix_plot(model_name: str, cm: pd.DataFrame) -> None:
    plt.figure()
    plt.imshow(cm.values)
    plt.title(f"Confusion matrix: {model_name}, horizon={TARGET_HORIZON}")
    plt.xlabel("Predicted label")
    plt.ylabel("True label")

    plt.xticks([0, 1], ["down", "up"])
    plt.yticks([0, 1], ["down", "up"])

    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            plt.text(j, i, str(cm.values[i, j]), ha="center", va="center")

    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_baseline_model_confusion_matrix_{model_name}_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved confusion matrix plot to: {out_path}")


def save_probability_plot(model_name: str, y_score: pd.Series) -> None:
    plt.figure()
    plt.hist(y_score.dropna(), bins=30)
    plt.title(f"Predicted probability of UP: {model_name}, horizon={TARGET_HORIZON}")
    plt.xlabel("Predicted probability of up")
    plt.ylabel("Frequency")
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_baseline_model_probabilities_{model_name}_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved probability plot to: {out_path}")


def build_confusion_matrix_df(y_true, y_pred) -> pd.DataFrame:
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])

    cm_df = pd.DataFrame(
        cm,
        index=["true_down", "true_up"],
        columns=["pred_down", "pred_up"],
    )

    return cm_df


def extract_coefficients(model_name: str, model: Pipeline, feature_cols: list[str]) -> pd.DataFrame:
    logit = model.named_steps["logistic_regression"]
    coefficients = logit.coef_[0]

    coef_df = pd.DataFrame(
        {
            "model": model_name,
            "feature": feature_cols,
            "coefficient": coefficients,
            "abs_coefficient": abs(coefficients),
        }
    ).sort_values(["model", "abs_coefficient"], ascending=[True, False])

    return coef_df


def print_split_diagnostics(
    df: pd.DataFrame,
    train: pd.DataFrame,
    test: pd.DataFrame,
    train_runs: list[str],
    test_runs: list[str],
) -> None:
    print("\n[INFO] Dataset diagnostics")
    print(f"Total rows for horizon {TARGET_HORIZON}: {len(df)}")
    print(f"Total runs: {df['run_name'].nunique()}")

    print("\n[INFO] Train runs:")
    for run in train_runs:
        print(f"  {run}")

    print("\n[INFO] Test runs:")
    for run in test_runs:
        print(f"  {run}")

    print("\n[INFO] Train class distribution:")
    print(train["target_label"].value_counts())
    print(train["target_label"].value_counts(normalize=True))

    print("\n[INFO] Test class distribution:")
    print(test["target_label"].value_counts())
    print(test["target_label"].value_counts(normalize=True))


def main() -> None:
    ensure_output_dirs()

    df = load_dataset()
    train, test, train_runs, test_runs = split_by_runs(df)

    print_split_diagnostics(
        df=df,
        train=train,
        test=test,
        train_runs=train_runs,
        test_runs=test_runs,
    )

    y_test = test["target"]

    metrics_rows = []
    confusion_rows = []
    coefficient_frames = []
    prediction_frames = []

    # 1. Majority baseline
    y_pred_majority, _ = make_majority_baseline(train, test)

    majority_metrics = compute_metrics(
        y_true=y_test,
        y_pred=y_pred_majority,
        y_score=None,
    )

    metrics_rows.append(
        {
            "model": "majority_baseline",
            "feature_set": "none",
            **majority_metrics,
        }
    )

    cm_majority = build_confusion_matrix_df(y_test, y_pred_majority)
    cm_majority_long = cm_majority.reset_index().rename(columns={"index": "true_label"})
    cm_majority_long.insert(0, "model", "majority_baseline")
    confusion_rows.append(cm_majority_long)

    save_confusion_matrix_plot("majority_baseline", cm_majority)

    # Save majority predictions
    majority_pred_df = test[
        [
            "run_name",
            "row_in_run",
            "event_time",
            "target_horizon",
            "target_label",
            "future_mid_return_10e_bps",
        ]
    ].copy()
    majority_pred_df["model"] = "majority_baseline"
    majority_pred_df["y_true"] = y_test.values
    majority_pred_df["y_pred"] = y_pred_majority.values
    majority_pred_df["predicted_label"] = pd.Series(y_pred_majority.values).map({0: "down", 1: "up"}).values
    majority_pred_df["predicted_proba_up"] = None
    prediction_frames.append(majority_pred_df)

    # 2. Logistic models
    for model_name, feature_cols in FEATURE_SETS.items():
        print("\n" + "=" * 80)
        print(f"[INFO] Training model: {model_name}")
        print("=" * 80)

        y_pred, y_score, model = train_logistic_model(
            train=train,
            test=test,
            feature_cols=feature_cols,
        )

        test_clean = test.loc[y_pred.index].copy()
        y_test_clean = test_clean["target"]

        model_metrics = compute_metrics(
            y_true=y_test_clean,
            y_pred=y_pred,
            y_score=y_score,
        )

        metrics_rows.append(
            {
                "model": model_name,
                "feature_set": ",".join(feature_cols),
                **model_metrics,
            }
        )

        cm = build_confusion_matrix_df(y_test_clean, y_pred)
        cm_long = cm.reset_index().rename(columns={"index": "true_label"})
        cm_long.insert(0, "model", model_name)
        confusion_rows.append(cm_long)

        save_confusion_matrix_plot(model_name, cm)
        save_probability_plot(model_name, y_score)

        coef_df = extract_coefficients(
            model_name=model_name,
            model=model,
            feature_cols=feature_cols,
        )
        coefficient_frames.append(coef_df)

        pred_df = test_clean[
            [
                "run_name",
                "row_in_run",
                "event_time",
                "target_horizon",
                "target_label",
                "future_mid_return_10e_bps",
            ]
        ].copy()

        pred_df["model"] = model_name
        pred_df["y_true"] = y_test_clean.values
        pred_df["y_pred"] = y_pred.values
        pred_df["predicted_label"] = pd.Series(y_pred.values).map({0: "down", 1: "up"}).values
        pred_df["predicted_proba_up"] = y_score.values
        prediction_frames.append(pred_df)

    # Save outputs
    metrics_df = pd.DataFrame(metrics_rows)
    metrics_df.to_csv(METRICS_PATH, index=False)

    confusion_df = pd.concat(confusion_rows, ignore_index=True)
    confusion_df.to_csv(CONFUSION_PATH, index=False)

    if coefficient_frames:
        coefficients_df = pd.concat(coefficient_frames, ignore_index=True)
        coefficients_df.to_csv(COEFFICIENTS_PATH, index=False)
    else:
        coefficients_df = pd.DataFrame()

    predictions_df = pd.concat(prediction_frames, ignore_index=True)
    predictions_df.to_csv(PREDICTIONS_PATH, index=False)

    print(f"\n[INFO] Saved metrics to: {METRICS_PATH}")
    print(metrics_df)

    print(f"\n[INFO] Saved confusion matrices to: {CONFUSION_PATH}")
    print(confusion_df)

    print(f"\n[INFO] Saved coefficients to: {COEFFICIENTS_PATH}")
    print(coefficients_df)

    print(f"\n[INFO] Saved predictions to: {PREDICTIONS_PATH}")

    print("\n[INFO] Multi-run baseline modeling completed successfully.")


if __name__ == "__main__":
    main()