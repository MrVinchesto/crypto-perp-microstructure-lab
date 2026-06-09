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


INPUT_PATH = Path("data/processed/modeling_dataset_nonflat.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

TARGET_HORIZON = 10
TRAIN_FRACTION = 0.70

FEATURE_COLUMNS = [
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


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_dataset() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_labels.py first."
        )

    df = pd.read_csv(INPUT_PATH)
    df = df[df["target_horizon"] == TARGET_HORIZON].copy()
    df = df.sort_values("event_time").reset_index(drop=True)

    if df.empty:
        raise ValueError(f"No rows found for target_horizon={TARGET_HORIZON}")

    return df


def validate_columns(df: pd.DataFrame) -> None:
    required = FEATURE_COLUMNS + ["target_label", "event_time"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def prepare_xy(df: pd.DataFrame):
    validate_columns(df)

    work = df.copy()

    # Binary encoding:
    # down -> 0
    # up   -> 1
    work = work[work["target_label"].isin(["down", "up"])].copy()
    work["target"] = work["target_label"].map({"down": 0, "up": 1})

    # Drop rows with missing feature values
    work = work.dropna(subset=FEATURE_COLUMNS + ["target"]).reset_index(drop=True)

    X = work[FEATURE_COLUMNS]
    y = work["target"].astype(int)

    return work, X, y


def chronological_train_test_split(work: pd.DataFrame, X: pd.DataFrame, y: pd.Series):
    split_idx = int(len(work) * TRAIN_FRACTION)

    if split_idx <= 0 or split_idx >= len(work):
        raise ValueError("Invalid train/test split. Need more observations.")

    X_train = X.iloc[:split_idx].copy()
    X_test = X.iloc[split_idx:].copy()

    y_train = y.iloc[:split_idx].copy()
    y_test = y.iloc[split_idx:].copy()

    work_train = work.iloc[:split_idx].copy()
    work_test = work.iloc[split_idx:].copy()

    return work_train, work_test, X_train, X_test, y_train, y_test


def majority_baseline_predictions(y_train: pd.Series, n_test: int) -> int:
    majority_class = int(y_train.value_counts().idxmax())
    return majority_class


def compute_metrics(y_true, y_pred, y_score=None) -> dict:
    metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "precision_up": precision_score(y_true, y_pred, pos_label=1, zero_division=0),
        "recall_up": recall_score(y_true, y_pred, pos_label=1, zero_division=0),
        "f1_up": f1_score(y_true, y_pred, pos_label=1, zero_division=0),
        "n_obs": len(y_true),
        "up_share": float(pd.Series(y_true).mean()),
    }

    if y_score is not None and len(set(y_true)) == 2:
        metrics["roc_auc"] = roc_auc_score(y_true, y_score)
    else:
        metrics["roc_auc"] = None

    return metrics


def train_logistic_regression(X_train: pd.DataFrame, y_train: pd.Series) -> Pipeline:
    if y_train.nunique() < 2:
        raise ValueError(
            "Training set contains only one class. "
            "Collect more data or adjust the split before training a classifier."
        )

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
    return model


def save_metrics_table(baseline_metrics: dict, model_metrics: dict) -> pd.DataFrame:
    rows = []

    baseline_row = {"model": "majority_baseline", **baseline_metrics}
    model_row = {"model": "logistic_regression_balanced", **model_metrics}

    rows.append(baseline_row)
    rows.append(model_row)

    metrics_df = pd.DataFrame(rows)

    out_path = TABLES_DIR / f"baseline_model_metrics_h{TARGET_HORIZON}.csv"
    metrics_df.to_csv(out_path, index=False)

    print(f"[INFO] Saved metrics to: {out_path}")
    print("\n[INFO] Metrics:")
    print(metrics_df)

    return metrics_df


def save_confusion_matrix_table(y_true, y_pred, model_name: str) -> pd.DataFrame:
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])

    cm_df = pd.DataFrame(
        cm,
        index=["true_down", "true_up"],
        columns=["pred_down", "pred_up"],
    )

    out_path = TABLES_DIR / f"{model_name}_confusion_matrix_h{TARGET_HORIZON}.csv"
    cm_df.to_csv(out_path)

    print(f"[INFO] Saved confusion matrix to: {out_path}")
    print(f"\n[INFO] Confusion matrix for {model_name}:")
    print(cm_df)

    return cm_df


def save_confusion_matrix_plot(cm_df: pd.DataFrame, model_name: str) -> None:
    plt.figure()
    plt.imshow(cm_df.values)
    plt.title(f"Confusion matrix: {model_name}, horizon={TARGET_HORIZON}")
    plt.xlabel("Predicted label")
    plt.ylabel("True label")

    plt.xticks([0, 1], ["down", "up"])
    plt.yticks([0, 1], ["down", "up"])

    for i in range(cm_df.shape[0]):
        for j in range(cm_df.shape[1]):
            plt.text(j, i, str(cm_df.values[i, j]), ha="center", va="center")

    plt.tight_layout()

    out_path = FIGURES_DIR / f"{model_name}_confusion_matrix_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved confusion matrix plot to: {out_path}")


def save_feature_coefficients(model: Pipeline) -> pd.DataFrame:
    logit = model.named_steps["logistic_regression"]
    coefficients = logit.coef_[0]

    coef_df = pd.DataFrame(
        {
            "feature": FEATURE_COLUMNS,
            "coefficient": coefficients,
            "abs_coefficient": abs(coefficients),
        }
    ).sort_values("abs_coefficient", ascending=False)

    out_path = TABLES_DIR / f"baseline_model_feature_coefficients_h{TARGET_HORIZON}.csv"
    coef_df.to_csv(out_path, index=False)

    print(f"[INFO] Saved feature coefficients to: {out_path}")
    print("\n[INFO] Top coefficients:")
    print(coef_df.head(10))

    return coef_df


def save_probability_plot(y_score) -> None:
    plt.figure()
    plt.hist(y_score, bins=20)
    plt.title(f"Predicted probability of UP, horizon={TARGET_HORIZON}")
    plt.xlabel("Predicted probability of up")
    plt.ylabel("Frequency")
    plt.tight_layout()

    out_path = FIGURES_DIR / f"baseline_model_probabilities_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved probability histogram to: {out_path}")


def save_predictions(work_test: pd.DataFrame, y_test, y_pred, y_score) -> None:
    pred_df = work_test[
        [
            "event_time",
            "target_horizon",
            "target_label",
            "future_mid_return_10e_bps",
        ]
    ].copy()

    pred_df["y_true"] = y_test.values
    pred_df["y_pred"] = y_pred
    pred_df["predicted_label"] = pd.Series(y_pred).map({0: "down", 1: "up"}).values
    pred_df["predicted_proba_up"] = y_score

    out_path = TABLES_DIR / f"baseline_model_predictions_h{TARGET_HORIZON}.csv"
    pred_df.to_csv(out_path, index=False)

    print(f"[INFO] Saved predictions to: {out_path}")


def print_dataset_diagnostics(work, y_train, y_test) -> None:
    print("\n[INFO] Dataset diagnostics")
    print(f"Total rows for horizon {TARGET_HORIZON}: {len(work)}")
    print(f"Train rows: {len(y_train)}")
    print(f"Test rows: {len(y_test)}")

    print("\n[INFO] Train class distribution:")
    print(y_train.value_counts(normalize=True).rename(index={0: "down", 1: "up"}))

    print("\n[INFO] Test class distribution:")
    print(y_test.value_counts(normalize=True).rename(index={0: "down", 1: "up"}))


def main() -> None:
    ensure_output_dirs()

    df = load_dataset()
    work, X, y = prepare_xy(df)

    work_train, work_test, X_train, X_test, y_train, y_test = chronological_train_test_split(
        work=work,
        X=X,
        y=y,
    )

    print_dataset_diagnostics(work, y_train, y_test)

    # Majority baseline
    majority_class = majority_baseline_predictions(y_train, n_test=len(y_test))
    y_pred_baseline = pd.Series([majority_class] * len(y_test), index=y_test.index)

    baseline_metrics = compute_metrics(
        y_true=y_test,
        y_pred=y_pred_baseline,
        y_score=None,
    )

    baseline_cm = save_confusion_matrix_table(
        y_true=y_test,
        y_pred=y_pred_baseline,
        model_name="majority_baseline",
    )
    save_confusion_matrix_plot(baseline_cm, model_name="majority_baseline")

    # Logistic regression baseline
    model = train_logistic_regression(X_train, y_train)

    y_pred_model = model.predict(X_test)
    y_score_model = model.predict_proba(X_test)[:, 1]

    model_metrics = compute_metrics(
        y_true=y_test,
        y_pred=y_pred_model,
        y_score=y_score_model,
    )

    model_cm = save_confusion_matrix_table(
        y_true=y_test,
        y_pred=y_pred_model,
        model_name="logistic_regression_balanced",
    )
    save_confusion_matrix_plot(model_cm, model_name="logistic_regression_balanced")

    save_metrics_table(
        baseline_metrics=baseline_metrics,
        model_metrics=model_metrics,
    )

    save_feature_coefficients(model)
    save_probability_plot(y_score_model)
    save_predictions(
        work_test=work_test,
        y_test=y_test,
        y_pred=y_pred_model,
        y_score=y_score_model,
    )

    print("\n[INFO] Baseline model training completed successfully.")


if __name__ == "__main__":
    main()