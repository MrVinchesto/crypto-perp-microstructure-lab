from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


TARGET_HORIZON = 10

INPUT_PATH = Path(
    f"reports/tables/multirun_baseline_model_predictions_h{TARGET_HORIZON}.csv"
)

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

DIAGNOSTICS_PATH = TABLES_DIR / f"multirun_threshold_diagnostics_h{TARGET_HORIZON}.csv"
SELECTED_SIGNALS_PATH = TABLES_DIR / f"multirun_threshold_selected_signals_h{TARGET_HORIZON}.csv"

THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]

MODELS_TO_ANALYZE = [
    "logit_signal",
    "logit_full",
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_predictions() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/train_multirun_baseline_model.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    required_cols = [
        "model",
        "run_name",
        "row_in_run",
        "event_time",
        "target_label",
        "future_mid_return_10e_bps",
        "predicted_proba_up",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[df["model"].isin(MODELS_TO_ANALYZE)].copy()

    df["predicted_proba_up"] = pd.to_numeric(
        df["predicted_proba_up"],
        errors="coerce",
    )

    df = df.dropna(subset=["predicted_proba_up", "future_mid_return_10e_bps"]).copy()

    return df


def assign_threshold_signal(row: pd.Series, threshold: float) -> str:
    p_up = row["predicted_proba_up"]

    if p_up >= threshold:
        return "up"

    if p_up <= 1.0 - threshold:
        return "down"

    return "no_signal"


def add_threshold_signals(df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for model_name, model_df in df.groupby("model", sort=True):
        total_observations = len(model_df)

        for threshold in THRESHOLDS:
            temp = model_df.copy()
            temp["threshold"] = threshold
            temp["signal"] = temp.apply(
                lambda row: assign_threshold_signal(row, threshold),
                axis=1,
            )

            temp["has_signal"] = temp["signal"].isin(["up", "down"])

            temp["is_correct_signal"] = (
                (temp["signal"] == "up") & (temp["target_label"] == "up")
            ) | (
                (temp["signal"] == "down") & (temp["target_label"] == "down")
            )

            temp["signed_return_bps"] = None

            up_mask = temp["signal"] == "up"
            down_mask = temp["signal"] == "down"

            temp.loc[up_mask, "signed_return_bps"] = temp.loc[
                up_mask,
                "future_mid_return_10e_bps",
            ]

            temp.loc[down_mask, "signed_return_bps"] = -temp.loc[
                down_mask,
                "future_mid_return_10e_bps",
            ]

            temp["signed_return_bps"] = pd.to_numeric(
                temp["signed_return_bps"],
                errors="coerce",
            )

            temp["total_observations_for_model"] = total_observations

            frames.append(temp)

    result = pd.concat(frames, ignore_index=True)

    return result


def summarize_one_subset(
    subset: pd.DataFrame,
    model_name: str,
    threshold: float,
    signal_group: str,
    total_observations: int,
) -> dict:
    n_signals = len(subset)

    if n_signals == 0:
        return {
            "model": model_name,
            "threshold": threshold,
            "signal_group": signal_group,
            "n_signals": 0,
            "coverage": 0.0,
            "precision": None,
            "mean_future_return_bps": None,
            "median_future_return_bps": None,
            "mean_signed_return_bps": None,
            "median_signed_return_bps": None,
            "std_signed_return_bps": None,
            "positive_signed_return_share": None,
            "avg_predicted_proba_up": None,
        }

    return {
        "model": model_name,
        "threshold": threshold,
        "signal_group": signal_group,
        "n_signals": n_signals,
        "coverage": n_signals / total_observations,
        "precision": subset["is_correct_signal"].mean(),
        "mean_future_return_bps": subset["future_mid_return_10e_bps"].mean(),
        "median_future_return_bps": subset["future_mid_return_10e_bps"].median(),
        "mean_signed_return_bps": subset["signed_return_bps"].mean(),
        "median_signed_return_bps": subset["signed_return_bps"].median(),
        "std_signed_return_bps": subset["signed_return_bps"].std(),
        "positive_signed_return_share": (subset["signed_return_bps"] > 0).mean(),
        "avg_predicted_proba_up": subset["predicted_proba_up"].mean(),
    }


def build_threshold_diagnostics(signals: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (model_name, threshold), group in signals.groupby(["model", "threshold"], sort=True):
        total_observations = int(group["total_observations_for_model"].iloc[0])

        up_subset = group[group["signal"] == "up"].copy()
        down_subset = group[group["signal"] == "down"].copy()
        both_subset = group[group["has_signal"]].copy()

        rows.append(
            summarize_one_subset(
                subset=up_subset,
                model_name=model_name,
                threshold=threshold,
                signal_group="up",
                total_observations=total_observations,
            )
        )

        rows.append(
            summarize_one_subset(
                subset=down_subset,
                model_name=model_name,
                threshold=threshold,
                signal_group="down",
                total_observations=total_observations,
            )
        )

        rows.append(
            summarize_one_subset(
                subset=both_subset,
                model_name=model_name,
                threshold=threshold,
                signal_group="both",
                total_observations=total_observations,
            )
        )

    diagnostics = pd.DataFrame(rows)

    return diagnostics


def save_selected_signals(signals: pd.DataFrame) -> pd.DataFrame:
    selected = signals[signals["has_signal"]].copy()

    cols = [
        "model",
        "threshold",
        "run_name",
        "row_in_run",
        "event_time",
        "target_label",
        "future_mid_return_10e_bps",
        "predicted_proba_up",
        "signal",
        "is_correct_signal",
        "signed_return_bps",
    ]

    selected = selected[cols].copy()
    selected.to_csv(SELECTED_SIGNALS_PATH, index=False)

    print(f"[INFO] Saved selected threshold signals to: {SELECTED_SIGNALS_PATH}")

    return selected


def save_metric_plot(
    diagnostics: pd.DataFrame,
    model_name: str,
    signal_group: str,
    metric: str,
    ylabel: str,
    filename: str,
) -> None:
    subset = diagnostics[
        (diagnostics["model"] == model_name) &
        (diagnostics["signal_group"] == signal_group)
    ].copy()

    plt.figure()
    plt.plot(subset["threshold"], subset[metric], marker="o")
    plt.title(f"{metric} by threshold: {model_name}, {signal_group}")
    plt.xlabel("Threshold")
    plt.ylabel(ylabel)
    plt.tight_layout()

    out_path = FIGURES_DIR / filename
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved plot to: {out_path}")


def save_all_plots(diagnostics: pd.DataFrame) -> None:
    for model_name in MODELS_TO_ANALYZE:
        save_metric_plot(
            diagnostics=diagnostics,
            model_name=model_name,
            signal_group="both",
            metric="coverage",
            ylabel="Coverage",
            filename=f"multirun_threshold_coverage_{model_name}_h{TARGET_HORIZON}.png",
        )

        save_metric_plot(
            diagnostics=diagnostics,
            model_name=model_name,
            signal_group="both",
            metric="precision",
            ylabel="Precision / hit rate",
            filename=f"multirun_threshold_precision_{model_name}_h{TARGET_HORIZON}.png",
        )

        save_metric_plot(
            diagnostics=diagnostics,
            model_name=model_name,
            signal_group="both",
            metric="mean_signed_return_bps",
            ylabel="Mean signed return, bps",
            filename=f"multirun_threshold_mean_signed_return_{model_name}_h{TARGET_HORIZON}.png",
        )

        save_metric_plot(
            diagnostics=diagnostics,
            model_name=model_name,
            signal_group="up",
            metric="precision",
            ylabel="UP precision",
            filename=f"multirun_threshold_up_precision_{model_name}_h{TARGET_HORIZON}.png",
        )

        save_metric_plot(
            diagnostics=diagnostics,
            model_name=model_name,
            signal_group="down",
            metric="precision",
            ylabel="DOWN precision",
            filename=f"multirun_threshold_down_precision_{model_name}_h{TARGET_HORIZON}.png",
        )


def print_key_diagnostics(diagnostics: pd.DataFrame) -> None:
    print("\n[INFO] Threshold diagnostics, combined UP/DOWN signals:")
    print(
        diagnostics[diagnostics["signal_group"] == "both"][
            [
                "model",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_signed_return_bps",
                "positive_signed_return_share",
            ]
        ]
    )

    print("\n[INFO] UP signal diagnostics:")
    print(
        diagnostics[diagnostics["signal_group"] == "up"][
            [
                "model",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_future_return_bps",
            ]
        ]
    )

    print("\n[INFO] DOWN signal diagnostics:")
    print(
        diagnostics[diagnostics["signal_group"] == "down"][
            [
                "model",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "mean_future_return_bps",
                "mean_signed_return_bps",
            ]
        ]
    )


def main() -> None:
    ensure_output_dirs()

    predictions = load_predictions()
    signals = add_threshold_signals(predictions)

    diagnostics = build_threshold_diagnostics(signals)
    diagnostics.to_csv(DIAGNOSTICS_PATH, index=False)

    selected = save_selected_signals(signals)

    save_all_plots(diagnostics)

    print(f"[INFO] Saved threshold diagnostics to: {DIAGNOSTICS_PATH}")
    print(f"[INFO] Total selected signal rows across thresholds: {len(selected)}")

    print_key_diagnostics(diagnostics)

    print("\n[INFO] Threshold analysis completed successfully.")


if __name__ == "__main__":
    main()