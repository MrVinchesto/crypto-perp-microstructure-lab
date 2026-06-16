from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = Path("data/processed/basic_features_all.csv")

PROCESSED_DIR = Path("data/processed")
REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

LABELED_DATASET_PATH = PROCESSED_DIR / "labeled_dataset_all.csv"
NONFLAT_DATASET_PATH = PROCESSED_DIR / "modeling_dataset_nonflat_all.csv"

LABEL_DISTRIBUTION_PATH = TABLES_DIR / "label_distribution_all.csv"
LABEL_DISTRIBUTION_BY_RUN_PATH = TABLES_DIR / "label_distribution_by_run.csv"
NONFLAT_BASELINE_PATH = TABLES_DIR / "nonflat_baseline_all.csv"
TARGET_SUMMARY_PATH = TABLES_DIR / "target_summary_all.csv"

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5
HORIZONS = [1, 5, 10]


def ensure_output_dirs() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
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
        raise ValueError("Column 'run_name' is missing. Cannot safely label by run.")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def label_from_tick_change(tick_change: float) -> str:
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > DEAD_ZONE_TICKS:
        return "up"

    if tick_change < -DEAD_ZONE_TICKS:
        return "down"

    return "flat"


def add_labels_for_single_run(run_df: pd.DataFrame) -> pd.DataFrame:
    df = run_df.copy()
    df = df.sort_values("event_time").reset_index(drop=True)

    for h in HORIZONS:
        future_mid_col = f"future_mid_price_{h}e"
        price_change_col = f"future_mid_change_{h}e"
        tick_change_col = f"future_mid_change_ticks_{h}e"
        return_col = f"future_mid_return_{h}e"
        return_bps_col = f"future_mid_return_{h}e_bps"
        label_col = f"label_{h}e"

        df[future_mid_col] = df["mid_price"].shift(-h)
        df[price_change_col] = df[future_mid_col] - df["mid_price"]
        df[tick_change_col] = df[price_change_col] / PRICE_TICK

        df[return_col] = df[future_mid_col] / df["mid_price"] - 1
        df[return_bps_col] = df[return_col] * 10000

        df[label_col] = df[tick_change_col].apply(label_from_tick_change)

    return df


def build_labeled_dataset(df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        print(f"[INFO] Building labels for run: {run_name}, rows={len(run_df)}")
        labeled_run = add_labels_for_single_run(run_df)
        frames.append(labeled_run)

    labeled = pd.concat(frames, ignore_index=True)
    labeled.insert(0, "labeled_global_row_id", range(len(labeled)))

    return labeled


def save_labeled_datasets(labeled: pd.DataFrame) -> None:
    labeled.to_csv(LABELED_DATASET_PATH, index=False)
    print(f"[INFO] Saved labeled dataset to: {LABELED_DATASET_PATH}")

    nonflat_frames = []

    for h in HORIZONS:
        label_col = f"label_{h}e"

        temp = labeled[labeled[label_col].isin(["up", "down"])].copy()
        temp["target_horizon"] = h
        temp["target_label"] = temp[label_col]

        nonflat_frames.append(temp)

    nonflat = pd.concat(nonflat_frames, ignore_index=True)
    nonflat.to_csv(NONFLAT_DATASET_PATH, index=False)

    print(f"[INFO] Saved non-flat modeling dataset to: {NONFLAT_DATASET_PATH}")


def build_label_distribution(labeled: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for h in HORIZONS:
        label_col = f"label_{h}e"
        counts = labeled[label_col].value_counts(dropna=False)
        shares = labeled[label_col].value_counts(normalize=True, dropna=False)

        for label in ["down", "flat", "up", "unknown"]:
            rows.append(
                {
                    "horizon_events": h,
                    "label": label,
                    "count": int(counts.get(label, 0)),
                    "share": float(shares.get(label, 0.0)),
                }
            )

    result = pd.DataFrame(rows)
    result.to_csv(LABEL_DISTRIBUTION_PATH, index=False)

    print(f"[INFO] Saved label distribution to: {LABEL_DISTRIBUTION_PATH}")

    return result


def build_label_distribution_by_run(labeled: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for run_name, run_df in labeled.groupby("run_name", sort=True):
        for h in HORIZONS:
            label_col = f"label_{h}e"

            counts = run_df[label_col].value_counts(dropna=False)
            shares = run_df[label_col].value_counts(normalize=True, dropna=False)

            for label in ["down", "flat", "up", "unknown"]:
                rows.append(
                    {
                        "run_name": run_name,
                        "horizon_events": h,
                        "label": label,
                        "count": int(counts.get(label, 0)),
                        "share": float(shares.get(label, 0.0)),
                    }
                )

    result = pd.DataFrame(rows)
    result.to_csv(LABEL_DISTRIBUTION_BY_RUN_PATH, index=False)

    print(f"[INFO] Saved label distribution by run to: {LABEL_DISTRIBUTION_BY_RUN_PATH}")

    return result


def build_nonflat_baseline(labeled: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for h in HORIZONS:
        label_col = f"label_{h}e"
        nonflat = labeled[labeled[label_col].isin(["up", "down"])].copy()

        if len(nonflat) == 0:
            rows.append(
                {
                    "horizon_events": h,
                    "n_nonflat": 0,
                    "up_share_nonflat": None,
                    "down_share_nonflat": None,
                    "majority_class": None,
                    "majority_baseline_accuracy": None,
                }
            )
            continue

        shares = nonflat[label_col].value_counts(normalize=True)
        majority_class = shares.idxmax()
        majority_accuracy = shares.max()

        rows.append(
            {
                "horizon_events": h,
                "n_nonflat": len(nonflat),
                "up_share_nonflat": float(shares.get("up", 0.0)),
                "down_share_nonflat": float(shares.get("down", 0.0)),
                "majority_class": majority_class,
                "majority_baseline_accuracy": float(majority_accuracy),
            }
        )

    result = pd.DataFrame(rows)
    result.to_csv(NONFLAT_BASELINE_PATH, index=False)

    print(f"[INFO] Saved non-flat baseline to: {NONFLAT_BASELINE_PATH}")

    return result


def build_target_summary(labeled: pd.DataFrame) -> pd.DataFrame:
    cols = []

    for h in HORIZONS:
        cols.extend(
            [
                f"future_mid_change_{h}e",
                f"future_mid_change_ticks_{h}e",
                f"future_mid_return_{h}e_bps",
            ]
        )

    summary = labeled[cols].describe().T
    summary.to_csv(TARGET_SUMMARY_PATH)

    print(f"[INFO] Saved target summary to: {TARGET_SUMMARY_PATH}")

    return summary


def save_label_distribution_plots(label_distribution: pd.DataFrame) -> None:
    for h in HORIZONS:
        subset = label_distribution[label_distribution["horizon_events"] == h].copy()
        subset = subset[subset["label"].isin(["down", "flat", "up"])]

        plt.figure()
        plt.bar(subset["label"], subset["share"])
        plt.title(f"Label distribution across all runs, horizon={h} events")
        plt.xlabel("Label")
        plt.ylabel("Share")
        plt.ylim(0, 1)
        plt.tight_layout()

        out_path = FIGURES_DIR / f"label_distribution_all_h{h}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved figure to: {out_path}")


def print_diagnostics(
    labeled: pd.DataFrame,
    label_distribution: pd.DataFrame,
    nonflat_baseline: pd.DataFrame,
) -> None:
    print("\n[INFO] Labeled dataset diagnostics:")
    print(f"Rows: {len(labeled)}")
    print(f"Runs: {labeled['run_name'].nunique()}")

    print("\n[INFO] Label distribution:")
    print(label_distribution)

    print("\n[INFO] Non-flat baseline:")
    print(nonflat_baseline)

    print("\n[INFO] Unknown labels by horizon:")
    for h in HORIZONS:
        label_col = f"label_{h}e"
        unknown_count = (labeled[label_col] == "unknown").sum()
        print(f"h={h}: unknown={unknown_count}")


def main() -> None:
    ensure_output_dirs()

    features = load_features()
    labeled = build_labeled_dataset(features)

    save_labeled_datasets(labeled)

    label_distribution = build_label_distribution(labeled)
    build_label_distribution_by_run(labeled)
    nonflat_baseline = build_nonflat_baseline(labeled)
    build_target_summary(labeled)
    save_label_distribution_plots(label_distribution)

    print_diagnostics(
        labeled=labeled,
        label_distribution=label_distribution,
        nonflat_baseline=nonflat_baseline,
    )

    print("\n[INFO] Multi-run labeling pipeline completed successfully.")


if __name__ == "__main__":
    main()