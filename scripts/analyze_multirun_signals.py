from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = Path("data/processed/labeled_dataset_all.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

HORIZONS = [1, 5, 10]

SIGNAL_COLUMNS = [
    "imbalance_1",
    "imbalance_5",
    "imbalance_10",
    "microprice_deviation_bps",
]

TARGET_COLUMNS = [
    f"future_mid_return_{h}e_bps"
    for h in HORIZONS
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_labels_all_runs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    if "run_name" not in df.columns:
        raise ValueError("Column 'run_name' is missing.")

    df = df.sort_values(["run_name", "event_time"]).reset_index(drop=True)

    return df


def validate_columns(df: pd.DataFrame) -> None:
    required_cols = ["run_name"] + SIGNAL_COLUMNS + TARGET_COLUMNS
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def build_signal_summary(df: pd.DataFrame) -> pd.DataFrame:
    cols = SIGNAL_COLUMNS + TARGET_COLUMNS
    summary = df[cols].describe().T

    out_path = TABLES_DIR / "multirun_signal_summary.csv"
    summary.to_csv(out_path)

    print(f"[INFO] Saved signal summary to: {out_path}")

    return summary


def build_correlation_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for signal in SIGNAL_COLUMNS:
        for target in TARGET_COLUMNS:
            clean = df[[signal, target]].dropna()

            if len(clean) < 5:
                pearson_corr = None
                spearman_corr = None
            else:
                pearson_corr = clean[signal].corr(clean[target], method="pearson")
                spearman_corr = clean[signal].corr(clean[target], method="spearman")

            rows.append(
                {
                    "signal": signal,
                    "target": target,
                    "pearson_corr": pearson_corr,
                    "spearman_corr": spearman_corr,
                    "n_obs": len(clean),
                }
            )

    result = pd.DataFrame(rows)

    out_path = TABLES_DIR / "multirun_correlation_table.csv"
    result.to_csv(out_path, index=False)

    print(f"[INFO] Saved correlation table to: {out_path}")
    print("\n[INFO] Correlation table:")
    print(result)

    return result


def build_spearman_by_run(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        for signal in SIGNAL_COLUMNS:
            for target in TARGET_COLUMNS:
                clean = run_df[[signal, target]].dropna()

                if len(clean) < 5:
                    corr = None
                else:
                    corr = clean[signal].corr(clean[target], method="spearman")

                rows.append(
                    {
                        "run_name": run_name,
                        "signal": signal,
                        "target": target,
                        "spearman_corr": corr,
                        "n_obs": len(clean),
                    }
                )

    result = pd.DataFrame(rows)

    out_path = TABLES_DIR / "multirun_spearman_by_run.csv"
    result.to_csv(out_path, index=False)

    print(f"[INFO] Saved per-run Spearman table to: {out_path}")

    return result


def build_imbalance_bucket_returns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    # Use imbalance_5 as the main bucket signal.
    # It is less noisy than level-1 imbalance and still close to top-of-book pressure.
    signal = "imbalance_5"

    work["imbalance_5_bucket"] = pd.qcut(
        work[signal],
        q=5,
        duplicates="drop",
    )

    target_cols = TARGET_COLUMNS

    bucket_stats = (
        work
        .groupby("imbalance_5_bucket", observed=True)[target_cols]
        .agg(["count", "mean", "median", "std"])
    )

    # Flatten MultiIndex columns
    bucket_stats.columns = [
        f"{target}_{stat}"
        for target, stat in bucket_stats.columns
    ]

    bucket_stats = bucket_stats.reset_index()
    bucket_stats["imbalance_5_bucket"] = bucket_stats["imbalance_5_bucket"].astype(str)

    out_path = TABLES_DIR / "multirun_imbalance_bucket_returns.csv"
    bucket_stats.to_csv(out_path, index=False)

    print(f"[INFO] Saved bucket returns to: {out_path}")
    print("\n[INFO] Bucket returns:")
    print(bucket_stats)

    return bucket_stats


def save_bucket_return_plots(bucket_stats: pd.DataFrame) -> None:
    for h in HORIZONS:
        target_mean_col = f"future_mid_return_{h}e_bps_mean"

        plt.figure()
        plt.bar(
            bucket_stats["imbalance_5_bucket"],
            bucket_stats[target_mean_col],
        )
        plt.title(f"Mean future return by imbalance_5 bucket, horizon={h} events")
        plt.xlabel("imbalance_5 bucket")
        plt.ylabel(f"Mean future_mid_return_{h}e_bps")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"multirun_imbalance_bucket_returns_h{h}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved bucket plot to: {out_path}")


def save_spearman_by_run_plot(spearman_by_run: pd.DataFrame) -> None:
    target = "future_mid_return_10e_bps"
    signal = "imbalance_5"

    subset = spearman_by_run[
        (spearman_by_run["target"] == target) &
        (spearman_by_run["signal"] == signal)
    ].copy()

    subset = subset.sort_values("run_name")

    plt.figure()
    plt.bar(subset["run_name"], subset["spearman_corr"])
    plt.axhline(0)
    plt.title("Spearman correlation by run: imbalance_5 vs 10-event future return")
    plt.xlabel("Run")
    plt.ylabel("Spearman correlation")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = FIGURES_DIR / "multirun_spearman_by_run_h10.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved per-run Spearman plot to: {out_path}")


def print_key_diagnostics(
    df: pd.DataFrame,
    corr_table: pd.DataFrame,
    spearman_by_run: pd.DataFrame,
) -> None:
    print("\n[INFO] Dataset diagnostics:")
    print(f"Rows: {len(df)}")
    print(f"Runs: {df['run_name'].nunique()}")

    print("\n[INFO] Rows by run:")
    print(df["run_name"].value_counts().sort_index())

    print("\n[INFO] Main all-run correlations for imbalance_5:")
    print(
        corr_table[corr_table["signal"] == "imbalance_5"][
            ["signal", "target", "pearson_corr", "spearman_corr", "n_obs"]
        ]
    )

    print("\n[INFO] Per-run Spearman for imbalance_5 and 10-event target:")
    subset = spearman_by_run[
        (spearman_by_run["signal"] == "imbalance_5") &
        (spearman_by_run["target"] == "future_mid_return_10e_bps")
    ][["run_name", "spearman_corr", "n_obs"]]

    print(subset)


def main() -> None:
    ensure_output_dirs()

    df = load_data()
    validate_columns(df)

    signal_summary = build_signal_summary(df)
    corr_table = build_correlation_table(df)
    spearman_by_run = build_spearman_by_run(df)
    bucket_stats = build_imbalance_bucket_returns(df)

    save_bucket_return_plots(bucket_stats)
    save_spearman_by_run_plot(spearman_by_run)

    print_key_diagnostics(
        df=df,
        corr_table=corr_table,
        spearman_by_run=spearman_by_run,
    )

    print("\n[INFO] Multi-run signal analysis completed successfully.")


if __name__ == "__main__":
    main()