from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = Path("data/processed/basic_features.csv")

REPORTS_DIR = Path("reports")
FIGURES_DIR = REPORTS_DIR / "figures"
TABLES_DIR = REPORTS_DIR / "tables"


HORIZONS = [1, 5, 10]


def ensure_output_dirs() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)
    df = df.sort_values("event_time").reset_index(drop=True)
    return df


def add_research_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["microprice_deviation"] = df["microprice"] - df["mid_price"]
    df["microprice_deviation_bps"] = (
        df["microprice_deviation"] / df["mid_price"] * 10000
    )

    for h in HORIZONS:
        future_mid = df["mid_price"].shift(-h)
        df[f"future_mid_return_{h}e"] = future_mid / df["mid_price"] - 1
        df[f"future_mid_return_{h}e_bps"] = df[f"future_mid_return_{h}e"] * 10000

    return df


def save_summary_stats(df: pd.DataFrame) -> None:
    cols = [
        "event_gap_ms",
        "transaction_lag_ms",
        "spread",
        "spread_bps",
        "mid_return_bps",
        "microprice_deviation_bps",
        "imbalance_1",
        "imbalance_5",
        "imbalance_10",
        "bid_depth_5",
        "ask_depth_5",
        "bid_depth_10",
        "ask_depth_10",
    ]

    existing_cols = [c for c in cols if c in df.columns]
    summary = df[existing_cols].describe().T
    out_path = TABLES_DIR / "summary_stats.csv"
    summary.to_csv(out_path)

    print(f"[INFO] Saved summary stats to: {out_path}")


def save_correlation_table(df: pd.DataFrame) -> None:
    signal_cols = [
        "imbalance_1",
        "imbalance_5",
        "imbalance_10",
        "microprice_deviation_bps",
    ]

    target_cols = [
        f"future_mid_return_{h}e_bps"
        for h in HORIZONS
    ]

    rows = []

    for signal in signal_cols:
        for target in target_cols:
            clean = df[[signal, target]].dropna()

            if len(clean) < 5:
                corr = None
            else:
                corr = clean[signal].corr(clean[target])

            rows.append(
                {
                    "signal": signal,
                    "target": target,
                    "correlation": corr,
                    "n_obs": len(clean),
                }
            )

    corr_df = pd.DataFrame(rows)
    out_path = TABLES_DIR / "correlation_table.csv"
    corr_df.to_csv(out_path, index=False)

    print(f"[INFO] Saved correlation table to: {out_path}")
    print("\n[INFO] Correlation table:")
    print(corr_df)


def save_imbalance_bucket_table(df: pd.DataFrame) -> None:
    work = df.copy()

    # qcut делит наблюдения на группы по квантилям.
    # duplicates='drop' нужен на случай, если значений мало или много одинаковых.
    work["imbalance_1_bucket"] = pd.qcut(
        work["imbalance_1"],
        q=5,
        duplicates="drop"
    )

    target_cols = [
        f"future_mid_return_{h}e_bps"
        for h in HORIZONS
    ]

    bucket_stats = (
        work
        .groupby("imbalance_1_bucket", observed=True)[target_cols]
        .mean()
        .reset_index()
    )

    bucket_stats["imbalance_1_bucket"] = bucket_stats["imbalance_1_bucket"].astype(str)

    out_path = TABLES_DIR / "imbalance_bucket_forward_returns.csv"
    bucket_stats.to_csv(out_path, index=False)

    print(f"[INFO] Saved imbalance bucket table to: {out_path}")
    print("\n[INFO] Imbalance bucket forward returns:")
    print(bucket_stats)


def save_histogram(df: pd.DataFrame, column: str, filename: str, title: str) -> None:
    data = df[column].dropna()

    plt.figure()
    plt.hist(data, bins=30)
    plt.title(title)
    plt.xlabel(column)
    plt.ylabel("Frequency")
    plt.tight_layout()

    out_path = FIGURES_DIR / filename
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure to: {out_path}")


def save_bucket_plot(df: pd.DataFrame, horizon: int) -> None:
    work = df.copy()

    work["imbalance_1_bucket"] = pd.qcut(
        work["imbalance_1"],
        q=5,
        duplicates="drop"
    )

    target = f"future_mid_return_{horizon}e_bps"

    grouped = (
        work
        .groupby("imbalance_1_bucket", observed=True)[target]
        .mean()
        .reset_index()
    )

    grouped["bucket_label"] = grouped["imbalance_1_bucket"].astype(str)

    plt.figure()
    plt.bar(grouped["bucket_label"], grouped[target])
    plt.title(f"Mean future mid return by imbalance_1 bucket, horizon={horizon} events")
    plt.xlabel("imbalance_1 bucket")
    plt.ylabel(f"Mean {target}")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = FIGURES_DIR / f"future_return_by_imbalance_bucket_h{horizon}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure to: {out_path}")


def print_basic_diagnostics(df: pd.DataFrame) -> None:
    print("\n[INFO] Dataset shape:")
    print(df.shape)

    print("\n[INFO] Median event gap in ms:")
    print(df["event_gap_ms"].median())

    print("\n[INFO] Spread values:")
    print(df["spread"].value_counts().sort_index())

    print("\n[INFO] Quote changed frequency:")
    print(df["quote_changed"].value_counts(dropna=False))

    print("\n[INFO] Imbalance summary:")
    print(df[["imbalance_1", "imbalance_5", "imbalance_10"]].describe())

    print("\n[INFO] Future return summary:")
    future_cols = [f"future_mid_return_{h}e_bps" for h in HORIZONS]
    print(df[future_cols].describe())


def main() -> None:
    ensure_output_dirs()

    df = load_data()
    df = add_research_columns(df)

    print_basic_diagnostics(df)

    save_summary_stats(df)
    save_correlation_table(df)
    save_imbalance_bucket_table(df)

    save_histogram(
        df=df,
        column="imbalance_1",
        filename="imbalance_1_hist.png",
        title="Distribution of level-1 order book imbalance",
    )

    save_histogram(
        df=df,
        column="microprice_deviation_bps",
        filename="microprice_deviation_hist.png",
        title="Distribution of microprice deviation from mid-price",
    )

    for h in HORIZONS:
        save_bucket_plot(df, horizon=h)

    print("\n[INFO] Exploratory analysis completed successfully.")


if __name__ == "__main__":
    main()