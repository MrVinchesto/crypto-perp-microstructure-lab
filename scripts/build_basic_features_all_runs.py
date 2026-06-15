from pathlib import Path

import pandas as pd


INPUT_PATH = Path("data/processed/top_of_book_all.csv")
OUTPUT_PATH = Path("data/processed/basic_features_all.csv")
SUMMARY_PATH = Path("reports/tables/basic_features_all_summary.csv")


def add_features_for_single_run(run_df: pd.DataFrame) -> pd.DataFrame:
    df = run_df.copy()
    df = df.sort_values("event_time").reset_index(drop=True)

    df["mid_price_prev"] = df["mid_price"].shift(1)
    df["event_gap_ms"] = df["event_time"].diff()
    df["transaction_lag_ms"] = df["event_time"] - df["transaction_time"]

    df["mid_price_change"] = df["mid_price"] - df["mid_price_prev"]
    df["mid_return"] = df["mid_price"] / df["mid_price_prev"] - 1
    df["mid_return_bps"] = df["mid_return"] * 10000

    df["spread_bps"] = df["spread"] / df["mid_price"] * 10000

    df["best_bid_prev"] = df["best_bid"].shift(1)
    df["best_ask_prev"] = df["best_ask"].shift(1)

    df["quote_changed"] = (
        (df["best_bid"] != df["best_bid_prev"]) |
        (df["best_ask"] != df["best_ask_prev"])
    ).astype(int)

    df["microprice_deviation"] = df["microprice"] - df["mid_price"]
    df["microprice_deviation_bps"] = (
        df["microprice_deviation"] / df["mid_price"] * 10000
    )

    return df


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for run_name, run_df in df.groupby("run_name"):
        rows.append(
            {
                "run_name": run_name,
                "n_rows": len(run_df),
                "start_event_time": run_df["event_time"].min(),
                "end_event_time": run_df["event_time"].max(),
                "median_event_gap_ms": run_df["event_gap_ms"].median(),
                "mean_spread": run_df["spread"].mean(),
                "median_spread": run_df["spread"].median(),
                "quote_change_share": run_df["quote_changed"].mean(),
                "mean_imbalance_1": run_df["imbalance_1"].mean(),
                "mean_imbalance_5": run_df["imbalance_5"].mean(),
                "mean_imbalance_10": run_df["imbalance_10"].mean(),
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/build_top_of_book_all_runs.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    if "run_name" not in df.columns:
        raise ValueError("Column 'run_name' is missing. Cannot compute features safely by run.")

    frames = []

    for run_name, run_df in df.groupby("run_name", sort=True):
        print(f"[INFO] Building features for run: {run_name}, rows={len(run_df)}")
        features_run = add_features_for_single_run(run_df)
        frames.append(features_run)

    features_all = pd.concat(frames, ignore_index=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    features_all.to_csv(OUTPUT_PATH, index=False)

    summary = build_summary(features_all)
    summary.to_csv(SUMMARY_PATH, index=False)

    print(f"\n[INFO] Saved combined basic features to: {OUTPUT_PATH}")
    print(f"[INFO] Saved features summary to: {SUMMARY_PATH}")
    print(f"[INFO] Total rows: {len(features_all)}")
    print(f"[INFO] Runs: {features_all['run_name'].nunique()}")

    print("\n[INFO] First rows:")
    print(features_all.head())

    print("\n[INFO] Summary:")
    print(summary)


if __name__ == "__main__":
    main()