from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


FEATURES_PATH = Path("data/processed/trade_flow_features.csv")
COLLECTION_LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")

DAY26_RAW_SIGNALS_PATH = Path("reports/tables/trade_flow_test_raw_signals.csv")
DAY26_COOLDOWN_SIGNALS_PATH = Path("reports/tables/trade_flow_test_cooldown_signals.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

COLLECTION_BATCH = "fresh_trades_large_day25"

PRICE_TICK = 0.1

HORIZONS = [1, 5, 10, 20, 50, 100]

TICK_DEAD_ZONES = [0.0, 0.5, 1.0, 2.0, 5.0, 10.0]
BPS_DEAD_ZONES = [0.0, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0]

COST_LEVELS_BPS = [0.5, 1.0, 2.0, 3.0, 5.0]

DATASET_SUMMARY_PATH = TABLES_DIR / "day27_dataset_summary.csv"
FUTURE_RETURN_DISTRIBUTION_PATH = TABLES_DIR / "day27_future_return_distribution.csv"
FUTURE_RETURN_BY_RUN_PATH = TABLES_DIR / "day27_future_return_distribution_by_run.csv"
LABEL_TICK_DZ_PATH = TABLES_DIR / "day27_label_distribution_tick_deadzone.csv"
LABEL_BPS_DZ_PATH = TABLES_DIR / "day27_label_distribution_bps_deadzone.csv"
LABEL_BY_RUN_PATH = TABLES_DIR / "day27_label_distribution_by_run.csv"
COST_OPPORTUNITY_PATH = TABLES_DIR / "day27_cost_opportunity_summary.csv"
DAY26_SIGNAL_MECHANICS_PATH = TABLES_DIR / "day27_day26_signal_mechanics.csv"
DAY26_SIGNAL_BY_RUN_PATH = TABLES_DIR / "day27_day26_signal_mechanics_by_run.csv"


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_large_trade_runs() -> list[str]:
    if not COLLECTION_LOG_PATH.exists():
        raise FileNotFoundError(f"Missing collection log: {COLLECTION_LOG_PATH}")

    log = pd.read_csv(COLLECTION_LOG_PATH)

    required_cols = [
        "collection_batch",
        "collection_seconds_requested",
        "status",
        "run_number",
        "run_name",
    ]

    missing = [col for col in required_cols if col not in log.columns]

    if missing:
        raise ValueError(f"Collection log missing columns: {missing}")

    large = log[
        (log["collection_batch"] == COLLECTION_BATCH) &
        (log["collection_seconds_requested"] == 300) &
        (log["status"] == "success")
    ].copy()

    large = large.sort_values("run_number")

    runs = large["run_name"].dropna().astype(str).tolist()

    if not runs:
        raise ValueError("No large fresh trade runs found.")

    return runs


def load_features(runs: list[str]) -> pd.DataFrame:
    if not FEATURES_PATH.exists():
        raise FileNotFoundError(f"Missing features file: {FEATURES_PATH}")

    df = pd.read_csv(FEATURES_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "mid_price",
        "spread_bps",
        "imbalance_5",
        "microprice_deviation_bps",
        "trade_count",
        "trade_volume",
        "signed_trade_volume",
        "trade_imbalance",
        "trade_imbalance_rolling_20e",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"trade_flow_features.csv missing columns: {missing}")

    df = df[df["run_name"].isin(runs)].copy()

    run_order = {run_name: idx for idx, run_name in enumerate(runs)}
    df["run_order"] = df["run_name"].map(run_order)

    df = df.sort_values(["run_order", "row_in_run", "event_time"]).reset_index(drop=True)

    if df.empty:
        raise ValueError("Filtered feature dataset is empty.")

    return df


def add_future_returns(df: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for run_name, group in df.groupby("run_name", sort=False):
        temp = group.copy()
        temp = temp.sort_values("row_in_run").reset_index(drop=True)

        for horizon in HORIZONS:
            future_mid = temp["mid_price"].shift(-horizon)
            future_change = future_mid - temp["mid_price"]
            future_return = future_mid / temp["mid_price"] - 1.0

            temp[f"future_mid_price_h{horizon}"] = future_mid
            temp[f"future_mid_change_h{horizon}"] = future_change
            temp[f"future_mid_change_ticks_h{horizon}"] = future_change / PRICE_TICK
            temp[f"future_mid_return_bps_h{horizon}"] = future_return * 10000.0

        frames.append(temp)

    return pd.concat(frames, ignore_index=True)


def label_from_tick_deadzone(tick_change: float, dead_zone_ticks: float) -> str:
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > dead_zone_ticks:
        return "up"

    if tick_change < -dead_zone_ticks:
        return "down"

    return "flat"


def label_from_bps_deadzone(return_bps: float, dead_zone_bps: float) -> str:
    if pd.isna(return_bps):
        return "unknown"

    if return_bps > dead_zone_bps:
        return "up"

    if return_bps < -dead_zone_bps:
        return "down"

    return "flat"


def summarize_dataset(df: pd.DataFrame, runs: list[str]) -> pd.DataFrame:
    rows = []

    rows.append(
        {
            "metric": "n_large_runs",
            "value": len(runs),
        }
    )

    rows.append(
        {
            "metric": "n_rows",
            "value": len(df),
        }
    )

    rows.append(
        {
            "metric": "first_run",
            "value": runs[0],
        }
    )

    rows.append(
        {
            "metric": "last_run",
            "value": runs[-1],
        }
    )

    rows.append(
        {
            "metric": "mean_rows_per_run",
            "value": df.groupby("run_name").size().mean(),
        }
    )

    rows.append(
        {
            "metric": "mean_spread_bps",
            "value": df["spread_bps"].mean(),
        }
    )

    rows.append(
        {
            "metric": "share_rows_with_trades",
            "value": (df["trade_count"] > 0).mean(),
        }
    )

    rows.append(
        {
            "metric": "total_trade_count_assigned",
            "value": df["trade_count"].sum(),
        }
    )

    rows.append(
        {
            "metric": "total_trade_volume_assigned",
            "value": df["trade_volume"].sum(),
        }
    )

    return pd.DataFrame(rows)


def quantile_value(series: pd.Series, q: float) -> float:
    clean = series.dropna()

    if clean.empty:
        return np.nan

    return float(clean.quantile(q))


def summarize_future_return_distribution(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for horizon in HORIZONS:
        col = f"future_mid_return_bps_h{horizon}"
        series = df[col].dropna()
        abs_series = series.abs()

        rows.append(
            {
                "horizon": horizon,
                "n": len(series),
                "mean_bps": series.mean(),
                "std_bps": series.std(),
                "min_bps": series.min(),
                "q01_bps": quantile_value(series, 0.01),
                "q05_bps": quantile_value(series, 0.05),
                "q10_bps": quantile_value(series, 0.10),
                "q25_bps": quantile_value(series, 0.25),
                "median_bps": series.median(),
                "q75_bps": quantile_value(series, 0.75),
                "q90_bps": quantile_value(series, 0.90),
                "q95_bps": quantile_value(series, 0.95),
                "q99_bps": quantile_value(series, 0.99),
                "max_bps": series.max(),
                "mean_abs_bps": abs_series.mean(),
                "median_abs_bps": abs_series.median(),
                "share_abs_ge_0_5_bps": (abs_series >= 0.5).mean(),
                "share_abs_ge_1_bps": (abs_series >= 1.0).mean(),
                "share_abs_ge_2_bps": (abs_series >= 2.0).mean(),
                "share_abs_ge_3_bps": (abs_series >= 3.0).mean(),
                "share_abs_ge_5_bps": (abs_series >= 5.0).mean(),
            }
        )

    return pd.DataFrame(rows)


def summarize_future_return_by_run(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for horizon in HORIZONS:
        col = f"future_mid_return_bps_h{horizon}"

        for run_name, group in df.groupby("run_name", sort=False):
            series = group[col].dropna()
            abs_series = series.abs()

            rows.append(
                {
                    "run_name": run_name,
                    "horizon": horizon,
                    "n": len(series),
                    "mean_bps": series.mean(),
                    "std_bps": series.std(),
                    "median_bps": series.median(),
                    "mean_abs_bps": abs_series.mean(),
                    "median_abs_bps": abs_series.median(),
                    "share_positive": (series > 0).mean(),
                    "share_abs_ge_1_bps": (abs_series >= 1.0).mean(),
                    "share_abs_ge_2_bps": (abs_series >= 2.0).mean(),
                }
            )

    return pd.DataFrame(rows)


def summarize_labels_for_tick_deadzone(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for horizon in HORIZONS:
        tick_col = f"future_mid_change_ticks_h{horizon}"
        ret_col = f"future_mid_return_bps_h{horizon}"

        for dead_zone in TICK_DEAD_ZONES:
            labels = df[tick_col].apply(
                lambda value: label_from_tick_deadzone(value, dead_zone)
            )

            counts = labels.value_counts()
            total = len(labels)

            up_count = int(counts.get("up", 0))
            down_count = int(counts.get("down", 0))
            flat_count = int(counts.get("flat", 0))
            unknown_count = int(counts.get("unknown", 0))
            nonflat_count = up_count + down_count

            nonflat_returns = df.loc[labels.isin(["up", "down"]), ret_col].dropna().abs()

            rows.append(
                {
                    "deadzone_type": "ticks",
                    "horizon": horizon,
                    "dead_zone_ticks": dead_zone,
                    "dead_zone_bps_approx_at_row_price": np.nan,
                    "total_rows": total,
                    "up_count": up_count,
                    "down_count": down_count,
                    "flat_count": flat_count,
                    "unknown_count": unknown_count,
                    "nonflat_count": nonflat_count,
                    "up_share_total": up_count / total,
                    "down_share_total": down_count / total,
                    "flat_share_total": flat_count / total,
                    "unknown_share_total": unknown_count / total,
                    "nonflat_share_total": nonflat_count / total,
                    "up_share_nonflat": up_count / nonflat_count if nonflat_count > 0 else np.nan,
                    "mean_abs_return_bps_nonflat": nonflat_returns.mean(),
                    "median_abs_return_bps_nonflat": nonflat_returns.median(),
                }
            )

    return pd.DataFrame(rows)


def summarize_labels_for_bps_deadzone(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for horizon in HORIZONS:
        ret_col = f"future_mid_return_bps_h{horizon}"

        for dead_zone in BPS_DEAD_ZONES:
            labels = df[ret_col].apply(
                lambda value: label_from_bps_deadzone(value, dead_zone)
            )

            counts = labels.value_counts()
            total = len(labels)

            up_count = int(counts.get("up", 0))
            down_count = int(counts.get("down", 0))
            flat_count = int(counts.get("flat", 0))
            unknown_count = int(counts.get("unknown", 0))
            nonflat_count = up_count + down_count

            nonflat_returns = df.loc[labels.isin(["up", "down"]), ret_col].dropna().abs()

            rows.append(
                {
                    "deadzone_type": "bps",
                    "horizon": horizon,
                    "dead_zone_bps": dead_zone,
                    "total_rows": total,
                    "up_count": up_count,
                    "down_count": down_count,
                    "flat_count": flat_count,
                    "unknown_count": unknown_count,
                    "nonflat_count": nonflat_count,
                    "up_share_total": up_count / total,
                    "down_share_total": down_count / total,
                    "flat_share_total": flat_count / total,
                    "unknown_share_total": unknown_count / total,
                    "nonflat_share_total": nonflat_count / total,
                    "up_share_nonflat": up_count / nonflat_count if nonflat_count > 0 else np.nan,
                    "mean_abs_return_bps_nonflat": nonflat_returns.mean(),
                    "median_abs_return_bps_nonflat": nonflat_returns.median(),
                }
            )

    return pd.DataFrame(rows)


def summarize_labels_by_run(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    diagnostic_specs = [
        {
            "label_spec": "current_tick_deadzone_0_5",
            "deadzone_type": "ticks",
            "deadzone_value": 0.5,
        },
        {
            "label_spec": "cost_aware_bps_deadzone_1_0",
            "deadzone_type": "bps",
            "deadzone_value": 1.0,
        },
        {
            "label_spec": "cost_aware_bps_deadzone_2_0",
            "deadzone_type": "bps",
            "deadzone_value": 2.0,
        },
    ]

    for horizon in HORIZONS:
        tick_col = f"future_mid_change_ticks_h{horizon}"
        ret_col = f"future_mid_return_bps_h{horizon}"

        for spec in diagnostic_specs:
            for run_name, group in df.groupby("run_name", sort=False):
                if spec["deadzone_type"] == "ticks":
                    labels = group[tick_col].apply(
                        lambda value: label_from_tick_deadzone(
                            value,
                            spec["deadzone_value"],
                        )
                    )
                else:
                    labels = group[ret_col].apply(
                        lambda value: label_from_bps_deadzone(
                            value,
                            spec["deadzone_value"],
                        )
                    )

                counts = labels.value_counts()
                total = len(labels)

                up_count = int(counts.get("up", 0))
                down_count = int(counts.get("down", 0))
                flat_count = int(counts.get("flat", 0))
                unknown_count = int(counts.get("unknown", 0))
                nonflat_count = up_count + down_count

                rows.append(
                    {
                        "label_spec": spec["label_spec"],
                        "deadzone_type": spec["deadzone_type"],
                        "deadzone_value": spec["deadzone_value"],
                        "run_name": run_name,
                        "horizon": horizon,
                        "total_rows": total,
                        "up_count": up_count,
                        "down_count": down_count,
                        "flat_count": flat_count,
                        "unknown_count": unknown_count,
                        "nonflat_count": nonflat_count,
                        "up_share_total": up_count / total,
                        "down_share_total": down_count / total,
                        "flat_share_total": flat_count / total,
                        "nonflat_share_total": nonflat_count / total,
                        "up_share_nonflat": up_count / nonflat_count if nonflat_count > 0 else np.nan,
                    }
                )

    return pd.DataFrame(rows)


def summarize_cost_opportunity(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for horizon in HORIZONS:
        ret_col = f"future_mid_return_bps_h{horizon}"
        returns = df[ret_col].dropna()
        abs_returns = returns.abs()

        for cost_bps in COST_LEVELS_BPS:
            rows.append(
                {
                    "horizon": horizon,
                    "round_trip_cost_bps": cost_bps,
                    "n": len(returns),
                    "share_abs_return_ge_cost": (abs_returns >= cost_bps).mean(),
                    "share_return_ge_cost": (returns >= cost_bps).mean(),
                    "share_return_le_minus_cost": (returns <= -cost_bps).mean(),
                    "mean_abs_return_bps_when_ge_cost": abs_returns[abs_returns >= cost_bps].mean(),
                    "perfect_direction_mean_net_bps": (abs_returns - cost_bps).mean(),
                    "perfect_direction_positive_net_share": ((abs_returns - cost_bps) > 0).mean(),
                }
            )

    return pd.DataFrame(rows)


def load_day26_signals() -> pd.DataFrame:
    frames = []

    if DAY26_RAW_SIGNALS_PATH.exists():
        raw = pd.read_csv(DAY26_RAW_SIGNALS_PATH)
        frames.append(raw)

    if DAY26_COOLDOWN_SIGNALS_PATH.exists():
        cooldown = pd.read_csv(DAY26_COOLDOWN_SIGNALS_PATH)
        frames.append(cooldown)

    if not frames:
        return pd.DataFrame()

    signals = pd.concat(frames, ignore_index=True, sort=False)

    required_cols = [
        "evaluation_method",
        "feature_set",
        "horizon",
        "run_name",
        "signal",
        "signed_return_bps",
        "future_mid_return_bps",
        "is_correct_signal",
        "is_positive_signed_return",
    ]

    missing = [col for col in required_cols if col not in signals.columns]

    if missing:
        raise ValueError(f"Day 26 signal files missing columns: {missing}")

    return signals


def summarize_day26_signal_mechanics(signals: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if signals.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon), group in signals.groupby(
        ["evaluation_method", "feature_set", "horizon"],
        sort=True,
    ):
        signed = group["signed_return_bps"].dropna()

        rows.append(
            {
                "evaluation_method": method,
                "feature_set": feature_set,
                "horizon": horizon,
                "n": len(group),
                "precision": group["is_correct_signal"].mean(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "mean_signed_return_bps": signed.mean(),
                "std_signed_return_bps": signed.std(),
                "min_signed_return_bps": signed.min(),
                "q05_signed_return_bps": quantile_value(signed, 0.05),
                "q25_signed_return_bps": quantile_value(signed, 0.25),
                "median_signed_return_bps": signed.median(),
                "q75_signed_return_bps": quantile_value(signed, 0.75),
                "q95_signed_return_bps": quantile_value(signed, 0.95),
                "max_signed_return_bps": signed.max(),
                "share_signed_return_ge_0_5_bps": (signed >= 0.5).mean(),
                "share_signed_return_ge_1_bps": (signed >= 1.0).mean(),
                "share_signed_return_ge_2_bps": (signed >= 2.0).mean(),
                "share_signed_return_ge_3_bps": (signed >= 3.0).mean(),
                "up_share": (group["signal"] == "up").mean(),
                "down_share": (group["signal"] == "down").mean(),
                "runs_with_signals": group["run_name"].nunique(),
            }
        )

    return pd.DataFrame(rows)


def summarize_day26_signal_by_run(signals: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if signals.empty:
        return pd.DataFrame(rows)

    for (method, feature_set, horizon, run_name), group in signals.groupby(
        ["evaluation_method", "feature_set", "horizon", "run_name"],
        sort=True,
    ):
        signed = group["signed_return_bps"].dropna()

        rows.append(
            {
                "evaluation_method": method,
                "feature_set": feature_set,
                "horizon": horizon,
                "run_name": run_name,
                "n": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": signed.mean(),
                "median_signed_return_bps": signed.median(),
                "share_signed_return_ge_1_bps": (signed >= 1.0).mean(),
                "up_count": int((group["signal"] == "up").sum()),
                "down_count": int((group["signal"] == "down").sum()),
            }
        )

    return pd.DataFrame(rows)


def save_future_return_histograms(df: pd.DataFrame) -> None:
    for horizon in [10, 20, 50]:
        col = f"future_mid_return_bps_h{horizon}"
        series = df[col].dropna()

        plt.figure(figsize=(9, 5))
        plt.hist(series, bins=100)
        plt.axvline(0)
        plt.axvline(1.0, linestyle="--")
        plt.axvline(-1.0, linestyle="--")
        plt.title(f"Future mid return distribution, h{horizon}")
        plt.xlabel("Future mid return, bps")
        plt.ylabel("Count")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"day27_future_return_hist_h{horizon}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved figure: {out_path}")


def save_flat_share_figures(tick_labels: pd.DataFrame, bps_labels: pd.DataFrame) -> None:
    plt.figure(figsize=(9, 5))

    for horizon, group in tick_labels.groupby("horizon", sort=True):
        group = group.sort_values("dead_zone_ticks")
        plt.plot(
            group["dead_zone_ticks"],
            group["flat_share_total"],
            marker="o",
            label=f"h{horizon}",
        )

    plt.title("Flat share by tick dead zone")
    plt.xlabel("Dead zone, ticks")
    plt.ylabel("Flat share")
    plt.legend()
    plt.tight_layout()

    out_path = FIGURES_DIR / "day27_flat_share_by_tick_deadzone.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")

    plt.figure(figsize=(9, 5))

    for horizon, group in bps_labels.groupby("horizon", sort=True):
        group = group.sort_values("dead_zone_bps")
        plt.plot(
            group["dead_zone_bps"],
            group["flat_share_total"],
            marker="o",
            label=f"h{horizon}",
        )

    plt.title("Flat share by bps dead zone")
    plt.xlabel("Dead zone, bps")
    plt.ylabel("Flat share")
    plt.legend()
    plt.tight_layout()

    out_path = FIGURES_DIR / "day27_flat_share_by_bps_deadzone.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")


def save_cost_opportunity_figure(cost_opportunity: pd.DataFrame) -> None:
    plt.figure(figsize=(9, 5))

    for horizon, group in cost_opportunity.groupby("horizon", sort=True):
        group = group.sort_values("round_trip_cost_bps")
        plt.plot(
            group["round_trip_cost_bps"],
            group["share_abs_return_ge_cost"],
            marker="o",
            label=f"h{horizon}",
        )

    plt.title("Share of future moves large enough to cover cost")
    plt.xlabel("Round-trip cost, bps")
    plt.ylabel("Share |future return| >= cost")
    plt.legend()
    plt.tight_layout()

    out_path = FIGURES_DIR / "day27_cost_opportunity_share.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")


def main() -> None:
    ensure_output_dirs()

    runs = load_large_trade_runs()

    print("[INFO] Large fresh runs:")
    for idx, run_name in enumerate(runs, start=1):
        print(f"  {idx}: {run_name}")

    features = load_features(runs)
    features = add_future_returns(features)

    print(f"\n[INFO] Filtered rows: {len(features)}")
    print(f"[INFO] Runs: {features['run_name'].nunique()}")

    dataset_summary = summarize_dataset(features, runs)
    future_return_distribution = summarize_future_return_distribution(features)
    future_return_by_run = summarize_future_return_by_run(features)
    tick_label_distribution = summarize_labels_for_tick_deadzone(features)
    bps_label_distribution = summarize_labels_for_bps_deadzone(features)
    label_by_run = summarize_labels_by_run(features)
    cost_opportunity = summarize_cost_opportunity(features)

    day26_signals = load_day26_signals()
    day26_signal_mechanics = summarize_day26_signal_mechanics(day26_signals)
    day26_signal_by_run = summarize_day26_signal_by_run(day26_signals)

    dataset_summary.to_csv(DATASET_SUMMARY_PATH, index=False)
    future_return_distribution.to_csv(FUTURE_RETURN_DISTRIBUTION_PATH, index=False)
    future_return_by_run.to_csv(FUTURE_RETURN_BY_RUN_PATH, index=False)
    tick_label_distribution.to_csv(LABEL_TICK_DZ_PATH, index=False)
    bps_label_distribution.to_csv(LABEL_BPS_DZ_PATH, index=False)
    label_by_run.to_csv(LABEL_BY_RUN_PATH, index=False)
    cost_opportunity.to_csv(COST_OPPORTUNITY_PATH, index=False)

    if not day26_signal_mechanics.empty:
        day26_signal_mechanics.to_csv(DAY26_SIGNAL_MECHANICS_PATH, index=False)

    if not day26_signal_by_run.empty:
        day26_signal_by_run.to_csv(DAY26_SIGNAL_BY_RUN_PATH, index=False)

    save_future_return_histograms(features)
    save_flat_share_figures(tick_label_distribution, bps_label_distribution)
    save_cost_opportunity_figure(cost_opportunity)

    print("\n" + "=" * 80)
    print("[INFO] Saved outputs")
    print("=" * 80)

    output_paths = [
        DATASET_SUMMARY_PATH,
        FUTURE_RETURN_DISTRIBUTION_PATH,
        FUTURE_RETURN_BY_RUN_PATH,
        LABEL_TICK_DZ_PATH,
        LABEL_BPS_DZ_PATH,
        LABEL_BY_RUN_PATH,
        COST_OPPORTUNITY_PATH,
        DAY26_SIGNAL_MECHANICS_PATH,
        DAY26_SIGNAL_BY_RUN_PATH,
    ]

    for path in output_paths:
        if path.exists():
            print(f"[INFO] {path}")

    print("\n[INFO] Dataset summary:")
    print(dataset_summary)

    print("\n[INFO] Future return distribution:")
    print(future_return_distribution)

    print("\n[INFO] Tick dead-zone label distribution, current 0.5 tick rows:")
    current_tick = tick_label_distribution[tick_label_distribution["dead_zone_ticks"] == 0.5]
    print(current_tick)

    print("\n[INFO] Bps dead-zone label distribution, 1 bps rows:")
    one_bps = bps_label_distribution[bps_label_distribution["dead_zone_bps"] == 1.0]
    print(one_bps)

    print("\n[INFO] Cost opportunity summary:")
    print(cost_opportunity)

    print("\n[INFO] Day 27 diagnostics completed successfully.")


if __name__ == "__main__":
    main()