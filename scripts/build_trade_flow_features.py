import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


RAW_ROOT = Path("data/raw/BTCUSDT")
BASIC_FEATURES_PATH = Path("data/processed/basic_features_all.csv")
RAW_INVENTORY_PATH = Path("reports/tables/raw_runs_inventory.csv")

OUTPUT_PATH = Path("data/processed/trade_flow_features.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")

SUMMARY_PATH = TABLES_DIR / "trade_flow_feature_summary.csv"
RUN_SUMMARY_PATH = TABLES_DIR / "trade_flow_feature_summary_by_run.csv"
CORRELATION_PATH = TABLES_DIR / "trade_flow_correlation_table.csv"


ROLLING_WINDOWS_EVENTS = [5, 10, 20, 50]
EPS = 1e-12


def ensure_output_dirs() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_basic_features() -> pd.DataFrame:
    if not BASIC_FEATURES_PATH.exists():
        raise FileNotFoundError(
            f"Basic features file not found: {BASIC_FEATURES_PATH}"
        )

    df = pd.read_csv(BASIC_FEATURES_PATH)

    required_cols = [
        "run_name",
        "row_in_run",
        "event_time",
        "transaction_time",
        "mid_price",
        "mid_return_bps",
        "spread_bps",
        "imbalance_5",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"basic_features_all.csv missing columns: {missing}")

    df = df.sort_values(["run_name", "event_time", "row_in_run"]).reset_index(drop=True)

    return df


def load_inventory() -> pd.DataFrame:
    if not RAW_INVENTORY_PATH.exists():
        raise FileNotFoundError(
            f"Raw inventory not found: {RAW_INVENTORY_PATH}"
        )

    inventory = pd.read_csv(RAW_INVENTORY_PATH)

    required_cols = [
        "run_name",
        "has_trades",
        "trade_events",
    ]

    missing = [col for col in required_cols if col not in inventory.columns]

    if missing:
        raise ValueError(f"raw_runs_inventory.csv missing columns: {missing}")

    return inventory


def get_runs_with_trades(inventory: pd.DataFrame) -> list[str]:
    runs = inventory[
        (inventory["has_trades"] == True) &
        (inventory["trade_events"] > 0)
    ]["run_name"].astype(str).tolist()

    if not runs:
        raise ValueError("No raw runs with trade_events > 0 found.")

    return runs


def load_trades_for_run(run_name: str) -> pd.DataFrame:
    trades_path = RAW_ROOT / run_name / "trades.jsonl"

    if not trades_path.exists():
        raise FileNotFoundError(f"Trades file not found: {trades_path}")

    rows = []

    with trades_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            event = json.loads(line)

            price = float(event["p"])
            quantity = float(event["q"])
            trade_time = int(event["T"])
            event_time = int(event["E"])
            buyer_is_maker = bool(event["m"])

            # Binance aggTrade convention:
            # m = True  -> buyer is maker -> seller is taker -> aggressive sell
            # m = False -> buyer is taker -> aggressive buy
            aggressive_side = "sell" if buyer_is_maker else "buy"
            signed_quantity = quantity if aggressive_side == "buy" else -quantity
            notional = price * quantity
            signed_notional = notional if aggressive_side == "buy" else -notional

            rows.append(
                {
                    "run_name": run_name,
                    "trade_event_time": event_time,
                    "trade_time": trade_time,
                    "agg_trade_id": event.get("a"),
                    "price": price,
                    "quantity": quantity,
                    "notional": notional,
                    "buyer_is_maker": buyer_is_maker,
                    "aggressive_side": aggressive_side,
                    "signed_quantity": signed_quantity,
                    "signed_notional": signed_notional,
                }
            )

    trades = pd.DataFrame(rows)

    if trades.empty:
        return trades

    trades = trades.sort_values("trade_time").reset_index(drop=True)

    return trades


def initialize_trade_feature_columns(run_df: pd.DataFrame) -> pd.DataFrame:
    df = run_df.copy()

    zero_float_cols = [
        "trade_count",
        "buy_trade_count",
        "sell_trade_count",
        "trade_volume",
        "buy_trade_volume",
        "sell_trade_volume",
        "signed_trade_volume",
        "trade_notional",
        "buy_trade_notional",
        "sell_trade_notional",
        "signed_trade_notional",
        "avg_trade_size",
        "avg_trade_notional",
        "vwap_trade_price",
        "trade_imbalance",
        "notional_imbalance",
        "trade_intensity_per_second",
    ]

    for col in zero_float_cols:
        df[col] = 0.0

    return df


def aggregate_trades_to_orderbook_rows(run_df: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    df = initialize_trade_feature_columns(run_df)
    df = df.sort_values(["event_time", "row_in_run"]).reset_index(drop=True)

    if trades.empty:
        return add_rolling_trade_features(df)

    event_times = df["event_time"].astype("int64").to_numpy()
    trade_times = trades["trade_time"].astype("int64").to_numpy()

    # For each trade, find the first order-book row whose event_time is >= trade_time.
    # This assigns trades to the interval ending at the current order-book event.
    row_positions = np.searchsorted(event_times, trade_times, side="left")

    valid_mask = row_positions < len(df)

    if not valid_mask.any():
        return add_rolling_trade_features(df)

    assigned = trades.loc[valid_mask].copy()
    assigned["row_position"] = row_positions[valid_mask]

    grouped = assigned.groupby("row_position")

    for row_position, group in grouped:
        buy_mask = group["aggressive_side"] == "buy"
        sell_mask = group["aggressive_side"] == "sell"

        trade_count = len(group)
        buy_trade_count = int(buy_mask.sum())
        sell_trade_count = int(sell_mask.sum())

        trade_volume = group["quantity"].sum()
        buy_trade_volume = group.loc[buy_mask, "quantity"].sum()
        sell_trade_volume = group.loc[sell_mask, "quantity"].sum()
        signed_trade_volume = group["signed_quantity"].sum()

        trade_notional = group["notional"].sum()
        buy_trade_notional = group.loc[buy_mask, "notional"].sum()
        sell_trade_notional = group.loc[sell_mask, "notional"].sum()
        signed_trade_notional = group["signed_notional"].sum()

        avg_trade_size = group["quantity"].mean()
        avg_trade_notional = group["notional"].mean()

        if trade_volume > 0:
            vwap_trade_price = trade_notional / trade_volume
        else:
            vwap_trade_price = np.nan

        trade_imbalance = signed_trade_volume / (trade_volume + EPS)
        notional_imbalance = signed_trade_notional / (trade_notional + EPS)

        df.loc[row_position, "trade_count"] = trade_count
        df.loc[row_position, "buy_trade_count"] = buy_trade_count
        df.loc[row_position, "sell_trade_count"] = sell_trade_count
        df.loc[row_position, "trade_volume"] = trade_volume
        df.loc[row_position, "buy_trade_volume"] = buy_trade_volume
        df.loc[row_position, "sell_trade_volume"] = sell_trade_volume
        df.loc[row_position, "signed_trade_volume"] = signed_trade_volume
        df.loc[row_position, "trade_notional"] = trade_notional
        df.loc[row_position, "buy_trade_notional"] = buy_trade_notional
        df.loc[row_position, "sell_trade_notional"] = sell_trade_notional
        df.loc[row_position, "signed_trade_notional"] = signed_trade_notional
        df.loc[row_position, "avg_trade_size"] = avg_trade_size
        df.loc[row_position, "avg_trade_notional"] = avg_trade_notional
        df.loc[row_position, "vwap_trade_price"] = vwap_trade_price
        df.loc[row_position, "trade_imbalance"] = trade_imbalance
        df.loc[row_position, "notional_imbalance"] = notional_imbalance

    df = add_trade_intensity(df)
    df = add_rolling_trade_features(df)

    return df


def add_trade_intensity(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    if "event_gap_ms" in result.columns:
        gap_seconds = result["event_gap_ms"] / 1000.0
    else:
        gap_seconds = result["event_time"].diff() / 1000.0

    gap_seconds = gap_seconds.replace(0, np.nan)

    result["trade_intensity_per_second"] = result["trade_count"] / gap_seconds
    result["trade_intensity_per_second"] = result["trade_intensity_per_second"].replace(
        [np.inf, -np.inf],
        np.nan,
    )
    result["trade_intensity_per_second"] = result["trade_intensity_per_second"].fillna(0.0)

    return result


def add_rolling_trade_features(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    for window in ROLLING_WINDOWS_EVENTS:
        suffix = f"{window}e"

        result[f"trade_count_rolling_sum_{suffix}"] = (
            result["trade_count"].rolling(window, min_periods=1).sum()
        )

        result[f"trade_volume_rolling_sum_{suffix}"] = (
            result["trade_volume"].rolling(window, min_periods=1).sum()
        )

        result[f"signed_trade_volume_rolling_sum_{suffix}"] = (
            result["signed_trade_volume"].rolling(window, min_periods=1).sum()
        )

        result[f"trade_notional_rolling_sum_{suffix}"] = (
            result["trade_notional"].rolling(window, min_periods=1).sum()
        )

        result[f"signed_trade_notional_rolling_sum_{suffix}"] = (
            result["signed_trade_notional"].rolling(window, min_periods=1).sum()
        )

        result[f"buy_trade_volume_rolling_sum_{suffix}"] = (
            result["buy_trade_volume"].rolling(window, min_periods=1).sum()
        )

        result[f"sell_trade_volume_rolling_sum_{suffix}"] = (
            result["sell_trade_volume"].rolling(window, min_periods=1).sum()
        )

        total_volume = result[f"trade_volume_rolling_sum_{suffix}"]
        signed_volume = result[f"signed_trade_volume_rolling_sum_{suffix}"]

        result[f"trade_imbalance_rolling_{suffix}"] = (
            signed_volume / (total_volume + EPS)
        )

        total_notional = result[f"trade_notional_rolling_sum_{suffix}"]
        signed_notional = result[f"signed_trade_notional_rolling_sum_{suffix}"]

        result[f"notional_imbalance_rolling_{suffix}"] = (
            signed_notional / (total_notional + EPS)
        )

        result[f"trade_intensity_rolling_mean_{suffix}"] = (
            result["trade_intensity_per_second"].rolling(window, min_periods=1).mean()
        )

    return result


def build_trade_flow_features() -> pd.DataFrame:
    basic = load_basic_features()
    inventory = load_inventory()
    runs_with_trades = get_runs_with_trades(inventory)

    print(f"[INFO] Runs with trade_events > 0: {runs_with_trades}")

    frames = []

    for run_name in runs_with_trades:
        run_df = basic[basic["run_name"] == run_name].copy()

        if run_df.empty:
            print(
                f"[WARNING] Run {run_name} has trades but is missing from basic_features_all.csv. Skipping."
            )
            continue

        trades = load_trades_for_run(run_name)

        print(
            f"[INFO] Processing run {run_name}: "
            f"book_rows={len(run_df)}, trades={len(trades)}"
        )

        enriched = aggregate_trades_to_orderbook_rows(run_df=run_df, trades=trades)
        frames.append(enriched)

    if not frames:
        raise ValueError("No trade-flow feature frames were created.")

    result = pd.concat(frames, ignore_index=True)

    return result


def create_summary(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        "trade_count",
        "buy_trade_count",
        "sell_trade_count",
        "trade_volume",
        "buy_trade_volume",
        "sell_trade_volume",
        "signed_trade_volume",
        "trade_notional",
        "buy_trade_notional",
        "sell_trade_notional",
        "signed_trade_notional",
        "trade_imbalance",
        "notional_imbalance",
        "trade_intensity_per_second",
    ]

    summary = df[numeric_cols].describe().T.reset_index()
    summary = summary.rename(columns={"index": "feature"})

    return summary


def create_summary_by_run(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for run_name, group in df.groupby("run_name", sort=True):
        rows.append(
            {
                "run_name": run_name,
                "rows": len(group),
                "rows_with_trades": int((group["trade_count"] > 0).sum()),
                "share_rows_with_trades": (group["trade_count"] > 0).mean(),
                "total_trades_assigned": group["trade_count"].sum(),
                "total_buy_trades": group["buy_trade_count"].sum(),
                "total_sell_trades": group["sell_trade_count"].sum(),
                "total_trade_volume": group["trade_volume"].sum(),
                "total_buy_trade_volume": group["buy_trade_volume"].sum(),
                "total_sell_trade_volume": group["sell_trade_volume"].sum(),
                "total_signed_trade_volume": group["signed_trade_volume"].sum(),
                "mean_trade_imbalance": group["trade_imbalance"].mean(),
                "mean_trade_imbalance_rolling_20e": group["trade_imbalance_rolling_20e"].mean(),
            }
        )

    return pd.DataFrame(rows)


def create_correlation_table(df: pd.DataFrame) -> pd.DataFrame:
    feature_cols = [
        "trade_count",
        "trade_volume",
        "signed_trade_volume",
        "trade_imbalance",
        "notional_imbalance",
        "trade_imbalance_rolling_5e",
        "trade_imbalance_rolling_10e",
        "trade_imbalance_rolling_20e",
        "trade_imbalance_rolling_50e",
        "signed_trade_volume_rolling_sum_20e",
        "trade_intensity_rolling_mean_20e",
        "imbalance_5",
        "microprice_deviation_bps",
    ]

    target_cols = []

    for horizon in [1, 5, 10, 20, 50]:
        target_col = f"future_mid_return_bps_h{horizon}"

        df[target_col] = (
            df.groupby("run_name")["mid_price"].shift(-horizon) / df["mid_price"] - 1.0
        ) * 10000.0

        target_cols.append(target_col)

    rows = []

    for feature in feature_cols:
        if feature not in df.columns:
            continue

        for target in target_cols:
            temp = df[[feature, target]].dropna()

            if len(temp) < 10:
                corr = np.nan
                n = len(temp)
            else:
                corr = temp[feature].corr(temp[target])
                n = len(temp)

            rows.append(
                {
                    "feature": feature,
                    "target": target,
                    "pearson_corr": corr,
                    "n": n,
                }
            )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        ["target", "pearson_corr"],
        ascending=[True, False],
    ).reset_index(drop=True)

    return result


def save_figures(df: pd.DataFrame) -> None:
    # Figure 1: rolling trade imbalance
    plt.figure(figsize=(12, 6))

    for run_name, group in df.groupby("run_name", sort=True):
        group = group.sort_values("row_in_run")
        plt.plot(
            group["row_in_run"],
            group["trade_imbalance_rolling_20e"],
            label=run_name,
        )

    plt.axhline(0)
    plt.title("Rolling trade imbalance, 20 events")
    plt.xlabel("Row in run")
    plt.ylabel("Trade imbalance")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / "trade_flow_rolling_imbalance_20e.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")

    # Figure 2: signed trade volume rolling sum
    plt.figure(figsize=(12, 6))

    for run_name, group in df.groupby("run_name", sort=True):
        group = group.sort_values("row_in_run")
        plt.plot(
            group["row_in_run"],
            group["signed_trade_volume_rolling_sum_20e"],
            label=run_name,
        )

    plt.axhline(0)
    plt.title("Rolling signed trade volume, 20 events")
    plt.xlabel("Row in run")
    plt.ylabel("Signed trade volume")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / "trade_flow_signed_volume_20e.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")

    # Figure 3: trade count over order book rows
    plt.figure(figsize=(12, 6))

    for run_name, group in df.groupby("run_name", sort=True):
        group = group.sort_values("row_in_run")
        plt.plot(
            group["row_in_run"],
            group["trade_count_rolling_sum_20e"],
            label=run_name,
        )

    plt.title("Rolling trade count, 20 events")
    plt.xlabel("Row in run")
    plt.ylabel("Trade count")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / "trade_flow_trade_count_20e.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved figure: {out_path}")


def main() -> None:
    ensure_output_dirs()

    features = build_trade_flow_features()
    features.to_csv(OUTPUT_PATH, index=False)

    summary = create_summary(features)
    summary_by_run = create_summary_by_run(features)
    correlation = create_correlation_table(features.copy())

    summary.to_csv(SUMMARY_PATH, index=False)
    summary_by_run.to_csv(RUN_SUMMARY_PATH, index=False)
    correlation.to_csv(CORRELATION_PATH, index=False)

    save_figures(features)

    print(f"\n[INFO] Saved trade-flow features to: {OUTPUT_PATH}")
    print(f"[INFO] Saved summary to: {SUMMARY_PATH}")
    print(f"[INFO] Saved summary by run to: {RUN_SUMMARY_PATH}")
    print(f"[INFO] Saved correlation table to: {CORRELATION_PATH}")

    print("\n[INFO] Summary by run:")
    print(summary_by_run)

    print("\n[INFO] Top correlations:")
    print(correlation.head(20))

    print("\n[INFO] Trade-flow feature build completed successfully.")


if __name__ == "__main__":
    main()