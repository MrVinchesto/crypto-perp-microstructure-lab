from pathlib import Path

import pandas as pd


INPUT_PATH = Path("data/processed/top_of_book.csv")
OUTPUT_PATH = Path("data/processed/basic_features.csv")


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)

    # На всякий случай сортируем по event_time
    df = df.sort_values("event_time").reset_index(drop=True)

    # Предыдущее значение mid-price
    df["mid_price_prev"] = df["mid_price"].shift(1)

    # Сколько времени прошло между соседними событиями
    df["event_gap_ms"] = df["event_time"].diff()

    # Разница между event time и transaction time
    df["transaction_lag_ms"] = df["event_time"] - df["transaction_time"]

    # Абсолютное изменение mid-price
    df["mid_price_change"] = df["mid_price"] - df["mid_price_prev"]

    # Доходность mid-price
    df["mid_return"] = df["mid_price"] / df["mid_price_prev"] - 1

    # Доходность в bps
    df["mid_return_bps"] = df["mid_return"] * 10000

    # Спред в bps
    df["spread_bps"] = df["spread"] / df["mid_price"] * 10000

    # Изменился ли лучший bid или лучший ask
    df["best_bid_prev"] = df["best_bid"].shift(1)
    df["best_ask_prev"] = df["best_ask"].shift(1)

    df["quote_changed"] = (
        (df["best_bid"] != df["best_bid_prev"]) |
        (df["best_ask"] != df["best_ask_prev"])
    ).astype(int)

    # Сохраняем результат
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"[INFO] Saved features to: {OUTPUT_PATH}")
    print("[INFO] First rows:")
    print(df.head())

    print("\n[INFO] Summary stats:")
    cols = [
        "event_gap_ms",
        "transaction_lag_ms",
        "spread",
        "spread_bps",
        "mid_price_change",
        "mid_return_bps",
    ]
    print(df[cols].describe())

    print("\n[INFO] Quote change frequency:")
    print(df["quote_changed"].value_counts(dropna=False))


if __name__ == "__main__":
    main()