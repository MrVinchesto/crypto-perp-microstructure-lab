from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = Path("data/processed/basic_features.csv")

PROCESSED_DIR = Path("data/processed")
REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

LABELED_DATASET_PATH = PROCESSED_DIR / "labeled_dataset.csv"
NONFLAT_DATASET_PATH = PROCESSED_DIR / "modeling_dataset_nonflat.csv"

PRICE_TICK = 0.1
DEAD_ZONE_TICKS = 0.5
HORIZONS = [1, 5, 10]


def ensure_output_dirs() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_features() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH)
    df = df.sort_values("event_time").reset_index(drop=True)

    return df


def add_microprice_deviation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "microprice" not in df.columns:
        raise ValueError("Column 'microprice' is missing. Rebuild top_of_book and basic_features first.")

    df["microprice_deviation"] = df["microprice"] - df["mid_price"]
    df["microprice_deviation_bps"] = (
        df["microprice_deviation"] / df["mid_price"] * 10000
    )

    return df


def label_from_tick_change(tick_change: float) -> str:
    if pd.isna(tick_change):
        return "unknown"

    if tick_change > DEAD_ZONE_TICKS:
        return "up"

    if tick_change < -DEAD_ZONE_TICKS:
        return "down"

    return "flat"


def add_future_targets_and_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

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


def save_labeled_datasets(df: pd.DataFrame) -> None:
    df.to_csv(LABELED_DATASET_PATH, index=False)
    print(f"[INFO] Saved labeled dataset to: {LABELED_DATASET_PATH}")

    nonflat_frames = []

    for h in HORIZONS:
        label_col = f"label_{h}e"
        temp = df[df[label_col].isin(["up", "down"])].copy()
        temp["target_horizon"] = h
        temp["target_label"] = temp[label_col]
        nonflat_frames.append(temp)

    nonflat = pd.concat(nonflat_frames, ignore_index=True)
    nonflat.to_csv(NONFLAT_DATASET_PATH, index=False)

    print(f"[INFO] Saved non-flat modeling dataset to: {NONFLAT_DATASET_PATH}")


def build_label_distribution_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for h in HORIZONS:
        label_col = f"label_{h}e"
        counts = df[label_col].value_counts(dropna=False)
        shares = df[label_col].value_counts(normalize=True, dropna=False)

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
    out_path = TABLES_DIR / "label_distribution.csv"
    result.to_csv(out_path, index=False)

    print(f"[INFO] Saved label distribution to: {out_path}")

    return result


def build_target_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = []

    for h in HORIZONS:
        cols.extend(
            [
                f"future_mid_change_{h}e",
                f"future_mid_change_ticks_{h}e",
                f"future_mid_return_{h}e_bps",
            ]
        )

    summary = df[cols].describe().T
    out_path = TABLES_DIR / "target_summary.csv"
    summary.to_csv(out_path)

    print(f"[INFO] Saved target summary to: {out_path}")

    return summary


def build_nonflat_baseline_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for h in HORIZONS:
        label_col = f"label_{h}e"

        nonflat = df[df[label_col].isin(["up", "down"])].copy()

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
    out_path = TABLES_DIR / "nonflat_baseline.csv"
    result.to_csv(out_path, index=False)

    print(f"[INFO] Saved non-flat baseline table to: {out_path}")

    return result


def save_label_distribution_plots(label_distribution: pd.DataFrame) -> None:
    for h in HORIZONS:
        subset = label_distribution[label_distribution["horizon_events"] == h].copy()
        subset = subset[subset["label"].isin(["down", "flat", "up"])]

        plt.figure()
        plt.bar(subset["label"], subset["share"])
        plt.title(f"Label distribution, horizon={h} events")
        plt.xlabel("Label")
        plt.ylabel("Share")
        plt.ylim(0, 1)
        plt.tight_layout()

        out_path = FIGURES_DIR / f"label_distribution_h{h}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved figure to: {out_path}")


def print_diagnostics(
    label_distribution: pd.DataFrame,
    target_summary: pd.DataFrame,
    nonflat_baseline: pd.DataFrame,
) -> None:
    print("\n[INFO] Label distribution:")
    print(label_distribution)

    print("\n[INFO] Target summary:")
    print(target_summary)

    print("\n[INFO] Non-flat baseline:")
    print(nonflat_baseline)


def main() -> None:
    ensure_output_dirs()

    df = load_features()
    df = add_microprice_deviation(df)
    df = add_future_targets_and_labels(df)

    save_labeled_datasets(df)

    label_distribution = build_label_distribution_table(df)
    target_summary = build_target_summary_table(df)
    nonflat_baseline = build_nonflat_baseline_table(df)

    save_label_distribution_plots(label_distribution)

    print_diagnostics(
        label_distribution=label_distribution,
        target_summary=target_summary,
        nonflat_baseline=nonflat_baseline,
    )

    print("\n[INFO] Labeling pipeline completed successfully.")


if __name__ == "__main__":
    main()