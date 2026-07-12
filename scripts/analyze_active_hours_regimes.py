from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ============================================================================
# Configuration
# ============================================================================

TRADE_FLOW_PATH = Path("data/processed/trade_flow_features.csv")
COLLECTION_LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")
TRADE_FLOW_SUMMARY_BY_RUN_PATH = Path(
    "reports/tables/trade_flow_feature_summary_by_run.csv"
)

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)


BATCH_TO_REGIME = {
    "fresh_trades_large_day25": "Saturday Day 25",
    "weekday_active_tue_day28": "Tuesday Day 28",
    "weekday_active_wed_day29": "Wednesday Day 29",
    "weekday_active_thu_day30": "Thursday Day 30",
}

REGIME_ORDER = list(BATCH_TO_REGIME.values())

HORIZONS = [10, 20, 50, 100]
BPS_THRESHOLDS = [0.5, 1.0, 2.0, 3.0, 5.0]
LABEL_DEAD_ZONES_BPS = [0.5, 1.0, 2.0]

MIN_DEPTH_EVENTS = 2800
REQUIRED_COLLECTION_SECONDS = 300


# ============================================================================
# Helper functions
# ============================================================================

def require_columns(
    df: pd.DataFrame,
    required_columns: list[str],
    dataframe_name: str,
) -> None:
    """Raise a clear error if an input table is missing required columns."""
    missing = [column for column in required_columns if column not in df.columns]

    if missing:
        raise ValueError(
            f"{dataframe_name} is missing required columns: {missing}"
        )


def safe_share(mask: pd.Series) -> float:
    """Return the mean of a boolean mask, or NaN when it is empty."""
    if len(mask) == 0:
        return float("nan")

    return float(mask.mean())


def save_figure(path: Path) -> None:
    """Apply a standard layout, save the current figure, and close it."""
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()

    print(f"[INFO] Saved figure: {path}")


# ============================================================================
# Load collection log and processed run inventory
# ============================================================================

print("[INFO] Loading collection log...")

log = pd.read_csv(COLLECTION_LOG_PATH)

require_columns(
    log,
    [
        "collection_batch",
        "run_name",
        "status",
        "depth_events",
        "trade_events",
        "collection_seconds_requested",
    ],
    "fresh_trade_collection_log.csv",
)

for column in [
    "depth_events",
    "trade_events",
    "collection_seconds_requested",
]:
    log[column] = pd.to_numeric(log[column], errors="coerce")

selected_log = log[
    log["collection_batch"].isin(BATCH_TO_REGIME.keys())
].copy()

selected_log["run_name"] = selected_log["run_name"].astype(str)
selected_log["regime"] = selected_log["collection_batch"].map(BATCH_TO_REGIME)


print("[INFO] Loading trade-flow features run inventory...")

trade_flow_run_inventory = pd.read_csv(
    TRADE_FLOW_PATH,
    usecols=["run_name"],
)

available_processed_runs = set(
    trade_flow_run_inventory["run_name"].dropna().astype(str).unique()
)


# ============================================================================
# Build strict quality filter
# ============================================================================

selected_log["is_success"] = selected_log["status"].eq("success")

selected_log["has_full_duration"] = (
    selected_log["collection_seconds_requested"]
    .eq(REQUIRED_COLLECTION_SECONDS)
)

selected_log["has_sufficient_depth"] = (
    selected_log["depth_events"].ge(MIN_DEPTH_EVENTS)
)

selected_log["has_trades"] = selected_log["trade_events"].gt(0)

selected_log["available_in_trade_flow_features"] = (
    selected_log["run_name"].isin(available_processed_runs)
)

selected_log["strict_valid"] = (
    selected_log["is_success"]
    & selected_log["has_full_duration"]
    & selected_log["has_sufficient_depth"]
    & selected_log["has_trades"]
    & selected_log["available_in_trade_flow_features"]
)


# ============================================================================
# Explain excluded runs
# ============================================================================

def exclusion_reason(row: pd.Series) -> str:
    reasons: list[str] = []

    if not row["is_success"]:
        reasons.append("collection_failed")

    if not row["has_full_duration"]:
        reasons.append("incomplete_duration")

    if not row["has_sufficient_depth"]:
        reasons.append("low_or_missing_depth")

    if not row["has_trades"]:
        reasons.append("no_trades")

    if not row["available_in_trade_flow_features"]:
        reasons.append("missing_processed_features")

    return ";".join(reasons)


selected_log["exclusion_reason"] = selected_log.apply(
    exclusion_reason,
    axis=1,
)

excluded_runs = selected_log[
    ~selected_log["strict_valid"]
][
    [
        "collection_batch",
        "regime",
        "run_name",
        "status",
        "depth_events",
        "trade_events",
        "collection_seconds_requested",
        "available_in_trade_flow_features",
        "exclusion_reason",
    ]
].copy()

excluded_runs.to_csv(
    TABLES_DIR / "day31_excluded_runs.csv",
    index=False,
)


# ============================================================================
# Quality-filter summary
# ============================================================================

quality_rows: list[dict[str, object]] = []

for batch, regime in BATCH_TO_REGIME.items():
    batch_log = selected_log[
        selected_log["collection_batch"].eq(batch)
    ].copy()

    successful = batch_log[
        batch_log["is_success"]
        & batch_log["has_full_duration"]
    ]

    depth_quality = successful[
        successful["has_sufficient_depth"]
        & successful["has_trades"]
    ]

    strict_valid = batch_log[
        batch_log["strict_valid"]
    ]

    quality_rows.append(
        {
            "collection_batch": batch,
            "regime": regime,
            "log_rows": len(batch_log),
            "successful_300s_runs": successful["run_name"].nunique(),
            "depth_and_trade_quality_runs": depth_quality["run_name"].nunique(),
            "strict_processed_runs": strict_valid["run_name"].nunique(),
            "excluded_runs": (
                batch_log["run_name"].nunique()
                - strict_valid["run_name"].nunique()
            ),
        }
    )

quality_summary = pd.DataFrame(quality_rows)

quality_summary["regime"] = pd.Categorical(
    quality_summary["regime"],
    categories=REGIME_ORDER,
    ordered=True,
)

quality_summary = quality_summary.sort_values("regime")

quality_summary.to_csv(
    TABLES_DIR / "day31_quality_filter_summary.csv",
    index=False,
)


# Keep exactly one metadata row per strict-valid run.
strict_log = (
    selected_log[selected_log["strict_valid"]]
    .drop_duplicates(subset=["run_name"], keep="last")
    .copy()
)

strict_runs = set(strict_log["run_name"])

run_to_regime = strict_log.set_index("run_name")["regime"].to_dict()
run_to_batch = strict_log.set_index("run_name")["collection_batch"].to_dict()


# ============================================================================
# Activity comparison
# ============================================================================

print("[INFO] Loading trade-flow summary by run...")

activity_by_run = pd.read_csv(TRADE_FLOW_SUMMARY_BY_RUN_PATH)

require_columns(
    activity_by_run,
    [
        "run_name",
        "total_trades_assigned",
        "share_rows_with_trades",
        "total_trade_volume",
    ],
    "trade_flow_feature_summary_by_run.csv",
)

activity_by_run["run_name"] = activity_by_run["run_name"].astype(str)

activity_by_run = activity_by_run[
    activity_by_run["run_name"].isin(strict_runs)
].copy()

activity_by_run["collection_batch"] = (
    activity_by_run["run_name"].map(run_to_batch)
)

activity_by_run["regime"] = (
    activity_by_run["run_name"].map(run_to_regime)
)

activity_by_run["regime"] = pd.Categorical(
    activity_by_run["regime"],
    categories=REGIME_ORDER,
    ordered=True,
)

activity_by_run = activity_by_run.sort_values(
    ["regime", "run_name"]
)

activity_by_run.to_csv(
    TABLES_DIR / "day31_activity_by_run.csv",
    index=False,
)

activity_summary = (
    activity_by_run
    .groupby("regime", observed=True)
    .agg(
        n_runs=("run_name", "nunique"),
        mean_trades_per_run=("total_trades_assigned", "mean"),
        median_trades_per_run=("total_trades_assigned", "median"),
        mean_share_rows_with_trades=("share_rows_with_trades", "mean"),
        median_share_rows_with_trades=("share_rows_with_trades", "median"),
        mean_trade_volume_per_run=("total_trade_volume", "mean"),
        median_trade_volume_per_run=("total_trade_volume", "median"),
    )
    .reset_index()
)

activity_summary.to_csv(
    TABLES_DIR / "day31_activity_summary.csv",
    index=False,
)


# ============================================================================
# Load only columns needed for future-return analysis
# ============================================================================

print("[INFO] Loading price data for strict-valid runs...")

price_data = pd.read_csv(
    TRADE_FLOW_PATH,
    usecols=["run_name", "row_in_run", "mid_price"],
)

require_columns(
    price_data,
    ["run_name", "row_in_run", "mid_price"],
    "trade_flow_features.csv",
)

price_data["run_name"] = price_data["run_name"].astype(str)
price_data["mid_price"] = pd.to_numeric(
    price_data["mid_price"],
    errors="coerce",
)

price_data = price_data[
    price_data["run_name"].isin(strict_runs)
].copy()

price_data["regime"] = price_data["run_name"].map(run_to_regime)
price_data["collection_batch"] = price_data["run_name"].map(run_to_batch)

price_data = price_data.sort_values(
    ["run_name", "row_in_run"]
).reset_index(drop=True)


# ============================================================================
# Build future returns within each run
# ============================================================================

print("[INFO] Building future returns...")

for horizon in HORIZONS:
    future_mid = (
        price_data
        .groupby("run_name", sort=False)["mid_price"]
        .shift(-horizon)
    )

    price_data[f"future_mid_return_bps_h{horizon}"] = (
        (future_mid / price_data["mid_price"]) - 1.0
    ) * 10_000.0


# ============================================================================
# Future-return distribution by regime
# ============================================================================

future_summary_rows: list[dict[str, object]] = []
future_by_run_rows: list[dict[str, object]] = []
label_rows: list[dict[str, object]] = []
cost_rows: list[dict[str, object]] = []


for regime in REGIME_ORDER:
    regime_data = price_data[
        price_data["regime"].eq(regime)
    ]

    for horizon in HORIZONS:
        return_column = f"future_mid_return_bps_h{horizon}"

        returns = (
            regime_data[return_column]
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )

        absolute_returns = returns.abs()

        summary_row: dict[str, object] = {
            "regime": regime,
            "horizon_events": horizon,
            "n_observations": len(returns),
            "mean_return_bps": returns.mean(),
            "mean_abs_return_bps": absolute_returns.mean(),
            "median_abs_return_bps": absolute_returns.median(),
            "p90_abs_return_bps": absolute_returns.quantile(0.90),
            "p95_abs_return_bps": absolute_returns.quantile(0.95),
        }

        for threshold in BPS_THRESHOLDS:
            column_name = (
                f"share_abs_return_ge_"
                f"{str(threshold).replace('.', '_')}bps"
            )

            summary_row[column_name] = safe_share(
                absolute_returns.ge(threshold)
            )

        future_summary_rows.append(summary_row)

        # Cost opportunity:
        # mean(abs(return)) - cost is a theoretical perfect-direction upper bound
        # if every row were traded.
        for cost_bps in BPS_THRESHOLDS:
            cost_rows.append(
                {
                    "regime": regime,
                    "horizon_events": horizon,
                    "round_trip_cost_bps": cost_bps,
                    "mean_abs_return_bps": absolute_returns.mean(),
                    "perfect_direction_mean_net_bps": (
                        absolute_returns.mean() - cost_bps
                    ),
                    "share_moves_covering_cost": safe_share(
                        absolute_returns.ge(cost_bps)
                    ),
                }
            )

        # Cost-aware three-class label distribution.
        for dead_zone_bps in LABEL_DEAD_ZONES_BPS:
            up_share = safe_share(returns.gt(dead_zone_bps))
            down_share = safe_share(returns.lt(-dead_zone_bps))
            flat_share = safe_share(
                returns.abs().le(dead_zone_bps)
            )

            label_rows.append(
                {
                    "regime": regime,
                    "horizon_events": horizon,
                    "dead_zone_bps": dead_zone_bps,
                    "n_observations": len(returns),
                    "up_share": up_share,
                    "down_share": down_share,
                    "flat_share": flat_share,
                    "nonflat_share": up_share + down_share,
                }
            )


# Per-run return statistics for stability analysis.
for run_name, run_data in price_data.groupby("run_name", sort=False):
    regime = run_to_regime[run_name]
    batch = run_to_batch[run_name]

    for horizon in HORIZONS:
        return_column = f"future_mid_return_bps_h{horizon}"

        returns = (
            run_data[return_column]
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )

        absolute_returns = returns.abs()

        future_by_run_rows.append(
            {
                "collection_batch": batch,
                "regime": regime,
                "run_name": run_name,
                "horizon_events": horizon,
                "n_observations": len(returns),
                "mean_return_bps": returns.mean(),
                "mean_abs_return_bps": absolute_returns.mean(),
                "median_abs_return_bps": absolute_returns.median(),
                "share_abs_return_ge_0_5bps": safe_share(
                    absolute_returns.ge(0.5)
                ),
                "share_abs_return_ge_1_0bps": safe_share(
                    absolute_returns.ge(1.0)
                ),
                "share_abs_return_ge_2_0bps": safe_share(
                    absolute_returns.ge(2.0)
                ),
            }
        )


future_summary = pd.DataFrame(future_summary_rows)
future_by_run = pd.DataFrame(future_by_run_rows)
label_distribution = pd.DataFrame(label_rows)
cost_opportunity = pd.DataFrame(cost_rows)

for dataframe in [
    future_summary,
    future_by_run,
    label_distribution,
    cost_opportunity,
]:
    dataframe["regime"] = pd.Categorical(
        dataframe["regime"],
        categories=REGIME_ORDER,
        ordered=True,
    )

future_summary = future_summary.sort_values(
    ["regime", "horizon_events"]
)

future_by_run = future_by_run.sort_values(
    ["regime", "run_name", "horizon_events"]
)

label_distribution = label_distribution.sort_values(
    ["regime", "horizon_events", "dead_zone_bps"]
)

cost_opportunity = cost_opportunity.sort_values(
    ["regime", "horizon_events", "round_trip_cost_bps"]
)


future_summary.to_csv(
    TABLES_DIR / "day31_future_return_summary.csv",
    index=False,
)

future_by_run.to_csv(
    TABLES_DIR / "day31_future_return_by_run.csv",
    index=False,
)

label_distribution.to_csv(
    TABLES_DIR / "day31_cost_aware_label_distribution.csv",
    index=False,
)

cost_opportunity.to_csv(
    TABLES_DIR / "day31_cost_opportunity_summary.csv",
    index=False,
)


# ============================================================================
# Figures
# ============================================================================

# Figure 1: trades per run
activity_boxplot_data = [
    activity_by_run.loc[
        activity_by_run["regime"].eq(regime),
        "total_trades_assigned",
    ].dropna()
    for regime in REGIME_ORDER
]

plt.figure(figsize=(11, 6))
plt.boxplot(activity_boxplot_data)
plt.xticks(
    range(1, len(REGIME_ORDER) + 1),
    REGIME_ORDER,
    rotation=15,
)
plt.ylabel("Trades assigned per 5-minute run")
plt.title("Trade activity by market regime")
plt.grid(axis="y", alpha=0.3)

save_figure(
    FIGURES_DIR / "day31_trades_per_run_by_regime.png"
)


# Figure 2: mean absolute return by horizon
plt.figure(figsize=(10, 6))

for regime in REGIME_ORDER:
    regime_summary = future_summary[
        future_summary["regime"].eq(regime)
    ]

    plt.plot(
        regime_summary["horizon_events"],
        regime_summary["mean_abs_return_bps"],
        marker="o",
        label=regime,
    )

plt.xlabel("Prediction horizon, order-book events")
plt.ylabel("Mean absolute future return, bps")
plt.title("Mean absolute future return by regime")
plt.legend()
plt.grid(alpha=0.3)

save_figure(
    FIGURES_DIR / "day31_mean_abs_return_by_regime.png"
)


# Figure 3: share of moves at least 1 bps
plt.figure(figsize=(10, 6))

for regime in REGIME_ORDER:
    regime_summary = future_summary[
        future_summary["regime"].eq(regime)
    ]

    plt.plot(
        regime_summary["horizon_events"],
        regime_summary["share_abs_return_ge_1_0bps"],
        marker="o",
        label=regime,
    )

plt.xlabel("Prediction horizon, order-book events")
plt.ylabel("Share of |future return| ≥ 1 bps")
plt.title("Economically meaningful price-move opportunity")
plt.legend()
plt.grid(alpha=0.3)

save_figure(
    FIGURES_DIR / "day31_share_moves_ge_1bps.png"
)


# Figure 4: 1 bps cost-aware non-flat share
labels_1bps = label_distribution[
    label_distribution["dead_zone_bps"].eq(1.0)
]

plt.figure(figsize=(10, 6))

for regime in REGIME_ORDER:
    regime_labels = labels_1bps[
        labels_1bps["regime"].eq(regime)
    ]

    plt.plot(
        regime_labels["horizon_events"],
        regime_labels["nonflat_share"],
        marker="o",
        label=regime,
    )

plt.xlabel("Prediction horizon, order-book events")
plt.ylabel("Non-flat share under 1 bps dead zone")
plt.title("Cost-aware label opportunity by regime")
plt.legend()
plt.grid(alpha=0.3)

save_figure(
    FIGURES_DIR / "day31_nonflat_share_1bps.png"
)


# ============================================================================
# Console summary
# ============================================================================

print()
print("=" * 80)
print("[INFO] Day 31 quality-filter summary")
print("=" * 80)
print(quality_summary.to_string(index=False))

print()
print("=" * 80)
print("[INFO] Day 31 activity summary")
print("=" * 80)
print(activity_summary.to_string(index=False))

print()
print("=" * 80)
print("[INFO] h50 and h100 future-return opportunity")
print("=" * 80)

print(
    future_summary[
        future_summary["horizon_events"].isin([50, 100])
    ][
        [
            "regime",
            "horizon_events",
            "mean_abs_return_bps",
            "median_abs_return_bps",
            "share_abs_return_ge_1_0bps",
            "share_abs_return_ge_2_0bps",
        ]
    ].to_string(index=False)
)

print()
print("[INFO] Day 31 active-hours regime analysis completed successfully.")