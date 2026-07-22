from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# =============================================================================
# Day 45: one-time evaluation of the frozen final holdout specification
# =============================================================================

DATA_PATH = Path("data/processed/trade_flow_features.csv")
LOG_PATH = Path("reports/tables/fresh_trade_collection_log.csv")
PREREGISTRATION_PATH = Path("reports/day43_final_holdout_preregistration.md")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")
REPORT_PATH = Path("reports/day45_final_holdout_evaluation.md")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_BATCHES = [
    "weekday_active_tue_day28",
    "weekday_active_wed_day29",
    "weekday_active_thu_day30",
    "weekday_active_fri_day39",
]
HOLDOUT_BATCH = "weekday_active_final_holdout_day44"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
PRIMARY_COST_BPS = 1.0
COOLDOWN_EVENTS = 50

MOVE_C = 0.03
DIRECTION_C = 1.0
MOVE_THRESHOLD = 0.65
DIRECTION_THRESHOLD = 0.65

MIN_COLLECTION_SECONDS = 295
MIN_DEPTH_EVENTS = 2500
MIN_TRADE_EVENTS = 1
MIN_PROCESSED_ROWS = 2500

COST_GRID_BPS = [0.5, 1.0, 1.5, 2.0]
BOOTSTRAP_SAMPLES = 10_000
RANDOM_STATE = 42

REDUCED_BOOK_FEATURES = [
    "spread_bps",
    "event_gap_ms",
    "best_bid_qty",
    "best_ask_qty",
    "imbalance_1",
    "imbalance_5",
    "microprice_deviation_bps",
    "quote_changed",
]

MEDIUM_TRADE_FLOW_FEATURES = [
    "trade_count",
    "buy_trade_count",
    "sell_trade_count",
    "trade_volume",
    "buy_trade_volume",
    "sell_trade_volume",
    "signed_trade_volume",
    "avg_trade_size",
    "trade_imbalance",
    "trade_intensity_per_second",
    "trade_count_rolling_sum_5e",
    "trade_count_rolling_sum_20e",
    "trade_count_rolling_sum_50e",
    "trade_volume_rolling_sum_5e",
    "trade_volume_rolling_sum_20e",
    "trade_volume_rolling_sum_50e",
    "signed_trade_volume_rolling_sum_5e",
    "signed_trade_volume_rolling_sum_20e",
    "signed_trade_volume_rolling_sum_50e",
    "trade_imbalance_rolling_5e",
    "trade_imbalance_rolling_20e",
    "trade_imbalance_rolling_50e",
    "trade_intensity_rolling_mean_20e",
    "trade_intensity_rolling_mean_50e",
]


def require_columns(frame: pd.DataFrame, columns: list[str], name: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def make_model(c_value: float) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    C=c_value,
                    class_weight="balanced",
                    solver="liblinear",
                    max_iter=2000,
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )


def classifier_metrics(y_true: pd.Series, probability: np.ndarray) -> dict[str, float | int]:
    y = y_true.astype(int).to_numpy()
    prediction = (probability >= 0.5).astype(int)
    result: dict[str, float | int] = {
        "n_observations": int(len(y)),
        "positive_class_share": float(y.mean()),
        "balanced_accuracy": float(balanced_accuracy_score(y, prediction)),
        "precision_positive": float(precision_score(y, prediction, zero_division=0)),
        "recall_positive": float(recall_score(y, prediction, zero_division=0)),
        "brier_score": float(brier_score_loss(y, probability)),
    }
    if np.unique(y).size >= 2:
        result["roc_auc"] = float(roc_auc_score(y, probability))
        result["average_precision"] = float(average_precision_score(y, probability))
    else:
        result["roc_auc"] = np.nan
        result["average_precision"] = np.nan
    return result


def build_quality_table(log: pd.DataFrame, processed_rows: pd.Series) -> pd.DataFrame:
    quality = log.drop_duplicates("run_name", keep="last").copy()
    quality["run_name"] = quality["run_name"].astype(str)
    quality["processed_rows"] = quality["run_name"].map(processed_rows).fillna(0).astype(int)
    quality["status_ok"] = quality["status"].astype(str).str.strip().str.lower().eq("success")

    duration_source = (
        quality["collection_seconds_meta"]
        if "collection_seconds_meta" in quality.columns
        else quality["collection_seconds_requested"]
    )
    quality["duration_seconds_used"] = pd.to_numeric(duration_source, errors="coerce")
    quality["duration_ok"] = quality["duration_seconds_used"] >= MIN_COLLECTION_SECONDS
    quality["depth_ok"] = pd.to_numeric(quality["depth_events"], errors="coerce") >= MIN_DEPTH_EVENTS
    quality["trades_ok"] = pd.to_numeric(quality["trade_events"], errors="coerce") >= MIN_TRADE_EVENTS
    quality["processed_ok"] = quality["processed_rows"] >= MIN_PROCESSED_ROWS
    quality["strict_quality_ok"] = (
        quality["status_ok"]
        & quality["duration_ok"]
        & quality["depth_ok"]
        & quality["trades_ok"]
        & quality["processed_ok"]
    )

    def reason(row: pd.Series) -> str:
        reasons: list[str] = []
        if not row["status_ok"]:
            reasons.append("status_not_success")
        if not row["duration_ok"]:
            reasons.append("duration_below_295")
        if not row["depth_ok"]:
            reasons.append("depth_below_2500")
        if not row["trades_ok"]:
            reasons.append("no_trade_events")
        if not row["processed_ok"]:
            reasons.append("processed_rows_below_2500_or_missing")
        return ";".join(reasons)

    quality["exclusion_reason"] = quality.apply(reason, axis=1)
    return quality


def select_first_eligible(candidates: pd.DataFrame) -> pd.DataFrame:
    candidates = candidates[candidates["signal_direction"] != 0].copy()
    selected: list[pd.DataFrame] = []

    for _, run_frame in candidates.groupby("run_name", sort=False):
        run_frame = run_frame.sort_values("row_in_run")
        selected_indices: list[int] = []
        previous_row: int | None = None

        for row_index, row in run_frame.iterrows():
            current_row = int(row["row_in_run"])
            if previous_row is None or current_row > previous_row + COOLDOWN_EVENTS:
                selected_indices.append(row_index)
                previous_row = current_row

        if selected_indices:
            selected.append(run_frame.loc[selected_indices])

    if not selected:
        return candidates.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=True)


def cluster_bootstrap(signals: pd.DataFrame) -> dict[str, float]:
    groups = {
        run_name: frame["net_return_bps"].to_numpy()
        for run_name, frame in signals.groupby("run_name")
    }
    run_names = np.array(list(groups), dtype=object)
    if len(run_names) == 0:
        return {
            "bootstrap_mean_net_bps": np.nan,
            "bootstrap_ci_lower_2_5": np.nan,
            "bootstrap_ci_upper_97_5": np.nan,
            "bootstrap_share_mean_net_positive": np.nan,
        }

    rng = np.random.default_rng(RANDOM_STATE)
    means = np.empty(BOOTSTRAP_SAMPLES)
    for index in range(BOOTSTRAP_SAMPLES):
        sampled_runs = rng.choice(run_names, size=len(run_names), replace=True)
        sampled_returns = np.concatenate([groups[run_name] for run_name in sampled_runs])
        means[index] = sampled_returns.mean()

    return {
        "bootstrap_mean_net_bps": float(means.mean()),
        "bootstrap_ci_lower_2_5": float(np.quantile(means, 0.025)),
        "bootstrap_ci_upper_97_5": float(np.quantile(means, 0.975)),
        "bootstrap_share_mean_net_positive": float((means > 0).mean()),
    }


# Freeze verification
if not PREREGISTRATION_PATH.exists():
    raise FileNotFoundError("Final holdout preregistration is missing.")
print("[INFO] Final preregistration found.")

# Load data
log = pd.read_csv(LOG_PATH)
require_columns(
    log,
    [
        "collection_batch",
        "run_name",
        "status",
        "collection_seconds_requested",
        "depth_events",
        "trade_events",
    ],
    "fresh_trade_collection_log.csv",
)

relevant_batches = TRAIN_BATCHES + [HOLDOUT_BATCH]
relevant_log = log[log["collection_batch"].isin(relevant_batches)].copy()
relevant_runs = set(relevant_log["run_name"].dropna().astype(str))

header = pd.read_csv(DATA_PATH, nrows=0)
required_columns = list(
    dict.fromkeys(
        ["run_name", "row_in_run", "mid_price"]
        + REDUCED_BOOK_FEATURES
        + MEDIUM_TRADE_FLOW_FEATURES
    )
)
require_columns(header, required_columns, "trade_flow_features.csv")

data = pd.read_csv(DATA_PATH, usecols=required_columns)
data["run_name"] = data["run_name"].astype(str)
data = data[data["run_name"].isin(relevant_runs)].copy()
data = data.sort_values(["run_name", "row_in_run"]).reset_index(drop=True)
features = list(dict.fromkeys(REDUCED_BOOK_FEATURES + MEDIUM_TRADE_FLOW_FEATURES))
data[features] = data[features].replace([np.inf, -np.inf], np.nan)

processed_rows = data.groupby("run_name").size()
quality = build_quality_table(relevant_log, processed_rows)
quality.to_csv(TABLES_DIR / "day45_final_run_quality_audit.csv", index=False)

holdout_quality = quality[quality["collection_batch"].eq(HOLDOUT_BATCH)].copy()
holdout_quality.to_csv(TABLES_DIR / "day45_final_holdout_quality.csv", index=False)
holdout_quality[~holdout_quality["strict_quality_ok"]].to_csv(
    TABLES_DIR / "day45_final_holdout_exclusions.csv", index=False
)

run_to_batch = dict(zip(quality["run_name"], quality["collection_batch"]))
strict_runs = set(quality.loc[quality["strict_quality_ok"], "run_name"])
data = data[data["run_name"].isin(strict_runs)].copy()
data["collection_batch"] = data["run_name"].map(run_to_batch)

future_mid = data.groupby("run_name", sort=False)["mid_price"].shift(-HORIZON)
data["future_return_bps"] = (future_mid / data["mid_price"] - 1.0) * 10_000.0
data["move_target"] = np.where(
    data["future_return_bps"].notna(),
    (data["future_return_bps"].abs() > DEAD_ZONE_BPS).astype(float),
    np.nan,
)
data["direction_target"] = np.where(
    data["future_return_bps"] > DEAD_ZONE_BPS,
    1.0,
    np.where(data["future_return_bps"] < -DEAD_ZONE_BPS, 0.0, np.nan),
)

model_data = data[data["future_return_bps"].notna()].copy()
train = model_data[model_data["collection_batch"].isin(TRAIN_BATCHES)].copy()
holdout = model_data[model_data["collection_batch"].eq(HOLDOUT_BATCH)].copy()

if train.empty:
    raise ValueError("Frozen training sample is empty.")
if holdout.empty:
    raise ValueError("Final holdout sample is empty. Process the Day 44 runs first.")

holdout_strict_runs = holdout["run_name"].nunique()
print(f"[INFO] Strict holdout runs: {holdout_strict_runs}")

# Fit frozen models once
move_model = make_model(MOVE_C)
move_model.fit(train[MEDIUM_TRADE_FLOW_FEATURES], train["move_target"].astype(int))

train_direction = train[train["direction_target"].notna()].copy()
holdout_direction = holdout[holdout["direction_target"].notna()].copy()
direction_model = make_model(DIRECTION_C)
direction_model.fit(
    train_direction[REDUCED_BOOK_FEATURES],
    train_direction["direction_target"].astype(int),
)

move_probability = move_model.predict_proba(holdout[MEDIUM_TRADE_FLOW_FEATURES])[:, 1]
direction_probability_all = direction_model.predict_proba(holdout[REDUCED_BOOK_FEATURES])[:, 1]
direction_probability_nonflat = direction_model.predict_proba(
    holdout_direction[REDUCED_BOOK_FEATURES]
)[:, 1]

metrics = pd.DataFrame(
    [
        {
            "stage": "move",
            "feature_set": "medium_trade_flow",
            **classifier_metrics(holdout["move_target"], move_probability),
        },
        {
            "stage": "direction",
            "feature_set": "reduced_book",
            **classifier_metrics(
                holdout_direction["direction_target"],
                direction_probability_nonflat,
            ),
        },
    ]
)
metrics.to_csv(TABLES_DIR / "day45_final_classifier_metrics.csv", index=False)

# Frozen deployment
candidates = holdout[["run_name", "row_in_run", "future_return_bps"]].copy()
candidates["move_probability"] = move_probability
candidates["direction_probability_up"] = direction_probability_all
move_ok = candidates["move_probability"] >= MOVE_THRESHOLD
long_ok = candidates["direction_probability_up"] >= DIRECTION_THRESHOLD
short_ok = candidates["direction_probability_up"] <= 1.0 - DIRECTION_THRESHOLD
candidates["signal_direction"] = np.where(
    move_ok & long_ok,
    1,
    np.where(move_ok & short_ok, -1, 0),
)

signals = select_first_eligible(candidates)
signals["signed_return_bps"] = signals["signal_direction"] * signals["future_return_bps"]
signals["net_return_bps"] = signals["signed_return_bps"] - PRIMARY_COST_BPS
signals["direction_correct"] = signals["signed_return_bps"] > 0
signals["net_profitable"] = signals["net_return_bps"] > 0
signals["direction_label"] = np.where(
    signals["signal_direction"] == 1,
    "UP_LONG",
    "DOWN_SHORT",
)
signals["outcome_bucket"] = np.select(
    [
        signals["signed_return_bps"] <= 0,
        (signals["signed_return_bps"] > 0)
        & (signals["signed_return_bps"] <= PRIMARY_COST_BPS),
        signals["signed_return_bps"] > PRIMARY_COST_BPS,
    ],
    [
        "wrong_direction",
        "correct_direction_below_cost",
        "profitable_after_cost",
    ],
    default="unclassified",
)
signals.to_csv(TABLES_DIR / "day45_final_holdout_signals.csv", index=False)

# Summaries
if signals.empty:
    summary = {
        "holdout_batch": HOLDOUT_BATCH,
        "eligible_runs": int(holdout_strict_runs),
        "n_signals": 0,
        "n_signal_runs": 0,
        "signal_run_coverage": 0.0,
        "directional_precision": np.nan,
        "positive_net_signal_share": np.nan,
        "mean_gross_bps": np.nan,
        "median_gross_bps": np.nan,
        "mean_net_bps": np.nan,
        "median_net_bps": np.nan,
        "total_net_bps": 0.0,
        "break_even_cost_bps": np.nan,
    }
else:
    summary = {
        "holdout_batch": HOLDOUT_BATCH,
        "eligible_runs": int(holdout_strict_runs),
        "n_signals": int(len(signals)),
        "n_signal_runs": int(signals["run_name"].nunique()),
        "signal_run_coverage": float(signals["run_name"].nunique() / holdout_strict_runs),
        "directional_precision": float(signals["direction_correct"].mean()),
        "positive_net_signal_share": float(signals["net_profitable"].mean()),
        "mean_gross_bps": float(signals["signed_return_bps"].mean()),
        "median_gross_bps": float(signals["signed_return_bps"].median()),
        "mean_net_bps": float(signals["net_return_bps"].mean()),
        "median_net_bps": float(signals["net_return_bps"].median()),
        "total_net_bps": float(signals["net_return_bps"].sum()),
        "break_even_cost_bps": float(signals["signed_return_bps"].mean()),
    }
summary_frame = pd.DataFrame([summary])
summary_frame.to_csv(TABLES_DIR / "day45_final_holdout_summary.csv", index=False)

if signals.empty:
    per_run = pd.DataFrame()
    per_direction = pd.DataFrame()
    outcomes = pd.DataFrame()
else:
    per_run = signals.groupby("run_name", as_index=False).agg(
        n_signals=("run_name", "size"),
        directional_precision=("direction_correct", "mean"),
        positive_net_signal_share=("net_profitable", "mean"),
        mean_gross_bps=("signed_return_bps", "mean"),
        mean_net_bps=("net_return_bps", "mean"),
        median_net_bps=("net_return_bps", "median"),
        total_net_bps=("net_return_bps", "sum"),
    )
    per_direction = signals.groupby("direction_label", as_index=False).agg(
        n_signals=("run_name", "size"),
        n_runs=("run_name", "nunique"),
        directional_precision=("direction_correct", "mean"),
        positive_net_signal_share=("net_profitable", "mean"),
        mean_gross_bps=("signed_return_bps", "mean"),
        median_gross_bps=("signed_return_bps", "median"),
        mean_net_bps=("net_return_bps", "mean"),
        median_net_bps=("net_return_bps", "median"),
        total_net_bps=("net_return_bps", "sum"),
    )
    outcomes = signals.groupby("outcome_bucket", as_index=False).agg(
        n_signals=("run_name", "size"),
        n_runs=("run_name", "nunique"),
        mean_gross_bps=("signed_return_bps", "mean"),
        mean_net_bps=("net_return_bps", "mean"),
        total_net_bps=("net_return_bps", "sum"),
    )
    outcomes["share"] = outcomes["n_signals"] / len(signals)

per_run.to_csv(TABLES_DIR / "day45_final_per_run_summary.csv", index=False)
per_direction.to_csv(TABLES_DIR / "day45_final_per_direction_summary.csv", index=False)
outcomes.to_csv(TABLES_DIR / "day45_final_outcome_decomposition.csv", index=False)

# Robustness
robustness: dict[str, float | int | str] = {
    "holdout_batch": HOLDOUT_BATCH,
    "n_signals": int(len(signals)),
    "n_signal_runs": int(signals["run_name"].nunique()) if not signals.empty else 0,
}

if not signals.empty:
    best_signal = signals["net_return_bps"].idxmax()
    worst_signal = signals["net_return_bps"].idxmin()
    robustness["best_signal_net_bps"] = float(signals.loc[best_signal, "net_return_bps"])
    robustness["worst_signal_net_bps"] = float(signals.loc[worst_signal, "net_return_bps"])
    robustness["mean_net_without_best_signal_bps"] = float(
        signals.drop(index=best_signal)["net_return_bps"].mean()
    )
    robustness["mean_net_without_worst_signal_bps"] = float(
        signals.drop(index=worst_signal)["net_return_bps"].mean()
    )

    if not per_run.empty:
        best_run = per_run.sort_values("total_net_bps", ascending=False).iloc[0]["run_name"]
        worst_run = per_run.sort_values("total_net_bps", ascending=True).iloc[0]["run_name"]
        robustness["best_run_name"] = best_run
        robustness["worst_run_name"] = worst_run
        robustness["mean_net_without_best_run_bps"] = float(
            signals[~signals["run_name"].eq(best_run)]["net_return_bps"].mean()
        )
        robustness["mean_net_without_worst_run_bps"] = float(
            signals[~signals["run_name"].eq(worst_run)]["net_return_bps"].mean()
        )
        loo_means = [
            signals[~signals["run_name"].eq(run_name)]["net_return_bps"].mean()
            for run_name in signals["run_name"].drop_duplicates()
        ]
        robustness["leave_one_run_out_min_mean_net_bps"] = float(np.min(loo_means))
        robustness["leave_one_run_out_median_mean_net_bps"] = float(np.median(loo_means))
        robustness["leave_one_run_out_max_mean_net_bps"] = float(np.max(loo_means))

    robustness.update(cluster_bootstrap(signals))

robustness_frame = pd.DataFrame([robustness])
robustness_frame.to_csv(TABLES_DIR / "day45_final_robustness_summary.csv", index=False)

# Cost sensitivity
cost_rows = []
for cost_bps in COST_GRID_BPS:
    if signals.empty:
        net = pd.Series(dtype=float)
    else:
        net = signals["signed_return_bps"] - cost_bps
    cost_rows.append(
        {
            "cost_bps": cost_bps,
            "n_signals": int(len(signals)),
            "mean_net_bps": float(net.mean()) if len(net) else np.nan,
            "median_net_bps": float(net.median()) if len(net) else np.nan,
            "total_net_bps": float(net.sum()) if len(net) else 0.0,
            "positive_net_signal_share": float((net > 0).mean()) if len(net) else np.nan,
        }
    )
costs = pd.DataFrame(cost_rows)
costs.to_csv(TABLES_DIR / "day45_final_cost_sensitivity.csv", index=False)

# Final status
mean_net = summary["mean_net_bps"]
bootstrap_lower = robustness.get("bootstrap_ci_lower_2_5", np.nan)
bootstrap_positive_share = robustness.get("bootstrap_share_mean_net_positive", np.nan)
point_status = (
    "POSITIVE_POINT_ESTIMATE"
    if pd.notna(mean_net) and mean_net > 0
    else "NON_POSITIVE_POINT_ESTIMATE"
)
if pd.notna(bootstrap_lower) and bootstrap_lower > 0:
    evidence_status = "STRONG_RUN_CLUSTER_EVIDENCE"
elif pd.notna(bootstrap_positive_share) and bootstrap_positive_share >= 0.75:
    evidence_status = "MODERATE_BUT_NOT_STRONG_EVIDENCE"
else:
    evidence_status = "INSUFFICIENT_ROBUST_EVIDENCE"

decision = pd.DataFrame(
    [
        {
            "point_estimate_status": point_status,
            "evidence_status": evidence_status,
            "mean_net_bps": mean_net,
            "bootstrap_ci_lower_2_5": bootstrap_lower,
            "bootstrap_ci_upper_97_5": robustness.get("bootstrap_ci_upper_97_5", np.nan),
            "bootstrap_share_mean_net_positive": bootstrap_positive_share,
            "model_status": "FINAL_FROZEN_HOLDOUT_EVALUATED",
        }
    ]
)
decision.to_csv(TABLES_DIR / "day45_final_decision.csv", index=False)

# Figures
if not per_run.empty:
    sorted_runs = per_run.sort_values("total_net_bps")
    plt.figure(figsize=(10, 6))
    plt.bar(np.arange(len(sorted_runs)), sorted_runs["total_net_bps"])
    plt.axhline(0.0, linewidth=1)
    plt.xlabel("Signal-bearing runs sorted by total net result")
    plt.ylabel("Total net return per run, bps")
    plt.title("Day 45 final holdout run-level outcomes")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "day45_final_run_net_results.png", dpi=150, bbox_inches="tight")
    plt.close()

if not signals.empty:
    ordered_signals = signals.sort_values(["run_name", "row_in_run"])
    plt.figure(figsize=(10, 6))
    plt.plot(np.arange(1, len(signals) + 1), ordered_signals["net_return_bps"].cumsum())
    plt.axhline(0.0, linewidth=1)
    plt.xlabel("Selected signal number")
    plt.ylabel("Cumulative net return, bps")
    plt.title("Day 45 final holdout cumulative signal result")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "day45_final_cumulative_net.png", dpi=150, bbox_inches="tight")
    plt.close()

plt.figure(figsize=(8, 5))
plt.plot(costs["cost_bps"], costs["mean_net_bps"], marker="o")
plt.axhline(0.0, linewidth=1)
plt.xlabel("Assumed round-trip cost, bps")
plt.ylabel("Mean net return, bps")
plt.title("Day 45 final holdout cost sensitivity")
plt.tight_layout()
plt.savefig(FIGURES_DIR / "day45_final_cost_sensitivity.png", dpi=150, bbox_inches="tight")
plt.close()

# Report
report = f"""# Day 45 Final Frozen Holdout Evaluation

## Research status

This is the one-time evaluation of the specification frozen and preregistered
on Day 43. No threshold, feature, model, horizon, quality rule, cost assumption,
entry rule or cooldown rule was changed after the holdout was inspected.

## Frozen specification

- Training batches: Tuesday, Wednesday, Thursday and Friday active-hours data
- Holdout batch: `{HOLDOUT_BATCH}`
- MOVE model: medium trade-flow logistic regression, C={MOVE_C}
- MOVE threshold: {MOVE_THRESHOLD}
- Direction model: reduced-book logistic regression, C={DIRECTION_C}
- Direction threshold: {DIRECTION_THRESHOLD}
- Horizon: {HORIZON} book events
- Dead zone: {DEAD_ZONE_BPS:.1f} bps
- Entry: first eligible
- Cooldown: {COOLDOWN_EVENTS} events
- Primary cost: {PRIMARY_COST_BPS:.1f} bps

## Strict technical sample

- Raw holdout attempts: {len(holdout_quality)}
- Strict eligible holdout runs: {holdout_strict_runs}
- Excluded holdout runs: {len(holdout_quality) - int(holdout_quality['strict_quality_ok'].sum())}

## Classifier diagnostics

{metrics.to_markdown(index=False)}

## Primary deployment result

{summary_frame.to_markdown(index=False)}

## Direction decomposition

{per_direction.to_markdown(index=False) if not per_direction.empty else 'No selected signals.'}

## Outcome decomposition

{outcomes.to_markdown(index=False) if not outcomes.empty else 'No selected signals.'}

## Robustness

{robustness_frame.to_markdown(index=False)}

## Cost sensitivity

{costs.to_markdown(index=False)}

## Final interpretation

- Point-estimate status: `{point_status}`
- Evidence status: `{evidence_status}`

A positive point estimate does not by itself establish robust profitability.
The conclusion must reflect run-level concentration, bootstrap uncertainty,
transaction-cost sensitivity and consistency with the earlier chronological
folds. This holdout is evaluated once; no post-hoc replacement policy is allowed.
"""
REPORT_PATH.write_text(report, encoding="utf-8")

print("\n" + "=" * 88)
print("[INFO] Final holdout quality")
print("=" * 88)
print(
    holdout_quality[
        [
            "run_name",
            "status",
            "depth_events",
            "trade_events",
            "processed_rows",
            "strict_quality_ok",
            "exclusion_reason",
        ]
    ].to_string(index=False)
)

print("\n" + "=" * 88)
print("[INFO] Final classifier metrics")
print("=" * 88)
print(metrics.to_string(index=False))

print("\n" + "=" * 88)
print("[INFO] Final deployment summary")
print("=" * 88)
print(summary_frame.to_string(index=False))

print("\n" + "=" * 88)
print("[INFO] Final robustness")
print("=" * 88)
print(robustness_frame.to_string(index=False))

print("\n" + "=" * 88)
print("[INFO] Final decision")
print("=" * 88)
print(decision.to_string(index=False))

print("\n[INFO] Day 45 final frozen holdout evaluation completed successfully.")