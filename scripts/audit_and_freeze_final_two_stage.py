from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# =============================================================================
# Day 43: final robustness audit and specification freeze
#
# No model is refitted and no threshold is searched.
# The script analyzes only the chronological OOS signals generated on Day 42.
# =============================================================================

SIGNALS_PATH = Path("reports/tables/day42_selected_signals.csv")
DEPLOYMENT_PATH = Path("reports/tables/day42_fold_deployment_results.csv")
QUALITY_PATH = Path("reports/tables/day42_quality_summary.csv")

TABLES_DIR = Path("reports/tables")
FIGURES_DIR = Path("reports/figures")
AUDIT_REPORT_PATH = Path("reports/day43_final_robustness_audit.md")
PREREGISTRATION_PATH = Path("reports/day43_final_holdout_preregistration.md")

TABLES_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

CANDIDATE_MODEL = "two_stage_sensitivity_0p65_0p65"
BENCHMARK_MODEL = "two_stage_primary_0p75_0p65"
EXPECTED_FOLDS = [
    "fold_1_tue_to_wed",
    "fold_2_tue_wed_to_thu",
    "fold_3_tue_wed_thu_to_fri",
]

COST_GRID_BPS = [0.5, 1.0, 1.5, 2.0]
BOOTSTRAP_SAMPLES = 10_000
RANDOM_STATE = 42

FINAL_TRAIN_BATCHES = [
    "weekday_active_tue_day28",
    "weekday_active_wed_day29",
    "weekday_active_thu_day30",
    "weekday_active_fri_day39",
]
FINAL_HOLDOUT_BATCH = "weekday_active_final_holdout_day44"

HORIZON = 50
DEAD_ZONE_BPS = 1.0
MOVE_C = 0.03
DIRECTION_C = 1.0
MOVE_THRESHOLD = 0.65
DIRECTION_THRESHOLD = 0.65
COOLDOWN_EVENTS = 50
PRIMARY_COST_BPS = 1.0

MIN_COLLECTION_SECONDS = 295
MIN_DEPTH_EVENTS = 2_500
MIN_TRADE_EVENTS = 1
MIN_PROCESSED_ROWS = 2_500

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


def require_columns(frame: pd.DataFrame, required: list[str], name: str) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}")


def summarize(frame: pd.DataFrame) -> dict[str, float | int]:
    return {
        "n_signals": int(len(frame)),
        "n_runs": int(frame["run_name"].nunique()),
        "n_folds": int(frame["fold"].nunique()),
        "directional_precision": float(frame["direction_correct"].mean()),
        "positive_net_signal_share": float(frame["net_profitable"].mean()),
        "mean_gross_bps": float(frame["signed_return_bps"].mean()),
        "median_gross_bps": float(frame["signed_return_bps"].median()),
        "mean_net_bps": float(frame["net_return_bps"].mean()),
        "median_net_bps": float(frame["net_return_bps"].median()),
        "total_net_bps": float(frame["net_return_bps"].sum()),
    }


def leave_one_out(frame: pd.DataFrame, cluster: str) -> pd.DataFrame:
    rows = []
    for value in frame[cluster].drop_duplicates():
        remaining = frame[frame[cluster] != value]
        rows.append(
            {
                f"excluded_{cluster}": value,
                "remaining_signals": int(len(remaining)),
                "remaining_runs": int(remaining["run_name"].nunique()),
                "mean_net_bps": float(remaining["net_return_bps"].mean()),
                "median_net_bps": float(remaining["net_return_bps"].median()),
                "total_net_bps": float(remaining["net_return_bps"].sum()),
            }
        )
    return pd.DataFrame(rows)


def cluster_bootstrap(frame: pd.DataFrame) -> dict[str, float]:
    groups = {
        run_name: run_frame["net_return_bps"].to_numpy()
        for run_name, run_frame in frame.groupby("run_name")
    }
    run_names = np.array(list(groups), dtype=object)
    rng = np.random.default_rng(RANDOM_STATE)
    means = np.empty(BOOTSTRAP_SAMPLES)

    for index in range(BOOTSTRAP_SAMPLES):
        sampled_runs = rng.choice(run_names, size=len(run_names), replace=True)
        sampled_returns = np.concatenate([groups[run] for run in sampled_runs])
        means[index] = sampled_returns.mean()

    return {
        "bootstrap_mean_net_bps": float(means.mean()),
        "bootstrap_ci_lower_2_5": float(np.quantile(means, 0.025)),
        "bootstrap_ci_upper_97_5": float(np.quantile(means, 0.975)),
        "bootstrap_share_mean_net_positive": float((means > 0).mean()),
    }


def audit_model(model_name: str, frame: pd.DataFrame):
    per_run = (
        frame.groupby(["fold", "run_name"], as_index=False)
        .agg(
            n_signals=("run_name", "size"),
            mean_net_bps=("net_return_bps", "mean"),
            median_net_bps=("net_return_bps", "median"),
            total_net_bps=("net_return_bps", "sum"),
        )
        .sort_values("total_net_bps", ascending=False)
    )

    best_signal_index = frame["net_return_bps"].idxmax()
    worst_signal_index = frame["net_return_bps"].idxmin()
    best_signal = frame.loc[best_signal_index]
    worst_signal = frame.loc[worst_signal_index]
    best_run = per_run.iloc[0]
    worst_run = per_run.iloc[-1]

    without_best_run = frame[
        ~(
            frame["fold"].eq(best_run["fold"])
            & frame["run_name"].eq(best_run["run_name"])
        )
    ]
    without_worst_run = frame[
        ~(
            frame["fold"].eq(worst_run["fold"])
            & frame["run_name"].eq(worst_run["run_name"])
        )
    ]

    leave_run = leave_one_out(frame, "run_name")
    leave_fold = leave_one_out(frame, "fold")

    audit = {
        "model": model_name,
        **summarize(frame),
        "best_signal_fold": best_signal["fold"],
        "best_signal_run": best_signal["run_name"],
        "best_signal_net_bps": float(best_signal["net_return_bps"]),
        "worst_signal_fold": worst_signal["fold"],
        "worst_signal_run": worst_signal["run_name"],
        "worst_signal_net_bps": float(worst_signal["net_return_bps"]),
        "mean_net_without_best_signal_bps": float(
            frame.drop(index=best_signal_index)["net_return_bps"].mean()
        ),
        "mean_net_without_worst_signal_bps": float(
            frame.drop(index=worst_signal_index)["net_return_bps"].mean()
        ),
        "best_run_fold": best_run["fold"],
        "best_run_name": best_run["run_name"],
        "best_run_total_net_bps": float(best_run["total_net_bps"]),
        "worst_run_fold": worst_run["fold"],
        "worst_run_name": worst_run["run_name"],
        "worst_run_total_net_bps": float(worst_run["total_net_bps"]),
        "mean_net_without_best_run_bps": float(without_best_run["net_return_bps"].mean()),
        "mean_net_without_worst_run_bps": float(without_worst_run["net_return_bps"].mean()),
        "leave_one_run_out_min_mean_net_bps": float(leave_run["mean_net_bps"].min()),
        "leave_one_run_out_median_mean_net_bps": float(leave_run["mean_net_bps"].median()),
        "leave_one_run_out_max_mean_net_bps": float(leave_run["mean_net_bps"].max()),
        "leave_one_fold_out_min_mean_net_bps": float(leave_fold["mean_net_bps"].min()),
        "leave_one_fold_out_median_mean_net_bps": float(leave_fold["mean_net_bps"].median()),
        "leave_one_fold_out_max_mean_net_bps": float(leave_fold["mean_net_bps"].max()),
        **cluster_bootstrap(frame),
    }

    leave_run["model"] = model_name
    leave_fold["model"] = model_name
    return audit, per_run, leave_run, leave_fold


print("[INFO] Loading Day 42 outputs...")
signals = pd.read_csv(SIGNALS_PATH)
deployment = pd.read_csv(DEPLOYMENT_PATH)
quality = pd.read_csv(QUALITY_PATH)

require_columns(
    signals,
    [
        "fold",
        "model",
        "run_name",
        "row_in_run",
        "signal_direction",
        "signed_return_bps",
        "net_return_bps",
        "direction_correct",
        "net_profitable",
    ],
    "day42_selected_signals.csv",
)

signals["run_name"] = signals["run_name"].astype(str)
models = [CANDIDATE_MODEL, BENCHMARK_MODEL]

for model_name in models:
    model_folds = set(signals.loc[signals["model"].eq(model_name), "fold"])
    missing = set(EXPECTED_FOLDS) - model_folds
    if missing:
        raise ValueError(f"{model_name} is missing folds: {sorted(missing)}")

fold_comparison = deployment[deployment["model"].isin(models)].copy()
fold_comparison.to_csv(TABLES_DIR / "day43_fold_comparison.csv", index=False)

robustness_rows = []
per_run_frames = []
leave_run_frames = []
leave_fold_frames = []

for model_name in models:
    model_signals = signals[signals["model"].eq(model_name)].copy()
    audit, per_run, leave_run, leave_fold = audit_model(model_name, model_signals)
    robustness_rows.append(audit)
    per_run["model"] = model_name
    per_run_frames.append(per_run)
    leave_run_frames.append(leave_run)
    leave_fold_frames.append(leave_fold)

robustness = pd.DataFrame(robustness_rows)
per_run = pd.concat(per_run_frames, ignore_index=True)
leave_run = pd.concat(leave_run_frames, ignore_index=True)
leave_fold = pd.concat(leave_fold_frames, ignore_index=True)

robustness.to_csv(TABLES_DIR / "day43_robustness_summary.csv", index=False)
per_run.to_csv(TABLES_DIR / "day43_per_run_summary.csv", index=False)
leave_run.to_csv(TABLES_DIR / "day43_leave_one_run_out.csv", index=False)
leave_fold.to_csv(TABLES_DIR / "day43_leave_one_fold_out.csv", index=False)

# Direction diagnostics.
direction_rows = []
for model_name in models:
    frame = signals[signals["model"].eq(model_name)].copy()
    frame["direction_label"] = np.where(
        frame["signal_direction"].eq(1), "UP_LONG", "DOWN_SHORT"
    )
    summary = (
        frame.groupby("direction_label", as_index=False)
        .agg(
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
    )
    summary["model"] = model_name
    direction_rows.append(summary)

per_direction = pd.concat(direction_rows, ignore_index=True)
per_direction.to_csv(TABLES_DIR / "day43_per_direction_summary.csv", index=False)

# Cost sensitivity.
cost_rows = []
for model_name in models:
    frame = signals[signals["model"].eq(model_name)]
    for cost in COST_GRID_BPS:
        net = frame["signed_return_bps"] - cost
        cost_rows.append(
            {
                "model": model_name,
                "cost_bps": cost,
                "n_signals": int(len(frame)),
                "n_runs": int(frame["run_name"].nunique()),
                "mean_net_bps": float(net.mean()),
                "median_net_bps": float(net.median()),
                "total_net_bps": float(net.sum()),
                "positive_net_signal_share": float((net > 0).mean()),
            }
        )

cost_sensitivity = pd.DataFrame(cost_rows)
cost_sensitivity.to_csv(TABLES_DIR / "day43_cost_sensitivity.csv", index=False)

# Freeze criteria: hard eligibility determines whether a provisional candidate
# may be taken to one final fresh holdout. Robustness criteria remain diagnostic.
candidate_fold = fold_comparison[fold_comparison["model"].eq(CANDIDATE_MODEL)]
candidate_audit = robustness[robustness["model"].eq(CANDIDATE_MODEL)].iloc[0]

criteria = [
    (
        "At least 2 of 3 chronological folds are positive",
        int((candidate_fold["mean_net_return_bps"] > 0).sum()),
        ">= 2",
        (candidate_fold["mean_net_return_bps"] > 0).sum() >= 2,
        "hard_eligibility",
    ),
    (
        "Median fold mean net is positive",
        float(candidate_fold["mean_net_return_bps"].median()),
        "> 0",
        candidate_fold["mean_net_return_bps"].median() > 0,
        "hard_eligibility",
    ),
    (
        "Pooled weighted mean net is positive",
        float(candidate_audit["mean_net_bps"]),
        "> 0",
        candidate_audit["mean_net_bps"] > 0,
        "hard_eligibility",
    ),
    (
        "At least 100 chronological OOS signals",
        int(candidate_audit["n_signals"]),
        ">= 100",
        candidate_audit["n_signals"] >= 100,
        "hard_eligibility",
    ),
    (
        "At least 40 signal-bearing runs",
        int(candidate_audit["n_runs"]),
        ">= 40",
        candidate_audit["n_runs"] >= 40,
        "hard_eligibility",
    ),
    (
        "Mean net remains positive without best signal",
        float(candidate_audit["mean_net_without_best_signal_bps"]),
        "> 0",
        candidate_audit["mean_net_without_best_signal_bps"] > 0,
        "robustness_diagnostic",
    ),
    (
        "Mean net remains positive without best run",
        float(candidate_audit["mean_net_without_best_run_bps"]),
        "> 0",
        candidate_audit["mean_net_without_best_run_bps"] > 0,
        "robustness_diagnostic",
    ),
    (
        "Mean net remains positive after removing any one fold",
        float(candidate_audit["leave_one_fold_out_min_mean_net_bps"]),
        "> 0",
        candidate_audit["leave_one_fold_out_min_mean_net_bps"] > 0,
        "robustness_diagnostic",
    ),
    (
        "Run-cluster bootstrap positive share is at least 75%",
        float(candidate_audit["bootstrap_share_mean_net_positive"]),
        ">= 0.75",
        candidate_audit["bootstrap_share_mean_net_positive"] >= 0.75,
        "robustness_diagnostic",
    ),
    (
        "Run-cluster bootstrap 95% lower bound is positive",
        float(candidate_audit["bootstrap_ci_lower_2_5"]),
        "> 0",
        candidate_audit["bootstrap_ci_lower_2_5"] > 0,
        "strong_evidence",
    ),
]

freeze_decision = pd.DataFrame(
    criteria,
    columns=["criterion", "value", "required", "passed", "role"],
)

hard_pass = bool(
    freeze_decision.loc[
        freeze_decision["role"].eq("hard_eligibility"), "passed"
    ].all()
)
final_decision = (
    "FREEZE_PROVISIONAL_CANDIDATE"
    if hard_pass
    else "DO_NOT_FREEZE_STOP_MODEL_DEVELOPMENT"
)
freeze_decision["final_decision"] = final_decision
freeze_decision.to_csv(TABLES_DIR / "day43_freeze_decision.csv", index=False)

# Figures.
fold_plot = fold_comparison.pivot(
    index="fold", columns="model", values="mean_net_return_bps"
).reindex(EXPECTED_FOLDS)
plt.figure(figsize=(10, 6))
fold_plot.plot(kind="bar", ax=plt.gca())
plt.axhline(0.0, linewidth=1)
plt.ylabel("Mean net return after 1 bps, bps")
plt.xlabel("Chronological validation fold")
plt.title("Day 43 candidate and benchmark by fold")
plt.xticks(rotation=20, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(FIGURES_DIR / "day43_fold_net_comparison.png", dpi=150)
plt.close()

cost_plot = cost_sensitivity.pivot(
    index="cost_bps", columns="model", values="mean_net_bps"
)
plt.figure(figsize=(9, 6))
for column in cost_plot.columns:
    plt.plot(cost_plot.index, cost_plot[column], marker="o", label=column)
plt.axhline(0.0, linewidth=1)
plt.ylabel("Mean net return, bps")
plt.xlabel("Assumed round-trip cost, bps")
plt.title("Day 43 cost sensitivity")
plt.grid(alpha=0.3)
plt.legend(fontsize=8)
plt.tight_layout()
plt.savefig(FIGURES_DIR / "day43_cost_sensitivity.png", dpi=150)
plt.close()

candidate_runs = per_run[per_run["model"].eq(CANDIDATE_MODEL)]
plt.figure(figsize=(9, 6))
plt.hist(candidate_runs["total_net_bps"], bins=20)
plt.axvline(0.0, linewidth=1)
plt.ylabel("Number of signal-bearing runs")
plt.xlabel("Total net return per run, bps")
plt.title("Day 43 candidate run-level outcomes")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(FIGURES_DIR / "day43_candidate_run_net_distribution.png", dpi=150)
plt.close()

# Reports.
audit_report = f"""# Day 43 Final Robustness Audit

## Research status

This audit uses only chronological out-of-sample signals generated on Day 42.
No model was refitted and no threshold was searched.

## Candidate

- Model: `{CANDIDATE_MODEL}`
- MOVE threshold: `{MOVE_THRESHOLD}`
- Direction threshold: `{DIRECTION_THRESHOLD}`
- Horizon: `{HORIZON}` events
- Cost: `{PRIMARY_COST_BPS:.1f}` bps
- Entry rule: first eligible
- Cooldown: `{COOLDOWN_EVENTS}` events

## Candidate pooled result

- Signals: `{int(candidate_audit['n_signals'])}`
- Signal-bearing runs: `{int(candidate_audit['n_runs'])}`
- Directional precision: `{candidate_audit['directional_precision']:.6f}`
- Positive-net signal share: `{candidate_audit['positive_net_signal_share']:.6f}`
- Mean gross: `{candidate_audit['mean_gross_bps']:.6f}` bps
- Mean net: `{candidate_audit['mean_net_bps']:.6f}` bps
- Median net: `{candidate_audit['median_net_bps']:.6f}` bps

## Robustness

- Mean net without best signal: `{candidate_audit['mean_net_without_best_signal_bps']:.6f}` bps
- Mean net without best run: `{candidate_audit['mean_net_without_best_run_bps']:.6f}` bps
- Worst leave-one-fold-out mean net: `{candidate_audit['leave_one_fold_out_min_mean_net_bps']:.6f}` bps
- Run-cluster bootstrap 95% CI: `[{candidate_audit['bootstrap_ci_lower_2_5']:.6f}, {candidate_audit['bootstrap_ci_upper_97_5']:.6f}]`
- Bootstrap share of positive means: `{candidate_audit['bootstrap_share_mean_net_positive']:.6f}`

## Decision

`{final_decision}`

A frozen candidate is not a validated profitable strategy. It is only an
eligible specification for one final independent holdout.
"""
AUDIT_REPORT_PATH.write_text(audit_report, encoding="utf-8")

if hard_pass:
    move_features_text = "\n".join(f"- `{feature}`" for feature in MEDIUM_TRADE_FLOW_FEATURES)
    direction_features_text = "\n".join(f"- `{feature}`" for feature in REDUCED_BOOK_FEATURES)
    train_batches_text = "\n".join(f"- `{batch}`" for batch in FINAL_TRAIN_BATCHES)

    preregistration = f"""# Final Fresh-Holdout Preregistration

## Status

This specification was frozen after the Day 43 robustness audit and before
collecting or evaluating the final holdout batch.

## Final training data

Strict-quality runs from:

{train_batches_text}

Friday is development data and is not an independent test for this model.

## Final holdout batch

- Batch label: `{FINAL_HOLDOUT_BATCH}`
- Target: 36 runs
- Requested duration: 300 seconds per run
- Collection window: next eligible weekday active-hours window
- Symbol: BTCUSDT USD-M perpetual futures

## Technical quality rules

- collection status is success;
- collection duration is at least {MIN_COLLECTION_SECONDS} seconds;
- depth events are at least {MIN_DEPTH_EVENTS};
- trade events are at least {MIN_TRADE_EVENTS};
- processed rows are at least {MIN_PROCESSED_ROWS};
- top-of-book reconstruction succeeds.

No run may be excluded because of PnL, volatility, prediction quality or model
outcomes.

## Frozen MOVE stage

- Logistic regression
- `C = {MOVE_C}`
- class weighting: balanced
- features:

{move_features_text}

## Frozen direction stage

- Logistic regression
- `C = {DIRECTION_C}`
- class weighting: balanced
- features:

{direction_features_text}

## Frozen deployment rule

- Horizon: {HORIZON} book events
- MOVE target dead zone: {DEAD_ZONE_BPS:.1f} bps
- MOVE threshold: {MOVE_THRESHOLD}
- Direction threshold: {DIRECTION_THRESHOLD}
- Entry: first eligible observation
- Cooldown: {COOLDOWN_EVENTS} events
- Round-trip cost assumption: {PRIMARY_COST_BPS:.1f} bps

No threshold, feature, hyperparameter, horizon, cost assumption, entry rule or
quality rule may be changed after the holdout data are inspected.

## Primary evaluation

The primary estimand is mean net signed h50 return per selected signal after a
1 bps round-trip cost.

The report must include signal count, signal-bearing runs, directional
precision, gross and net returns, LONG/SHORT results, run-level robustness,
leave-one-run-out results, run-cluster bootstrap and cost sensitivity.

The final holdout is evaluated once. No post-hoc replacement model or threshold
is permitted.
"""
    PREREGISTRATION_PATH.write_text(preregistration, encoding="utf-8")
elif PREREGISTRATION_PATH.exists():
    PREREGISTRATION_PATH.unlink()

print()
print("=" * 88)
print("[INFO] Day 43 fold comparison")
print("=" * 88)
print(
    fold_comparison[
        [
            "fold",
            "model",
            "n_signals",
            "n_signal_runs",
            "directional_precision",
            "mean_net_return_bps",
            "median_net_return_bps",
            "total_net_return_bps",
        ]
    ].to_string(index=False)
)

print()
print("=" * 88)
print("[INFO] Day 43 robustness summary")
print("=" * 88)
print(robustness.to_string(index=False))

print()
print("=" * 88)
print("[INFO] Day 43 freeze decision")
print("=" * 88)
print(freeze_decision.to_string(index=False))

print()
print(f"[INFO] Final decision: {final_decision}")
print("[INFO] Day 43 robustness audit completed successfully.")