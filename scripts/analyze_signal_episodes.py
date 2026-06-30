from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = Path("reports/tables/multirun_advanced_ml_validation_selected_signals_h50.csv")

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

TARGET_HORIZON = 50
COOLDOWN_EVENTS = 50
EPISODE_GAP_EVENTS = 50

EPISODES_PATH = TABLES_DIR / "multirun_signal_episodes_h50.csv"
COOLDOWN_SIGNALS_PATH = TABLES_DIR / "multirun_signal_cooldown_selected_h50.csv"
EVALUATION_ALL_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_evaluation_h50.csv"
SUMMARY_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_summary_h50.csv"
PER_RUN_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_per_run_h50.csv"
DIRECTION_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_direction_h50.csv"
COST_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_cost_sanity_h50.csv"
CONCENTRATION_PATH = TABLES_DIR / "multirun_signal_episode_cooldown_concentration_h50.csv"

COST_SCENARIOS = [
    {"cost_scenario": "no_cost", "round_trip_cost_bps": 0.0},
    {"cost_scenario": "very_low_cost", "round_trip_cost_bps": 1.0},
    {"cost_scenario": "low_cost", "round_trip_cost_bps": 2.0},
    {"cost_scenario": "medium_cost", "round_trip_cost_bps": 3.0},
    {"cost_scenario": "near_best_raw_edge", "round_trip_cost_bps": 3.5},
    {"cost_scenario": "high_cost", "round_trip_cost_bps": 4.0},
    {"cost_scenario": "expensive_taker_like", "round_trip_cost_bps": 5.0},
    {"cost_scenario": "very_expensive_round_trip", "round_trip_cost_bps": 9.0},
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_selected_signals() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. "
            "Run scripts/validate_advanced_ml_candidates.py first."
        )

    df = pd.read_csv(INPUT_PATH)

    required_cols = [
        "model",
        "algorithm",
        "feature_set",
        "threshold",
        "run_name",
        "row_in_run",
        "event_time",
        "target_label",
        "signal",
        "predicted_proba_up",
        "future_mid_return_bps",
        "signed_return_bps",
        "is_correct_signal",
        "is_positive_signed_return",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.sort_values(["model", "run_name", "row_in_run"]).reset_index(drop=True)

    df["is_correct_signal"] = df["is_correct_signal"].astype(bool)
    df["is_positive_signed_return"] = df["is_positive_signed_return"].astype(bool)

    return df


def build_signal_episodes(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    group_cols = ["model", "run_name", "signal"]

    for (model_name, run_name, signal), group in df.groupby(group_cols, sort=True):
        group = group.sort_values("row_in_run").reset_index(drop=True)

        episode_id = 0
        current_episode_rows = []
        previous_row = None

        for _, row in group.iterrows():
            row_number = int(row["row_in_run"])

            if previous_row is None:
                current_episode_rows = [row]
                previous_row = row_number
                continue

            gap = row_number - previous_row

            if gap <= EPISODE_GAP_EVENTS:
                current_episode_rows.append(row)
            else:
                episode_id += 1
                rows.append(
                    summarize_episode(
                        episode_rows=current_episode_rows,
                        model_name=model_name,
                        run_name=run_name,
                        signal=signal,
                        episode_id=episode_id,
                    )
                )

                current_episode_rows = [row]

            previous_row = row_number

        if current_episode_rows:
            episode_id += 1
            rows.append(
                summarize_episode(
                    episode_rows=current_episode_rows,
                    model_name=model_name,
                    run_name=run_name,
                    signal=signal,
                    episode_id=episode_id,
                )
            )

    episodes = pd.DataFrame(rows)

    if episodes.empty:
        return episodes

    episodes = episodes.sort_values(
        ["model", "run_name", "episode_start_row"]
    ).reset_index(drop=True)

    return episodes


def summarize_episode(
    episode_rows: list,
    model_name: str,
    run_name: str,
    signal: str,
    episode_id: int,
) -> dict:
    episode_df = pd.DataFrame(episode_rows)

    first = episode_df.iloc[0]
    last = episode_df.iloc[-1]

    return {
        "evaluation_method": "episode_first_signal",
        "model": model_name,
        "algorithm": first["algorithm"],
        "feature_set": first["feature_set"],
        "threshold": first["threshold"],
        "run_name": run_name,
        "signal": signal,
        "episode_id": episode_id,
        "episode_start_row": int(first["row_in_run"]),
        "episode_end_row": int(last["row_in_run"]),
        "episode_duration_events": int(last["row_in_run"] - first["row_in_run"]),
        "n_raw_signals_in_episode": len(episode_df),
        "target_label": first["target_label"],
        "predicted_proba_up": first["predicted_proba_up"],
        "future_mid_return_bps": first["future_mid_return_bps"],
        "signed_return_bps": first["signed_return_bps"],
        "is_correct_signal": bool(first["is_correct_signal"]),
        "is_positive_signed_return": bool(first["is_positive_signed_return"]),
        "mean_raw_signed_return_bps_in_episode": episode_df["signed_return_bps"].mean(),
        "max_raw_signed_return_bps_in_episode": episode_df["signed_return_bps"].max(),
        "min_raw_signed_return_bps_in_episode": episode_df["signed_return_bps"].min(),
    }


def apply_cooldown(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    group_cols = ["model", "run_name"]

    for (model_name, run_name), group in df.groupby(group_cols, sort=True):
        group = group.sort_values("row_in_run").reset_index(drop=True)

        last_selected_row = None
        cooldown_trade_id = 0

        for _, row in group.iterrows():
            row_number = int(row["row_in_run"])

            if last_selected_row is not None:
                if row_number <= last_selected_row + COOLDOWN_EVENTS:
                    continue

            cooldown_trade_id += 1
            last_selected_row = row_number

            rows.append(
                {
                    "evaluation_method": "cooldown_first_signal",
                    "model": model_name,
                    "algorithm": row["algorithm"],
                    "feature_set": row["feature_set"],
                    "threshold": row["threshold"],
                    "run_name": run_name,
                    "signal": row["signal"],
                    "cooldown_trade_id": cooldown_trade_id,
                    "episode_start_row": row_number,
                    "episode_end_row": row_number,
                    "episode_duration_events": 0,
                    "n_raw_signals_in_episode": 1,
                    "target_label": row["target_label"],
                    "predicted_proba_up": row["predicted_proba_up"],
                    "future_mid_return_bps": row["future_mid_return_bps"],
                    "signed_return_bps": row["signed_return_bps"],
                    "is_correct_signal": bool(row["is_correct_signal"]),
                    "is_positive_signed_return": bool(row["is_positive_signed_return"]),
                    "mean_raw_signed_return_bps_in_episode": row["signed_return_bps"],
                    "max_raw_signed_return_bps_in_episode": row["signed_return_bps"],
                    "min_raw_signed_return_bps_in_episode": row["signed_return_bps"],
                }
            )

    cooldown = pd.DataFrame(rows)

    if cooldown.empty:
        return cooldown

    cooldown = cooldown.sort_values(
        ["model", "run_name", "episode_start_row"]
    ).reset_index(drop=True)

    return cooldown


def summarize_overall(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name), group in evaluation.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "algorithm": group["algorithm"].iloc[0],
                "feature_set": group["feature_set"].iloc[0],
                "threshold": group["threshold"].iloc[0],
                "n_trades_or_episodes": len(group),
                "raw_signals_represented": group["n_raw_signals_in_episode"].sum(),
                "avg_raw_signals_per_episode": group["n_raw_signals_in_episode"].mean(),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_share": (group["signal"] == "up").mean(),
                "down_share": (group["signal"] == "down").mean(),
                "break_even_round_trip_cost_bps": group["signed_return_bps"].mean(),
            }
        )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        ["evaluation_method", "break_even_round_trip_cost_bps", "precision"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return result


def summarize_per_run(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name, run_name), group in evaluation.groupby(
        ["evaluation_method", "model", "run_name"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "run_name": run_name,
                "n_trades_or_episodes": len(group),
                "raw_signals_represented": group["n_raw_signals_in_episode"].sum(),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "up_count": int((group["signal"] == "up").sum()),
                "down_count": int((group["signal"] == "down").sum()),
                "total_signed_return_bps": group["signed_return_bps"].sum(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "run_name"]
    ).reset_index(drop=True)


def summarize_direction(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name, signal), group in evaluation.groupby(
        ["evaluation_method", "model", "signal"],
        sort=True,
    ):
        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "signal": signal,
                "n_trades_or_episodes": len(group),
                "precision": group["is_correct_signal"].mean(),
                "mean_signed_return_bps": group["signed_return_bps"].mean(),
                "median_signed_return_bps": group["signed_return_bps"].median(),
                "positive_signed_return_share": group["is_positive_signed_return"].mean(),
                "mean_future_return_bps": group["future_mid_return_bps"].mean(),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "signal"]
    ).reset_index(drop=True)


def summarize_cost_scenarios(evaluation: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name), group in evaluation.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        for scenario in COST_SCENARIOS:
            cost_name = scenario["cost_scenario"]
            cost_bps = scenario["round_trip_cost_bps"]

            temp = group.copy()
            temp["net_signed_return_bps"] = temp["signed_return_bps"] - cost_bps
            temp["is_net_positive"] = temp["net_signed_return_bps"] > 0

            rows.append(
                {
                    "evaluation_method": method,
                    "model": model_name,
                    "cost_scenario": cost_name,
                    "round_trip_cost_bps": cost_bps,
                    "n_trades_or_episodes": len(temp),
                    "gross_mean_signed_return_bps": temp["signed_return_bps"].mean(),
                    "net_mean_signed_return_bps": temp["net_signed_return_bps"].mean(),
                    "net_median_signed_return_bps": temp["net_signed_return_bps"].median(),
                    "net_positive_share": temp["is_net_positive"].mean(),
                }
            )

    result = pd.DataFrame(rows)

    return result.sort_values(
        ["evaluation_method", "model", "round_trip_cost_bps"]
    ).reset_index(drop=True)


def summarize_concentration(per_run: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (method, model_name), group in per_run.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        total_count = group["n_trades_or_episodes"].sum()

        if total_count == 0:
            continue

        temp = group.copy()
        temp["run_share"] = temp["n_trades_or_episodes"] / total_count

        rows.append(
            {
                "evaluation_method": method,
                "model": model_name,
                "test_runs_with_trades_or_episodes": int((temp["n_trades_or_episodes"] > 0).sum()),
                "max_run_share": temp["run_share"].max(),
                "positive_run_share": (temp["mean_signed_return_bps"] > 0).mean(),
                "min_run_mean_signed_return_bps": temp["mean_signed_return_bps"].min(),
                "max_run_mean_signed_return_bps": temp["mean_signed_return_bps"].max(),
                "all_runs_positive": bool((temp["mean_signed_return_bps"] > 0).all()),
            }
        )

    result = pd.DataFrame(rows)

    return result.sort_values(
        [
            "evaluation_method",
            "all_runs_positive",
            "positive_run_share",
            "min_run_mean_signed_return_bps",
        ],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)


def save_summary_plot(summary: pd.DataFrame) -> None:
    for method, group in summary.groupby("evaluation_method", sort=True):
        group = group.sort_values("break_even_round_trip_cost_bps", ascending=False)

        plt.figure(figsize=(10, 6))
        plt.bar(
            group["model"],
            group["break_even_round_trip_cost_bps"],
        )
        plt.axhline(0)
        plt.xticks(rotation=75, ha="right")
        plt.title(f"Break-even cost by model: {method}")
        plt.xlabel("Model")
        plt.ylabel("Break-even round-trip cost, bps")
        plt.tight_layout()

        out_path = FIGURES_DIR / f"multirun_signal_{method}_summary_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved summary plot to: {out_path}")


def save_cost_plot(cost: pd.DataFrame) -> None:
    for method, method_df in cost.groupby("evaluation_method", sort=True):
        plt.figure(figsize=(10, 6))

        for model_name, group in method_df.groupby("model", sort=True):
            group = group.sort_values("round_trip_cost_bps")
            plt.plot(
                group["round_trip_cost_bps"],
                group["net_mean_signed_return_bps"],
                marker="o",
                label=model_name,
            )

        plt.axhline(0)
        plt.title(f"Cost sanity: {method}")
        plt.xlabel("Round-trip cost, bps")
        plt.ylabel("Net mean signed return, bps")
        plt.legend(fontsize=7)
        plt.tight_layout()

        out_path = FIGURES_DIR / f"multirun_signal_{method}_cost_sanity_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved cost plot to: {out_path}")


def save_per_run_plot(per_run: pd.DataFrame) -> None:
    for (method, model_name), group in per_run.groupby(
        ["evaluation_method", "model"],
        sort=True,
    ):
        group = group.sort_values("run_name")

        plt.figure(figsize=(8, 5))
        plt.bar(
            group["run_name"],
            group["mean_signed_return_bps"],
        )
        plt.axhline(0)
        plt.xticks(rotation=45, ha="right")
        plt.title(f"Per-run mean signed return: {model_name}, {method}")
        plt.xlabel("Run")
        plt.ylabel("Mean signed return, bps")
        plt.tight_layout()

        safe_model_name = model_name.replace("/", "_")
        out_path = FIGURES_DIR / f"multirun_signal_{method}_per_run_{safe_model_name}_h50.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"[INFO] Saved per-run plot to: {out_path}")


def save_all_plots(summary: pd.DataFrame, cost: pd.DataFrame, per_run: pd.DataFrame) -> None:
    save_summary_plot(summary)
    save_cost_plot(cost)
    save_per_run_plot(per_run)


def main() -> None:
    ensure_output_dirs()

    selected = load_selected_signals()

    print(f"[INFO] Loaded selected signals: {INPUT_PATH}")
    print(f"[INFO] Raw selected signals: {len(selected)}")

    print("\n[INFO] Raw selected signals by model:")
    print(selected["model"].value_counts())

    episodes = build_signal_episodes(selected)
    cooldown = apply_cooldown(selected)

    episodes.to_csv(EPISODES_PATH, index=False)
    cooldown.to_csv(COOLDOWN_SIGNALS_PATH, index=False)

    print(f"\n[INFO] Saved episodes to: {EPISODES_PATH}")
    print(f"[INFO] Saved cooldown-selected signals to: {COOLDOWN_SIGNALS_PATH}")

    print(f"[INFO] Episode-level rows: {len(episodes)}")
    print(f"[INFO] Cooldown-selected rows: {len(cooldown)}")

    evaluation_all = pd.concat(
        [episodes, cooldown],
        ignore_index=True,
        sort=False,
    )

    evaluation_all.to_csv(EVALUATION_ALL_PATH, index=False)

    summary = summarize_overall(evaluation_all)
    per_run = summarize_per_run(evaluation_all)
    direction = summarize_direction(evaluation_all)
    cost = summarize_cost_scenarios(evaluation_all)
    concentration = summarize_concentration(per_run)

    summary.to_csv(SUMMARY_PATH, index=False)
    per_run.to_csv(PER_RUN_PATH, index=False)
    direction.to_csv(DIRECTION_PATH, index=False)
    cost.to_csv(COST_PATH, index=False)
    concentration.to_csv(CONCENTRATION_PATH, index=False)

    save_all_plots(
        summary=summary,
        cost=cost,
        per_run=per_run,
    )

    print(f"\n[INFO] Saved evaluation rows to: {EVALUATION_ALL_PATH}")
    print(f"[INFO] Saved summary to: {SUMMARY_PATH}")
    print(f"[INFO] Saved per-run summary to: {PER_RUN_PATH}")
    print(f"[INFO] Saved direction summary to: {DIRECTION_PATH}")
    print(f"[INFO] Saved cost sanity to: {COST_PATH}")
    print(f"[INFO] Saved concentration summary to: {CONCENTRATION_PATH}")

    print("\n[INFO] Summary:")
    print(
        summary[
            [
                "evaluation_method",
                "model",
                "n_trades_or_episodes",
                "raw_signals_represented",
                "avg_raw_signals_per_episode",
                "precision",
                "mean_signed_return_bps",
                "break_even_round_trip_cost_bps",
                "up_share",
                "down_share",
            ]
        ]
    )

    print("\n[INFO] Concentration:")
    print(concentration)

    print("\n[INFO] Signal episode / cooldown analysis completed successfully.")


if __name__ == "__main__":
    main()