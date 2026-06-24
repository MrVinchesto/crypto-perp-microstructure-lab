from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


TARGET_HORIZON = 10

SELECTED_SIGNALS_PATH = Path(
    f"reports/tables/multirun_threshold_selected_signals_h{TARGET_HORIZON}.csv"
)

THRESHOLD_DIAGNOSTICS_PATH = Path(
    f"reports/tables/multirun_threshold_diagnostics_h{TARGET_HORIZON}.csv"
)

REPORTS_DIR = Path("reports")
TABLES_DIR = REPORTS_DIR / "tables"
FIGURES_DIR = REPORTS_DIR / "figures"

OUTPUT_SANITY_PATH = TABLES_DIR / f"multirun_transaction_cost_sanity_h{TARGET_HORIZON}.csv"
OUTPUT_BREAKEVEN_PATH = TABLES_DIR / f"multirun_transaction_cost_break_even_h{TARGET_HORIZON}.csv"

MODELS_TO_ANALYZE = [
    "logit_signal",
    "logit_full",
]

COST_SCENARIOS = [
    {
        "cost_scenario": "no_cost",
        "round_trip_cost_bps": 0.0,
        "description": "Gross signal without transaction costs",
    },
    {
        "cost_scenario": "very_optimistic_passive_like",
        "round_trip_cost_bps": 0.5,
        "description": "Very optimistic low-cost passive-like scenario",
    },
    {
        "cost_scenario": "optimistic_low_cost",
        "round_trip_cost_bps": 1.0,
        "description": "Optimistic low round-trip cost scenario",
    },
    {
        "cost_scenario": "moderate_cost",
        "round_trip_cost_bps": 2.0,
        "description": "Moderate round-trip cost scenario",
    },
    {
        "cost_scenario": "expensive_taker_like",
        "round_trip_cost_bps": 5.0,
        "description": "Expensive aggressive/taker-like scenario",
    },
    {
        "cost_scenario": "very_expensive_round_trip",
        "round_trip_cost_bps": 9.0,
        "description": "Very expensive round-trip cost scenario",
    },
]


def ensure_output_dirs() -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def load_selected_signals() -> pd.DataFrame:
    if not SELECTED_SIGNALS_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {SELECTED_SIGNALS_PATH}. "
            "Run scripts/analyze_model_thresholds.py first."
        )

    df = pd.read_csv(SELECTED_SIGNALS_PATH)

    required_cols = [
        "model",
        "threshold",
        "run_name",
        "row_in_run",
        "event_time",
        "target_label",
        "future_mid_return_10e_bps",
        "predicted_proba_up",
        "signal",
        "is_correct_signal",
        "signed_return_bps",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns in selected signals: {missing}")

    df = df[df["model"].isin(MODELS_TO_ANALYZE)].copy()

    numeric_cols = [
        "threshold",
        "future_mid_return_10e_bps",
        "predicted_proba_up",
        "signed_return_bps",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["threshold", "signed_return_bps"]).copy()

    return df


def load_threshold_diagnostics() -> pd.DataFrame:
    if not THRESHOLD_DIAGNOSTICS_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {THRESHOLD_DIAGNOSTICS_PATH}. "
            "Run scripts/analyze_model_thresholds.py first."
        )

    df = pd.read_csv(THRESHOLD_DIAGNOSTICS_PATH)

    required_cols = [
        "model",
        "threshold",
        "signal_group",
        "n_signals",
        "coverage",
        "precision",
        "mean_signed_return_bps",
    ]

    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns in diagnostics: {missing}")

    df = df[df["model"].isin(MODELS_TO_ANALYZE)].copy()

    return df


def infer_total_observations(threshold_diagnostics: pd.DataFrame) -> pd.DataFrame:
    both = threshold_diagnostics[
        threshold_diagnostics["signal_group"] == "both"
    ].copy()

    both["inferred_total_observations"] = (
        both["n_signals"] / both["coverage"]
    ).round().astype(int)

    return both[
        [
            "model",
            "threshold",
            "inferred_total_observations",
        ]
    ]


def expand_with_cost_scenarios(selected_signals: pd.DataFrame) -> pd.DataFrame:
    cost_df = pd.DataFrame(COST_SCENARIOS)

    selected_signals = selected_signals.copy()
    selected_signals["_join_key"] = 1

    cost_df["_join_key"] = 1

    expanded = selected_signals.merge(cost_df, on="_join_key").drop(columns=["_join_key"])

    expanded["net_signed_return_bps"] = (
        expanded["signed_return_bps"] - expanded["round_trip_cost_bps"]
    )

    expanded["is_net_positive"] = expanded["net_signed_return_bps"] > 0

    return expanded


def summarize_transaction_costs(
    expanded: pd.DataFrame,
    total_obs: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    for (
        model_name,
        threshold,
        cost_scenario,
        round_trip_cost_bps,
    ), group in expanded.groupby(
        ["model", "threshold", "cost_scenario", "round_trip_cost_bps"],
        sort=True,
    ):
        total_row = total_obs[
            (total_obs["model"] == model_name) &
            (total_obs["threshold"] == threshold)
        ]

        if total_row.empty:
            total_observations = None
            coverage = None
        else:
            total_observations = int(total_row["inferred_total_observations"].iloc[0])
            coverage = len(group) / total_observations

        for signal_group_name, subset in [
            ("up", group[group["signal"] == "up"]),
            ("down", group[group["signal"] == "down"]),
            ("both", group),
        ]:
            n_signals = len(subset)

            if n_signals == 0:
                rows.append(
                    {
                        "model": model_name,
                        "threshold": threshold,
                        "cost_scenario": cost_scenario,
                        "round_trip_cost_bps": round_trip_cost_bps,
                        "signal_group": signal_group_name,
                        "n_signals": 0,
                        "coverage": 0.0,
                        "gross_mean_signed_return_bps": None,
                        "net_mean_signed_return_bps": None,
                        "net_median_signed_return_bps": None,
                        "net_positive_share": None,
                        "gross_positive_share": None,
                        "mean_future_return_bps": None,
                    }
                )
                continue

            rows.append(
                {
                    "model": model_name,
                    "threshold": threshold,
                    "cost_scenario": cost_scenario,
                    "round_trip_cost_bps": round_trip_cost_bps,
                    "signal_group": signal_group_name,
                    "n_signals": n_signals,
                    "coverage": coverage if signal_group_name == "both" else (
                        n_signals / total_observations if total_observations else None
                    ),
                    "gross_mean_signed_return_bps": subset["signed_return_bps"].mean(),
                    "net_mean_signed_return_bps": subset["net_signed_return_bps"].mean(),
                    "net_median_signed_return_bps": subset["net_signed_return_bps"].median(),
                    "net_positive_share": subset["is_net_positive"].mean(),
                    "gross_positive_share": (subset["signed_return_bps"] > 0).mean(),
                    "mean_future_return_bps": subset["future_mid_return_10e_bps"].mean(),
                }
            )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        [
            "model",
            "threshold",
            "signal_group",
            "round_trip_cost_bps",
        ]
    ).reset_index(drop=True)

    return result


def build_break_even_table(threshold_diagnostics: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in threshold_diagnostics.iterrows():
        model_name = row["model"]
        threshold = row["threshold"]
        signal_group = row["signal_group"]

        gross_mean_signed_return = row["mean_signed_return_bps"]

        rows.append(
            {
                "model": model_name,
                "threshold": threshold,
                "signal_group": signal_group,
                "n_signals": row["n_signals"],
                "coverage": row["coverage"],
                "precision": row["precision"],
                "break_even_round_trip_cost_bps": gross_mean_signed_return,
                "interpretation": (
                    "Average net signal is positive only if total round-trip costs "
                    "are below this value."
                ),
            }
        )

    result = pd.DataFrame(rows)

    return result


def save_net_return_plot(sanity: pd.DataFrame, model_name: str) -> None:
    subset = sanity[
        (sanity["model"] == model_name) &
        (sanity["signal_group"] == "both")
    ].copy()

    plt.figure()

    for cost_scenario, scenario_df in subset.groupby("cost_scenario", sort=False):
        scenario_df = scenario_df.sort_values("threshold")
        plt.plot(
            scenario_df["threshold"],
            scenario_df["net_mean_signed_return_bps"],
            marker="o",
            label=cost_scenario,
        )

    plt.axhline(0)
    plt.title(f"Net mean signed return by threshold: {model_name}, h={TARGET_HORIZON}")
    plt.xlabel("Threshold")
    plt.ylabel("Net mean signed return, bps")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_transaction_cost_net_return_{model_name}_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved net return plot to: {out_path}")


def save_profitable_share_plot(sanity: pd.DataFrame, model_name: str) -> None:
    subset = sanity[
        (sanity["model"] == model_name) &
        (sanity["signal_group"] == "both")
    ].copy()

    plt.figure()

    for cost_scenario, scenario_df in subset.groupby("cost_scenario", sort=False):
        scenario_df = scenario_df.sort_values("threshold")
        plt.plot(
            scenario_df["threshold"],
            scenario_df["net_positive_share"],
            marker="o",
            label=cost_scenario,
        )

    plt.title(f"Net positive share by threshold: {model_name}, h={TARGET_HORIZON}")
    plt.xlabel("Threshold")
    plt.ylabel("Share of signals with net return > 0")
    plt.legend(fontsize=7)
    plt.tight_layout()

    out_path = FIGURES_DIR / f"multirun_transaction_cost_profitable_share_{model_name}_h{TARGET_HORIZON}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"[INFO] Saved profitable share plot to: {out_path}")


def save_all_plots(sanity: pd.DataFrame) -> None:
    for model_name in MODELS_TO_ANALYZE:
        save_net_return_plot(sanity, model_name)
        save_profitable_share_plot(sanity, model_name)


def print_key_diagnostics(
    sanity: pd.DataFrame,
    break_even: pd.DataFrame,
) -> None:
    print("\n[INFO] Break-even round-trip cost, combined UP/DOWN signals:")
    print(
        break_even[
            break_even["signal_group"] == "both"
        ][
            [
                "model",
                "threshold",
                "n_signals",
                "coverage",
                "precision",
                "break_even_round_trip_cost_bps",
            ]
        ]
    )

    print("\n[INFO] Transaction cost sanity, combined UP/DOWN signals:")
    print(
        sanity[
            sanity["signal_group"] == "both"
        ][
            [
                "model",
                "threshold",
                "cost_scenario",
                "round_trip_cost_bps",
                "n_signals",
                "coverage",
                "gross_mean_signed_return_bps",
                "net_mean_signed_return_bps",
                "net_positive_share",
            ]
        ]
    )


def main() -> None:
    ensure_output_dirs()

    selected_signals = load_selected_signals()
    threshold_diagnostics = load_threshold_diagnostics()

    total_obs = infer_total_observations(threshold_diagnostics)

    expanded = expand_with_cost_scenarios(selected_signals)

    sanity = summarize_transaction_costs(
        expanded=expanded,
        total_obs=total_obs,
    )

    break_even = build_break_even_table(threshold_diagnostics)

    sanity.to_csv(OUTPUT_SANITY_PATH, index=False)
    break_even.to_csv(OUTPUT_BREAKEVEN_PATH, index=False)

    save_all_plots(sanity)

    print(f"[INFO] Saved transaction cost sanity table to: {OUTPUT_SANITY_PATH}")
    print(f"[INFO] Saved break-even table to: {OUTPUT_BREAKEVEN_PATH}")

    print_key_diagnostics(
        sanity=sanity,
        break_even=break_even,
    )

    print("\n[INFO] Transaction-cost sanity check completed successfully.")


if __name__ == "__main__":
    main()