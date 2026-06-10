import subprocess
import sys
import time


N_RUNS = 8
SLEEP_BETWEEN_RUNS_SECONDS = 30


def main() -> None:
    print("[INFO] Starting multiple raw data collection runs")
    print(f"[INFO] Number of runs: {N_RUNS}")
    print(f"[INFO] Sleep between runs: {SLEEP_BETWEEN_RUNS_SECONDS} seconds")

    for run_id in range(1, N_RUNS + 1):
        print("\n" + "=" * 80)
        print(f"[INFO] Starting run {run_id}/{N_RUNS}")
        print("=" * 80)

        subprocess.run(
            [sys.executable, "scripts/collect_raw.py"],
            check=True,
        )

        print(f"[INFO] Finished run {run_id}/{N_RUNS}")

        if run_id < N_RUNS:
            print(f"[INFO] Sleeping for {SLEEP_BETWEEN_RUNS_SECONDS} seconds...")
            time.sleep(SLEEP_BETWEEN_RUNS_SECONDS)

    print("\n[INFO] Multiple raw data collection completed successfully.")


if __name__ == "__main__":
    main()