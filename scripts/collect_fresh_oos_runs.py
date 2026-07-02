from pathlib import Path
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import yaml


CONFIG_PATH = Path("config.yaml")
RAW_BASE_DIR = Path("data/raw")
REPORTS_TABLES_DIR = Path("reports/tables")

SYMBOL = "BTCUSDT"

N_RUNS = 12
COLLECTION_SECONDS = 300
PAUSE_BETWEEN_RUNS_SECONDS = 5

COLLECTION_LOG_PATH = REPORTS_TABLES_DIR / "fresh_oos_collection_log.csv"


def ensure_dirs() -> None:
    REPORTS_TABLES_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_config(config: dict) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)


def list_run_dirs() -> set[Path]:
    symbol_dir = RAW_BASE_DIR / SYMBOL

    if not symbol_dir.exists():
        return set()

    return {
        path
        for path in symbol_dir.iterdir()
        if path.is_dir()
    }


def get_new_run_dir(before_dirs: set[Path], after_dirs: set[Path]) -> Path | None:
    new_dirs = sorted(after_dirs - before_dirs)

    if not new_dirs:
        return None

    return new_dirs[-1]


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0

    count = 0

    with path.open("r", encoding="utf-8") as f:
        for _ in f:
            count += 1

    return count


def read_meta(run_dir: Path) -> dict:
    meta_path = run_dir / "meta.json"

    if not meta_path.exists():
        return {}

    with meta_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def summarize_run(run_dir: Path, run_number: int, status: str, error: str | None = None) -> dict:
    depth_path = run_dir / "depth.jsonl"
    trades_path = run_dir / "trades.jsonl"
    snapshot_path = run_dir / "snapshot.json"

    meta = read_meta(run_dir)

    return {
        "collection_batch": "fresh_oos_day20",
        "run_number": run_number,
        "status": status,
        "error": error,
        "run_name": run_dir.name,
        "run_dir": str(run_dir),
        "collection_seconds_requested": COLLECTION_SECONDS,
        "started_or_logged_at_utc": utc_now_iso(),
        "snapshot_exists": snapshot_path.exists(),
        "depth_events": count_jsonl_lines(depth_path),
        "trade_events": count_jsonl_lines(trades_path),
        "meta_symbol": meta.get("symbol"),
        "meta_collection_seconds": meta.get("collection_seconds"),
        "meta_started_at": meta.get("started_at"),
        "meta_finished_at": meta.get("finished_at"),
    }


def append_log_row(row: dict) -> None:
    if COLLECTION_LOG_PATH.exists():
        old = pd.read_csv(COLLECTION_LOG_PATH)
        new = pd.concat([old, pd.DataFrame([row])], ignore_index=True)
    else:
        new = pd.DataFrame([row])

    new.to_csv(COLLECTION_LOG_PATH, index=False)


def run_single_collection(run_number: int) -> dict:
    print("=" * 80)
    print(f"[INFO] Starting fresh OOS collection run {run_number}/{N_RUNS}")
    print(f"[INFO] Requested collection_seconds: {COLLECTION_SECONDS}")
    print("=" * 80)

    before_dirs = list_run_dirs()

    command = [
        sys.executable,
        "scripts/collect_raw.py",
    ]

    completed = subprocess.run(
        command,
        capture_output=False,
        text=True,
    )

    after_dirs = list_run_dirs()
    new_run_dir = get_new_run_dir(before_dirs, after_dirs)

    if completed.returncode != 0:
        error = f"collect_raw.py returned non-zero exit code: {completed.returncode}"

        print(f"[WARNING] {error}")

        if new_run_dir is not None:
            return summarize_run(
                run_dir=new_run_dir,
                run_number=run_number,
                status="failed",
                error=error,
            )

        return {
            "collection_batch": "fresh_oos_day20",
            "run_number": run_number,
            "status": "failed",
            "error": error,
            "run_name": None,
            "run_dir": None,
            "collection_seconds_requested": COLLECTION_SECONDS,
            "started_or_logged_at_utc": utc_now_iso(),
            "snapshot_exists": False,
            "depth_events": 0,
            "trade_events": 0,
            "meta_symbol": None,
            "meta_collection_seconds": None,
            "meta_started_at": None,
            "meta_finished_at": None,
        }

    if new_run_dir is None:
        error = "No new raw run directory detected."

        print(f"[WARNING] {error}")

        return {
            "collection_batch": "fresh_oos_day20",
            "run_number": run_number,
            "status": "failed",
            "error": error,
            "run_name": None,
            "run_dir": None,
            "collection_seconds_requested": COLLECTION_SECONDS,
            "started_or_logged_at_utc": utc_now_iso(),
            "snapshot_exists": False,
            "depth_events": 0,
            "trade_events": 0,
            "meta_symbol": None,
            "meta_collection_seconds": None,
            "meta_started_at": None,
            "meta_finished_at": None,
        }

    row = summarize_run(
        run_dir=new_run_dir,
        run_number=run_number,
        status="success",
        error=None,
    )

    print(f"[INFO] New run directory: {new_run_dir}")
    print(f"[INFO] Depth events: {row['depth_events']}")
    print(f"[INFO] Trade events: {row['trade_events']}")

    return row


def main() -> None:
    ensure_dirs()

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")

    backup_path = CONFIG_PATH.with_suffix(".yaml.day20_backup")

    print(f"[INFO] Backing up config to: {backup_path}")
    shutil.copy2(CONFIG_PATH, backup_path)

    original_config = load_config()

    updated_config = dict(original_config)
    updated_config["symbol"] = SYMBOL
    updated_config["collection_seconds"] = COLLECTION_SECONDS
    updated_config["output_dir"] = str(RAW_BASE_DIR)

    print("[INFO] Temporarily updating config.yaml for fresh OOS collection")
    save_config(updated_config)

    try:
        for run_number in range(1, N_RUNS + 1):
            row = run_single_collection(run_number)
            append_log_row(row)

            print(f"[INFO] Logged run {run_number} to: {COLLECTION_LOG_PATH}")

            if run_number < N_RUNS:
                print(f"[INFO] Sleeping {PAUSE_BETWEEN_RUNS_SECONDS} seconds before next run")
                time.sleep(PAUSE_BETWEEN_RUNS_SECONDS)

    finally:
        print("[INFO] Restoring original config.yaml")
        save_config(original_config)

    print("\n[INFO] Fresh OOS collection completed.")
    print(f"[INFO] Collection log saved to: {COLLECTION_LOG_PATH}")


if __name__ == "__main__":
    main()