import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


CONFIG_PATH = Path("config.yaml")
RAW_ROOT = Path("data/raw/BTCUSDT")
TABLES_DIR = Path("reports/tables")
LOG_PATH = TABLES_DIR / "fresh_trade_collection_log.csv"

COLLECTION_BATCH = "weekday_active_final_holdout_day44"

SYMBOL = "BTCUSDT"
SYMBOL_LOWER = SYMBOL.lower()

# Main plan:
# 36 runs * 300 seconds = approximately 3 hours of raw market data.
NUM_RUNS = 36
COLLECTION_SECONDS = 300
PAUSE_BETWEEN_RUNS_SECONDS = 5

COLLECTOR_SCRIPT = Path("scripts/collect_raw_with_trades.py")

DEPTH_STREAM = f"{SYMBOL_LOWER}@depth@100ms"
TRADE_STREAM = f"{SYMBOL_LOWER}@aggTrade"


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


def backup_config() -> Path:
    backup_path = CONFIG_PATH.with_suffix(".yaml.day25_backup")
    shutil.copy2(CONFIG_PATH, backup_path)
    print(f"[INFO] Backed up config to: {backup_path}")
    return backup_path


def restore_config(backup_path: Path) -> None:
    if backup_path.exists():
        shutil.copy2(backup_path, CONFIG_PATH)
        backup_path.unlink()
        print("[INFO] Restored original config.yaml")
    else:
        print("[WARNING] Backup config not found. config.yaml was not restored.")


def prepare_config_for_collection() -> None:
    config = load_config()

    config["symbol"] = SYMBOL
    config["depth_stream"] = DEPTH_STREAM
    config["trade_stream"] = TRADE_STREAM
    config["collection_seconds"] = COLLECTION_SECONDS
    config["output_dir"] = "data/raw"

    save_config(config)

    print("[INFO] Updated config.yaml for batch collection")
    print(f"[INFO] symbol: {SYMBOL}")
    print(f"[INFO] depth_stream: {DEPTH_STREAM}")
    print(f"[INFO] trade_stream: {TRADE_STREAM}")
    print(f"[INFO] collection_seconds: {COLLECTION_SECONDS}")


def list_run_dirs() -> set[str]:
    if not RAW_ROOT.exists():
        return set()

    return {
        path.name
        for path in RAW_ROOT.iterdir()
        if path.is_dir()
    }


def read_meta(run_name: str) -> dict:
    meta_path = RAW_ROOT / run_name / "meta.json"

    if not meta_path.exists():
        return {}

    with meta_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def find_new_run(before_runs: set[str], after_runs: set[str]) -> str | None:
    new_runs = sorted(after_runs - before_runs)

    if not new_runs:
        return None

    if len(new_runs) == 1:
        return new_runs[0]

    # If more than one run appeared, choose the newest by directory modification time.
    new_run_paths = [RAW_ROOT / run_name for run_name in new_runs]
    newest = max(new_run_paths, key=lambda path: path.stat().st_mtime)

    return newest.name


def run_one_collection(run_number: int) -> dict:
    print("\n" + "=" * 80)
    print(f"[INFO] Starting run {run_number}/{NUM_RUNS}")
    print("=" * 80)

    before_runs = list_run_dirs()
    started_at = utc_now_iso()

    command = [
        sys.executable,
        str(COLLECTOR_SCRIPT),
    ]

    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )

        status = "success"
        error_message = ""
        stdout_tail = completed.stdout[-2000:]
        stderr_tail = completed.stderr[-2000:]

    except subprocess.CalledProcessError as exc:
        status = "failed"
        error_message = str(exc)
        stdout_tail = exc.stdout[-2000:] if exc.stdout else ""
        stderr_tail = exc.stderr[-2000:] if exc.stderr else ""

    finished_at = utc_now_iso()
    after_runs = list_run_dirs()
    run_name = find_new_run(before_runs, after_runs)

    meta = read_meta(run_name) if run_name else {}

    depth_events = meta.get("depth_events")
    trade_events = meta.get("trade_events")
    collection_seconds = meta.get("collection_seconds")
    snapshot_last_update_id = meta.get("snapshot_lastUpdateId")

    row = {
        "collection_batch": COLLECTION_BATCH,
        "run_number": run_number,
        "status": status,
        "run_name": run_name,
        "symbol": SYMBOL,
        "collection_seconds_requested": COLLECTION_SECONDS,
        "collection_seconds_meta": collection_seconds,
        "depth_events": depth_events,
        "trade_events": trade_events,
        "snapshot_lastUpdateId": snapshot_last_update_id,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "error_message": error_message,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }

    print(f"[INFO] Run {run_number} status: {status}")
    print(f"[INFO] Run name: {run_name}")
    print(f"[INFO] Depth events: {depth_events}")
    print(f"[INFO] Trade events: {trade_events}")

    if status != "success":
        print("[WARNING] Collector failed. stderr tail:")
        print(stderr_tail)

    return row


def append_log(rows: list[dict]) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    new_log = pd.DataFrame(rows)

    if LOG_PATH.exists():
        old_log = pd.read_csv(LOG_PATH)
        combined = pd.concat([old_log, new_log], ignore_index=True)
    else:
        combined = new_log

    combined.to_csv(LOG_PATH, index=False)
    print(f"[INFO] Saved collection log to: {LOG_PATH}")


def print_final_summary(rows: list[dict]) -> None:
    df = pd.DataFrame(rows)

    print("\n" + "=" * 80)
    print("[INFO] Batch collection summary")
    print("=" * 80)

    if df.empty:
        print("[WARNING] No rows collected.")
        return

    print(df[
        [
            "run_number",
            "status",
            "run_name",
            "depth_events",
            "trade_events",
            "collection_seconds_requested",
        ]
    ])

    success_count = int((df["status"] == "success").sum())
    failed_count = int((df["status"] != "success").sum())

    total_depth = pd.to_numeric(df["depth_events"], errors="coerce").fillna(0).sum()
    total_trades = pd.to_numeric(df["trade_events"], errors="coerce").fillna(0).sum()

    print(f"\n[INFO] Success count: {success_count}")
    print(f"[INFO] Failed count: {failed_count}")
    print(f"[INFO] Total depth events: {int(total_depth)}")
    print(f"[INFO] Total trade events: {int(total_trades)}")

    if success_count > 0:
        avg_depth = total_depth / success_count
        avg_trades = total_trades / success_count
        print(f"[INFO] Average depth events per successful run: {avg_depth:.2f}")
        print(f"[INFO] Average trade events per successful run: {avg_trades:.2f}")


def main() -> None:
    if not COLLECTOR_SCRIPT.exists():
        raise FileNotFoundError(f"Collector script not found: {COLLECTOR_SCRIPT}")

    print("=" * 80)
    print("[INFO] Starting multi-run collection with trades")
    print(f"[INFO] Batch: {COLLECTION_BATCH}")
    print(f"[INFO] Runs: {NUM_RUNS}")
    print(f"[INFO] Seconds per run: {COLLECTION_SECONDS}")
    print(f"[INFO] Pause between runs: {PAUSE_BETWEEN_RUNS_SECONDS}")
    print("=" * 80)

    backup_path = backup_config()
    rows = []

    try:
        prepare_config_for_collection()

        for run_number in range(1, NUM_RUNS + 1):
            row = run_one_collection(run_number)
            rows.append(row)
            append_log([row])

            if run_number < NUM_RUNS:
                print(f"[INFO] Pausing {PAUSE_BETWEEN_RUNS_SECONDS} seconds before next run...")
                time.sleep(PAUSE_BETWEEN_RUNS_SECONDS)

    finally:
        restore_config(backup_path)

    print_final_summary(rows)

    print("\n[INFO] Multi-run trade collection completed.")


if __name__ == "__main__":
    main()