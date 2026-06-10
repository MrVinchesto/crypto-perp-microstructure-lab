import json
from pathlib import Path

import pandas as pd


RAW_BASE = Path("data/raw/BTCUSDT")
REPORTS_TABLES_DIR = Path("reports/tables")
OUTPUT_PATH = REPORTS_TABLES_DIR / "raw_runs_inventory.csv"


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0

    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def load_meta(path: Path) -> dict:
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def inspect_run(run_dir: Path) -> dict:
    snapshot_path = run_dir / "snapshot.json"
    depth_path = run_dir / "depth.jsonl"
    trades_path = run_dir / "trades.jsonl"
    meta_path = run_dir / "meta.json"

    meta = load_meta(meta_path)

    return {
        "run_dir": str(run_dir),
        "run_name": run_dir.name,
        "has_snapshot": snapshot_path.exists(),
        "has_depth": depth_path.exists(),
        "has_trades": trades_path.exists(),
        "has_meta": meta_path.exists(),
        "depth_events": count_jsonl_lines(depth_path),
        "trade_events": count_jsonl_lines(trades_path),
        "symbol": meta.get("symbol"),
        "collection_seconds": meta.get("collection_seconds"),
        "snapshot_lastUpdateId": meta.get("snapshot_lastUpdateId"),
        "collected_at_utc": meta.get("collected_at_utc"),
    }


def main() -> None:
    if not RAW_BASE.exists():
        raise FileNotFoundError(f"Raw data directory not found: {RAW_BASE}")

    run_dirs = sorted([p for p in RAW_BASE.iterdir() if p.is_dir()])

    if not run_dirs:
        raise FileNotFoundError(f"No run directories found in: {RAW_BASE}")

    rows = [inspect_run(run_dir) for run_dir in run_dirs]

    inventory = pd.DataFrame(rows)

    REPORTS_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    inventory.to_csv(OUTPUT_PATH, index=False)

    print(f"[INFO] Found {len(inventory)} raw runs")
    print(f"[INFO] Saved raw runs inventory to: {OUTPUT_PATH}")

    print("\n[INFO] Last 10 runs:")
    print(inventory.tail(10))

    print("\n[INFO] Basic event count summary:")
    print(inventory[["depth_events", "trade_events"]].describe())


if __name__ == "__main__":
    main()