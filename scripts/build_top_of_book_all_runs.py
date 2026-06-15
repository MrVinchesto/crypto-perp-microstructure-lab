import json
from pathlib import Path

import pandas as pd

from build_top_of_book import (
    build_top_of_book,
    load_depth_events,
    load_snapshot,
)


RAW_BASE = Path("data/raw/BTCUSDT")
PROCESSED_BASE = Path("data/processed")
REPORTS_TABLES_DIR = Path("reports/tables")

OUTPUT_PATH = PROCESSED_BASE / "top_of_book_all.csv"
SUMMARY_PATH = REPORTS_TABLES_DIR / "top_of_book_all_runs_summary.csv"


def load_meta(path: Path) -> dict:
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_run_dirs(raw_base: Path) -> list[Path]:
    if not raw_base.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_base}")

    run_dirs = sorted([p for p in raw_base.iterdir() if p.is_dir()])

    if not run_dirs:
        raise FileNotFoundError(f"No run directories found in: {raw_base}")

    return run_dirs


def process_single_run(run_dir: Path) -> tuple[pd.DataFrame | None, dict]:
    snapshot_path = run_dir / "snapshot.json"
    depth_path = run_dir / "depth.jsonl"
    meta_path = run_dir / "meta.json"

    summary = {
        "run_name": run_dir.name,
        "run_dir": str(run_dir),
        "status": "unknown",
        "n_depth_events_raw": None,
        "n_top_of_book_rows": 0,
        "snapshot_lastUpdateId": None,
        "collected_at_utc": None,
        "error": None,
    }

    try:
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Missing snapshot file: {snapshot_path}")

        if not depth_path.exists():
            raise FileNotFoundError(f"Missing depth file: {depth_path}")

        snapshot = load_snapshot(snapshot_path)
        depth_events = load_depth_events(depth_path)
        meta = load_meta(meta_path)

        summary["n_depth_events_raw"] = len(depth_events)
        summary["snapshot_lastUpdateId"] = snapshot.get("lastUpdateId")
        summary["collected_at_utc"] = meta.get("collected_at_utc")

        if len(depth_events) == 0:
            raise ValueError("Depth file contains zero events")

        df = build_top_of_book(snapshot=snapshot, depth_events=depth_events)

        df.insert(0, "run_name", run_dir.name)
        df.insert(1, "row_in_run", range(len(df)))
        df.insert(2, "collected_at_utc", meta.get("collected_at_utc"))
        df["source_run_dir"] = str(run_dir)

        summary["status"] = "success"
        summary["n_top_of_book_rows"] = len(df)

        return df, summary

    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)

        return None, summary


def main() -> None:
    PROCESSED_BASE.mkdir(parents=True, exist_ok=True)
    REPORTS_TABLES_DIR.mkdir(parents=True, exist_ok=True)

    run_dirs = get_run_dirs(RAW_BASE)

    print(f"[INFO] Found {len(run_dirs)} raw runs")

    frames = []
    summaries = []

    for i, run_dir in enumerate(run_dirs, start=1):
        print("\n" + "=" * 80)
        print(f"[INFO] Processing run {i}/{len(run_dirs)}: {run_dir.name}")
        print("=" * 80)

        df_run, summary = process_single_run(run_dir)
        summaries.append(summary)

        if df_run is not None:
            frames.append(df_run)
            print(f"[INFO] Success: {len(df_run)} top-of-book rows")
        else:
            print(f"[WARNING] Failed: {summary['error']}")

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(SUMMARY_PATH, index=False)

    print(f"\n[INFO] Saved run summary to: {SUMMARY_PATH}")

    if not frames:
        raise ValueError("No runs were processed successfully")

    combined = pd.concat(frames, ignore_index=True)
    combined.insert(0, "global_row_id", range(len(combined)))

    combined.to_csv(OUTPUT_PATH, index=False)

    print(f"[INFO] Saved combined top-of-book to: {OUTPUT_PATH}")
    print(f"[INFO] Total rows: {len(combined)}")
    print(f"[INFO] Successful runs: {(summary_df['status'] == 'success').sum()}")
    print(f"[INFO] Failed runs: {(summary_df['status'] == 'failed').sum()}")

    print("\n[INFO] Combined dataset preview:")
    print(combined.head())

    print("\n[INFO] Run summary:")
    print(summary_df)


if __name__ == "__main__":
    main()