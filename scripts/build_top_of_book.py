import json
from decimal import Decimal
from pathlib import Path

import pandas as pd


RAW_BASE = Path("data/raw/BTCUSDT")
PROCESSED_BASE = Path("data/processed")


def get_latest_run_dir(base: Path) -> Path:
    run_dirs = [p for p in base.iterdir() if p.is_dir()]
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found in {base}")
    return sorted(run_dirs)[-1]


def load_snapshot(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_depth_events(path: Path) -> list[dict]:
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def snapshot_to_books(snapshot: dict):
    bids = {}
    asks = {}

    for price, qty in snapshot["bids"]:
        p = Decimal(price)
        q = Decimal(qty)
        if q != 0:
            bids[p] = q

    for price, qty in snapshot["asks"]:
        p = Decimal(price)
        q = Decimal(qty)
        if q != 0:
            asks[p] = q

    return bids, asks


def apply_side_updates(side_book: dict, updates: list[list[str]]):
    for price, qty in updates:
        p = Decimal(price)
        q = Decimal(qty)

        if q == 0:
            side_book.pop(p, None)
        else:
            side_book[p] = q


def get_best_bid_ask(bids: dict, asks: dict):
    if not bids or not asks:
        return None, None

    best_bid = max(bids.keys())
    best_ask = min(asks.keys())
    return best_bid, best_ask


def build_top_of_book(snapshot: dict, depth_events: list[dict]) -> pd.DataFrame:
    last_update_id = snapshot["lastUpdateId"]

    # 1. Drop stale events
    filtered = [e for e in depth_events if e["u"] >= last_update_id]

    if not filtered:
        raise ValueError("No usable depth events after filtering by snapshot lastUpdateId")

    # 2. Find first event satisfying Binance start condition:
    # U <= lastUpdateId <= u
    start_idx = None
    for i, event in enumerate(filtered):
        if event["U"] <= last_update_id <= event["u"]:
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("Could not find first valid event satisfying U <= lastUpdateId <= u")

    usable_events = filtered[start_idx:]

    bids, asks = snapshot_to_books(snapshot)

    rows = []
    prev_u = None

    for i, event in enumerate(usable_events):
        # 3. Continuity check after first event
        if prev_u is not None:
            if event.get("pu") != prev_u:
                raise ValueError(
                    f"Sequence broken at event index {i}: pu={event.get('pu')} != previous u={prev_u}"
                )

        apply_side_updates(bids, event["b"])
        apply_side_updates(asks, event["a"])

        best_bid, best_ask = get_best_bid_ask(bids, asks)
        if best_bid is None or best_ask is None:
            prev_u = event["u"]
            continue

        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / Decimal("2")

        rows.append(
            {
                "event_time": event["E"],
                "transaction_time": event["T"],
                "first_update_id": event["U"],
                "final_update_id": event["u"],
                "prev_final_update_id": event.get("pu"),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
                "spread": float(spread),
                "mid_price": float(mid),
            }
        )

        prev_u = event["u"]

    if not rows:
        raise ValueError("No top-of-book rows were produced")

    return pd.DataFrame(rows)


def main():
    latest_run = get_latest_run_dir(RAW_BASE)
    print(f"[INFO] Using raw data from: {latest_run}")

    snapshot = load_snapshot(latest_run / "snapshot.json")
    depth_events = load_depth_events(latest_run / "depth.jsonl")

    print(f"[INFO] Loaded snapshot lastUpdateId: {snapshot['lastUpdateId']}")
    print(f"[INFO] Loaded {len(depth_events)} depth events")

    df = build_top_of_book(snapshot, depth_events)

    PROCESSED_BASE.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_BASE / "top_of_book.csv"
    df.to_csv(out_path, index=False)

    print(f"[INFO] Saved top-of-book to: {out_path}")
    print("[INFO] First rows:")
    print(df.head())
    print("[INFO] Summary:")
    print(df[["best_bid", "best_ask", "spread", "mid_price"]].describe())


if __name__ == "__main__":
    main()