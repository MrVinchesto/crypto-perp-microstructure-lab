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


def get_best_bid_ask_with_qty(bids: dict, asks: dict):
    if not bids or not asks:
        return None, None, None, None

    best_bid = max(bids.keys())
    best_ask = min(asks.keys())

    best_bid_qty = bids[best_bid]
    best_ask_qty = asks[best_ask]

    return best_bid, best_ask, best_bid_qty, best_ask_qty


def top_n_depth(book: dict, side: str, n: int) -> Decimal:
    if side not in {"bid", "ask"}:
        raise ValueError("side must be either 'bid' or 'ask'")

    if not book:
        return Decimal("0")

    if side == "bid":
        prices = sorted(book.keys(), reverse=True)
    else:
        prices = sorted(book.keys())

    top_prices = prices[:n]
    return sum(book[p] for p in top_prices)


def safe_imbalance(bid_qty: Decimal, ask_qty: Decimal) -> Decimal | None:
    denom = bid_qty + ask_qty
    if denom == 0:
        return None
    return (bid_qty - ask_qty) / denom


def safe_microprice(
    best_bid: Decimal,
    best_ask: Decimal,
    best_bid_qty: Decimal,
    best_ask_qty: Decimal,
) -> Decimal | None:
    denom = best_bid_qty + best_ask_qty
    if denom == 0:
        return None

    return (best_ask * best_bid_qty + best_bid * best_ask_qty) / denom


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

        best_bid, best_ask, best_bid_qty, best_ask_qty = get_best_bid_ask_with_qty(bids, asks)

        if best_bid is None or best_ask is None:
            prev_u = event["u"]
            continue

        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / Decimal("2")

        bid_depth_5 = top_n_depth(bids, side="bid", n=5)
        ask_depth_5 = top_n_depth(asks, side="ask", n=5)

        bid_depth_10 = top_n_depth(bids, side="bid", n=10)
        ask_depth_10 = top_n_depth(asks, side="ask", n=10)

        imbalance_1 = safe_imbalance(best_bid_qty, best_ask_qty)
        imbalance_5 = safe_imbalance(bid_depth_5, ask_depth_5)
        imbalance_10 = safe_imbalance(bid_depth_10, ask_depth_10)

        microprice = safe_microprice(
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_qty=best_bid_qty,
            best_ask_qty=best_ask_qty,
        )

        rows.append(
            {
                "event_time": event["E"],
                "transaction_time": event["T"],
                "first_update_id": event["U"],
                "final_update_id": event["u"],
                "prev_final_update_id": event.get("pu"),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
                "best_bid_qty": float(best_bid_qty),
                "best_ask_qty": float(best_ask_qty),
                "spread": float(spread),
                "mid_price": float(mid),
                "microprice": float(microprice) if microprice is not None else None,
                "bid_depth_5": float(bid_depth_5),
                "ask_depth_5": float(ask_depth_5),
                "bid_depth_10": float(bid_depth_10),
                "ask_depth_10": float(ask_depth_10),
                "imbalance_1": float(imbalance_1) if imbalance_1 is not None else None,
                "imbalance_5": float(imbalance_5) if imbalance_5 is not None else None,
                "imbalance_10": float(imbalance_10) if imbalance_10 is not None else None,
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

    print(f"[INFO] Saved enriched top-of-book to: {out_path}")

    print("\n[INFO] First rows:")
    print(df.head())

    print("\n[INFO] Summary:")
    cols = [
        "best_bid",
        "best_ask",
        "spread",
        "mid_price",
        "microprice",
        "best_bid_qty",
        "best_ask_qty",
        "bid_depth_5",
        "ask_depth_5",
        "imbalance_1",
        "imbalance_5",
        "imbalance_10",
    ]
    print(df[cols].describe())

    print("\n[INFO] Imbalance signs:")
    print((df["imbalance_1"] > 0).value_counts())


if __name__ == "__main__":
    main()