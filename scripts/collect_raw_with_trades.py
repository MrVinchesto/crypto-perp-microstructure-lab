import asyncio
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import yaml
import websockets


CONFIG_PATH = Path("config.yaml")

BINANCE_REST_BASE = "https://fapi.binance.com"

DEPTH_PUBLIC_BASE = "wss://fstream.binance.com/public/stream?streams="
TRADE_MARKET_BASE = "wss://fstream.binance.com/market/stream?streams="


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_run_dir(output_dir: str, symbol: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / symbol / timestamp
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def fetch_depth_snapshot(symbol: str, limit: int) -> dict:
    url = f"{BINANCE_REST_BASE}/fapi/v1/depth"

    params = {
        "symbol": symbol,
        "limit": limit,
    }

    print(f"[INFO] Fetching snapshot from: {url}")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    snapshot = response.json()

    if "lastUpdateId" not in snapshot:
        raise ValueError("Snapshot does not contain lastUpdateId")

    print(f"[INFO] Snapshot lastUpdateId: {snapshot['lastUpdateId']}")

    return snapshot


async def collect_depth_events(
    ws_url: str,
    depth_events: list[dict],
    stop_time: float,
) -> None:
    print(f"[INFO] Connecting depth websocket: {ws_url}")

    async with websockets.connect(ws_url, ping_interval=180, ping_timeout=600) as ws:
        print("[INFO] Depth websocket connected")

        while time.time() < stop_time:
            timeout = max(0.1, stop_time - time.time())

            try:
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

            payload = json.loads(message)

            if "data" in payload:
                data = payload["data"]
            else:
                data = payload

            if data.get("e") == "depthUpdate":
                depth_events.append(data)

    print(f"[INFO] Depth collection finished. Events: {len(depth_events)}")


async def collect_trade_events(
    ws_url: str,
    trade_events: list[dict],
    stop_time: float,
) -> None:
    print(f"[INFO] Connecting trade websocket: {ws_url}")

    async with websockets.connect(ws_url, ping_interval=180, ping_timeout=600) as ws:
        print("[INFO] Trade websocket connected")

        while time.time() < stop_time:
            timeout = max(0.1, stop_time - time.time())

            try:
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

            payload = json.loads(message)

            if "data" in payload:
                data = payload["data"]
            else:
                data = payload

            if data.get("e") == "aggTrade":
                trade_events.append(data)

    print(f"[INFO] Trade collection finished. Events: {len(trade_events)}")


def write_json(path: Path, obj: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


async def collect_raw_with_trades() -> None:
    config = load_config()

    symbol = str(config["symbol"]).upper()
    symbol_lower = symbol.lower()

    snapshot_limit = int(config.get("snapshot_limit", 1000))
    collection_seconds = int(config.get("collection_seconds", 60))
    output_dir = str(config.get("output_dir", "data/raw"))

    depth_stream = str(config.get("depth_stream", f"{symbol_lower}@depth@100ms"))
    trade_stream = str(config.get("trade_stream", f"{symbol_lower}@aggTrade"))

    depth_ws_url = DEPTH_PUBLIC_BASE + depth_stream
    trade_ws_url = TRADE_MARKET_BASE + trade_stream

    run_dir = make_run_dir(output_dir=output_dir, symbol=symbol)

    print("=" * 80)
    print("[INFO] Starting raw collection with separate depth/trade websockets")
    print(f"[INFO] Symbol: {symbol}")
    print(f"[INFO] Collection seconds: {collection_seconds}")
    print(f"[INFO] Run dir: {run_dir}")
    print("=" * 80)

    depth_events: list[dict] = []
    trade_events: list[dict] = []

    started_at = time.time()
    stop_time = started_at + collection_seconds

    depth_task = asyncio.create_task(
        collect_depth_events(
            ws_url=depth_ws_url,
            depth_events=depth_events,
            stop_time=stop_time,
        )
    )

    trade_task = asyncio.create_task(
        collect_trade_events(
            ws_url=trade_ws_url,
            trade_events=trade_events,
            stop_time=stop_time,
        )
    )

    # Give websocket streams a short moment to start buffering events before snapshot.
    await asyncio.sleep(2)

    snapshot = fetch_depth_snapshot(
        symbol=symbol,
        limit=snapshot_limit,
    )

    await asyncio.gather(depth_task, trade_task)

    finished_at = time.time()

    snapshot_path = run_dir / "snapshot.json"
    depth_path = run_dir / "depth.jsonl"
    trades_path = run_dir / "trades.jsonl"
    meta_path = run_dir / "meta.json"

    write_json(snapshot_path, snapshot)
    write_jsonl(depth_path, depth_events)
    write_jsonl(trades_path, trade_events)

    collected_at_utc = utc_now_iso()

    meta = {
        "collector": "collect_raw_with_trades.py",
        "exchange": config.get("exchange", "binance_futures"),
        "symbol": symbol,
        "depth_stream": depth_stream,
        "trade_stream": trade_stream,
        "depth_ws_url": depth_ws_url,
        "trade_ws_url": trade_ws_url,
        "snapshot_limit": snapshot_limit,
        "collection_seconds": collection_seconds,
        "collected_at_utc": collected_at_utc,
        "started_at_utc": collected_at_utc,
        "wall_clock_started_at": started_at,
        "wall_clock_finished_at": finished_at,
        "depth_events": len(depth_events),
        "trade_events": len(trade_events),
        "snapshot_lastUpdateId": snapshot.get("lastUpdateId"),
    }

    write_json(meta_path, meta)

    print("\n[INFO] Raw collection completed")
    print(f"[INFO] Snapshot: {snapshot_path}")
    print(f"[INFO] Depth events: {len(depth_events)}")
    print(f"[INFO] Trade events: {len(trade_events)}")
    print(f"[INFO] Meta: {meta_path}")

    if len(depth_events) == 0:
        raise RuntimeError("No depth events collected.")

    if len(trade_events) == 0:
        raise RuntimeError("No trade events collected.")


def main() -> None:
    asyncio.run(collect_raw_with_trades())


if __name__ == "__main__":
    main()