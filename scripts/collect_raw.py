import asyncio
import json
import time
from pathlib import Path

import requests
import websockets
import yaml


REST_BASE = "https://fapi.binance.com"
WS_BASE = "wss://fstream.binance.com/stream?streams="


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_run_dir(output_dir: str, symbol: str) -> Path:
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = Path(output_dir) / symbol / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_json(path: Path, obj: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def fetch_snapshot(symbol: str, limit: int) -> dict:
    url = f"{REST_BASE}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": limit}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


async def collect_streams(depth_stream: str, trade_stream: str, run_dir: Path, seconds: int) -> None:
    url = WS_BASE + f"{depth_stream}/{trade_stream}"

    depth_path = run_dir / "depth.jsonl"
    trades_path = run_dir / "trades.jsonl"

    start = time.time()

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        with open(depth_path, "a", encoding="utf-8") as depth_file, \
             open(trades_path, "a", encoding="utf-8") as trades_file:

            while time.time() - start < seconds:
                raw_msg = await ws.recv()
                msg = json.loads(raw_msg)

                stream = msg.get("stream")
                data = msg.get("data", {})

                if stream is None:
                    continue

                if "depth" in stream:
                    depth_file.write(json.dumps(data, ensure_ascii=False) + "\n")
                elif "aggTrade" in stream:
                    trades_file.write(json.dumps(data, ensure_ascii=False) + "\n")


def save_meta(run_dir: Path, config: dict, snapshot: dict) -> None:
    meta = {
        "symbol": config["symbol"],
        "depth_stream": config["depth_stream"],
        "trade_stream": config["trade_stream"],
        "snapshot_limit": config["snapshot_limit"],
        "collection_seconds": config["collection_seconds"],
        "snapshot_lastUpdateId": snapshot.get("lastUpdateId"),
        "collected_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    save_json(run_dir / "meta.json", meta)


async def main():
    config = load_config()

    run_dir = make_run_dir(config["output_dir"], config["symbol"])
    print(f"[INFO] Run dir: {run_dir}")

    print("[INFO] Fetching snapshot...")
    snapshot = fetch_snapshot(config["symbol"], config["snapshot_limit"])
    save_json(run_dir / "snapshot.json", snapshot)

    save_meta(run_dir, config, snapshot)

    print("[INFO] Collecting websocket data...")
    await collect_streams(
        depth_stream=config["depth_stream"],
        trade_stream=config["trade_stream"],
        run_dir=run_dir,
        seconds=config["collection_seconds"],
    )

    print("[INFO] Done.")


if __name__ == "__main__":
    asyncio.run(main())