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


async def collect_streams(config: dict, run_dir: Path) -> None:
    depth_stream = config["depth_stream"]
    trade_stream = config["trade_stream"]
    seconds = config["collection_seconds"]

    url = WS_BASE + f"{depth_stream}/{trade_stream}"

    depth_path = run_dir / "depth.jsonl"
    trades_path = run_dir / "trades.jsonl"

    # Буферы: сначала открываем stream и начинаем собирать события
    buffered_depth_events = []
    buffered_trade_events = []

    snapshot = None
    snapshot_received = False

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
                    # 1) сначала буферизуем depth updates
                    if not snapshot_received:
                        buffered_depth_events.append(data)

                        # как только получили первые depth events, берем snapshot
                        snapshot = fetch_snapshot(config["symbol"], config["snapshot_limit"])
                        save_json(run_dir / "snapshot.json", snapshot)

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

                        # записываем уже забуференные depth events
                        for event in buffered_depth_events:
                            depth_file.write(json.dumps(event, ensure_ascii=False) + "\n")

                        # записываем забуференные trades, если уже были
                        for event in buffered_trade_events:
                            trades_file.write(json.dumps(event, ensure_ascii=False) + "\n")

                        snapshot_received = True
                    else:
                        depth_file.write(json.dumps(data, ensure_ascii=False) + "\n")

                elif "aggTrade" in stream:
                    if not snapshot_received:
                        buffered_trade_events.append(data)
                    else:
                        trades_file.write(json.dumps(data, ensure_ascii=False) + "\n")


async def main():
    config = load_config()

    run_dir = make_run_dir(config["output_dir"], config["symbol"])
    print(f"[INFO] Run dir: {run_dir}")
    print("[INFO] Opening websocket, buffering events, then fetching snapshot...")

    await collect_streams(config, run_dir)

    print("[INFO] Done.")


if __name__ == "__main__":
    asyncio.run(main())