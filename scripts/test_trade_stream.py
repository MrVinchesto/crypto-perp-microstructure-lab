import asyncio
import json
import time

import websockets


SYMBOL = "btcusdt"
TRADE_STREAM = f"{SYMBOL}@aggTrade"
TRADE_WS_URL = (
    f"wss://fstream.binance.com/market/stream?streams={TRADE_STREAM}"
)

TEST_SECONDS = 20


async def main() -> None:
    print(f"[INFO] Connecting to: {TRADE_WS_URL}")
    print(f"[INFO] Listening for {TEST_SECONDS} seconds")

    started_at = time.time()
    trade_count = 0
    first_payload = None

    async with websockets.connect(TRADE_WS_URL, ping_interval=180, ping_timeout=600) as ws:
        while time.time() - started_at < TEST_SECONDS:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                print("[WARNING] No message received in the last 5 seconds")
                continue

            payload = json.loads(message)

            if "data" in payload:
                data = payload["data"]
            else:
                data = payload

            if data.get("e") == "aggTrade":
                trade_count += 1

                if first_payload is None:
                    first_payload = data
                    print("[INFO] First aggTrade payload:")
                    print(json.dumps(first_payload, indent=2))

    print(f"[INFO] Total aggTrade events received: {trade_count}")

    if trade_count == 0:
        raise RuntimeError("No aggTrade events received. Trade stream still not working.")

    print("[INFO] Trade stream sanity test passed.")


if __name__ == "__main__":
    asyncio.run(main())