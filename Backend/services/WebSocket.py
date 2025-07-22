import asyncio
import websockets
import json
import logging
from datetime import datetime, timezone
from typing import Set
import os
import sys
from pathlib import Path
from redis.asyncio import Redis
from dotenv import load_dotenv

# ------------------ SETUP ------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocket-streamer")

sys.path.append(str(Path(__file__).resolve().parent))
if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
REDIS_URL = os.environ.get("REDIS_URL")
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

SYMBOLS_KEY = "stock:symbols"
PRICE_PREFIX = "stock:price:"
TRADE_PREFIX = "stock:trade:"

MAX_CONCURRENT_WRITES = 200  # Optional cap
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WRITES)

# ------------------ REDIS KEY UPDATE ------------------

async def write_trade_to_redis(redis: Redis, symbol: str, price: float, trade_info: dict):
    async with semaphore:
        try:
            price_key = f"{PRICE_PREFIX}{symbol}"
            trade_key = f"{TRADE_PREFIX}{symbol}"
            await redis.set(price_key, price)
            await redis.setex(trade_key, 3600, json.dumps(trade_info))
            logger.info(f"[Redis] {symbol} @ {price} - Updated")
        except Exception as e:
            logger.error(f"[Redis] Failed {symbol}: {e}")

# ------------------ TRADE HANDLER ------------------

async def handle_trade_data(redis: Redis, message: dict):
    if message.get("type") != "trade":
        return

    now = datetime.now(timezone.utc).isoformat()
    for trade in message.get("data", []):
        symbol = trade.get("s")
        price = trade.get("p")
        timestamp = trade.get("t")
        volume = trade.get("v", 0)

        if not symbol or price is None:
            continue

        trade_info = {
            "price": price,
            "timestamp": timestamp,
            "volume": volume,
            "updated_at": now
        }

        # Fire-and-forget: no await here; Redis write begins in parallel
        asyncio.create_task(write_trade_to_redis(redis, symbol, price, trade_info))

# ------------------ SUBSCRIPTION ------------------

async def subscribe_symbols(ws, symbols: Set[str]):
    for symbol in symbols:
        try:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"[WS] Subscribed: {symbol}")
        except Exception as e:
            logger.warning(f"[WS] Failed to subscribe {symbol}: {e}")

# ------------------ MAIN STREAM ------------------

async def get_symbols(redis: Redis) -> Set[str]:
    try:
        symbols = await redis.smembers(SYMBOLS_KEY)
        return {s.decode() if isinstance(s, bytes) else s for s in symbols}
    except Exception as e:
        logger.error(f"[Redis] Symbol fetch failed: {e}")
        return set()

async def stream_trades():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("[Redis] Connected")

    reconnect_delay = 3
    max_delay = 60

    while True:
        try:
            logger.info("[WS] Connecting to Finnhub...")
            async with websockets.connect(FINNHUB_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[WS] Connected ✅")
                reconnect_delay = 3  # reset backoff

                symbols = await get_symbols(redis)
                if not symbols:
                    logger.warning("[WS] No symbols found in Redis — retrying in 30s.")
                    await asyncio.sleep(30)
                    continue

                await subscribe_symbols(ws, symbols)

                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(msg)
                    asyncio.create_task(handle_trade_data(redis, data))

        except asyncio.TimeoutError:
            logger.warning("[WS] Timeout — sending ping...")
        except websockets.ConnectionClosed:
            logger.warning("[WS] Connection closed — reconnecting...")
        except Exception as e:
            logger.error(f"[WS] Error: {e}")

        logger.info(f"[WS] Reconnecting in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_delay)

# ------------------ BOOT ------------------

if __name__ == "__main__":
    try:
        logger.info("🚀 Starting ultra-low-latency WebSocket stream...")
        asyncio.run(stream_trades())
    except Exception as e:
        logger.exception(f"[FATAL] WebSocket crashed: {e}")
