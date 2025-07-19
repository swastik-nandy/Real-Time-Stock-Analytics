import asyncio
import websockets
import json
import logging
from datetime import datetime, timezone
from typing import Set
from pathlib import Path
import sys
import aioredis

# Add 'backend' to path so we can import from app.core
sys.path.append(str(Path(__file__).resolve().parents[1]))
from app.core.config import settings  # <-- centralized config

# -------- CONFIG --------
FINNHUB_API_KEY = settings.finnhub_api_key
REDIS_URL = settings.redis_url
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

SYMBOLS_KEY = "stock:symbols"
PRICE_PREFIX = "stock:price:"
TRADE_PREFIX = "stock:trade:"

# -------- Logging --------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocket-streamer")

MAX_CONCURRENT_WRITES = 100
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WRITES)

# -------- Helpers --------

async def get_symbols(redis) -> Set[str]:
    try:
        raw = await redis.smembers(SYMBOLS_KEY)
        return {s.decode() if isinstance(s, bytes) else s for s in raw}
    except Exception as e:
        logger.error(f"[Redis] Failed to fetch symbols: {e}")
        return set()

async def subscribe(ws, symbols: Set[str], subscribed: Set[str]):
    for symbol in symbols:
        try:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            subscribed.add(symbol)
            logger.info(f"[WS] Subscribed: {symbol}")
        except Exception as e:
            logger.warning(f"[WS] Failed to subscribe {symbol}: {e}")

async def unsubscribe(ws, symbols: Set[str], subscribed: Set[str]):
    for symbol in symbols:
        try:
            await ws.send(json.dumps({"type": "unsubscribe", "symbol": symbol}))
            subscribed.discard(symbol)
            logger.info(f"[WS] Unsubscribed: {symbol}")
        except Exception as e:
            logger.warning(f"[WS] Failed to unsubscribe {symbol}: {e}")

async def manage_subscriptions(ws, redis, subscribed: Set[str]):
    while True:
        try:
            current = await get_symbols(redis)
            to_add = current - subscribed
            to_remove = subscribed - current

            if to_add:
                await subscribe(ws, to_add, subscribed)
            if to_remove:
                await unsubscribe(ws, to_remove, subscribed)
        except Exception as e:
            logger.error(f"[Sub] Subscription update failed: {e}")

        await asyncio.sleep(30)

async def write_and_log(redis, symbol, price_key, price, trade_key, trade_info, exchange, asset):
    async with semaphore:
        try:
            await redis.set(price_key, price)
            await redis.setex(trade_key, 3600, json.dumps(trade_info))
            logger.info(f"{exchange} : {asset} - {price} - [Updated]")
        except Exception as e:
            logger.error(f"[Redis] Failed to update {symbol}: {e}")

async def handle_trade_data(redis, data: dict):
    if data.get("type") != "trade":
        return

    now = datetime.now(timezone.utc).isoformat()
    tasks = []

    for trade in data.get("data", []):
        symbol = trade.get("s")
        price = trade.get("p")
        timestamp = trade.get("t")
        volume = trade.get("v", 0)

        if symbol and price is not None:
            exchange, asset = symbol.split(":", 1)
            price_key = f"{PRICE_PREFIX}{symbol}"
            trade_key = f"{TRADE_PREFIX}{symbol}"
            trade_info = {
                "price": price,
                "timestamp": timestamp,
                "volume": volume,
                "updated_at": now
            }

            tasks.append(write_and_log(
                redis, symbol, price_key, price, trade_key, trade_info, exchange, asset
            ))

    if tasks:
        await asyncio.gather(*tasks)

# -------- Stream Loop --------

async def stream_loop():
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    delay = 3
    max_delay = 60

    while True:
        subscribed_symbols: Set[str] = set()
        try:
            async with websockets.connect(FINNHUB_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[WS] Connected to Finnhub")
                delay = 3  # reset backoff

                symbols = await get_symbols(redis)
                await subscribe(ws, symbols, subscribed_symbols)

                asyncio.create_task(manage_subscriptions(ws, redis, subscribed_symbols))

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        await handle_trade_data(redis, data)
                    except asyncio.TimeoutError:
                        logger.warning("[WS] Timeout — sending ping")
                        await ws.ping()
                    except websockets.ConnectionClosed:
                        logger.warning("[WS] Disconnected — reconnecting...")
                        break
                    except Exception as e:
                        logger.error(f"[WS] Message error: {e}")
        except Exception as e:
            logger.error(f"[WS] Connection error: {e}")

        logger.info(f"[WS] Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, max_delay)

if __name__ == "__main__":
    asyncio.run(stream_loop())
