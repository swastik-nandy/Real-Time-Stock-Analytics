import asyncio
import websockets
import json
import logging
from datetime import datetime, timezone, time
from typing import Set
import os
import sys
from pathlib import Path

from redis.asyncio import Redis
from dotenv import load_dotenv

# ------------------ BOOT CONFIRMATION ------------------

print("✅ WebSocket.py launched successfully")  # visible in container logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocket-streamer")
logger.info("[BOOT] ✅ WebSocket.py launched successfully")

# ------------------ PATH & ENV ------------------

sys.path.append(str(Path(__file__).resolve().parents[1]))  # Add project root to PYTHONPATH

if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
REDIS_URL = os.environ.get("REDIS_URL")
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

SYMBOLS_KEY = "stock:symbols"
PRICE_PREFIX = "stock:price:"
TRADE_PREFIX = "stock:trade:"

# ------------------ ENV LOGGING ------------------

logger.info(f"[ENV] FINNHUB_API_KEY is {'SET' if FINNHUB_API_KEY else 'MISSING'}")
logger.info(f"[ENV] REDIS_URL is {REDIS_URL or 'MISSING'}")

MAX_CONCURRENT_WRITES = 100
semaphore = asyncio.Semaphore(MAX_CONCURRENT_WRITES)

# ------------------ FETCHER IMPORT ------------------

from fetcher import run_fetcher

# ------------------ FLAG ------------------

fetched_today = False

# ------------------ HELPERS ------------------

async def get_symbols(redis) -> Set[str]:
    try:
        raw = await redis.smembers(SYMBOLS_KEY)
        symbols = {s.decode() if isinstance(s, bytes) else s for s in raw}
        logger.info(f"[Redis] Loaded {len(symbols)} symbols")
        return symbols
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
            logger.debug("[Sub] Checking for subscription updates...")
            current = await get_symbols(redis)
            to_add = current - subscribed
            to_remove = subscribed - current

            if to_add:
                logger.info(f"[Sub] New symbols to subscribe: {to_add}")
                await subscribe(ws, to_add, subscribed)
            if to_remove:
                logger.info(f"[Sub] Symbols to unsubscribe: {to_remove}")
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

# ------------------ STREAM LOOP ------------------

async def stream_loop():
    global fetched_today

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("[Redis] Connected successfully")

    delay = 3
    max_delay = 60

    while True:
        logger.info("[WS] Beginning main loop...")
        subscribed_symbols: Set[str] = set()

        try:
            async with websockets.connect(FINNHUB_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[WS] Connected to Finnhub")
                delay = 3  # RESET BACKOFF

                symbols = await get_symbols(redis)
                if not symbols:
                    logger.warning("[WS] No symbols found in Redis. Sleeping 30s and retrying loop.")
                    await asyncio.sleep(30)
                    continue

                await subscribe(ws, symbols, subscribed_symbols)
                asyncio.create_task(manage_subscriptions(ws, redis, subscribed_symbols))

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        await handle_trade_data(redis, data)

                        now = datetime.utcnow()
                        current_time = now.time()

                        if current_time == time(0, 0) and fetched_today:
                            fetched_today = False
                            logger.info("[🌙] Midnight reset — fetcher flag cleared")

                        if not fetched_today and time(13, 0) <= current_time < time(21, 0):
                            logger.info("[⏱️] 13:00 UTC reached — triggering fetcher")
                            asyncio.create_task(run_fetcher())
                            fetched_today = True

                    except asyncio.TimeoutError:
                        logger.warning("[WS] Timeout — sending ping")
                        await ws.ping()
                    except websockets.ConnectionClosed:
                        logger.warning("[WS] Disconnected — reconnecting...")
                        break
                    except Exception as e:
                        logger.error(f"[WS] Message error: {e}")

        except Exception as e:
            logger.error(f"[WS] Connection error (outer loop): {e}")

        logger.info(f"[WS] Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, max_delay)

# ------------------ ENTRY ------------------

async def main():
    while True:
        try:
            await stream_loop()
        except Exception as e:
            logger.exception(f"[MAIN LOOP] stream_loop crashed unexpectedly: {e}")
        logger.info("[MAIN LOOP] stream_loop exited — retrying in 10s")
        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        logger.info("[MAIN] Starting WebSocket service...")
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"[FATAL] WebSocket crashed at top-level: {e}")
