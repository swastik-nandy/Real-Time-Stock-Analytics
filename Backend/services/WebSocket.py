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
import psutil

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

# ------------------ REDIS QUEUE ------------------

WRITE_QUEUE = asyncio.Queue(maxsize=1000)
MICRO_BATCH_SIZE = 50
FLUSH_INTERVAL = 0.01  # 10ms max wait before forcing a flush

# ------------------ REDIS WRITER ------------------

async def redis_writer(redis: Redis):
    while True:
        batch = []

        try:
            # Wait for first item
            item = await WRITE_QUEUE.get()
            batch.append(item)

            # Try to get more items up to batch size without blocking
            for _ in range(MICRO_BATCH_SIZE - 1):
                item = WRITE_QUEUE.get_nowait()
                batch.append(item)

        except asyncio.QueueEmpty:
            pass

        if not batch:
            continue

        try:
            pipe = redis.pipeline()
            for symbol, price, trade_info in batch:
                pipe.set(f"{PRICE_PREFIX}{symbol}", price)
                pipe.setex(f"{TRADE_PREFIX}{symbol}", 3600, json.dumps(trade_info))
            await pipe.execute()
        except Exception as e:
            logger.error(f"[Redis] Pipeline write failed: {e}")
        finally:
            for _ in batch:
                WRITE_QUEUE.task_done()

        # Tiny sleep if underloaded to avoid tight CPU loop
        await asyncio.sleep(FLUSH_INTERVAL if len(batch) < MICRO_BATCH_SIZE else 0)

# ------------------ TRADE HANDLER ------------------

async def handle_trade_data(message: dict):
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

        try:
            WRITE_QUEUE.put_nowait((symbol, price, trade_info))
        except asyncio.QueueFull:
            logger.warning(f"[Queue] Full. Skipped trade for {symbol}")

# ------------------ SYMBOL UTILS ------------------

async def get_symbols(redis: Redis) -> Set[str]:
    try:
        symbols = await redis.smembers(SYMBOLS_KEY)
        return {s.decode() if isinstance(s, bytes) else s for s in symbols}
    except Exception as e:
        logger.error(f"[Redis] Symbol fetch failed: {e}")
        return set()

async def subscribe_symbols(ws, symbols: Set[str]):
    for symbol in symbols:
        try:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        except Exception as e:
            logger.warning(f"[WS] Failed to subscribe {symbol}: {e}")

# ------------------ MEMORY MONITOR ------------------

async def memory_monitor():
    while True:
        mem = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
        logger.info(f"[MEMORY] RAM: {mem:.2f} MB | Queue: {WRITE_QUEUE.qsize()}")
        await asyncio.sleep(1800)

# ------------------ MAIN LOOP ------------------

async def stream_trades():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("[Redis] Connected ✅")

    asyncio.create_task(redis_writer(redis))
    asyncio.create_task(memory_monitor())

    reconnect_delay = 3
    max_delay = 60

    while True:
        try:
            logger.info("[WS] Connecting to Finnhub...")
            async with websockets.connect(FINNHUB_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[WS] Connected ✅")
                reconnect_delay = 3

                symbols = await get_symbols(redis)
                if not symbols:
                    logger.warning("[WS] No symbols found — retrying in 30s")
                    await asyncio.sleep(30)
                    continue

                await subscribe_symbols(ws, symbols)

                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(msg)
                    await handle_trade_data(data)

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
        logger.info("🚀 Starting micro-batch WebSocket streamer...")
        asyncio.run(stream_trades())
    except Exception as e:
        logger.exception(f"[FATAL] WebSocket crashed: {e}")
