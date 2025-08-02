import os
import asyncio
import json
import time as pytime
import logging
from datetime import datetime, time
from pathlib import Path

from redis.asyncio import Redis
import asyncpg
from dotenv import load_dotenv

# -------------------- LOGGING SETUP ---------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fetcher")

# -------------------- ENV SETUP ---------------------
if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
TRADE_KEY_PREFIX = "stock:trade:"
SYMBOL_SET_KEY = "stock:symbols"
FETCH_INTERVAL = 10  # seconds

# -------------------- FETCH + WRITE ---------------------
async def fetch_and_store(redis, pg_pool):
    try:
        symbols = await redis.smembers(SYMBOL_SET_KEY)
        symbols = {s.decode() if isinstance(s, bytes) else s for s in symbols}
        logger.info(f"Fetched {len(symbols)} symbols from Redis")
    except Exception as e:
        logger.error(f"Failed to fetch symbols from Redis: {e}")
        return

    pipe = redis.pipeline()
    for symbol in symbols:
        pipe.get(f"{TRADE_KEY_PREFIX}{symbol}")  # Pull full trade info
    results = await pipe.execute()

    rows = []
    for symbol, raw_trade in zip(symbols, results):
        if raw_trade is None:
            logger.debug(f"No trade data found in Redis for {symbol}, skipping.")
            continue
        try:
            trade_info = json.loads(raw_trade)
            price = float(trade_info["price"])
            timestamp_ms = trade_info["timestamp"]
            trade_time = datetime.utcfromtimestamp(timestamp_ms / 1000.0)

            async with pg_pool.acquire() as conn:
                stock_id = await conn.fetchval("SELECT id FROM stocks WHERE symbol = $1", symbol)
                if stock_id:
                    rows.append((stock_id, price, trade_time))
                else:
                    logger.warning(f"Stock symbol {symbol} not found in DB, skipping.")
        except Exception as e:
            logger.error(f"[ERROR] Problem with {symbol}: {e}")

    if rows:
        try:
            async with pg_pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO stock_price_history (stock_id, price, trade_time_stamp)
                    VALUES ($1, $2, $3)
                    """,
                    rows
                )
            logger.info(f"✅ Inserted {len(rows)} rows with real trade timestamps")
        except Exception as e:
            logger.error(f"[ERROR] Insert failed: {e}")
    else:
        logger.info("No rows to insert in this cycle.")

# -------------------- MAIN LOOP ---------------------
async def run_fetcher():
    logger.info(f"🚀 Fetcher launched at {datetime.utcnow().isoformat()}")

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))

    start_time = time(0, 32)
    end_time = time(23, 55)

    while True:
        now = datetime.utcnow()
        if now.time() < start_time:
            logger.debug("⏳ Waiting for fetch window to open...")
            await asyncio.sleep(1)
            continue
        if now.time() >= end_time:
            logger.info(f"🛑 Fetch window closed at {now.time()}")
            break

        start = pytime.time()
        await fetch_and_store(redis, pg_pool)
        elapsed = pytime.time() - start

        if elapsed < FETCH_INTERVAL:
            await asyncio.sleep(FETCH_INTERVAL - elapsed)
        else:
            logger.warning(f"⚠️ Took {elapsed:.2f}s — skipping dynamic sleep")
            await asyncio.sleep(FETCH_INTERVAL)

    await redis.close()
    await pg_pool.close()
    logger.info("Fetcher shutdown complete.")
