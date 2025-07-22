import os
import asyncio
import json
import time as pytime
from datetime import datetime, time
from pathlib import Path

from redis.asyncio import Redis
import asyncpg
from dotenv import load_dotenv

# -------------------- ENV SETUP ---------------------
if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
PRICE_KEY_PREFIX = "stock:price:"
SYMBOL_SET_KEY = "stock:symbols"
FETCH_INTERVAL = 10  # seconds

# -------------------- FETCH + WRITE ---------------------
async def fetch_and_store(redis, pg_pool):
    now = datetime.utcnow()
    symbols = await redis.smembers(SYMBOL_SET_KEY)
    symbols = {s.decode() if isinstance(s, bytes) else s for s in symbols}

    pipe = redis.pipeline()
    for symbol in symbols:
        pipe.get(f"{PRICE_KEY_PREFIX}{symbol}")
    results = await pipe.execute()

    rows = []
    for symbol, raw_price in zip(symbols, results):
        if raw_price is None:
            continue
        try:
            price = float(raw_price)
            async with pg_pool.acquire() as conn:
                stock_id = await conn.fetchval("SELECT id FROM stocks WHERE symbol = $1", symbol)
                if stock_id:
                    rows.append((stock_id, price, now))
        except Exception as e:
            print(f"[ERROR] Problem with {symbol}: {e}")

    if rows:
        try:
            async with pg_pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO stock_price_history (stock_id, price, last_updated)
                    VALUES ($1, $2, $3)
                    """,
                    rows
                )
            print(f"[✅] Inserted {len(rows)} rows at {now.isoformat()}")
        except Exception as e:
            print(f"[ERROR] Insert failed: {e}")

# -------------------- MAIN LOOP ---------------------
async def run_fetcher():
    print("🚀 Fetcher launched at", datetime.utcnow().isoformat())

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))

    start_time = time(13, 0)
    end_time = time(21, 0)

    while True:
        now = datetime.utcnow()
        if now.time() < start_time:
            await asyncio.sleep(1)
            continue
        if now.time() >= end_time:
            print(f"🛑 Fetch window closed at {now.time()}")
            break

        start = pytime.time()
        await fetch_and_store(redis, pg_pool)
        elapsed = pytime.time() - start

        if elapsed < FETCH_INTERVAL:
            await asyncio.sleep(FETCH_INTERVAL - elapsed)
        else:
            print(f"⚠️ Took {elapsed:.2f}s — skipping dynamic sleep")
            await asyncio.sleep(FETCH_INTERVAL)

    await redis.close()
    await pg_pool.close()
