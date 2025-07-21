import os
import asyncio
import json
import time as pytime
from datetime import datetime, time
from pathlib import Path

import aioredis
import asyncpg
from dotenv import load_dotenv

# ----------------------------------------- ENV SETUP -------------------------------------------------

if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

PRICE_KEY_PREFIX = "stock:price:"
FETCH_INTERVAL = 10  # seconds

# ------------------------------------------- UTILS ----------------------------------------------------

async def load_symbols(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, symbol FROM stocks")
        return {row["symbol"]: row["id"] for row in rows}

async def fetch_and_store(redis, pg_pool, symbol_to_id):
    now = datetime.utcnow()
    pipe = redis.pipeline()
    for symbol in symbol_to_id:
        pipe.get(f"{PRICE_KEY_PREFIX}{symbol}")
    results = await pipe.execute()

    rows = []
    for symbol, raw_price in zip(symbol_to_id.keys(), results):
        if raw_price is None:
            continue
        try:
            price = float(raw_price)
            stock_id = symbol_to_id[symbol] 
            rows.append((stock_id, price, now))
        except Exception as e:
            print(f"[ERROR] Parse failed for {symbol}: {e}")

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

# ------------------------------------ SINGLE LOOP FETCHER --------------------------------------

fetcher_running = False  # GLOBAL FLAG

async def run_fetcher():
    global fetcher_running
    if fetcher_running:
        print("⚠️ Fetcher already running, skipping re-launch.")
        return

    fetcher_running = True
    print("🚀 Fetcher launched at", datetime.utcnow().isoformat())

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))
    symbol_to_id = await load_symbols(pg_pool)

    start_time = time(13, 0)
    end_time = time(21, 0)

    while True:
        now = datetime.utcnow()
        current_time = now.time()

        if current_time < start_time:
            print(f"⏳ Waiting for 13:00 UTC — currently {now.time().strftime('%H:%M:%S')} UTC")
            await asyncio.sleep(1)
            continue

        if current_time >= end_time:
            print(f"🛑 Time exceeded 21:00 UTC — exiting fetcher at {now.isoformat()}")
            break

        start = pytime.time()
        await fetch_and_store(redis, pg_pool, symbol_to_id)
        elapsed = pytime.time() - start

        if elapsed < FETCH_INTERVAL:
            await asyncio.sleep(FETCH_INTERVAL - elapsed)
        else:
            print(f"⚠️ Insert took {elapsed:.2f}s — skipping dynamic sleep")
            await asyncio.sleep(FETCH_INTERVAL)

    await redis.close()
    await pg_pool.close()
    fetcher_running = False

# ------------------------------------------ ENTRYPOINT ------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_fetcher())
