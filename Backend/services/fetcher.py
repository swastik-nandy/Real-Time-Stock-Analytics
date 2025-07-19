import os
import asyncio
import json
import time as pytime
from datetime import datetime, time
import aioredis
import asyncpg
from pathlib import Path
import sys

# -------- Import settings from config --------
sys.path.append(str(Path(__file__).resolve().parents[1]))  # adds backend/
from app.core.config import settings

# -------- CONFIG --------
REDIS_URL = settings.redis_url
DATABASE_URL = settings.database_url
PRICE_KEY_PREFIX = "stock:price:"
FETCH_INTERVAL = 10  # seconds

# -------- TIME WINDOW --------
START_UTC = time(13, 0)
END_UTC = time(21, 0)

# -------- UTILS --------

def is_within_window(now: datetime) -> bool:
    return START_UTC <= now.time() < END_UTC

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

# -------- MAIN LOOP --------

async def main():
    print("🚀 Fetcher started")
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))
    symbol_to_id = await load_symbols(pg_pool)

    while True:
        now = datetime.utcnow()

        if now.time() >= END_UTC:
            print(f"🛑 Fetcher exiting at {now.isoformat()} — window closed.")
            break

        if not is_within_window(now):
            print(f"⏳ Not within write window yet ({now.time()} UTC). Sleeping...")
            await asyncio.sleep(10)
            continue

        start = pytime.time()
        await fetch_and_store(redis, pg_pool, symbol_to_id)
        elapsed = pytime.time() - start
        await asyncio.sleep(max(0, FETCH_INTERVAL - elapsed))

if __name__ == "__main__":
    asyncio.run(main())
