# services/SyncRedis.py

import os
from dotenv import load_dotenv
from pathlib import Path
from redis.asyncio import Redis
import asyncpg
from sqlalchemy import select
from datetime import datetime

if os.environ.get("ENV") != "fly":
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

# -------------------------- LOAD STOCK SYMBOLS FROM DB ---------------------------------------------

async def fetch_symbols():
    try:
        conn = await asyncpg.connect(DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))
        rows = await conn.fetch("SELECT symbol FROM stocks")
        await conn.close()
        return [row["symbol"] for row in rows]
    except Exception as e:
        print(f"❌ Failed to fetch symbols from DB: {e}")
        return []

# ----------------------------------- INITIAL POPULATION -------------------------------------

async def initialize_redis_symbols():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    symbols = await fetch_symbols()

    if not symbols:
        print("⚠️ No symbols found.")
        return

    try:
        await redis.delete("stock:symbols")
        BATCH_SIZE = 1000
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            await redis.sadd("stock:symbols", *batch)
        print(f"✅ Initialized Redis with {len(symbols)} symbols.")
    except Exception as e:
        print(f"❌ Redis init failed: {e}")


# ------------------------------------INCREMENTAL SYNC --------------------------------------


async def sync_symbols_to_redis():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    current_symbols = set(await fetch_symbols())

    try:
        existing_symbols = await redis.smembers("stock:symbols")
        existing_symbols = set(existing_symbols)

        to_add = current_symbols - existing_symbols
        to_remove = existing_symbols - current_symbols

        if to_add:
            await redis.sadd("stock:symbols", *to_add)
            print(f" Added {len(to_add)} new symbols.")
        if to_remove:
            await redis.srem("stock:symbols", *to_remove)
            print(f" Removed {len(to_remove)} symbols.")
        if not to_add and not to_remove:
            print("✅ Redis already in sync.")
    except Exception as e:
        print(f"❌ Redis sync failed: {e}")
