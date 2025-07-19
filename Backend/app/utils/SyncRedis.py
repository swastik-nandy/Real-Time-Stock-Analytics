# ---------------------------------
# REDIS SYNC HELPERS
# ---------------------------------

from sqlalchemy import select
from app.db.database import async_session
from app.models.stock import Stock
from app.db.redis_client import get_redis  # Use the getter

# --------------------------------------------------
# INITIAL REDIS POPULATION (RUN ON BACKEND STARTUP)
# --------------------------------------------------

async def initialize_redis_symbols():
    redis = get_redis()
    if redis is None:
        print("⚠️ Redis client not initialized. Skipping initial sync.")
        return

    async with async_session() as session:
        result = await session.execute(select(Stock.symbol))
        symbols = [row[0] for row in result.fetchall()]

        if symbols:
            try:
                await redis.delete("stock:symbols")  # Clear old set
                BATCH_SIZE = 1000
                for i in range(0, len(symbols), BATCH_SIZE):
                    batch = symbols[i:i + BATCH_SIZE]
                    await redis.sadd("stock:symbols", *batch)
                print(f"✅ Initialized Redis with {len(symbols)} symbols.")
            except Exception as e:
                print(f"❌ Failed to initialize Redis: {e}")
        else:
            print("⚠️ No symbols found in 'stocks' table.")

# --------------------------------------------------
# INCREMENTAL REDIS UPDATE (ON DB CHANGE NOTIFICATION)
# --------------------------------------------------

async def sync_symbols_to_redis():
    redis = get_redis()
    if redis is None:
        print("⚠️ Redis client not initialized yet. Skipping symbol sync.")
        return

    async with async_session() as session:
        result = await session.execute(select(Stock.symbol))
        current_symbols = {row[0] for row in result.fetchall()}

    existing_symbols = await redis.smembers("stock:symbols")
    existing_symbols = {s.decode() if isinstance(s, bytes) else s for s in existing_symbols}

    to_add = current_symbols - existing_symbols
    to_remove = existing_symbols - current_symbols

    try:
        if to_add:
            await redis.sadd("stock:symbols", *to_add)
            print(f"➕ Added {len(to_add)} new symbols to Redis.")
        if to_remove:
            await redis.srem("stock:symbols", *to_remove)
            print(f"➖ Removed {len(to_remove)} symbols from Redis.")
        if not to_add and not to_remove:
            print(" Redis already in sync with database.")
    except Exception as e:
        print(f"❌ Failed to sync Redis symbols: {e}")
