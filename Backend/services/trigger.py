import asyncio
import os
import asyncpg
from redis.asyncio import Redis
from datetime import datetime, time
from pathlib import Path
from dotenv import load_dotenv

from SyncRedis import initialize_redis_symbols, sync_symbols_to_redis
from cleaner import run_cleanup
from fetcher import run_fetcher

# ---------------------------- ENV SETUP ----------------------------

if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
DSN = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

# ---------------------- POSTGRES LISTENER -------------------------

async def on_stock_change(conn, pid, channel, payload):
    print("\n📣 Detected change in 'stocks' table.")
    print("🔁 Syncing Redis with updated symbols...")
    await sync_symbols_to_redis()

async def listen_to_stock_changes():
    try:
        conn = await asyncpg.connect(DSN)
        await conn.add_listener("stock_changed", on_stock_change)
        print("👂 Listening to 'stock_changed' notifications from Postgres...")
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        print(f"❌ LISTEN failed: {e}")

# ------------------------ TIME TRIGGERS ----------------------------

fetcher_running = False
cleanup_stock_done = False
cleanup_predicted_done = False

async def table_not_empty(conn, table: str) -> bool:
    try:
        result = await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1);")
        return result
    except Exception as e:
        print(f"❌ Failed to check {table}: {e}")
        return False

async def time_based_trigger():
    global fetcher_running, cleanup_stock_done, cleanup_predicted_done

    pg_pool = await asyncpg.create_pool(DSN)

    while True:
        now = datetime.utcnow()
        current_time = now.time()

        # ------------------- CLEANUP stock_price_history -------------------

        if time(0, 5) <= current_time < time(0, 30) and not cleanup_stock_done:
            async with pg_pool.acquire() as conn:
                not_empty = await table_not_empty(conn, "stock_price_history")
                if not_empty:
                    print("🧹 Running cleanup for stock_price_history")
                    await run_cleanup()
                else:
                    print("✅ stock_price_history already empty — skipping cleanup")
            cleanup_stock_done = True

        if current_time >= time(0, 30):
            cleanup_stock_done = False  # Reset flag after window ends

        # ------------------- CLEANUP predicted_prices (exactly at 23:00 UTC) -------------------

        if current_time >= time(23, 0) and not cleanup_predicted_done:
            async with pg_pool.acquire() as conn:
                not_empty = await table_not_empty(conn, "predicted_prices")
                if not_empty:
                    print("🧹 Running cleanup for predicted_prices")
                    await run_cleanup()
                else:
                    print("✅ predicted_prices already empty — skipping cleanup")
            cleanup_predicted_done = True

        if current_time < time(23, 0):
            cleanup_predicted_done = False  # Reset flag before 23:00 window

        # ------------------- FETCHER KICK  -------------------

        if time(0, 32) <= current_time <= time(23, 59, 50) and not fetcher_running:
            print(f"\n🚀 Starting fetcher at {current_time}")
            asyncio.create_task(run_fetcher())
            fetcher_running = True

        if current_time >= time(23, 59, 50):
            fetcher_running = False  # Reset flag after window

        await asyncio.sleep(30)

# ------------------------ MAIN ENTRY ------------------------------

async def main():
    print("🚀 Trigger Controller Started")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    await redis.ping()
    print("[Redis] Connected")

    await initialize_redis_symbols()

    await asyncio.gather(
        listen_to_stock_changes(),
        time_based_trigger()
    )

if __name__ == "__main__":
    asyncio.run(main())
