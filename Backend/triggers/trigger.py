import asyncio
import os
import asyncpg
import aioredis
import aiohttp
from datetime import datetime, time
from pathlib import Path
from dotenv import load_dotenv

from Backend.Utils.SyncRedis import initialize_redis_symbols, sync_symbols_to_redis
from Backend.Utils.cleaner import run_cleanup

# ----------------------------------------- ENV SETUP ----------------------------------------------------------

if os.environ.get("ENV") != "fly":
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
DSN = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
FETCH_TRIGGER_TOKEN = os.environ.get("FETCH_TRIGGER_TOKEN")
FETCHER_URL = os.environ.get("FETCHER_URL", "https://your-fetcher.fly.dev/start-fetcher")  # set this in .env or fly secrets

# ---------------------------------------- REDIS INIT----------------------------------------

async def init_redis():
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
        print("[Redis] Connected")
    except Exception as e:
        print(f"⚠️ Redis init failed: {e}")
    return redis

# -------------------------------------POSTGRES LISTENER ---------------------------------------

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

# --------------------------------------------TIME TRIGGER ------------------------------------------------

cleanup_hours = {12, 23}
fetcher_start_time = time(12, 55)
fetcher_end_time = time(21, 0)

last_cleanup_hour = None
fetcher_running = False

async def trigger_fetcher():
    headers = {
        "Authorization": f"Bearer {FETCH_TRIGGER_TOKEN}"
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(FETCHER_URL, headers=headers) as resp:
                if resp.status == 200:
                    print("✅ Fetcher successfully triggered.")
                else:
                    print(f"❌ Fetcher trigger failed — Status: {resp.status}")
        except Exception as e:
            print(f"❌ HTTP error while triggering fetcher: {e}")

async def time_based_trigger():
    global last_cleanup_hour, fetcher_running

    while True:
        now = datetime.utcnow()
        current_hour = now.hour
        current_time = now.time()

        # RUN CLEANUP IF NOT DONE TODAY ----------------------------------------------

        if current_hour in cleanup_hours and current_hour != last_cleanup_hour:
            print(f"\n🧹 Triggering cleanup at hour {current_hour}")
            await run_cleanup()
            last_cleanup_hour = current_hour

        # LAUNCH FETCHER VIA SECURE HTTP TRIGGER ----------------------------------------

        if fetcher_start_time <= current_time < fetcher_end_time and not fetcher_running:
            print(f"\n🚀 Triggering fetcher remotely at {current_time}")
            await trigger_fetcher()
            fetcher_running = True

        # RESET FETCHER FLAG --------------------------------------------

        if current_time >= fetcher_end_time and fetcher_running:
            print(f"\n🛑 Fetcher window ended at {current_time}")
            fetcher_running = False

        await asyncio.sleep(30)

# ---------------------------- MAIN ------------------------------

async def main():
    print("🚀 Trigger Controller Started")
    await init_redis()
    await initialize_redis_symbols()

    await asyncio.gather(
        listen_to_stock_changes(),
        time_based_trigger()
    )

if __name__ == "__main__":
    asyncio.run(main())
