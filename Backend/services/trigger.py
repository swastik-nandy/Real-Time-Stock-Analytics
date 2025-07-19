# services/trigger.py

# IMPORTS & CONFIGURATION
# ---------------------------------
import asyncio
from datetime import datetime, time
import asyncpg
import subprocess
import sys
from pathlib import Path

# Add 'backend' to sys.path to allow app.* imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.utils.SyncRedis import initialize_redis_symbols, sync_symbols_to_redis
from app.utils.cleaner import run_cleanup
from app.db.redis_client import init_redis
from app.core.config import settings

# DSN for asyncpg LISTEN/NOTIFY
DSN = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")

# Time-based Triggers
CLEANUP_TIMES_UTC = [time(12, 0), time(23, 0)]
FETCHER_START_UTC = time(13, 0)
FETCHER_END_UTC = time(21, 0)

# State flags
last_cleanup_run = None
fetcher_started = False

# ---------------------------------
# POSTGRES LISTEN/NOTIFY HANDLER
# ---------------------------------

async def on_stock_change(conn, pid, channel, payload):
    print("\n📣 Detected change in 'stocks' table.")
    print("🔁 Running incremental Redis sync...")
    await sync_symbols_to_redis()

async def listen_to_stock_changes():
    try:
        conn = await asyncpg.connect(DSN)
        await conn.add_listener("stock_changed", on_stock_change)
        print("👂 Listening to 'stock_changed' notifications from Postgres...")
        while True:
            await asyncio.sleep(3600)  # keep alive
    except Exception as e:
        print(f"❌ LISTEN failed: {e}")

# ---------------------------------
# TIME-BASED TRIGGER LOOP
# ---------------------------------

async def time_based_trigger():
    global last_cleanup_run, fetcher_started
    print("⏰ Starting time-based trigger loop...")
    while True:
        now_utc = datetime.utcnow()
        now_time = now_utc.time().replace(second=0, microsecond=0)

        # --- CLEANER ---
        for scheduled_time in CLEANUP_TIMES_UTC:
            if now_time == scheduled_time and last_cleanup_run != now_time:
                print(f"\n🧹 Time matched ({scheduled_time}) — Running cleaner...")
                await run_cleanup()
                last_cleanup_run = now_time

        # --- FETCHER ---
        if FETCHER_START_UTC <= now_time <= FETCHER_END_UTC and not fetcher_started:
            print(f"\n🚀 Time in range ({FETCHER_START_UTC}–{FETCHER_END_UTC}) — Launching fetcher...")
            subprocess.Popen(["python", "services/fetcher.py"])
            fetcher_started = True

        await asyncio.sleep(30)

# ---------------------------------
# MAIN CONTROLLER
# ---------------------------------

async def main():
    print("🚀 Starting Trigger Controller")

    print("🔌 Initializing Redis client...")
    await init_redis()

    print("📦 Performing initial Redis sync with DB...")
    await initialize_redis_symbols()

    await asyncio.gather(
        listen_to_stock_changes(),
        time_based_trigger()
    )

if __name__ == "__main__":
    asyncio.run(main())
