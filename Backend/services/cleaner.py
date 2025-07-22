# services/cleaner.py

import asyncio
from datetime import datetime, time
import asyncpg
import os
from dotenv import load_dotenv
from pathlib import Path

# --------------------------------------- ENVIRONMENT -------------------------------------------

if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

# --------------------------------------- TABLE EMPTY CHECK --------------------------------------

async def table_not_empty(conn, table: str) -> bool:
    try:
        result = await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1);")
        return result
    except Exception as e:
        print(f"❌ Failed to check if {table} is empty: {e}")
        return False

# ------------------------------------ MAIN CLEANUP TASK --------------------------------------

async def run_cleanup():
    now_utc = datetime.utcnow()
    current_time = now_utc.time()
    print(f"[CLEANER] Triggered at: {now_utc.isoformat()} UTC")

    try:
        pg_pool = await asyncpg.create_pool(DATABASE_URL)

        async with pg_pool.acquire() as conn:
            start = datetime.utcnow()

            # --- stock_price_history: Between 12:00 - 13:00 UTC ---
            if time(12, 0) <= current_time < time(13, 0):
                if await table_not_empty(conn, "stock_price_history"):
                    print("🧹 Cleaning stock_price_history")
                    await conn.execute("TRUNCATE stock_price_history RESTART IDENTITY")
                    await conn.execute("VACUUM FULL VERBOSE ANALYZE stock_price_history")
                else:
                    print("✅ stock_price_history already empty — skipping")

            # --- predicted_prices: Between 23:00 - 00:00 UTC ---
            elif time(23, 0) <= current_time or current_time < time(0, 0):
                if await table_not_empty(conn, "predicted_prices"):
                    print("🧹 Cleaning predicted_prices")
                    await conn.execute("TRUNCATE predicted_prices RESTART IDENTITY")
                    await conn.execute("VACUUM FULL VERBOSE ANALYZE predicted_prices")
                else:
                    print("✅ predicted_prices already empty — skipping")

            else:
                print("⏳ Not in cleanup window — skipping")

            duration = (datetime.utcnow() - start).total_seconds()
            print(f"[CLEANER] Finished in {duration:.2f} seconds ✅")

    except Exception as e:
        print(f"[ERROR] Cleanup failed: {e}")

# ------------------------------------ CLI ENTRY ----------------------------------------

if __name__ == "__main__":
    asyncio.run(run_cleanup())
