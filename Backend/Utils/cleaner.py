# services/cleaner.py

import asyncio
from datetime import datetime
import asyncpg
import os
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------ENVIRONMENT -------------------------------------------

if os.environ.get("ENV") != "fly":
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")


# ------------------------------------ MAIN CLEANUP TASK --------------------------------------

async def run_cleanup():
    now_utc = datetime.utcnow()
    hour = now_utc.hour
    print(f"[CLEANER] Triggered at UTC hour: {hour}")

    try:
        pg_pool = await asyncpg.create_pool(DATABASE_URL)

        async with pg_pool.acquire() as conn:
            start_time = datetime.utcnow()

            if hour == 12:
                print("🧹 TRUNCATE + RESTART IDENTITY for stock_price_history")
                await conn.execute("TRUNCATE stock_price_history RESTART IDENTITY")
                print("🧹 VACUUM FULL stock_price_history")
                await conn.execute("VACUUM FULL VERBOSE ANALYZE stock_price_history")

            elif hour == 23:
                print("🧹 TRUNCATE + RESTART IDENTITY for predicted_prices")
                await conn.execute("TRUNCATE predicted_prices RESTART IDENTITY")
                print("🧹 VACUUM FULL predicted_prices")
                await conn.execute("VACUUM FULL VERBOSE ANALYZE predicted_prices")

            else:
                print("[CLEANER] Skipping: No task scheduled for this hour")
                return

            duration = (datetime.utcnow() - start_time).total_seconds()
            print(f"[CLEANER] Duration: {duration:.2f} seconds")

        print("[CLEANER] Completed successfully ✅")

    except Exception as e:
        print(f"[ERROR] Cleanup failed: {e}")

# ------------------------------------ OPEN CLI ENTRY ----------------------------------------

if __name__ == "__main__":
    asyncio.run(run_cleanup())
