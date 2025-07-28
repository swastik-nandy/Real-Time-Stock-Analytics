import asyncio
import asyncpg
import pandas as pd
from pathlib import Path
import os

# Get the database URL from GitHub secrets
DATABASE_URL = os.environ.get("DATABASE_URL")

# Path where the CSV will be written
CSV_PATH = Path("stock_price_history.csv")

async def export_stock_price_history():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT * FROM stock_price_history")
        await conn.close()

        if not rows:
            print("⚠️ No data found in stock_price_history.")
            return

        df = pd.DataFrame([dict(row) for row in rows])
        df.to_csv(CSV_PATH, index=False)
        print(f"✅ Exported {len(df)} rows to {CSV_PATH}")
    except Exception as e:
        print(f"❌ Export failed: {e}")

if __name__ == "__main__":
    asyncio.run(export_stock_price_history())
