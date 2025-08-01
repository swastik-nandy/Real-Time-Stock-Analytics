import asyncio
import asyncpg
import pandas as pd
from pathlib import Path
import os
import subprocess
from datetime import datetime
import shutil

# -------------------- CONFIG ---------------------

raw_url = os.environ.get("DATABASE_URL")
if raw_url and raw_url.startswith("postgresql+asyncpg://"):
    DATABASE_URL = raw_url.replace("postgresql+asyncpg://", "postgresql://")
else:
    DATABASE_URL = raw_url

GIT_REPO_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = GIT_REPO_DIR / "stock_price_history.csv"
BRANCH_NAME = "backups"
COMMIT_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
REPO = os.environ.get("GITHUB_REPO", "").strip()  # e.g., "swastik-nandy/Real-Time-Stock-Analytics"

# -------------------- EXPORT FUNCTION --------------------

async def export_stock_price_history():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT * FROM stock_price_history")
        await conn.close()

        if not rows:
            print("❌ No data found in stock_price_history.")
            return False

        df = pd.DataFrame([dict(row) for row in rows])
        df.to_csv(CSV_PATH, index=False)
        print(f"✅ Exported {len(df)} rows to {CSV_PATH}")
        return True

    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False

# -------------------- GIT COMMIT & PUSH --------------------

def commit_and_push():
    try:
        os.chdir(GIT_REPO_DIR)

        # Wipe old .git if broken
        if (GIT_REPO_DIR / ".git").exists():
            shutil.rmtree(GIT_REPO_DIR / ".git", ignore_errors=True)

        print("🌀 Initializing fresh Git repository...")
        subprocess.run(["git", "init"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "remote", "add", "origin",
                        f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git"], check=True)

        subprocess.run(["git", "fetch", "origin"], check=False)
        subprocess.run(["git", "checkout", "-B", BRANCH_NAME], check=True)

        subprocess.run(["git", "add", str(CSV_PATH)], check=True)
        subprocess.run(["git", "commit", "-m", f"📊 Daily backup: {COMMIT_TIME}"], check=False)

        push_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git"
        print(f"🚀 Git pushing to: {push_url} on branch: {BRANCH_NAME}")
        subprocess.run(["git", "push", "--force", push_url, f"HEAD:{BRANCH_NAME}"], check=True)

        print("✅ Backup pushed to GitHub.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# -------------------- MAIN --------------------

if __name__ == "__main__":
    success = asyncio.run(export_stock_price_history())
    if success:
        commit_and_push()
