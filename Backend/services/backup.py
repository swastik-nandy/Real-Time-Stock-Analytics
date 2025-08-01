import asyncio
import asyncpg
import pandas as pd
from pathlib import Path
import os
import subprocess
from datetime import datetime

# -------------------- CONFIG --------------------

raw_url = os.environ.get("DATABASE_URL")
if raw_url and raw_url.startswith("postgresql+asyncpg://"):
    DATABASE_URL = raw_url.replace("postgresql+asyncpg://", "postgresql://")
else:
    DATABASE_URL = raw_url

GIT_REPO_DIR = Path(__file__).resolve().parents[1]  # repo root
CSV_PATH = GIT_REPO_DIR / "stock_price_history.csv"
BRANCH_NAME = "backups"
COMMIT_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = os.environ.get("GITHUB_REPO")  # e.g., "swastik-nandy/Real-Time-Stock-Analytics"

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

        # Check if inside a git repo
        result = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], capture_output=True)
        if result.returncode != 0:
            print("⚠️ Not a Git repo. Initializing a fresh repo...")
            subprocess.run(["git", "init"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"], check=True)
            subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
            subprocess.run(["git", "remote", "add", "origin", f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git"], check=True)

        # Checkout or create backups branch
        result = subprocess.run(["git", "checkout", BRANCH_NAME])
        if result.returncode != 0:
            print(f"⚠️ Branch '{BRANCH_NAME}' not found. Creating it...")
            subprocess.run(["git", "checkout", "-b", BRANCH_NAME], check=True)

        subprocess.run(["git", "pull", "origin", BRANCH_NAME], check=False)
        subprocess.run(["git", "add", str(CSV_PATH)], check=True)
        subprocess.run(["git", "commit", "-m", f"📊 Daily backup: {COMMIT_TIME}"], check=False)
        subprocess.run(["git", "push", f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git", f"HEAD:{BRANCH_NAME}"], check=True)

        print("✅ Backup pushed to GitHub.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")

# -------------------- MAIN --------------------

if __name__ == "__main__":
    success = asyncio.run(export_stock_price_history())
    if success:
        commit_and_push()
