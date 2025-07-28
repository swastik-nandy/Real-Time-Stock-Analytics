import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------------------------
# Conditionally Load `.env` in Local
# ---------------------------------
if os.environ.get("ENV") != "fly":
    ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(dotenv_path=ENV_PATH, override=True)
    print("ENV TEST:", os.getenv("DATABASE_URL"))


# ---------------------------------
# Pydantic Settings
# ---------------------------------
class Settings(BaseSettings):
    database_url: str
    redis_url: str
    finnhub_api_key: str

    model_config = SettingsConfigDict(env_file_encoding="utf-8")

# Exported settings
settings = Settings()
