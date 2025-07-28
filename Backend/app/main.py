from fastapi import FastAPI
from app.api.routes import router as api_router
from app.db.redis_client import init_redis, close_redis
from Backend.Utils.SyncRedis import initialize_redis_symbols

app = FastAPI()

# Include only the API router (WebSocket runs separately)
app.include_router(api_router)

@app.on_event("startup")
async def startup_event():
    # Initialize Redis connection
    await init_redis()

    # Perform a one-time sync of stock symbols to Redis
    await initialize_redis_symbols()

@app.on_event("shutdown")
async def shutdown_event():
    await close_redis()
