# app/db/redis_client.py
import aioredis
from app.core.config import settings

_redis = None  

async def init_redis():
    global _redis
    _redis = await aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        retry_on_timeout=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    try:
        await _redis.ping()
        print("[Redis] Connected")
    except Exception as e:
        print(f"⚠️ Redis init ping failed: {e}")

async def close_redis():
    global _redis
    if _redis:
        try:
            await _redis.close()
            await _redis.wait_closed()
            print("[Redis] Connection closed")
        except Exception as e:
            print(f"⚠️ Redis close failed: {e}")

def get_redis():
    return _redis
