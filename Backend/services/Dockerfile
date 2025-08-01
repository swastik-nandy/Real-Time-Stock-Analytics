FROM python:3.11-slim

WORKDIR /app

COPY . .

# Install build tools, git, and clean up after
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Environment setup
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=DEBUG

# Launch both services with better logging and debugging
CMD ["sh", "-c", "\
    echo '🟢 Container started at: $(date)'; \
    echo '📂 Current working dir: $(pwd)'; \
    echo '📦 Installed packages:'; pip list; \
    echo '🔍 Checking Redis connection...'; \
    python -c 'import os, redis; print(redis.Redis.from_url(os.environ.get(\"REDIS_URL\")).ping())'; \
    echo '🔍 Checking Postgres connection...'; \
    python -c 'import asyncpg, asyncio, os; asyncio.run(asyncpg.connect(os.environ.get(\"DATABASE_URL\").replace(\"postgresql+asyncpg://\", \"postgresql://\")))'; \
    echo '🚀 Launching trigger.py & WebSocket.py...'; \
    python trigger.py & \
    python WebSocket.py && \
    wait"]
