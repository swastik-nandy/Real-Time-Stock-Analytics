FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# Install build tools, git, and clean up after
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files AFTER deps installed
COPY . .

# Set up Git and ensure backups branch is initialized
RUN git config --global user.email "actions@github.com" && \
    git config --global user.name "github-actions" && \
    git fetch origin backups || true && \
    git checkout -B backups origin/backups || git checkout -b backups

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
