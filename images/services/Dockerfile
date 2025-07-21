# Base image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Copy the backend code into the image
COPY ../../Backend /app

# Install dependencies
RUN apt-get update && apt-get install -y gcc build-essential && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r app/requirements.txt

# Set environment
ENV PYTHONUNBUFFERED=1

# Entrypoint — launches WebSocket.py which will trigger fetcher internally
CMD ["python", "services/WebSocket.py"]
