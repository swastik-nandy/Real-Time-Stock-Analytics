# Dockerfile

FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose FastAPI port
EXPOSE 8080

# Entrypoint for uvicorn FastAPI server
CMD ["uvicorn", "services.fetcher:app", "--host", "0.0.0.0", "--port", "8080"]
