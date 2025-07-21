FROM python:3.10

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y tree && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "echo '🔥 STARTING CONTAINER' && tree && echo '📁 FILES LISTED' && env && echo '🚀 LAUNCHING PYTHON' && python WebSocket.py"]
