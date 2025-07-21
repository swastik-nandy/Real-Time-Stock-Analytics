FROM python:3.10

WORKDIR /app

COPY . /app

# Optional : still keep update+clean in case you want to add things later
RUN apt-get update && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "ls -al && python WebSocket.py"]
