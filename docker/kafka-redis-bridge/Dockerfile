FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends libsnappy-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir kafka-python redis python-snappy
WORKDIR /app
CMD ["python", "-m", "serving.bridge.kafka_redis_bridge"]
