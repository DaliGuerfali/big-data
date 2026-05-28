FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" redis httpx
WORKDIR /app
CMD ["python", "-m", "uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8084"]
