FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir httpx zfec pika prometheus-client

COPY metadata_service /app/metadata_service
COPY common /app/common
COPY repair_worker /app/repair_worker

# 9100: Prometheus metrics
EXPOSE 9100

# ROLE=scheduler or ROLE=worker (see repair_worker/main.py)
CMD ["python", "-u", "-m", "repair_worker.main"]
