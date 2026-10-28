
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir httpx

COPY metadata_service /app/metadata_service
COPY repair_worker /app/repair_worker

CMD ["python", "-u", "-m", "repair_worker.main"]
