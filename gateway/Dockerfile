
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    httpx \
    psycopg2-binary \
    zfec \
    python-multipart

COPY metadata_service /app/metadata_service
COPY gateway /app/gateway

EXPOSE 8020

CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8020"]
