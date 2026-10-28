
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn httpx

COPY storage_node /app/storage_node

EXPOSE 8000

CMD ["uvicorn", "storage_node.main:app", "--host", "0.0.0.0", "--port", "8000"]
