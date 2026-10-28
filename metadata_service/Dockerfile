
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary

COPY metadata_service /app/metadata_service

EXPOSE 8010

CMD ["uvicorn", "metadata_service.main:app", "--host", "0.0.0.0", "--port", "8010"]
