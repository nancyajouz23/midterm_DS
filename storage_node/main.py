import asyncio
import hashlib
import os
import re
import shutil
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
)
from prometheus_client.core import GaugeMetricFamily, REGISTRY

METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8010")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage_data"))
NODE_ID = os.getenv("NODE_ID", "node1")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

# Address other services use to reach this node. It is sent in every
# heartbeat, so new nodes join the cluster without any configuration
# change (Docker: http://storage-node-8:8000, Kubernetes:
# http://storage-node-3.storage-node:8000).
NODE_URL = os.getenv("NODE_URL", f"http://{socket.gethostname()}:8000")

STORAGE_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------
# Metrics
# -------------------------

REQUESTS = Counter(
    "storage_node_requests",
    "Chunk requests handled by this node",
    ["method", "status"]
)
LATENCY = Histogram(
    "storage_node_request_seconds",
    "Chunk request latency",
    ["method"]
)
BYTES = Counter(
    "storage_node_bytes",
    "Chunk bytes written to / read from this node",
    ["direction"]
)


def disk_stats():
    chunks, used = 0, 0
    for file in STORAGE_DIR.iterdir():
        if file.is_file():
            chunks += 1
            used += file.stat().st_size
    return chunks, used


class DiskCollector:
    def collect(self):
        chunks, used = disk_stats()
        usage = shutil.disk_usage(STORAGE_DIR)
        labels = ["node_id"]
        for name, doc, value in (
            ("storage_node_chunks", "Chunks stored on this node", chunks),
            ("storage_node_disk_used_bytes",
             "Bytes of chunk data on this node", used),
            ("storage_node_disk_free_bytes",
             "Free space on the node's disk", usage.free),
            ("storage_node_disk_total_bytes",
             "Size of the node's disk", usage.total),
        ):
            metric = GaugeMetricFamily(name, doc, labels=labels)
            metric.add_metric([NODE_ID], value)
            yield metric


REGISTRY.register(DiskCollector())


# -------------------------
# Heartbeats
# -------------------------

async def send_heartbeats():
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                chunk_count, _ = disk_stats()
                payload = {
                    "node_id": NODE_ID,
                    "free_space": shutil.disk_usage(STORAGE_DIR).free,
                    "chunk_count": chunk_count,
                    "url": NODE_URL
                }
                response = await client.post(
                    f"{METADATA_URL}/heartbeats", json=payload
                )
                response.raise_for_status()
                print(f"Heartbeat sent: {NODE_ID}", flush=True)
            except Exception as e:
                print(f"Heartbeat failed: {e}", flush=True)

            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(send_heartbeats())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Distributed Storage Node", lifespan=lifespan)


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    if not request.url.path.startswith("/chunks/"):
        return await call_next(request)
    start = time.perf_counter()
    response = await call_next(request)
    LATENCY.labels(request.method).observe(time.perf_counter() - start)
    REQUESTS.labels(request.method, str(response.status_code)).inc()
    return response


# -------------------------
# Chunk API
# -------------------------

def get_chunk_path(chunk_hash: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{64}", chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")
    return STORAGE_DIR / chunk_hash


def read_verified_chunk(chunk_hash: str) -> bytes:
    path = get_chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    data = path.read_bytes()
    actual_hash = hashlib.sha256(data).hexdigest()

    if actual_hash != chunk_hash:
        raise HTTPException(status_code=409, detail="Corrupted chunk detected")

    return data


@app.get("/health")
def health():
    """Liveness/readiness: the process answers and the disk is usable."""
    if not os.access(STORAGE_DIR, os.W_OK):
        raise HTTPException(503, "Storage directory not writable")
    return {"status": "healthy", "node_id": NODE_ID}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.put("/chunks/{chunk_hash}", status_code=201)
async def put_chunk(chunk_hash: str, request: Request):
    path = get_chunk_path(chunk_hash)
    data = await request.body()

    actual_hash = hashlib.sha256(data).hexdigest()

    if actual_hash != chunk_hash:
        raise HTTPException(status_code=400, detail="SHA-256 mismatch")

    if path.exists():
        read_verified_chunk(chunk_hash)
        return {"message": "Chunk already exists", "hash": chunk_hash}

    # Write to a temporary file first so a crash never leaves a
    # half-written chunk under its final name.
    temp = path.with_suffix(".tmp")
    temp.write_bytes(data)
    temp.replace(path)
    BYTES.labels("in").inc(len(data))

    return {"message": "Chunk stored", "hash": chunk_hash, "size": len(data)}


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    data = read_verified_chunk(chunk_hash)
    BYTES.labels("out").inc(len(data))
    return Response(content=data, media_type="application/octet-stream")


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    data = read_verified_chunk(chunk_hash)
    return Response(
        status_code=200,
        headers={
            "Content-Length": str(len(data)),
            "X-Chunk-Hash": chunk_hash
        }
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    path = get_chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    path.unlink()
    return {"message": "Chunk deleted", "hash": chunk_hash}
