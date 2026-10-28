
import hashlib
import os
import re
from pathlib import Path
import asyncio
import shutil
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response

METADATA_URL = os.getenv(
    "METADATA_URL",
    "http://metadata:8010"
)

async def send_heartbeats():
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            try:
                free_space = shutil.disk_usage(STORAGE_DIR).free

                chunk_count = sum(
                    1 for file in STORAGE_DIR.iterdir()
                    if file.is_file()
                )

                payload = {
                    "node_id": NODE_ID,
                    "free_space": free_space,
                    "chunk_count": chunk_count
                }

                response = await client.post(
                    f"{METADATA_URL}/heartbeats",
                    json=payload
                )

                response.raise_for_status()

                print(f"Heartbeat sent: {NODE_ID}", flush=True)

            except Exception as e:
                print(f"Heartbeat failed: {e}", flush=True)

            await asyncio.sleep(5)


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

app = FastAPI(
    title="Distributed Storage Node",
    lifespan=lifespan
)

STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage_data"))
NODE_ID = os.getenv("NODE_ID", "node1")

STORAGE_DIR.mkdir(parents=True, exist_ok=True)


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
    return {"status": "healthy", "node_id": NODE_ID}


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

    path.write_bytes(data)

    return {"message": "Chunk stored", "hash": chunk_hash, "size": len(data)}


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    data = read_verified_chunk(chunk_hash)
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
