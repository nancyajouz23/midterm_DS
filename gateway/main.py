
import asyncio
import hashlib
import json
import os
import secrets
import tempfile
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import (
    APIRouter, Depends, FastAPI, HTTPException,
    Query, Request, Response
)
from fastapi.responses import StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
from zfec.easyfec import Encoder, Decoder

from metadata_service.chunking import fixed_size_chunks
from metadata_service.placement import ConsistentHashRing


METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8010")
DATABASE_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["API_KEY"]
RATE_LIMIT = 60

N = 3
W = 2
R = 2
K = 4
M = 2

NODES = {
    f"node{i}": f"http://storage-node-{i}:8000"
    for i in range(1, 7)
}

ring = ConsistentHashRing(sorted(NODES), virtual_nodes=128)
encoder = Encoder(K, K + M)
decoder = Decoder(K, K + M)

requests_by_key = defaultdict(deque)
rate_lock = Lock()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def db():
    return psycopg2.connect(DATABASE_URL)


@asynccontextmanager
async def lifespan(app):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bucket_settings (
                    bucket TEXT PRIMARY KEY REFERENCES buckets(name),
                    mode TEXT NOT NULL
                        CHECK (mode IN ('replication', 'erasure'))
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS erasure_shards (
                    bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    shard_index INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    shard_hash VARCHAR(64) NOT NULL,
                    shard_size BIGINT NOT NULL,
                    padlen INTEGER NOT NULL,
                    PRIMARY KEY (
                        bucket, object_key, version,
                        position, shard_index
                    ),
                    FOREIGN KEY (bucket, object_key, version)
                        REFERENCES objects(bucket, object_key, version)
                        ON DELETE CASCADE
                )
            """)
    yield


app = FastAPI(
    title="Distributed Object Storage Gateway",
    lifespan=lifespan
)


def authorize(key=Depends(api_key_header)):
    if not key or not secrets.compare_digest(key, API_KEY):
        raise HTTPException(401, "Invalid API key")

    now = time.monotonic()

    with rate_lock:
        history = requests_by_key[key]

        while history and now - history[0] >= 60:
            history.popleft()

        if len(history) >= RATE_LIMIT:
            raise HTTPException(429, "Rate limit exceeded")

        history.append(now)


router = APIRouter(dependencies=[Depends(authorize)])


def metadata_request(client, method, path, **kwargs):
    try:
        response = client.request(
            method, f"{METADATA_URL}{path}", **kwargs
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            exc.response.status_code,
            exc.response.text
        )
    except httpx.RequestError:
        raise HTTPException(503, "Metadata service unavailable")


class BucketBody(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    mode: str = "replication"


@router.post("/buckets", status_code=201)
def create_bucket(body: BucketBody):
    if body.mode not in ("replication", "erasure"):
        raise HTTPException(400, "Mode must be replication or erasure")

    with httpx.Client(timeout=30) as client:
        metadata_request(
            client, "POST", "/buckets",
            json={"name": body.name}
        )

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bucket_settings (bucket, mode)
                VALUES (%s, %s)
                ON CONFLICT (bucket) DO NOTHING
            """, (body.name, body.mode))
            cur.execute(
                "SELECT mode FROM bucket_settings WHERE bucket=%s",
                (body.name,)
            )
            saved_mode = cur.fetchone()[0]

    return {"bucket": body.name, "mode": saved_mode}


@router.get("/buckets")
def list_buckets():
    with db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT b.name, b.created_at,
                       COALESCE(s.mode, 'replication') AS mode
                FROM buckets b
                LEFT JOIN bucket_settings s ON b.name=s.bucket
                ORDER BY b.name
            """)
            return cur.fetchall()


def get_mode(bucket):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT mode FROM bucket_settings WHERE bucket=%s",
                (bucket,)
            )
            row = cur.fetchone()

    if row is None:
        raise HTTPException(404, "Bucket not configured in gateway")
    return row[0]


def write_replicas(client, chunk_hash, data):
    targets = ring.get_nodes(chunk_hash, N)
    successful = []

    for node in targets:
        try:
            r = client.put(
                f"{NODES[node]}/chunks/{chunk_hash}",
                content=data
            )
            r.raise_for_status()
            successful.append(node)
        except httpx.HTTPError:
            pass

    if len(successful) < W:
        raise HTTPException(
            503, f"Write quorum failed: {len(successful)}/{W}"
        )

    return successful


def write_erasure(client, data):
    shard_data = encoder.encode(data)
    shard_rows = []

    # Each shard goes to a different physical node.
    for i, shard in enumerate(shard_data):
        node = f"node{i + 1}"
        digest = hashlib.sha256(shard).hexdigest()

        try:
            r = client.put(
                f"{NODES[node]}/chunks/{digest}",
                content=shard
            )
            r.raise_for_status()
        except httpx.HTTPError:
            raise HTTPException(
                503, f"Erasure shard upload failed on {node}"
            )

        shard_rows.append((
            i, node, digest, len(shard), (-len(data)) % K
        ))

    return shard_rows


def store_file(file_path, bucket, key, tags):
    mode = get_mode(bucket)
    chunks = []
    placements = {}
    ec_rows = []

    with httpx.Client(timeout=90) as client:
        for position, (chunk_hash, data) in enumerate(
            fixed_size_chunks(file_path)
        ):
            chunks.append({
                "hash": chunk_hash,
                "size": len(data)
            })

            if mode == "replication":
                placements[chunk_hash] = write_replicas(
                    client, chunk_hash, data
                )
            else:
                for shard in write_erasure(client, data):
                    ec_rows.append((position, *shard))

        info = metadata_request(
            client, "POST", "/objects/metadata",
            json={
                "bucket": bucket,
                "key": key,
                "size": Path(file_path).stat().st_size,
                "chunks": chunks,
                "tags": {
                    **tags,
                    "redundancy": mode
                }
            }
        )

        version = info["version"]

        if mode == "replication":
            for chunk_hash, nodes in placements.items():
                for node in nodes:
                    metadata_request(
                        client, "POST", "/chunks/locations",
                        json={
                            "chunk_hash": chunk_hash,
                            "node_id": node
                        }
                    )
        else:
            with db() as conn:
                with conn.cursor() as cur:
                    for position, index, node, digest, size, padlen in ec_rows:
                        cur.execute("""
                            INSERT INTO erasure_shards
                            (bucket, object_key, version, position,
                             shard_index, node_id, shard_hash,
                             shard_size, padlen)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (
                            bucket, key, version, position,
                            index, node, digest, size, padlen
                        ))

    return {
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": Path(file_path).stat().st_size,
        "chunks": len(chunks),
        "mode": mode
    }


@router.put("/objects/{bucket}/{key:path}", status_code=201)
async def upload_object(
    bucket: str,
    key: str,
    request: Request,
    tags: str = Query("{}")
):
    if not key:
        raise HTTPException(400, "Object key cannot be empty")

    try:
        parsed_tags = json.loads(tags)
        if not isinstance(parsed_tags, dict) or not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in parsed_tags.items()
        ):
            raise ValueError()
    except (ValueError, TypeError):
        raise HTTPException(400, "Tags must be a JSON object of strings")

    # Stream request body to a temporary file.
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_path = temp.name
            async for block in request.stream():
                temp.write(block)

        return await asyncio.to_thread(
            store_file, temp_path, bucket, key, parsed_tags
        )
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def get_object(bucket, key, version=None):
    with httpx.Client(timeout=30) as client:
        params = {"version": version} if version else {}
        return metadata_request(
            client, "GET",
            f"/objects/metadata/{bucket}/{key}",
            params=params
        )


def read_replicated_chunk(client, digest, size):
    locations = metadata_request(
        client, "GET", f"/chunks/locations/{digest}"
    )["nodes"]

    valid = []
    data = None

    for node in locations:
        if node not in NODES:
            continue
        try:
            r = client.get(f"{NODES[node]}/chunks/{digest}")
            r.raise_for_status()

            if (
                len(r.content) == size
                and hashlib.sha256(r.content).hexdigest() == digest
            ):
                valid.append(node)
                data = r.content
                if len(valid) >= R:
                    return data
        except httpx.HTTPError:
            pass

    raise RuntimeError(f"Read quorum failed for {digest[:12]}")


def read_erasure_chunk(client, obj, position, digest, size):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT shard_index, node_id, shard_hash,
                       shard_size, padlen
                FROM erasure_shards
                WHERE bucket=%s AND object_key=%s
                  AND version=%s AND position=%s
                ORDER BY shard_index
            """, (
                obj["bucket"], obj["object_key"],
                obj["version"], position
            ))
            rows = cur.fetchall()

    blocks = []
    indices = []

    for index, node, shard_hash, shard_size, padlen in rows:
        try:
            r = client.get(f"{NODES[node]}/chunks/{shard_hash}")
            r.raise_for_status()
            if (
                len(r.content) != shard_size
                or hashlib.sha256(r.content).hexdigest() != shard_hash
            ):
                continue
            blocks.append(r.content)
            indices.append(index)

            if len(blocks) == K:
                recovered = decoder.decode(blocks, indices, padlen)
                if (
                    len(recovered) != size
                    or hashlib.sha256(recovered).hexdigest() != digest
                ):
                    raise RuntimeError("Reconstructed chunk corrupted")
                return recovered
        except httpx.HTTPError:
            pass

    raise RuntimeError("Insufficient valid erasure shards")


def chunk_stream(obj, start, end):
    mode = obj["tags"].get("redundancy", "replication")
    offset = 0

    with httpx.Client(timeout=30) as client:
        for position, chunk in enumerate(obj["chunks"]):
            chunk_start = offset
            chunk_end = offset + chunk["size"] - 1
            offset += chunk["size"]

            if chunk_end < start or chunk_start > end:
                continue

            if mode == "erasure":
                data = read_erasure_chunk(
                    client, obj, position,
                    chunk["hash"], chunk["size"]
                )
            else:
                data = read_replicated_chunk(
                    client, chunk["hash"], chunk["size"]
                )

            left = max(0, start - chunk_start)
            right = min(len(data), end - chunk_start + 1)
            yield data[left:right]


def parse_range(value, size):
    if not value:
        return 0, size - 1, 200

    import re
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value.strip())

    if not match or (not match[1] and not match[2]):
        raise HTTPException(
            416, "Invalid byte range",
            headers={"Content-Range": f"bytes */{size}"}
        )

    if not match[1]:
        length = int(match[2])
        start = max(0, size - length)
        end = size - 1
        if length == 0:
            raise HTTPException(416, "Invalid suffix range")
    else:
        start = int(match[1])
        end = int(match[2]) if match[2] else size - 1
        end = min(end, size - 1)

    if start > end or start >= size:
        raise HTTPException(
            416, "Unsatisfiable byte range",
            headers={"Content-Range": f"bytes */{size}"}
        )

    return start, end, 206


@router.get("/object-metadata/{bucket}/{key:path}")
def object_metadata(bucket: str, key: str, version: int | None = None):
    return get_object(bucket, key, version)


@router.get("/objects/{bucket}/{key:path}")
def download_object(
    bucket: str,
    key: str,
    request: Request,
    version: int | None = None
):
    obj = get_object(bucket, key, version)
    size = obj["size"]

    if size == 0:
        if request.headers.get("range"):
            raise HTTPException(416, "Empty object")
        return Response(content=b"", media_type="application/octet-stream")

    start, end, status = parse_range(
        request.headers.get("range"), size
    )

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1)
    }

    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    return StreamingResponse(
        chunk_stream(obj, start, end),
        status_code=status,
        media_type="application/octet-stream",
        headers=headers
    )


@router.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str, version: int | None = None):
    obj = get_object(bucket, key, version)

    with httpx.Client(timeout=30) as client:
        result = metadata_request(
            client, "DELETE",
            f"/objects/metadata/{bucket}/{key}"
            f"/versions/{obj['version']}"
        )

    return {
        "deleted": True,
        "version": obj["version"],
        "unreferenced_chunks": result["unreferenced_chunks"]
    }


@router.get("/search")
def search_objects(
    bucket: str | None = None,
    prefix: str = "",
    min_size: int | None = None,
    max_size: int | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    tag_key: str | None = None,
    tag_value: str | None = None
):
    sql = """
        SELECT bucket, object_key, version, size, tags, created_at
        FROM objects WHERE TRUE
    """
    values = []

    if bucket:
        sql += " AND bucket=%s"
        values.append(bucket)
    if prefix:
        sql += " AND LEFT(object_key, LENGTH(%s))=%s"
        values.extend([prefix, prefix])
    if min_size is not None:
        sql += " AND size >= %s"
        values.append(min_size)
    if max_size is not None:
        sql += " AND size <= %s"
        values.append(max_size)
    if created_after:
        sql += " AND created_at >= %s::timestamptz"
        values.append(created_after)
    if created_before:
        sql += " AND created_at <= %s::timestamptz"
        values.append(created_before)
    if tag_key and tag_value is not None:
        sql += " AND tags->>%s = %s"
        values.extend([tag_key, tag_value])

    sql += " ORDER BY created_at DESC LIMIT 500"

    try:
        with db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, values)
                return cur.fetchall()
    except psycopg2.Error:
        raise HTTPException(400, "Invalid search parameters")


@router.get("/objects")
def list_objects(bucket: str | None = None, prefix: str = ""):
    return search_objects(bucket=bucket, prefix=prefix)


@router.get("/cluster/status")
def cluster_status():
    with httpx.Client(timeout=15) as client:
        nodes = metadata_request(client, "GET", "/nodes")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.bucket, o.object_key, o.version,
                       o.tags->>'redundancy' AS mode,
                       oc.position,
                       COUNT(DISTINCT cl.node_id)
                           FILTER (WHERE n.last_seen >
                               NOW() - INTERVAL '15 seconds')
                FROM objects o
                JOIN object_chunks oc USING
                    (bucket, object_key, version)
                LEFT JOIN chunk_locations cl
                    ON cl.chunk_hash=oc.chunk_hash
                LEFT JOIN storage_nodes n ON n.node_id=cl.node_id
                WHERE o.tags->>'redundancy'='replication'
                GROUP BY o.bucket,o.object_key,o.version,
                         mode,oc.position
            """)
            replicas = cur.fetchall()

            cur.execute("""
                SELECT e.bucket,e.object_key,e.version,
                       e.position,
                       COUNT(*) FILTER (WHERE n.last_seen >
                           NOW() - INTERVAL '15 seconds')
                FROM erasure_shards e
                LEFT JOIN storage_nodes n ON n.node_id=e.node_id
                GROUP BY e.bucket,e.object_key,e.version,e.position
            """)
            shards = cur.fetchall()

    degraded = set()
    inaccessible = set()

    for bucket, key, version, mode, position, count in replicas:
        identity = (bucket, key, version)
        if count < N:
            degraded.add(identity)
        if count < R:
            inaccessible.add(identity)

    for bucket, key, version, position, count in shards:
        identity = (bucket, key, version)
        if count < K + M:
            degraded.add(identity)
        if count < K:
            inaccessible.add(identity)

    return {
        "nodes": nodes,
        "online_nodes": sum(n["online"] for n in nodes),
        "total_free_space": sum(
            n["free_space"] for n in nodes if n["online"]
        ),
        "degraded_objects": len(degraded),
        "inaccessible_objects": len(inaccessible),
        "repair_worker": "see repair-worker logs"
    }


@app.get("/health")
def health():
    return {"status": "healthy", "service": "gateway"}


app.include_router(router)
