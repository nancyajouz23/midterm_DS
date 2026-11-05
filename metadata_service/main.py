
import os
import re
from collections import Counter

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

app = FastAPI(title="Distributed Storage Metadata Service")

DATABASE_URL = os.environ["DATABASE_URL"]

# Nodes send a heartbeat every 5 s; a node that misses three in a row
# (no heartbeat for 15 s) is considered failed.
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))


def connect():
    return psycopg2.connect(DATABASE_URL)


# -------------------------
# Database initialization
# -------------------------

@app.on_event("startup")
def initialize_database():
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS storage_nodes (
                    node_id VARCHAR(100) PRIMARY KEY,
                    free_space BIGINT NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    last_seen TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS buckets (
                    name TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_hash VARCHAR(64) PRIMARY KEY,
                    size BIGINT NOT NULL,
                    ref_count BIGINT NOT NULL DEFAULT 0
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    bucket TEXT NOT NULL REFERENCES buckets(name),
                    object_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    size BIGINT NOT NULL,
                    tags JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (bucket, object_key, version)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS object_chunks (
                    bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    chunk_hash VARCHAR(64) NOT NULL
                        REFERENCES chunks(chunk_hash),
                    PRIMARY KEY (
                        bucket, object_key, version, position
                    ),
                    FOREIGN KEY (bucket, object_key, version)
                        REFERENCES objects(bucket, object_key, version)
                        ON DELETE CASCADE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS chunk_locations (
                    chunk_hash VARCHAR(64) NOT NULL
                        REFERENCES chunks(chunk_hash)
                        ON DELETE CASCADE,
                    node_id VARCHAR(100) NOT NULL
                        REFERENCES storage_nodes(node_id),
                    PRIMARY KEY (chunk_hash, node_id)
                )
            """)

            # Erasure-coded shard placement (written by the gateway,
            # relocated by the repair worker).
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

            # Repair jobs: one per repair round that found damage,
            # with progress counters and repair traffic.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repair_jobs (
                    job_id SERIAL PRIMARY KEY,
                    reason TEXT NOT NULL,
                    failed_nodes TEXT[] NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'running',
                    chunks_total INTEGER NOT NULL DEFAULT 0,
                    chunks_repaired INTEGER NOT NULL DEFAULT 0,
                    chunks_failed INTEGER NOT NULL DEFAULT 0,
                    bytes_read BIGINT NOT NULL DEFAULT 0,
                    bytes_written BIGINT NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ DEFAULT NOW(),
                    finished_at TIMESTAMPTZ
                )
            """)

            # Per-chunk repair traffic, used to compare replication
            # and erasure coding per bucket.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS repair_events (
                    event_id BIGSERIAL PRIMARY KEY,
                    job_id INTEGER REFERENCES repair_jobs(job_id)
                        ON DELETE CASCADE,
                    chunk_hash VARCHAR(64) NOT NULL,
                    mode TEXT NOT NULL,
                    target_node TEXT NOT NULL,
                    bytes_read BIGINT NOT NULL,
                    bytes_written BIGINT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)



# -------------------------
# Node health and heartbeats
# -------------------------

class Heartbeat(BaseModel):
    node_id: str
    free_space: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


@app.get("/health")
def health():
    return {"status": "healthy", "service": "metadata"}


@app.post("/heartbeats")
def receive_heartbeat(data: Heartbeat):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO storage_nodes
                    (node_id, free_space, chunk_count, last_seen)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (node_id)
                DO UPDATE SET
                    free_space = EXCLUDED.free_space,
                    chunk_count = EXCLUDED.chunk_count,
                    last_seen = NOW()
            """, (
                data.node_id,
                data.free_space,
                data.chunk_count
            ))

    return {"message": "Heartbeat received", "node_id": data.node_id}


@app.get("/nodes")
def list_nodes():
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT node_id, free_space, chunk_count,
                       last_seen,
                       (last_seen > NOW() - make_interval(secs => %s))
                           AS online,
                       EXTRACT(EPOCH FROM NOW() - last_seen)::int
                           AS seconds_since_heartbeat
                FROM storage_nodes
                ORDER BY node_id
            """, (HEARTBEAT_TIMEOUT,))
            return cur.fetchall()


# -------------------------
# Buckets
# -------------------------

class BucketRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


@app.post("/buckets", status_code=201)
def create_bucket(data: BucketRequest):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO buckets (name)
                VALUES (%s)
                ON CONFLICT DO NOTHING
            """, (data.name,))
            created = cur.rowcount == 1

    return {"bucket": data.name, "created": created}


@app.get("/buckets")
def list_buckets():
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT name, created_at
                FROM buckets ORDER BY name
            """)
            return cur.fetchall()


# -------------------------
# Object metadata
# -------------------------

class ChunkInfo(BaseModel):
    hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    size: int = Field(ge=0)


class ObjectRequest(BaseModel):
    bucket: str
    key: str = Field(min_length=1)
    size: int = Field(ge=0)
    chunks: list[ChunkInfo]
    tags: dict[str, str] = Field(default_factory=dict)


@app.post("/objects/metadata", status_code=201)
def register_object(data: ObjectRequest):
    if sum(chunk.size for chunk in data.chunks) != data.size:
        raise HTTPException(
            status_code=400,
            detail="Object size does not match chunk sizes"
        )

    with connect() as conn:
        with conn.cursor() as cur:
            # Serialize version allocation per bucket.
            cur.execute(
                "SELECT name FROM buckets WHERE name = %s FOR UPDATE",
                (data.bucket,)
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail="Bucket not found"
                )

            cur.execute("""
                SELECT COALESCE(MAX(version), 0) + 1
                FROM objects
                WHERE bucket = %s AND object_key = %s
            """, (data.bucket, data.key))
            version = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO objects
                    (bucket, object_key, version, size, tags)
                VALUES (%s, %s, %s, %s, %s::jsonb)
            """, (
                data.bucket,
                data.key,
                version,
                data.size,
                __import__("json").dumps(data.tags)
            ))

            for position, chunk in enumerate(data.chunks):
                cur.execute("""
                    INSERT INTO chunks (chunk_hash, size, ref_count)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (chunk_hash)
                    DO UPDATE SET
                        ref_count = chunks.ref_count + 1
                    RETURNING size
                """, (chunk.hash, chunk.size))

                stored_size = cur.fetchone()[0]
                if stored_size != chunk.size:
                    raise HTTPException(
                        status_code=409,
                        detail="Conflicting size for an existing chunk"
                    )

                cur.execute("""
                    INSERT INTO object_chunks
                        (bucket, object_key, version,
                         position, chunk_hash)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    data.bucket, data.key, version,
                    position, chunk.hash
                ))

    return {
        "message": "Object metadata saved",
        "bucket": data.bucket,
        "key": data.key,
        "version": version,
        "chunk_count": len(data.chunks)
    }


@app.get("/objects/metadata/{bucket}/{key}")
def get_object_metadata(
    bucket: str,
    key: str,
    version: int | None = Query(default=None, ge=1)
):
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if version is None:
                cur.execute("""
                    SELECT *
                    FROM objects
                    WHERE bucket = %s AND object_key = %s
                    ORDER BY version DESC
                    LIMIT 1
                """, (bucket, key))
            else:
                cur.execute("""
                    SELECT *
                    FROM objects
                    WHERE bucket = %s
                      AND object_key = %s
                      AND version = %s
                """, (bucket, key, version))

            obj = cur.fetchone()
            if obj is None:
                raise HTTPException(404, "Object not found")

            cur.execute("""
                SELECT oc.position,
                       oc.chunk_hash AS hash,
                       c.size
                FROM object_chunks oc
                JOIN chunks c
                  ON c.chunk_hash = oc.chunk_hash
                WHERE oc.bucket = %s
                  AND oc.object_key = %s
                  AND oc.version = %s
                ORDER BY oc.position
            """, (bucket, key, obj["version"]))

            obj["chunks"] = cur.fetchall()
            return obj


# -------------------------
# Deduplication monitoring
# -------------------------

@app.get("/chunks/references/{chunk_hash}")
def get_chunk_references(chunk_hash: str):
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")

    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT chunk_hash, size, ref_count
                FROM chunks
                WHERE chunk_hash = %s
            """, (chunk_hash,))

            result = cur.fetchone()
            if result is None:
                raise HTTPException(404, "Chunk not registered")

            return result


# -------------------------
# Delete an object version
# -------------------------

@app.delete("/objects/metadata/{bucket}/{key}/versions/{version}")
def delete_object_metadata(bucket: str, key: str, version: int):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT version FROM objects
                WHERE bucket = %s AND object_key = %s
                  AND version = %s
                FOR UPDATE
            """, (bucket, key, version))

            if cur.fetchone() is None:
                raise HTTPException(404, "Object version not found")

            cur.execute("""
                SELECT chunk_hash
                FROM object_chunks
                WHERE bucket = %s AND object_key = %s
                  AND version = %s
            """, (bucket, key, version))

            counts = Counter(row[0] for row in cur.fetchall())

            cur.execute("""
                DELETE FROM objects
                WHERE bucket = %s AND object_key = %s
                  AND version = %s
            """, (bucket, key, version))

            orphaned = []

            for chunk_hash, count in counts.items():
                cur.execute("""
                    UPDATE chunks
                    SET ref_count = ref_count - %s
                    WHERE chunk_hash = %s
                      AND ref_count >= %s
                    RETURNING ref_count
                """, (count, chunk_hash, count))

                row = cur.fetchone()
                if row is None:
                    raise HTTPException(
                        409, "Inconsistent chunk reference count"
                    )

                if row[0] == 0:
                    orphaned.append(chunk_hash)

    return {
        "message": "Object metadata deleted",
        "bucket": bucket,
        "key": key,
        "version": version,
        "unreferenced_chunks": orphaned
    }


# -------------------------
# Physical chunk locations
# -------------------------

class ChunkLocationRequest(BaseModel):
    chunk_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    node_id: str = Field(min_length=1)


@app.post("/chunks/locations", status_code=201)
def register_chunk_location(data: ChunkLocationRequest):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM chunks WHERE chunk_hash = %s",
                (data.chunk_hash,)
            )

            if cur.fetchone() is None:
                raise HTTPException(404, "Chunk metadata not found")

            cur.execute(
                "SELECT 1 FROM storage_nodes WHERE node_id = %s",
                (data.node_id,)
            )

            if cur.fetchone() is None:
                raise HTTPException(404, "Storage node not registered")

            cur.execute("""
                INSERT INTO chunk_locations (chunk_hash, node_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (data.chunk_hash, data.node_id))

    return {
        "message": "Chunk location registered",
        "chunk_hash": data.chunk_hash,
        "node_id": data.node_id
    }


@app.get("/chunks/locations/{chunk_hash}")
def get_chunk_locations(chunk_hash: str):
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT node_id
                FROM chunk_locations
                WHERE chunk_hash = %s
                ORDER BY node_id
            """, (chunk_hash,))

            nodes = [row[0] for row in cur.fetchall()]

    return {"chunk_hash": chunk_hash, "nodes": nodes}


@app.get("/chunks/repair-candidates")
def repair_candidates():
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT c.chunk_hash, c.size
                FROM chunks c
                JOIN object_chunks oc
                    ON oc.chunk_hash = c.chunk_hash
                JOIN objects o
                    ON o.bucket = oc.bucket
                    AND o.object_key = oc.object_key
                    AND o.version = oc.version
                WHERE c.ref_count > 0
                  AND o.tags->>'redundancy' = 'replication'
                  -- skip uploads still registering their locations
                  AND o.created_at < NOW() - INTERVAL '30 seconds'
                ORDER BY c.chunk_hash
            """)
            return cur.fetchall()


@app.delete("/chunks/{chunk_hash}")
def purge_chunk(chunk_hash: str):
    """
    Garbage-collect a chunk whose reference count reached zero.

    The row is removed only if no object references it any more
    (ref_count = 0). Its location records are returned so the caller
    can delete the physical copies from the storage nodes.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ref_count FROM chunks
                WHERE chunk_hash = %s
                FOR UPDATE
            """, (chunk_hash,))
            row = cur.fetchone()

            if row is None:
                return {"chunk_hash": chunk_hash, "purged": False,
                        "reason": "not registered", "nodes": []}

            if row[0] > 0:
                return {"chunk_hash": chunk_hash, "purged": False,
                        "reason": "still referenced", "nodes": []}

            cur.execute("""
                SELECT node_id FROM chunk_locations
                WHERE chunk_hash = %s
                ORDER BY node_id
            """, (chunk_hash,))
            nodes = [r[0] for r in cur.fetchall()]

            # chunk_locations rows are removed by ON DELETE CASCADE.
            cur.execute(
                "DELETE FROM chunks WHERE chunk_hash = %s AND ref_count = 0",
                (chunk_hash,)
            )

    return {"chunk_hash": chunk_hash, "purged": True, "nodes": nodes}


@app.delete("/chunks/locations/{chunk_hash}/{node_id}")
def unregister_chunk_location(chunk_hash: str, node_id: str):
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM chunk_locations
                WHERE chunk_hash = %s AND node_id = %s
            """, (chunk_hash, node_id))
            deleted = cur.rowcount

    return {"deleted": deleted > 0}


# -------------------------
# Erasure-coded shards (repair worker)
# -------------------------

@app.get("/erasure/chunks")
def erasure_chunks():
    """
    Every distinct erasure-coded chunk with the current placement of
    its shards, for scrubbing and repair.
    """
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT oc.chunk_hash, c.size, e.padlen,
                       e.shard_index, e.shard_hash,
                       e.shard_size, e.node_id
                FROM erasure_shards e
                JOIN object_chunks oc
                  ON oc.bucket = e.bucket
                 AND oc.object_key = e.object_key
                 AND oc.version = e.version
                 AND oc.position = e.position
                JOIN chunks c ON c.chunk_hash = oc.chunk_hash
                JOIN objects o
                  ON o.bucket = e.bucket
                 AND o.object_key = e.object_key
                 AND o.version = e.version
                WHERE c.ref_count > 0
                  AND o.created_at < NOW() - INTERVAL '30 seconds'
                ORDER BY oc.chunk_hash, e.shard_index
            """)
            rows = cur.fetchall()

    chunks = {}
    for row in rows:
        chunk = chunks.setdefault(row["chunk_hash"], {
            "chunk_hash": row["chunk_hash"],
            "size": row["size"],
            "padlen": row["padlen"],
            "shards": []
        })
        chunk["shards"].append({
            "shard_index": row["shard_index"],
            "shard_hash": row["shard_hash"],
            "shard_size": row["shard_size"],
            "node_id": row["node_id"]
        })

    return list(chunks.values())


class ShardRelocation(BaseModel):
    shard_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    old_node: str = Field(min_length=1)
    new_node: str = Field(min_length=1)


@app.post("/erasure/shards/relocate")
def relocate_shard(data: ShardRelocation):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE erasure_shards
                SET node_id = %s
                WHERE shard_hash = %s AND node_id = %s
            """, (data.new_node, data.shard_hash, data.old_node))
            updated = cur.rowcount

    return {"updated_rows": updated}


# -------------------------
# Repair jobs (progress + repair traffic)
# -------------------------

class RepairJobRequest(BaseModel):
    reason: str = Field(min_length=1)
    failed_nodes: list[str] = Field(default_factory=list)
    chunks_total: int = Field(ge=0)


class RepairJobUpdate(BaseModel):
    status: str | None = Field(
        default=None, pattern="^(running|completed|partial)$"
    )
    chunks_repaired: int = Field(default=0, ge=0)
    chunks_failed: int = Field(default=0, ge=0)


class RepairEvent(BaseModel):
    chunk_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: str = Field(pattern="^(replication|erasure)$")
    target_node: str
    bytes_read: int = Field(ge=0)
    bytes_written: int = Field(ge=0)


@app.post("/repair-jobs", status_code=201)
def create_repair_job(data: RepairJobRequest):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO repair_jobs
                    (reason, failed_nodes, chunks_total)
                VALUES (%s, %s, %s)
                RETURNING job_id
            """, (data.reason, data.failed_nodes, data.chunks_total))
            job_id = cur.fetchone()[0]

    return {"job_id": job_id}


@app.patch("/repair-jobs/{job_id}")
def update_repair_job(job_id: int, data: RepairJobUpdate):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE repair_jobs SET
                    chunks_repaired = chunks_repaired + %s,
                    chunks_failed = chunks_failed + %s,
                    status = COALESCE(%s, status),
                    finished_at = CASE WHEN %s IS NULL
                                       THEN finished_at ELSE NOW() END
                WHERE job_id = %s
            """, (
                data.chunks_repaired, data.chunks_failed,
                data.status,
                None if data.status in (None, "running") else data.status,
                job_id
            ))
            if cur.rowcount == 0:
                raise HTTPException(404, "Repair job not found")

    return {"job_id": job_id}


@app.post("/repair-jobs/{job_id}/events", status_code=201)
def add_repair_event(job_id: int, data: RepairEvent):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO repair_events
                    (job_id, chunk_hash, mode, target_node,
                     bytes_read, bytes_written)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                job_id, data.chunk_hash, data.mode,
                data.target_node, data.bytes_read, data.bytes_written
            ))
            cur.execute("""
                UPDATE repair_jobs SET
                    bytes_read = bytes_read + %s,
                    bytes_written = bytes_written + %s
                WHERE job_id = %s
            """, (data.bytes_read, data.bytes_written, job_id))

    return {"job_id": job_id}


@app.get("/repair-jobs")
def list_repair_jobs(limit: int = Query(default=20, ge=1, le=200)):
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM repair_jobs
                ORDER BY job_id DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
