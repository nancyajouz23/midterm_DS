
import os
import re
import time
from collections import Counter

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client.core import (
    CollectorRegistry, CounterMetricFamily, GaugeMetricFamily
)
from pydantic import BaseModel, Field

app = FastAPI(title="Distributed Storage Metadata Service")

DATABASE_URL = os.environ["DATABASE_URL"]

# Nodes send a heartbeat every 5 s; a node that misses three in a row
# (no heartbeat for 15 s) is considered failed.
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))


def connect():
    return psycopg2.connect(DATABASE_URL, connect_timeout=5)


# -------------------------
# Database initialization
# -------------------------

@app.on_event("startup")
def initialize_database():
    # The database may still be electing a leader (replicated
    # PostgreSQL), so retry for a while instead of crashing.
    for attempt in range(60):
        try:
            create_schema()
            return
        except psycopg2.Error as exc:
            print(f"Database not ready ({exc}); retrying", flush=True)
            time.sleep(2)
    create_schema()


def create_schema():
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
            # url: where the node can be reached (sent in heartbeats),
            # so nodes can join without changing any configuration.
            # rebalanced: FALSE for a node that just joined until the
            # scheduler has moved its share of the data onto it.
            cur.execute("""
                ALTER TABLE storage_nodes
                    ADD COLUMN IF NOT EXISTS url TEXT,
                    ADD COLUMN IF NOT EXISTS first_seen
                        TIMESTAMPTZ DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS rebalanced
                        BOOLEAN NOT NULL DEFAULT TRUE
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
            # repair (failure / scrub) or rebalance (node joined).
            cur.execute("""
                ALTER TABLE repair_jobs
                    ADD COLUMN IF NOT EXISTS job_type
                        TEXT NOT NULL DEFAULT 'repair'
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
    url: str | None = None


@app.get("/health")
def health():
    return {"status": "healthy", "service": "metadata"}


@app.post("/heartbeats")
def receive_heartbeat(data: Heartbeat):
    with connect() as conn:
        with conn.cursor() as cur:
            # A node seen for the first time starts with
            # rebalanced = FALSE, which makes the scheduler move its
            # share of the existing data onto it.
            cur.execute("""
                INSERT INTO storage_nodes
                    (node_id, free_space, chunk_count, last_seen,
                     url, first_seen, rebalanced)
                VALUES (%s, %s, %s, NOW(), %s, NOW(), FALSE)
                ON CONFLICT (node_id)
                DO UPDATE SET
                    free_space = EXCLUDED.free_space,
                    chunk_count = EXCLUDED.chunk_count,
                    url = COALESCE(EXCLUDED.url, storage_nodes.url),
                    last_seen = NOW()
            """, (
                data.node_id,
                data.free_space,
                data.chunk_count,
                data.url
            ))

    return {"message": "Heartbeat received", "node_id": data.node_id}


def fetch_nodes():
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT node_id, url, free_space, chunk_count,
                       last_seen, first_seen, rebalanced,
                       (last_seen > NOW() - make_interval(secs => %s))
                           AS online,
                       EXTRACT(EPOCH FROM NOW() - last_seen)::int
                           AS seconds_since_heartbeat
                FROM storage_nodes
                ORDER BY node_id
            """, (HEARTBEAT_TIMEOUT,))
            return cur.fetchall()


@app.get("/nodes")
def list_nodes():
    return fetch_nodes()


@app.post("/nodes/{node_id}/rebalanced")
def mark_node_rebalanced(node_id: str):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE storage_nodes SET rebalanced = TRUE "
                "WHERE node_id = %s",
                (node_id,)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Unknown node")
    return {"node_id": node_id, "rebalanced": True}


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


@app.get("/chunks/placements")
def replicated_placements(
    nodes: list[str] | None = Query(default=None),
    chunk_hash: str | None = None
):
    """
    Replicated chunks with the nodes holding their copies.
    nodes=...      only chunks with a copy on one of these nodes
                   (used when those nodes failed),
    chunk_hash=... a single chunk (fresh state for a worker).
    """
    sql = """
        SELECT c.chunk_hash, c.size,
               COALESCE(
                   array_agg(cl.node_id ORDER BY cl.node_id)
                       FILTER (WHERE cl.node_id IS NOT NULL),
                   '{}'
               ) AS nodes
        FROM chunks c
        LEFT JOIN chunk_locations cl ON cl.chunk_hash = c.chunk_hash
        WHERE c.ref_count > 0
          AND EXISTS (
              SELECT 1 FROM object_chunks oc
              JOIN objects o
                ON o.bucket = oc.bucket
               AND o.object_key = oc.object_key
               AND o.version = oc.version
              WHERE oc.chunk_hash = c.chunk_hash
                AND o.tags->>'redundancy' = 'replication'
                AND (%s OR o.created_at < NOW() - INTERVAL '30 seconds')
          )
    """
    values = [chunk_hash is not None]
    if chunk_hash:
        sql += " AND c.chunk_hash = %s"
        values.append(chunk_hash)
    sql += " GROUP BY c.chunk_hash, c.size"
    if nodes:
        sql += " HAVING bool_or(cl.node_id = ANY(%s))"
        values.append(nodes)
    sql += " ORDER BY c.chunk_hash"

    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, values)
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
def erasure_chunks(
    nodes: list[str] | None = Query(default=None),
    chunk_hash: str | None = None
):
    """
    Every distinct erasure-coded chunk with the current placement of
    its shards, for scrubbing, repair and rebalancing.
    nodes=...      only chunks with a shard on one of these nodes,
    chunk_hash=... a single chunk (fresh state for a worker).
    """
    sql = """
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
    """
    values = []
    if chunk_hash:
        sql += " AND oc.chunk_hash = %s"
        values.append(chunk_hash)
    else:
        # skip uploads that are still being registered
        sql += " AND o.created_at < NOW() - INTERVAL '30 seconds'"
    sql += " ORDER BY oc.chunk_hash, e.shard_index"

    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, values)
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

    result = list(chunks.values())
    if nodes:
        wanted = set(nodes)
        result = [
            chunk for chunk in result
            if any(s["node_id"] in wanted for s in chunk["shards"])
        ]
    return result


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
    job_type: str = Field(default="repair", pattern="^(repair|rebalance)$")


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
                    (reason, failed_nodes, chunks_total, job_type)
                VALUES (%s, %s, %s, %s)
                RETURNING job_id
            """, (
                data.reason, data.failed_nodes,
                data.chunks_total, data.job_type
            ))
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

            # Jobs are processed by several queue workers in parallel;
            # whichever worker finishes the last chunk closes the job.
            cur.execute("""
                UPDATE repair_jobs SET
                    status = CASE WHEN chunks_failed > 0
                                  THEN 'partial' ELSE 'completed' END,
                    finished_at = NOW()
                WHERE job_id = %s
                  AND status = 'running'
                  AND chunks_repaired + chunks_failed >= chunks_total
                RETURNING status
            """, (job_id,))
            closed = cur.fetchone()

    return {"job_id": job_id, "closed": closed[0] if closed else None}


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
def list_repair_jobs(
    limit: int = Query(default=20, ge=1, le=200),
    status: str | None = None
):
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute("""
                    SELECT * FROM repair_jobs WHERE status = %s
                    ORDER BY job_id DESC LIMIT %s
                """, (status, limit))
            else:
                cur.execute("""
                    SELECT * FROM repair_jobs
                    ORDER BY job_id DESC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()


# -------------------------
# Monitoring (Prometheus)
# -------------------------

@app.get("/prometheus/targets")
def prometheus_targets():
    """
    Prometheus HTTP service discovery: every registered storage node,
    so a node that joins is scraped automatically.
    """
    targets = []
    for node in fetch_nodes():
        if not node["url"]:
            continue
        address = node["url"].split("://", 1)[-1].rstrip("/")
        targets.append({
            "targets": [address],
            "labels": {"node_id": node["node_id"]}
        })
    return targets


class StorageCollector:
    """Cluster-wide metrics read from the metadata database."""

    def collect(self):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT job_type,
                           COALESCE(SUM(chunks_repaired), 0),
                           COALESCE(SUM(chunks_failed), 0),
                           COALESCE(SUM(bytes_read + bytes_written), 0),
                           COUNT(*) FILTER (WHERE status = 'running')
                    FROM repair_jobs GROUP BY job_type
                """)
                jobs = cur.fetchall()

                cur.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) "
                            "FROM objects")
                objects, logical = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM chunks "
                            "WHERE ref_count > 0")
                (chunks,) = cur.fetchone()

        nodes = fetch_nodes()

        chunks_done = CounterMetricFamily(
            "storage_jobs_chunks",
            "Chunks processed by repair/rebalance jobs",
            labels=["job_type", "result"]
        )
        traffic = CounterMetricFamily(
            "storage_jobs_traffic_bytes",
            "Bytes read + written by repair/rebalance jobs",
            labels=["job_type"]
        )
        running = GaugeMetricFamily(
            "storage_jobs_running", "Jobs in progress", labels=["job_type"]
        )
        for job_type, repaired, failed, moved, active in jobs:
            chunks_done.add_metric([job_type, "ok"], repaired)
            chunks_done.add_metric([job_type, "failed"], failed)
            traffic.add_metric([job_type], moved)
            running.add_metric([job_type], active)

        yield chunks_done
        yield traffic
        yield running
        yield GaugeMetricFamily(
            "storage_objects", "Stored object versions", value=objects
        )
        yield GaugeMetricFamily(
            "storage_logical_bytes", "Bytes stored by users", value=logical
        )
        yield GaugeMetricFamily(
            "storage_unique_chunks", "Chunks after deduplication",
            value=chunks
        )
        yield GaugeMetricFamily(
            "storage_nodes_online", "Nodes with a recent heartbeat",
            value=sum(1 for n in nodes if n["online"])
        )
        yield GaugeMetricFamily(
            "storage_nodes_registered", "Nodes that ever registered",
            value=len(nodes)
        )


registry = CollectorRegistry()
registry.register(StorageCollector())


@app.get("/metrics")
def metrics():
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
