
import os
import re
from collections import Counter

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

app = FastAPI(title="Distributed Storage Metadata Service")

DATABASE_URL = os.environ["DATABASE_URL"]


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
                       (last_seen > NOW() - INTERVAL '15 seconds')
                           AS online
                FROM storage_nodes
                ORDER BY node_id
            """)
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
                ORDER BY c.chunk_hash
            """)
            return cur.fetchall()


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
