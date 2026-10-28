
import hashlib
import json
import os

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
DATABASE_URL = os.environ["DATABASE_URL"]
INDEX = "ds-objects"


def document_id(bucket, key, version):
    value = f"{bucket}\0{key}\0{version}"
    return hashlib.sha256(value.encode()).hexdigest()


def ensure_index(client):
    response = client.head(f"{ES_URL}/{INDEX}")
    if response.status_code == 404:
        response = client.put(
            f"{ES_URL}/{INDEX}",
            json={
                "mappings": {
                    "properties": {
                        "bucket": {"type": "keyword"},
                        "object_key": {"type": "keyword"},
                        "version": {"type": "integer"},
                        "size": {"type": "long"},
                        "created_at": {"type": "date"},
                        "tags": {"type": "flattened"}
                    }
                }
            }
        )
        response.raise_for_status()
    else:
        response.raise_for_status()


def serialize_object(obj):
    return {
        "bucket": obj["bucket"],
        "object_key": obj["object_key"],
        "version": obj["version"],
        "size": obj["size"],
        "tags": obj["tags"],
        "created_at": obj["created_at"].isoformat()
    }


def index_object(obj):
    with httpx.Client(timeout=30) as client:
        ensure_index(client)
        response = client.put(
            f"{ES_URL}/{INDEX}/_doc/"
            f"{document_id(obj['bucket'], obj['object_key'], obj['version'])}",
            params={"refresh": "wait_for"},
            json=serialize_object(obj)
        )
        response.raise_for_status()


def remove_object(bucket, key, version):
    with httpx.Client(timeout=30) as client:
        response = client.delete(
            f"{ES_URL}/{INDEX}/_doc/{document_id(bucket, key, version)}",
            params={"refresh": "wait_for"}
        )
        if response.status_code != 404:
            response.raise_for_status()


def reindex_all():
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT bucket, object_key, version,
                       size, tags, created_at
                FROM objects
            """)
            objects = cur.fetchall()

    with httpx.Client(timeout=90) as client:
        response = client.delete(f"{ES_URL}/{INDEX}")
        if response.status_code != 404:
            response.raise_for_status()

        ensure_index(client)

        for start in range(0, len(objects), 250):
            batch = objects[start:start + 250]
            lines = []

            for obj in batch:
                lines.append(json.dumps({
                    "index": {
                        "_index": INDEX,
                        "_id": document_id(
                            obj["bucket"],
                            obj["object_key"],
                            obj["version"]
                        )
                    }
                }))
                lines.append(json.dumps(serialize_object(obj)))

            response = client.post(
                f"{ES_URL}/_bulk",
                params={"refresh": "wait_for"},
                content="\n".join(lines) + "\n",
                headers={"Content-Type": "application/x-ndjson"}
            )
            response.raise_for_status()

            if response.json().get("errors"):
                raise RuntimeError("Elasticsearch bulk indexing failed")

    return {"indexed_objects": len(objects)}


def search_index(
    bucket=None, prefix="", min_size=None, max_size=None,
    created_after=None, created_before=None,
    tag_key=None, tag_value=None
):
    filters = []

    if bucket:
        filters.append({"term": {"bucket": bucket}})
    if prefix:
        filters.append({"prefix": {"object_key": prefix}})

    size_range = {}
    if min_size is not None:
        size_range["gte"] = min_size
    if max_size is not None:
        size_range["lte"] = max_size
    if size_range:
        filters.append({"range": {"size": size_range}})

    date_range = {}
    if created_after:
        date_range["gte"] = created_after
    if created_before:
        date_range["lte"] = created_before
    if date_range:
        filters.append({"range": {"created_at": date_range}})

    if tag_key and tag_value is not None:
        filters.append({"term": {f"tags.{tag_key}": tag_value}})

    with httpx.Client(timeout=30) as client:
        response = client.post(
            f"{ES_URL}/{INDEX}/_search",
            json={
                "size": 500,
                "query": {"bool": {"filter": filters}},
                "sort": [{"created_at": "desc"}]
            }
        )
        response.raise_for_status()

        return [
            hit["_source"]
            for hit in response.json()["hits"]["hits"]
        ]
