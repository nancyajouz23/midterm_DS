
import hashlib
import os
import uuid
from collections import Counter
from pathlib import Path

import httpx

from metadata_service.chunking import (
    fixed_size_chunks,
    file_sha256,
)
from metadata_service.placement import ConsistentHashRing


METADATA_URL = "http://localhost:8010"

NODE_URLS = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003",
    "node4": "http://localhost:8004",
}

OUTPUT_DIR = Path("demo_output")
SOURCE = OUTPUT_DIR / "distributed_original.bin"
RESTORED = OUTPUT_DIR / "distributed_restored.bin"

FILE_SIZE_MB = 64


def prepare_file():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE.exists():
        print("Generating 64 MiB sample file...")

        with SOURCE.open("wb") as file:
            for _ in range(FILE_SIZE_MB):
                file.write(os.urandom(1024 * 1024))


def get_online_nodes(client):
    response = client.get(f"{METADATA_URL}/nodes")
    response.raise_for_status()

    nodes = sorted(
        node["node_id"]
        for node in response.json()
        if node["online"] and node["node_id"] in NODE_URLS
    )

    if len(nodes) < 4:
        raise RuntimeError(
            f"Expected four online storage nodes, found: {nodes}"
        )

    return nodes


def upload_distributed(client, ring):
    print("\n=== DISTRIBUTED CHUNK UPLOAD ===")

    chunks = []
    placement = {}
    distribution = Counter()

    for index, (chunk_hash, data) in enumerate(
        fixed_size_chunks(SOURCE),
        start=1
    ):
        # Choose target using consistent hashing
        node_id = ring.get_primary_node(chunk_hash)
        node_url = NODE_URLS[node_id]

        # Upload actual bytes to the selected node
        response = client.put(
            f"{node_url}/chunks/{chunk_hash}",
            content=data,
            headers={
                "Content-Type": "application/octet-stream"
            }
        )
        response.raise_for_status()

        chunks.append({
            "hash": chunk_hash,
            "size": len(data)
        })

        placement[chunk_hash] = node_id
        distribution[node_id] += 1

        print(
            f"Chunk {index:02d} "
            f"-> {node_id} "
            f"| {len(data):,} bytes "
            f"| HTTP {response.status_code}"
        )

    return chunks, placement, distribution


def save_metadata(client, chunks, placement):
    print("\n=== POSTGRESQL METADATA ===")

    bucket = f"distributed-test-{uuid.uuid4().hex[:8]}"
    key = "distributed-demo.bin"

    # Create bucket
    response = client.post(
        f"{METADATA_URL}/buckets",
        json={"name": bucket}
    )
    response.raise_for_status()

    # Store ordered object chunk manifest
    response = client.post(
        f"{METADATA_URL}/objects/metadata",
        json={
            "bucket": bucket,
            "key": key,
            "size": SOURCE.stat().st_size,
            "chunks": chunks,
            "tags": {
                "experiment": "consistent-hashing",
                "chunking": "fixed"
            }
        }
    )
    response.raise_for_status()

    object_result = response.json()

    print("Object registered:", object_result)

    # Store the physical location of each unique chunk
    for chunk_hash, node_id in placement.items():
        response = client.post(
            f"{METADATA_URL}/chunks/locations",
            json={
                "chunk_hash": chunk_hash,
                "node_id": node_id
            }
        )
        response.raise_for_status()

    print(f"Chunk locations registered: {len(placement)}")

    return bucket, key


def download_distributed(client, bucket, key):
    print("\n=== DISTRIBUTED FILE DOWNLOAD ===")

    response = client.get(
        f"{METADATA_URL}/objects/metadata/{bucket}/{key}"
    )
    response.raise_for_status()

    metadata = response.json()

    with RESTORED.open("wb") as output:
        for index, chunk in enumerate(metadata["chunks"], start=1):
            chunk_hash = chunk["hash"]

            # Retrieve recorded locations from PostgreSQL
            location_response = client.get(
                f"{METADATA_URL}/chunks/locations/{chunk_hash}"
            )
            location_response.raise_for_status()

            nodes = location_response.json()["nodes"]

            if not nodes:
                raise RuntimeError(
                    f"No registered location for {chunk_hash}"
                )

            node_id = nodes[0]
            node_url = NODE_URLS[node_id]

            # Stream the chunk from its physical storage node
            digest = hashlib.sha256()
            downloaded_size = 0

            with client.stream(
                "GET",
                f"{node_url}/chunks/{chunk_hash}"
            ) as chunk_response:
                chunk_response.raise_for_status()

                for block in chunk_response.iter_bytes():
                    digest.update(block)
                    downloaded_size += len(block)
                    output.write(block)

            if digest.hexdigest() != chunk_hash:
                raise RuntimeError(
                    f"Chunk {index} failed SHA-256 verification"
                )

            if downloaded_size != chunk["size"]:
                raise RuntimeError(
                    f"Chunk {index} has an incorrect size"
                )

            print(
                f"Chunk {index:02d} "
                f"<- {node_id} | Verified"
            )


def main():
    prepare_file()

    with httpx.Client(timeout=90) as client:
        online_nodes = get_online_nodes(client)

        print("=== DISTRIBUTED STORAGE TEST ===")
        print("Online nodes:", online_nodes)

        # Construct ring using the actual online nodes
        ring = ConsistentHashRing(
            nodes=online_nodes,
            virtual_nodes=128
        )

        chunks, placement, distribution = upload_distributed(
            client, ring
        )

        bucket, key = save_metadata(
            client, chunks, placement
        )

        download_distributed(client, bucket, key)

    original_hash = file_sha256(SOURCE)
    restored_hash = file_sha256(RESTORED)

    print("\n=== CHUNK DISTRIBUTION ===")

    for node_id in online_nodes:
        print(
            f"{node_id}: {distribution[node_id]} chunks"
        )

    print("\n=== INTEGRITY VERIFICATION ===")
    print("Original SHA-256:", original_hash)
    print("Restored SHA-256:", restored_hash)
    print("Integrity verified:", original_hash == restored_hash)

    if original_hash != restored_hash:
        raise RuntimeError(
            "Distributed file reconstruction failed"
        )

    print("\nSUCCESS: Distributed upload and download verified.")


if __name__ == "__main__":
    main()
