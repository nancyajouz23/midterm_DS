
import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path

import httpx

from metadata_service.chunking import (
    file_sha256,
    fixed_size_chunks,
)
from metadata_service.placement import ConsistentHashRing


METADATA_URL = "http://localhost:8010"

NODE_URLS = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003",
    "node4": "http://localhost:8004",
}

REPLICATION_FACTOR = 3  # N
WRITE_QUORUM = 2        # W
READ_QUORUM = 2         # R

OUTPUT_DIR = Path("demo_output")
SOURCE = OUTPUT_DIR / "replicated_original.bin"
RESTORED = OUTPUT_DIR / "replicated_restored.bin"
MANIFEST = OUTPUT_DIR / "replica_manifest.json"


def prepare_file():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE.exists():
        print("Generating a 12 MiB sample file...")

        with SOURCE.open("wb") as file:
            for _ in range(12):
                file.write(os.urandom(1024 * 1024))
            file.write(os.urandom(137))


def create_ring():
    # Preserve the same ring membership even when
    # a node is temporarily unavailable.
    return ConsistentHashRing(
        nodes=sorted(NODE_URLS),
        virtual_nodes=128
    )


def write_replicas(client, ring):
    chunks = []
    locations = {}

    print("\n=== REPLICATED CHUNK UPLOAD ===")

    for index, (chunk_hash, data) in enumerate(
        fixed_size_chunks(SOURCE), start=1
    ):
        targets = ring.get_nodes(
            chunk_hash,
            replication_factor=REPLICATION_FACTOR
        )

        successful = []

        print(f"\nChunk {index} targets: {targets}")

        for node_id in targets:
            try:
                response = client.put(
                    f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
                    content=data,
                    headers={
                        "Content-Type": "application/octet-stream"
                    },
                )
                response.raise_for_status()

                successful.append(node_id)
                print(f"  {node_id}: WRITE OK")

            except httpx.HTTPError as exc:
                print(f"  {node_id}: WRITE FAILED ({exc})")

        if len(successful) < WRITE_QUORUM:
            raise RuntimeError(
                f"Write quorum failed for chunk {index}: "
                f"{len(successful)}/{WRITE_QUORUM}"
            )

        print(
            f"  Write quorum satisfied: "
            f"{len(successful)} >= {WRITE_QUORUM}"
        )

        chunks.append({
            "hash": chunk_hash,
            "size": len(data)
        })

        locations[chunk_hash] = successful

    return chunks, locations


def register_metadata(client, chunks, locations):
    bucket = f"replication-test-{uuid.uuid4().hex[:8]}"
    key = "replicated-demo.bin"

    response = client.post(
        f"{METADATA_URL}/buckets",
        json={"name": bucket}
    )
    response.raise_for_status()

    response = client.post(
        f"{METADATA_URL}/objects/metadata",
        json={
            "bucket": bucket,
            "key": key,
            "size": SOURCE.stat().st_size,
            "chunks": chunks,
            "tags": {
                "redundancy": "replication",
                "replication_factor": "3"
            }
        }
    )
    response.raise_for_status()

    print("\n=== METADATA REGISTRATION ===")
    print("Object:", response.json())

    for chunk_hash, nodes in locations.items():
        for node_id in nodes:
            response = client.post(
                f"{METADATA_URL}/chunks/locations",
                json={
                    "chunk_hash": chunk_hash,
                    "node_id": node_id
                }
            )
            response.raise_for_status()

    manifest = {
        "bucket": bucket,
        "key": key,
        "original_sha256": file_sha256(SOURCE),
        "replication_factor": REPLICATION_FACTOR,
        "write_quorum": WRITE_QUORUM,
        "read_quorum": READ_QUORUM,
        "replica_locations": locations
    }

    MANIFEST.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8"
    )

    print("Replica locations registered in PostgreSQL.")
    print(f"Manifest saved: {MANIFEST}")

    return manifest


def read_with_quorum(client, chunk_hash, expected_size):
    response = client.get(
        f"{METADATA_URL}/chunks/locations/{chunk_hash}"
    )
    response.raise_for_status()

    nodes = response.json()["nodes"]
    verified_replicas = []
    verified_data = None

    for node_id in nodes:
        if node_id not in NODE_URLS:
            continue

        try:
            response = client.get(
                f"{NODE_URLS[node_id]}/chunks/{chunk_hash}"
            )
            response.raise_for_status()

            data = response.content

            # Verify integrity independently of the node.
            actual_hash = hashlib.sha256(data).hexdigest()

            if actual_hash != chunk_hash:
                print(f"  {node_id}: CORRUPTED REPLICA")
                continue

            if len(data) != expected_size:
                print(f"  {node_id}: INCORRECT SIZE")
                continue

            verified_replicas.append(node_id)

            if verified_data is None:
                verified_data = data

            print(f"  {node_id}: READ VERIFIED")

        except httpx.HTTPError as exc:
            print(f"  {node_id}: UNAVAILABLE ({exc})")

    if len(verified_replicas) < READ_QUORUM:
        raise RuntimeError(
            f"Read quorum failed: "
            f"{len(verified_replicas)}/{READ_QUORUM}"
        )

    print(
        f"  Read quorum satisfied: "
        f"{len(verified_replicas)} >= {READ_QUORUM}"
    )

    return verified_data


def download_object(client, manifest):
    print("\n=== QUORUM FILE DOWNLOAD ===")

    response = client.get(
        f"{METADATA_URL}/objects/metadata/"
        f"{manifest['bucket']}/{manifest['key']}"
    )
    response.raise_for_status()

    metadata = response.json()

    with RESTORED.open("wb") as output:
        for index, chunk in enumerate(
            metadata["chunks"], start=1
        ):
            print(f"\nReading chunk {index}:")

            data = read_with_quorum(
                client,
                chunk["hash"],
                chunk["size"]
            )

            output.write(data)

    restored_hash = file_sha256(RESTORED)
    expected_hash = manifest["original_sha256"]

    print("\n=== INTEGRITY VERIFICATION ===")
    print("Original SHA-256:", expected_hash)
    print("Restored SHA-256:", restored_hash)
    print("Integrity verified:", restored_hash == expected_hash)

    if restored_hash != expected_hash:
        raise RuntimeError("Reconstructed file is corrupted!")

    print("\nSUCCESS: Quorum read and file reconstruction verified.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["upload", "read"]
    )
    args = parser.parse_args()

    if not (1 <= WRITE_QUORUM <= REPLICATION_FACTOR):
        raise ValueError("Invalid write quorum")

    if not (1 <= READ_QUORUM <= REPLICATION_FACTOR):
        raise ValueError("Invalid read quorum")

    if WRITE_QUORUM + READ_QUORUM <= REPLICATION_FACTOR:
        raise ValueError("Quorum condition W + R > N not met")

    with httpx.Client(timeout=20) as client:
        if args.mode == "upload":
            prepare_file()

            ring = create_ring()
            chunks, locations = write_replicas(
                client, ring
            )
            manifest = register_metadata(
                client, chunks, locations
            )
            download_object(client, manifest)

        else:
            if not MANIFEST.exists():
                raise FileNotFoundError(
                    "Run replication_demo upload first"
                )

            manifest = json.loads(
                MANIFEST.read_text(encoding="utf-8")
            )
            download_object(client, manifest)


if __name__ == "__main__":
    main()
