
import hashlib
import os
import time

import httpx

from metadata_service.placement import ConsistentHashRing


METADATA_URL = os.getenv(
    "METADATA_URL", "http://metadata:8010"
)

NODES = {
    f"node{i}": f"http://storage-node-{i}:8000"
    for i in range(1, 7)
}

REPLICATION_FACTOR = 3
INTERVAL = int(os.getenv("REPAIR_INTERVAL", "10"))

ring = ConsistentHashRing(sorted(NODES), virtual_nodes=128)


def repair_chunk(client, chunk):
    chunk_hash = chunk["chunk_hash"]
    expected_size = chunk["size"]

    response = client.get(
        f"{METADATA_URL}/chunks/locations/{chunk_hash}"
    )
    response.raise_for_status()

    registered = response.json()["nodes"]
    healthy = []
    bad = []

    # Periodic integrity scrubbing using HEAD.
    for node_id in registered:
        if node_id not in NODES:
            bad.append(node_id)
            continue

        try:
            result = client.head(
                f"{NODES[node_id]}/chunks/{chunk_hash}"
            )
            result.raise_for_status()

            if int(result.headers.get("content-length", -1)) != expected_size:
                bad.append(node_id)
            else:
                healthy.append(node_id)

        except httpx.HTTPError:
            bad.append(node_id)

    # All replicas verified.
    if len(healthy) >= REPLICATION_FACTOR:
        print(
            f"HEALTHY {chunk_hash[:12]} "
            f"replicas={len(healthy)}",
            flush=True
        )
        return

    print(
        f"DEGRADED {chunk_hash[:12]} "
        f"healthy={healthy} bad={bad}",
        flush=True
    )

    # Find an intact source.
    source_data = None

    for node_id in healthy:
        try:
            result = client.get(
                f"{NODES[node_id]}/chunks/{chunk_hash}"
            )
            result.raise_for_status()

            data = result.content

            if (
                len(data) == expected_size
                and hashlib.sha256(data).hexdigest() == chunk_hash
            ):
                source_data = data
                break

        except httpx.HTTPError:
            continue

    if source_data is None:
        print(
            f"REPAIR FAILED {chunk_hash[:12]}: no valid source",
            flush=True
        )
        return

    # Prefer a fresh node instead of a failed node.
    preferred = ring.get_nodes(
        chunk_hash, replication_factor=len(NODES)
    )

    candidates = [
        node_id for node_id in preferred
        if node_id not in healthy and node_id not in bad
    ]

    for node_id in candidates:
        if len(healthy) >= REPLICATION_FACTOR:
            break

        try:
            result = client.put(
                f"{NODES[node_id]}/chunks/{chunk_hash}",
                content=source_data,
                headers={
                    "Content-Type": "application/octet-stream"
                }
            )
            result.raise_for_status()

            # Confirm the repaired copy is readable and valid.
            check = client.head(
                f"{NODES[node_id]}/chunks/{chunk_hash}"
            )
            check.raise_for_status()

            if int(check.headers.get("content-length", -1)) != expected_size:
                raise RuntimeError("Repaired chunk size mismatch")

            location = client.post(
                f"{METADATA_URL}/chunks/locations",
                json={
                    "chunk_hash": chunk_hash,
                    "node_id": node_id
                }
            )
            location.raise_for_status()

            healthy.append(node_id)

            print(
                f"REPAIRED {chunk_hash[:12]} -> {node_id}",
                flush=True
            )

        except (httpx.HTTPError, RuntimeError) as exc:
            print(
                f"REPAIR FAILED {node_id}: {exc}",
                flush=True
            )

    # Remove obsolete location records after replacement.
    if len(healthy) >= REPLICATION_FACTOR:
        for node_id in bad:
            try:
                result = client.delete(
                    f"{METADATA_URL}/chunks/locations/"
                    f"{chunk_hash}/{node_id}"
                )
                result.raise_for_status()
            except httpx.HTTPError as exc:
                print(f"LOCATION CLEANUP FAILED: {exc}", flush=True)

        print(
            f"REPLICATION RESTORED {chunk_hash[:12]} "
            f"replicas={len(healthy)}",
            flush=True
        )
    else:
        print(
            f"STILL DEGRADED {chunk_hash[:12]} "
            f"replicas={len(healthy)}",
            flush=True
        )


def scan():
    with httpx.Client(timeout=8) as client:
        response = client.get(
            f"{METADATA_URL}/chunks/repair-candidates"
        )
        response.raise_for_status()

        chunks = response.json()

        print(
            f"\nSCRUB START: {len(chunks)} replicated chunks",
            flush=True
        )

        for chunk in chunks:
            try:
                repair_chunk(client, chunk)
            except Exception as exc:
                print(
                    f"CHUNK ERROR {chunk['chunk_hash'][:12]}: {exc}",
                    flush=True
                )

        print("SCRUB COMPLETE", flush=True)


def main():
    while True:
        try:
            scan()
        except Exception as exc:
            print(f"SCAN FAILED: {exc}", flush=True)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
