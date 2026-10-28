
import argparse
import hashlib
import json
import os
from pathlib import Path

import httpx
from zfec.easyfec import Encoder, Decoder

from metadata_service.chunking import fixed_size_chunks, file_sha256

K = 4
TOTAL_SHARDS = 6

NODES = {
    index: f"http://localhost:{8001 + index}"
    for index in range(TOTAL_SHARDS)
}

OUTPUT = Path("demo_output")
SOURCE = OUTPUT / "erasure_original.bin"
RESTORED = OUTPUT / "erasure_restored.bin"
MANIFEST = OUTPUT / "erasure_manifest.json"


def create_file():
    OUTPUT.mkdir(exist_ok=True)

    if not SOURCE.exists():
        with SOURCE.open("wb") as f:
            for _ in range(8):
                f.write(os.urandom(1024 * 1024))
            f.write(b"Erasure coding test")


def upload():
    create_file()
    encoder = Encoder(K, TOTAL_SHARDS)

    manifest = {
        "original_sha256": file_sha256(SOURCE),
        "k": K,
        "total_shards": TOTAL_SHARDS,
        "chunks": []
    }

    with httpx.Client(timeout=30) as client:
        for chunk_index, (chunk_hash, data) in enumerate(
            fixed_size_chunks(SOURCE), start=1
        ):
            shards = encoder.encode(data)
            padlen = (-len(data)) % K

            chunk_record = {
                "hash": chunk_hash,
                "size": len(data),
                "padlen": padlen,
                "shards": []
            }

            print(f"\nChunk {chunk_index}:")

            for shard_index, shard in enumerate(shards):
                shard_hash = hashlib.sha256(shard).hexdigest()
                node_url = NODES[shard_index]

                response = client.put(
                    f"{node_url}/chunks/{shard_hash}",
                    content=shard,
                    headers={
                        "Content-Type": "application/octet-stream"
                    }
                )
                response.raise_for_status()

                chunk_record["shards"].append({
                    "index": shard_index,
                    "node": f"node{shard_index + 1}",
                    "hash": shard_hash,
                    "size": len(shard)
                })

                print(
                    f"  Shard {shard_index} -> "
                    f"node{shard_index + 1}: OK"
                )

            manifest["chunks"].append(chunk_record)

    MANIFEST.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8"
    )

    print("\nSUCCESS: All six shards stored per chunk.")
    print("Manifest:", MANIFEST)


def download():
    if not MANIFEST.exists():
        raise FileNotFoundError("Run upload first")

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    decoder = Decoder(K, TOTAL_SHARDS)

    with httpx.Client(timeout=8) as client:
        with RESTORED.open("wb") as output:
            for chunk_index, chunk in enumerate(
                manifest["chunks"], start=1
            ):
                blocks = []
                indices = []

                print(f"\nReading chunk {chunk_index}:")

                for shard in chunk["shards"]:
                    node_index = int(shard["node"][4:]) - 1

                    try:
                        response = client.get(
                            f"{NODES[node_index]}/chunks/"
                            f"{shard['hash']}"
                        )
                        response.raise_for_status()

                        data = response.content

                        if (
                            hashlib.sha256(data).hexdigest()
                            != shard["hash"]
                            or len(data) != shard["size"]
                        ):
                            raise ValueError("Invalid shard")

                        blocks.append(data)
                        indices.append(shard["index"])
                        print(f"  {shard['node']}: VERIFIED")

                        if len(blocks) == K:
                            break

                    except (httpx.HTTPError, ValueError):
                        print(f"  {shard['node']}: UNAVAILABLE")

                if len(blocks) < K:
                    raise RuntimeError(
                        f"Only {len(blocks)} of {K} "
                        f"required shards available"
                    )

                recovered = decoder.decode(
                    blocks, indices, chunk["padlen"]
                )

                if (
                    len(recovered) != chunk["size"]
                    or hashlib.sha256(recovered).hexdigest()
                    != chunk["hash"]
                ):
                    raise RuntimeError("Chunk reconstruction failed")

                output.write(recovered)
                print(f"  Reconstructed using {K} shards")

    verified = (
        file_sha256(RESTORED) == manifest["original_sha256"]
    )

    print("\n=== ERASURE CODING VERIFICATION ===")
    print("Original SHA-256:", manifest["original_sha256"])
    print("Restored SHA-256:", file_sha256(RESTORED))
    print("Integrity verified:", verified)

    if not verified:
        raise RuntimeError("File integrity verification failed")

    print("SUCCESS: Erasure-coded file recovered.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["upload", "read"])
    args = parser.parse_args()

    if args.mode == "upload":
        upload()
    else:
        download()
