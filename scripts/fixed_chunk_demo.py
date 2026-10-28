
import json
import os
from pathlib import Path

import httpx

from metadata_service.chunking import (
    CHUNK_SIZE,
    file_sha256,
    fixed_size_chunks,
)

NODE_URL = "http://localhost:8001"
OUTPUT_DIR = Path("demo_output")
ORIGINAL = OUTPUT_DIR / "original.bin"
RESTORED = OUTPUT_DIR / "restored.bin"
MANIFEST = OUTPUT_DIR / "manifest.json"


def create_sample_file():
    OUTPUT_DIR.mkdir(exist_ok=True)

    if ORIGINAL.exists():
        return

    # 10 MiB + 123 bytes, requiring three chunks
    with ORIGINAL.open("wb") as file:
        for _ in range(10):
            file.write(os.urandom(1024 * 1024))
        file.write(os.urandom(123))


def main():
    create_sample_file()

    manifest = {
        "file_name": ORIGINAL.name,
        "file_size": ORIGINAL.stat().st_size,
        "chunk_size": CHUNK_SIZE,
        "chunks": [],
    }

    print("=== FIXED-SIZE CHUNKING ===")
    print(f"File size: {ORIGINAL.stat().st_size} bytes")
    print(f"Chunk size: {CHUNK_SIZE} bytes")

    with httpx.Client(timeout=60) as client:
        # Split and upload chunks in order
        for index, (chunk_hash, data) in enumerate(
            fixed_size_chunks(ORIGINAL),
            start=1
        ):
            response = client.put(
                f"{NODE_URL}/chunks/{chunk_hash}",
                content=data,
                headers={
                    "Content-Type": "application/octet-stream"
                },
            )
            response.raise_for_status()

            manifest["chunks"].append({
                "index": index,
                "hash": chunk_hash,
                "size": len(data),
            })

            print(
                f"Chunk {index}: {len(data)} bytes "
                f"| SHA-256: {chunk_hash[:16]}... "
                f"| HTTP {response.status_code}"
            )

        MANIFEST.write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8"
        )

        # Download chunks in their original order
        with RESTORED.open("wb") as output:
            for chunk in manifest["chunks"]:
                url = f"{NODE_URL}/chunks/{chunk['hash']}"

                with client.stream("GET", url) as response:
                    response.raise_for_status()

                    for block in response.iter_bytes():
                        output.write(block)

    # Verify complete file reconstruction
    original_hash = file_sha256(ORIGINAL)
    restored_hash = file_sha256(RESTORED)

    print("\n=== VERIFICATION ===")
    print(f"Total chunks: {len(manifest['chunks'])}")
    print(f"Original SHA-256: {original_hash}")
    print(f"Restored SHA-256: {restored_hash}")
    print(f"Integrity verified: {original_hash == restored_hash}")

    if original_hash != restored_hash:
        raise RuntimeError("File reconstruction failed!")


if __name__ == "__main__":
    main()
