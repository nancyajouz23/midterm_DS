
import os
import time
from collections import Counter
from pathlib import Path

from metadata_service.chunking import (
    fixed_size_chunks,
    content_defined_chunks,
)

# Output files
OUTPUT_DIR = Path("demo_output")
ORIGINAL = OUTPUT_DIR / "cdc_original.bin"
MODIFIED = OUTPUT_DIR / "cdc_modified.bin"

FILE_SIZE_MB = 32
INSERTED_DATA = b"INSERTED_DATA_" * 9 + b"X" * 11


def prepare_files():
    """Generate the original file and a modified version."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Create a 32 MiB sample file
    if not ORIGINAL.exists():
        print("Generating original 32 MiB test file...")

        with ORIGINAL.open("wb") as file:
            for _ in range(FILE_SIZE_MB):
                file.write(os.urandom(1024 * 1024))

    # Insert 137 bytes at the beginning of the file
    # without changing the original content
    with MODIFIED.open("wb") as output:
        output.write(INSERTED_DATA)

        with ORIGINAL.open("rb") as source:
            while True:
                block = source.read(1024 * 1024)

                if not block:
                    break

                output.write(block)

    print("Test files prepared successfully.")
    print(f"Inserted bytes: {len(INSERTED_DATA)}")


def analyze(name, chunker):
    """Analyze chunk reuse between original and modified files."""

    print(f"\n=== {name} ===")

    # Chunk original file
    start = time.perf_counter()

    original_chunks = [
        (chunk_hash, len(data))
        for chunk_hash, data in chunker(ORIGINAL)
    ]

    original_time = time.perf_counter() - start

    # Chunk modified file
    start = time.perf_counter()

    modified_chunks = [
        (chunk_hash, len(data))
        for chunk_hash, data in chunker(MODIFIED)
    ]

    modified_time = time.perf_counter() - start

    # Count chunks using their SHA-256 hashes
    original_counts = Counter(
        chunk_hash for chunk_hash, _ in original_chunks
    )

    modified_counts = Counter(
        chunk_hash for chunk_hash, _ in modified_chunks
    )

    # Record sizes for every unique chunk
    chunk_sizes = {
        chunk_hash: size
        for chunk_hash, size in original_chunks
    }

    # Find reusable chunks
    reused_chunks = 0
    reused_bytes = 0

    for chunk_hash, count in original_counts.items():
        matching_count = min(
            count,
            modified_counts.get(chunk_hash, 0)
        )

        reused_chunks += matching_count
        reused_bytes += (
            matching_count * chunk_sizes[chunk_hash]
        )

    original_size = ORIGINAL.stat().st_size

    reuse_percent = (
        reused_bytes / original_size * 100
        if original_size > 0 else 0
    )

    # Display results
    print(f"Original chunks: {len(original_chunks)}")
    print(f"Modified chunks: {len(modified_chunks)}")
    print(f"Reusable chunks: {reused_chunks}")
    print(f"Reusable bytes: {reused_bytes:,}")
    print(f"Data reuse: {reuse_percent:.2f}%")
    print(f"Original processing time: {original_time:.3f} s")
    print(f"Modified processing time: {modified_time:.3f} s")

    return {
        "method": name,
        "original_chunks": len(original_chunks),
        "modified_chunks": len(modified_chunks),
        "reused_chunks": reused_chunks,
        "reused_bytes": reused_bytes,
        "reuse_percent": reuse_percent,
        "processing_time": original_time + modified_time,
    }


def main():
    prepare_files()

    print("\n================================")
    print("   CHUNKING COMPARISON TEST")
    print("================================")

    print(f"Original file: {ORIGINAL.stat().st_size:,} bytes")
    print(f"Modified file: {MODIFIED.stat().st_size:,} bytes")

    # Test fixed-size chunking
    fixed_results = analyze(
        "FIXED-SIZE CHUNKING",
        fixed_size_chunks
    )

    # Test content-defined chunking
    cdc_results = analyze(
        "CONTENT-DEFINED CHUNKING",
        content_defined_chunks
    )

    # Final comparison
    improvement = (
        cdc_results["reuse_percent"]
        - fixed_results["reuse_percent"]
    )

    print("\n================================")
    print("       FINAL COMPARISON")
    print("================================")

    print(
        f"Fixed-size data reuse: "
        f"{fixed_results['reuse_percent']:.2f}%"
    )

    print(
        f"CDC data reuse: "
        f"{cdc_results['reuse_percent']:.2f}%"
    )

    print(
        f"CDC reuse improvement: "
        f"{improvement:.2f} percentage points"
    )

    print(
        f"Fixed-size total processing time: "
        f"{fixed_results['processing_time']:.3f} s"
    )

    print(
        f"CDC total processing time: "
        f"{cdc_results['processing_time']:.3f} s"
    )

    if improvement > 0:
        print("\nRESULT: CDC achieved better data reuse.")
    elif improvement == 0:
        print("\nRESULT: Both methods achieved equal data reuse.")
    else:
        print("\nRESULT: Fixed-size chunking achieved better data reuse.")


if __name__ == "__main__":
    main()
