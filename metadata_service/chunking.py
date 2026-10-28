
import hashlib
from pathlib import Path
from typing import Iterator

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixed_size_chunks(
    file_path: str | Path,
    chunk_size: int = CHUNK_SIZE
) -> Iterator[tuple[str, bytes]]:
    """
    Read a file in fixed-size chunks.
    Yield (chunk_hash, chunk_bytes) in file order.
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")

    with open(file_path, "rb") as file:
        while True:
            data = file.read(chunk_size)

            if not data:
                break

            yield sha256(data), data


def file_sha256(file_path: str | Path) -> str:
    """Calculate the hash of a complete file."""
    digest = hashlib.sha256()

    with open(file_path, "rb") as file:
        while block := file.read(1024 * 1024):
            digest.update(block)

    return digest.hexdigest()


import random

# Content-defined chunking settings
CDC_MIN_SIZE = 2 * 1024 * 1024
CDC_AVG_SIZE = 4 * 1024 * 1024
CDC_MAX_SIZE = 8 * 1024 * 1024

# Deterministic gear table: same contents -> same boundaries
_random = random.Random(42)
GEAR_TABLE = [_random.getrandbits(64) for _ in range(256)]

MASK_64 = (1 << 64) - 1
BOUNDARY_MASK = (1 << 21) - 1


def content_defined_chunks(file_path):
    """
    Gear rolling-hash content-defined chunking.

    Minimum chunk: 2 MiB
    Target average: approximately 4 MiB
    Maximum chunk: 8 MiB

    Yields (sha256_hash, chunk_data) in file order.
    """
    chunk = bytearray()
    fingerprint = 0

    with open(file_path, "rb") as file:
        while True:
            block = file.read(64 * 1024)

            if not block:
                break

            for byte in block:
                chunk.append(byte)

                fingerprint = (
                    (fingerprint << 1) + GEAR_TABLE[byte]
                ) & MASK_64

                size = len(chunk)

                boundary_found = (
                    size >= CDC_MIN_SIZE
                    and (fingerprint & BOUNDARY_MASK) == 0
                )

                if boundary_found or size >= CDC_MAX_SIZE:
                    data = bytes(chunk)
                    yield sha256(data), data

                    chunk.clear()
                    fingerprint = 0

    # Include the remaining bytes
    if chunk:
        data = bytes(chunk)
        yield sha256(data), data
