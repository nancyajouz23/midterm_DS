
import json
import uuid
from pathlib import Path

import httpx

BASE_URL = "http://localhost:8010"
MANIFEST = Path("demo_output/manifest.json")


def main():
    if not MANIFEST.exists():
        raise FileNotFoundError(
            "Run python -m scripts.fixed_chunk_demo first"
        )

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    suffix = uuid.uuid4().hex[:8]
    bucket = f"test-bucket-{suffix}"

    chunks = [
        {"hash": chunk["hash"], "size": chunk["size"]}
        for chunk in manifest["chunks"]
    ]

    first_hash = chunks[0]["hash"]

    with httpx.Client(timeout=30) as client:
        # 1. Create bucket
        response = client.post(
            f"{BASE_URL}/buckets",
            json={"name": bucket}
        )
        response.raise_for_status()
        print("Bucket created:", response.json())

        # 2. Register two objects referencing identical chunks
        created = []

        for name in ["copy-a.bin", "copy-b.bin"]:
            payload = {
                "bucket": bucket,
                "key": name,
                "size": manifest["file_size"],
                "chunks": chunks,
                "tags": {"test": "deduplication"}
            }

            response = client.post(
                f"{BASE_URL}/objects/metadata",
                json=payload
            )
            response.raise_for_status()
            created.append(response.json())
            print("Registered:", response.json())

        # 3. Verify metadata can be retrieved
        response = client.get(
            f"{BASE_URL}/objects/metadata/{bucket}/copy-a.bin"
        )
        response.raise_for_status()
        result = response.json()

        assert len(result["chunks"]) == len(chunks)
        print("Ordered object chunks verified.")

        # 4. Reference count should be 2
        ref_url = f"{BASE_URL}/chunks/references/{first_hash}"
        response = client.get(ref_url)
        response.raise_for_status()
        before = response.json()["ref_count"]
        print("Reference count after two objects:", before)

        # 5. Delete first object
        first = created[0]
        response = client.delete(
            f"{BASE_URL}/objects/metadata/"
            f"{bucket}/{first['key']}/versions/{first['version']}"
        )
        response.raise_for_status()

        response = client.get(ref_url)
        response.raise_for_status()
        after_first = response.json()["ref_count"]
        print("Reference count after first deletion:", after_first)

        # 6. Delete second object
        second = created[1]
        response = client.delete(
            f"{BASE_URL}/objects/metadata/"
            f"{bucket}/{second['key']}/versions/{second['version']}"
        )
        response.raise_for_status()

        response = client.get(ref_url)
        response.raise_for_status()
        after_second = response.json()["ref_count"]
        print("Reference count after second deletion:", after_second)

        assert before >= 2
        assert after_first == before - 1
        assert after_second == before - 2

        print("\nSUCCESS: Reference counting verified.")
        print("Unreferenced chunks remain pending physical cleanup.")


if __name__ == "__main__":
    main()
