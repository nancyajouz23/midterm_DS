
import os
import uuid

import httpx
import pytest


BASE_URL = "http://localhost:8020"
API_KEY = os.environ["GATEWAY_API_KEY"]

HEADERS = {
    "X-API-Key": API_KEY
}


@pytest.fixture(scope="module")
def client():
    with httpx.Client(
        base_url=BASE_URL,
        headers=HEADERS,
        timeout=120
    ) as http_client:
        yield http_client


def test_01_gateway_health():
    response = httpx.get(
        f"{BASE_URL}/health",
        timeout=10
    )

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_02_authentication():
    # Request without API key must be rejected
    response = httpx.get(
        f"{BASE_URL}/buckets",
        timeout=10
    )

    assert response.status_code == 401

    # Request with API key must succeed
    response = httpx.get(
        f"{BASE_URL}/buckets",
        headers=HEADERS,
        timeout=10
    )

    assert response.status_code == 200


@pytest.mark.parametrize(
    "mode",
    ["replication", "erasure"]
)
def test_03_object_workflow(client, mode):
    bucket = f"pytest-{mode}-{uuid.uuid4().hex[:8]}"
    key = "sample.bin"

    # 1. Create bucket
    response = client.post(
        "/buckets",
        json={
            "name": bucket,
            "mode": mode
        }
    )

    assert response.status_code == 201
    assert response.json()["mode"] == mode

    # 2. Generate sample data
    original_data = os.urandom(128 * 1024 + 37)

    # 3. Upload object
    response = client.put(
        f"/objects/{bucket}/{key}",
        content=original_data,
        headers={
            "Content-Type": "application/octet-stream"
        }
    )

    assert response.status_code == 201
    assert response.json()["size"] == len(original_data)
    assert response.json()["mode"] == mode

    # 4. Download object
    response = client.get(
        f"/objects/{bucket}/{key}"
    )

    assert response.status_code == 200
    assert response.content == original_data

    # 5. Test byte-range download
    response = client.get(
        f"/objects/{bucket}/{key}",
        headers={"Range": "bytes=0-99"}
    )

    assert response.status_code == 206
    assert response.content == original_data[:100]
    assert len(response.content) == 100

    # 6. Verify object metadata
    response = client.get(
        f"/object-metadata/{bucket}/{key}"
    )

    assert response.status_code == 200

    metadata = response.json()

    assert metadata["size"] == len(original_data)
    assert len(metadata["chunks"]) >= 1

    # 7. Search by prefix
    response = client.get(
        "/search",
        params={
            "bucket": bucket,
            "prefix": "sample"
        }
    )

    assert response.status_code == 200
    assert any(
        obj["object_key"] == key
        for obj in response.json()
    )

    # 8. Delete object
    response = client.delete(
        f"/objects/{bucket}/{key}"
    )

    assert response.status_code == 200
    assert response.json()["deleted"] is True

    # 9. Verify deleted metadata is unavailable
    response = client.get(
        f"/object-metadata/{bucket}/{key}"
    )

    assert response.status_code == 404


def test_04_cluster_status(client):
    response = client.get("/cluster/status")

    assert response.status_code == 200

    data = response.json()

    assert "nodes" in data
    assert "online_nodes" in data
    assert "total_free_space" in data
    assert "degraded_objects" in data

    assert data["online_nodes"] >= 4
