"""
Workload benchmark: replication (N=3) versus erasure coding (4+2).

Workloads (from the midterm specification):
  small : many small files  (default 5,000 files of 1-50 KB, text/JSON)
  large : a few large files (default 10 files of 100 MB, binary)

For each workload and each redundancy mode the script:
  1. generates the dataset (deterministic, reused on later runs),
  2. uploads every file through the gateway and measures throughput,
  3. downloads every file, verifies SHA-256 and measures throughput,
  4. reads the bucket's storage overhead (physical / logical bytes),
  5. (--failure-test) stops one storage node, waits for heartbeat-based
     failure detection and the repair job, and measures repair traffic,
     then restarts the node.

Results are written to benchmark_results/<workload>_<timestamp>.json/.md

Usage (from the project folder, stack running):
  python -m scripts.workload_benchmark --workload small --failure-test
  python -m scripts.workload_benchmark --workload large --failure-test

Tip: the gateway allows 60 requests/minute per API key. For the small
workload restart the gateway with a higher limit first, e.g.
  GATEWAY_RATE_LIMIT=1000000 docker compose -f compose.storage.yaml
      --env-file .env up -d gateway
"""

import argparse
import hashlib
import json
import os
import random
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import httpx


GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8020")
DATASETS = Path("datasets")
RESULTS = Path("benchmark_results")
MB = 1024 * 1024


def api_key():
    key = os.getenv("GATEWAY_API_KEY")
    if key:
        return key
    for line in Path(".env").read_text().splitlines():
        if line.startswith("GATEWAY_API_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("Set GATEWAY_API_KEY or create .env")


# -------------------------
# Dataset generation
# -------------------------

WORDS = (
    "distributed storage chunk replica shard node gateway metadata "
    "hash ring quorum repair scrub object bucket version erasure "
    "parity heartbeat failure recovery consistent placement"
).split()


def generate_small(count, seed=42):
    """count text/JSON files of 1-50 KB."""
    folder = DATASETS / f"small_{count}"
    if folder.exists() and len(list(folder.iterdir())) == count:
        return folder

    folder.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(seed)

    for i in range(count):
        target = rnd.randint(1024, 50 * 1024)
        if i % 2 == 0:
            records = []
            while len(json.dumps(records)) < target:
                records.append({
                    "id": rnd.randint(0, 10**9),
                    "name": " ".join(rnd.choices(WORDS, k=3)),
                    "value": rnd.random(),
                    "tags": rnd.sample(WORDS, 2)
                })
            data = json.dumps(records)[:target].encode()
            name = f"record_{i:05d}.json"
        else:
            text = []
            while sum(len(w) + 1 for w in text) < target:
                text.append(rnd.choice(WORDS))
            data = " ".join(text)[:target].encode()
            name = f"note_{i:05d}.txt"

        (folder / name).write_bytes(data)

    return folder


def generate_large(count, size_mb, seed=7):
    """count binary files of size_mb MB (incompressible, like video)."""
    folder = DATASETS / f"large_{count}x{size_mb}MB"
    folder.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(seed)

    for i in range(count):
        path = folder / f"video_{i:02d}.bin"
        if path.exists() and path.stat().st_size == size_mb * MB:
            continue
        with path.open("wb") as file:
            for _ in range(size_mb):
                file.write(rnd.randbytes(MB))

    return folder


# -------------------------
# Benchmark steps
# -------------------------

def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while block := file.read(MB):
            digest.update(block)
    return digest.hexdigest()


def upload_all(client, bucket, files, workers):
    latencies = []

    def upload(path):
        start = time.perf_counter()
        with open(path, "rb") as body:
            r = client.put(
                f"/objects/{bucket}/{path.name}",
                content=body,
                params={"tags": json.dumps({"workload": bucket})}
            )
        r.raise_for_status()
        latencies.append(time.perf_counter() - start)

    start = time.perf_counter()
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(upload, files))
    return time.perf_counter() - start, latencies


def download_all(client, bucket, files, workers):
    expected = {path.name: sha256_file(path) for path in files}
    latencies = []
    errors = []

    def download(path):
        start = time.perf_counter()
        digest = hashlib.sha256()
        with client.stream("GET", f"/objects/{bucket}/{path.name}") as r:
            r.raise_for_status()
            for block in r.iter_bytes():
                digest.update(block)
        latencies.append(time.perf_counter() - start)
        if digest.hexdigest() != expected[path.name]:
            errors.append(path.name)

    start = time.perf_counter()
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(download, files))
    return time.perf_counter() - start, latencies, errors


def summarize(seconds, latencies, total_bytes, count):
    ordered = sorted(latencies)
    return {
        "seconds": round(seconds, 2),
        "throughput_MBps": round(total_bytes / MB / seconds, 2),
        "files_per_second": round(count / seconds, 2),
        "latency_avg_ms": round(statistics.mean(ordered) * 1000, 1),
        "latency_p95_ms": round(
            ordered[int(len(ordered) * 0.95) - 1] * 1000, 1
        )
    }


def latest_job_id(client):
    jobs = client.get("/cluster/status").json()["repair_jobs"]
    return jobs[0]["job_id"] if jobs else 0


def failure_test(client, buckets, node, timeout):
    """Stop a node, wait for detection + repair, measure repair traffic."""
    # The repair worker ignores objects uploaded in the last 30 s
    # (their locations may still be registering), so let them settle.
    print("\nWaiting 35 s so the new objects become eligible for repair ...")
    time.sleep(35)

    since = datetime.now(timezone.utc).isoformat()
    last_job = latest_job_id(client)

    print(f"\nStopping {node} to simulate a failure ...")
    subprocess.run(["docker", "stop", node], check=True,
                   capture_output=True)
    stopped = time.perf_counter()

    detected = None
    finished = None
    job = None

    while time.perf_counter() - stopped < timeout:
        status = client.get("/cluster/status").json()
        node_id = node.replace("storage-node-", "node")
        offline = [n["node_id"] for n in status["nodes"] if not n["online"]]

        if detected is None and node_id in offline:
            detected = time.perf_counter() - stopped
            print(f"  failure detected after {detected:.1f}s "
                  "(missed heartbeats)")

        jobs = [j for j in status["repair_jobs"] if j["job_id"] > last_job]
        running = [j for j in jobs if j["status"] == "running"]

        # Done when the failure was detected, at least one repair job
        # ran, none is still running and no object is degraded any more.
        if (
            detected is not None and jobs and not running
            and status["degraded_objects"] == 0
        ):
            job = jobs
            finished = time.perf_counter() - stopped
            break

        if jobs:
            j = jobs[0]
            print(f"  repair job {j['job_id']}: "
                  f"{j['chunks_repaired']}/{j['chunks_total']} chunks")
        time.sleep(3)

    print(f"Restarting {node} ...")
    subprocess.run(["docker", "start", node], check=True,
                   capture_output=True)

    traffic = {
        mode: client.get(
            f"/buckets/{bucket}/repair-traffic", params={"since": since}
        ).json()
        for mode, bucket in buckets.items()
    }

    return {
        "failed_node": node,
        "detection_seconds": round(detected, 1) if detected else None,
        "repair_completed_seconds": round(finished, 1) if finished else None,
        "repair_jobs": job,
        "repair_traffic": traffic
    }


# -------------------------
# Report
# -------------------------

def write_report(result):
    RESULTS.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = RESULTS / f"{result['workload']}_{stamp}"
    base.with_suffix(".json").write_text(
        json.dumps(result, indent=2, default=str)
    )

    lines = [
        f"# Workload benchmark: {result['workload']}",
        "",
        f"Dataset: {result['files']} files, "
        f"{result['dataset_bytes'] / MB:.1f} MB total. "
        f"Date: {result['date']}",
        "",
        "| Metric | Replication (N=3) | Erasure coding (4+2) |",
        "|---|---|---|",
    ]

    rows = [
        ("Upload throughput (MB/s)", "upload", "throughput_MBps"),
        ("Upload files/s", "upload", "files_per_second"),
        ("Upload latency avg / p95 (ms)", "upload", None),
        ("Download throughput (MB/s)", "download", "throughput_MBps"),
        ("Download latency avg / p95 (ms)", "download", None),
    ]
    modes = result["modes"]
    for label, step, field in rows:
        cells = []
        for mode in ("replication", "erasure"):
            data = modes[mode][step]
            cells.append(
                f"{data['latency_avg_ms']} / {data['latency_p95_ms']}"
                if field is None else str(data[field])
            )
        lines.append(f"| {label} | {cells[0]} | {cells[1]} |")

    def stat(mode, key, fmt="{}"):
        return fmt.format(modes[mode]["storage"][key])

    lines += [
        f"| Logical data after dedup (MB) | "
        f"{modes['replication']['storage']['unique_chunk_bytes'] / MB:.1f} | "
        f"{modes['erasure']['storage']['unique_chunk_bytes'] / MB:.1f} |",
        f"| Physical data stored (MB) | "
        f"{modes['replication']['storage']['physical_bytes'] / MB:.1f} | "
        f"{modes['erasure']['storage']['physical_bytes'] / MB:.1f} |",
        f"| Storage overhead (physical / logical) | "
        f"{stat('replication', 'storage_overhead')}x | "
        f"{stat('erasure', 'storage_overhead')}x |",
        f"| Overhead with 4 KB disk blocks | "
        f"{stat('replication', 'storage_overhead_4k_blocks')}x | "
        f"{stat('erasure', 'storage_overhead_4k_blocks')}x |",
        f"| Stored copies / shards | "
        f"{stat('replication', 'stored_copies')} | "
        f"{stat('erasure', 'stored_copies')} |",
    ]

    failure = result.get("failure_test")
    if failure:
        rep = failure["repair_traffic"]
        lines += [
            f"| Repaired chunks after {failure['failed_node']} failed | "
            f"{rep['replication']['repaired_chunks']} | "
            f"{rep['erasure']['repaired_chunks']} |",
            f"| Repair traffic read (MB) | "
            f"{rep['replication']['bytes_read'] / MB:.2f} | "
            f"{rep['erasure']['bytes_read'] / MB:.2f} |",
            f"| Repair traffic written (MB) | "
            f"{rep['replication']['bytes_written'] / MB:.2f} | "
            f"{rep['erasure']['bytes_written'] / MB:.2f} |",
            f"| Total repair traffic (MB) | "
            f"{rep['replication']['repair_traffic_bytes'] / MB:.2f} | "
            f"{rep['erasure']['repair_traffic_bytes'] / MB:.2f} |",
            "",
            f"Failure detected after {failure['detection_seconds']} s; "
            f"repair job finished after "
            f"{failure['repair_completed_seconds']} s.",
        ]

    base.with_suffix(".md").write_text("\n".join(lines) + "\n")
    return base


# -------------------------
# Main
# -------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--workload", choices=["small", "large"],
                        required=True)
    parser.add_argument("--small-count", type=int, default=5000)
    parser.add_argument("--large-count", type=int, default=10)
    parser.add_argument("--large-size-mb", type=int, default=100)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--failure-test", action="store_true")
    parser.add_argument("--fail-node", default="storage-node-2")
    parser.add_argument("--repair-timeout", type=int, default=900)
    args = parser.parse_args()

    if args.workload == "small":
        folder = generate_small(args.small_count)
        workers = args.workers or 16
    else:
        folder = generate_large(args.large_count, args.large_size_mb)
        workers = args.workers or 2

    files = sorted(folder.iterdir())
    total = sum(path.stat().st_size for path in files)
    print(f"Dataset {folder}: {len(files)} files, {total / MB:.1f} MB")

    stamp = datetime.now().strftime("%m%d%H%M%S")
    buckets = {
        mode: f"bench-{args.workload}-{mode[:3]}-{stamp}"
        for mode in ("replication", "erasure")
    }

    result = {
        "workload": args.workload,
        "date": datetime.now().isoformat(timespec="seconds"),
        "files": len(files),
        "dataset_bytes": total,
        "workers": workers,
        "buckets": buckets,
        "modes": {}
    }

    with httpx.Client(
        base_url=GATEWAY,
        headers={"X-API-Key": api_key()},
        timeout=600,
        limits=httpx.Limits(max_connections=workers + 4)
    ) as client:
        for mode, bucket in buckets.items():
            print(f"\n=== {mode}: bucket {bucket} ===")
            client.post("/buckets", json={
                "name": bucket, "mode": mode
            }).raise_for_status()

            seconds, lat = upload_all(client, bucket, files, workers)
            upload = summarize(seconds, lat, total, len(files))
            print(f"upload:   {upload}")

            seconds, lat, errors = download_all(
                client, bucket, files, workers
            )
            download = summarize(seconds, lat, total, len(files))
            download["integrity_errors"] = len(errors)
            print(f"download: {download}")

            storage = client.get(f"/buckets/{bucket}/stats").json()
            print(f"storage:  overhead {storage['storage_overhead']}x "
                  f"({storage['physical_bytes'] / MB:.1f} MB stored for "
                  f"{storage['unique_chunk_bytes'] / MB:.1f} MB of data)")

            result["modes"][mode] = {
                "upload": upload,
                "download": download,
                "storage": storage
            }

        if args.failure_test:
            result["failure_test"] = failure_test(
                client, buckets, args.fail_node, args.repair_timeout
            )
            for mode, traffic in result["failure_test"][
                "repair_traffic"
            ].items():
                print(f"repair traffic {mode}: {traffic}")

    base = write_report(result)
    print(f"\nResults written to {base}.md and {base}.json")
    print(base.with_suffix(".md").read_text())


if __name__ == "__main__":
    main()
