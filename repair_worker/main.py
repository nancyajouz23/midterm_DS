"""
Repair worker: failure detection, scrubbing and repair.

Every REPAIR_INTERVAL seconds it:
  1. Reads node heartbeats from the metadata service. A node that has
     not sent a heartbeat for HEARTBEAT_TIMEOUT seconds (3 missed
     heartbeats) is marked FAILED, and every copy on it is treated as
     lost without contacting it.
  2. Scrubs the copies on live nodes: the storage node re-reads the
     chunk and verifies its SHA-256 (HEAD), so bit rot is detected.
     Corrupted copies are deleted.
  3. Repairs what is missing:
       - replication: copy a verified replica to another node,
       - erasure coding: read K healthy shards, decode the chunk,
         re-encode it and rebuild only the missing shards.
     Each round that finds damage is recorded as a repair job with its
     progress and repair traffic (bytes read + written).
"""

import hashlib
import os
import time

import httpx
from zfec.easyfec import Decoder, Encoder

from metadata_service.placement import ConsistentHashRing


METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8010")
NODE_COUNT = int(os.getenv("STORAGE_NODE_COUNT", "7"))
INTERVAL = int(os.getenv("REPAIR_INTERVAL", "10"))

NODES = {
    f"node{i}": f"http://storage-node-{i}:8000"
    for i in range(1, NODE_COUNT + 1)
}

REPLICATION_FACTOR = 3
K = 4  # erasure coding data shards
M = 2  # erasure coding parity shards

ring = ConsistentHashRing(sorted(NODES), virtual_nodes=128)
encoder = Encoder(K, K + M)
decoder = Decoder(K, K + M)

# Nodes already reported as failed, so the event is logged once.
known_failed = set()

# Damage that could not be repaired last round (e.g. no spare node);
# it is retried only when the cluster membership changes.
unrepairable = None

# Nodes that did not answer during the current scan; skipped until the
# next scan so one dead node cannot stall scrubbing.
unreachable = set()


def log(message):
    print(message, flush=True)


# -------------------------
# Node health
# -------------------------

def node_health(client):
    """Return (online, failed) node sets based on heartbeats."""
    response = client.get(f"{METADATA_URL}/nodes")
    response.raise_for_status()

    online, failed = set(), set()
    for node in response.json():
        if node["node_id"] not in NODES:
            continue
        if node["online"]:
            online.add(node["node_id"])
        else:
            failed.add(node["node_id"])
            if node["node_id"] not in known_failed:
                log(
                    f"NODE FAILED {node['node_id']}: no heartbeat for "
                    f"{node['seconds_since_heartbeat']}s -> repair triggered"
                )

    for node_id in known_failed - failed:
        log(f"NODE RECOVERED {node_id}: heartbeats received again")

    # Nodes that never sent a heartbeat are unknown -> failed.
    failed |= set(NODES) - online - failed

    known_failed.clear()
    known_failed.update(failed)
    return online, failed


def alive(node_id, online):
    """Usable as a repair target: heartbeating and answering this scan."""
    return node_id in online and node_id not in unreachable


def probe(client, node_id, digest, expected_size):
    """
    Check one copy. Returns 'ok', 'corrupt' (bad data, safe to delete),
    'missing' or 'unreachable'. Transport errors are retried, because
    a pooled keep-alive connection may have been closed by the node.
    """
    if node_id in unreachable:
        return "unreachable"

    for attempt in range(3):
        try:
            r = client.head(f"{NODES[node_id]}/chunks/{digest}")
        except httpx.TransportError:
            time.sleep(0.2 * (attempt + 1))
            continue

        if r.status_code == 200:
            size = int(r.headers.get("content-length", -1))
            return "ok" if size == expected_size else "corrupt"
        if r.status_code == 409:
            return "corrupt"
        if r.status_code == 404:
            return "missing"
        return "unreachable"

    log(f"NODE UNREACHABLE {node_id}: skipped for the rest of this scan")
    unreachable.add(node_id)
    return "unreachable"


def fetch_verified(client, node_id, digest, expected_size):
    try:
        r = client.get(f"{NODES[node_id]}/chunks/{digest}")
        r.raise_for_status()
    except httpx.HTTPError:
        return None

    data = r.content
    if (
        len(data) == expected_size
        and hashlib.sha256(data).hexdigest() == digest
    ):
        return data
    return None


def store(client, node_id, digest, data):
    """Write a copy and confirm it reads back correctly."""
    try:
        r = client.put(
            f"{NODES[node_id]}/chunks/{digest}",
            content=data,
            headers={"Content-Type": "application/octet-stream"}
        )
        r.raise_for_status()
    except httpx.HTTPError as exc:
        log(f"  write to {node_id} failed: {exc}")
        return False

    return probe(client, node_id, digest, len(data)) == "ok"


def delete_copy(client, node_id, digest):
    try:
        client.delete(f"{NODES[node_id]}/chunks/{digest}")
    except httpx.HTTPError:
        pass


# -------------------------
# Repair job bookkeeping
# -------------------------

class RepairJob:
    """Created lazily, only when a round actually finds damage."""

    def __init__(self, client, failed):
        self.client = client
        self.failed = sorted(failed)
        self.job_id = None
        self.repaired = 0
        self.failed_chunks = 0

    def start(self, chunks_total):
        reason = (
            "node failure (missed heartbeats): " + ", ".join(self.failed)
            if self.failed else "scrub: corrupted or missing copies"
        )
        r = self.client.post(f"{METADATA_URL}/repair-jobs", json={
            "reason": reason,
            "failed_nodes": self.failed,
            "chunks_total": chunks_total
        })
        r.raise_for_status()
        self.job_id = r.json()["job_id"]
        log(
            f"REPAIR JOB {self.job_id} STARTED ({reason}): "
            f"{chunks_total} degraded chunks"
        )

    def event(self, chunk_hash, mode, target, bytes_read, bytes_written):
        self.client.post(
            f"{METADATA_URL}/repair-jobs/{self.job_id}/events",
            json={
                "chunk_hash": chunk_hash, "mode": mode,
                "target_node": target,
                "bytes_read": bytes_read,
                "bytes_written": bytes_written
            }
        ).raise_for_status()

    def progress(self, ok):
        if ok:
            self.repaired += 1
        else:
            self.failed_chunks += 1
        self.client.patch(
            f"{METADATA_URL}/repair-jobs/{self.job_id}",
            json={
                "chunks_repaired": int(ok),
                "chunks_failed": int(not ok)
            }
        ).raise_for_status()

    def finish(self):
        status = "completed" if self.failed_chunks == 0 else "partial"
        self.client.patch(
            f"{METADATA_URL}/repair-jobs/{self.job_id}",
            json={"status": status}
        ).raise_for_status()
        log(
            f"REPAIR JOB {self.job_id} {status.upper()}: "
            f"repaired={self.repaired} failed={self.failed_chunks}"
        )


# -------------------------
# Replication
# -------------------------

def inspect_replicated(client, chunk, online):
    """Classify the registered replicas of one chunk."""
    digest, size = chunk["chunk_hash"], chunk["size"]
    r = client.get(f"{METADATA_URL}/chunks/locations/{digest}")
    r.raise_for_status()

    healthy, lost, corrupt, waiting = [], [], [], []
    for node_id in r.json()["nodes"]:
        if node_id not in online:
            lost.append(node_id)  # failed node (missed heartbeats)
            continue
        state = probe(client, node_id, digest, size)
        if state == "ok":
            healthy.append(node_id)
        elif state in ("corrupt", "missing"):
            corrupt.append(node_id)
        else:
            # Not answering but still heartbeating: not declared failed
            # yet, so its copy is not replaced (avoids repair storms on
            # short network glitches).
            waiting.append(node_id)

    return healthy, lost, corrupt, waiting


def repair_replicated(client, job, chunk, online,
                      healthy, lost, corrupt, waiting):
    digest, size = chunk["chunk_hash"], chunk["size"]
    log(
        f"DEGRADED replication {digest[:12]} healthy={healthy} "
        f"failed={lost} corrupt={corrupt}"
    )

    # Remove corrupted copies so the node can receive a clean one.
    for node_id in corrupt:
        delete_copy(client, node_id, digest)
        client.delete(
            f"{METADATA_URL}/chunks/locations/{digest}/{node_id}"
        )

    source = None
    for node_id in healthy:
        source = fetch_verified(client, node_id, digest, size)
        if source is not None:
            break

    if source is None:
        log(f"REPAIR FAILED {digest[:12]}: no valid replica left")
        return False

    bytes_read = len(source)
    targets = [
        node_id for node_id in ring.get_nodes(digest, len(NODES))
        if alive(node_id, online) and node_id not in healthy
    ]

    for node_id in targets:
        if len(healthy) + len(waiting) >= REPLICATION_FACTOR:
            break
        if not store(client, node_id, digest, source):
            continue

        client.post(f"{METADATA_URL}/chunks/locations", json={
            "chunk_hash": digest, "node_id": node_id
        }).raise_for_status()
        healthy.append(node_id)
        job.event(digest, "replication", node_id, bytes_read, size)
        bytes_read = 0  # source is read once, written many times
        log(f"REPAIRED replica {digest[:12]} -> {node_id}")

    if len(healthy) + len(waiting) < REPLICATION_FACTOR:
        log(f"STILL DEGRADED {digest[:12]} replicas={len(healthy)}")
        return False

    for node_id in lost:
        client.delete(
            f"{METADATA_URL}/chunks/locations/{digest}/{node_id}"
        )
    return True


# -------------------------
# Erasure coding
# -------------------------

def inspect_erasure(client, chunk, online):
    """Return (healthy shards, missing shards) of one erasure chunk."""
    healthy, missing = [], []
    for shard in chunk["shards"]:
        node_id = shard["node_id"]
        if node_id not in online or node_id not in NODES:
            missing.append({**shard, "state": "failed"})
            continue
        state = probe(
            client, node_id, shard["shard_hash"], shard["shard_size"]
        )
        if state == "ok":
            healthy.append(shard)
        elif state in ("corrupt", "missing"):
            missing.append({**shard, "state": state})
        # 'unreachable' on a heartbeating node: wait for the heartbeat
        # timeout before declaring the shard lost.
    return healthy, missing


def repair_erasure(client, job, chunk, online, healthy, missing):
    digest = chunk["chunk_hash"]
    log(
        f"DEGRADED erasure {digest[:12]} healthy_shards={len(healthy)} "
        "missing=" + str([
            f"#{s['shard_index']}@{s['node_id']}({s['state']})"
            for s in missing
        ])
    )

    for shard in missing:
        if shard["state"] in ("corrupt", "missing"):
            delete_copy(client, shard["node_id"], shard["shard_hash"])

    # Read any K verified shards and decode the original chunk.
    blocks, indices, bytes_read = [], [], 0
    for shard in healthy:
        data = fetch_verified(
            client, shard["node_id"],
            shard["shard_hash"], shard["shard_size"]
        )
        if data is None:
            continue
        blocks.append(data)
        indices.append(shard["shard_index"])
        bytes_read += len(data)
        if len(blocks) == K:
            break

    if len(blocks) < K:
        log(
            f"REPAIR FAILED {digest[:12]}: only {len(blocks)} of "
            f"{K} required shards available (data unrecoverable)"
        )
        return False

    original = decoder.decode(blocks, indices, chunk["padlen"])
    if hashlib.sha256(original).hexdigest() != digest:
        log(f"REPAIR FAILED {digest[:12]}: decoded chunk hash mismatch")
        return False

    rebuilt = encoder.encode(original)
    used = {s["node_id"] for s in healthy}
    all_ok = True

    for shard in missing:
        data = rebuilt[shard["shard_index"]]
        if hashlib.sha256(data).hexdigest() != shard["shard_hash"]:
            log(f"REPAIR FAILED {digest[:12]}: rebuilt shard mismatch")
            all_ok = False
            continue

        # Same node if it is alive, otherwise a node that holds no
        # other shard of this chunk (keeps one shard per node).
        candidates = (
            [shard["node_id"]] if alive(shard["node_id"], online) else []
        )
        candidates += [
            node_id for node_id in ring.get_nodes(digest, len(NODES))
            if alive(node_id, online) and node_id not in used
            and node_id != shard["node_id"]
        ]

        target = None
        for node_id in candidates:
            if node_id in used:
                continue
            if store(client, node_id, shard["shard_hash"], data):
                target = node_id
                break

        if target is None:
            log(
                f"STILL DEGRADED {digest[:12]} shard "
                f"#{shard['shard_index']}: no spare node available"
            )
            all_ok = False
            continue

        used.add(target)
        if target != shard["node_id"]:
            client.post(f"{METADATA_URL}/erasure/shards/relocate", json={
                "shard_hash": shard["shard_hash"],
                "old_node": shard["node_id"],
                "new_node": target
            }).raise_for_status()

        job.event(digest, "erasure", target, bytes_read, len(data))
        bytes_read = 0  # the K shards are read once per chunk
        log(
            f"REBUILT shard #{shard['shard_index']} of {digest[:12]} "
            f"-> {target}"
        )

    return all_ok


# -------------------------
# Main loop
# -------------------------

def scan():
    unreachable.clear()
    timeout = httpx.Timeout(30, connect=3)

    with httpx.Client(timeout=timeout) as client:
        online, failed = node_health(client)

        replicated = client.get(f"{METADATA_URL}/chunks/repair-candidates")
        replicated.raise_for_status()
        erasure = client.get(f"{METADATA_URL}/erasure/chunks")
        erasure.raise_for_status()

        replicated, erasure = replicated.json(), erasure.json()
        log(
            f"\nSCRUB START: {len(replicated)} replicated chunks, "
            f"{len(erasure)} erasure-coded chunks, "
            f"online={sorted(online)} failed={sorted(failed)}"
        )

        # Pass 1: scrub everything and collect damaged chunks.
        work = []
        for chunk in replicated:
            try:
                state = inspect_replicated(client, chunk, online)
                healthy, _, corrupt, waiting = state
                if (len(healthy) + len(waiting) < REPLICATION_FACTOR
                        or corrupt):
                    work.append(("replication", chunk, state))
            except Exception as exc:
                log(f"CHUNK ERROR {chunk['chunk_hash'][:12]}: {exc}")

        for chunk in erasure:
            try:
                state = inspect_erasure(client, chunk, online)
                if state[1]:
                    work.append(("erasure", chunk, state))
            except Exception as exc:
                log(f"CHUNK ERROR {chunk['chunk_hash'][:12]}: {exc}")

        global unrepairable

        if not work:
            unrepairable = None
            log("SCRUB COMPLETE: all chunks healthy")
            return

        key = (
            frozenset(online),
            frozenset(chunk["chunk_hash"] for _, chunk, _ in work)
        )
        if key == unrepairable:
            log(
                f"WAITING: {len(work)} chunks still degraded and cannot "
                "be repaired until a node joins or recovers"
            )
            return

        # Pass 2: repair, tracked as one repair job.
        job = RepairJob(client, failed)
        job.start(len(work))

        for mode, chunk, state in work:
            try:
                if mode == "replication":
                    ok = repair_replicated(client, job, chunk, online, *state)
                else:
                    ok = repair_erasure(client, job, chunk, online, *state)
            except Exception as exc:
                log(f"CHUNK ERROR {chunk['chunk_hash'][:12]}: {exc}")
                ok = False
            job.progress(ok)

        job.finish()
        unrepairable = key if job.repaired == 0 else None


def main():
    while True:
        try:
            scan()
        except Exception as exc:
            log(f"SCAN FAILED: {exc}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
