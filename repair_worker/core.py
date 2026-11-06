"""
Repair, rebalance and garbage-collection logic shared by the scheduler
(which detects problems) and the queue workers (which fix them).

Every function receives a Context: an HTTP client, the current view of
the cluster (node URLs, which nodes are online) and the set of nodes
that did not answer during this piece of work.
"""

import hashlib
import os
import socket
import time

import httpx
from zfec.easyfec import Decoder, Encoder

from common.cluster import ClusterView

METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8010")
WORKER_NAME = os.getenv("HOSTNAME", socket.gethostname())

REPLICATION_FACTOR = 3
K = 4  # erasure coding data shards
M = 2  # erasure coding parity shards

encoder = Encoder(K, K + M)
decoder = Decoder(K, K + M)

view = ClusterView(METADATA_URL)


def log(message):
    print(message, flush=True)


class Context:
    def __init__(self, client, force_refresh=False):
        self.client = client
        self.view = view.refresh(client, force=force_refresh)
        self.online = set(self.view.online)
        self.unreachable = set()

    def url(self, node_id):
        return self.view.url(node_id)

    def alive(self, node_id):
        """Usable target: heartbeating and answering right now."""
        return node_id in self.online and node_id not in self.unreachable


def new_client():
    return httpx.Client(timeout=httpx.Timeout(30, connect=3))


# -------------------------
# Talking to storage nodes
# -------------------------

def probe(ctx, node_id, digest, expected_size):
    """
    Check one copy: 'ok', 'corrupt' (bad data, safe to delete),
    'missing' or 'unreachable'. Transport errors are retried because a
    pooled keep-alive connection may have been closed by the node.
    """
    url = ctx.url(node_id)
    if url is None or node_id in ctx.unreachable:
        return "unreachable"

    for attempt in range(3):
        try:
            r = ctx.client.head(f"{url}/chunks/{digest}")
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

    log(f"NODE UNREACHABLE {node_id}")
    ctx.unreachable.add(node_id)
    return "unreachable"


def fetch_verified(ctx, node_id, digest, expected_size):
    url = ctx.url(node_id)
    if url is None:
        return None
    try:
        r = ctx.client.get(f"{url}/chunks/{digest}")
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


def store(ctx, node_id, digest, data):
    """Write a copy and confirm it reads back correctly."""
    url = ctx.url(node_id)
    if url is None:
        return False
    try:
        r = ctx.client.put(
            f"{url}/chunks/{digest}",
            content=data,
            headers={"Content-Type": "application/octet-stream"}
        )
        r.raise_for_status()
    except httpx.HTTPError as exc:
        log(f"  write to {node_id} failed: {exc}")
        return False

    return probe(ctx, node_id, digest, len(data)) == "ok"


def delete_copy(ctx, node_id, digest):
    url = ctx.url(node_id)
    if url is None:
        return False
    try:
        r = ctx.client.delete(f"{url}/chunks/{digest}")
        return r.status_code in (200, 404)
    except httpx.HTTPError:
        return False


def metadata(ctx, method, path, **kwargs):
    r = ctx.client.request(method, f"{METADATA_URL}{path}", **kwargs)
    r.raise_for_status()
    return r.json()


# -------------------------
# Job progress reporting
# -------------------------

class JobReporter:
    def __init__(self, ctx, job_id, kind):
        self.ctx = ctx
        self.job_id = job_id
        self.kind = kind  # used in logs only

    def event(self, chunk_hash, mode, target, bytes_read, bytes_written):
        if self.job_id is None:
            return
        metadata(
            self.ctx, "POST", f"/repair-jobs/{self.job_id}/events",
            json={
                "chunk_hash": chunk_hash, "mode": mode,
                "target_node": target,
                "bytes_read": bytes_read,
                "bytes_written": bytes_written
            }
        )

    def progress(self, ok):
        if self.job_id is None:
            return None
        result = metadata(
            self.ctx, "PATCH", f"/repair-jobs/{self.job_id}",
            json={"chunks_repaired": int(ok), "chunks_failed": int(not ok)}
        )
        if result.get("closed"):
            log(f"{self.kind.upper()} JOB {self.job_id} "
                f"{result['closed'].upper()}")
        return result


# -------------------------
# Inspection (scrubbing)
# -------------------------

def inspect_replicated(ctx, digest, size, nodes):
    """Classify the registered replicas of one chunk."""
    healthy, lost, corrupt, waiting = [], [], [], []
    for node_id in nodes:
        if node_id not in ctx.online:
            lost.append(node_id)  # failed node (missed heartbeats)
            continue
        state = probe(ctx, node_id, digest, size)
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


def replicated_needs_repair(state):
    healthy, lost, corrupt, waiting = state
    return bool(corrupt) or len(healthy) + len(waiting) < REPLICATION_FACTOR


def inspect_erasure(ctx, chunk):
    """Return (healthy shards, missing shards) of one erasure chunk."""
    healthy, missing = [], []
    for shard in chunk["shards"]:
        node_id = shard["node_id"]
        if node_id not in ctx.online:
            missing.append({**shard, "state": "failed"})
            continue
        state = probe(ctx, node_id, shard["shard_hash"], shard["shard_size"])
        if state == "ok":
            healthy.append(shard)
        elif state in ("corrupt", "missing"):
            missing.append({**shard, "state": state})
        # 'unreachable' on a heartbeating node: wait for the heartbeat
        # timeout before declaring the shard lost.
    return healthy, missing


# -------------------------
# Repair
# -------------------------

def repair_replicated(ctx, job, digest):
    rows = metadata(ctx, "GET", "/chunks/placements",
                    params={"chunk_hash": digest})
    if not rows:
        return True  # chunk deleted meanwhile
    size, nodes = rows[0]["size"], rows[0]["nodes"]
    healthy, lost, corrupt, waiting = inspect_replicated(
        ctx, digest, size, nodes
    )

    if not replicated_needs_repair((healthy, lost, corrupt, waiting)):
        for node_id in lost:  # replaced earlier; forget the dead copy
            metadata(ctx, "DELETE", f"/chunks/locations/{digest}/{node_id}")
        return True

    log(f"[{WORKER_NAME}] DEGRADED replication {digest[:12]} "
        f"healthy={healthy} failed={lost} corrupt={corrupt}")

    for node_id in corrupt:
        delete_copy(ctx, node_id, digest)
        metadata(ctx, "DELETE", f"/chunks/locations/{digest}/{node_id}")

    source = None
    for node_id in healthy:
        source = fetch_verified(ctx, node_id, digest, size)
        if source is not None:
            break

    if source is None:
        log(f"[{WORKER_NAME}] REPAIR FAILED {digest[:12]}: "
            "no valid replica left")
        return False

    bytes_read = len(source)
    targets = [
        n for n in ctx.view.order(digest)
        if ctx.alive(n) and n not in healthy
    ]

    for node_id in targets:
        if len(healthy) + len(waiting) >= REPLICATION_FACTOR:
            break
        if not store(ctx, node_id, digest, source):
            continue
        metadata(ctx, "POST", "/chunks/locations",
                 json={"chunk_hash": digest, "node_id": node_id})
        healthy.append(node_id)
        job.event(digest, "replication", node_id, bytes_read, size)
        bytes_read = 0  # the source is read once, written many times
        log(f"[{WORKER_NAME}] REPAIRED replica {digest[:12]} -> {node_id}")

    if len(healthy) + len(waiting) < REPLICATION_FACTOR:
        log(f"[{WORKER_NAME}] STILL DEGRADED {digest[:12]} "
            f"replicas={len(healthy)}")
        return False

    for node_id in lost:
        metadata(ctx, "DELETE", f"/chunks/locations/{digest}/{node_id}")
    return True


def load_erasure_chunk(ctx, digest):
    rows = metadata(ctx, "GET", "/erasure/chunks",
                    params={"chunk_hash": digest})
    return rows[0] if rows else None


def decode_chunk(ctx, chunk, healthy):
    """Read K verified shards and rebuild the original chunk."""
    blocks, indices, bytes_read = [], [], 0
    for shard in healthy:
        if shard["shard_index"] in indices:
            continue
        data = fetch_verified(
            ctx, shard["node_id"], shard["shard_hash"], shard["shard_size"]
        )
        if data is None:
            continue
        blocks.append(data)
        indices.append(shard["shard_index"])
        bytes_read += len(data)
        if len(blocks) == K:
            break

    if len(blocks) < K:
        return None, bytes_read
    original = decoder.decode(blocks, indices, chunk["padlen"])
    if hashlib.sha256(original).hexdigest() != chunk["chunk_hash"]:
        return None, bytes_read
    return original, bytes_read


def repair_erasure(ctx, job, digest):
    chunk = load_erasure_chunk(ctx, digest)
    if chunk is None:
        return True
    healthy, missing = inspect_erasure(ctx, chunk)
    if not missing:
        return True

    log(f"[{WORKER_NAME}] DEGRADED erasure {digest[:12]} "
        f"healthy_shards={len(healthy)} missing=" + str([
            f"#{s['shard_index']}@{s['node_id']}({s['state']})"
            for s in missing
        ]))

    for shard in missing:
        if shard["state"] in ("corrupt", "missing"):
            delete_copy(ctx, shard["node_id"], shard["shard_hash"])

    # Deduplication: objects that share this chunk may have been written
    # with different shard layouts, so the same shard can be stored on
    # several nodes. If an identical shard is still healthy on another
    # node, point the lost location to it; nothing has to be rebuilt.
    healthy_copy = {}
    for shard in healthy:
        healthy_copy.setdefault(shard["shard_hash"], shard["node_id"])
    to_rebuild = []
    for shard in missing:
        node_id = healthy_copy.get(shard["shard_hash"])
        if node_id is None:
            to_rebuild.append(shard)
            continue
        metadata(ctx, "POST", "/erasure/shards/relocate", json={
            "shard_hash": shard["shard_hash"],
            "old_node": shard["node_id"],
            "new_node": node_id
        })
        job.event(digest, "erasure", node_id, 0, 0)
        log(f"[{WORKER_NAME}] RELINKED shard #{shard['shard_index']} of "
            f"{digest[:12]} -> {node_id} (identical copy already there)")
    missing = to_rebuild
    if not missing:
        return True

    original, bytes_read = decode_chunk(ctx, chunk, healthy)
    if original is None:
        log(f"[{WORKER_NAME}] REPAIR FAILED {digest[:12]}: fewer than "
            f"{K} valid shards (data unrecoverable)")
        return False

    rebuilt = encoder.encode(original)
    used = {s["node_id"] for s in healthy}
    all_ok = True

    for shard in missing:
        data = rebuilt[shard["shard_index"]]
        if hashlib.sha256(data).hexdigest() != shard["shard_hash"]:
            all_ok = False
            continue

        # Same node if it is alive, otherwise a node that holds no
        # other shard of this chunk (keeps one shard per node).
        candidates = [shard["node_id"]] if ctx.alive(shard["node_id"]) else []
        candidates += [
            n for n in ctx.view.order(digest)
            if ctx.alive(n) and n not in used and n != shard["node_id"]
        ]

        target = None
        for node_id in candidates:
            if node_id in used:
                continue
            if store(ctx, node_id, shard["shard_hash"], data):
                target = node_id
                break

        if target is None:
            log(f"[{WORKER_NAME}] STILL DEGRADED {digest[:12]} shard "
                f"#{shard['shard_index']}: no spare node available")
            all_ok = False
            continue

        used.add(target)
        if target != shard["node_id"]:
            metadata(ctx, "POST", "/erasure/shards/relocate", json={
                "shard_hash": shard["shard_hash"],
                "old_node": shard["node_id"],
                "new_node": target
            })
        job.event(digest, "erasure", target, bytes_read, len(data))
        bytes_read = 0  # the K shards are read once per chunk
        log(f"[{WORKER_NAME}] REBUILT shard #{shard['shard_index']} of "
            f"{digest[:12]} -> {target}")

    return all_ok


# -------------------------
# Rebalancing (node joined)
# -------------------------

def replicated_misplaced(ctx, digest, nodes):
    desired = set(ctx.view.desired(digest, REPLICATION_FACTOR))
    current = {n for n in nodes if n in ctx.online}
    return bool(desired - current)


def erasure_misplaced(ctx, chunk):
    desired = set(ctx.view.desired(chunk["chunk_hash"], K + M))
    nodes = [s["node_id"] for s in chunk["shards"] if s["node_id"] in ctx.online]
    return len(set(nodes)) != len(nodes) or bool(set(nodes) - desired)


def rebalance_replicated(ctx, job, digest):
    """Move copies to the nodes the ring now assigns to the chunk."""
    rows = metadata(ctx, "GET", "/chunks/placements",
                    params={"chunk_hash": digest})
    if not rows:
        return True
    size, nodes = rows[0]["size"], rows[0]["nodes"]
    healthy = [
        n for n in nodes
        if n in ctx.online and probe(ctx, n, digest, size) == "ok"
    ]
    desired = ctx.view.desired(digest, REPLICATION_FACTOR)
    to_add = [n for n in desired if n not in healthy and ctx.alive(n)]
    to_remove = [n for n in healthy if n not in desired]
    if not to_add:
        # Already on the assigned nodes: only extra copies (more than N,
        # found by the scrub) are removed.
        if len(healthy) - len(to_remove) >= REPLICATION_FACTOR:
            for node_id in to_remove:
                metadata(ctx, "DELETE",
                         f"/chunks/locations/{digest}/{node_id}")
                delete_copy(ctx, node_id, digest)
                log(f"[{WORKER_NAME}] REMOVED extra replica {digest[:12]} "
                    f"from {node_id}")
        return True

    source = None
    for node_id in healthy:
        source = fetch_verified(ctx, node_id, digest, size)
        if source is not None:
            break
    if source is None:
        return False  # leave it to the repair path

    bytes_read = len(source)
    for node_id in to_add:
        if not store(ctx, node_id, digest, source):
            return False
        metadata(ctx, "POST", "/chunks/locations",
                 json={"chunk_hash": digest, "node_id": node_id})
        job.event(digest, "replication", node_id, bytes_read, size)
        bytes_read = 0
        log(f"[{WORKER_NAME}] MOVED replica {digest[:12]} -> {node_id}")

    # The new copies are in place: drop the ones no longer assigned,
    # keeping exactly N copies (on the nodes the ring assigns).
    if len(healthy) + len(to_add) - len(to_remove) >= REPLICATION_FACTOR:
        for node_id in to_remove:
            metadata(ctx, "DELETE", f"/chunks/locations/{digest}/{node_id}")
            delete_copy(ctx, node_id, digest)
    return True


def rebalance_erasure(ctx, job, digest):
    chunk = load_erasure_chunk(ctx, digest)
    if chunk is None:
        return True
    desired = ctx.view.desired(digest, K + M)

    holders, misplaced = set(), []
    for shard in chunk["shards"]:
        node_id = shard["node_id"]
        if node_id in desired and node_id not in holders:
            holders.add(node_id)
        elif node_id in ctx.online:
            misplaced.append(shard)  # wrong node, or a second shard there

    free = [n for n in desired if n not in holders and ctx.alive(n)]
    ok = True
    for shard, target in zip(misplaced, free):
        data = fetch_verified(
            ctx, shard["node_id"], shard["shard_hash"], shard["shard_size"]
        )
        if data is None or not store(ctx, target, shard["shard_hash"], data):
            ok = False
            continue
        metadata(ctx, "POST", "/erasure/shards/relocate", json={
            "shard_hash": shard["shard_hash"],
            "old_node": shard["node_id"],
            "new_node": target
        })
        delete_copy(ctx, shard["node_id"], shard["shard_hash"])
        job.event(digest, "erasure", target, len(data), len(data))
        log(f"[{WORKER_NAME}] MOVED shard #{shard['shard_index']} of "
            f"{digest[:12]} {shard['node_id']} -> {target}")
    return ok


# -------------------------
# Garbage collection
# -------------------------

def garbage_collect(ctx, digest, shards):
    """
    Delete a chunk nobody references any more. The metadata service
    removes the record only if the reference count is still zero, so a
    chunk re-uploaded in the meantime is kept.
    """
    purge = metadata(ctx, "DELETE", f"/chunks/{digest}")
    if not purge["purged"] and purge.get("reason") != "not registered":
        log(f"[{WORKER_NAME}] GC skipped {digest[:12]}: {purge['reason']}")
        return True

    failures = 0
    for node_id in ctx.view.registered:
        if not delete_copy(ctx, node_id, digest):
            failures += 1
    for node_id, shard_hash in shards:
        if not delete_copy(ctx, node_id, shard_hash):
            failures += 1

    log(f"[{WORKER_NAME}] GC deleted {digest[:12]} "
        f"({len(shards)} shards, {failures} nodes unreachable)")
    return failures == 0
