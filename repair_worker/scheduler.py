"""
Repair scheduler: detects problems and queues the work.

Every INTERVAL seconds (main loop):
  * reads heartbeats: a node silent for 15 s (3 missed heartbeats) is
    FAILED; every chunk with a copy/shard on it is queued for repair,
  * a node that joined the cluster (first heartbeat) triggers a
    rebalance: chunks the ring now assigns to it are queued for moving.

Every SCRUB_INTERVAL seconds (background thread):
  * scrubbing: every copy on a live node is re-read and its SHA-256
    checked by the storage node; damaged chunks are queued for repair.

Each batch of queued work is a job (repair_jobs table) whose progress
is updated by the workers that process its messages.
"""

import os
import threading
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server

from common.jobqueue import Publisher
from repair_worker import core
from repair_worker.core import log, metadata

INTERVAL = int(os.getenv("REPAIR_INTERVAL", "10"))
SCRUB_INTERVAL = int(os.getenv("SCRUB_INTERVAL", "30"))
JOB_TIMEOUT = int(os.getenv("JOB_TIMEOUT", "900"))
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))
# After the metadata service was unreachable, wait until every live node
# has had time to send a heartbeat again before declaring failures.
FAILURE_GRACE = HEARTBEAT_TIMEOUT + 10

PUBLISHED = Counter(
    "storage_scheduler_published", "Messages queued", ["task"]
)
SCRUB_SECONDS = Gauge(
    "storage_scheduler_scrub_seconds", "Duration of the last scrub pass"
)
QUEUE_DEPTH = Gauge(
    "storage_scheduler_queue_depth", "Messages waiting", ["task"]
)

# Only one batch of work is queued at a time; both loops check this.
schedule_lock = threading.Lock()


class Scheduler:
    def __init__(self, name):
        self.name = name
        self.publisher = Publisher()
        self.known_failed = set()
        self.unrepairable = None  # work that failed last time
        self.metadata_error_at = time.monotonic()  # grace period at start

    # ----- jobs -----

    def active_job(self, ctx):
        """The running job, after closing jobs that are stuck."""
        for job in metadata(ctx, "GET", "/repair-jobs",
                            params={"status": "running", "limit": 50}):
            started = datetime.fromisoformat(job["started_at"])
            age = (datetime.now(timezone.utc) - started).total_seconds()
            if age > JOB_TIMEOUT:
                metadata(ctx, "PATCH", f"/repair-jobs/{job['job_id']}",
                         json={"status": "partial"})
                log(f"JOB {job['job_id']} TIMED OUT after {int(age)}s")
                continue
            return job
        return None

    def queue_job(self, ctx, task, job_type, reason, items, failed=()):
        """Create a job and queue one message per chunk."""
        job = metadata(ctx, "POST", "/repair-jobs", json={
            "reason": reason,
            "failed_nodes": sorted(failed),
            "chunks_total": len(items),
            "job_type": job_type
        })
        messages = [
            {"job_id": job["job_id"], "mode": mode, "chunk_hash": digest}
            for mode, digest in items
        ]
        self.publisher.publish(task, messages)
        PUBLISHED.labels(task).inc(len(messages))
        log(f"{job_type.upper()} JOB {job['job_id']} QUEUED ({reason}): "
            f"{len(items)} chunks -> queue storage.{task}")
        return job["job_id"]

    def skip_unrepairable(self, ctx, items):
        key = (frozenset(ctx.online), frozenset(items))
        if key == self.unrepairable:
            log(f"WAITING: {len(items)} chunks still degraded and cannot be "
                "repaired until a node joins or recovers")
            return True
        return False

    def remember_result(self, ctx, job_id, items):
        """After a job: if nothing could be repaired, do not retry it."""
        jobs = metadata(ctx, "GET", "/repair-jobs", params={"limit": 20})
        job = next((j for j in jobs if j["job_id"] == job_id), None)
        if job and job["status"] != "running" and job["chunks_failed"] \
                and job["chunks_repaired"] == 0:
            self.unrepairable = (frozenset(ctx.online), frozenset(items))
        elif job and job["status"] != "running":
            self.unrepairable = None

    # ----- main loop: failures and new nodes -----

    def tick(self, ctx):
        if time.monotonic() - ctx.view.loaded_at > 2:
            # The node list could not be refreshed: metadata service or
            # its database is down. Decide nothing on stale data.
            self.metadata_error_at = time.monotonic()
            log("METADATA UNAVAILABLE: skipping this round")
            return
        nodes = ctx.view.nodes
        registered = {n["node_id"] for n in nodes if n.get("url")}
        failed = registered - ctx.online

        # Safeguards against false failures: while the metadata
        # database was unavailable no heartbeat could be recorded, so
        # every node looks silent for a while afterwards.
        if failed:
            since_outage = time.monotonic() - self.metadata_error_at
            if since_outage < FAILURE_GRACE:
                log(f"GRACE PERIOD: metadata was unavailable "
                    f"{int(since_outage)}s ago; waiting for heartbeats from "
                    f"{sorted(failed)} before declaring failures")
                return
            if len(failed) > len(registered) / 2:
                log(f"TOO MANY NODES SILENT ({len(failed)}/{len(registered)})"
                    ": probably a metadata or network problem, not repairing")
                return

        for node_id in sorted(failed - self.known_failed):
            seconds = next(n["seconds_since_heartbeat"] for n in nodes
                           if n["node_id"] == node_id)
            log(f"NODE FAILED {node_id}: no heartbeat for {seconds}s "
                "-> repair triggered")
        for node_id in sorted(self.known_failed - failed):
            log(f"NODE RECOVERED {node_id}: heartbeats received again")
        self.known_failed = failed

        if self.active_job(ctx):
            return

        joined = sorted(
            n["node_id"] for n in nodes
            if n["node_id"] in ctx.online and not n["rebalanced"]
        )
        if joined:
            self.rebalance(ctx, joined)
            return

        if failed:
            self.repair_failed_nodes(ctx, failed)

    def repair_failed_nodes(self, ctx, failed):
        params = {"nodes": sorted(failed)}
        items = [
            ("replication", c["chunk_hash"])
            for c in metadata(ctx, "GET", "/chunks/placements", params=params)
        ] + [
            ("erasure", c["chunk_hash"])
            for c in metadata(ctx, "GET", "/erasure/chunks", params=params)
        ]
        if not items or self.skip_unrepairable(ctx, items):
            return
        reason = "node failure (missed heartbeats): " + ", ".join(sorted(failed))
        self.last_job = (self.queue_job(
            ctx, "repair", "repair", reason, items, failed
        ), items)

    def rebalance(self, ctx, joined):
        items = [
            ("replication", c["chunk_hash"])
            for c in metadata(ctx, "GET", "/chunks/placements")
            if core.replicated_misplaced(ctx, c["chunk_hash"], c["nodes"])
        ] + [
            ("erasure", c["chunk_hash"])
            for c in metadata(ctx, "GET", "/erasure/chunks")
            if core.erasure_misplaced(ctx, c)
        ]
        reason = f"rebalance: {', '.join(joined)} joined the cluster"
        if items:
            self.queue_job(ctx, "rebalance", "rebalance", reason, items)
        else:
            log(f"REBALANCE: {', '.join(joined)} joined, nothing to move")
        for node_id in joined:
            metadata(ctx, "POST", f"/nodes/{node_id}/rebalanced")

    # ----- background thread: scrubbing -----

    def scrub(self, ctx):
        start = time.perf_counter()
        replicated = metadata(ctx, "GET", "/chunks/placements")
        erasure = metadata(ctx, "GET", "/erasure/chunks")

        items = []
        for chunk in replicated:
            state = core.inspect_replicated(
                ctx, chunk["chunk_hash"], chunk["size"], chunk["nodes"]
            )
            healthy, lost, corrupt, waiting = state
            # Copies lost on failed nodes are handled by the main loop;
            # here: corrupted/missing copies and under-replicated chunks.
            if corrupt or len(healthy) + len(waiting) + len(lost) \
                    < core.REPLICATION_FACTOR:
                items.append(("replication", chunk["chunk_hash"]))

        for chunk in erasure:
            healthy, missing = core.inspect_erasure(ctx, chunk)
            if any(s["state"] in ("corrupt", "missing") for s in missing):
                items.append(("erasure", chunk["chunk_hash"]))

        SCRUB_SECONDS.set(time.perf_counter() - start)
        log(f"SCRUB COMPLETE: {len(replicated)} replicated + {len(erasure)} "
            f"erasure-coded chunks checked in "
            f"{time.perf_counter() - start:.1f}s, {len(items)} damaged")
        return items


def main_loop(scheduler):
    while True:
        try:
            with core.new_client() as client:
                ctx = core.Context(client, force_refresh=True)
                with schedule_lock:
                    scheduler.tick(ctx)
                    if getattr(scheduler, "last_job", None):
                        job_id, items = scheduler.last_job
                        scheduler.remember_result(ctx, job_id, items)
                try:
                    for task, depth in scheduler.publisher.queue_depths().items():
                        QUEUE_DEPTH.labels(task).set(depth)
                except Exception:
                    pass
        except Exception as exc:
            scheduler.metadata_error_at = time.monotonic()
            log(f"SCHEDULER ERROR: {exc}")
        time.sleep(INTERVAL)


def scrub_loop(scheduler):
    while True:
        time.sleep(SCRUB_INTERVAL)
        try:
            with core.new_client() as client:
                ctx = core.Context(client, force_refresh=True)
                if getattr(scheduler, "last_job", None):
                    scheduler.remember_result(ctx, *scheduler.last_job)
                items = scheduler.scrub(ctx)
                if not items:
                    scheduler.unrepairable = None
                    continue
                with schedule_lock:
                    if scheduler.active_job(ctx) or \
                            scheduler.skip_unrepairable(ctx, items):
                        continue
                    scheduler.last_job = (scheduler.queue_job(
                        ctx, "repair", "repair",
                        "scrub: corrupted or missing copies", items
                    ), items)
        except Exception as exc:
            log(f"SCRUB ERROR: {exc}")


def main():
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    log(f"SCHEDULER started: heartbeat check every {INTERVAL}s, "
        f"scrub every {SCRUB_INTERVAL}s")
    main_scheduler = Scheduler("main")
    # The scrub thread has its own RabbitMQ connection (pika
    # connections are not thread-safe).
    scrub_scheduler = Scheduler("scrub")
    threading.Thread(
        target=scrub_loop, args=(scrub_scheduler,), daemon=True
    ).start()
    main_loop(main_scheduler)
