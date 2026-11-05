"""
Queue worker: takes repair, rebalance and garbage-collection jobs from
RabbitMQ and executes them. Several workers run in parallel; RabbitMQ
hands each message to exactly one of them (round-robin, one message at
a time), so the work is spread across the workers.
"""

import json
import os
import time

import pika
from prometheus_client import Counter, Histogram, start_http_server

from common import jobqueue
from repair_worker import core
from repair_worker.core import WORKER_NAME, log

TASKS = Counter(
    "storage_worker_tasks", "Queue messages processed", ["task", "result"]
)
DURATION = Histogram(
    "storage_worker_task_seconds", "Time to process one message", ["task"]
)


def handle(task, message):
    with core.new_client() as client:
        ctx = core.Context(client)
        job = core.JobReporter(ctx, message.get("job_id"), task)
        digest = message["chunk_hash"]

        try:
            if task == "repair":
                ok = (core.repair_replicated if message["mode"] == "replication"
                      else core.repair_erasure)(ctx, job, digest)
            elif task == "rebalance":
                ok = (core.rebalance_replicated
                      if message["mode"] == "replication"
                      else core.rebalance_erasure)(ctx, job, digest)
            else:
                ok = core.garbage_collect(ctx, digest, message.get("shards", []))
        except Exception as exc:
            log(f"[{WORKER_NAME}] {task} {digest[:12]} ERROR: {exc}")
            ok = False

        try:
            job.progress(ok)
        except Exception as exc:
            log(f"[{WORKER_NAME}] could not report progress: {exc}")
        return ok


def consume():
    connection = jobqueue.connect()
    channel = connection.channel()
    jobqueue.declare(channel)
    channel.basic_qos(prefetch_count=1)

    def callback_for(task):
        def callback(ch, method, properties, body):
            start = time.perf_counter()
            try:
                ok = handle(task, json.loads(body))
            except Exception as exc:  # malformed message
                log(f"[{WORKER_NAME}] bad {task} message: {exc}")
                ok = False
            DURATION.labels(task).observe(time.perf_counter() - start)
            TASKS.labels(task, "ok" if ok else "failed").inc()
            # Acknowledge only after the work is done: if this worker
            # dies first, RabbitMQ redelivers the message to another one.
            ch.basic_ack(delivery_tag=method.delivery_tag)
        return callback

    for task, queue in jobqueue.QUEUES.items():
        channel.basic_consume(queue=queue, on_message_callback=callback_for(task))

    log(f"WORKER {WORKER_NAME} waiting for jobs on "
        f"{', '.join(jobqueue.QUEUES.values())}")
    channel.start_consuming()


def main():
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))
    while True:
        try:
            consume()
        except pika.exceptions.AMQPError as exc:
            log(f"WORKER {WORKER_NAME}: lost RabbitMQ connection ({exc}); "
                "reconnecting")
            time.sleep(3)
