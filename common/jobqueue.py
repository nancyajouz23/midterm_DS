"""
RabbitMQ job queues.

  storage.repair     rebuild missing / corrupted copies and shards
  storage.rebalance  move data onto newly added nodes
  storage.gc         delete chunks that no object references any more

Queues are durable and messages persistent, so jobs survive a broker
restart. Workers take one message at a time (prefetch 1) and
acknowledge it only after the work is done, so a worker that crashes
mid-job hands its message to another worker.
"""

import json
import os
import time

import pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "")

QUEUES = {
    "repair": "storage.repair",
    "rebalance": "storage.rebalance",
    "gc": "storage.gc",
}


def connect(url=None, attempts=30, delay=2.0):
    params = pika.URLParameters(url or RABBITMQ_URL)
    params.heartbeat = 600
    params.blocked_connection_timeout = 300
    last = None
    for _ in range(attempts):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPError as exc:
            last = exc
            time.sleep(delay)
    raise last


def declare(channel):
    for queue in QUEUES.values():
        channel.queue_declare(queue=queue, durable=True)


class Publisher:
    """Long-lived publisher (one per thread; pika is not thread-safe)."""

    def __init__(self, url=None):
        self.url = url or RABBITMQ_URL
        self.connection = None
        self.channel = None

    def _open(self):
        if self.connection is None or self.connection.is_closed:
            self.connection = connect(self.url)
            self.channel = self.connection.channel()
            declare(self.channel)

    def publish(self, task, messages):
        for attempt in range(2):
            try:
                self._open()
                for message in messages:
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=QUEUES[task],
                        body=json.dumps(message),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # persistent
                            content_type="application/json"
                        )
                    )
                return
            except pika.exceptions.AMQPError:
                self.connection = None
                if attempt:
                    raise

    def queue_depths(self):
        self._open()
        return {
            task: self.channel.queue_declare(
                queue=queue, durable=True, passive=True
            ).method.message_count
            for task, queue in QUEUES.items()
        }


def publish_once(task, messages, url=None):
    """Open a connection, publish, close (for occasional publishers)."""
    url = url or RABBITMQ_URL
    if not url:
        raise RuntimeError("RABBITMQ_URL not configured")
    connection = connect(url, attempts=2, delay=0.5)
    try:
        channel = connection.channel()
        declare(channel)
        for message in messages:
            channel.basic_publish(
                exchange="",
                routing_key=QUEUES[task],
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
    finally:
        connection.close()
