"""
Cluster membership as seen by the metadata service.

Storage nodes announce themselves (id + URL) in their heartbeats, so the
set of nodes is not hard-coded: a node that starts sending heartbeats
joins the consistent hashing ring automatically.

  registered  every node that ever sent a heartbeat (ring members)
  online      nodes with a heartbeat in the last 15 s
"""

import os
import threading
import time

import httpx

from metadata_service.placement import ConsistentHashRing

VIRTUAL_NODES = 128

# Used only if the metadata service cannot be reached before the first
# successful refresh (Docker Compose naming).
FALLBACK_COUNT = int(os.getenv("STORAGE_NODE_COUNT", "7"))
FALLBACK_URLS = {
    f"node{i}": f"http://storage-node-{i}:8000"
    for i in range(1, FALLBACK_COUNT + 1)
}

_rings = {}
_rings_lock = threading.Lock()


def ring_for(node_ids):
    """Consistent hashing ring for a set of nodes (cached)."""
    key = frozenset(node_ids)
    with _rings_lock:
        ring = _rings.get(key)
        if ring is None:
            ring = ConsistentHashRing(sorted(key), VIRTUAL_NODES)
            _rings[key] = ring
        return ring


class ClusterView:
    def __init__(self, metadata_url, ttl=3.0):
        self.metadata_url = metadata_url
        self.ttl = ttl
        self.lock = threading.Lock()
        self.loaded_at = 0.0
        self.nodes = []
        self.urls = dict(FALLBACK_URLS)
        self.online = set(FALLBACK_URLS)

    def refresh(self, client=None, force=False):
        with self.lock:
            if not force and time.monotonic() - self.loaded_at < self.ttl:
                return self
            try:
                own = client is None
                client = client or httpx.Client(timeout=5)
                try:
                    r = client.get(f"{self.metadata_url}/nodes")
                    r.raise_for_status()
                    nodes = r.json()
                finally:
                    if own:
                        client.close()
            except httpx.HTTPError:
                return self  # keep the last known view

            urls = {n["node_id"]: n["url"] for n in nodes if n.get("url")}
            if urls:
                self.nodes = nodes
                self.urls = urls
                self.online = {
                    n["node_id"] for n in nodes
                    if n["online"] and n["node_id"] in urls
                }
            self.loaded_at = time.monotonic()
            return self

    @property
    def registered(self):
        return set(self.urls)

    @property
    def ring(self):
        return ring_for(self.urls)

    def url(self, node_id):
        return self.urls.get(node_id)

    def order(self, chunk_hash):
        """All nodes, clockwise around the ring from the chunk hash."""
        return self.ring.get_nodes(chunk_hash, len(self.urls))

    def placement(self, chunk_hash, count):
        """
        Write targets: the first `count` live nodes clockwise; failed
        nodes are used only if there are not enough live ones.
        """
        order = self.order(chunk_hash)
        live = [n for n in order if n in self.online]
        dead = [n for n in order if n not in self.online]
        return (live + dead)[:count]

    def desired(self, chunk_hash, count):
        """Where the copies/shards of a chunk should be right now."""
        return [n for n in self.order(chunk_hash) if n in self.online][:count]
