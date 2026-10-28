
import hashlib
import re
from bisect import bisect_left, insort


class ConsistentHashRing:
    def __init__(self, nodes=None, virtual_nodes=128):
        if virtual_nodes < 1:
            raise ValueError("virtual_nodes must be positive")

        self.virtual_nodes = virtual_nodes
        self.ring = []
        self.nodes = set()

        for node in nodes or []:
            self.add_node(node)

    @staticmethod
    def _hash(value):
        return int(
            hashlib.sha256(value.encode("utf-8")).hexdigest(),
            16
        )

    def add_node(self, node_id):
        """Add a physical node with multiple virtual positions."""
        if not node_id or node_id in self.nodes:
            raise ValueError("Invalid or duplicate node ID")

        self.nodes.add(node_id)

        for index in range(self.virtual_nodes):
            position = self._hash(f"{node_id}:{index}")
            insort(self.ring, (position, node_id))

    def remove_node(self, node_id):
        """Remove a node and all its virtual positions."""
        if node_id not in self.nodes:
            raise ValueError(f"Unknown node: {node_id}")

        self.nodes.remove(node_id)
        self.ring = [
            entry for entry in self.ring
            if entry[1] != node_id
        ]

    def get_nodes(self, chunk_hash, replication_factor=1):
        """
        Return distinct physical nodes responsible for a chunk,
        ordered clockwise around the consistent hashing ring.
        """
        if not re.fullmatch(r"[a-fA-F0-9]{64}", chunk_hash):
            raise ValueError("Expected a SHA-256 chunk hash")

        if replication_factor < 1:
            raise ValueError("Replication factor must be positive")

        if len(self.nodes) < replication_factor:
            raise ValueError(
                "Not enough nodes for the requested replication factor"
            )

        position = int(chunk_hash, 16)
        ring_positions = [entry[0] for entry in self.ring]

        index = bisect_left(ring_positions, position)

        selected = []
        seen = set()

        for offset in range(len(self.ring)):
            node_id = self.ring[
                (index + offset) % len(self.ring)
            ][1]

            if node_id not in seen:
                selected.append(node_id)
                seen.add(node_id)

                if len(selected) == replication_factor:
                    break

        return selected

    def get_primary_node(self, chunk_hash):
        return self.get_nodes(chunk_hash, 1)[0]
