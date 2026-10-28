
import hashlib
from collections import Counter

from metadata_service.placement import ConsistentHashRing

INITIAL_NODES = ["node1", "node2", "node3", "node4"]
SAMPLE_SIZE = 10000


def generate_chunk_hashes():
    """Generate deterministic sample chunk identifiers."""
    return [
        hashlib.sha256(
            f"benchmark-chunk-{i}".encode()
        ).hexdigest()
        for i in range(SAMPLE_SIZE)
    ]


def get_placements(ring, hashes):
    return {
        chunk_hash: ring.get_primary_node(chunk_hash)
        for chunk_hash in hashes
    }


def print_distribution(label, placements):
    counts = Counter(placements.values())

    print(f"\n=== {label} ===")

    for node_id in sorted(counts):
        count = counts[node_id]
        percentage = count / SAMPLE_SIZE * 100

        print(
            f"{node_id}: {count} chunks "
            f"({percentage:.2f}%)"
        )


def compare_placements(before, after):
    moved = sum(
        before[chunk_hash] != after[chunk_hash]
        for chunk_hash in before
    )

    percentage = moved / len(before) * 100
    return moved, percentage


def main():
    hashes = generate_chunk_hashes()

    # Initial cluster: four nodes
    ring = ConsistentHashRing(INITIAL_NODES)
    initial = get_placements(ring, hashes)

    print("CONSISTENT HASHING PLACEMENT BENCHMARK")
    print(f"Sample chunks: {SAMPLE_SIZE}")
    print(f"Virtual nodes per physical node: {ring.virtual_nodes}")

    print_distribution("INITIAL FOUR NODES", initial)

    # Check three distinct replication targets
    example_hash = hashes[0]
    replica_targets = ring.get_nodes(
        example_hash, replication_factor=3
    )

    print("\nExample chunk replica targets:")
    print(replica_targets)

    # Add a fifth node
    ring.add_node("node5")
    after_add = get_placements(ring, hashes)

    print_distribution("AFTER ADDING NODE 5", after_add)

    moved_add, percent_add = compare_placements(
        initial, after_add
    )

    # Return to initial cluster
    ring.remove_node("node5")

    # Remove Node 3
    ring.remove_node("node3")
    after_remove = get_placements(ring, hashes)

    print_distribution("AFTER REMOVING NODE 3", after_remove)

    moved_remove, percent_remove = compare_placements(
        initial, after_remove
    )

    print("\n=== FINAL PLACEMENT RESULTS ===")

    print(
        f"Adding node5: {moved_add}/{SAMPLE_SIZE} "
        f"primary placements changed ({percent_add:.2f}%)"
    )

    print(
        f"Removing node3: {moved_remove}/{SAMPLE_SIZE} "
        f"primary placements changed ({percent_remove:.2f}%)"
    )

    print(
        f"Unchanged after adding: {100 - percent_add:.2f}%"
    )

    print(
        f"Unchanged after removing: {100 - percent_remove:.2f}%"
    )


if __name__ == "__main__":
    main()
