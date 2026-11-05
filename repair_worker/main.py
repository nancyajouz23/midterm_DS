"""
Entry point of the repair service.

ROLE=scheduler  detect failures, scrub, notice new nodes, queue jobs
ROLE=worker     execute repair / rebalance / GC jobs from RabbitMQ
"""

import os

ROLE = os.getenv("ROLE", "worker")

if __name__ == "__main__":
    if ROLE == "scheduler":
        from repair_worker.scheduler import main
    else:
        from repair_worker.worker import main
    main()
