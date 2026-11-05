# Distributed Object Storage (Distributed Systems midterm)

A distributed object store: files are split into SHA-256 content-addressed
chunks, placed on storage nodes with consistent hashing, kept available with
replication (N=3, W=2, R=2) or Reed–Solomon erasure coding (4+2), repaired
automatically, and exposed through an S3-like API, a web UI and Grafana.

## Architecture

```
            Web UI (React, :3000)          Grafana (:3001) <- Prometheus (:9090)
                   |                                              ^ metrics
             nginx load balancer (:8020)                          |
              /                \                                  |
        gateway-1           gateway-2  ---- RabbitMQ (:15672) ----+
              \                /             storage.repair / rebalance / gc
            metadata service (:8010)               |
                   |                     repair-scheduler -> 2 repair workers
     HAProxy (:7000) -> Patroni pg1/pg2/pg3   (detects)         (repairs)
                        + etcd1/2/3                |
                                                   v
        storage-node-1 ... storage-node-7 (+8)  (each with its own volume)
                                Elasticsearch (object search)
```

| Component | Folder | Role |
|---|---|---|
| Storage node | `storage_node/` | PUT/GET/HEAD/DELETE chunks, SHA-256 check on every read, heartbeats (id, URL, free space, chunk count) |
| Metadata service | `metadata_service/` | buckets, objects, versions, chunk reference counts, chunk locations, nodes, repair jobs |
| Gateway | `gateway/` | S3-like API, chunking, placement (consistent hashing), replication/erasure coding, search, API key + rate limit |
| Repair | `repair_worker/` | `ROLE=scheduler`: failure detection, scrubbing, rebalancing; `ROLE=worker`: executes queued jobs |
| Shared code | `common/` | cluster membership + hash ring, RabbitMQ queues |
| Web UI | `web-ui/` | upload/download/browse, search, cluster + repair monitoring |
| Metadata DB | `patroni/` | PostgreSQL replicated with Patroni, 3-member etcd, HAProxy to the leader |
| Load balancer | `lb/` | nginx in front of the two gateways |
| Monitoring | `monitoring/` | Prometheus config, Grafana datasource + dashboard |
| Kubernetes | `k8s/` | StatefulSet, probes, HPA, cloud deployment guide ([k8s/README.md](k8s/README.md)) |

## Run locally (Docker Compose)

```powershell
copy .env.example .env      # then edit the passwords
docker volume create ds-midterm-node1-data   # ... up to node6 (first time only)
docker compose -f compose.storage.yaml --env-file .env up -d --build
```

| URL | What |
|---|---|
| http://localhost:8020 | API (load balancer), docs at `/docs` of a gateway |
| http://localhost:3000 | Web UI (enter the API key) |
| http://localhost:3001 | Grafana dashboard "Distributed Object Storage" |
| http://localhost:9090 | Prometheus |
| http://localhost:15672 | RabbitMQ management (user `ds`) |
| http://localhost:7000 | HAProxy stats: which PostgreSQL member is the leader |

Add an 8th storage node (rebalancing):

```powershell
docker compose -f compose.storage.yaml --env-file .env --profile scale-out up -d storage-node-8
```

## Tests and benchmarks

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:GATEWAY_API_KEY = "<your key>"
python -m pytest tests -v
python -m scripts.workload_benchmark --workload small --failure-test
python -m scripts.workload_benchmark --workload large --failure-test
```

Other scripts: `scripts/compare_chunking.py` (fixed-size vs content-defined
chunking), `scripts/test_placement.py` (data moved when nodes are
added/removed).

## Where each requirement is implemented

| Task | Implementation |
|---|---|
| 2 Storage node | `storage_node/main.py` |
| 3 Chunking, metadata, placement, dedup | `metadata_service/chunking.py`, `placement.py`, `main.py`; garbage collection in `repair_worker/core.py` |
| 4 Replication, erasure coding, repair, scrubbing | `gateway/main.py` (writes/reads), `repair_worker/` (detection, repair), `scripts/workload_benchmark.py` |
| 5 API, auth, rate limit | `gateway/main.py`, `tests/test_gateway.py` |
| 6 Web UI, Elasticsearch | `web-ui/src/App.jsx`, `gateway/search_index.py` |
| 7 Load balancer | `lb/nginx.conf` (2 gateways) |
| 7 Job queue | `common/jobqueue.py`, `repair_worker/scheduler.py`, `repair_worker/worker.py` |
| 7 StatefulSet + probes | `k8s/30-storage-nodes.yaml` |
| 7 Replicated metadata store | `patroni/` + etcd services in `compose.storage.yaml` |
| 7 Prometheus & Grafana | `monitoring/`, `/metrics` on every service |
| 8 Dockerfiles, Compose, Kubernetes, HPA, rebalancing | `*/Dockerfile`, `compose.storage.yaml`, `k8s/` |
