# Tasks 7 and 8 – report paragraphs, tests and screenshots

Run everything in **PowerShell** in the project folder. Start with:

```powershell
docker compose -f compose.storage.yaml --env-file .env --profile scale-out up -d
.\.venv\Scripts\Activate.ps1
$key = (Select-String -Path .env -Pattern '^GATEWAY_API_KEY=').Line.Split('=',2)[1]; $h = @{"X-API-Key" = $key}; $env:GATEWAY_API_KEY = $key
```

📸 = take a screenshot.

---

## TASK 7.1 – Load balancer in front of two gateways

**Paragraph**
> An nginx load balancer was placed in front of two identical gateway instances (gateway-1 and gateway-2). Clients and the web UI only use the load balancer (port 8020), which distributes requests round-robin across the gateways. Every response carries an `X-Served-By` header that names the instance that handled it. If a gateway stops, nginx stops trying to reach it after 1 second, retries the request on the other instance and skips the failed one, so clients keep working. The gateways hold no state of their own (everything is in the metadata database and on the storage nodes), so any instance can serve any request.

**Test**
1. Requests alternate between the two gateways. 📸
   ```powershell
   1..6 | % { (Invoke-WebRequest http://localhost:8020/health -UseBasicParsing).Headers["X-Served-By"] }
   ```
2. Stop one gateway and repeat: every request still succeeds and is served by gateway-2. 📸
   ```powershell
   docker stop gateway-1
   1..6 | % { $r = Invoke-WebRequest http://localhost:8020/buckets -Headers $h -UseBasicParsing; "$($r.StatusCode) $($r.Headers['X-Served-By'])" }
   docker start gateway-1
   ```
3. Grafana → panel **"Throughput per gateway instance"** shows both lines. 📸

## TASK 7.2 – Job queue (RabbitMQ) and workers

**Paragraph**
> Repair, rebalancing and garbage-collection work is distributed through RabbitMQ. The repair service runs in two roles. The **scheduler** detects work: nodes that missed their heartbeats, damaged copies found by scrubbing, and new nodes. It creates a job and publishes one message per affected chunk to the durable queues `storage.repair`, `storage.rebalance` and `storage.gc`. Two **workers** consume these queues. RabbitMQ hands each message to one worker at a time (prefetch 1), and a worker acknowledges a message only after the work is done, so a message from a crashed worker is redelivered to another one. Each worker reports its progress to the metadata service, and the last worker to finish a job closes it. When an object is deleted, the gateway publishes a GC message for every chunk that is no longer referenced, and a worker deletes its copies from the storage nodes.

**Test**
1. Open http://localhost:15672 (user `ds`, password `ds-rabbit` unless you set `RABBITMQ_PASSWORD`), go to **Queues**: the 3 queues each have **2 consumers**. 📸
2. Stop a node and watch the job spread over both workers:
   ```powershell
   docker stop storage-node-2
   ```
   Wait about 30 s, then run:
   ```powershell
   docker logs --since 2m midtermds-repair-scheduler-1 | Select-String "FAILED|QUEUED"
   (docker logs --since 2m midtermds-repair-worker-1 | Select-String "REPAIRED|REBUILT").Count
   (docker logs --since 2m midtermds-repair-worker-2 | Select-String "REPAIRED|REBUILT").Count
   docker start storage-node-2
   ```
   📸 The scheduler queued N chunks, and each worker repaired about half of them.
3. RabbitMQ → Queues → `storage.repair` → message rates graph during the repair. 📸

## TASK 7.3 – Storage nodes as a Kubernetes StatefulSet with probes

**Paragraph**
> On Kubernetes the storage nodes run as a StatefulSet. Each pod has a stable name (storage-node-0 … storage-node-N), a stable DNS name through a headless service, and its own PersistentVolumeClaim created from `volumeClaimTemplates`. If a pod is deleted or rescheduled, it comes back with the same name and the same volume, so its chunks are kept. A **readiness probe** on `/health` sends traffic only to nodes that are ready. A **liveness probe** restarts a node that stops answering: after 3 failed checks, 10 seconds apart, Kubernetes kills the container and starts it again. Each node registers its own address in its heartbeats, so no configuration lists the nodes.

**Test** (see `k8s/README.md` for the deployment)
1. Show the pods and their volumes. 📸
   ```powershell
   kubectl -n ds-storage get statefulset,pods,pvc
   ```
2. Delete a pod: it is recreated with the same name, volume and data. 📸
   ```powershell
   kubectl -n ds-storage exec storage-node-2 -- sh -c "ls /data | wc -l"
   kubectl -n ds-storage delete pod storage-node-2
   kubectl -n ds-storage get pod storage-node-2
   kubectl -n ds-storage exec storage-node-2 -- sh -c "ls /data | wc -l"
   ```
3. Liveness probe: freeze a node so it stops answering, and Kubernetes restarts it (RESTARTS becomes 1). 📸
   ```powershell
   docker exec desktop-control-plane sh -c "kill -STOP $(docker exec desktop-control-plane crictl inspect --output go-template --template '{{.info.pid}}' $(docker exec desktop-control-plane crictl ps --name storage-node --label io.kubernetes.pod.name=storage-node-4 -q))"
   ```
   If that command is too long, run its parts one at a time. Wait about 40 s, then:
   ```powershell
   kubectl -n ds-storage get pod storage-node-4
   kubectl -n ds-storage get events --field-selector involvedObject.name=storage-node-4
   ```
   The events show "Liveness probe failed" and then "will be restarted".

## TASK 7.4 – Replicated metadata store (Patroni + 3-member etcd)

**Paragraph**
> The metadata database is no longer a single point of failure. PostgreSQL runs as three members (pg1, pg2, pg3) managed by Patroni: one leader accepts writes and the two others are streaming replicas. Patroni stores the cluster state and the leader lock in a 3-member etcd cluster, which keeps working when one etcd member fails (it needs a majority of 2 of 3). If the leader stops, its lock expires and Patroni promotes the most up-to-date replica. HAProxy checks each member's Patroni API (`/primary`) and always forwards connections to the current leader, so the services need no change after a failover. When the old leader comes back, it rejoins as a replica.

**Test**
1. Show the cluster. 📸
   ```powershell
   docker exec pg1 /opt/patroni/bin/patronictl -c /etc/patroni.yml list
   ```
   Also open http://localhost:7000 (HAProxy: only the leader is green). 📸
2. Stop the leader (replace pg1 if another member is the leader):
   ```powershell
   docker stop pg1
   ```
3. After about 30 s, run the commands below. 📸 A new leader was elected and the API still works.
   ```powershell
   docker exec pg2 /opt/patroni/bin/patronictl -c /etc/patroni.yml list
   Invoke-RestMethod http://localhost:8020/buckets -Headers $h | Select-Object -First 3
   ```
4. Restart the old leader. 📸 pg1 is back as a **Replica**.
   ```powershell
   docker start pg1
   ```
   Wait 20 s, then:
   ```powershell
   docker exec pg2 /opt/patroni/bin/patronictl -c /etc/patroni.yml list
   ```
5. Optional, for the report: `docker logs --since 3m midtermds-repair-scheduler-1` shows
   `GRACE PERIOD ...`. The scheduler does not mistake the database outage for node failures.

## TASK 7.5 – Prometheus and Grafana

**Paragraph**
> Every service exposes Prometheus metrics on `/metrics`. Gateways report request rate, latency histograms and bytes uploaded or downloaded. Storage nodes report disk usage, free space and chunk count. The metadata service reports degraded objects, online nodes, and the chunks and bytes processed by repair and rebalance jobs. Workers report the jobs they processed, and RabbitMQ reports its queue depth. Prometheus finds new storage nodes automatically through the metadata service (`/prometheus/targets`), and all worker replicas through DNS. A Grafana dashboard, provisioned automatically, shows throughput and latency, load balancing, disk usage per node, chunk distribution, degraded objects, the repair and rebalance rate and traffic, and the job queue depth.

**Test**
1. http://localhost:9090/targets: all targets are **UP**. 📸
2. http://localhost:3001 (admin / admin, then skip the password change), open **Dashboards →
   Distributed Storage → Distributed Object Storage**. 📸 one screenshot of the top and one of the bottom.
3. Stop a node (as in 7.2) and take a screenshot while the dashboard shows
   **Degraded objects > 0** and the **repair rate** spike. 📸

---

## TASK 8.1 – Dockerfiles and Docker Compose

**Paragraph**
> Every service has its own Dockerfile: storage node, metadata, gateway, repair (scheduler and worker), web UI, and Patroni PostgreSQL. `compose.storage.yaml` runs the complete system locally: 7 storage nodes (plus an optional 8th), metadata, 2 gateways behind nginx, RabbitMQ, the scheduler and 2 workers, the Patroni/etcd/HAProxy database cluster, Elasticsearch, the web UI, Prometheus and Grafana. The test suite (`pytest`, 7 tests) runs against this deployment.

**Test** 📸 `docker compose -f compose.storage.yaml --env-file .env ps` and `python -m pytest tests -v` (7 passed).

## TASK 8.2 – Kubernetes deployment and gateway autoscaling

**Paragraph**
> The system is also deployed on Kubernetes with the manifests in `k8s/`: a namespace, PostgreSQL, the metadata service, the storage node StatefulSet, RabbitMQ with the scheduler and 2 workers, the gateway with a LoadBalancer Service, Elasticsearch and the web UI. Passwords come from Kubernetes Secrets. The gateway Deployment has a HorizontalPodAutoscaler: it keeps 2 to 6 pods, aiming for 50 % CPU, so it adds gateway pods under load and removes them a minute after the load stops. The same manifests deploy to a managed cloud cluster (AKS, EKS or GKE) after the images are pushed to a container registry; the steps are in `k8s/README.md`.

**Test**
1. 📸 `kubectl -n ds-storage get pods,svc,hpa`
2. The API (http://localhost:8030/health) and web UI (http://localhost:3030) run on Kubernetes. 📸
3. Autoscaling (needs metrics-server, see `k8s/README.md` §2). 📸 `kubectl -n ds-storage get hpa gateway --watch`
   during the load test, showing the replica count going up.

## TASK 8.3 – Adding a storage node triggers rebalancing

**Paragraph**
> When a storage node is added, it registers itself with its first heartbeat and is marked as "not yet rebalanced". The scheduler sees the new node and works out which chunks the consistent hashing ring now assigns to it. It creates a rebalance job and queues one message per chunk. The workers copy each chunk (or erasure-coded shard) to the new node, update the metadata, then delete the copy that is no longer assigned. Because of consistent hashing, only the chunks the ring assigns to the new node move, about 1/8 of the data when going from 7 to 8 nodes. All other chunks stay where they are.

**Test, Docker Compose** 📸 (do it in the web UI's Cluster Monitor tab, or in the terminal)
```powershell
docker compose -f compose.storage.yaml --env-file .env --profile scale-out up -d storage-node-8
docker logs -f midtermds-repair-scheduler-1      # REBALANCE JOB ... node8 joined
(Invoke-RestMethod http://localhost:8010/nodes) | Format-Table node_id, chunk_count
```

**Test, Kubernetes** 📸
```powershell
kubectl -n ds-storage scale statefulset storage-node --replicas=9
kubectl -n ds-storage logs deploy/repair-scheduler | Select-String REBALANCE
```

---

### Results from the developer test run (for reference)

| Test | Result |
|---|---|
| Node failure (Compose) | detected after 15 s, 309 chunks repaired in ~20 s, split 155 / 154 between the 2 workers |
| Database leader stopped | new leader elected, API working again after ~35 s, old leader rejoined as replica |
| Gateway stopped | all requests still succeed (first one ~1 s slower) |
| node8 added (Compose) | 389 chunks rebalanced in 36 s (275 / 273 moves per worker) |
| storage-node-7 added (Kubernetes) | 330 chunks rebalanced in 28 s (165 / 165) |
| Pod deleted (Kubernetes) | back in 5 s with the same PVC and all 353 chunks |
| Frozen node (Kubernetes) | liveness probe restarted it after 36 s |
