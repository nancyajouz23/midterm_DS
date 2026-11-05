# Kubernetes deployment

| Manifest | What it deploys |
|---|---|
| `00-namespace.yaml` | namespace `ds-storage` |
| `10-postgres.yaml` | metadata database (StatefulSet + PVC) |
| `20-metadata.yaml` | metadata service |
| `30-storage-nodes.yaml` | **storage nodes: StatefulSet, one PersistentVolumeClaim per node, liveness + readiness probes** |
| `40-queue-and-repair.yaml` | RabbitMQ, repair scheduler, 2 repair workers |
| `50-gateway.yaml` | gateway Deployment (2 pods), LoadBalancer Service, **HorizontalPodAutoscaler (2–6 pods, CPU 50 %)** |
| `60-search-and-ui.yaml` | Elasticsearch, web UI |
| `metrics-server.yaml` | metrics-server v0.7.2 (needed by the autoscaler, see below) |

## 1. Deploy on Docker Desktop (local)

```bash
bash scripts/k8s_load_images.sh
kubectl apply -f k8s/00-namespace.yaml
kubectl -n ds-storage create secret generic ds-secrets --from-env-file=.env
kubectl -n ds-storage create secret generic rabbitmq-secret --from-literal=RABBITMQ_PASSWORD=<choose-a-password>
kubectl apply -k k8s/
kubectl -n ds-storage get pods
```

* API: <http://localhost:8030> (Service `gateway`, type LoadBalancer)
* Web UI: <http://localhost:3030>

## 2. Autoscaling the gateway

The HPA needs **metrics-server**. Managed clouds (AKS, GKE) already run it.
On Docker Desktop install it yourself. Local clusters use self-signed kubelet
certificates, so metrics-server must be started with `--kubelet-insecure-tls`
(it then skips checking the kubelet's certificate; acceptable on a local
test cluster, not in production):

```bash
kubectl apply -f k8s/metrics-server.yaml
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl top pods -n ds-storage          # works after ~1 minute
kubectl -n ds-storage get hpa gateway --watch
```

Generate load (uploads use CPU for hashing and erasure coding):

```bash
GATEWAY_URL=http://localhost:8030 python -m scripts.workload_benchmark --workload small --small-count 2000 --workers 32
```

The HPA raises the gateway from 2 to up to 6 pods while the load lasts and
scales back down a minute after it stops.

## 3. Rebalancing when a storage node is added

```bash
kubectl -n ds-storage scale statefulset storage-node --replicas=8
kubectl -n ds-storage logs deploy/repair-scheduler -f     # REBALANCE JOB ... storage-node-7 joined
```

The new pod registers itself with its heartbeat, the scheduler queues every
chunk the consistent hashing ring now assigns to it, and the workers move
them (visible in the web UI → Cluster Monitor as a "rebalance" job).

## 4. Self-healing (probes)

```bash
kubectl -n ds-storage delete pod storage-node-2      # recreated with the same PVC and data
kubectl -n ds-storage get events --field-selector involvedObject.name=storage-node-2
```

A node that hangs (process alive but not answering) fails its liveness probe
3 times (30 s) and is restarted automatically.

## 5. Deploying to a cloud (AKS / EKS / GKE)

The same manifests work on a managed cluster; only the images must come from
a registry the cluster can pull from.

1. Create a registry and push the images, e.g. Azure:
   ```bash
   az acr create -g ds-rg -n dsstorageacr --sku Basic
   az acr login -n dsstorageacr
   for i in metadata storage-node gateway repair web-ui; do
     docker tag ds-storage/$i:1.0 dsstorageacr.azurecr.io/ds-storage/$i:1.0
     docker push dsstorageacr.azurecr.io/ds-storage/$i:1.0
   done
   ```
   (AWS: `aws ecr create-repository` + `docker push <account>.dkr.ecr.<region>.amazonaws.com/...`;
   GCP: Artifact Registry `<region>-docker.pkg.dev/<project>/ds-storage/...`.)
2. Create the cluster and connect kubectl, e.g.
   `az aks create -g ds-rg -n ds-aks --node-count 3 --attach-acr dsstorageacr --generate-ssh-keys`
   then `az aks get-credentials -g ds-rg -n ds-aks`.
3. Point the manifests at the registry (no file edits needed):
   ```bash
   cd k8s
   kustomize edit set image ds-storage/metadata=dsstorageacr.azurecr.io/ds-storage/metadata:1.0
   # ... same for storage-node, gateway, repair, web-ui
   ```
4. Create the two secrets and `kubectl apply -k k8s/` as in step 1.
   `type: LoadBalancer` services get a public IP: `kubectl -n ds-storage get svc`.
5. metrics-server is built in on AKS/GKE (EKS: `kubectl apply -f k8s/metrics-server.yaml`,
   without the insecure flag), so the HPA works directly.
6. Delete everything afterwards to stop paying: `az group delete -n ds-rg`.
