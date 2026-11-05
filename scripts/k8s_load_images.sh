#!/usr/bin/env bash
# Build the images with Docker Compose, tag them for Kubernetes and load
# them into Docker Desktop's Kubernetes node (it has its own image store,
# like `kind load docker-image`).
#
#   bash scripts/k8s_load_images.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep Linux paths as-is

NODE=${K8S_NODE_CONTAINER:-desktop-control-plane}
COMPOSE="docker compose -f compose.storage.yaml --env-file .env"

$COMPOSE build metadata storage-node-1 gateway-1 repair-worker web-ui

docker tag midtermds-metadata:latest       ds-storage/metadata:1.0
docker tag midtermds-storage-node-1:latest ds-storage/storage-node:1.0
docker tag midtermds-gateway-1:latest      ds-storage/gateway:1.0
docker tag midtermds-repair-worker:latest  ds-storage/repair:1.0
docker tag midtermds-web-ui:latest         ds-storage/web-ui:1.0

load() {
  echo "loading $1"
  docker save --platform linux/amd64 "$1" \
    | docker exec -i "$NODE" ctr --namespace=k8s.io images import \
        --digests --snapshotter=overlayfs --platform linux/amd64 -
}

for image in ds-storage/metadata:1.0 ds-storage/storage-node:1.0 \
             ds-storage/gateway:1.0 ds-storage/repair:1.0 ds-storage/web-ui:1.0 \
             postgres:16 rabbitmq:3.13-management \
             docker.elastic.co/elasticsearch/elasticsearch:8.17.0; do
  load "$image"
done

docker exec "$NODE" crictl images | grep -E "ds-storage|postgres|rabbitmq|elasticsearch"
