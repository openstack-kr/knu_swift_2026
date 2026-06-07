#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$BASE_DIR/.." && pwd)"
ENV_FILE="${CONTAINER_SYNC_RECON_ENV_FILE:-$REPO_DIR/.env}"

load_env() {
  if [[ ! -f "$ENV_FILE" ]]; then
    return
  fi
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    [[ -z "$line" || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    case "$key" in
      CONTAINER_SYNC_RECON_NODE_*|CONTAINER_SYNC_RECON_NODES|CONTAINER_SYNC_RECON_BASTION|CONTAINER_SYNC_RECON_SSH_KEY|CONTAINER_SYNC_RECON_REMOTE_DIR)
        export "$key=$value"
        ;;
    esac
  done < "$ENV_FILE"
}

load_env

SSH_KEY="${CONTAINER_SYNC_RECON_SSH_KEY:-/home/ubuntu/.ssh/pyk-public.pem}"
BASTION="${CONTAINER_SYNC_RECON_BASTION:-ubuntu@133.186.209.214}"
NODES="${CONTAINER_SYNC_RECON_NODES:-src1=ubuntu@192.168.0.39,src2=ubuntu@192.168.0.56,src3=ubuntu@192.168.0.102}"
REMOTE_DIR="${CONTAINER_SYNC_RECON_REMOTE_DIR:-/opt/container-sync-recon-exporter}"
SERVICE_NAME="swift-container-sync-recon-exporter.service"

SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=no)
PROXY_OPTS=(-o "ProxyCommand=ssh -i $SSH_KEY -o StrictHostKeyChecking=no -W %h:%p $BASTION")

copy_to_node() {
  local label="$1"
  local target="$2"
  local tmp_dir="/tmp/container-sync-recon-exporter.$$"
  echo "[$label] installing exporter on $target"
  ssh "${SSH_OPTS[@]}" "${PROXY_OPTS[@]}" "$target" "mkdir -p '$tmp_dir'"
  scp "${SSH_OPTS[@]}" "${PROXY_OPTS[@]}" \
    "$BASE_DIR/container_sync_recon_exporter.py" \
    "$BASE_DIR/systemd/$SERVICE_NAME" \
    "$target:$tmp_dir/"
  ssh "${SSH_OPTS[@]}" "${PROXY_OPTS[@]}" "$target" \
    "sudo mkdir -p '$REMOTE_DIR' && sudo install -m 0644 '$tmp_dir/container_sync_recon_exporter.py' '$REMOTE_DIR/container_sync_recon_exporter.py' && sudo install -m 0644 '$tmp_dir/$SERVICE_NAME' '/etc/systemd/system/$SERVICE_NAME' && rm -rf '$tmp_dir' && sudo systemctl daemon-reload && sudo systemctl enable --now '$SERVICE_NAME' && sudo systemctl restart '$SERVICE_NAME' && sudo systemctl --no-pager --full status '$SERVICE_NAME' | head -n 12"
}

IFS=',' read -ra entries <<< "$NODES"
for entry in "${entries[@]}"; do
  entry="${entry//[[:space:]]/}"
  [[ -z "$entry" ]] && continue
  if [[ "$entry" == *=* ]]; then
    label="${entry%%=*}"
    target="${entry#*=}"
  else
    label="$entry"
    target="$entry"
  fi
  copy_to_node "$label" "$target"
done
