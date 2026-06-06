#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SSH_KEY="${SSH_KEY:-/home/ubuntu/.ssh/pyk-public.pem}"
BASTION="${BASTION:-ubuntu@133.186.209.214}"
QUICKWIT_INGEST_URL="${QUICKWIT_INGEST_URL:-http://192.168.0.123:7280/api/v1/swift-container-sync-objects/ingest}"
INSTALL_VECTOR="${INSTALL_VECTOR:-false}"
VECTOR_DEB_PATH="${VECTOR_DEB_PATH:-}"

CONFIG_SRC="${SCRIPT_DIR}/container-sync-object-vector.toml"
SERVICE_SRC="${SCRIPT_DIR}/systemd/vector-container-sync-object.service"

NODES=(
  "source-container-src1 ubuntu@192.168.0.39"
  "source-container-src2 ubuntu@192.168.0.56"
  "source-container-src3 ubuntu@192.168.0.102"
)

ssh_base=(
  ssh
  -i "${SSH_KEY}"
  -o StrictHostKeyChecking=no
  -o "ProxyCommand=ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -W %h:%p ${BASTION}"
)

scp_base=(
  scp
  -i "${SSH_KEY}"
  -o StrictHostKeyChecking=no
  -o "ProxyCommand=ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no -W %h:%p ${BASTION}"
)

for entry in "${NODES[@]}"; do
  site="${entry%% *}"
  host="${entry#* }"
  echo "==> Deploying Vector container-sync object collector to ${site} (${host})"

  "${scp_base[@]}" "${CONFIG_SRC}" "${host}:/tmp/container-sync-object-vector.toml"
  "${scp_base[@]}" "${SERVICE_SRC}" "${host}:/tmp/vector-container-sync-object.service"
  if [ -n "${VECTOR_DEB_PATH}" ]; then
    "${scp_base[@]}" "${VECTOR_DEB_PATH}" "${host}:/tmp/vector.deb"
  fi

  "${ssh_base[@]}" "${host}" bash -s -- "${site}" "${QUICKWIT_INGEST_URL}" "${INSTALL_VECTOR}" <<'REMOTE'
set -euo pipefail

site="$1"
quickwit_ingest_url="$2"
install_vector="$3"

if ! command -v vector >/dev/null 2>&1; then
  if [ -f /tmp/vector.deb ]; then
    sudo apt-get install -y /tmp/vector.deb
  elif [ "${install_vector}" = "true" ]; then
    bash -c "$(curl -L https://setup.vector.dev)"
    sudo apt-get update
    sudo apt-get install -y vector
  else
    echo "vector is not installed. Re-run with INSTALL_VECTOR=true, set VECTOR_DEB_PATH=/path/to/vector.deb, or install vector first." >&2
    exit 1
  fi
fi

sudo install -d -m 0755 /etc/vector /etc/default /var/lib/vector/container-sync-object
sudo install -m 0644 /tmp/container-sync-object-vector.toml /etc/vector/container-sync-object.toml
sudo install -m 0644 /tmp/vector-container-sync-object.service /etc/systemd/system/vector-container-sync-object.service

tmp_env="$(mktemp)"
cat > "${tmp_env}" <<EOF
VECTOR_SITE=${site}
QUICKWIT_INGEST_URL=${quickwit_ingest_url}
EOF
sudo install -m 0644 "${tmp_env}" /etc/default/vector-container-sync-object
rm -f "${tmp_env}" /tmp/container-sync-object-vector.toml /tmp/vector-container-sync-object.service /tmp/vector.deb

sudo systemctl daemon-reload
sudo systemctl enable --now vector-container-sync-object.service
sudo systemctl restart vector-container-sync-object.service
sudo systemctl --no-pager --full status vector-container-sync-object.service | sed -n '1,14p'
REMOTE
done
