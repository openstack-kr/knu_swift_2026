#!/bin/bash
set -u

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

echo "[1/6] SRC container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo pkill -f swift-container-sync 2>/dev/null || true; echo '$node container-sync stopped'" &
done
wait

echo "[2/6] SRC account/container 서버 중지"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo swift-init account-server stop || true; \
         sudo swift-init container-server stop || true; \
         echo '$node account/container stopped'" &
done
wait

echo "[3/6] SRC object 서버 중지"
for node in "${OBJ_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo swift-init object-server stop || true; \
         echo '$node object stopped'" &
done
wait

echo "[4/6] SRC account/container DB 삭제"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo find /srv/node -path '*/accounts/*' -name '*.db' -delete; \
         sudo find /srv/node -path '*/containers/*' -name '*.db' -delete; \
         sudo find /srv/node -path '*/tmp/*' -type f -delete 2>/dev/null || true; \
         echo '$node account/container DB deleted'" &
done
wait

echo "[5/6] SRC object 데이터 삭제"
for node in "${OBJ_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo find /srv/node -name '*.data' -delete; \
         sudo find /srv/node -name '*.ts' -delete; \
         sudo find /srv/node -path '*/async_pending/*' -delete; \
         sudo find /srv/node -path '*/tmp/*' -type f -delete 2>/dev/null || true; \
         echo '$node object data deleted'" &
done
wait

echo "[6/6] SRC 서버 재시작"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo swift-init account-server start || true; \
         sudo swift-init container-server start || true; \
         sudo swift-init account-replicator start || true; \
         sudo swift-init container-replicator start || true; \
         echo '$node account/container started'" &
done

for node in "${OBJ_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo swift-init object-server start || true; \
         sudo swift-init object-replicator start || true; \
         echo '$node object started'" &
done
wait

sleep 3

echo ""
echo "완료 — SRC 테스트 데이터 삭제됨."
