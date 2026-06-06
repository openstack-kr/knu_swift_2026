#!/bin/bash
# SP1, SP2 를 -1로 리셋하고 DST 오브젝트를 전부 삭제한다.

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

echo "[0/4] 기존 container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "pkill -f swift-container-sync 2>/dev/null; echo '$node done'" &
done
wait
sleep 1

echo "[1/4] SP 리셋 (3개 SRC 노드 동시)"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "find /srv/node -name '*.db' -path '*/containers/*' \
         -exec sqlite3 {} \
           'UPDATE container_info SET x_container_sync_point1=-1, x_container_sync_point2=-1;' \; \
         2>/dev/null && echo '$node done'" &
done
wait

echo "[2/4] DST 오브젝트 데이터 직접 삭제 (툼스톤 없이)"
for node in "${DST_OBJ_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$DST_PROXY" \
        "ssh -i ~/.ssh/pyk-public.pem -o StrictHostKeyChecking=no ubuntu@$node \
         'sudo find /srv/node -name \"*.data\" -delete && \
          sudo find /srv/node -name \"*.ts\" -delete && \
          sudo find /srv/node -path \"*/async_pending/*\" -delete && \
          echo \"$node done\"'" &
done
wait

echo "[3/4] DST 컨테이너 DB 직접 삭제 후 재생성"
# DST 컨테이너 노드에서 dst-* 컨테이너 DB 파일 직접 삭제
for node in "${DST_CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$DST_PROXY" \
        "ssh -i ~/.ssh/pyk-public.pem -o StrictHostKeyChecking=no ubuntu@$node \
         'sudo python -c \"
import sqlite3, glob, os
dbs = glob.glob(\\\"/srv/node/*/containers/*/*/*/*.db\\\")
for db in dbs:
    try:
        c = sqlite3.connect(db)
        name = c.execute(\\\"SELECT container FROM container_stat\\\").fetchone()
        c.close()
        if name and name[0].startswith(\\\"dst-\\\"):
            os.remove(db)
            wal = db + \\\"-wal\\\"
            shm = db + \\\"-shm\\\"
            if os.path.exists(wal): os.remove(wal)
            if os.path.exists(shm): os.remove(shm)
    except: pass
print(\\\"done\\\")
\"'" &
done
wait

# DST 컨테이너 재생성 (빈 DB로)
for i in $(seq 1 "$N_CONTAINERS"); do
    cname=$(printf '%s-%03d' "$DST_PREFIX" "$i")
    swift -A "$DST_AUTH" -U "$DST_USER" -K "$DST_KEY" post "$cname" \
        -H "X-Container-Sync-Key: secret123" 2>/dev/null &
    [ $((i % 20)) -eq 0 ] && wait
done
wait

echo "[4/4] SRC async_pending 정리"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo find /srv/node -path '*/async_pending/*' -delete 2>/dev/null && echo '$node done'" &
done
wait

echo "리셋 완료"
