#!/bin/bash
set -u

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

SYNC_PY="$(dirname "$0")/sync.py"
BACKEND_PY="$(dirname "$0")/backend_parallel.py"

MAX_WAIT=${MAX_WAIT:-1800}

echo "[0/5] 기존 container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo pkill -f swift-container-sync 2>/dev/null || true; echo '$node stopped'" &
done
wait
sleep 2

echo "[1/5] sync.py / backend_parallel.py 배포"
for node in "${CONT_NODES[@]}"; do
    (
        scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SYNC_PY" \
            "$SSH_USER@$node:~/swift/swift/container/sync.py"

        scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$BACKEND_PY" \
            "$SSH_USER@$node:~/swift/swift/container/backend_parallel.py"
    ) &
done
wait

echo "[2/5] container-sync 시작"
T_START=$(date +%s)

for node in "${CONT_NODES[@]}"; do
    ssh -f -n -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "source ${REMOTE_VENV}/bin/activate && \
         > /tmp/container-sync.log && \
         nohup swift-container-sync ${REMOTE_CONF} > /tmp/container-sync.log 2>&1 < /dev/null &"
    echo "$node started"
done

echo "시작: $(date '+%H:%M:%S')"
echo "[3/5] old-row 완료까지 폴링: SP2 >= SP1"
echo "대상: ${SRC_PREFIX}-001 ~ ${SRC_PREFIX}-$(printf "%03d" "$N_CONTAINERS")"
echo "최대 대기: ${MAX_WAIT}s"

while true; do
    DONE=0
    TOTAL=0

    for node in "${CONT_NODES[@]}"; do
        OUT=$(
            ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" "
                sudo find /srv/node -name '*.db' -path '*/sync_containers/*' | while read db; do
                    sudo sqlite3 -cmd '.timeout 5000' \"\$db\" '
                        SELECT
                            container,
                            x_container_sync_point1,
                            x_container_sync_point2
                        FROM container_info
                        WHERE container LIKE \"${SRC_PREFIX}-%\"
                          AND x_container_sync_point1 > -1;
                    ' 2>/dev/null
                done | awk -F'|' -v prefix='${SRC_PREFIX}' -v n='${N_CONTAINERS}' '
                    {
                        split(\$1, a, \"-\")
                        num = a[2] + 0

                        if (num >= 1 && num <= n) {
                            total += 1
                            if (\$3 >= \$2) {
                                done += 1
                            }
                        }
                    }
                    END {
                        print done+0, total+0
                    }'
            "
        )

        NODE_DONE=$(echo "$OUT" | awk '{print $1}')
        NODE_TOTAL=$(echo "$OUT" | awk '{print $2}')

        DONE=$((DONE + NODE_DONE))
        TOTAL=$((TOTAL + NODE_TOTAL))
    done

    ELAPSED=$(( $(date +%s) - T_START ))
    echo "[$ELAPSED s] old-row done: $DONE / $TOTAL"

    if [ "$TOTAL" -gt 0 ] && [ "$DONE" -ge "$TOTAL" ]; then
        RESULT="completed"
        break
    fi

    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        RESULT="timeout"
        echo "[ERROR] timeout: old-row not completed within ${MAX_WAIT}s"
        break
    fi

    sleep 5
done

T_END=$(date +%s)
T_ELAPSED=$((T_END - T_START))

echo "[4/5] container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "sudo pkill -f swift-container-sync 2>/dev/null || true" &
done
wait

echo "[5/5] 결과"
echo "================================="
echo "대상 컨테이너 : ${N_CONTAINERS}"
echo "예상 entries : $((N_CONTAINERS * ${#CONT_NODES[@]}))"
echo "최종 완료    : ${DONE} / ${TOTAL}"
echo "결과 상태    : ${RESULT}"
echo "old-row 처리 시간: ${T_ELAPSED}초"
echo "================================="

if [ "$RESULT" != "completed" ]; then
    exit 1
fi
        
