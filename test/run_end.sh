#!/bin/bash
# 3개 노드에서 container-sync를 실행하고 DST에 오브젝트가 다 쌓일 때까지 시간을 잰다.
# 사전 조건: reset.sh 완료, 노드에 원하는 sync.py / backend.py 배포된 상태

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

BASE_DIR="$(dirname "$0")"
SYNC_PY="${BASE_DIR}/sync.py"
BACKEND_PY="${BASE_DIR}/backend_parallel.py"

if [ ! -f "$SYNC_PY" ]; then
    echo "ERROR: sync.py not found at $SYNC_PY"
    exit 1
fi

if [ ! -f "$BACKEND_PY" ]; then
    echo "ERROR: backend.py not found at $BACKEND_PY"
    exit 1
fi

dst_object_count() {
    declare -A max_counts
    local cname objects num total

    while IFS='|' read -r cname objects _; do
        case "$cname" in
            "$DST_PREFIX"-???)
                num=${cname#"$DST_PREFIX"-}
                num=$((10#$num))
                if [ "$num" -ge 1 ] && [ "$num" -le "$N_CONTAINERS" ]; then
                    objects=${objects:-0}
                    if [ "$objects" -gt "${max_counts[$cname]:-0}" ]; then
                        max_counts[$cname]=$objects
                    fi
                fi
                ;;
        esac
    done < <(
        for node in "${DST_CONT_NODES[@]}"; do
            ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$DST_PROXY" \
                "ssh -i ~/.ssh/pyk-public.pem -o StrictHostKeyChecking=no ubuntu@$node \
                 \"find /srv/node -name \\\"*.db\\\" -path \\\"*/containers/*\\\" \\
                  -exec sqlite3 {} \\\"SELECT container, object_count FROM container_stat;\\\" \\\; \\
                  2>/dev/null\""
        done
    )

    total=0
    for objects in "${max_counts[@]}"; do
        total=$((total + objects))
    done
    echo "$total"
}

echo "[0/6] sync.py / backend.py 배포"
for node in "${CONT_NODES[@]}"; do
    (
        scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SYNC_PY" \
            "$SSH_USER@$node:~/swift/swift/container/sync.py" &&
        scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$BACKEND_PY" \
            "$SSH_USER@$node:~/swift/swift/container/backend_parallel.py"
    ) &
done
wait
echo "  완료"

echo "[1/6] DST 서버 기동 확인"
for node in "${DST_OBJ_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$SSH_USER@$DST_PROXY" \
        "ssh -i ~/.ssh/pyk-public.pem -o StrictHostKeyChecking=no -o ConnectTimeout=5 ubuntu@$node \
         'pgrep -f swift-object-server > /dev/null && echo \"$node already running\" || \
          (nohup /usr/local/bin/swift-object-server /etc/swift/object-server.conf \
           > /tmp/object-server.log 2>&1 & echo \"$node started\")'" &
done
wait
echo "  완료"

for node in "${DST_CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$SSH_USER@$DST_PROXY" \
        "ssh -i ~/.ssh/pyk-public.pem -o StrictHostKeyChecking=no -o ConnectTimeout=5 ubuntu@$node \
         \"sudo swift-init account-server start >/dev/null 2>&1 || true; \\
          sudo swift-init container-server start >/dev/null 2>&1 || true; \\
          sudo swift-init account-updater start >/dev/null 2>&1 || true; \\
          sudo swift-init container-updater start >/dev/null 2>&1 || true; \\
          sudo swift-init container-replicator start >/dev/null 2>&1 || true; \\
          echo \\\"$node account/container ready\\\"\"" &
done
wait
echo "  account/container 완료"

echo "[2/6] 기존 container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "pkill -f swift-container-sync 2>/dev/null; echo '$node done'" &
done
wait
sleep 2

GOAL=$TARGET

echo "[3/6] container-sync 시작"
for node in "${CONT_NODES[@]}"; do
    ssh -f -n -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "source ${REMOTE_VENV}/bin/activate && \
         > /tmp/container-sync.log && \
         nohup swift-container-sync ${REMOTE_CONF} > /tmp/container-sync.log 2>&1 < /dev/null &"
    echo "  $node started"
done

T_START=$(date +%s)
echo "시작: $(date '+%H:%M:%S')  목표: ${GOAL}개"

echo "[4/6] 완료까지 폴링"
while true; do
    CURRENT=$(dst_object_count)
    CURRENT=${CURRENT:-0}
    ELAPSED=$(( $(date +%s) - T_START ))
    echo "  [${ELAPSED}s] ${CURRENT} / ${GOAL}"
    [ "$CURRENT" -ge "$GOAL" ] && break
    sleep 10
done

T_END=$(date +%s)

echo "[5/6] container-sync 종료"
for node in "${CONT_NODES[@]}"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "pkill -f swift-container-sync 2>/dev/null" &
done
wait
sleep 3

echo "(SP 상태 확인)"
for node in "${CONT_NODES[@]}"; do
    CNT=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "find /srv/node -name '*.db' -path '*/containers/*' \
         -exec sqlite3 {} \
           'SELECT count(*) FROM container_info WHERE container LIKE \"src-%\" AND x_container_sync_point1 > -1;' \; \
         2>/dev/null | awk '{s+=\$1} END{print s+0}'" 2>/dev/null)
    echo "  $node  SP1>-1: ${CNT:-0}/100"
done

T_ELAPSED=$(( T_END - T_START ))

echo "[6/6] 노드별 로그 수집 및 지표 파싱"
TOTAL_PUTS=0
TOTAL_SYNCS=0
TOTAL_FAILS=0

for node in "${CONT_NODES[@]}"; do
    LOG=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "journalctl -t container-sync --since '@${T_START}' --until '@${T_END}' --no-pager 2>/dev/null | grep 'Container sync report'")

    PUTS=$(echo "$LOG"  | grep -oP 'puts: \K[0-9]+'       | awk '{s+=$1} END{print s+0}')
    SYNCS=$(echo "$LOG" | grep -oP 'total_rows: \K[0-9]+' | awk '{s+=$1} END{print s+0}')
    FAILS=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" \
        "journalctl -t container-sync --since '@${T_START}' --until '@${T_END}' --no-pager 2>/dev/null | grep -c 'ERROR Syncing' || echo 0" | tr -d '[:space:]')

    echo "  $node  puts=${PUTS}  total_rows=${SYNCS}  errors=${FAILS}"
    TOTAL_PUTS=$((TOTAL_PUTS + PUTS))
    TOTAL_SYNCS=$((TOTAL_SYNCS + SYNCS))
    TOTAL_FAILS=$((TOTAL_FAILS + FAILS))
done

echo ""
echo "================================="
echo "소요 시간  : ${T_ELAPSED}초"
echo "총 PUT     : ${TOTAL_PUTS}"
echo "총 rows    : ${TOTAL_SYNCS}"
echo "총 에러    : ${TOTAL_FAILS}"
echo "================================="
