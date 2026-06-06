#!/bin/bash
set -u

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

echo "=== check old rows ==="

TOTAL=0

for node in "${CONT_NODES[@]}"; do
    echo ""
    echo "[$node]"

    OUT=$(ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" "
        sudo find /srv/node -name '*.db' -path '*/containers/*' | while read db; do
            sudo sqlite3 \"\$db\" \"
                SELECT container, x_container_sync_point1, x_container_sync_point2
                FROM container_info
                WHERE container LIKE '${SRC_PREFIX}-%'
                  AND x_container_sync_point1 > x_container_sync_point2;
            \" 2>/dev/null
        done
    ")

    echo "$OUT"
    CNT=$(echo "$OUT" | grep -c "${SRC_PREFIX}-" || true)
    TOTAL=$((TOTAL + CNT))
done

echo ""
echo "old-row containers total: $TOTAL"
echo "expected replica entries: $((N_CONTAINERS * 3))"
