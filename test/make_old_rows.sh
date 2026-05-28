#!/bin/bash
set -u

source "$(dirname "$0")/config.sh"
source ~/venv/bin/activate

echo "=== make old rows ==="
echo "target: ${SRC_PREFIX}-001 ~ ${SRC_PREFIX}-$(printf "%03d" "$N_CONTAINERS")"

for node in "${CONT_NODES[@]}"; do
    echo "[$node]"

    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_USER@$node" "
        sudo find /srv/node -name '*.db' -path '*/containers/*' | while read db; do
            cname=\$(sudo sqlite3 \"\$db\" \"SELECT container FROM container_info;\" 2>/dev/null)

            case \"\$cname\" in
                ${SRC_PREFIX}-*)
                    num=\${cname#${SRC_PREFIX}-}
                    num=\$((10#\$num))

                    if [ \"\$num\" -ge 1 ] && [ \"\$num\" -le ${N_CONTAINERS} ]; then
                        maxrow=\$(sudo sqlite3 \"\$db\" \"SELECT COALESCE(MAX(ROWID), -1) FROM object;\" 2>/dev/null)

                        sudo sqlite3 \"\$db\" \"
                            UPDATE container_info
                            SET x_container_sync_point1 = \$maxrow,
                                x_container_sync_point2 = -1;
                        \"

                        sudo sqlite3 \"\$db\" \"
                            UPDATE container_info
                            SET metadata = json_remove(
                                metadata,
                                '\$.\\"X-Container-Sysmeta-Parallel-Retry-State\\"'
                            )
                            WHERE metadata LIKE '%X-Container-Sysmeta-Parallel-Retry-State%';
                        \" 2>/dev/null || true

                        echo \"OPEN \$cname sp1=\$maxrow sp2=-1\"
                    fi
                    ;;
            esac
        done
    "
done

echo "완료"
