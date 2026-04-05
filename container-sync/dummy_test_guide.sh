# 각 서버 터미널에서 순서에 맞게 입력

# src 서버, dst 서버: 데몬 정지
swift-init container-sync stop
# 필요하면 이것도 pkill -9 -f swift-container-sync

# ===== src, dst 서버 공통 설정 =====
N_CONTAINER=10
N_OBJECT=100

SRC_PREFIX="src9"
DST_PREFIX="dst9"

AUTH_URL="http://127.0.0.1:8080/auth/v1.0"
USER="test:tester"
KEY="testing"

REALM="realm1"
CLUSTER="saio2"
SYNC_KEY="secret123"

# dst 서버: container 생성
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post "$DST_PREFIX-$i"
done
# dst 서버: sync key 설정
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post -k "$SYNC_KEY" "$DST_PREFIX-$i"
done 

# src 서버: container 생성
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post "$SRC_PREFIX-$i"
done

# src 서버: sync metadata 설정
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post \
    -t "//$REALM/$CLUSTER/AUTH_test/$DST_PREFIX-$i" \
    -k "$SYNC_KEY" \
    "$SRC_PREFIX-$i"
done

# src서버: object 업로드
printf 'a' > "$tmpfile"
for i in $(seq -w 1 $N_CONTAINER); do
  for j in $(seq -w 1 $N_OBJECT); do
    swift -A "$AUTH_URL" -U "$USER" -K "$KEY" upload \
      "$SRC_PREFIX-$i" "$tmpfile" --object-name "obj-$j"
  done
done

# dst서버: 각 컨테이너 비어있는지 확인
for i in $(seq -w 1 $N_CONTAINER); do
  count=$(swift -A "$AUTH_URL" -U "$USER" -K "$KEY" list "$DST_PREFIX-$i" | wc -l)
  echo "$DST_PREFIX-$i : $count"
done

# src 서버: sync 실행
time sudo swift-init container-sync once

# dst서버: 컨테이너 찼는지 확인
for i in $(seq -w 1 $N_CONTAINER); do
  count=$(swift -A "$AUTH_URL" -U "$USER" -K "$KEY" list "$DST_PREFIX-$i" | wc -l)
  echo "$DST_PREFIX-$i : $count"
done



