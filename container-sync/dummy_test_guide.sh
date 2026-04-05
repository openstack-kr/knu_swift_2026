# 각 서버 터미널에서 순서에 맞게 입력

# 모든 swift 프로세스 종료
sudo swift-init all stop

# ======================================================
# **초기화 해야하는 경우에만** 각 srv, dst서버에서 디스크 초기화
sudo rm -rf /srv/1/node/*
sudo rm -rf /srv/2/node/*
sudo rm -rf /srv/3/node/*
sudo rm -rf /srv/4/node/*

ls /srv/1/node
ls /srv/2/node
ls /srv/3/node
ls /srv/4/node

# **초기화 해야하는 경우에만** 이전에 돌린 경우: /srv/*/node 아래의 Swift storage 디렉토리 복구
sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5
sudo mkdir -p /srv/2/node/sdb2 /srv/2/node/sdb6
sudo mkdir -p /srv/3/node/sdb3 /srv/3/node/sdb7
sudo mkdir -p /srv/4/node/sdb4 /srv/4/node/sdb8

sudo chown -R ubuntu:ubuntu /srv/1/node /srv/2/node /srv/3/node /srv/4/node
# ======================================================
# 각 srv, dst 서버: container sync 제외하고 swift 프로세스 시작
sudo swift-init main start
swift-init container-sync stop

# 각 srv, dst 서버: 공통 설정, 아래 변수 값은 원하는대로 조정
N_CONTAINER=10
N_OBJECT=10

SRC_PREFIX="src"
DST_PREFIX="dst"

AUTH_URL="http://127.0.0.1:8080/auth/v1.0"
USER="test:tester"
KEY="testing"

REALM="realm1"
CLUSTER="saio4"
SYNC_KEY="secret123"
# ======================================================
# dst 서버: container 생성
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post "$DST_PREFIX-$i"
done
# dst 서버: sync key 설정
for i in $(seq -w 1 $N_CONTAINER); do
  swift -A "$AUTH_URL" -U "$USER" -K "$KEY" post -k "$SYNC_KEY" "$DST_PREFIX-$i"
done 
# ======================================================
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
# ======================================================
# src서버: object 업로드
tmpfile=$(mktemp)
printf 'a' > "$tmpfile"
for i in $(seq -w 1 $N_CONTAINER); do
  for j in $(seq -w 1 $N_OBJECT); do
    swift -A "$AUTH_URL" -U "$USER" -K "$KEY" upload \
      "$SRC_PREFIX-$i" "$tmpfile" --object-name "obj-$j"
  done
done
# ======================================================
# dst서버: 각 컨테이너 비어있는지 확인
for i in $(seq -w 1 $N_CONTAINER); do
  count=$(swift -A "$AUTH_URL" -U "$USER" -K "$KEY" list "$DST_PREFIX-$i" | wc -l)
  echo "$DST_PREFIX-$i : $count"
done
# ======================================================
# src 서버: sync 실행
time sudo swift-init container-sync once
# ======================================================
# dst서버: 컨테이너 찼는지 확인
for i in $(seq -w 1 $N_CONTAINER); do
  count=$(swift -A "$AUTH_URL" -U "$USER" -K "$KEY" list "$DST_PREFIX-$i" | wc -l)
  echo "$DST_PREFIX-$i : $count"
done



