# Swift Container Sync Observability

OpenStack Swift `container-sync`의 동작 상태를 운영자가 웹, Prometheus/Grafana, Quickwit에서 확인할 수 있도록 만든 관측성 구성이다.

이 디렉터리는 서버 역할별 제출용 묶음이다. 실제 실행 위치의 파일은 기존 경로에도 유지하고, 여기에는 검토와 배포를 쉽게 하기 위한 복사본을 둔다.

## 목표

- `container-sync`가 최근 실행됐는지 확인한다.
- 어느 container node가 sync 작업을 멈췄거나 오래된 recon 값을 갖는지 확인한다.
- account/container 단위로 복제 진행률과 backlog를 본다.
- 특정 object가 언제, 어느 node에서, 어떤 method로 복제됐는지 검색한다.
- 실패가 발생하면 실패율과 object 단위 실패 이유를 빠르게 찾는다.

## 전체 구조

관측 데이터는 두 갈래로 나뉜다.

1. Recon data
   - daemon/container 단위의 수치 상태
   - Prometheus/Grafana와 Overview 웹 화면에서 사용

2. Quickwit log
   - object/container event 단위의 검색 가능한 로그
   - Object History 웹 화면과 Quickwit/Grafana log 검색에서 사용

```text
source container node
  container-sync
    -> /var/cache/swift/container.recon
    -> /var/log/swift/container-sync.log

/var/cache/swift/container.recon
  -> Swift recon middleware /recon/container-sync
  -> node container-sync-recon-exporter :8010
  -> Prometheus /metrics
  -> Grafana dashboard
  -> tst2 central web :8010

/var/log/swift/container-sync.log
  -> Vector
  -> Quickwit swift-container-sync-objects index
  -> Object History web
  -> Quickwit/Grafana search
```

## 디렉터리 구성

```text
monitoring/swift-container-sync-observability/
  swift-patches/
    container/sync.py
    common/middleware/recon.py
    cli/recon.py
  container-server/
    recon-exporter/
    vector-object-logs/
  proxy-server/
    quickwit-log-agent/
  tst2-monitoring-server/
    docker-compose.yml
    prometheus.example.yml
    swift_container_sync_object_index.yaml
    grafana/
```

### swift-patches

Swift 코드에 들어가는 패치 파일이다.

- `container/sync.py`
  - `container-sync` daemon 본체
  - Swift 관례대로 `dump_recon_cache()`와 `RECON_CONTAINER_FILE`을 사용해 `/var/cache/swift/container.recon`에 `container_sync_*` 값을 기록
  - object/container 단위 sync event를 `/var/log/swift/container-sync.log`에 JSON log로 기록

- `common/middleware/recon.py`
  - Swift recon middleware에 `/recon/container-sync` endpoint 추가
  - `container_sync_time`, `container_sync_last`, `container_sync_stats`, `container_sync_daemon`, `container_sync_containers`, `container_sync_hostname`을 반환

- `cli/recon.py`
  - `swift-recon container --container-sync` 옵션 추가
  - CLI에서 container-sync recon 요약 확인 가능

### container-server

각 source container node에 배포되는 구성이다.

- `recon-exporter/`
  - node 로컬 Swift recon API를 읽는 HTTP exporter
  - `/metrics`: Prometheus scrape endpoint
  - `/api/state`: recon JSON 상태
  - `/`, `/containers`, `/logs`: 운영자용 웹 화면

- `vector-object-logs/`
  - `/var/log/swift/container-sync.log`를 tail
  - `container-sync-object-event`, `container-sync-container-event` JSON log를 파싱
  - Quickwit `swift-container-sync-objects` index로 전송
  - `sync_to` 원문은 제거하고 path 중심 필드만 저장

### proxy-server

Proxy 서버 access log 수집 구성이다.

- `quickwit-log-agent/`
  - Swift proxy log를 Quickwit으로 전송하는 Python collector와 systemd unit
  - container-sync recon 자체와는 별도지만, 전체 Swift 로그 검색 구성에 포함된다.

### tst2-monitoring-server

tst2 모니터링 서버에서 실행되는 구성이다.

- `docker-compose.yml`
  - Prometheus, Grafana, 중앙 container-sync recon 웹/exporter 실행

- `prometheus.example.yml`
  - node exporter scrape job 예시

- `swift_container_sync_object_index.yaml`
  - Quickwit object/container sync event index mapping

- `grafana/`
  - Prometheus dashboard
  - Quickwit datasource
  - Swift proxy/container-sync dashboard JSON

## Recon Data

`container-sync`는 직접 Prometheus로 값을 전송하지 않는다. 값을 로컬 recon 파일에 기록하고, 다른 구성 요소가 그 값을 읽는다.

```text
sync.py
  -> /var/cache/swift/container.recon
  -> /recon/container-sync
  -> container-sync-recon-exporter
  -> /metrics, /api/state, web
```

### 기록 위치

```text
/var/cache/swift/container.recon
```

### 추가 key

```text
container_sync_time
container_sync_last
container_sync_stats
container_sync_daemon
container_sync_containers
container_sync_hostname
```

### daemon 단위 값

`container_sync_daemon`에 들어간다.

- 마지막 실행 시작 시각
- 마지막 실행 종료 시각
- 마지막 실행 시간
- scanned container 수
- synced container 수
- skipped container 수
- failed container 수
- `container_time` 초과 container 수
- new backlog rows
- retry backlog rows
- max new backlog rows
- max retry backlog rows

### 누적 작업 통계

`container_sync_stats`에 들어간다.

- syncs: sync 성공 container 수
- skips: skip container 수
- failures: failure container 수
- attempted: scan된 container 수
- time_exhausted: `container_time` 제한에 걸린 container 수
- puts: PUT 성공 수
- deletes: DELETE 성공 수
- bytes: 전송 bytes
- row_attempts: row attempt 수
- row_successes: row success 수
- row_failures: row failure 수
- remote_head_skips: remote head skip 수

### container 단위 값

`container_sync_containers`에 account/container별로 들어간다.

- account
- container
- status / last_status
- last_reason
- updated timestamp
- sync_point1
- sync_point2
- max_row
- object_count
- new_backlog_rows
- retry_backlog_rows
- time_exhausted

복제 진행률은 exporter에서 보수적으로 다음 기준으로 계산한다.

```text
replication_rate = sync_point2 / max_row
scan_rate        = sync_point1 / max_row
```

`sync_point2`는 retry 구간까지 확인된 지점을 뜻하므로, 운영 화면의 `Replication` 값은 `sync_point2 / max_row` 기준이다.

## Quickwit Log

Quickwit은 object/container event log 검색을 담당한다. recon이 수치 상태라면, Quickwit log는 “어떤 object가 언제 처리됐는가”를 찾기 위한 데이터다.

### object event

`sync.py`는 object row 처리 결과를 다음 prefix로 기록한다.

```text
container-sync-object-event {json}
```

주요 필드:

- event_type: `container_sync_object`
- timestamp
- host
- account
- container
- object
- method: PUT / DELETE / HEAD 등
- outcome: success / failure / skipped
- reason
- source_path
- remote_container_path
- remote_path
- request_time
- duration_ms
- row_id
- deleted
- object_bytes
- bytes_sent

### container event

container 하나의 sync report도 다음 prefix로 기록한다.

```text
container-sync-container-event {json}
```

주요 필드:

- event_type: `container_sync_container`
- account
- container
- outcome
- sync_point1
- sync_point2
- max_row
- object_count
- replication_rate
- puts
- deletes
- request_time
- time_exhausted

### Vector 처리

Vector는 `/var/log/swift/container-sync.log`를 tail 하면서 위 event만 파싱한다.

처리 내용:

- JSON event 파싱
- `sync_to` 제거
- site/host/program 보강
- source_path, remote_path, path 보강
- status_class, duration_ms, bytes_sent 보강
- Quickwit ingest API로 전송

### Quickwit index

index id:

```text
swift-container-sync-objects
```

index mapping:

```text
monitoring/tst2-monitoring-server/swift_container_sync_object_index.yaml
monitoring/swift-container-sync-observability/tst2-monitoring-server/swift_container_sync_object_index.yaml
```

검색 예시:

```text
event_type:"container_sync_object"
object:"obj-0806"
account:"AUTH_test" AND container:"src-010"
method:"PUT" AND outcome:"success"
outcome:"failure"
event_type:"container_sync_container" AND container:"src-010"
```

주의: Quickwit의 원본 `timestamp`는 UTC다. 웹 UI에서는 사람이 보기 쉽도록 KST로 표시한다.

## Web UI

중앙 웹은 tst2 monitoring server의 `container-sync-recon-exporter`가 제공한다.

기본 URL:

```text
http://<monitoring-server>:8010/
```

### Overview

```text
http://<monitoring-server>:8010/
```

확인할 수 있는 것:

- Recon nodes up
- New backlog rows
- Retry backlog rows
- Failed containers
- node별 recon age
- node별 last run/scanned/synced/failed
- node별 backlog

### Containers

```text
http://<monitoring-server>:8010/containers
```

확인할 수 있는 것:

- account/container 목록
- replication rate
- total objects/rows
- new backlog
- retry backlog
- 최근 object event 기준 failure count
- error rate
- 관련 node 목록
- failure/history 상세 링크
- PUT/DELETE trend
- container row/object trend

주의: table의 object event 기반 항목은 Quickwit query 결과 기준이다. recon 기반 복제율과 backlog는 `/recon/container-sync`에서 가져온다.

### Object History

```text
http://<monitoring-server>:8010/logs
```

확인할 수 있는 것:

- 특정 account/container/object 검색
- method/outcome/reason/host/site/path 필터
- object가 언제 어느 node에서 처리됐는지 확인
- 실패 이유 확인

JSON API:

```text
http://<monitoring-server>:8010/api/object-logs?q=object:<object-name>
```

## Prometheus Metrics

각 container node의 exporter가 `/metrics`를 제공한다.

```text
http://<container-node>:8010/metrics
```

중앙 exporter도 aggregate 확인용 `/metrics`를 제공한다.

```text
http://<monitoring-server>:8010/metrics
```

주요 metric:

```text
swift_container_sync_recon_up
swift_container_sync_recon_read_timestamp_seconds
swift_container_sync_recon_last_update_timestamp_seconds

swift_container_sync_recon_daemon_last_run_timestamp_seconds
swift_container_sync_recon_daemon_last_run_finished_timestamp_seconds
swift_container_sync_recon_daemon_last_run_duration_seconds
swift_container_sync_recon_daemon_scanned_containers
swift_container_sync_recon_daemon_synced_containers
swift_container_sync_recon_daemon_skipped_containers
swift_container_sync_recon_daemon_failed_containers
swift_container_sync_recon_daemon_new_backlog_rows
swift_container_sync_recon_daemon_retry_backlog_rows

swift_container_sync_recon_puts_total
swift_container_sync_recon_deletes_total
swift_container_sync_recon_bytes_total
swift_container_sync_recon_row_attempts_total
swift_container_sync_recon_row_successes_total
swift_container_sync_recon_row_failures_total

swift_container_sync_recon_container_replication_ratio
swift_container_sync_recon_container_scan_ratio
swift_container_sync_recon_container_sync_point1_rows
swift_container_sync_recon_container_sync_point2_rows
swift_container_sync_recon_container_max_row_rows
swift_container_sync_recon_container_object_count
swift_container_sync_recon_container_new_backlog_rows
swift_container_sync_recon_container_retry_backlog_rows
swift_container_sync_recon_container_status
```

## Grafana

Grafana는 Prometheus 시계열과 Quickwit log datasource를 시각화한다.

URL:

```text
http://<monitoring-server>:3000/
```

기본 계정:

```text
admin / admin
```

Container Sync dashboard:

```text
http://<monitoring-server>:3000/d/swift_container_sync/swift-container-sync
```

주요 패널:

- Recon Nodes Up
- Failed Containers
- New Backlog Rows
- Retry Backlog Rows
- Row Failure Rate
- Max Run Duration
- Backlog Rows by Node
- Object Row Processing Rate
- PUT/DELETE Rate
- Last Run Duration by Node
- Container Scan Results
- Bytes Sent Rate
- Recon Age by Node
- Top Container Backlog

Grafana는 시간순 추세를 보는 곳이고, Object History 웹은 특정 object/account/container 검색을 위한 곳이다.

## 배포

### 1. Swift patch 배포

source container node의 Swift checkout에 다음 파일을 반영한다.

```text
swift-patches/container/sync.py
swift-patches/common/middleware/recon.py
swift-patches/cli/recon.py
```

실제 배포 대상 예시:

```text
~/swift/swift/container/sync.py
~/swift/swift/common/middleware/recon.py
~/swift/swift/cli/recon.py
```

`common/middleware/recon.py`를 바꾼 뒤에는 container-server가 새 코드를 로드하도록 재시작한다.

```bash
sudo swift-init container-server restart
```

환경에 따라 `swift-init`이 관리하지 않는 오래된 `swift-container-server` 프로세스가 있을 수 있다. 이 경우 해당 프로세스를 재기동해야 `/recon/container-sync`가 새 key를 반환한다.

확인:

```bash
curl http://127.0.0.1:6201/recon/container-sync
```

정상 응답에는 다음 key가 포함되어야 한다.

```text
container_sync_containers
container_sync_daemon
container_sync_hostname
container_sync_last
container_sync_stats
container_sync_time
```

### 2. Node recon exporter 배포

각 source container node에 exporter를 배포한다.

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/tst2-monitoring-server
./container-sync-recon-exporter/deploy_node_exporters.sh
```

확인:

```bash
curl http://<container-node>:8010/healthz
curl http://<container-node>:8010/api/state
curl http://<container-node>:8010/metrics
```

### 3. Vector object log collector 배포

Quickwit object/container event 검색을 위해 각 source container node에 Vector collector를 배포한다.

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/container-sync-object-vector
./deploy_container_sync_object_vector.sh
```

확인:

```bash
systemctl status vector-container-sync-object
tail -f /var/log/swift/container-sync.log
```

### 4. Quickwit index 생성

Quickwit에 `swift-container-sync-objects` index를 생성한다.

```bash
curl -XPOST http://<quickwit-host>:7280/api/v1/indexes \
  -H 'content-type: application/yaml' \
  --data-binary @swift_container_sync_object_index.yaml
```

이미 index가 있으면 mapping 변경 내용에 따라 index 재생성이 필요할 수 있다.

### 5. tst2 monitoring server 실행

로컬 설정 파일을 준비한다.

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/tst2-monitoring-server
cp prometheus.example.yml prometheus.yml
```

`.env`에는 실제 node exporter URL과 Quickwit URL을 지정한다.

```bash
CONTAINER_SYNC_RECON_WEB_PORT=8010
CONTAINER_SYNC_RECON_URLS=http://192.168.0.39:8010/api/state,http://192.168.0.56:8010/api/state,http://192.168.0.102:8010/api/state
CONTAINER_SYNC_RECON_NODE_ALIASES=192.168.0.39=knu-src-cont1,192.168.0.56=knu-src-cont2,192.168.0.102=knu-src-cont3
CONTAINER_SYNC_OBJECT_QUICKWIT_SEARCH_URL=http://quickwit:7280/api/v1/swift-container-sync-objects/search
```

실행:

```bash
docker-compose up -d prometheus grafana container-sync-recon-exporter
```

접속:

```text
Container Sync Recon Web: http://<monitoring-server>:8010/
Object History Web:      http://<monitoring-server>:8010/logs
Prometheus:              http://<monitoring-server>:9091/
Grafana:                 http://<monitoring-server>:3000/
```

## 검증 체크리스트

### Recon

container node에서 확인:

```bash
curl http://127.0.0.1:6201/recon/container-sync
curl http://127.0.0.1:8010/api/state
curl http://127.0.0.1:8010/metrics | grep swift_container_sync_recon_container_replication_ratio
```

monitoring server에서 확인:

```bash
curl http://127.0.0.1:8010/api/state
curl http://127.0.0.1:8010/metrics
```

### Quickwit log

```bash
curl 'http://127.0.0.1:8010/api/object-logs?q=event_type:%22container_sync_object%22&max_hits=5'
curl 'http://127.0.0.1:8010/api/object-logs?q=object:%22obj-0806%22&max_hits=20'
```

### Web

- `/`: 3개 node가 up인지 확인
- `/containers`: account/container 목록과 replication `100.0%` 확인
- `/logs`: object history가 KST timestamp로 보이는지 확인

## 운영 시 해석

### New backlog rows

```text
max_row - sync_point1
```

아직 새 row 영역을 scan하지 못한 양이다.

### Retry backlog rows

```text
sync_point1 - sync_point2
```

이미 한 번 scan한 row 중 모든 node/slot 기준으로 retry 확인이 끝나지 않은 구간이다.

### Replication

```text
sync_point2 / max_row
```

container DB row 기준 진행률이다. 실제 remote object listing을 다시 비교한 값은 아니다.

### Object History timestamp

Quickwit 원본 timestamp는 UTC다. 웹 화면은 KST로 변환해서 표시한다.

### run_end.sh의 total_rows

`run_end.sh`의 `total_rows`는 container sync report의 `total_rows` 값을 단순 합산한 값이다. 이는 실제 전송 object 수가 아니라 container DB의 row 규모가 중복 집계된 값일 수 있다. 실제 원격 PUT 성공 수는 `puts`를 기준으로 보는 것이 더 적절하다.

## Troubleshooting

### `/recon/container-sync`에 `container_sync_containers`가 없다

원인:

- `common/middleware/recon.py` 패치가 미배포
- container-server가 예전 프로세스로 계속 떠 있음

확인:

```bash
grep -nA8 'def get_container_sync_info' ~/swift/swift/common/middleware/recon.py
ps -ef | grep swift-container-server
curl http://127.0.0.1:6201/recon/container-sync
```

해결:

```bash
sudo swift-init container-server restart
```

`swift-init`이 관리하지 않는 프로세스가 있으면 해당 `swift-container-server`를 직접 재기동한다.

### 웹에서 node는 up인데 Last run이 never다

Recon API가 `container_sync_last`만 반환하고 `container_sync_daemon`을 반환하지 못하는 상태일 수 있다. 위 항목처럼 middleware와 container-server 재기동 상태를 확인한다.

### Object History가 하루 전 날짜로 보인다

Quickwit 원본 `timestamp`는 UTC다. `2026-06-05T19:02:33Z`는 KST로 `2026-06-06 04:02:33 KST`다. 웹 화면은 KST로 표시한다.

### 웹의 Events 숫자가 run_end.sh의 PUT 수와 다르다

웹의 Events는 Quickwit query 결과 기준이다. `Max events` 제한, 검색 조건, 시간 범위에 따라 달라질 수 있다. 특정 실험 run의 전체 PUT 수와 정확히 맞추려면 run 시작/종료 시간 또는 run id 기준의 query가 필요하다.

### Prometheus에 container별 metric이 없다

확인:

```bash
curl http://<container-node>:8010/metrics | grep swift_container_sync_recon_container
```

없다면 node exporter가 읽는 `/recon/container-sync`에 `container_sync_containers`가 있는지 먼저 확인한다.

## 제외 항목

이 묶음에는 다음 파일을 포함하지 않는다.

- `.env`
- SSH private key
- 실제 운영 로그 파일
- 백업 파일
- 서버별 민감한 endpoint/token/password

서버 주소, 인증값, Quickwit ingest/search URL은 각 환경의 `.env` 또는 systemd environment에서 관리한다.
