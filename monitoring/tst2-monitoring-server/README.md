# Swift Proxy Logs with Quickwit and Grafana

Swift Proxy access 로그를 Quickwit에 적재하고 Grafana Quickwit datasource plugin으로 요청량/오류/지연/최근 로그를 보는 구성입니다.

## 구성

- Quickwit: `swift-proxy-logs` index에 proxy log 저장, `swift-container-sync-objects` index에 container-sync object event 저장 및 검색
- Grafana: `quickwit-quickwit-datasource` plugin 사용
- Grafana datasource UID: `swift_quickwit_logs`, `swift_quickwit_container_sync_objects`
- Grafana dashboard UID: `swift_proxy_logs`
- Container sync dashboard UID: `swift_container_sync`
- Container Sync Recon Web: container-sync daemon recon 값을 별도 웹 화면과 Prometheus metric으로 노출

## 실행

Fresh clone 기준으로는 실제 endpoint가 들어간 로컬 설정 파일을 먼저 만들어야 합니다. `prometheus.yml`과 `.env`는 Git에 올리지 않는 로컬 전용 파일입니다.

```bash
cp prometheus.example.yml prometheus.yml
# .env 파일에 실제 Swift auth endpoint/user/key 또는 token을 입력
# prometheus.yml 파일에 실제 node-exporter target을 입력

docker-compose up -d
```

실행 전에 다음 전제가 필요합니다.

- Quickwit이 `quickwit-swift_default` Docker network 안에서 `quickwit:7280` 이름으로 접근 가능해야 합니다.
- `swift-proxy-logs` Quickwit index가 `swift_proxy_index.yaml` 매핑으로 생성되어 있어야 합니다.
- `swift-container-sync-objects` Quickwit index가 `swift_container_sync_object_index.yaml` 매핑으로 생성되어 있어야 합니다.
- Proxy Logs dashboard에 데이터를 보려면 `swift-proxy-log-source`/`swift-proxy-log-replica` 같은 collector가 remote Swift proxy log를 Quickwit으로 ingest 중이어야 합니다.
- Container Sync dashboard에 데이터를 보려면 `.env`의 Swift auth 설정이 맞고, source account에 `X-Container-Sync-To`가 설정된 container가 있거나 `SYNC_CONTAINERS`가 지정되어야 합니다.
- Container Sync Recon Web에 데이터를 보려면 각 container node에 `container-sync-recon-exporter`가 실행 중이고, monitoring server에서 node exporter의 `:8010` HTTP endpoint에 접근 가능해야 합니다.

접속 URL:

- Proxy logs dashboard: `http://<monitoring-server>:3000/d/swift_proxy_logs/swift-proxy-logs`
- Container sync dashboard: `http://<monitoring-server>:3000/d/swift_container_sync/swift-container-sync`
- Container sync recon web: `http://<monitoring-server>:8010/`
- Container sync object log search web: `http://<monitoring-server>:8010/logs`
- Container sync recon metrics: `http://<container-node>:8010/metrics` 또는 aggregate 확인용 `http://<monitoring-server>:8010/metrics`
- Quickwit API: `http://<monitoring-server>:7280/api/v1`
- Container sync object log datasource: Grafana datasource `Swift Container Sync Object Logs`
- Container sync object log JSON API: `http://<monitoring-server>:8010/api/object-logs?q=object:<object-name>`

기본 Grafana 계정은 compose 기준 `admin / admin`입니다.

## Provisioning 파일

- Datasource: `grafana/provisioning/datasources/swift-quickwit.yml`
- Dashboard provider: `grafana/provisioning/dashboards/swift-proxy-logs.yml`
- Proxy logs dashboard JSON: `grafana/dashboards/swift-proxy-logs.json`
- Container sync dashboard JSON: `grafana/dashboards/swift-container-sync.json`

## Container Sync Lag Metrics

`sync-lag-exporter`는 source/replica Swift container listing을 비교해서 Prometheus metric을 노출합니다. Grafana dashboard에는 다음 패널이 추가됩니다.

- Synced Containers: source account에서 `X-Container-Sync-To`가 설정된 container 수
- Checked Objects: sync 대상 source container 전체 object 수
- Overall Sync Lag: replica에 아직 없거나 hash/size가 다른 source object의 전체 p95/max age
- Unsynced Ratio: 전체 checked objects 중 missing 또는 mismatch 비율
- Objects by Server: 전체 source/replica object 수 비교
- Lagging Containers: unsynced object가 많은 container 상위 목록
- Sync Exporter Health: exporter의 최근 scrape 성공 여부

실행 전 `.env` 또는 shell 환경변수로 Swift endpoint와 대상 container를 지정합니다.

```bash
SOURCE_AUTH_URL=http://<source-swift>:8080/auth/v1.0
REPLICA_AUTH_URL=http://<replica-swift>:8080/auth/v1.0
SOURCE_AUTH_USER=<source-auth-user>
REPLICA_AUTH_USER=<replica-auth-user>
SOURCE_AUTH_KEY=<source-auth-key>
REPLICA_AUTH_KEY=<replica-auth-key>
DISCOVER_SYNC_CONTAINERS=true
# Optional override only when auto-discovery is not wanted:
# SYNC_CONTAINERS=src-001:dst-001,src-002:dst-002
```


`No data`가 보이면 먼저 Prometheus에서 다음 query를 확인합니다.

```promql
swift_container_sync_configured
swift_container_sync_last_scrape_success
```

`swift_container_sync_configured`가 `0`이면 Swift auth/storage URL이 비어 있거나 자동 발견된 sync container가 없는 상태입니다. `configured=1`인데 `last_scrape_success=0`이면 Swift endpoint, auth user/key, container sync header 또는 네트워크 접근을 확인합니다. 자동 발견을 끄고 싶을 때만 `SYNC_CONTAINERS=src-001:dst-001`처럼 수동 매핑합니다.

노출되는 주요 metric:

```text
swift_container_sync_containers
swift_container_sync_checked_objects
swift_container_sync_unsynced_ratio
swift_container_sync_lag_seconds{container="...",quantile="p95"}
swift_container_sync_lag_seconds{container="...",quantile="max"}
swift_container_sync_unsynced_objects{container="..."}
swift_container_sync_mismatch_objects{container="..."}
swift_container_sync_last_scrape_success{container="..."}
```

## Container Sync Recon Web and Metrics

`container-sync` daemon은 Swift 표준 `container.recon`에 `container_sync_*` 값을 남깁니다. Swift `recon` middleware는 `/recon/container-sync`에서 이 값을 JSON으로 노출합니다. 각 container node에는 `container-sync-recon-exporter`를 systemd 서비스로 붙이고, exporter는 로컬 Swift recon API(`http://127.0.0.1:6201/recon/container-sync`)를 읽어 `/metrics`와 `/api/state`를 제공합니다. Prometheus는 각 node exporter의 `/metrics`를 직접 scrape하고, tst2의 compose exporter는 각 node의 `/api/state`를 읽어 별도 웹 화면으로 합쳐 보여줍니다. recon 파일을 scp로 복사하는 syncer는 더 이상 필요하지 않습니다.

Swift node의 `[container-sync]` 설정에는 다음 값을 둘 수 있습니다. 기본값만으로도 `/var/cache/swift/container.recon`에 기록합니다.

```ini
container_sync_recon_enabled = true
recon_cache_path = /var/cache/swift
recon_interval = 15
recon_max_containers = 1000
```

각 container node에 exporter를 배포합니다. 배포는 SSH/SCP를 사용하지만, 운영 중 recon 수집은 Swift recon API와 HTTP `/metrics`, `/api/state`로 이루어집니다.

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/tst2-monitoring-server
./container-sync-recon-exporter/deploy_node_exporters.sh
```

node exporter가 정상인지 확인합니다.

```bash
curl http://192.168.0.39:6201/recon/container-sync
curl http://192.168.0.39:8010/healthz
curl http://192.168.0.39:8010/metrics | head
```

monitoring server `.env`에는 tst2 웹이 읽을 node exporter URL을 지정합니다.

```bash
CONTAINER_SYNC_RECON_URLS=http://192.168.0.39:8010/api/state,http://192.168.0.56:8010/api/state,http://192.168.0.102:8010/api/state
CONTAINER_SYNC_RECON_WEB_PORT=8010
CONTAINER_SYNC_RECON_WEB_REFRESH_SECONDS=15
CONTAINER_SYNC_RECON_MAX_WEB_CONTAINERS=200
```

Swift 기본 recon CLI에서도 container-sync recon을 확인할 수 있습니다.

```bash
swift-recon container --container-sync --swiftdir /etc/swift
```

Prometheus는 node exporter들을 직접 scrape합니다. `prometheus.yml`의 `swift-container-sync-recon` job 예시는 다음과 같습니다.

```yaml
- job_name: 'swift-container-sync-recon'
  scrape_interval: 15s
  scrape_timeout: 5s
  static_configs:
    - targets:
        - '192.168.0.39:8010'
        - '192.168.0.56:8010'
        - '192.168.0.102:8010'
```

별도 웹 화면에서 보이는 핵심 값:

- Recon nodes up: exporter가 recon 값을 정상적으로 읽은 node 수
- New backlog rows: `max_row - sync_point1` 기준 새 변경 backlog
- Retry backlog rows: `sync_point1 - sync_point2` 기준 retry backlog
- Failed containers: 마지막 상태가 failure인 container 수
- Container table: node/account/container/status/sync point/retry rotation/reason

Prometheus가 scrape하는 주요 recon metric:

```text
swift_container_sync_recon_up
swift_container_sync_recon_last_update_timestamp_seconds
swift_container_sync_recon_daemon_scanned_containers
swift_container_sync_recon_daemon_synced_containers
swift_container_sync_recon_daemon_failed_containers
swift_container_sync_recon_row_attempts_total
swift_container_sync_recon_row_failures_total
swift_container_sync_recon_container_new_backlog_rows
swift_container_sync_recon_container_retry_backlog_rows
swift_container_sync_recon_container_retry_slot_point
```

## Dashboard Panels

### Swift Proxy Logs

Quickwit의 `swift-proxy-logs` index에 적재된 Swift proxy access/error log를 기준으로 보여줍니다.

- Total Requests: 선택한 시간 범위의 전체 log document 수입니다.
- 2xx Success: `status`가 200-299인 성공 응답 수입니다.
- 4xx Client Errors: `status`가 400-499인 client error 응답 수입니다.
- 5xx Server Errors: `status`가 500-599인 server error 응답 수입니다.
- Avg Latency: `request_time` 필드의 평균값입니다. Swift proxy가 요청을 처리한 평균 시간입니다.
- Traffic Over Time: 시간 흐름에 따른 전체 요청량입니다.
- Status Class Over Time: 시간 흐름에 따른 2xx/3xx/4xx/5xx 요청량입니다.
- Top Hosts: log를 발생시킨 proxy host별 요청 수입니다. source/replica proxy가 함께 들어오면 어느 쪽 traffic인지 비교할 때 씁니다.
- Requests by Method: GET/HEAD/PUT/CONNECT 등 HTTP method별 요청 수입니다.
- Latency by Method: method별 평균 `request_time` 추이입니다. 특정 method만 느려지는지 확인합니다.
- Error Traffic: 4xx와 5xx error traffic만 따로 보는 시계열입니다.
- Status Class Breakdown: 선택한 시간 범위의 status class별 전체 분포입니다.
- Recent Proxy Logs: 최근 raw proxy log message와 파싱된 필드를 확인하는 로그 뷰입니다.

### Swift Container Sync

Prometheus가 `sync-lag-exporter`에서 scrape한 metric을 기준으로 source/replica container sync 상태를 보여줍니다.

- Sync Containers: source account에서 sync 대상으로 발견되었거나 `SYNC_CONTAINERS`로 지정된 source container 수입니다.
- Checked Objects: sync 대상 source container들에서 확인한 전체 source object 수입니다.
- Unsynced: replica에 아직 없는 source object 수입니다. 전체 합계(`scope="all"`) 기준입니다.
- Mismatch: source와 replica 양쪽에 있지만 hash 또는 size가 다른 object 수입니다.
- Health: 최근 scrape 성공 여부입니다. `1`이면 정상, `0`이면 Swift auth/network/container listing 중 문제가 있었다는 뜻입니다.
- Overall Sync Lag: missing 또는 mismatch object의 age입니다. p95는 대부분의 지연 수준, max는 가장 오래 밀린 object를 의미합니다.
- Unsynced Ratio: `(missing + mismatch) / checked source objects` 비율입니다. 0에 가까울수록 source/replica가 잘 맞습니다.
- Objects by Server: source 전체 object 수와 replica 전체 object 수 비교입니다.
- Lagging Containers: unsynced object가 많은 container 상위 목록입니다. 어떤 container가 sync backlog를 만드는지 찾는 용도입니다.

## Index

`swift_proxy_index.yaml`은 `source`/`replica` 구분을 위한 `site`, HTTP method/status/path, 처리 시간, transaction id, user agent, access/error 구분 필드를 포함합니다. 기존 index가 이미 생성되어 있다면 새 필드를 terms aggregation에 쓰기 위해 index 재생성이 필요합니다.

## Log Collector

`src-proxy-log-agent/swift_proxy_to_quickwit.py`는 Swift access 로그 한 줄에서 다음 값을 추출합니다.

- `timestamp`, `site`, `host`, `program`
- `client_ip`, `remote_addr`
- `method`, `path`, `protocol`, `status`, `status_text`, `status_class`
- `bytes_sent`, `request_time`, `transaction_id`, `user_agent`
- `message`, `log_type`, `error_code`

원본/복제 대상 구분은 수집기 실행 환경에 `SWIFT_SITE_NAME=source` 또는 `SWIFT_SITE_NAME=replica`로 지정합니다.


Parser notes:

- HTTP access lines are stored as `log_type=access`.
- WSGI/error lines are stored as `log_type=error` and keep their numeric code in `error_code`.
- `status_text` is a raw text copy of `status` for exact status-code terms aggregation in Grafana/Quickwit. Existing indexes must be recreated before this mapped field is available for old data.

## Background Collector Services

Central collector script:

```bash
/opt/swift-log-collector/swift_quickwit_collector.py
```

Installed systemd units:

```bash
/etc/systemd/system/swift-proxy-log-source.service
/etc/systemd/system/swift-proxy-log-replica.service
```

The units tail remote proxy logs over SSH and ingest into local Quickwit:

- source: `ubuntu@<source-swift-proxy>:/var/log/swift/proxy.log`
- replica: `ubuntu@<replica-swift-proxy>:/var/log/swift/proxy.log`
- ingest: `http://127.0.0.1:7280/api/v1/swift-proxy-logs/ingest`

Before starting the services, place the SSH private key at:

```bash
/home/ubuntu/.ssh/<collector-key>.pem
chmod 600 /home/ubuntu/.ssh/<collector-key>.pem
```

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now swift-proxy-log-source swift-proxy-log-replica
systemctl is-active swift-proxy-log-source swift-proxy-log-replica
```

Check logs:

```bash
journalctl -u swift-proxy-log-source -f
journalctl -u swift-proxy-log-replica -f
```

## Container Sync Object Logs with Vector

`container-sync`는 object 단위 처리 결과를 `/var/log/swift/container-sync.log`에 `container-sync-object-event { ... }` 형태의 JSON 로그로 남깁니다. 이 로그는 recon과 분리된 이력 데이터입니다. recon은 현재 상태/시계열 지표용이고, object event log는 Quickwit에서 account/container/object 기준으로 검색하는 용도입니다.

daemon은 account/container/object, method, outcome, reason, timestamp, row id 같은 원천 이벤트만 남깁니다. Quickwit index에 맞춘 `source_path`, `remote_path`, `status_class`, `duration_ms`, `bytes_sent` 같은 보강 필드는 Vector remap 단계에서 생성합니다.

운영 표준 경로는 각 container node에서 Vector가 로컬 로그 파일을 tail해서 Quickwit으로 전송하는 방식입니다. tst2가 SSH로 각 node의 로그를 끌어오는 Python object collector 방식은 더 이상 표준 경로가 아닙니다.

데이터 흐름:

```text
container node /var/log/swift/container-sync.log
        -> vector-container-sync-object.service
        -> Quickwit index swift-container-sync-objects
        -> Quickwit API 또는 Grafana datasource Swift Container Sync Object Logs
```

브라우저 검색 화면:

```bash
http://<monitoring-server>:8010/logs
```

JSON API:

```bash
http://<monitoring-server>:8010/api/object-logs?q=object:<object-name>&max_hits=20
```

Quickwit index 생성:

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/tst2-monitoring-server
curl -X POST http://127.0.0.1:7280/api/v1/indexes \
  -H 'Content-Type: application/yaml' \
  --data-binary @swift_container_sync_object_index.yaml
```

Vector 배포:

```bash
cd /home/ubuntu/knu_swift_2026/monitoring/container-sync-object-vector
./deploy_container_sync_object_vector.sh
```

node가 외부 repository DNS/HTTP에 접근할 수 없으면 먼저 Vector `.deb` 파일을 준비한 뒤 다음처럼 배포합니다.

```bash
VECTOR_DEB_PATH=/path/to/vector.deb ./deploy_container_sync_object_vector.sh
```

외부 repository 접근이 가능하면 공식 Vector APT 설치 스크립트(`https://setup.vector.dev`)로 설치까지 같이 수행할 수 있습니다.

```bash
INSTALL_VECTOR=true ./deploy_container_sync_object_vector.sh
```

서비스 확인:

```bash
systemctl status vector-container-sync-object.service
journalctl -u vector-container-sync-object.service -f
```

검색 예시:

```bash
curl 'http://127.0.0.1:7280/api/v1/swift-container-sync-objects/search?query=object:<object-name>&max_hits=20'
curl 'http://127.0.0.1:7280/api/v1/swift-container-sync-objects/search?query=account:AUTH_test%20AND%20container:src-001&max_hits=20'
curl 'http://127.0.0.1:7280/api/v1/swift-container-sync-objects/search?query=method:PUT%20AND%20outcome:success&max_hits=20'
```

주요 필드:

- `account`, `container`, `object`
- `source_path`, `remote_container_path`, `remote_path`, `path`
- `method`, `outcome`, `reason`, `status_class`
- `host`, `site`, `program`
- `start_timestamp`, `end_timestamp`, `request_time`, `duration_ms`
- `row_id`, `deleted`, `object_bytes`, `bytes_sent`

민감한 원격 endpoint 전체 URL인 `sync_to`는 daemon 로그에 남기지 않습니다. Vector는 daemon이 남긴 `remote_container_path`와 object 이름으로 검색에 필요한 path 필드를 보강합니다.
