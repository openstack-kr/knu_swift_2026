# Swift Proxy Logs with Quickwit and Grafana

Swift Proxy access 로그를 Quickwit에 적재하고 Grafana Quickwit datasource plugin으로 요청량/오류/지연/최근 로그를 보는 구성입니다.

## 구성

- Quickwit: `swift-proxy-logs` index 저장 및 검색
- Grafana: `quickwit-quickwit-datasource` plugin 사용
- Grafana datasource UID: `swift_quickwit_logs`
- Grafana dashboard UID: `swift_proxy_logs`
- Container sync dashboard UID: `swift_container_sync`

## 실행

```bash
docker-compose up -d
```

접속 URL:

- Proxy logs dashboard: `http://<monitoring-server>:3000/d/swift_proxy_logs/swift-proxy-logs`
- Container sync dashboard: `http://<monitoring-server>:3000/d/swift_container_sync/swift-container-sync`
- Quickwit API: `http://<monitoring-server>:7280/api/v1`

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

## Dashboard Panels

- Request Rate
- Requests by Status Class
- Requests by Site
- Requests by Method
- Average Request Time
- Top Paths
- 4xx / 5xx Requests
- Recent Proxy Logs

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
