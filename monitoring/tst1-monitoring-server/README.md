# TST1 Monitoring Server

Container Sync recon metric과 Quickwit event log를 Grafana에서 확인하기 위한 monitoring server 파일 묶음입니다.

## 구성

- Grafana dashboard
  - `grafana/dashboards/swift-container-sync.json`
  - `grafana/dashboards/swift-container-sync-quickwit.json`
- Quickwit index mapping
  - `swift-container-sync-events-index.yaml`
- Container Sync recon exporter 복사본
  - `container-sync-recon-exporter/container_sync_metrics.py`
  - `container-sync-recon-exporter/systemd/swift-container-sync-metrics.service`

## 접속

Recon/Prometheus dashboard:

```text
http://127.0.0.1:3000/d/swift-container-sync/swift-container-sync
```

Quickwit API:

```text
http://<monitoring-server>:7280/api/v1
```

## Recon/Prometheus 흐름

```text
container node /recon/container-sync
  -> container_sync_metrics.py
  -> Prometheus
  -> grafana/dashboards/swift-container-sync.json
```

## Quickwit event 흐름

```text
container-sync-event JSON log
  -> Vector
  -> Quickwit swift-container-sync-events index
  -> grafana/dashboards/swift-container-sync-quickwit.json
```

## Quickwit index

`swift-container-sync-events-index.yaml`은 `container-sync-event` JSON log를 저장하기 위한 index mapping입니다.
주요 검색 필드는 다음과 같습니다.

```text
event_type
account
container
object
method
outcome
reason
state
status_code
status_class
source_path
remote_container_path
```

## Grafana dashboard

### `swift-container-sync.json`

`/recon/container-sync` 값을 exporter가 Prometheus metric으로 변환한 데이터를 봅니다.

- sync count
- PUT/DELETE count
- skip/failure count
- sweep duration
- last run timestamp

### `swift-container-sync-quickwit.json`

Quickwit에 적재된 container-sync event log를 봅니다.

- state event
- report event
- object success/failure/skipped event
- container/account/object 기준 검색
- failure reason 확인
