# Swift Container Sync Observability

OpenStack Swift `container-sync`의 recon 상태와 event log를 운영자가 Prometheus/Grafana, Quickwit에서 확인할 수 있도록 정리한 제출용 묶음입니다.

이 디렉터리는 서버 역할별 파일을 모아 둡니다. 실제 실행 위치의 파일은 기존 경로에도 유지하고, 여기에는 검토와 배포를 쉽게 하기 위한 복사본을 둡니다.

## 목표

- `container-sync`가 최근 실행됐는지 확인합니다.
- `container-sync` sweep 시간과 누적 처리 통계를 확인합니다.
- `/recon/container-sync` API로 container node의 recon 값을 조회합니다.
- object/container 처리 과정을 JSON event log로 남깁니다.
- Vector로 event log를 Quickwit에 적재하고 Grafana에서 조회합니다.

## 전체 구조

```text
source container node
  container-sync
    -> /var/cache/swift/container.recon
    -> /var/log/swift/container-sync.log

/var/cache/swift/container.recon
  -> Swift recon middleware /recon/container-sync
  -> container_sync_metrics.py
  -> Prometheus /metrics
  -> Grafana dashboard

/var/log/swift/container-sync.log
  -> Vector
  -> Quickwit swift-container-sync-events index
  -> Grafana Quickwit dashboard
```

## 디렉터리 구성

```text
monitoring/swift-container-sync-observability/
  swift-patches/
    container/sync.py
    common/middleware/recon.py
  container-server/
    recon-exporter/
    vector-object-logs/
  tst1-monitoring-server/
    swift-container-sync-events-index.yaml
    grafana/
```

### swift-patches

Swift 코드에 들어가는 패치 파일입니다.

- `container/sync.py`
  - `container-sync` daemon 본체
  - `dump_recon_cache()`로 `/var/cache/swift/container.recon`에 `container_sync_stats`, `container_sync_sweep`, `container_sync_last` 값을 기록
  - `container-sync-event {json}` 형태로 state/report/object event log 기록
  - object event에서 `deleted` 값을 bool로 기록

- `common/middleware/recon.py`
  - Swift recon middleware에 `/recon/container-sync` endpoint 추가
  - `container_sync_stats`, `container_sync_sweep`, `container_sync_last` 반환

### container-server

각 container node에 배포되는 구성입니다.

- `recon-exporter/`
  - `/recon/container-sync` API를 읽는 Prometheus exporter
  - `/metrics` endpoint 제공
  - systemd service 파일 포함

- `vector-object-logs/`
  - `/var/log/swift/container-sync.log` tail
  - `container-sync-event` JSON log 파싱
  - Quickwit `swift-container-sync-events` index로 전송

### tst1-monitoring-server

모니터링 서버에서 사용하는 구성입니다.

- `swift-container-sync-events-index.yaml`
  - Quickwit event index mapping

- `grafana/dashboards/`
  - Prometheus 기반 Container Sync recon dashboard
  - Quickwit 기반 Container Sync event dashboard

## Recon Data

`container-sync`는 Prometheus로 직접 값을 보내지 않고 Swift recon cache에 값을 남깁니다.

```text
sync.py
  -> /var/cache/swift/container.recon
  -> /recon/container-sync
  -> container_sync_metrics.py
  -> /metrics
```

### 기록 위치

```text
/var/cache/swift/container.recon
```

### 추가 key

```text
container_sync_stats
container_sync_sweep
container_sync_last
```

### 누적 작업 통계

`container_sync_stats`에 들어갑니다.

- sync 성공 수
- DELETE 성공 수
- PUT 성공 수
- skip 수
- failure 수

## Quickwit Log

Quickwit은 container-sync event log 검색을 담당합니다.

### event prefix

```text
container-sync-event {json}
```

### 주요 event_type

```text
container_sync_state
container_sync_report
container_sync_object
```

### object event 주요 필드

- `event_type`
- `account`
- `container`
- `object`
- `method`
- `outcome`
- `reason`
- `row_id`
- `deleted`
- `bytes`
- `source_path`
- `remote_container_path`
- `duration_ms`
- `timestamp`
- `status_code`
- `status_class`

## 주요 접속 위치

| 화면/API | URL |
| --- | --- |
| Grafana Container Sync Recon Dashboard | `http://127.0.0.1:3000/d/swift-container-sync/swift-container-sync` |
| Container Sync recon API | `http://<container-node>:6201/recon/container-sync` |
| Container Sync metrics exporter | `http://<container-node>:8000/metrics` |
| Quickwit API | `http://<monitoring-server>:7280/api/v1` |
