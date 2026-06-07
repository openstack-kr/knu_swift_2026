# Swift Monitoring

이 디렉터리는 OpenStack Swift `container-sync` 모니터링 작업 파일을 모아 둔 곳입니다.
구성의 중심은 **Container Sync recon metric**과 **Container Sync event log 검색**입니다.

## 역할 구분

| 도구 | 주 용도 | 확인하는 데이터 |
| --- | --- | --- |
| Swift recon middleware | Container Sync recon API 노출 | `container_sync_stats`, `container_sync_sweep`, `container_sync_last` |
| Container Sync Metrics Exporter | recon API를 Prometheus metric으로 변환 | `swift_container_sync_*` metric |
| Prometheus | exporter metric 저장 | sync 성공/실패, sweep 시간, 마지막 실행 시각 |
| Grafana | Prometheus/Quickwit 데이터 시각화 | recon dashboard, Quickwit event dashboard |
| Vector | container-sync 로그 수집 | `container-sync-event` JSON log |
| Quickwit | event log 검색 | state/report/object event |

## 전체 흐름

### Recon/Prometheus

```text
container-sync daemon
  -> /var/cache/swift/container.recon
  -> /recon/container-sync
  -> container_sync_metrics.py
  -> Prometheus
  -> Grafana
```

### Event/Quickwit

```text
container-sync daemon
  -> /var/log/swift/container-sync.log
  -> Vector
  -> Quickwit swift-container-sync-events index
  -> Grafana Quickwit dashboard
```

## 주요 접속 위치

| 화면/API | URL |
| --- | --- |
| Grafana Container Sync Recon Dashboard | `http://127.0.0.1:3000/d/swift-container-sync/swift-container-sync` |
| Container Sync recon API | `http://<container-node>:6201/recon/container-sync` |
| Container Sync metrics exporter | `http://<container-node>:8000/metrics` |
| Quickwit API | `http://<monitoring-server>:7280/api/v1` |

## 주요 파일

| 경로 | 역할 |
| --- | --- |
| `monitoring/swift-container-sync-observability/` | Container Sync 모니터링 전체 제출 묶음 |
| `monitoring/swift-container-sync-observability/swift-patches/container/sync.py` | recon 기록과 `container-sync-event` JSON log가 추가된 Swift 코드 |
| `monitoring/swift-container-sync-observability/swift-patches/common/middleware/recon.py` | `/recon/container-sync` endpoint가 추가된 recon middleware |
| `monitoring/swift-container-sync-observability/container-server/recon-exporter/` | recon API를 Prometheus metric으로 바꾸는 exporter |
| `monitoring/swift-container-sync-observability/container-server/vector-object-logs/` | container-sync log를 Quickwit으로 보내는 Vector 구성 |
| `monitoring/tst1-monitoring-server/grafana/dashboards/` | Grafana dashboard JSON |
| `monitoring/tst1-monitoring-server/swift-container-sync-events-index.yaml` | Quickwit event index mapping |
| `monitoring/container-sync-object-vector/` | container node 배포용 Vector 구성 복사본 |

## 수정한 파일 목록

### Recon/Prometheus

| 실제 경로 | 설명 |
| --- | --- |
| `/home/ubuntu/swift/swift/container/sync.py` | Container Sync 데몬이 recon 값을 기록하도록 수정 |
| `/home/ubuntu/swift/swift/common/middleware/recon.py` | `GET /recon/container-sync` API 경로 추가 |
| `/home/ubuntu/swift-container-sync-metrics/container_sync_metrics.py` | recon API를 읽어 Prometheus metric으로 변환 |
| `/home/ubuntu/swift-container-sync-metrics/swift-container-sync-metrics.service` | metrics exporter systemd service |
| `/home/ubuntu/swift-container-sync-metrics/swift-container-sync-dashboard.json` | Grafana Recon/Prometheus dashboard |
| `/var/lib/grafana/dashboards/swift-container-sync.json` | Grafana가 실제로 읽는 dashboard |

### Event/Quickwit

| 실제 경로 | 설명 |
| --- | --- |
| `/home/ubuntu/swift-container-sync-vector/container-sync-vector.toml` | Vector tail/parsing/Quickwit 전송 구성 |
| `/home/ubuntu/swift-container-sync-vector/swift-container-sync-events-index.yaml` | Quickwit index mapping |
| `/home/ubuntu/swift-container-sync-vector/swift-container-sync-vector.service` | Vector systemd service |
| `/etc/vector/container-sync-vector.toml` | 실제 container node에 배포된 Vector config |
| `/etc/systemd/system/swift-container-sync-vector.service` | 실제 container node에 배포된 Vector service |
| `/home/ubuntu/swift/swift/container/sync.py` | `container-sync-event` JSON log 추가 |
| `/home/ubuntu/swift-container-sync-vector/swift-container-sync-quickwit-dashboard.json` | Grafana Quickwit dashboard |
| `/var/lib/grafana/dashboards/swift-container-sync-quickwit.json` | Grafana가 실제로 읽는 Quickwit dashboard |

## 배포 대상 container 노드 파일

```text
/home/ubuntu/swift/swift/container/sync.py
/home/ubuntu/swift/swift/common/middleware/recon.py
/opt/swift-container-sync-metrics/container_sync_metrics.py
/etc/systemd/system/swift-container-sync-metrics.service
/etc/vector/container-sync-vector.toml
/etc/systemd/system/swift-container-sync-vector.service
```
