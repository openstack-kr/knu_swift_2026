# Swift Monitoring

이 디렉터리는 OpenStack Swift 운영 상태를 관측하기 위한 구성 파일을 모아 둔 곳입니다.
현재 구성의 중심은 **Container Sync 관측**입니다. 운영자는 별도 웹 화면에서 현재 상태와 object 이력을 검색하고, Grafana에서는 Prometheus에 쌓인 시계열 지표를 시간 흐름으로 확인합니다.

## 역할 구분

| 도구 | 주 용도 | 확인하는 데이터 |
| --- | --- | --- |
| Container Sync Recon Web | 운영자가 바로 보는 현재 상태와 검색 화면 | recon 상태, container 진행률, object/container event 검색 |
| Prometheus | exporter가 노출하는 metric 저장 | `swift_container_sync_recon_*` 시계열 metric |
| Grafana | Prometheus/Quickwit 데이터를 시각화 | backlog, 실패율, 처리율, recon age, 처리 시간 추이 |
| Quickwit | 로그 검색 엔진 | object 단위 복제 이력, 실패 사유, container event |
| Vector | container node의 로그 수집기 | `/var/log/swift/container-sync.log` tail 및 Quickwit ingest |

## 관측 웹에서 확인 가능한 지표

관측 웹은 `container-sync-recon-exporter`가 제공하는 화면입니다. 기본 접속 위치는 monitoring server 기준 `http://<monitoring-server>:8010/`입니다.

| 화면 | 지표/필드 | 의미 | 주 사용 상황 |
| --- | --- | --- | --- |
| Overview | Recon nodes up | recon 값을 정상적으로 읽은 container node 수 | 특정 node의 exporter 또는 Swift recon API가 죽었는지 확인 |
| Overview | New backlog rows | `max_row - sync_point1` 기준, 아직 새 row 영역에서 scan되지 않은 row 수 | 새 object 변경분이 밀리고 있는지 확인 |
| Overview | Retry backlog rows | `sync_point1 - sync_point2` 기준, retry 확인 구간에 남은 row 수 | 이전 cycle에서 전 노드 확인이 끝나지 않은 row가 남았는지 확인 |
| Overview | Failed containers | 마지막 sync 상태가 failure인 container 수 | 장애 또는 원격지 통신 실패 감지 |
| Overview | Node status | node별 `up/down/not_found/error` 상태 | node 단위 recon 수집 상태 확인 |
| Overview | Recon age | 마지막 recon 갱신 후 지난 시간 | daemon이 최근에 실제 작업했는지 확인 |
| Overview | Last run | 마지막 container-sync 실행 시각 | 실행 주기가 멈췄는지 확인 |
| Overview | Duration s | 마지막 실행 소요 시간 | run 시간이 갑자기 길어졌는지 확인 |
| Overview | Scanned / Synced / Skipped / Failed | 마지막 scan에서 처리한 container 수 | daemon cycle이 정상적으로 container를 훑는지 확인 |
| Overview | Max backlog | node가 관측한 container backlog 최대값 | 어느 node에 backlog가 몰리는지 빠르게 확인 |
| Containers | Account / Container | sync 대상 container 식별자 | 특정 고객/account/container 조회 |
| Containers | Replication | `sync_point2 / max_row` 기준 검증 완료 비율 | container가 보수적으로 어디까지 복제 확인됐는지 확인 |
| Containers | Total objects/rows | object 수 또는 container DB row 수 | container 규모 확인 |
| Containers | New backlog / Retry backlog | container별 backlog row 수 | 어떤 container가 지연을 만드는지 확인 |
| Containers | Failures | 해당 container 관련 실패 event 수 | 실패가 특정 container에 집중되는지 확인 |
| Containers | Error rate | object/container event 대비 실패 비율 | 장애 주입, 네트워크 drop 등으로 오류율이 상승하는지 확인 |
| Containers | Nodes | 해당 container를 보고한 node 목록 | 어떤 replica node가 처리했는지 확인 |
| Containers | Details | object history 검색으로 이어지는 링크 | container 상세 이력으로 이동 |
| Object History | Account / Container / Object 검색 | Quickwit에 적재된 object event 조회 | 특정 object가 언제 복제됐는지 검색 |
| Object History | Method | `PUT`, `DELETE`, `HEAD` 등 처리 method | 실제 전송인지, 삭제인지, remote-current skip인지 구분 |
| Object History | Outcome | `success`, `failure`, `skipped` | object 처리 결과 확인 |
| Object History | Reason | 실패 또는 skip 사유 | 원격지 404/409, client exception, versioning skip 등 원인 확인 |
| Object History | Host / Node | event를 남긴 container node | 어느 node가 처리했는지 추적 |
| Object History | Timestamp / Duration | 처리 시각과 소요 시간 | 언제 복제됐고 느렸는지 확인 |
| Object History | Row id / Bytes | container DB row와 전송 byte | row 진행 상황과 전송량 확인 |

웹 화면은 “지금 운영자가 무엇을 찾아야 하는가”에 맞춰져 있습니다. 특정 account/container/object를 바로 넣어 검색하거나, 실패율이 높은 container에서 object history로 내려가는 흐름을 의도했습니다.

## Grafana에서 확인 가능한 지표

Grafana dashboard는 `http://<monitoring-server>:3000/d/swift_container_sync/swift-container-sync`에서 확인합니다. Grafana는 순간 조회보다 시간 흐름과 추세를 보는 데 적합합니다.

| 패널 | 기반 metric/query | 의미 | 보는 방법 |
| --- | --- | --- | --- |
| Recon Nodes Up | `sum(swift_container_sync_recon_up)` | Prometheus가 정상으로 보는 recon exporter 수 | 기대 node 수보다 작으면 exporter/recon API/네트워크 확인 |
| Failed Sync Containers | `sum(swift_container_sync_recon_daemon_failed_containers)` | 마지막 daemon cycle에서 실패한 container 수 | 0보다 커지면 container table과 object history 확인 |
| New Backlog Rows | `sum(swift_container_sync_recon_daemon_new_backlog_rows)` | 새 row 영역에서 아직 scan되지 않은 전체 backlog | 지속 증가하면 sync 처리량 부족 또는 daemon 정지 의심 |
| Retry Backlog Rows | `sum(swift_container_sync_recon_daemon_retry_backlog_rows)` | retry 확인 구간에 남은 전체 backlog | 특정 노드가 확인하지 못한 row가 남는지 확인 |
| Row Failure Rate | `rate(swift_container_sync_recon_row_failures_total[5m])` | object row 처리 실패율 | 네트워크 drop, remote proxy 장애 시 급증 가능 |
| Max Run Duration | `max(swift_container_sync_recon_daemon_last_run_duration_seconds)` | 최근 run 중 가장 긴 실행 시간 | `container_time`에 가까워지면 처리 시간이 부족한지 확인 |
| Backlog Rows by Node | node별 new/retry backlog | backlog가 특정 node에 몰리는지 확인 | node 색상별로 비교 |
| Object Row Processing Rate | row attempt/success/failure rate | object row 처리량과 성공/실패 흐름 | 시나리오 실행 시 처리 속도와 실패율 변화 확인 |
| PUT / DELETE Rate | `rate(swift_container_sync_recon_puts_total[5m])`, `rate(...deletes_total[5m])` | 실제 원격 PUT/DELETE 처리율 | sync 부하와 처리량 확인 |
| Remote Current Skip Rate | `rate(swift_container_sync_recon_remote_head_skips_total[5m])` | 원격지 object가 이미 최신이라 skip된 비율 | 재시도나 중복 확인 과정에서 skip이 많은지 확인 |
| Last Run Duration by Node | node별 last run duration | node별 실행 시간 차이 | 특정 node만 느려지는지 확인 |
| Container Scan Results | scanned/synced/skipped/failed by node | container scan 결과 | sync 대상이 아닌 container가 많거나 실패가 많은지 확인 |
| Bytes Sent Rate | `rate(swift_container_sync_recon_bytes_total[5m])` | 원격지로 전송한 byte 처리율 | 대용량 object sync 부하 확인 |
| Recon Age by Node | `time() - last_update_timestamp` | node별 recon freshness | 값이 계속 증가하면 daemon/exporter 갱신 중단 의심 |
| Top Container Backlog | container별 max backlog 상위 | backlog가 큰 account/container 목록 | 운영자가 우선 확인할 container 선정 |
| Object History Web Link | 웹 링크 패널 | Grafana에서 object 검색 웹으로 이동 | 상세 검색은 웹/Quickwit에서 수행 |

Grafana는 alert rule과도 연결하기 좋습니다. 예를 들어 `Row Failure Rate > 0`, `Recon Age` 급증, `Failed Sync Containers > 0`, `Retry Backlog Rows` 지속 증가 같은 조건을 알림 후보로 볼 수 있습니다.

## 시계열 데이터 처리 흐름

시계열 데이터는 recon 값을 Prometheus metric으로 바꿔 저장하는 흐름입니다. Container Sync daemon은 Prometheus로 직접 전송하지 않고, Swift 관례에 맞게 recon cache 파일에 값을 남깁니다.

```text
container-sync daemon
  -> /var/cache/swift/container.recon
  -> Swift recon middleware /recon/container-sync
  -> container-sync-recon-exporter :8010
  -> /metrics
  -> Prometheus scrape
  -> Grafana dashboard
```

처리 단계는 다음과 같습니다.

| 단계 | 위치 | 설명 |
| --- | --- | --- |
| 1. daemon 실행 | container node | `swift-container-sync`가 container DB를 scan하고 object row를 처리합니다. |
| 2. recon 기록 | container node | `dump_recon_cache()`로 `/var/cache/swift/container.recon`에 `container_sync_*` 값을 기록합니다. |
| 3. recon API 노출 | container node | Swift recon middleware가 `/recon/container-sync`로 recon JSON을 반환합니다. |
| 4. exporter 변환 | container node | `container-sync-recon-exporter`가 recon JSON을 읽고 Prometheus metric으로 변환합니다. |
| 5. scrape | monitoring server | Prometheus가 각 node의 `:8010/metrics`를 주기적으로 수집합니다. |
| 6. 시각화 | monitoring server | Grafana가 Prometheus query로 backlog, 실패율, 처리율, recon age를 그립니다. |

이 흐름은 “몇 개가 처리됐는가”, “얼마나 밀렸는가”, “최근에 daemon이 돌았는가” 같은 수치 질문에 답합니다.

## 로그 관련 처리 흐름

로그 흐름은 object/container 단위 event를 검색하기 위한 흐름입니다. Recon이 집계 지표라면, Quickwit 로그는 “특정 object가 실제로 언제 어떻게 처리됐는가”를 찾기 위한 원장에 가깝습니다.

```text
container-sync daemon
  -> /var/log/swift/container-sync.log
  -> Vector container-sync-object-vector
  -> Quickwit ingest API
  -> swift-container-sync-objects index
  -> Object History Web / Quickwit search / Grafana datasource
```

처리 단계는 다음과 같습니다.

| 단계 | 위치 | 설명 |
| --- | --- | --- |
| 1. event 생성 | container node | object row 처리 결과를 `container-sync-object-event {json}` 형태로 기록합니다. |
| 2. container report 생성 | container node | container 하나의 처리 요약을 `container-sync-container-event {json}` 형태로 기록합니다. |
| 3. 로그 tail | container node | Vector가 `/var/log/swift/container-sync.log`를 tail 합니다. |
| 4. 필터링/파싱 | container node | Vector가 container-sync event prefix만 필터링하고 JSON 필드를 파싱합니다. |
| 5. 민감 필드 정리 | container node | `sync_to` 원문은 제거하고 검색에 필요한 path/account/container/object 중심 필드만 보냅니다. |
| 6. Quickwit ingest | monitoring server | `swift-container-sync-objects` index에 event document를 저장합니다. |
| 7. 검색 | monitoring server | 웹의 Object History 또는 Quickwit/Grafana datasource에서 account/container/object 기준으로 검색합니다. |

이 흐름은 “이 object가 복제된 적 있는가”, “어느 node에서 실패했는가”, “장애 시점에 어떤 object들이 실패했는가” 같은 검색 질문에 답합니다.

## 주요 접속 위치

| 화면/API | URL |
| --- | --- |
| Container Sync Recon Web | `http://<monitoring-server>:8010/` |
| Container 목록/진행률 | `http://<monitoring-server>:8010/containers` |
| Object History 검색 | `http://<monitoring-server>:8010/logs` |
| Recon JSON API | `http://<monitoring-server>:8010/api/state` |
| Object log JSON API | `http://<monitoring-server>:8010/api/object-logs?q=object:<object-name>` |
| Node exporter metrics | `http://<container-node>:8010/metrics` |
| Grafana Container Sync Dashboard | `http://<monitoring-server>:3000/d/swift_container_sync/swift-container-sync` |
| Quickwit API | `http://<monitoring-server>:7280/api/v1` |

## 주요 파일

| 경로 | 역할 |
| --- | --- |
| `monitoring/swift-container-sync-observability/` | Container Sync 관측성 제출/배포 묶음 |
| `monitoring/swift-container-sync-observability/swift-patches/container/sync.py` | recon 기록과 object/container event log가 추가된 container-sync 패치 |
| `monitoring/tst2-monitoring-server/container-sync-recon-exporter/` | recon API를 읽어 웹 화면과 `/metrics`를 제공하는 exporter |
| `monitoring/tst2-monitoring-server/grafana/dashboards/swift-container-sync.json` | Grafana Container Sync dashboard |
| `monitoring/tst2-monitoring-server/prometheus.example.yml` | Prometheus scrape job 예시 |
| `monitoring/container-sync-object-vector/` | container-sync object/container event 로그를 Quickwit으로 보내는 Vector 구성 |
| `monitoring/tst2-monitoring-server/swift_container_sync_object_index.yaml` | Quickwit object log index mapping |
| `monitoring/tst2-monitoring-server/README.md` | tst2 monitoring server 실행 방법과 세부 구성 |

## 운영자가 보는 기준

- 현재 상태와 상세 검색은 **Container Sync Recon Web**에서 봅니다.
- 시간 흐름, 처리율, backlog 추세, 알림 후보는 **Grafana**에서 봅니다.
- 특정 object/account/container의 복제 이력과 실패 원인은 **Quickwit 기반 Object History**에서 봅니다.
- process가 살아 있는지만 보는 것은 충분하지 않습니다. recon age, last run, backlog, failure rate를 함께 봐야 실제 sync가 정상인지 판단할 수 있습니다.
