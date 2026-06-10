# Swift Monitoring

현재 디렉터리는 OpenStack Swift 운영 상태를 관측하기 위한 구성 파일을 모아 둔 곳입니다.
모니터링 대상은 **Container Sync daemon**입니다. 운영자는 별도 웹 화면에서 현재 상태와 Container, object 이력을 검색하고, Grafana에서는 Prometheus metric과 Quickwit event log를 시간 흐름으로 시각화해 확인할 수 있습니다.

## 시스템 구성도
<img width="1160" height="358" alt="모니터링_시스템구성도" src="https://github.com/user-attachments/assets/629bdba5-dee4-441a-ac11-9cc8211e7bf6" />

## 역할 구분

| 도구 | 주 용도 | 확인하는 데이터 |
| --- | --- | --- |
| Container Sync Recon Web | 운영자가 바로 보는 현재 상태와 상세 검색 화면 | recon 상태, container 진행률, object/container event 조회/검색 |
| Prometheus | exporter가 노출하는 metric 저장 | `swift_container_sync_recon_*`, `swift_container_sync_*` 시계열 metric |
| Grafana | Prometheus metric과 Quickwit event log 시각화 | recon 상태, 처리율, backlog, 실패율, 처리 시간, event 추이 |
| Quickwit | 로그 검색 엔진 | object 단위 복제 이력, 실패 사유, container event |
| Vector | container node의 로그 수집기 | `/var/log/swift/container-sync.log` tail 및 Quickwit ingest |
| Sync Lag Exporter | source/replica container를 직접 비교하는 exporter | 미동기화 object 수, mismatch 수, lag seconds, scrape 성공 여부 |

## 관측 웹에서 확인 가능한 지표

웹은 `container-sync-recon-exporter`가 제공하는 화면으로, 기본 접속 위치는 `http://<monitoring-server>:8010/`입니다.

| 화면 | 지표/필드 | 의미 | 주 사용 상황 |
| --- | --- | --- | --- |
| Containers | Account / Container filter | 특정 account/container로 목록 필터링 | 특정 고객/account/container 조회 |
| Containers | Max events | Quickwit에서 요약에 사용할 최근 event 최대 개수 | 최근 event 기준 집계 범위 조정 |
| Containers | recon containers | recon에서 수집된 container 개수 | recon 데이터가 들어오는지 빠르게 확인 |
| Containers | container events / object events | Quickwit에서 조회된 container/object event 수 | 로그 적재와 검색 상태 확인 |
| Containers | PUT / DELETE Trend | 최근 object event의 `PUT`, `DELETE` 추이 | 복제 작업과 삭제 작업의 최근 흐름 확인 |
| Containers | Container Row / Object Trend | container event의 `max_row`, `object_count` 추이 | row 증가와 object 개수 변화를 빠르게 확인 |
| Containers | Account / Container | sync 대상 container 식별자 | 특정 고객/account/container 조회 |
| Containers | Replication | `sync_point2 / max_row` 기준 검증 완료 비율 | container가 보수적으로 어디까지 복제 확인됐는지 확인 |
| Containers | Total objects/rows | object 수 또는 container DB row 수 | container 규모 확인 |
| Containers | New backlog / Retry backlog | container별 backlog row 수 | 어떤 container가 지연을 만드는지 확인 |
| Containers | Failures | 해당 container 관련 실패 event 수 | 실패가 특정 container에 집중되는지 확인 |
| Containers | Error rate | object event 대비 실패 비율 | 장애 주입, 네트워크 drop 등으로 오류율이 상승하는지 확인 |
| Containers | Nodes | 해당 container를 보고한 node 목록 | 어떤 replica node가 처리했는지 확인 |
| Containers | Last seen | 마지막 event 또는 recon 갱신 시각 | 해당 container가 최근에 처리됐는지 확인 |
| Containers | Details | failure로 필터링된 Object History 링크 | container별 실패 event 상세로 이동 |
| Containers | Node / Failures / Events / Error rate | node별 object event 실패 비율 | 실패가 특정 node에 몰리는지 확인 |
| Containers | Recent failure table | 최근 failure event의 timestamp, host, account, container, reason, object | 최근 실패 원인을 빠르게 확인 |
| Object History | Quickwit query | Quickwit query 문자열 직접 입력 | `object:a.txt OR outcome:failure` 같은 조건 검색 |
| Object History | Account / Container / Object 검색 | Quickwit에 적재된 event 조회 | 특정 object가 언제 복제됐는지 검색 |
| Object History | Method | `PUT`, `DELETE`, `HEAD` 등 처리 method | 실제 전송인지, 삭제인지, remote-current skip인지 구분 |
| Object History | Outcome | `success`, `failure`, `skipped` | object 처리 결과 확인 |
| Object History | Reason | 실패 또는 skip 사유 | 원격지 404/409, client exception, versioning skip 등 원인 확인 |
| Object History | Host / Site / Path | event를 남긴 container node, site, log path | 어느 node와 수집 경로에서 나온 event인지 추적 |
| Object History | Timestamp / Duration ms | 처리 시각과 소요 시간 | 언제 복제됐고 느렸는지 확인 |
| Object History | Max hits | 화면에 표시할 검색 결과 최대 개수 | 큰 로그 검색의 응답량 제한 |

웹 화면은 `Containers`와 `Object History` 두 화면으로 구성됩니다. 루트(`/`)와 `/status`는 `/containers`로 이동하며, node별 recon freshness, daemon last run, scanned/synced/skipped/failed 같은 운영 지표는 웹 테이블이 아니라 `/api/state`, `/metrics`, Grafana에서 확인합니다. 특정 account/container/object를 바로 넣어 검색하거나, 실패율이 높은 container에서 object history로 내려가는 흐름을 의도했습니다.

## Sync Lag Exporter 지표

`sync-lag-exporter`는 recon 값만 보지 않고 Swift API로 source container와 replica container의 object 목록을 직접 비교해 Prometheus metric을 노출합니다. recon 지표가 daemon의 처리 상태를 보여 준다면, lag 지표는 "복제가 실제 결과물까지 맞춰졌는가"를 Prometheus에서 확인하는 보조 지표입니다.

| metric | 의미 | 주 사용 상황 |
| --- | --- | --- |
| `swift_container_sync_lag_seconds` | replica에 없거나 hash/size가 다른 source object의 age. `p50`, `p95`, `max` quantile label 제공 | 오래 밀린 object가 남아 있는지 확인 |
| `swift_container_sync_unsynced_objects` | source에는 있지만 replica에는 없는 object 수 | 실제 미복제 object 수 확인 |
| `swift_container_sync_mismatch_objects` | source와 replica 양쪽에 있지만 hash 또는 size가 다른 object 수 | 부분 전송, 덮어쓰기, 불일치 여부 확인 |
| `swift_container_sync_source_objects` | source container에서 확인한 object 수 | 비교 대상 규모 확인 |
| `swift_container_sync_replica_objects` | replica container에서 확인한 object 수 | replica 쪽 object 수 확인 |
| `swift_container_sync_last_scrape_success` | container별 마지막 비교 scrape 성공 여부 | 인증, 네트워크, Swift API 오류 감지 |
| `swift_container_sync_last_scrape_timestamp_seconds` | 마지막 비교 scrape 시각 | 비교 지표가 최근 값인지 확인 |
| `swift_container_sync_configured` | exporter 필수 설정 존재 여부 | exporter 설정 누락 확인 |
| `swift_container_sync_containers` | 비교 대상으로 잡힌 source/replica container pair 수 | 자동 발견 또는 수동 설정 결과 확인 |
| `swift_container_sync_checked_objects` | 전체 source object 확인 수 | 전체 비교 작업량 확인 |
| `swift_container_sync_unsynced_ratio` | 전체 source object 중 미동기화 또는 mismatch object 비율 | 테스트 시나리오나 장애 상황의 복제 완료율 확인 |

container pair는 `SYNC_CONTAINERS`로 직접 지정하거나, source container의 `X-Container-Sync-To` 값을 읽어 자동 발견할 수 있습니다. container별 metric에는 `container="<source>-><replica>"`, `source_container`, `replica_container` label이 붙고, 집계 값은 `scope="all"` label로 함께 노출됩니다.

## Grafana에서 확인 가능한 지표

Grafana는 두 개의 container-sync 대시보드로 나뉩니다. `Swift Container Sync Recon Metrics`는 recon exporter가 수집한 daemon 상태와 처리량을 Prometheus 시계열로 보고, `Swift Container Sync Events`는 Quickwit에 적재된 object/container event 로그를 시계열로 집계해 시각화합니다.

### Swift Container Sync Recon Metrics

접속 위치: `http://<monitoring-server>:3000/d/swift-container-sync/swift-container-sync`

| 패널 | 기반 metric/query | 의미 | 보는 방법 |
| --- | --- | --- | --- |
| Exporter 상태 | `swift_container_sync_recon_up` | node별 recon exporter scrape 성공 여부 | 기대 node가 빠지면 exporter/recon API/네트워크 확인 |
| Container Sync 서비스 상태 | `swift_container_sync_recon_up` 기반 상태 table | node별 container-sync exporter 상태 | 특정 node만 down이면 해당 node 서비스와 recon endpoint 확인 |
| 노드별 Sync 처리량 Top 10 | `topk(10, swift_container_sync_recon_row_successes_total)` | row sync 성공 누적 상위 node | 처리량이 특정 node에 치우치는지 확인 |
| 노드별 PUT 처리량 Top 10 | `topk(10, swift_container_sync_recon_puts_total)` | 원격 PUT 누적 상위 node | object 생성/갱신 sync 부하 확인 |
| 노드별 DELETE 처리량 Top 10 | `topk(10, swift_container_sync_recon_deletes_total)` | 원격 DELETE 누적 상위 node | 삭제 sync 부하 확인 |
| 노드별 Failure Top 10 | `topk(10, swift_container_sync_recon_row_failures_total)` | row 처리 실패 누적 상위 node | 실패가 몰리는 node 우선 점검 |
| 노드별 Skip Top 10 | `topk(10, swift_container_sync_recon_remote_head_skips_total)` | remote-current skip 누적 상위 node | 이미 최신인 object skip이 많은지 확인 |
| 노드별 최근 Run 상태 | `time() - swift_container_sync_recon_last_update_timestamp_seconds`, `swift_container_sync_recon_daemon_last_run_duration_seconds` | recon freshness와 최근 daemon run duration | recon age가 증가하거나 duration이 길어지는 node 확인 |

### Swift Container Sync Events

접속 위치: `http://<monitoring-server>:3000/d/swift-container-sync-quickwit/swift-container-sync-quickwit`

| 패널 | 기반 데이터/query | 의미 | 보는 방법 |
| --- | --- | --- | --- |
| Object 이벤트 수 | Quickwit object event count | 수집된 container-sync object event 전체량 | 로그 적재가 정상인지 확인 |
| PUT 성공 수 | `method:PUT AND outcome:success` 계열 event | 성공한 PUT sync event 수 | object 생성/갱신 복제 흐름 확인 |
| DELETE 성공 수 | `method:DELETE AND outcome:success` 계열 event | 성공한 DELETE sync event 수 | 삭제 복제 흐름 확인 |
| Failure 수 | `outcome:failure` 계열 event | 실패 event 수 | 장애 주입이나 remote 오류 발생 여부 확인 |
| 노드별 PUT 처리량 | Quickwit event를 host/node별 집계 | PUT 처리가 많은 node | node별 처리 편차 확인 |
| 노드별 DELETE 처리량 | Quickwit event를 host/node별 집계 | DELETE 처리가 많은 node | 삭제 처리 편차 확인 |
| 노드별 Sync 지연 시간(ms) | object event `duration_ms` 집계 | node별 sync 요청 처리 시간 | 특정 node의 처리 지연 확인 |
| Account별 PUT/DELETE 처리량 | account별 event 집계 | account별 sync 부하 | 특정 account에 부하가 몰리는지 확인 |
| Failure 원인별 추이 | failure reason별 event 집계 | 실패 사유 변화 | 404/409/client exception 등 원인 분류 |
| 노드별 Failure 원인 | node와 reason 기준 event 집계 | node별 실패 원인 분포 | 특정 node에서 반복되는 실패 사유 확인 |
| 컨테이너별 PUT/DELETE 처리량 | container별 event 집계 | container별 sync 부하 | 부하가 큰 container 선정 |
| 컨테이너별 Progress | container event의 progress 관련 field | container별 진행률 변화 | 특정 container가 정체되는지 확인 |
| 컨테이너 Row 현황 | container event의 `max_row`, `object_count` 등 | row/object 규모와 변화 | container DB row 증가와 object 수 변화 확인 |
| 최근 컨테이너 Run 현황 | 최근 container event table | 최근 처리된 container와 결과 | 상세 원인 분석 시작점으로 사용 |

Grafana는 alert rule과도 연결하기 좋습니다. 예를 들어 `Row Failure Rate > 0`, `Recon Age` 급증, `Failed Sync Containers > 0`, `Retry Backlog Rows` 지속 증가 같은 조건을 알림 후보로 볼 수 있습니다.

## 시계열 데이터 처리 흐름

시계열 데이터는 recon 값을 Prometheus metric으로 바꿔 저장하는 흐름으로 수집됩니다.

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

실제 복제 결과 비교가 필요한 경우에는 별도 exporter가 Swift API를 조회합니다.

```text
sync-lag-exporter
  -> source Swift API object listing
  -> replica Swift API object listing
  -> object hash/size/timestamp 비교
  -> /metrics
  -> Prometheus scrape
```

sync-lag metric을 통해 “source에 있는 object가 replica에도 실제로 존재하는가”, “남은 미동기화 object가 얼마나 오래됐는가” 같은 복제 결과를 검증할 수 있습니다.

## 로그 관련 처리 흐름

로그 흐름은 object/container 단위 event를 저장하고, 웹 검색과 Grafana 시계열 시각화에 쓰기 위한 흐름입니다. Recon이 집계 지표라면, Quickwit 로그는 “특정 object가 실제로 언제 어떻게 처리됐는가”를 찾기 위한 데이터입니다.

```text
container-sync daemon
  -> /var/log/swift/container-sync.log
  -> Vector container-sync-object-vector
  -> Quickwit ingest API
  -> swift-container-sync-objects index
  -> Object History Web / Quickwit search / Grafana Events dashboard
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
| 7. 조회/시각화 | monitoring server | 웹의 Object History와 Quickwit API는 account/container/object 기준 상세 검색에 쓰고, Grafana Events dashboard는 event를 시간 축으로 집계해 보여줍니다. |

로그 데이터는 웹에서는 “이 object가 복제된 적 있는가”, “어느 node에서 실패했는가” 같은 상세 이력 검색에 쓰이고, Grafana에서는 장애 시점의 실패량, 처리량, 지연 시간 추이를 보는 데 쓰입니다.

## 주요 접속 위치

| 화면/API | URL |
| --- | --- |
| Container Sync Recon Web | `http://<monitoring-server>:8010/` |
| Container 목록/진행률 | `http://<monitoring-server>:8010/containers` |
| Object History 검색 | `http://<monitoring-server>:8010/logs` |
| Recon JSON API | `http://<monitoring-server>:8010/api/state` |
| Object log JSON API | `http://<monitoring-server>:8010/api/object-logs?q=object:<object-name>` |
| Node exporter metrics | `http://<container-node>:8010/metrics` |
| Sync lag metrics | Prometheus scrape target `sync-lag-exporter:8000/metrics` |
| Grafana Recon Metrics Dashboard | `http://<monitoring-server>:3000/d/swift-container-sync/swift-container-sync` |
| Grafana Events Dashboard | `http://<monitoring-server>:3000/d/swift-container-sync-quickwit/swift-container-sync-quickwit` |
| Quickwit API | `http://<monitoring-server>:7280/api/v1` |

## 주요 파일

| 경로 | 역할 |
| --- | --- |
| `monitoring/swift-container-sync-observability/` | Container Sync 관측성 제출/배포 묶음 |
| `monitoring/swift-container-sync-observability/swift-patches/container/sync.py` | recon 기록과 object/container event log가 추가된 container-sync 패치 |
| `monitoring/tst2-monitoring-server/container-sync-recon-exporter/` | recon API를 읽어 웹 화면과 `/metrics`를 제공하는 exporter |
| `monitoring/tst2-monitoring-server/sync-lag-exporter/` | source/replica container object 목록을 비교해 실제 sync lag metric을 제공하는 exporter |
| `monitoring/tst2-monitoring-server/grafana/dashboards/swift-container-sync.json` | Grafana Swift Container Sync Recon Metrics dashboard |
| `monitoring/tst2-monitoring-server/grafana/dashboards/swift-container-sync-quickwit.json` | Grafana Swift Container Sync Events dashboard |
| `monitoring/tst2-monitoring-server/prometheus.example.yml` | Prometheus scrape job 예시 |
| `monitoring/container-sync-object-vector/` | container-sync object/container event 로그를 Quickwit으로 보내는 Vector 구성 |
| `monitoring/tst2-monitoring-server/swift_container_sync_object_index.yaml` | Quickwit object log index mapping |
| `monitoring/tst2-monitoring-server/README.md` | tst2 monitoring server 실행 방법과 세부 구성 |

## 운영자가 보는 기준

- 현재 상태와 상세 검색: **Container Sync Recon Web**
- 시간 흐름, 처리율, backlog 추세, event 추이, 알림 후보: **Grafana**
- source와 replica의 실제 object 차이: **Sync Lag Exporter 지표 또는 Prometheus query**
- 특정 object/account/container의 복제 이력과 실패 원인: **Quickwit 기반 Object History**
- recon age, last run, backlog, failure rate 등의 recon 지표와 unsynced object, lag seconds 등의 sync-lag 지표를 함께 활용해 실제 sync가 정상인지 판단하는 기준으로 활용 가능
