# Swift Container Sync Observability

이 디렉터리는 Container Sync 관측성 작업을 서버 역할별로 모아 둔 제출용 묶음이다.
실제 실행 위치의 파일도 그대로 유지하며, 여기에는 배포와 검토를 쉽게 하기 위한 복사본을 둔다.

## 구성

### swift-patches

Swift 코드에 들어가는 패치 파일이다.

- `container/sync.py`: container-sync daemon 본체. `container.recon`에 `container_sync_*` recon 값을 기록하고, object 단위 sync 결과를 JSON log로 남긴다.
- `common/middleware/recon.py`: Swift recon middleware에 `/recon/container-sync`와 `/recon/container_sync` API를 추가한다.
- `cli/recon.py`: `swift-recon container --container-sync` 옵션으로 container-sync recon 값을 확인할 수 있게 한다.

### container-server

각 source container node에 배포되는 모니터링 구성이다.

- `recon-exporter/`: node 로컬의 Swift recon API를 읽어 `/metrics`, `/api/state`, 웹 화면을 제공하는 exporter.
- `vector-object-logs/`: `/var/log/swift/container-sync.log`에서 `container-sync-object-event` JSON log만 tail 하여 Quickwit으로 전송하는 Vector 구성.

### proxy-server

Proxy 서버 쪽 Quickwit 로그 수집 구성이다.

- `quickwit-log-agent/`: Swift proxy access log를 Quickwit으로 전송하는 Python collector와 systemd unit.

### tst2-monitoring-server

tst2 모니터링 서버에서 실행되는 구성이다.

- `docker-compose.yml`: Prometheus, Grafana, 중앙 container-sync recon 웹/exporter 서비스.
- `prometheus.example.yml`: container node exporter scrape job 예시.
- `swift_container_sync_object_index.yaml`: Quickwit object-level sync log index 정의.
- `grafana/`: Quickwit datasource와 dashboard JSON.

## Recon 흐름

Container Sync daemon이 container DB를 scan하고 container별 진행 상태를 계산한다.
작업이 진행되면 `dump_recon_cache()`와 `RECON_CONTAINER_FILE` 관례를 사용해
`/var/cache/swift/container.recon`에 `container_sync_*` key를 기록한다.

Swift recon middleware는 이 값을 `/recon/container-sync`로 노출한다.
각 container node의 `swift-container-sync-recon-exporter`는 로컬 recon API를 읽고
Prometheus 형식의 `/metrics`, JSON API인 `/api/state`, 운영 확인용 웹 화면을 제공한다.
Prometheus는 각 container node의 exporter를 scrape하고, Grafana 또는 별도 웹에서 상태를 확인한다.

주요 recon 값은 다음과 같다.

- daemon 상태: 마지막 실행 시작/종료 시각, 실행 시간, scan/sync/skip/fail container 수, timeout 수
- 누적 작업 통계: PUT/DELETE 수, 전송 bytes, row attempt/success/failure 수, remote error 수
- container 진행률: account/container, sync point, max row, backlog, retry rotation/slot, last status/reason

## Quickwit object log 흐름

Recon은 daemon 상태와 수치형 진행률을 위한 데이터이고, Quickwit log는 object 단위 추적을 위한 데이터다.

Container Sync가 object row 하나를 처리할 때 PUT, DELETE, HEAD skip, failure 결과를
`container-sync-object-event` prefix가 붙은 JSON log로 `/var/log/swift/container-sync.log`에 기록한다.
각 container node의 Vector는 이 파일을 tail 하면서 해당 prefix의 로그만 필터링하고 JSON을 파싱한다.
민감하거나 중복되는 `sync_to` 원문은 저장하지 않고, account/container/object/path/method/outcome/reason
필드를 Quickwit `swift-container-sync-objects` index로 전송한다.

이후 운영자는 Quickwit 또는 Grafana datasource에서 다음처럼 검색할 수 있다.

- `object:a.txt`
- `account:AUTH_test AND container:src-001`
- `method:PUT AND outcome:success`
- `outcome:failure`

## 배포 순서

1. `swift-patches`의 Swift 파일을 proxy와 source container node의 Swift 코드 위치에 반영한다.
2. container node에서 Swift recon middleware가 `/recon/container-sync`를 응답하는지 확인한다.
3. `container-server/recon-exporter/deploy_node_exporters.sh`로 container node exporter를 배포한다.
4. Quickwit에 `tst2-monitoring-server/swift_container_sync_object_index.yaml` index를 생성한다.
5. `container-server/vector-object-logs/deploy_container_sync_object_vector.sh`로 Vector object log 수집기를 배포한다.
6. tst2에서 `docker-compose.yml`을 사용해 Prometheus/Grafana/중앙 웹을 실행한다.

## 제외 항목

이 묶음에는 `.env`, SSH private key, 실제 로그 파일, Python bytecode, 백업 파일을 포함하지 않는다.
서버별 주소와 인증값은 배포 환경의 `.env` 또는 systemd environment에서 관리한다.
