# container-sync 개선 알고리즘

OpenStack Swift의 `swift/container/sync.py` 개선 작업입니다.
**최종본은 [`patch2/sync.py`](patch2/sync.py)** 이며, 원본 및 중간 단계와
나란히 비교할 수 있도록 보관합니다.

## 시스템 구성도
<img width="1280" height="661" alt="object storage (16)" src="https://github.com/user-attachments/assets/bd6ca70a-c634-4366-8f3b-10dcd432baf1" />

## 액티비티 다이어그램
<img width="459" height="487" alt="스크린샷 2026-06-10 오후 3 54 43" src="https://github.com/user-attachments/assets/784ca99d-d0ed-43d7-b46e-6609c5a6af0d" />

## 파일 구성

| 경로 | 설명 |
| --- | --- |
| [origin/sync.py](origin/sync.py) | 원본 `swift/container/sync.py` |
| [patch1/sync.py](patch1/sync.py) | **개선 1**: Phase 2(신규 row) 를 `ContextPool` 로 병렬화 |
| [patch2/sync.py](patch2/sync.py) | **개선 1 + 개선 2**: Phase 1(재시도 구간) 에 owner slot · rotation 구조 추가 |
| [patch1/test_sync.py](patch1/test_sync.py) | 단위 테스트 |
| [patch1/container-server.conf-sample](patch1/container-server.conf-sample) | 새 설정 키가 반영된 샘플 conf |
| [patch1/overview_container_sync.rst](patch1/overview_container_sync.rst) | 동작 개요 문서 |

## 변경 동기

Swift 의 컨테이너 동기화는 서로 다른 컨테이너 간 오브젝트 변경 사항을
복제하는 기능입니다. 컨테이너 DB(SQLite3) 의 각 행(ROWID)은 오브젝트의
생성/삭제 이력을 의미하며, 동기화 진행 지점은 두 값으로 관리됩니다.

- `sync_point1` — 해당 노드가 동기화를 마친 마지막 행
- `sync_point2` — 재동기화가 진행된 마지막 행

동기화는 두 구간으로 나뉩니다.

- **Phase 1 (`sync_point2 < sync_point1`)** — 이미 한 번 처리된 구간을
  모든 노드가 다시 훑으며 누락된 행이 있으면 재동기화.
- **Phase 2 (`sync_point1 ≤ New Row`)** — 새로 추가된 행을 해시 기반으로
  노드 간 분담하여 동기화.

두 구간 모두 단일 프로세스에서 행 단위로 순차 처리되며, 행마다 원격
HEAD/PUT/DELETE HTTP 호출이 끼어 있어 네트워크 I/O 대기 시간이 누적됩니다.
또한 Phase 1 은 모든 노드가 같은 행을 다시 확인하므로 복제본 수만큼
HEAD 가 중복됩니다. 오브젝트 수가 늘어날수록 처리량이 떨어지고 동기화
지연이 커지는 구조입니다.

따라서 본 작업의 개선 방향은 두 가지입니다.

1. **행 단위 순차 처리를 병렬화** 하여 HTTP 호출이 끼어드는 대기 시간을
   다른 행 처리로 겹치게 한다.
2. **노드 간 중복 HEAD 를 제거** 하여 Phase 1 에서 발생하는 불필요한
   네트워크 I/O 자체를 줄인다.

## 개선 1 — Phase 2 병렬화 (/patch1)

- `swift.common.utils.ContextPool` 도입.
- 설정 키 `sync_row_concurrency` (기본 8) 추가.
- Phase 2 루프를 `ContextPool` 로 감싸 `container_sync_row` 호출을 최대
  `sync_row_concurrency` 개의 그린스레드로 동시 실행. `pool.spawn` 이 풀이
  가득 차면 블록되므로 별도 큐 없이 backpressure 가 걸립니다.
- 루프 종료 직전 `pool.waitall()` 로 모든 작업 완료를 보장 — `ContextPool`
  의 `__exit__` 가 실행 중인 코루틴을 죽이기 때문에 반드시 먼저 호출해야
  합니다.

## 개선 2 — Phase 1 의 owner slot + rotation (/patch2)

memcache 기반 상태 공유로 Phase 1 의 노드 간 중복을 제거합니다.

- 의존성: `swift.common.memcached.load_memcache`.
- 설정 키: `sync_row_batch_size` (기본 100). `__init__` 에서
  `self.retry_memcache = load_memcache(conf, self.logger)` 도 함께 등록.
- `owner_index` 별로 독립적인 retry slot (`{'point': N}`) 을 두고,
  각 슬롯을 처리할 노드를 `rotation` 값으로 결정:

  ```
  처리 노드 ordinal = (owner_index + rotation) % node_count
  ```

  같은 retry window 동안 모든 노드가 동일한 `rotation` 을 공유하므로
  하나의 owner slot 은 한 노드만 처리 → 중복 HEAD 가 사라집니다.

### Phase 1 흐름

1. **Read** `_read_retry_state()` — memcache 에서 `rotation` 과 각 slot 의
   `point` 를 읽고, 로컬 `sync_point2` 를 슬롯들의 최솟값으로 정렬.
2. **Sync** `_sync_retry_slot()` — 자기 차례의 slot 만 처리. `sync_row_batch_size`
   단위로 row 를 가져와 자기 owner row 만 골라 `ContextPool` 로 병렬 sync.
3. **Store** `_store_retry_slot()` — 갱신된 slot 들을 memcache 에 기록.
4. **Finalize** `_finalize_retry_state()` — memcache 상태를 재조회.
   - 모든 slot 이 `target_sync_point1` 에 도달 → `_complete_retry_state()`
     로 `rotation = 0` 으로 리셋하고 slot 들을 정렬.
   - 미완료 시 `incr` 기반 짧은 락 (TTL = `max(interval, container_time)`,
     `run_forever()` 시작 jitter 보다 김) 으로 단일 노드만 `rotation += 1`.

memcache 가 없으면 한 패스 안에서의 분배는 동작하지만 패스 간 상태 공유가
없어 다음 패스에서 모든 노드가 다시 같은 slot 을 보게 됩니다.

## 실험 결과

Object 크기는 모두 1Byte 로 통일.

### Phase 1: `sync_point2 < sync_point1` 구간 (개선 1 + 개선 2 적용)

| Object 개수 | Swift (원본) | 개선 | 시간 감소율 |
| ---: | ---: | ---: | ---: |
| 10,000 | 3m 27s | 15s | **93.09%** |
| 50,000 | 17m 56s | 1m 55s | **89.31%** |
| 300,000 | 112m 40s | 7m 28s | **93.37%** |

모든 조건에서 89% 이상의 처리 시간 감소.

### Phase 2: `sync_point1 ≤ New Row` 구간 (개선 1만 적용)

| Object 개수 | Swift (원본) | 개선 | 시간 감소율 |
| ---: | ---: | ---: | ---: |
| 10,000 | 5m 27s | 1m 46s | **67.58%** |
| 50,000 | 31m 56s | 9m 7s | **71.46%** |
| 300,000 | 161m 38s | 55m 8s | **65.89%** |

모든 조건에서 65% 이상의 처리 시간 감소.
