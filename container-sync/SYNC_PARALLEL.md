# sync_parallel_v4.py 요약

대상 파일:
- `sync_parallel_v4.py`
- `backend_parallel.py`

다이어그램:
- <img width="1680" height="1260" alt="image" src="https://github.com/user-attachments/assets/f62b5e80-f482-455a-9676-95265d6e49a2" />

## 목표

`sync_parallel_v4.py`의 목표는 기존 구조를 유지하면서
retry 구간의 중복 HEAD를 줄이고 row 처리를 병렬화하는 것이다.

- retry row를 모든 노드가 다시 보는 대신 `retry checker`에게 분담한다.
- stale checker가 생기면 다른 노드가 `takeover`해서 retry 구간을 계속 진행한다.
- retry/new row 모두 배치 단위로 읽고 `ContextPool`로 병렬 실행한다.

## v4 실행 흐름

1. 기본 준비
- `ContainerBroker(path)`와 ring 정보로 현재 노드 `ordinal`을 구한다.
- `x-container-sync-to`, `x-container-sync-key`가 없으면 skip 한다.

2. retry 구간
- `sync_point2 < sync_point1`이면 checker별 `retry_state`를 로드한다.
- row owner는 기존처럼 object hash로 계산하고,
  retry checker는 `(owner + 1) % replica_count`로 고정한다.
- 현재 노드는 자기에게 배정된 retry row만 실행하고,
  stale checker는 다른 노드가 takeover 한다.
- retry state는 memcache가 있으면 우선으로 갱신하고,
  종료 시 metadata에도 flush 한다.
- 최종 `sync_point2`는 checker별 point의 최소값이다.

3. 신규 row 구간
- `sync_point1` 이후 row는 기존처럼 owner만 실제 sync 한다.
- row는 batch로 읽고 `ContextPool`로 병렬 실행한다.
- 실제 PUT/DELETE 동작은 여전히 `container_sync_row()`가 담당한다.
- live object는 remote `HEAD`, 필요 시 source `GET` 후 remote `PUT`을 수행한다.

## v4에서 추가된 주요 설정

- `sync_row_concurrency`
  row 병렬 실행 수
- `sync_row_batch_size`
  한 번에 읽는 row 수
- `retry_takeover_timeout`
  checker를 stale 로 판단하는 시간

## sync.py 대비 차이

- `sync.py`는 retry 구간을 사실상 모든 노드가 다시 검사한다.
- `v4`는 retry row도 checker ownership을 부여한다.
- `v4`는 takeover, checker별 progress, batch 병렬 실행을 추가했다.
- `sync_point2`는 단일 progress가 아니라 checker별 point의 최소값으로 계산한다.

## v4 방식 예시

복제 수가 3이라고 가정한다.

- `sp1` 구간은 기존과 같이 write owner가 처리한다.
- 예를 들어 node 1이 `sp1`에서 `1`, `3`을 처리했다면,
  `sp2`에서는 owner가 아닌 checker 역할로 `2`, `4` 같은 다른 row를 본다.
- 즉 기존처럼 모든 노드가 retry 전체를 다시 훑는 대신,
  retry 구간도 대략 1/3씩 나눠서 보게 된다.
- 다만 sync fail 또는 노드 장애가 있으면 fallback/takeover 처리가 필요하다.

## 측정 메모

- memcached 유무에 따른 차이는 크지 않았다.
- `sp1`은 약 `2분 54초 -> 1분 17초`로 개선됐다.
- `sp2`는 약 `1분 55초 -> 17초`로 개선됐다.
- retry 구간 HEAD 요청은 약 1/3로 줄었고, 시간은 약 1/6까지 줄었다.
- object 수가 커질수록 `sp2` 개선 효과가 더 커졌다.

## 현실적인 고민

- 성능은 좋아졌지만 fallback/takeover 가 consistency 를 충분히 보수적으로
  보장하는지는 더 검토가 필요하다.
- 특히 노드 장애 시 중복 전송 방지와 미전송 방지 사이의 균형이 중요하다.
- 그래서 논문 관점에서는 성능 향상과 함께 한계점을 같이 적는 편이 안전하다.

## v5 아이디어

- 더 보수적인 fallback 규칙을 두어 correctness 쪽으로 기울일 수 있다.
- 또 다른 아이디어는 성공한 row를 별도 로컬 DB에 비트맵/장부 형태로 기록하는 것이다.
- 다만 원격 저장 상태와 로컬 장부 상태를 원자적으로 맞춰야 하므로 구현 난도가 높다.
- 그래서 현 단계에서는 구현보다 제안 아이디어나 향후 과제로 두는 편이 현실적이다.
