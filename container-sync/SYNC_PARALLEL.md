# sync_parallel.py 요약

대상 파일:
- `sync.py`
- `sync_parallel.py`
- `backend_parallel.py`

다이어그램:
- `diagrams/sync_parallel_algorithm.svg`

## 목표

`sync.py`의 retry 구간 중복 HEAD를 줄이는 것.

## 핵심 변경

1. 신규 row는 기존 방식 유지
- `sync_point1` 이후 row는 기존처럼 owner 노드가 처리

2. retry 구간은 checker 분리
- `sync_point2 ~ sync_point1` row는 write owner가 아닌
  별도 `retry checker`가 검사

3. takeover 추가
- checker가 오래 멈추면 다음 노드가 대신 검사

4. retry state 저장
- `backend_parallel.py`가 metadata에
  checker별 `{point, updated_at}`를 저장

5. 전역 `sp2`는 최소값
- `sp2 = min(retry_state[*].point)`
- 가장 느린 checker 기준으로만 전진

## sync.py와의 차이

`sync.py`
- retry 구간을 모든 노드가 다시 검사
- 신규 row만 owner 분산
- `sp2`는 단일 progress

`sync_parallel.py`
- retry 구간도 노드 책임을 분리
- stale checker는 takeover
- checker별 progress를 저장한 뒤 최소값으로 `sp2` 계산

## 바뀌지 않은 것

- 실제 sync 작업은 여전히 `container_sync_row()`가 수행
- remote `HEAD`, source `GET`, remote `PUT/DELETE` 구조는 유지
- eventual consistency 모델 자체를 바꾸지는 않음

## 한계

- retry state를 metadata에 저장하므로 상태 해석이 더 복잡해짐
- takeover 규칙이 잘못되면 correctness 리스크가 생길 수 있음
- 주 병목은 여전히 network/I/O
