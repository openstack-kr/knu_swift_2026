# sync_parallized.py 요약

대상 파일:
- `sync.py`
- `sync_parallized.py`

다이어그램:
- <img width="1700" height="1080" alt="image" src="https://github.com/user-attachments/assets/3aa57ea4-15c7-4082-9e9d-5b8a31fe8ece" />

## 목표

`sync.py`의 row-by-row 순차 처리 대신,
`sync_parallized.py`는 row를 묶어 더 많은 network I/O를 겹치게 하는 것이 목표

## 핵심 변경

1. row 1개 조회 대신 batch 조회
- `_get_row_batch()`로 여러 row를 한 번에 읽음

2. row 처리 병렬화
- `ContextPool`로 여러 `container_sync_row()`를 동시에 실행

3. batch 간에도 처리 이어가기
- `pending_missing`, `pending_new` 큐를 유지해
  현재 작업이 끝나기 전에 다음 batch row를 계속 밀어 넣음

4. sync point DB write 축소
- `broker.set_x_container_sync_points(...)`를 row마다 쓰지 않고
  batch flush 시점에만 갱신

5. HEAD timing 추가
- `_object_in_remote_container()`에서 `head.timing`과 slow HEAD log를 남김

## sync.py와의 차이

`sync.py`
- row 1개 조회
- row 1개 처리
- sync point 즉시 갱신
- 전체가 순차

`sync_parallized.py`
- batch 조회
- 여러 row 동시 처리
- pending queue로 batch 간도 이어서 처리
- sync point는 묶어서 갱신
- HEAD timing 계측 추가

## 바뀌지 않은 것

- 실제 무거운 작업은 여전히 `container_sync_row()` 내부에서 수행
- remote `HEAD`, source `GET`, remote `PUT/DELETE` 구조는 그대로임
- eventual consistency 모델 자체를 바꾸지는 않음

## 한계

- 주 병목은 여전히 network/I/O wait
- 같은 object에 대한 여러 row가 더 많이 겹칠 수 있음
- `InternalClient`를 여러 greenlet이 함께 쓰므로 client 레벨 병목 가능성은 남아 있음

## 검증

- `python3 -m py_compile swift/container/sync_parallelized.py`
- `python3 -m unittest test.unit.container.test_sync_parallelized`
