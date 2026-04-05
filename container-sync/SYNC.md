# 아래 문서는 CODEX가 작성한 것이라서 재확인 필요
# sync_parallelized 정리 문서

## 목표

`swift/container/sync.py`의 container sync 성능을 개선하기 위해 row 단위 병렬 처리를 추가했습니다.

다만 원래 `sync.py`가 가지던 sync point 동작은 가능한 한 그대로
유지하는 것을 목표로 했습니다.

## 관련 파일

- `swift/container/sync.py`

## 진입점 동작

- `execute sync_parallelized.py` 문장을 출력합니다.
- `swift.container.sync_parallelized`를 import 합니다.
- `sync_parallelized.main()`으로 실행을 넘깁니다.

## sync_parallelized.py에 추가한 병렬화 규칙

### 1. 설정값

다음 두 개의 설정값을 추가했습니다.

- `sync_row_concurrency`
- `sync_row_batch_size`

각 설정값의 의미는 다음과 같습니다.

- `sync_row_concurrency`: 동시에 실행할 row sync 작업 수
- `sync_row_batch_size`: 한 번에 읽어서 처리할 row 개수

현재 기본값은 다음과 같습니다.

- `sync_row_concurrency = 8`
- `sync_row_batch_size = sync_row_concurrency`

### 2. 추가한 helper 함수

병렬화 버전에는 다음 helper 함수가 추가되었습니다.

```python
def _get_row_batch(self, broker, sync_point, stop_sync_point=None):
    ...

def _run_row_batch(self, rows, sync_to, user_key, broker, info,
                   realm, realm_key):
    ...

def _row_is_mine(self, row, info, nodes, ordinal):
    ...
```

각 함수의 역할은 다음과 같습니다.

- `_get_row_batch`: broker에서 제한된 개수의 row를 가져옵니다.
- `_run_row_batch`: row 묶음을 `ContextPool`로 병렬 처리합니다.
- `_row_is_mine`: 기존과 동일하게 현재 노드가 처리해야 하는 row인지 판별합니다.

### 3. Pool 선택

병렬화 경로에서는 `ContextPool`을 사용했습니다.

이유는 다음과 같습니다.

- row sync 작업이 네트워크 I/O를 수행하는 중일 수 있습니다.
- 현재 scope가 종료될 때 남아 있는 coroutine을 정리할 필요가 있습니다.
- `ContextPool`은 종료 시 남은 coroutine을 정리하는 용도에 맞습니다.
- Swift 내부의 다른 cleanup 성격 코드와도 방향이 맞습니다.

## 코드 변경 내용

### A. batch 실행 helper

실제 병렬 실행은 `_run_row_batch()`에 모아두었습니다.

```python
if self.sync_row_concurrency <= 1 or len(rows) == 1:
    return [(row, self.container_sync_row(
        row, sync_to, user_key, broker, info, realm, realm_key))
        for row in rows]

pool_size = min(self.sync_row_concurrency, len(rows))
with ContextPool(pool_size) as pool:
    coros = []
    for row in rows:
        coros.append((row, pool.spawn(
            self.container_sync_row, row, sync_to, user_key,
            broker, info, realm, realm_key)))
    return [(row, coro.wait()) for row, coro in coros]
```

여기서 중요한 점은 다음과 같습니다.

- row 실행 자체는 병렬로 수행됩니다.
- 결과 수집은 입력 row 순서를 유지합니다.
- sync point 갱신은 worker 내부가 아니라 메인 흐름에서 처리합니다.

### B. 첫 번째 루프: retry 구간 (`sync_point2 -> sync_point1`)

원래 동작은 다음과 같습니다.

- 이전에 실패했거나 누락된 row를 다시 시도합니다.
- 어떤 row에서 처음 실패했는지 기억합니다.
- 마지막에는 그 실패 직전 지점으로 `sync_point2`를 되돌립니다.

병렬화 이후 동작은 다음과 같습니다.

- row를 batch로 읽습니다.
- batch를 병렬 실행합니다.
- 결과는 원래 row 순서대로 해석합니다.
- 첫 실패 row 직전으로 되돌리는 규칙은 유지합니다.

현재 로직은 다음과 같습니다.

```python
last_success_point = sync_point2
results = self._run_row_batch(...)
for row, success in results:
    if not success and next_sync_point is None:
        next_sync_point = last_success_point
    sync_point2 = row['ROWID']
    broker.set_x_container_sync_points(None, sync_point2)
    last_success_point = sync_point2
```

### C. 두 번째 루프: `sync_point1` 이후 신규 row 처리

원래 동작은 다음과 같습니다.

- 새 row를 순서대로 진행합니다.
- 현재 노드가 맡아야 하는 row만 실제 sync 합니다.
- row 처리 성공 여부와 별개로 `sync_point1`은 계속 전진합니다.

병렬화 이후 동작은 다음과 같습니다.

- row를 batch로 읽습니다.
- `sync_point1`은 기존과 같이 row 순서대로 갱신합니다.
- 현재 노드가 맡는 row만 따로 모읍니다.
- 모은 row만 병렬 실행합니다.

현재 로직은 다음과 같습니다.

```python
rows_to_sync = []
for row in rows:
    if self._row_is_mine(row, info, nodes, ordinal):
        rows_to_sync.append(row)
    sync_point1 = row['ROWID']
    broker.set_x_container_sync_points(sync_point1, None)
self._run_row_batch(rows_to_sync, sync_to, user_key, broker, info,
                    realm, realm_key)
```

## 바꾸지 않은 부분

다음 동작은 의도적으로 그대로 유지했습니다.

- 실제 PUT/DELETE 작업은 여전히 `container_sync_row()`가 담당합니다.
- 인증/헤더 생성 로직은 바꾸지 않았습니다.
- remote object 존재 확인 로직도 그대로 유지했습니다.
- sync point 갱신은 worker 내부가 아니라 메인 제어 흐름에서 수행합니다.

## 현재 남아 있는 trade-off

현재 stats 갱신은 `container_sync_row()` 내부에서 수행됩니다.

즉, 이제는 여러 greenthread가 동시에 stats를 갱신할 수 있습니다.

이번 단계에서는 일단 그대로 두었지만,
나중에 집계를 더 엄밀하게 맞추고 싶다면 worker는 결과만 반환하고
메인 루프에서 batch 완료 후 stats를 합산하는 구조로 바꿀 수 있습니다.

## 검증

현재까지 확인한 내용은 다음과 같습니다.

- `python3 -m py_compile swift/container/sync_parallelized.py`
- `python3 -m py_compile test/unit/container/test_sync_parallelized.py`
- `python3 -m unittest test.unit.container.test_sync_parallelized`

추가한 테스트는 다음 두 가지를 확인합니다.

- 첫 번째 루프에서 첫 실패 row 직전으로 rollback 되는지
- 두 번째 루프에서 batch 전체에 대해 sync point가 전진하는지
