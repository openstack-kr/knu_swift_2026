# knu_swift_2026

이 저장소는 OpenStack Swift `container-sync` 실험을 재현하기 위한 브랜치와 패치 파일을 정리한 저장소입니다.

현재 `experiment` 브랜치는 논문 재현용 구현을 담고 있습니다. 이 저장소는 Swift 전체 소스가 아니라, SAIO 환경에 덮어쓸 `container-sync` 관련 파일만 포함합니다.

## 포함 파일

- `container-sync/sync.py`
- `container-sync/backend.py`
- `test/test_init.sh`

## 브랜치 역할

- `experiment`: 논문 재현용 실험 브랜치

이 저장소에는 비교 기준용 baseline 브랜치를 따로 두지 않았습니다.
비교 대상은 OpenStack Swift 원본 `container-sync` 구현입니다.

- 원본 코드: https://opendev.org/openstack/swift/src/branch/master/swift/container/sync.py

기본적으로 같은 SAIO 환경에서 원본 코드와 `experiment` 구현을 각각 적용한 뒤 같은 방식으로 실행해 성능과 동작을 비교하는 방식으로 사용하면 됩니다.

## 사전 준비

실험을 시작하기 전에 아래 조건이 준비되어 있어야 합니다.

1. OpenStack Swift SAIO가 설치되어 있어야 합니다.
2. `container-sync`를 실행할 수 있는 Swift 환경이 구성되어 있어야 합니다.
3. 비교 실험에 사용할 source/target container, sync key, sync 대상 구성이 준비되어 있어야 합니다.

SAIO 설치는 OpenStack Swift 공식 문서를 참고하면 됩니다.

- https://docs.openstack.org/swift/latest/development_saio.html

초기 실험 환경 세팅은 [`test_init.sh`](/home/ubuntu/knu_swift_2026/test_init.sh) 순서대로 진행하면 됩니다.

## 적용 방법

SAIO에서 사용 중인 Swift 소스 경로를 먼저 확인합니다.

예시:

```bash
export SWIFT_SRC=/path/to/swift
```

그 다음 논문 재현 실험을 위해 `experiment` 브랜치의 파일을 Swift 소스에 덮어씁니다.

### 논문 재현 실험

```bash
git checkout experiment
cp container-sync/sync.py "$SWIFT_SRC/swift/container/sync.py"
cp container-sync/backend.py "$SWIFT_SRC/swift/container/backend.py"
```

파일 교체 후에는 SAIO의 관련 프로세스를 다시 시작해야 합니다. 사용 중인 SAIO 실행 방식에 맞춰 `container-server`, `proxy-server`, `object-server`, `account-server`, `container-sync` 데몬을 재시작하면 됩니다.

## 설정 포인트

`experiment` 브랜치에서는 아래 설정이 중요합니다.

- `sync_row_concurrency`: row 단위 병렬 실행 개수
- `sync_row_batch_size`: 한 번에 읽어오는 row batch 크기

코드 기본값은 다음과 같습니다.

```ini
[container-sync]
sync_row_concurrency = 8
sync_row_batch_size = 24
```

비교 실험에서는 원본 코드와 `experiment` 구현 간 공정성을 위해 아래 값들은 동일하게 유지하는 것을 권장합니다.

- SAIO 구성
- object 수와 object 크기

## `experiment` 브랜치 핵심 변경점

`experiment` 브랜치는 기존 코드 대비 아래 동작을 중심으로 실험할 수 있습니다.

1. row를 한 건씩 처리하지 않고 batch로 읽습니다.
2. row sync를 green thread 기반으로 병렬 실행합니다.
3. retry 진행 상태를 노드 당 slot 단위로 관리합니다.
4. retry 상태를 `memcached`에 공유해 노드 간 진행 상황을 반영합니다.
5. rotation 방식으로 retry 담당 owner slot을 분배합니다.

즉, 이 브랜치는 새 row 처리와 retry 처리 모두에서 병렬성과 공유 상태 기반 재시도를 실험하기 위한 구현입니다.

## 재현 실험 권장 절차

1. SAIO 설치 및 기본 동작 확인
2. `test_init.sh` 순서대로 초기 환경 세팅
3. 원본 Swift `container-sync` 코드로 baseline 실행 및 결과 기록
4. 실험 상태 초기화
5. `experiment` 적용
6. 동일 workload 실행 및 결과 기록
7. 처리 시간, sync 완료 수, retry 동작을 비교

실험 사이에는 가능한 한 같은 초기 상태를 맞추는 것이 중요합니다. 특히 `experiment`는 `memcached` 상태를 사용하므로, 실험 간에는 관련 cache 상태를 비우거나 fresh environment에서 다시 실행하는 것을 권장합니다.

실험 결과, `sync_point2 < sync_point1` 구간에서는 10,000 / 50,000 / 300,000 object 기준으로 기존 방식 대비 각각 `3m 27s -> 15s`, `17m 56s -> 1m 55s`, `112m 40s -> 7m 28s`로 측정되어 `89%` 이상의 처리 시간 감소를 확인했습니다. `sync_point1 <= New Row` 구간에서도 각각 `5m 27s -> 1m 46s`, `31m 56s -> 9m 7s`, `161m 38s -> 55m 8s`로 측정되어 약 `65%`에서 `71%` 수준의 처리 시간 감소를 확인했습니다.
