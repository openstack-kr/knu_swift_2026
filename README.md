# knu_swift_2026

OpenStack Swift의 Container Sync 성능과 운영 사용성을 개선하기 위해 진행한 프로젝트입니다.
Container Sync 처리 과정에서 발생하는 병목을 줄이기 위해 코드 패치를 적용하고, 운영자가 동기화 상태와 개선 효과를 확인할 수 있도록 모니터링 도구를 구성했습니다.

- 원본 코드 저장소: <https://opendev.org/openstack/swift/>

## 프로젝트 범위

1. **코드 패치** — 순차 처리를 병렬화하고 노드 간 중복 호출을 제거
2. **모니터링** — 운영자가 동기화 상태와 개선 효과를 관측할 수 있도록 지표 수집·시각화 수단 제공

## 브랜치 구성

전반적인 실험, 문서, 모니터링, 테스트 작업을 브랜치별로 나누어 관리했습니다.

| 브랜치 | 의미 |
| --- | --- |
| `main` | 프로젝트의 기준 브랜치입니다. 병합된 코드, 문서, 테스트, 모니터링 산출물이 모이는 기본 브랜치입니다. |
| `docs/README` | README와 하위 문서 정리를 위한 문서 작업 브랜치입니다. |
| `experiment` | Container Sync 개선안과 모니터링 구성을 함께 실험한 통합 실험 브랜치입니다. |
| `feature/container-sync-monitoring` | Container Sync 상태를 수집·시각화하기 위한 모니터링 구성, Swift 패치, Grafana/Prometheus/Quickwit 설정을 다루는 브랜치입니다. |
| `feature/hjy-sync` | 황지영 팀원의 아이디어 테스트를 위한 브랜치: `sync_multiprocess.py`, task queue, HTTP pool 등 멀티프로세스 기반 Container Sync 개선안을 다루는 브랜치입니다. |
| `feature/jw-sync` | 오지우 팀원의 아이디어 테스트를 위한 브랜치: `container-sync/sync.py` 중심의 Container Sync 개선 작업을 `main` 흐름과 맞춰 정리한 브랜치입니다. |
| `feature/ljy-sync` | 이주영 팀원의 아이디어 테스트를 위한 브랜치: 병렬 Sync 알고리즘을 단계별 버전(`sync_parallel_v1`~`v5`)으로 실험하고 문서화한 브랜치입니다. 최종 아이디어로 채택되었습니다. |
| `feature/main2` | `container-sync/sync.py`의 초기 개선 로직을 정리한 브랜치입니다. |
| `feature/main3` | `updated_owners` 처리 방식 등 `feature/main2` 이후의 세부 로직을 보정한 브랜치입니다. |
| `feature/main4` | retry slot 저장 로직과 주석 정리를 포함한 `container-sync/sync.py` 개선 브랜치입니다. |
| `feature/main4-sync-to-experiment` | `feature/main4`의 Container Sync 구현을 `experiment` 흐름에 반영하기 위한 연결 브랜치입니다. |
| `logger` | retry 상태가 완료되는 시점의 로그를 추가한 브랜치입니다. |
| `monitoring` | 운영 관측용 대시보드, 지표 흐름, 문서 개선을 진행한 모니터링 브랜치입니다. |
| `retry-state-logs` | Container Sync retry 상태 전환을 추적하기 위한 상세 로그를 추가한 브랜치입니다. |
| `test` | Container Sync 실험과 검증에 필요한 셸 스크립트를 `test/` 디렉터리에 모은 브랜치입니다. |

## 세부 README

- [container-sync/README.md](container-sync/README.md) - Container Sync 코드 패치와 병렬화 실험 설명
- [monitoring/README.md](monitoring/README.md) - 모니터링 구성, 지표 수집, 대시보드 설명

## 팀원

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/imjyong">
        <img src="https://github.com/imjyong.png" width="100px;" alt="Hwang Jiyoung"/><br />
        <sub><b>황지영</b></sub>
      </a><br />
      <sub>Project Lead</sub>
    </td>
    <td align="center">
      <a href="https://github.com/5hjiwoo">
        <img src="https://github.com/5hjiwoo.png" width="100px;" alt="Oh Jiwoo"/><br />
        <sub><b>오지우</b></sub>
      </a><br />
      <sub>Member</sub>
    </td>
    <td align="center">
      <a href="https://github.com/ale8ander">
        <img src="https://github.com/ale8ander.png" width="100px;" alt="Lee Juyeong"/><br />
        <sub><b>이주영</b></sub>
      </a><br />
      <sub>Member</sub>
    </td>
  </tr>
</table>
