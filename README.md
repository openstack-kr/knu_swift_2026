# knu_swift_2026

OpenStack Swift 의 Container Sync 성능 및 사용성 개선을 위해 진행한
프로젝트입니다. Swift 는 대규모 오브젝트 스토리지를 위한 분산 시스템이며,
본 프로젝트는 container sync에서
발생하는 병목을 코드 패치와 운영 도구 두 갈래로 다룹니다.

## 배경

1. **코드 패치** — 순차 처리를 병렬화하고 노드 간 중복 호출을 제거.
2. **모니터링** — 운영자가 동기화 상태와 개선 효과를 관측할 수 있도록
   지표 수집·시각화 수단을 함께 제공 (진행 중).

## 세부 README

- [container-sync/README.md](container-sync/README.md)
- [monitoring/README.md](monitoring/README.md)

## 팀원

- Oh Jiwoo
- Lee Juyeong
- Hwang Jiyoung
