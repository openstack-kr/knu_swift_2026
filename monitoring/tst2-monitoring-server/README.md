# Swift Proxy Logs with Quickwit and Grafana

Swift Proxy access 로그를 Quickwit에 적재하고 Grafana Quickwit datasource plugin으로 요청량/오류/지연/최근 로그를 보는 구성입니다.

## 구성

- Quickwit: `swift-proxy-logs` index 저장 및 검색
- Grafana: `quickwit-quickwit-datasource` plugin 사용
- Grafana datasource UID: `swift_quickwit_logs`
- Grafana dashboard UID: `swift_proxy_logs`

## 실행

```bash
docker-compose up -d
```

접속 URL:

- Grafana: `http://<monitoring-server>:3000/d/swift_proxy_logs/swift-proxy-logs`
- Quickwit API: `http://<monitoring-server>:7280/api/v1`

기본 Grafana 계정은 compose 기준 `admin / admin`입니다.

## Provisioning 파일

- Datasource: `grafana/provisioning/datasources/swift-quickwit.yml`
- Dashboard provider: `grafana/provisioning/dashboards/swift-proxy-logs.yml`
- Dashboard JSON: `grafana/dashboards/swift-proxy-logs.json`

## Dashboard Panels

- Request Rate
- Requests by Status Class
- Requests by Site
- Requests by Method
- Average Request Time
- Top Paths
- 4xx / 5xx Requests
- Recent Proxy Logs

## Index

`swift_proxy_index.yaml`은 `source`/`replica` 구분을 위한 `site`, HTTP method/status/path, 처리 시간, transaction id, user agent, access/error 구분 필드를 포함합니다. 기존 index가 이미 생성되어 있다면 새 필드를 terms aggregation에 쓰기 위해 index 재생성이 필요합니다.

## Log Collector

`src-proxy-log-agent/swift_proxy_to_quickwit.py`는 Swift access 로그 한 줄에서 다음 값을 추출합니다.

- `timestamp`, `site`, `host`, `program`
- `client_ip`, `remote_addr`
- `method`, `path`, `protocol`, `status`, `status_class`
- `bytes_sent`, `request_time`, `transaction_id`, `user_agent`
- `message`, `log_type`, `error_code`

원본/복제 대상 구분은 수집기 실행 환경에 `SWIFT_SITE_NAME=source` 또는 `SWIFT_SITE_NAME=replica`로 지정합니다.

## Background Collector Services

Central collector script:

```bash
/opt/swift-log-collector/swift_quickwit_collector.py
```

Installed systemd units:

```bash
/etc/systemd/system/swift-proxy-log-source.service
/etc/systemd/system/swift-proxy-log-replica.service
```

The units tail remote proxy logs over SSH and ingest into local Quickwit:

- source: `ubuntu@133.186.209.214:/var/log/swift/proxy.log`
- replica: `ubuntu@133.186.241.189:/var/log/swift/proxy.log`
- ingest: `http://127.0.0.1:7280/api/v1/swift-proxy-logs/ingest`

Before starting the services, place the SSH private key at:

```bash
/home/ubuntu/.ssh/pyk-public.pem
chmod 600 /home/ubuntu/.ssh/pyk-public.pem
```

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now swift-proxy-log-source swift-proxy-log-replica
systemctl is-active swift-proxy-log-source swift-proxy-log-replica
```

Check logs:

```bash
journalctl -u swift-proxy-log-source -f
journalctl -u swift-proxy-log-replica -f
```
