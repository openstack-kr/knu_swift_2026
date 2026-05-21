#!/usr/bin/env python3
import json
import re
import os
import socket
import subprocess
import time
from datetime import datetime, timezone
from urllib import request

QUICKWIT_INGEST_URL = "http://192.168.0.123:7280/api/v1/swift-proxy-logs/ingest"
LOG_FILE = "/var/log/swift/proxy.log"
INDEX_ID = "swift-proxy-logs"

HOST = socket.gethostname()
SITE = os.environ.get("SWIFT_SITE_NAME", HOST)

# Swift proxy 로그 형식이 환경마다 다르므로 우선 흔한 HTTP method/status/time 위주로 추출
METHOD_RE = re.compile(r'\b(GET|PUT|POST|DELETE|HEAD|COPY|OPTIONS|CONNECT)\b')
STATUS_RE = re.compile(r'\s(2\d\d|3\d\d|4\d\d|5\d\d)\s')
TIME_RE = re.compile(r'(\d+\.\d+)$')
REQUEST_RE = re.compile(r'\"(GET|PUT|POST|DELETE|HEAD|COPY|OPTIONS|CONNECT)\s+([^\"\s]+)[^\"]*\"')
QUOTED_RE = re.compile(r'\"([^\"]*)\"')
TX_RE = re.compile(r'\b(tx[0-9a-fA-F-]+)\b')

def status_class(status):
    if 200 <= status <= 599:
        return f"{status // 100}xx"
    return "unknown"

def to_rfc3339_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

SWIFT_ACCESS_RE = re.compile(
    r'^(?P<ts>\S+)\s+(?P<host>\S+)\s+(?P<program>[^:]+):\s+(?P<body>.*)$'
)

def parse_timestamp(value):
    try:
        return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return to_rfc3339_now()

def parse_u64(value, default=0):
    try:
        if value in ("", "-"):
            return default
        return int(value)
    except ValueError:
        return default

def parse_f64(value, default=0.0):
    try:
        if value in ("", "-"):
            return default
        return float(value)
    except ValueError:
        return default

def parse_swift_access(line):
    match = SWIFT_ACCESS_RE.match(line.strip())
    if not match:
        return None

    tokens = match.group("body").split()
    if len(tokens) < 16 or not METHOD_RE.fullmatch(tokens[3]):
        return None

    status = parse_u64(tokens[6])
    bytes_sent = parse_u64(tokens[11])
    tx_index = next((i for i, token in enumerate(tokens) if token.startswith("tx")), -1)
    tx = tokens[tx_index] if tx_index >= 0 else ""
    request_time = parse_f64(tokens[tx_index + 2] if tx_index >= 0 and len(tokens) > tx_index + 2 else tokens[15])

    return {
        "timestamp": parse_timestamp(match.group("ts")),
        "host": match.group("host"),
        "site": SITE,
        "program": match.group("program"),
        "client_ip": tokens[0],
        "remote_addr": tokens[1],
        "method": tokens[3],
        "path": tokens[4],
        "protocol": tokens[5],
        "status": status,
        "status_class": status_class(status),
        "bytes_sent": bytes_sent,
        "bytes": bytes_sent,
        "request_time": request_time,
        "transaction_id": tx,
        "user_agent": tokens[8] if len(tokens) > 8 else "",
        "message": line.strip(),
        "log_type": "access",
        "error_code": 0,
    }


def parse_line(line):
    access_doc = parse_swift_access(line)
    if access_doc:
        return access_doc

    if "proxy-server" not in line and "swift" not in line.lower():
        return None

    method = None
    status = None
    request_time = None
    path = ""

    request_match = REQUEST_RE.search(line)
    if request_match:
        method = request_match.group(1)
        path = request_match.group(2)
    else:
        m = METHOD_RE.search(line)
        if m:
            method = m.group(1)

    s = STATUS_RE.search(line)
    if s:
        status = int(s.group(1))

    t = TIME_RE.search(line.strip())
    if t:
        try:
            request_time = float(t.group(1))
        except ValueError:
            request_time = None

    if not path:
        # path는 /v1/... 형태가 있으면 대충 추출
        p = re.search(r'(/v1/[^\s"]+)', line)
        if p:
            path = p.group(1)

    tx = TX_RE.search(line)
    quoted = QUOTED_RE.findall(line)
    user_agent = ""
    if len(quoted) >= 3:
        user_agent = quoted[2]

    return {
        "timestamp": to_rfc3339_now(),
        "host": HOST,
        "site": SITE,
        "program": "proxy-server",
        "method": method or "",
        "path": path,
        "status": status or 0,
        "status_class": status_class(status or 0),
        "client_ip": "0.0.0.0",
        "remote_addr": "0.0.0.0",
        "protocol": "",
        "request_time": request_time or 0.0,
        "bytes_sent": 0,
        "bytes": 0,
        "transaction_id": tx.group(1) if tx else "",
        "user_agent": user_agent,
        "message": line.strip(),
        "log_type": "access" if status else "unknown",
        "error_code": 0,
    }

def post_batch(batch):
    if not batch:
        return

    body = "\n".join(json.dumps(x, ensure_ascii=False) for x in batch).encode()
    req = request.Request(
        QUICKWIT_INGEST_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as e:
        print(f"[WARN] ingest failed: {e}", flush=True)

def main():
    cmd = ["sudo", "tail", "-F", LOG_FILE]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    batch = []
    last_flush = time.time()

    for line in proc.stdout:
        doc = parse_line(line)
        if doc:
            batch.append(doc)

        now = time.time()
        if len(batch) >= 100 or now - last_flush >= 2:
            post_batch(batch)
            batch.clear()
            last_flush = now

if __name__ == "__main__":
    main()
