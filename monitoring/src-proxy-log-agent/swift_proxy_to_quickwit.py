#!/usr/bin/env python3
import json
import re
import socket
import subprocess
import time
from datetime import datetime, timezone
from urllib import request

QUICKWIT_INGEST_URL = "http://192.168.0.123:7280/api/v1/swift-proxy-logs/ingest"
LOG_FILE = "/var/log/swift/proxy.log"
INDEX_ID = "swift-proxy-logs"

HOST = socket.gethostname()

# Swift proxy 로그 형식이 환경마다 다르므로 우선 흔한 HTTP method/status/time 위주로 추출
METHOD_RE = re.compile(r'\b(GET|PUT|POST|DELETE|HEAD|COPY|OPTIONS)\b')
STATUS_RE = re.compile(r'\s(2\d\d|3\d\d|4\d\d|5\d\d)\s')
TIME_RE = re.compile(r'(\d+\.\d+)$')

def to_rfc3339_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def parse_line(line):
    if "proxy-server" not in line and "swift" not in line.lower():
        return None

    method = None
    status = None
    request_time = None
    path = ""

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

    # path는 /v1/... 형태가 있으면 대충 추출
    p = re.search(r'(/v1/[^\s"]+)', line)
    if p:
        path = p.group(1)

    return {
        "timestamp": to_rfc3339_now(),
        "host": HOST,
        "program": "proxy-server",
        "method": method or "",
        "path": path,
        "status": status or 0,
        "request_time": request_time or 0.0,
        "bytes": 0,
        "message": line.strip(),
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
