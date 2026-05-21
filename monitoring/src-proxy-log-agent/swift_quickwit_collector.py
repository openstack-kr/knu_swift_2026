#!/usr/bin/env python3
import argparse
import json
import re
import shlex
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib import request

DEFAULT_QUICKWIT_URL = "http://127.0.0.1:7280/api/v1/swift-proxy-logs/ingest"
DEFAULT_LOG_FILE = "/var/log/swift/proxy.log"
HOST = socket.gethostname()

METHOD_RE = re.compile(r'\b(GET|PUT|POST|DELETE|HEAD|COPY|OPTIONS|CONNECT)\b')
STATUS_RE = re.compile(r'\s(2\d\d|3\d\d|4\d\d|5\d\d)\s')
TIME_RE = re.compile(r'(\d+\.\d+)$')
REQUEST_RE = re.compile(r'\"(GET|PUT|POST|DELETE|HEAD|COPY|OPTIONS|CONNECT)\s+([^\"\s]+)\s+(HTTP/[^\"\s]+)\"')
QUOTED_RE = re.compile(r'\"([^\"]*)\"')
TX_RE = re.compile(r'\b(tx[0-9a-fA-F-]+)\b')
SWIFT_ACCESS_RE = re.compile(r'^(?P<ts>\S+)\s+(?P<host>\S+)\s+(?P<program>[^:]+):\s+(?P<body>.*)$')


def status_class(status):
    if 200 <= status <= 599:
        return f"{status // 100}xx"
    return "unknown"


def now_rfc3339():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value):
    try:
        return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return now_rfc3339()


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


def parse_swift_access(line, site):
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
        "site": site,
        "host": match.group("host"),
        "program": match.group("program"),
        "client_ip": tokens[0] if tokens[0] != "-" else "0.0.0.0",
        "remote_addr": tokens[1] if tokens[1] != "-" else "0.0.0.0",
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


def parse_line(line, site):
    doc = parse_swift_access(line, site)
    if doc:
        return doc

    if "proxy-server" not in line and "swift" not in line.lower():
        return None

    method = ""
    path = ""
    protocol = ""
    status = 0
    request_time = 0.0

    request_match = REQUEST_RE.search(line)
    if request_match:
        method = request_match.group(1)
        path = request_match.group(2)
        protocol = request_match.group(3)
    else:
        m = METHOD_RE.search(line)
        if m:
            method = m.group(1)

    s = STATUS_RE.search(line)
    if s:
        status = int(s.group(1))

    t = TIME_RE.search(line.strip())
    if t:
        request_time = parse_f64(t.group(1))

    if not path:
        p = re.search(r'(/v1/[^\s"]+)', line)
        if p:
            path = p.group(1)

    tx = TX_RE.search(line)
    quoted = QUOTED_RE.findall(line)
    user_agent = quoted[2] if len(quoted) >= 3 else ""

    return {
        "timestamp": now_rfc3339(),
        "site": site,
        "host": HOST,
        "program": "proxy-server",
        "client_ip": "0.0.0.0",
        "remote_addr": "0.0.0.0",
        "method": method,
        "path": path,
        "protocol": protocol,
        "status": status,
        "status_class": status_class(status),
        "bytes_sent": 0,
        "bytes": 0,
        "request_time": request_time,
        "transaction_id": tx.group(1) if tx else "",
        "user_agent": user_agent,
        "message": line.strip(),
        "log_type": "access" if status else "unknown",
        "error_code": 0,
    }


def post_batch(url, batch):
    if not batch:
        return
    body = "\n".join(json.dumps(doc, ensure_ascii=False) for doc in batch).encode()
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=10) as resp:
        resp.read()


def remote_tail_command(args):
    tail_args = "-F" if args.mode == "tail" else f"-n {args.lines}"
    remote = f"sudo tail {tail_args} {shlex.quote(args.log_file)}"
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=accept-new",
    ]
    if args.ssh_key:
        cmd.extend(["-i", args.ssh_key])
    cmd.extend([args.ssh_host, remote])
    return cmd


def local_tail_command(args):
    if args.mode == "tail":
        return ["sudo", "tail", "-F", args.log_file]
    return ["sudo", "tail", "-n", str(args.lines), args.log_file]


def collect_once(args):
    cmd = remote_tail_command(args) if args.ssh_host else local_tail_command(args)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    batch = []
    total = 0
    parsed = 0
    last_flush = time.time()

    try:
        for line in proc.stdout:
            total += 1
            doc = parse_line(line, args.site)
            if doc:
                parsed += 1
                batch.append(doc)

            now = time.time()
            if len(batch) >= args.batch_size or now - last_flush >= args.flush_interval:
                post_batch(args.quickwit_ingest_url, batch)
                batch.clear()
                last_flush = now

            if args.mode == "backfill" and total >= args.lines:
                break
    finally:
        if batch:
            post_batch(args.quickwit_ingest_url, batch)
        if args.mode == "backfill":
            proc.terminate()
        stderr = proc.stderr.read() if proc.stderr else ""
        if stderr.strip():
            print(stderr.strip(), file=sys.stderr)

    print(f"{args.site} total={total} parsed={parsed}", flush=True)
    return proc.wait() if args.mode == "tail" else 0


def main():
    parser = argparse.ArgumentParser(description="Collect Swift proxy logs into Quickwit")
    parser.add_argument("--site", required=True, help="site label, e.g. source or replica")
    parser.add_argument("--ssh-host", help="remote ssh target, e.g. ubuntu@133.186.209.214")
    parser.add_argument("--ssh-key", help="ssh private key path")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE)
    parser.add_argument("--quickwit-ingest-url", default=DEFAULT_QUICKWIT_URL)
    parser.add_argument("--mode", choices=["tail", "backfill"], default="tail")
    parser.add_argument("--lines", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--flush-interval", type=float, default=2.0)
    args = parser.parse_args()

    while True:
        try:
            code = collect_once(args)
            if args.mode == "backfill":
                raise SystemExit(code)
            print(f"[WARN] collector exited with code {code}; reconnecting in 5s", flush=True)
        except Exception as exc:
            print(f"[WARN] collector error: {exc}; reconnecting in 5s", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
