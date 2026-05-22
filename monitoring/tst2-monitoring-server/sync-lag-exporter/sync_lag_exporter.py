#!/usr/bin/env python3
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


SOURCE_SWIFT_URL = os.getenv("SOURCE_SWIFT_URL", "").rstrip("/")
REPLICA_SWIFT_URL = os.getenv("REPLICA_SWIFT_URL", "").rstrip("/")
SOURCE_AUTH_URL = os.getenv("SOURCE_AUTH_URL", "").rstrip("/")
REPLICA_AUTH_URL = os.getenv("REPLICA_AUTH_URL", "").rstrip("/")
SOURCE_AUTH_USER = os.getenv("SOURCE_AUTH_USER", "")
REPLICA_AUTH_USER = os.getenv("REPLICA_AUTH_USER", "")
SOURCE_AUTH_KEY = os.getenv("SOURCE_AUTH_KEY", "")
REPLICA_AUTH_KEY = os.getenv("REPLICA_AUTH_KEY", "")
SOURCE_AUTH_TOKEN = os.getenv("SOURCE_AUTH_TOKEN", "")
REPLICA_AUTH_TOKEN = os.getenv("REPLICA_AUTH_TOKEN", "")
DISCOVER_SYNC_CONTAINERS = os.getenv("DISCOVER_SYNC_CONTAINERS", "true").lower() not in ("0", "false", "no")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
MAX_PAGES = int(os.getenv("MAX_PAGES_PER_CONTAINER", "100"))
MAX_CONTAINERS = int(os.getenv("MAX_CONTAINERS", "10000"))
PORT = int(os.getenv("EXPORTER_PORT", "8000"))


def parse_container_pairs(value):
    pairs = []
    for raw in value.split(","):
        item = raw.strip()
        if not item:
            continue
        if ":" in item:
            source_container, replica_container = item.split(":", 1)
        elif "=" in item:
            source_container, replica_container = item.split("=", 1)
        else:
            source_container = replica_container = item
        source_container = source_container.strip()
        replica_container = replica_container.strip()
        if source_container and replica_container:
            pairs.append((source_container, replica_container))
    return pairs


def auth_token(auth_url, user, key):
    if not auth_url or not user or not key:
        return "", ""
    request = urllib.request.Request(auth_url)
    request.add_header("X-Auth-User", user)
    request.add_header("X-Auth-Key", key)
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        token = response.headers.get("X-Auth-Token", "")
        storage_url = response.headers.get("X-Storage-Url", "").rstrip("/")
    return token, storage_url


def swift_connection(base_url, token, auth_url, user, key):
    if base_url and token:
        return base_url, token
    auth_token_value, storage_url = auth_token(auth_url, user, key)
    return base_url or storage_url, token or auth_token_value


def escape_label(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def metric(name, value, labels=None):
    labels = labels or {}
    label_text = ""
    if labels:
        pairs = [f'{key}="{escape_label(val)}"' for key, val in sorted(labels.items())]
        label_text = "{" + ",".join(pairs) + "}"
    return f"{name}{label_text} {value}"


def parse_timestamp(value):
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    if "." in normalized:
        left, right = normalized.split(".", 1)
        tz = ""
        if "+" in right:
            frac, tz = right.split("+", 1)
            tz = "+" + tz
        elif "-" in right:
            frac, tz = right.split("-", 1)
            tz = "-" + tz
        else:
            frac = right
        normalized = f"{left}.{frac[:6]}{tz}"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def account_url(base_url, marker=""):
    params = {"format": "json"}
    if marker:
        params["marker"] = marker
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def container_url(base_url, container, marker=""):
    params = {"format": "json"}
    if marker:
        params["marker"] = marker
    return f"{base_url}/{urllib.parse.quote(container)}?{urllib.parse.urlencode(params)}"


def fetch_account_containers(base_url, token):
    containers = []
    marker = ""

    while len(containers) < MAX_CONTAINERS:
        request = urllib.request.Request(account_url(base_url, marker))
        if token:
            request.add_header("X-Auth-Token", token)

        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            page = json.loads(response.read().decode("utf-8"))

        if not page:
            break

        for item in page:
            name = item.get("name")
            if name:
                containers.append(name)
                if len(containers) >= MAX_CONTAINERS:
                    break

        marker = page[-1].get("name", "")
        if len(page) < 10000 or not marker:
            break

    return containers


def fetch_container_sync_to(base_url, token, container):
    request = urllib.request.Request(f"{base_url}/{urllib.parse.quote(container)}", method="HEAD")
    if token:
        request.add_header("X-Auth-Token", token)
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return response.headers.get("X-Container-Sync-To", "")


def replica_container_from_sync_to(sync_to):
    if not sync_to:
        return ""
    parsed = urllib.parse.urlparse(sync_to if "://" in sync_to else "http:" + sync_to)
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if len(parts) >= 2:
        return parts[-1]
    return ""


def discover_container_pairs(source_url, source_token):
    pairs = []
    for source_container in fetch_account_containers(source_url, source_token):
        sync_to = fetch_container_sync_to(source_url, source_token, source_container)
        replica_container = replica_container_from_sync_to(sync_to)
        if replica_container:
            pairs.append((source_container, replica_container))
    return pairs


def fetch_container_objects(base_url, token, container):
    objects = {}
    marker = ""

    for _ in range(MAX_PAGES):
        request = urllib.request.Request(container_url(base_url, container, marker))
        if token:
            request.add_header("X-Auth-Token", token)

        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            page = json.loads(response.read().decode("utf-8"))

        if not page:
            break

        for item in page:
            name = item.get("name")
            if not name:
                continue
            objects[name] = {
                "hash": item.get("hash", ""),
                "bytes": int(item.get("bytes", 0)),
                "last_modified": parse_timestamp(item.get("last_modified", "")),
            }

        marker = page[-1].get("name", "")
        if len(page) < 10000 or not marker:
            break

    return objects


def percentile(values, pct):
    if not values:
        return 0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * pct))
    return ordered[index]


def collect_metrics():
    now = time.time()
    lines = [
        "# HELP swift_container_sync_lag_seconds Age of source objects that are missing or mismatched on the replica.",
        "# TYPE swift_container_sync_lag_seconds gauge",
        "# HELP swift_container_sync_unsynced_objects Source objects missing from the replica.",
        "# TYPE swift_container_sync_unsynced_objects gauge",
        "# HELP swift_container_sync_mismatch_objects Objects present on both sides but with different hash or size.",
        "# TYPE swift_container_sync_mismatch_objects gauge",
        "# HELP swift_container_sync_source_objects Objects seen in the source container.",
        "# TYPE swift_container_sync_source_objects gauge",
        "# HELP swift_container_sync_replica_objects Objects seen in the replica container.",
        "# TYPE swift_container_sync_replica_objects gauge",
        "# HELP swift_container_sync_last_scrape_success Whether the latest scrape completed successfully for this container.",
        "# TYPE swift_container_sync_last_scrape_success gauge",
        "# HELP swift_container_sync_last_scrape_timestamp_seconds Unix timestamp of the latest scrape attempt.",
        "# TYPE swift_container_sync_last_scrape_timestamp_seconds gauge",
        "# HELP swift_container_sync_configured Whether required exporter configuration is present.",
        "# TYPE swift_container_sync_configured gauge",
        "# HELP swift_container_sync_containers Number of source containers configured for container sync.",
        "# TYPE swift_container_sync_containers gauge",
        "# HELP swift_container_sync_checked_objects Source objects checked across all synced containers.",
        "# TYPE swift_container_sync_checked_objects gauge",
        "# HELP swift_container_sync_unsynced_ratio Ratio of missing or mismatched source objects across all synced containers.",
        "# TYPE swift_container_sync_unsynced_ratio gauge",
    ]

    configured_pairs = parse_container_pairs(os.getenv("SYNC_CONTAINERS", ""))
    container_pairs = configured_pairs
    source_url, source_token = "", ""
    replica_url, replica_token = "", ""
    try:
        source_url, source_token = swift_connection(
            SOURCE_SWIFT_URL, SOURCE_AUTH_TOKEN, SOURCE_AUTH_URL, SOURCE_AUTH_USER, SOURCE_AUTH_KEY)
        replica_url, replica_token = swift_connection(
            REPLICA_SWIFT_URL, REPLICA_AUTH_TOKEN, REPLICA_AUTH_URL, REPLICA_AUTH_USER, REPLICA_AUTH_KEY)
    except (OSError, urllib.error.URLError, urllib.error.HTTPError):
        pass

    if source_url and source_token and DISCOVER_SYNC_CONTAINERS and not configured_pairs:
        try:
            container_pairs = discover_container_pairs(source_url, source_token)
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, ValueError, json.JSONDecodeError):
            container_pairs = []

    configured = bool(source_url and replica_url and container_pairs)
    lines.append(metric("swift_container_sync_configured", 1 if configured else 0))
    lines.append(metric("swift_container_sync_containers", len(container_pairs)))

    if not configured:
        lines.append(metric("swift_container_sync_lag_seconds", 0, {"container": "config", "quantile": "p95"}))
        lines.append(metric("swift_container_sync_lag_seconds", 0, {"container": "config", "quantile": "max"}))
        lines.append(metric("swift_container_sync_unsynced_objects", 0, {"container": "config"}))
        lines.append(metric("swift_container_sync_mismatch_objects", 0, {"container": "config"}))
        lines.append(metric("swift_container_sync_last_scrape_success", 0, {"container": "config"}))
        lines.append(metric("swift_container_sync_checked_objects", 0))
        lines.append(metric("swift_container_sync_unsynced_ratio", 0))
        lines.append(metric("swift_container_sync_last_scrape_timestamp_seconds", int(now), {"container": "config"}))
        return "\n".join(lines) + "\n"

    all_lag_values = []
    total_source_objects = 0
    total_replica_objects = 0
    total_unsynced = 0
    total_mismatch = 0
    successful_scrapes = 0

    for source_container, replica_container in container_pairs:
        labels = {
            "container": f"{source_container}->{replica_container}",
            "source_container": source_container,
            "replica_container": replica_container,
        }
        try:
            source = fetch_container_objects(source_url, source_token, source_container)
            replica = fetch_container_objects(replica_url, replica_token, replica_container)

            lag_values = []
            unsynced = 0
            mismatch = 0

            for name, source_obj in source.items():
                replica_obj = replica.get(name)
                if replica_obj is None:
                    unsynced += 1
                elif source_obj["hash"] != replica_obj["hash"] or source_obj["bytes"] != replica_obj["bytes"]:
                    mismatch += 1
                else:
                    continue

                if source_obj["last_modified"]:
                    lag_values.append(max(0, now - source_obj["last_modified"]))

            total_source_objects += len(source)
            total_replica_objects += len(replica)
            total_unsynced += unsynced
            total_mismatch += mismatch
            all_lag_values.extend(lag_values)
            successful_scrapes += 1

            lines.append(metric("swift_container_sync_source_objects", len(source), labels))
            lines.append(metric("swift_container_sync_replica_objects", len(replica), labels))
            lines.append(metric("swift_container_sync_unsynced_objects", unsynced, labels))
            lines.append(metric("swift_container_sync_mismatch_objects", mismatch, labels))
            lines.append(metric("swift_container_sync_lag_seconds", round(percentile(lag_values, 0.50), 3), {**labels, "quantile": "p50"}))
            lines.append(metric("swift_container_sync_lag_seconds", round(percentile(lag_values, 0.95), 3), {**labels, "quantile": "p95"}))
            lines.append(metric("swift_container_sync_lag_seconds", round(max(lag_values) if lag_values else 0, 3), {**labels, "quantile": "max"}))
            lines.append(metric("swift_container_sync_last_scrape_success", 1, labels))
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, ValueError, json.JSONDecodeError):
            lines.append(metric("swift_container_sync_last_scrape_success", 0, labels))

        lines.append(metric("swift_container_sync_last_scrape_timestamp_seconds", int(now), labels))

    aggregate_labels = {"scope": "all"}
    aggregate_unsynced = total_unsynced + total_mismatch
    lines.append(metric("swift_container_sync_source_objects", total_source_objects, aggregate_labels))
    lines.append(metric("swift_container_sync_replica_objects", total_replica_objects, aggregate_labels))
    lines.append(metric("swift_container_sync_unsynced_objects", total_unsynced, aggregate_labels))
    lines.append(metric("swift_container_sync_mismatch_objects", total_mismatch, aggregate_labels))
    lines.append(metric("swift_container_sync_checked_objects", total_source_objects))
    lines.append(metric("swift_container_sync_unsynced_ratio", round(aggregate_unsynced / total_source_objects, 6) if total_source_objects else 0))
    lines.append(metric("swift_container_sync_lag_seconds", round(percentile(all_lag_values, 0.50), 3), {**aggregate_labels, "quantile": "p50"}))
    lines.append(metric("swift_container_sync_lag_seconds", round(percentile(all_lag_values, 0.95), 3), {**aggregate_labels, "quantile": "p95"}))
    lines.append(metric("swift_container_sync_lag_seconds", round(max(all_lag_values) if all_lag_values else 0, 3), {**aggregate_labels, "quantile": "max"}))
    lines.append(metric("swift_container_sync_last_scrape_success", 1 if successful_scrapes == len(container_pairs) else 0, aggregate_labels))

    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/metrics"):
            self.send_response(404)
            self.end_headers()
            return

        body = collect_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), MetricsHandler)
    server.serve_forever()
