#!/usr/bin/env python3
import html
import json
import os
import socket
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen


PORT = int(os.getenv("EXPORTER_PORT", "8010"))
RECON_URLS = os.getenv("RECON_URLS", "")
RECON_PATHS = os.getenv("RECON_PATHS", "/var/cache/swift/container.recon")
RECON_NODE_ALIASES = os.getenv("RECON_NODE_ALIASES", "")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "5"))
WEB_REFRESH_SECONDS = int(os.getenv("WEB_REFRESH_SECONDS", "15"))
QUICKWIT_SEARCH_URL = os.getenv("QUICKWIT_SEARCH_URL", "")
QUICKWIT_DEFAULT_MAX_HITS = int(os.getenv("QUICKWIT_DEFAULT_MAX_HITS", "50"))
DEFAULT_RECON_FILE = "container.recon"
LOCAL_HOSTNAME = socket.gethostname()
KST = timezone(timedelta(hours=9))

TOTAL_COUNTERS = {
    "puts": "Successful remote PUT operations triggered by container-sync.",
    "deletes": "Successful remote DELETE operations triggered by container-sync.",
    "bytes": "Object bytes sent by container-sync PUT operations.",
    "row_attempts": "Object rows attempted by container-sync.",
    "row_successes": "Object rows completed successfully by container-sync.",
    "row_failures": "Object rows that failed during container-sync.",
    "remote_head_skips": "Rows skipped because the remote object was already current.",
}

DAEMON_GAUGES = {
    "last_run_timestamp": "Unix timestamp when the latest container-sync scan started.",
    "last_run_finished_timestamp": "Unix timestamp when the latest container-sync scan finished.",
    "last_run_duration_seconds": "Duration of the latest container-sync scan.",
    "scanned_containers": "Containers scanned during the latest run.",
    "synced_containers": "Containers successfully synced during the latest run.",
    "skipped_containers": "Containers skipped during the latest run.",
    "failed_containers": "Containers that failed during the latest run.",
    "time_exhausted_containers": "Containers whose sync pass reached the container_time limit.",
    "new_backlog_rows": "Total rows newer than sync point 1 in the latest run.",
    "retry_backlog_rows": "Total rows between sync point 2 and sync point 1 in the latest run.",
    "max_new_backlog_rows": "Largest new-row backlog seen on one container in the latest run.",
    "max_retry_backlog_rows": "Largest retry backlog seen on one container in the latest run.",
}


def recon_paths():
    if RECON_URLS.strip():
        return []
    paths = []
    for raw in RECON_PATHS.split(","):
        item = raw.strip()
        if not item:
            continue
        if os.path.isdir(item):
            item = os.path.join(item, DEFAULT_RECON_FILE)
        paths.append(item)
    return paths or [os.path.join("/var/cache/swift", DEFAULT_RECON_FILE)]


def recon_urls():
    return [raw.strip() for raw in RECON_URLS.split(",") if raw.strip()]


def node_from_path(path):
    parent = os.path.basename(os.path.dirname(path.rstrip(os.sep)))
    return parent or path


def node_aliases():
    aliases = {}
    for raw in RECON_NODE_ALIASES.split(","):
        raw = raw.strip()
        if not raw or "=" not in raw:
            continue
        source, name = raw.split("=", 1)
        aliases[source.strip()] = name.strip()
    return aliases


NODE_ALIASES = node_aliases()


def node_name(name):
    return NODE_ALIASES.get(name, name)


def is_local_node_name(name):
    return name in ("127.0.0.1", "localhost", "::1")


def node_from_url(url):
    parsed = urlparse(url)
    host = parsed.hostname or url
    if is_local_node_name(host):
        return node_name(LOCAL_HOSTNAME)
    return node_name(host)


def normalize_container_sync(parsed):
    legacy = parsed.get("container_sync")
    if isinstance(legacy, dict):
        legacy.setdefault("containers", legacy.get("container_sync_containers", {}) or {})
        return legacy

    has_standard_keys = any(key.startswith("container_sync_")
                            for key in parsed)
    if not has_standard_keys:
        return None

    return {
        "timestamp": parsed.get("container_sync_last", 0),
        "hostname": parsed.get("container_sync_hostname", ""),
        "daemon": parsed.get("container_sync_daemon", {}) or {},
        "totals": parsed.get("container_sync_stats", {}) or {},
        "containers": parsed.get("container_sync_containers", {}) or {},
    }


def empty_state(source, node, error="not_read"):
    return {
        "path": source,
        "node": node,
        "up": 0,
        "error": error,
        "container_sync": {},
        "read_timestamp": time.time(),
    }


def state_from_recon(parsed, source, node):
    result = empty_state(source, node, "invalid_recon")

    if not isinstance(parsed, dict):
        return result

    if "up" in parsed and "container_sync" in parsed:
        state = dict(parsed)
        state.setdefault("path", source)
        state.setdefault("node", node)
        state.setdefault("error", "" if state.get("up") else "not_read")
        state.setdefault("read_timestamp", time.time())
        container_sync = normalize_container_sync(state)
        if isinstance(container_sync, dict):
            state["container_sync"] = container_sync
            hostname = container_sync.get("hostname")
            if hostname:
                state["node"] = node_name(hostname)
            elif is_local_node_name(state.get("node")):
                state["node"] = node if not is_local_node_name(node) else node_name(LOCAL_HOSTNAME)
            else:
                state["node"] = node_name(state["node"])
        return state

    container_sync = normalize_container_sync(parsed)
    if not isinstance(container_sync, dict):
        result["error"] = "missing_container_sync"
        return result

    result["container_sync"] = container_sync
    result["node"] = node_name(container_sync.get("hostname") or result["node"])
    result["up"] = 1
    result["error"] = ""
    return result


def read_recon(path):
    result = empty_state(path, node_from_path(path))
    try:
        with open(path, "r") as fp:
            parsed = json.load(fp)
    except FileNotFoundError:
        result["error"] = "not_found"
        return result
    except json.JSONDecodeError:
        result["error"] = "invalid_json"
        return result
    except OSError:
        result["error"] = "read_error"
        return result

    return state_from_recon(parsed, path, result["node"])


def read_recon_url(url):
    result = empty_state(url, node_from_url(url))
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            body = response.read()
        parsed = json.loads(body.decode("utf-8"))
    except HTTPError as err:
        result["error"] = "http_%s" % err.code
        return [result]
    except URLError:
        result["error"] = "url_error"
        return [result]
    except TimeoutError:
        result["error"] = "timeout"
        return [result]
    except json.JSONDecodeError:
        result["error"] = "invalid_json"
        return [result]
    except OSError:
        result["error"] = "read_error"
        return [result]

    if isinstance(parsed, list):
        states = []
        for index, item in enumerate(parsed):
            item_source = "%s#%s" % (url, index)
            states.append(state_from_recon(item, item_source, result["node"]))
        return states or [result]

    return [state_from_recon(parsed, url, result["node"])]


def collect_state():
    states = []
    for url in recon_urls():
        states.extend(read_recon_url(url))
    for path in recon_paths():
        states.append(read_recon(path))
    return states


def escape_label(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def labels_text(labels):
    if not labels:
        return ""
    pairs = ['%s="%s"' % (key, escape_label(value))
             for key, value in sorted(labels.items())]
    return "{" + ",".join(pairs) + "}"


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def metric(name, value, labels=None):
    return "%s%s %s" % (name, labels_text(labels or {}), number(value))


def metric_suffix(key):
    if key.endswith("_timestamp"):
        return "%s_seconds" % key
    return key


def collect_metrics():
    lines = []
    metric_defs = {
        "swift_container_sync_recon_up": ("gauge", "Whether the recon file was read successfully."),
        "swift_container_sync_recon_read_timestamp_seconds": ("gauge", "Unix timestamp of the exporter read attempt."),
        "swift_container_sync_recon_last_update_timestamp_seconds": ("gauge", "Unix timestamp written by container-sync recon."),
    }
    for key, help_text in DAEMON_GAUGES.items():
        metric_defs["swift_container_sync_recon_daemon_%s" % metric_suffix(key)] = ("gauge", help_text)
    for key, help_text in TOTAL_COUNTERS.items():
        metric_defs["swift_container_sync_recon_%s_total" % key] = ("counter", help_text)
    metric_defs.update({
        "swift_container_sync_recon_container_replication_ratio": ("gauge", "Container sync verified progress, using sync_point2 divided by max_row."),
        "swift_container_sync_recon_container_scan_ratio": ("gauge", "Container sync scan progress, using sync_point1 divided by max_row."),
        "swift_container_sync_recon_container_sync_point1_rows": ("gauge", "Container sync point 1 row value."),
        "swift_container_sync_recon_container_sync_point2_rows": ("gauge", "Container sync point 2 row value."),
        "swift_container_sync_recon_container_max_row_rows": ("gauge", "Largest row id observed for the container database."),
        "swift_container_sync_recon_container_object_count": ("gauge", "Object count reported by the container database."),
        "swift_container_sync_recon_container_new_backlog_rows": ("gauge", "Rows newer than sync point 1 for the container."),
        "swift_container_sync_recon_container_retry_backlog_rows": ("gauge", "Rows between sync point 2 and sync point 1 for the container."),
        "swift_container_sync_recon_container_status": ("gauge", "Latest container-sync status for a container, labeled by status."),
    })

    for name, (metric_type, help_text) in sorted(metric_defs.items()):
        lines.append("# HELP %s %s" % (name, help_text))
        lines.append("# TYPE %s %s" % (name, metric_type))

    for state in collect_state():
        base_labels = {
            "node": state["node"],
            "path": state["path"],
            "error": state["error"],
        }
        lines.append(metric("swift_container_sync_recon_up", state["up"], base_labels))
        lines.append(metric("swift_container_sync_recon_read_timestamp_seconds",
                            state["read_timestamp"], base_labels))
        if not state["up"]:
            continue

        recon = state["container_sync"]
        node_labels = {"node": state["node"], "path": state["path"]}
        lines.append(metric("swift_container_sync_recon_last_update_timestamp_seconds",
                            recon.get("timestamp", 0), node_labels))

        daemon = recon.get("daemon", {}) or {}
        for key in DAEMON_GAUGES:
            lines.append(metric("swift_container_sync_recon_daemon_%s" % metric_suffix(key),
                                daemon.get(key, 0), node_labels))

        totals = recon.get("totals", {}) or {}
        for key in TOTAL_COUNTERS:
            lines.append(metric("swift_container_sync_recon_%s_total" % key,
                                totals.get(key, 0), node_labels))

        containers = recon.get("containers", {}) or {}
        if isinstance(containers, list):
            iterator = enumerate(containers)
        elif isinstance(containers, dict):
            iterator = containers.items()
        else:
            iterator = []
        for raw_key, item in iterator:
            if not isinstance(item, dict):
                continue
            account, container = container_identity(raw_key, item)
            if not account or not container:
                continue
            status = str(item.get("last_status") or item.get("status") or "unknown")
            labels = dict(node_labels)
            labels.update({
                "account": account,
                "container": container,
                "status": status,
            })
            max_row = number(item.get("max_row", 0))
            sync_point1 = number(item.get("sync_point1", 0))
            sync_point2 = number(item.get("sync_point2", 0))
            replication = 1.0 if max_row <= 0 else min(1.0, max(0.0, sync_point2 / max_row))
            scan = 1.0 if max_row <= 0 else min(1.0, max(0.0, sync_point1 / max_row))
            lines.append(metric("swift_container_sync_recon_container_replication_ratio", replication, labels))
            lines.append(metric("swift_container_sync_recon_container_scan_ratio", scan, labels))
            lines.append(metric("swift_container_sync_recon_container_sync_point1_rows", sync_point1, labels))
            lines.append(metric("swift_container_sync_recon_container_sync_point2_rows", sync_point2, labels))
            lines.append(metric("swift_container_sync_recon_container_max_row_rows", max_row, labels))
            lines.append(metric("swift_container_sync_recon_container_object_count", item.get("object_count", 0), labels))
            lines.append(metric("swift_container_sync_recon_container_new_backlog_rows", item.get("new_backlog_rows", 0), labels))
            lines.append(metric("swift_container_sync_recon_container_retry_backlog_rows", item.get("retry_backlog_rows", 0), labels))
            lines.append(metric("swift_container_sync_recon_container_status", 1, labels))

    return "\n".join(lines) + "\n"


def fmt_int(value):
    return "{:,}".format(int(number(value)))


def age_text(timestamp):
    timestamp = number(timestamp)
    if timestamp <= 0:
        return "never"
    seconds = max(0, int(time.time() - timestamp))
    if seconds < 60:
        return "%ss" % seconds
    minutes = seconds // 60
    if minutes < 60:
        return "%sm" % minutes
    hours = minutes // 60
    if hours < 48:
        return "%sh" % hours
    return "%sd" % (hours // 24)


def timestamp_text(value):
    if isinstance(value, (int, float)):
        return age_text(value)
    value = str(value or "")
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST")


def h(value):
    return html.escape(str(value), quote=True)


def param_value(params, key, default=""):
    values = params.get(key, [default])
    if not values:
        return default
    return values[0].strip()


def quote_query_value(value):
    value = str(value).strip()
    if not value:
        return '""'
    escaped = value.replace('\\', '\\\\').replace('"', '\\"')
    return '"%s"' % escaped


def bounded_max_hits(value):
    try:
        max_hits = int(value)
    except (TypeError, ValueError):
        max_hits = QUICKWIT_DEFAULT_MAX_HITS
    return max(1, min(max_hits, 200))


def build_object_log_query(params):
    raw_query = param_value(params, "q")
    if raw_query:
        return raw_query

    terms = []
    for field in ("account", "container", "object", "method", "outcome",
                  "reason", "host", "site"):
        value = param_value(params, field)
        if value:
            terms.append("%s:%s" % (field, quote_query_value(value)))

    path_value = param_value(params, "path")
    if path_value:
        terms.append("path:%s" % quote_query_value(path_value))

    return " AND ".join(terms) if terms else "*"


def quickwit_search(params):
    query = build_object_log_query(params)
    max_hits = bounded_max_hits(param_value(params, "max_hits", str(QUICKWIT_DEFAULT_MAX_HITS)))
    result = {
        "enabled": bool(QUICKWIT_SEARCH_URL),
        "query": query,
        "max_hits": max_hits,
        "num_hits": 0,
        "hits": [],
        "errors": [],
        "elapsed_time_micros": 0,
        "url": QUICKWIT_SEARCH_URL,
    }
    if not QUICKWIT_SEARCH_URL:
        result["errors"].append("quickwit_search_url_not_configured")
        return result

    separator = "&" if "?" in QUICKWIT_SEARCH_URL else "?"
    url = QUICKWIT_SEARCH_URL + separator + urlencode({
        "query": query,
        "max_hits": max_hits,
    })
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            body = response.read()
        parsed = json.loads(body.decode("utf-8"))
    except HTTPError as err:
        result["errors"].append("http_%s" % err.code)
        return result
    except URLError:
        result["errors"].append("url_error")
        return result
    except TimeoutError:
        result["errors"].append("timeout")
        return result
    except json.JSONDecodeError:
        result["errors"].append("invalid_json")
        return result
    except OSError:
        result["errors"].append("read_error")
        return result

    result["num_hits"] = parsed.get("num_hits", 0)
    result["hits"] = parsed.get("hits", []) or []
    result["errors"] = parsed.get("errors", []) or []
    result["elapsed_time_micros"] = parsed.get("elapsed_time_micros", 0)
    return result


def quickwit_query(query, max_hits=1000):
    result = {
        "enabled": bool(QUICKWIT_SEARCH_URL),
        "query": query,
        "max_hits": max_hits,
        "num_hits": 0,
        "hits": [],
        "errors": [],
        "elapsed_time_micros": 0,
        "url": QUICKWIT_SEARCH_URL,
    }
    if not QUICKWIT_SEARCH_URL:
        result["errors"].append("quickwit_search_url_not_configured")
        return result

    separator = "&" if "?" in QUICKWIT_SEARCH_URL else "?"
    url = QUICKWIT_SEARCH_URL + separator + urlencode({
        "query": query,
        "max_hits": max(1, min(int(max_hits), 5000)),
    })
    try:
        request = Request(url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            body = response.read()
        parsed = json.loads(body.decode("utf-8"))
    except HTTPError as err:
        result["errors"].append("http_%s" % err.code)
        return result
    except URLError:
        result["errors"].append("url_error")
        return result
    except TimeoutError:
        result["errors"].append("timeout")
        return result
    except json.JSONDecodeError:
        result["errors"].append("invalid_json")
        return result
    except OSError:
        result["errors"].append("read_error")
        return result

    result["num_hits"] = parsed.get("num_hits", 0)
    result["hits"] = parsed.get("hits", []) or []
    result["errors"] = parsed.get("errors", []) or []
    result["elapsed_time_micros"] = parsed.get("elapsed_time_micros", 0)
    return result



def object_query_terms(account="", container="", outcome=""):
    terms = ['event_type:"container_sync_object"']
    if account:
        terms.append("account:%s" % quote_query_value(account))
    if container:
        terms.append("container:%s" % quote_query_value(container))
    if outcome:
        terms.append("outcome:%s" % quote_query_value(outcome))
    return " AND ".join(terms)


def container_event_query_terms(account="", container=""):
    terms = ['event_type:"container_sync_container"']
    if account:
        terms.append("account:%s" % quote_query_value(account))
    if container:
        terms.append("container:%s" % quote_query_value(container))
    return " AND ".join(terms)


def hit_key(hit):
    return "%s/%s" % (hit.get("account", ""), hit.get("container", ""))


def timestamp_bucket(value):
    value = str(value or "")
    return value[:16].replace("T", " ") if len(value) >= 16 else value


def latest_container_rows(container_hits, object_hits):
    latest = {}
    for hit in container_hits:
        key = hit_key(hit)
        if not key.strip("/"):
            continue
        if key not in latest or str(hit.get("timestamp", "")) > str(latest[key].get("timestamp", "")):
            latest[key] = hit

    object_counts = {}
    failure_counts = {}
    for hit in object_hits:
        key = hit_key(hit)
        if not key.strip("/"):
            continue
        object_counts[key] = object_counts.get(key, 0) + 1
        if hit.get("outcome") == "failure":
            failure_counts[key] = failure_counts.get(key, 0) + 1
        if key not in latest or str(hit.get("timestamp", "")) > str(latest[key].get("timestamp", "")):
            latest[key] = hit

    rows = []
    for key, hit in latest.items():
        total = object_counts.get(key, 0)
        failures = failure_counts.get(key, 0)
        error_rate = (float(failures) / float(total)) if total else 0.0
        rows.append((key, hit, total, failures, error_rate))
    rows.sort(key=lambda item: (item[4], item[1].get("timestamp", "")), reverse=True)
    return rows


def object_log_summary(hits):
    summary = {}
    for hit in hits:
        key = hit_key(hit)
        if not key.strip("/"):
            continue
        item = summary.setdefault(key, {
            "events": 0,
            "failures": 0,
            "last_event": {},
        })
        item["events"] += 1
        if hit.get("outcome") == "failure":
            item["failures"] += 1
        if str(hit.get("timestamp", "")) > str(item["last_event"].get("timestamp", "")):
            item["last_event"] = hit
    return summary


def container_filters_match(account, container, wanted_account, wanted_container):
    if wanted_account and account != wanted_account:
        return False
    if wanted_container and container != wanted_container:
        return False
    return True


def container_identity(raw_key, item):
    account = str(item.get("account", "") or "")
    container = str(item.get("container", "") or "")
    if (not account or not container) and "/" in str(raw_key):
        key_account, key_container = str(raw_key).split("/", 1)
        account = account or key_account
        container = container or key_container
    return account, container


def collect_recon_container_rows(account_filter="", container_filter=""):
    grouped = {}
    for state in collect_state():
        if not state.get("up"):
            continue
        node = state.get("node") or "unknown"
        recon = state.get("container_sync", {}) or {}
        containers = recon.get("containers", {}) or {}
        if isinstance(containers, list):
            iterator = enumerate(containers)
        elif isinstance(containers, dict):
            iterator = containers.items()
        else:
            continue

        for raw_key, item in iterator:
            if not isinstance(item, dict):
                continue
            account, container = container_identity(raw_key, item)
            if not account or not container:
                continue
            if not container_filters_match(account, container,
                                           account_filter, container_filter):
                continue
            key = "%s/%s" % (account, container)
            row = grouped.setdefault(key, {
                "key": key,
                "account": account,
                "container": container,
                "nodes": set(),
                "statuses": set(),
                "sync_point1_values": [],
                "sync_point2_values": [],
                "max_row": 0,
                "object_count": 0,
                "new_backlog_rows": 0,
                "retry_backlog_rows": 0,
                "time_exhausted": 0,
                "updated": 0,
                "last_status": "",
                "last_reason": "",
            })
            row["nodes"].add(node)
            status = str(item.get("last_status") or item.get("status") or "")
            if status:
                row["statuses"].add(status)
            row["sync_point1_values"].append(number(item.get("sync_point1", 0)))
            row["sync_point2_values"].append(number(item.get("sync_point2", 0)))
            row["max_row"] = max(row["max_row"], number(item.get("max_row", 0)))
            row["object_count"] = max(row["object_count"], number(item.get("object_count", 0)))
            row["new_backlog_rows"] += number(item.get("new_backlog_rows", 0))
            row["retry_backlog_rows"] += number(item.get("retry_backlog_rows", 0))
            row["time_exhausted"] += number(item.get("time_exhausted", 0))
            updated = number(item.get("updated", 0))
            if updated >= row["updated"]:
                row["updated"] = updated
                row["last_status"] = status
                row["last_reason"] = str(item.get("last_reason") or status)

    rows = []
    for row in grouped.values():
        max_row = row["max_row"]
        point1 = min(row["sync_point1_values"]) if row["sync_point1_values"] else 0
        point2 = min(row["sync_point2_values"]) if row["sync_point2_values"] else 0
        row["sync_point1"] = max(0, point1)
        row["sync_point2"] = max(0, point2)
        row["replication_rate"] = 1.0 if max_row <= 0 else min(1.0, max(0.0, point2 / max_row))
        row["scan_rate"] = 1.0 if max_row <= 0 else min(1.0, max(0.0, point1 / max_row))
        row["nodes"] = sorted(row["nodes"])
        row["statuses"] = sorted(row["statuses"])
        rows.append(row)
    rows.sort(key=lambda row: (row.get("replication_rate", 0), -row.get("retry_backlog_rows", 0), row["key"]))
    return rows


def percent_text(value):
    return "%.1f%%" % (number(value) * 100.0)


def progress_class(value):
    value = number(value)
    if value >= 0.999:
        return "ok"
    if value >= 0.95:
        return "warn"
    return "bad"


def error_class(value):
    value = number(value)
    if value >= 0.2:
        return "bad"
    if value > 0:
        return "warn"
    return "ok"


def build_method_series(hits):
    buckets = {}
    for hit in hits:
        bucket = timestamp_bucket(hit.get("timestamp"))
        if not bucket:
            continue
        method = hit.get("method", "")
        buckets.setdefault(bucket, {"PUT": 0, "DELETE": 0})
        if method in ("PUT", "DELETE"):
            buckets[bucket][method] += 1
    return [(bucket, values.get("PUT", 0), values.get("DELETE", 0))
            for bucket, values in sorted(buckets.items())[-24:]]


def build_count_series(hits):
    buckets = {}
    for hit in hits:
        bucket = timestamp_bucket(hit.get("timestamp"))
        if not bucket:
            continue
        buckets.setdefault(bucket, {"max_row": 0, "objects": 0})
        buckets[bucket]["max_row"] = max(
            buckets[bucket]["max_row"], number(hit.get("max_row", 0)))
        buckets[bucket]["objects"] = max(
            buckets[bucket]["objects"], number(hit.get("object_count", 0)))
    return [(bucket, values["max_row"], values["objects"])
            for bucket, values in sorted(buckets.items())[-24:]]


def sparkline(points, labels, colors):
    width = 640
    height = 180
    pad = 28
    if not points:
        return '<div class="empty">No time series data</div>'
    max_value = max([1] + [max(values) for _, values in points])
    step = (width - pad * 2) / float(max(1, len(points) - 1))
    polylines = []
    for idx, label in enumerate(labels):
        coords = []
        for pos, (_, values) in enumerate(points):
            x = pad + pos * step
            y = height - pad - ((height - pad * 2) * (float(values[idx]) / float(max_value)))
            coords.append("%.1f,%.1f" % (x, y))
        polylines.append('<polyline fill="none" stroke="%s" stroke-width="2.5" points="%s" />' % (colors[idx], " ".join(coords)))
        if len(coords) == 1:
            x, y = coords[0].split(",")
            polylines.append('<circle cx="%s" cy="%s" r="4" fill="%s" />' % (x, y, colors[idx]))
    legend = " ".join('<span><i style="background:%s"></i>%s</span>' % (colors[i], h(labels[i])) for i in range(len(labels)))
    return '<div class="chart"><svg viewBox="0 0 %s %s" role="img"><line x1="%s" y1="%s" x2="%s" y2="%s" stroke="#d8dee8"/>%s</svg><div class="legend">%s</div></div>' % (
        width, height, pad, height - pad, width - pad, height - pad, "".join(polylines), legend)


def render_containers(params):
    account = param_value(params, "account")
    container = param_value(params, "container")
    try:
        max_hits = int(param_value(params, "max_hits", "1000") or 1000)
    except ValueError:
        max_hits = 1000
    max_hits = max(100, min(max_hits, 5000))

    object_result = quickwit_query(object_query_terms(account, container), max_hits=max_hits)
    container_result = quickwit_query(container_event_query_terms(account, container), max_hits=max_hits)
    failure_result = quickwit_query(object_query_terms(account, container, "failure"), max_hits=100)
    log_summary = object_log_summary(object_result.get("hits", []))
    recon_rows = collect_recon_container_rows(account, container)

    rows = []
    if recon_rows:
        for row in recon_rows:
            key = row["key"]
            logs = log_summary.get(key, {"events": 0, "failures": 0, "last_event": {}})
            total_events = logs.get("events", 0)
            failures = logs.get("failures", 0)
            error_rate = (float(failures) / float(total_events)) if total_events else 0.0
            detail_url = "/logs?" + urlencode({
                "account": row["account"],
                "container": row["container"],
                "outcome": "failure",
                "max_hits": "100",
            })
            total_objects = row.get("object_count") or row.get("max_row") or 0
            last_event = logs.get("last_event", {}) or {}
            last_seen = last_event.get("timestamp") or row.get("updated", 0)
            if isinstance(last_seen, (int, float)):
                last_seen = age_text(last_seen)
            detail_text = "failures" if failures else "history"
            rows.append('''<tr><td>%s</td><td>%s</td><td><span class="pill %s">%s</span></td><td class="num">%s</td><td class="num">%s</td><td class="num">%s</td><td class="num">%s/%s</td><td><span class="pill %s">%.1f%%</span></td><td>%s</td><td>%s</td><td><a href="%s">%s</a></td></tr>''' % (
                h(row["account"]), h(row["container"]),
                progress_class(row.get("replication_rate", 0)),
                h(percent_text(row.get("replication_rate", 0))),
                h(fmt_int(total_objects)),
                h(fmt_int(row.get("new_backlog_rows", 0))),
                h(fmt_int(row.get("retry_backlog_rows", 0))),
                h(failures), h(total_events),
                error_class(error_rate), error_rate * 100.0,
                h(", ".join(row.get("nodes", []))), h(timestamp_text(last_seen)),
                h(detail_url), h(detail_text)))
    else:
        for key, hit, total, failures, error_rate in latest_container_rows(
                container_result.get("hits", []), object_result.get("hits", [])):
            account_value = hit.get("account", "")
            container_value = hit.get("container", "")
            detail_url = "/logs?" + urlencode({
                "account": account_value,
                "container": container_value,
                "outcome": "failure",
                "max_hits": "100",
            })
            total_objects = hit.get("object_count") or hit.get("max_row") or total
            replication_rate = number(hit.get("replication_rate", 0))
            rows.append('''<tr><td>%s</td><td>%s</td><td><span class="pill %s">%s</span></td><td class="num">%s</td><td class="num">%s</td><td class="num">%s</td><td class="num">%s/%s</td><td><span class="pill %s">%.1f%%</span></td><td>%s</td><td>%s</td><td><a href="%s">failures</a></td></tr>''' % (
                h(account_value), h(container_value),
                progress_class(replication_rate), h(percent_text(replication_rate)),
                h(fmt_int(total_objects)), "n/a", "n/a",
                h(failures), h(total), error_class(error_rate),
                error_rate * 100.0, h(hit.get("host", "")),
                h(timestamp_text(hit.get("timestamp", ""))), h(detail_url)))
    table_body = "\n".join(rows) if rows else '<tr><td colspan="11" class="empty">No recon or object log data for matching containers</td></tr>'

    method_points = [(bucket, (puts, deletes)) for bucket, puts, deletes in build_method_series(object_result.get("hits", []))]
    row_points = [(bucket, (max_row, objects)) for bucket, max_row, objects in build_count_series(container_result.get("hits", []))]
    if not row_points:
        row_points = [(bucket, (events, objects)) for bucket, events, objects in build_count_series(object_result.get("hits", []))]

    node_totals = {}
    node_failures = {}
    for hit in object_result.get("hits", []):
        node = hit.get("host") or hit.get("site") or "unknown"
        node_totals[node] = node_totals.get(node, 0) + 1
        if hit.get("outcome") == "failure":
            node_failures[node] = node_failures.get(node, 0) + 1
    node_rows = []
    for node, total in sorted(node_totals.items()):
        failures = node_failures.get(node, 0)
        error_rate = (float(failures) / float(total)) if total else 0.0
        node_rows.append('''<tr><td>%s</td><td class="num">%s</td><td class="num">%s</td><td><span class="pill %s">%.1f%%</span></td></tr>''' % (
            h(node), h(failures), h(total), error_class(error_rate),
            error_rate * 100.0))
    node_body = "\n".join(node_rows) if node_rows else '<tr><td colspan="4" class="empty">No node error data</td></tr>'

    failure_rows = []
    for hit in failure_result.get("hits", [])[:50]:
        failure_rows.append('''<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td class="mono">%s</td></tr>''' % (
            h(timestamp_text(hit.get("timestamp", ""))), h(hit.get("host", "")), h(hit.get("account", "")),
            h(hit.get("container", "")), h(hit.get("reason", "")), h(hit.get("object", ""))))
    failure_body = "\n".join(failure_rows) if failure_rows else '<tr><td colspan="6" class="empty">No failures in recent object logs</td></tr>'
    errors = object_result.get("errors", []) + container_result.get("errors", []) + failure_result.get("errors", [])
    error_html = '<span class="error">%s</span>' % h(", ".join(errors)) if errors else ""

    return '''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Container Sync Containers</title>
<style>
:root { color-scheme: light; --bg: #f6f8fb; --ink: #172033; --muted: #667085; --line: #d8dee8; --panel: #ffffff; --ok: #087443; --bad: #b42318; --warn: #b54708; --blue: #155eef; }
* { box-sizing: border-box; } body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
header { padding: 24px 32px 18px; border-bottom: 1px solid var(--line); background: #fff; display: flex; justify-content: space-between; align-items: end; gap: 16px; }
h1 { margin: 0; font-size: 28px; font-weight: 720; letter-spacing: 0; } h2 { margin: 0 0 12px; font-size: 16px; }
nav { display: flex; gap: 14px; flex-wrap: wrap; } nav a, a { color: var(--blue); text-decoration: none; font-weight: 650; } nav a.active { color: var(--ink); }
main { padding: 24px 32px 36px; max-width: 1680px; margin: 0 auto; } .panel, .table-wrap { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }
.panel { padding: 16px; margin-bottom: 16px; } .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
form { display: grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap: 12px; align-items: end; } label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 650; }
input, button { height: 36px; border: 1px solid var(--line); border-radius: 6px; padding: 0 10px; font: inherit; background: #fff; color: var(--ink); } button { background: var(--blue); color: #fff; border-color: var(--blue); font-weight: 700; cursor: pointer; }
.meta { margin-top: 12px; color: var(--muted); font-size: 13px; display: flex; gap: 16px; flex-wrap: wrap; } .error { color: var(--bad); font-weight: 650; }
.table-wrap { overflow: auto; margin-bottom: 16px; } table { width: 100%%; border-collapse: collapse; min-width: 1260px; } th, td { padding: 11px 12px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: middle; }
th { position: sticky; top: 0; background: #fbfcfe; color: #475467; font-weight: 680; } td.num { text-align: right; font-variant-numeric: tabular-nums; } .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow-wrap: anywhere; }
.pill { display: inline-flex; align-items: center; height: 24px; padding: 0 9px; border-radius: 999px; font-weight: 680; font-size: 12px; } .pill.ok { color: var(--ok); background: #dcfae6; } .pill.bad { color: var(--bad); background: #fee4e2; } .pill.warn { color: var(--warn); background: #fef0c7; }
.empty { text-align: center; color: var(--muted); padding: 28px; } .chart svg { width: 100%%; height: 180px; display: block; } .legend { display: flex; gap: 14px; color: var(--muted); font-size: 12px; } .legend i { display: inline-block; width: 10px; height: 10px; margin-right: 5px; border-radius: 2px; }
@media (max-width: 1100px) { .grid, form { grid-template-columns: 1fr; } header, main { padding-left: 16px; padding-right: 16px; } }
</style></head><body><header><div><h1>Container Sync Containers</h1></div><nav>%s</nav></header><main>
<section class="panel"><form method="get" action="/containers"><label>Account<input name="account" value="%s"></label><label>Container<input name="container" value="%s"></label><label>Max events<input name="max_hits" type="number" min="100" max="5000" value="%s"></label><button type="submit">Apply</button></form><div class="meta"><span>recon containers: <strong>%s</strong></span><span>container events: <strong>%s</strong></span><span>object events: <strong>%s</strong></span>%s</div></section>
<section class="grid"><div class="panel"><h2>PUT / DELETE Trend</h2>%s</div><div class="panel"><h2>Container Row / Object Trend</h2>%s</div></section>
<section class="table-wrap"><table><thead><tr><th>Account</th><th>Container</th><th>Replication</th><th>Total objects/rows</th><th>New backlog</th><th>Retry backlog</th><th>Failures</th><th>Error rate</th><th>Nodes</th><th>Last seen</th><th>Details</th></tr></thead><tbody>%s</tbody></table></section>
<section class="table-wrap"><table><thead><tr><th>Node</th><th>Failures</th><th>Events</th><th>Error rate</th></tr></thead><tbody>%s</tbody></table></section>
<section class="table-wrap"><table><thead><tr><th>Timestamp</th><th>Host</th><th>Account</th><th>Container</th><th>Reason</th><th>Object</th></tr></thead><tbody>%s</tbody></table></section>
</main></body></html>''' % (
        nav_html("containers"), h(account), h(container), h(max_hits),
        h(len(recon_rows)), h(container_result.get("num_hits", 0)),
        h(object_result.get("num_hits", 0)), error_html,
        sparkline(method_points, ["PUT", "DELETE"], ["#155eef", "#b54708"]),
        sparkline(row_points, ["Max row", "Objects"], ["#155eef", "#087443"]),
        table_body, node_body, failure_body)


def ms_text(value):
    try:
        return "%.1f" % float(value)
    except (TypeError, ValueError):
        return "0.0"


def option_html(options, selected):
    items = []
    for option in options:
        attr = ' selected' if option == selected else ''
        items.append('<option value="%s"%s>%s</option>' % (h(option), attr, h(option)))
    return ''.join(items)


def nav_html(active):
    links = [
        ("/containers", "Containers", "containers"),
        ("/logs", "Object History", "logs"),
    ]
    return ''.join(
        '<a class="%s" href="%s">%s</a>' % (
            'active' if key == active else '', href, h(label))
        for href, label, key in links)




def render_object_logs(params):
    result = quickwit_search(params)
    fields = ["q", "account", "container", "object", "method", "outcome",
              "reason", "host", "site", "path", "max_hits"]
    values = {field: param_value(params, field) for field in fields}
    values["max_hits"] = str(result["max_hits"])

    rows = []
    for hit in result.get("hits", []):
        rows.append('''
          <tr>
            <td>%s</td>
            <td>%s</td>
            <td>%s</td>
            <td>%s</td>
            <td>%s</td>
            <td><span class="pill %s">%s</span></td>
            <td>%s</td>
            <td>%s</td>
            <td class="num">%s</td>
            <td class="mono">%s</td>
          </tr>''' % (
            h(timestamp_text(hit.get("timestamp", ""))),
            h(hit.get("site", "")),
            h(hit.get("host", "")),
            h(hit.get("account", "")),
            h(hit.get("container", "")),
            status_class(hit.get("outcome", "")),
            h(hit.get("outcome", "")),
            h(hit.get("method", "")),
            h(hit.get("reason", "")),
            h(ms_text(hit.get("duration_ms", 0))),
            h(hit.get("object", ""))))
    table_body = "\n".join(rows) if rows else '<tr><td colspan="10" class="empty">No object log data</td></tr>'
    error_text = ", ".join(str(err) for err in result.get("errors", []))
    if not error_text and not result.get("enabled"):
        error_text = "quickwit_search_url_not_configured"

    body = '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Container Sync Object Logs</title>
  <style>
    :root { color-scheme: light; --bg: #f6f8fb; --ink: #172033; --muted: #667085; --line: #d8dee8; --panel: #ffffff; --ok: #087443; --bad: #b42318; --warn: #b54708; --blue: #155eef; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header { padding: 24px 32px 18px; border-bottom: 1px solid var(--line); background: #fff; display: flex; justify-content: space-between; align-items: end; gap: 16px; }
    h1 { margin: 0; font-size: 28px; font-weight: 720; letter-spacing: 0; }
    nav a { color: var(--blue); text-decoration: none; font-weight: 650; font-size: 14px; }
    main { padding: 24px 32px 36px; max-width: 1680px; margin: 0 auto; }
    .panel, .table-wrap { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }
    .panel { padding: 16px; margin-bottom: 16px; }
    form { display: grid; grid-template-columns: repeat(6, minmax(120px, 1fr)); gap: 12px; align-items: end; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 650; }
    input, select, button { height: 36px; border: 1px solid var(--line); border-radius: 6px; padding: 0 10px; font: inherit; background: #fff; color: var(--ink); }
    input[name="q"] { grid-column: span 3; }
    button { background: var(--blue); color: #fff; border-color: var(--blue); font-weight: 700; cursor: pointer; }
    .meta { margin-top: 12px; color: var(--muted); font-size: 13px; display: flex; gap: 16px; flex-wrap: wrap; }
    .error { color: var(--bad); font-weight: 650; }
    .table-wrap { overflow: auto; }
    table { width: 100%%; border-collapse: collapse; min-width: 1280px; }
    th, td { padding: 11px 12px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: middle; }
    th { position: sticky; top: 0; background: #fbfcfe; color: #475467; font-weight: 680; }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow-wrap: anywhere; }
    .pill { display: inline-flex; align-items: center; height: 24px; padding: 0 9px; border-radius: 999px; font-weight: 680; font-size: 12px; }
    .pill.ok { color: var(--ok); background: #dcfae6; }
    .pill.bad { color: var(--bad); background: #fee4e2; }
    .pill.warn { color: var(--warn); background: #fef0c7; }
    .pill.muted { color: var(--muted); background: #eef2f6; }
    .empty { text-align: center; color: var(--muted); padding: 28px; }
    @media (max-width: 1100px) { form { grid-template-columns: repeat(2, minmax(0, 1fr)); } input[name="q"] { grid-column: span 2; } header, main { padding-left: 16px; padding-right: 16px; } }
  </style>
</head>
<body>
  <header>
    <div><h1>Container Sync Object Logs</h1></div>
    <nav>%s</nav>
  </header>
  <main>
    <section class="panel">
      <form method="get" action="/logs">
        <label>Quickwit query<input name="q" value="%s" placeholder="object:a.txt OR outcome:failure"></label>
        <label>Account<input name="account" value="%s"></label>
        <label>Container<input name="container" value="%s"></label>
        <label>Object<input name="object" value="%s"></label>
        <label>Method<select name="method"><option value="">any</option>%s</select></label>
        <label>Outcome<select name="outcome"><option value="">any</option>%s</select></label>
        <label>Reason<input name="reason" value="%s"></label>
        <label>Host<input name="host" value="%s"></label>
        <label>Site<input name="site" value="%s"></label>
        <label>Path<input name="path" value="%s"></label>
        <label>Max hits<input name="max_hits" type="number" min="1" max="200" value="%s"></label>
        <button type="submit">Search</button>
      </form>
      <div class="meta">
        <span>query: <strong>%s</strong></span>
        <span>hits: <strong>%s</strong></span>
        <span>elapsed: <strong>%sus</strong></span>
        %s
      </div>
    </section>
    <section class="table-wrap">
      <table>
        <thead><tr><th>Timestamp</th><th>Site</th><th>Host</th><th>Account</th><th>Container</th><th>Outcome</th><th>Method</th><th>Reason</th><th>Duration ms</th><th>Object</th></tr></thead>
        <tbody>%s</tbody>
      </table>
    </section>
  </main>
</body>
</html>''' % (
        nav_html("logs"), h(values["q"]), h(values["account"]), h(values["container"]),
        h(values["object"]), option_html(["GET", "HEAD", "PUT", "DELETE"], values["method"]),
        option_html(["success", "failure", "skipped"], values["outcome"]),
        h(values["reason"]), h(values["host"]), h(values["site"]),
        h(values["path"]), h(values["max_hits"]), h(result["query"]),
        h(result["num_hits"]), h(result.get("elapsed_time_micros", 0)),
        '<span class="error">%s</span>' % h(error_text) if error_text else '',
        table_body)
    return body



def status_class(status):
    if status == "success":
        return "ok"
    if status == "failure":
        return "bad"
    if status == "skipped":
        return "warn"
    return "muted"




class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            self.write_response(200, "text/plain; version=0.0.4; charset=utf-8",
                                collect_metrics().encode("utf-8"))
        elif path == "/api/state":
            self.write_response(200, "application/json; charset=utf-8",
                                json.dumps(collect_state(), sort_keys=True).encode("utf-8"))
        elif path == "/api/object-logs":
            params = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
            self.write_response(200, "application/json; charset=utf-8",
                                json.dumps(quickwit_search(params), sort_keys=True).encode("utf-8"))
        elif path == "/logs":
            params = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
            self.write_response(200, "text/html; charset=utf-8",
                                render_object_logs(params).encode("utf-8"))
        elif path in ("/containers", "/container-status"):
            params = parse_qs(self.path.split("?", 1)[1] if "?" in self.path else "")
            self.write_response(200, "text/html; charset=utf-8",
                                render_containers(params).encode("utf-8"))
        elif path in ("/", "/status"):
            self.write_redirect("/containers")
        elif path == "/healthz":
            self.write_response(200, "text/plain; charset=utf-8", b"ok\n")
        else:
            self.write_response(404, "text/plain; charset=utf-8", b"not found\n")

    def write_response(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_redirect(self, location):
        body = b""
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

    def log_message(self, fmt, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
