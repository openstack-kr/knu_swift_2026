#!/usr/bin/env python3
import html
import json
import os
import socket
import time
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

TOTAL_COUNTERS = {
    "puts": "Successful remote PUT operations triggered by container-sync.",
    "deletes": "Successful remote DELETE operations triggered by container-sync.",
    "bytes": "Object bytes sent by container-sync PUT operations.",
    "row_attempts": "Object rows attempted by container-sync.",
    "row_successes": "Object rows completed successfully by container-sync.",
    "row_failures": "Object rows that failed during container-sync.",
    "remote_head_skips": "Rows skipped because the remote object was already current.",
    "remote_not_founds": "DELETE rows where the remote object was already missing.",
    "remote_conflicts": "DELETE rows that received a remote conflict response.",
    "client_exception_failures": "Rows that failed with a Swift client exception.",
    "unexpected_failures": "Rows that failed with an unexpected exception.",
    "versioning_symlink_skips": "Rows skipped because they were versioning symlinks.",
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
        ("/", "Overview", "overview"),
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
            h(hit.get("timestamp", "")),
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


def render_html():
    states = collect_state()
    nodes_up = sum(1 for state in states if state["up"])
    daemon_rows = []
    total_new_backlog = 0
    total_retry_backlog = 0
    failed_containers = 0

    cards = []
    for state in states:
        recon = state.get("container_sync", {}) or {}
        daemon = recon.get("daemon", {}) or {}
        total_new_backlog += number(daemon.get("new_backlog_rows"))
        total_retry_backlog += number(daemon.get("retry_backlog_rows"))
        failed_containers += number(daemon.get("failed_containers"))

        update_age = age_text(recon.get("timestamp", 0)) if state["up"] else "n/a"
        last_run = age_text(daemon.get("last_run_timestamp", 0))
        cards.append('''
        <section class="node-card %s">
          <div class="node-head">
            <h2>%s</h2>
            <span>%s</span>
          </div>
          <dl>
            <div><dt>Recon age</dt><dd>%s</dd></div>
            <div><dt>Last run</dt><dd>%s</dd></div>
            <div><dt>Scanned</dt><dd>%s</dd></div>
            <div><dt>Synced</dt><dd>%s</dd></div>
            <div><dt>Failed</dt><dd>%s</dd></div>
          </dl>
          <p class="path">%s</p>
        </section>''' % (
            "up" if state["up"] else "down",
            h(state["node"]),
            "up" if state["up"] else h(state["error"]),
            h(update_age),
            h(last_run),
            h(fmt_int(daemon.get("scanned_containers", 0))),
            h(fmt_int(daemon.get("synced_containers", 0))),
            h(fmt_int(daemon.get("failed_containers", 0))),
            h(state["path"])))

        daemon_rows.append('''
        <tr>
          <td>%s</td>
          <td><span class="pill %s">%s</span></td>
          <td>%s</td>
          <td>%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
          <td class="num">%s</td>
        </tr>''' % (
            h(state["node"]),
            "ok" if state["up"] else "bad",
            "up" if state["up"] else h(state["error"]),
            h(update_age),
            h(last_run),
            h(fmt_int(daemon.get("last_run_duration_seconds", 0))),
            h(fmt_int(daemon.get("scanned_containers", 0))),
            h(fmt_int(daemon.get("synced_containers", 0))),
            h(fmt_int(daemon.get("skipped_containers", 0))),
            h(fmt_int(daemon.get("failed_containers", 0))),
            h(fmt_int(daemon.get("new_backlog_rows", 0))),
            h(fmt_int(daemon.get("retry_backlog_rows", 0))),
            h(fmt_int(
                number(daemon.get("max_new_backlog_rows")) +
                number(daemon.get("max_retry_backlog_rows")))))
        )

    table_body = "\n".join(daemon_rows) if daemon_rows else '<tr><td colspan="12" class="empty">No recon state</td></tr>'

    body = '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="%s">
  <title>Container Sync Recon</title>
  <style>
    :root { color-scheme: light; --bg: #f6f8fb; --ink: #172033; --muted: #667085; --line: #d8dee8; --panel: #ffffff; --ok: #087443; --bad: #b42318; --warn: #b54708; --blue: #155eef; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header { padding: 28px 32px 18px; border-bottom: 1px solid var(--line); background: #fff; }
    h1 { margin: 0; font-size: 28px; font-weight: 720; letter-spacing: 0; }
    .sub { margin-top: 6px; color: var(--muted); font-size: 14px; }
    nav { margin-top: 12px; display: flex; gap: 14px; flex-wrap: wrap; }
    nav a { color: var(--blue); text-decoration: none; font-weight: 650; font-size: 14px; }
    nav a.active { color: var(--ink); }
    main { padding: 24px 32px 36px; max-width: 1600px; margin: 0 auto; }
    .summary { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .summary-card, .node-card, .table-wrap { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }
    .summary-card { padding: 16px; }
    .summary-card span { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .summary-card strong { display: block; margin-top: 8px; font-size: 28px; line-height: 1; }
    .nodes { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .node-card { padding: 16px; }
    .node-card.down { border-color: #f1b8b2; }
    .node-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
    .node-head h2 { margin: 0; font-size: 17px; overflow-wrap: anywhere; }
    .node-head span { color: var(--muted); font-size: 13px; }
    .node-card.up .node-head span { color: var(--ok); }
    .node-card.down .node-head span { color: var(--bad); }
    dl { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; margin: 16px 0 0; }
    dt { color: var(--muted); font-size: 12px; }
    dd { margin: 4px 0 0; font-weight: 650; }
    .path { margin: 14px 0 0; color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }
    .table-wrap { overflow: auto; }
    table { width: 100%%; border-collapse: collapse; min-width: 1120px; }
    th, td { padding: 11px 12px; border-bottom: 1px solid var(--line); text-align: left; font-size: 13px; vertical-align: middle; }
    th { position: sticky; top: 0; background: #fbfcfe; color: #475467; font-weight: 680; }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .pill { display: inline-flex; align-items: center; height: 24px; padding: 0 9px; border-radius: 999px; font-weight: 680; font-size: 12px; }
    .pill.ok { color: var(--ok); background: #dcfae6; }
    .pill.bad { color: var(--bad); background: #fee4e2; }
    .pill.warn { color: var(--warn); background: #fef0c7; }
    .pill.muted { color: var(--muted); background: #eef2f6; }
    .empty { text-align: center; color: var(--muted); padding: 28px; }
    footer { color: var(--muted); font-size: 12px; margin-top: 12px; }
    @media (max-width: 820px) { header, main { padding-left: 16px; padding-right: 16px; } .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); } dl { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  </style>
</head>
<body>
  <header>
    <h1>Container Sync Recon</h1>
    <div class="sub">Auto refresh: %ss.</div>
    <nav>%s</nav>
  </header>
  <main>
    <section class="summary">
      <div class="summary-card"><span>Recon nodes up</span><strong>%s/%s</strong></div>
      <div class="summary-card"><span>New backlog rows</span><strong>%s</strong></div>
      <div class="summary-card"><span>Retry backlog rows</span><strong>%s</strong></div>
      <div class="summary-card"><span>Failed containers</span><strong>%s</strong></div>
    </section>
    <section class="nodes">%s</section>
    <section class="table-wrap">
      <table>
        <thead>
          <tr><th>Node</th><th>Status</th><th>Recon age</th><th>Last run</th><th>Duration s</th><th>Scanned</th><th>Synced</th><th>Skipped</th><th>Failed</th><th>New backlog</th><th>Retry backlog</th><th>Max backlog</th></tr>
        </thead>
        <tbody>%s</tbody>
      </table>
    </section>
    <footer>/metrics exposes the same recon values for Prometheus. /api/state returns raw parsed state. Use <a href="/logs">Object History</a> for account/container/object lookup.</footer>
  </main>
</body>
</html>''' % (
        WEB_REFRESH_SECONDS,
        WEB_REFRESH_SECONDS,
        nav_html("overview"),
        nodes_up,
        len(states),
        fmt_int(total_new_backlog),
        fmt_int(total_retry_backlog),
        fmt_int(failed_containers),
        "\n".join(cards),
        table_body)
    return body

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
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            target = "/logs" + (("?" + query) if query else "")
            body = ("Container status moved to %s\n" % target).encode("utf-8")
            self.send_response(302)
            self.send_header("Location", target)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path in ("/", "/status"):
            self.write_response(200, "text/html; charset=utf-8",
                                render_html().encode("utf-8"))
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

    def log_message(self, fmt, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
