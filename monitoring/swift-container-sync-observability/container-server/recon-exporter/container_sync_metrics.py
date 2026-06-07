#!/usr/bin/env python3

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen


RECON_URL = os.environ.get(
    'CONTAINER_SYNC_RECON_URL',
    'http://127.0.0.1:6201/recon/container-sync')
LISTEN_HOST = os.environ.get('CONTAINER_SYNC_METRICS_HOST', '0.0.0.0')
LISTEN_PORT = int(os.environ.get('CONTAINER_SYNC_METRICS_PORT', '8011'))

METRICS = (
    ('swift_container_sync_sweep_seconds',
     'Time taken to complete the most recent container sync sweep.',
     (('container_sync_sweep',),
      ('container_sync_time',),
      ('container_sync_daemon', 'last_run_duration_seconds'))),
    ('swift_container_sync_last_timestamp_seconds',
     'Unix timestamp of the most recent completed container sync sweep.',
     ('container_sync_last',)),
    ('swift_container_sync_syncs',
     'Containers synced during the most recent report interval.',
     ('container_sync_stats', 'syncs')),
    ('swift_container_sync_puts',
     'PUT operations during the most recent report interval.',
     ('container_sync_stats', 'puts')),
    ('swift_container_sync_deletes',
     'DELETE operations during the most recent report interval.',
     ('container_sync_stats', 'deletes')),
    ('swift_container_sync_skips',
     'Skipped operations during the most recent report interval.',
     ('container_sync_stats', 'skips')),
    ('swift_container_sync_failures',
     'Failed operations during the most recent report interval.',
     ('container_sync_stats', 'failures')),
)


def get_nested_value(data, path):
    paths = path
    if path and isinstance(path[0], str):
        paths = (path,)

    last_error = None
    for candidate in paths:
        try:
            value = data
            for key in candidate:
                value = value[key]
            if not isinstance(value, (int, float)):
                raise TypeError('%s must be a number' % '.'.join(candidate))
            return value
        except (KeyError, TypeError) as err:
            last_error = err
    raise last_error


def fetch_recon():
    with urlopen(RECON_URL, timeout=5) as response:
        return json.load(response)


def render_metrics(data):
    lines = []
    for name, help_text, path in METRICS:
        value = get_nested_value(data, path)
        lines.extend((
            '# HELP %s %s' % (name, help_text),
            '# TYPE %s gauge' % name,
            '%s %s' % (name, value),
        ))
    return ('\n'.join(lines) + '\n').encode('ascii')


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)
            return

        try:
            body = render_metrics(fetch_recon())
        except Exception as err:
            self.send_error(503, 'Unable to read container sync recon: %s' %
                            err)
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; version=0.0.4')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, message_format, *args):
        print('%s - %s' % (self.address_string(),
                           message_format % args))


def main():
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), MetricsHandler)
    print('Serving container sync metrics on %s:%s' %
          (LISTEN_HOST, LISTEN_PORT))
    server.serve_forever()


if __name__ == '__main__':
    main()
