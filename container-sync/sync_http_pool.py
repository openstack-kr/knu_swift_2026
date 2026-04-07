# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# idea-http-pool: HTTP Connection Pool Reuse (TCP 연결 재사용)
#
# [문제]
#   head_object() / put_object() / delete_object()는 swift.common.internal_client
#   의 SimpleClient를 매번 새로 인스턴스화한다.
#   SimpleClient.base_request()는 urllib_request.urlopen()을 사용하므로
#   매 요청마다 새 TCP 연결을 수립한다 (TCP 3-way handshake ~50ms 반복).
#   컨테이너 1개에 행이 1,000개이면 최소 2,000번의 TCP 연결이 발생한다.
#
# [아이디어]
#   urllib3.PoolManager를 프로세스 수준 싱글톤(_HTTP_POOL)으로 유지한다.
#   동일 호스트(dst)에 대한 연결을 풀에서 꺼내 재사용하므로 TCP 수립
#   오버헤드를 없앤다.
#   _PooledSyncClient 클래스가 head / put / delete 요청을 담당하며,
#   container_sync_row()와 _object_in_remote_container()에서
#   기존 head_object / put_object / delete_object 대신 이를 호출한다.
#
# [구현 상세]
#   - _HTTP_POOL: urllib3.PoolManager 프로세스 수준 싱글톤
#     num_pools=10 (호스트별 풀), maxsize=8 (풀당 유지 연결 수)
#   - _PooledSyncClient: sync_to URL을 받아 HEAD/PUT/DELETE 요청 수행
#   - urllib3는 Swift의 기존 의존성(requirements.txt)에 포함되어 있으므로
#     외부 라이브러리 추가 없음
#
# [효과]
#   1,000행 * 2회 연결 = 2,000번의 TCP 수립
#   -> 최초 maxsize(8)번만 수립 후 나머지는 재사용
#   -> 연결 수립 비용 ~50ms * 1,992번 = 약 100초 절감/컨테이너

import collections
import errno
import os
import uuid
from time import ctime, time
from random import choice, random
from struct import unpack_from

import urllib3
from eventlet import sleep, Timeout
from urllib.parse import urlparse

import swift.common.db
from swift.common.db import DatabaseConnectionError
from swift.container.backend import ContainerBroker
from swift.container.sync_store import ContainerSyncStore
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.daemon import run_daemon
from swift.common.internal_client import (
    InternalClient, UnexpectedResponse)
from swift.common.exceptions import ClientException
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.swob import normalize_etag
from swift.common.utils import (
    clean_content_type, config_true_value,
    FileLikeIter, get_logger, hash_path, quote, validate_sync_to,
    whataremyips, Timestamp, decode_timestamps, parse_options)
from swift.common.daemon import Daemon
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.wsgi import ConfigString
from swift.common.middleware.versioned_writes.object_versioning import (
    SYSMETA_VERSIONS_CONT, SYSMETA_VERSIONS_SYMLINK)


ic_conf_body = """
[DEFAULT]
[pipeline:main]
pipeline = catch_errors proxy-logging cache symlink proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true

[filter:symlink]
use = egg:swift#symlink

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
""".lstrip()


# 프로세스 수준 연결 풀 싱글톤.
# 모든 ContainerSync 인스턴스와 모든 컨테이너가 이 풀을 공유한다.
_HTTP_POOL = None


def _get_http_pool(conn_timeout=5):
    """
    urllib3.PoolManager 싱글톤을 반환한다.
    최초 호출 시 생성되고 이후에는 동일 객체를 반환한다.

    :param conn_timeout: TCP 연결 타임아웃(초). 최초 생성 시에만 사용됨.
    :returns: urllib3.PoolManager 인스턴스
    """
    global _HTTP_POOL
    if _HTTP_POOL is None:
        _HTTP_POOL = urllib3.PoolManager(
            num_pools=10,       # 호스트(포트)별 풀 최대 10개
            maxsize=8,          # 풀당 유지할 최대 연결 수
            timeout=urllib3.Timeout(
                connect=conn_timeout,
                read=conn_timeout * 6),
            retries=False)      # 재시도는 상위 로직(container_sync_row)에서 담당
    return _HTTP_POOL


class _PooledSyncClient(object):
    """
    head_object / put_object / delete_object 의 urllib3 기반 대체 구현.

    기존 SimpleClient는 매 요청마다 새 urllib_request 객체를 생성해
    TCP 연결을 수립한다. 이 클래스는 _get_http_pool()이 반환하는
    PoolManager를 통해 동일 호스트에 대한 연결을 재사용한다.

    container_sync_row()와 _object_in_remote_container()에서
    head_object / put_object / delete_object 대신 사용된다.
    """

    def __init__(self, container_url, conn_timeout=5):
        self._url = container_url.rstrip('/')
        self._conn_timeout = conn_timeout

    def _object_url(self, name):
        return '%s/%s' % (self._url, quote(name))

    def head(self, name, headers):
        """
        HEAD 요청. 성공 시 응답 헤더 dict 반환.
        404 → ClientException(http_status=404)
        그 외 4xx/5xx → ClientException
        """
        pool = _get_http_pool(self._conn_timeout)
        resp = pool.request(
            'HEAD', self._object_url(name),
            headers=headers,
            redirect=False,
            timeout=urllib3.Timeout(connect=self._conn_timeout,
                                    read=self._conn_timeout * 6))
        if resp.status == 404:
            raise ClientException('Not found', http_status=404)
        if resp.status >= 400:
            raise ClientException(
                'HEAD failed: %d' % resp.status, http_status=resp.status)
        # urllib3 HTTPHeaderDict → 일반 dict (소문자 키)
        return {k.lower(): v for k, v in resp.headers.items()}

    def put(self, name, headers, body):
        """PUT 요청. body는 bytes 또는 파일류 객체."""
        pool = _get_http_pool(self._conn_timeout)
        resp = pool.request(
            'PUT', self._object_url(name),
            headers=headers,
            body=body,
            redirect=False,
            timeout=urllib3.Timeout(connect=self._conn_timeout,
                                    read=self._conn_timeout * 6))
        if resp.status >= 400:
            raise ClientException(
                'PUT failed: %d' % resp.status, http_status=resp.status)

    def delete(self, name, headers):
        """DELETE 요청. 404/409는 정상 처리."""
        pool = _get_http_pool(self._conn_timeout)
        resp = pool.request(
            'DELETE', self._object_url(name),
            headers=headers,
            redirect=False,
            timeout=urllib3.Timeout(connect=self._conn_timeout,
                                    read=self._conn_timeout * 6))
        if resp.status not in (200, 204, 404, 409):
            raise ClientException(
                'DELETE failed: %d' % resp.status, http_status=resp.status)


class ContainerSync(Daemon):
    log_route = 'container-sync'

    def __init__(self, conf, container_ring=None, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route=self.log_route)
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = float(conf.get('interval', 300))
        self.container_time = int(conf.get('container_time', 60))
        self.realms_conf = ContainerSyncRealms(
            os.path.join(
                conf.get('swift_dir', '/etc/swift'),
                'container-sync-realms.conf'),
            self.logger)
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.http_proxies = [
            a.strip()
            for a in conf.get('sync_proxy', '').split(',')
            if a.strip()]
        self.sync_store = ContainerSyncStore(self.devices,
                                             self.logger,
                                             self.mount_check)
        self.container_syncs = 0
        self.container_deletes = 0
        self.container_puts = 0
        self.container_skips = 0
        self.container_failures = 0
        self.container_stats = collections.defaultdict(int)
        self.container_stats.clear()
        self.reported = time()
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.container_ring = container_ring or Ring(self.swift_dir,
                                                     ring_name='container')
        bind_ip = conf.get('bind_ip', '0.0.0.0')
        self._myips = whataremyips(bind_ip)
        self._myport = int(conf.get('bind_port', 6201))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = int(conf.get('request_tries') or 3)

        # idea-http-pool: 프로세스 수준 풀 초기화 (이후 요청은 재사용)
        _get_http_pool(self.conn_timeout)

        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            self.logger.warning(
                'Configuration option internal_client_conf_path not '
                'defined. Using default configuration, See '
                'internal-client.conf-sample for options')
            internal_client_conf = ConfigString(ic_conf_body)
        else:
            internal_client_conf = internal_client_conf_path
        try:
            self.swift = InternalClient(
                internal_client_conf, 'Swift Container Sync', request_tries,
                use_replication_network=True,
                global_conf={'log_name': '%s-ic' % conf.get(
                    'log_name', self.log_route)})
        except (OSError, IOError) as err:
            if err.errno != errno.ENOENT and \
                    not str(err).endswith(' not found'):
                raise
            raise SystemExit(
                'Unable to load internal client from config: '
                '%(conf)r (%(error)s)'
                % {'conf': internal_client_conf_path, 'error': err})

    def run_forever(self, *args, **kwargs):
        sleep(random() * self.interval)
        while True:
            begin = time()
            for path in self.sync_store.synced_containers_generator():
                self.container_stats.clear()
                self.container_sync(path)
                if time() - self.reported >= 3600:
                    self.report()
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        self.logger.info('Begin container sync "once" mode')
        begin = time()
        for path in self.sync_store.synced_containers_generator():
            self.container_sync(path)
            if time() - self.reported >= 3600:
                self.report()
        self.report()
        elapsed = time() - begin
        self.logger.info(
            'Container sync "once" mode completed: %.02fs', elapsed)

    def report(self):
        self.logger.info(
            'Since %(time)s: %(sync)s synced [%(delete)s deletes, %(put)s '
            'puts], %(skip)s skipped, %(fail)s failed',
            {'time': ctime(self.reported),
             'sync': self.container_syncs,
             'delete': self.container_deletes,
             'put': self.container_puts,
             'skip': self.container_skips,
             'fail': self.container_failures})
        self.reported = time()
        self.container_syncs = 0
        self.container_deletes = 0
        self.container_puts = 0
        self.container_skips = 0
        self.container_failures = 0

    def container_report(self, start, end, sync_point1, sync_point2, info,
                         max_row):
        self.logger.info('Container sync report: %(container)s, '
                         'time window start: %(start)s, '
                         'time window end: %(end)s, '
                         'puts: %(puts)s, '
                         'posts: %(posts)s, '
                         'deletes: %(deletes)s, '
                         'bytes: %(bytes)s, '
                         'sync_point1: %(point1)s, '
                         'sync_point2: %(point2)s, '
                         'total_rows: %(total)s',
                         {'container': '%s/%s' % (info['account'],
                                                  info['container']),
                          'start': start,
                          'end': end,
                          'puts': self.container_stats['puts'],
                          'posts': 0,
                          'deletes': self.container_stats['deletes'],
                          'bytes': self.container_stats['bytes'],
                          'point1': sync_point1,
                          'point2': sync_point2,
                          'total': max_row})

    def container_sync(self, path):
        broker = None
        try:
            broker = ContainerBroker(path, logger=self.logger)
            try:
                info = broker.get_info()
            except DatabaseConnectionError as db_err:
                if str(db_err).endswith("DB doesn't exist"):
                    self.sync_store.remove_synced_container(broker)
                raise

            x, nodes = self.container_ring.get_nodes(info['account'],
                                                     info['container'])
            for ordinal, node in enumerate(nodes):
                if is_local_device(self._myips, self._myport,
                                   node['ip'], node['port']):
                    break
            else:
                return
            if broker.metadata.get(SYSMETA_VERSIONS_CONT):
                self.container_skips += 1
                self.logger.increment('skips')
                self.logger.warning('Skipping container %s/%s with '
                                    'object versioning configured' % (
                                        info['account'], info['container']))
                return
            if not broker.is_deleted():
                sync_to = None
                user_key = None
                sync_point1 = info['x_container_sync_point1']
                sync_point2 = info['x_container_sync_point2']
                for key, (value, timestamp) in broker.metadata.items():
                    if key.lower() == 'x-container-sync-to':
                        sync_to = value
                    elif key.lower() == 'x-container-sync-key':
                        user_key = value
                if not sync_to or not user_key:
                    self.container_skips += 1
                    self.logger.increment('skips')
                    return
                err, sync_to, realm, realm_key = validate_sync_to(
                    sync_to, self.allowed_sync_hosts, self.realms_conf)
                if err:
                    self.logger.info(
                        'ERROR %(db_file)s: %(validate_sync_to_err)s',
                        {'db_file': str(broker),
                         'validate_sync_to_err': err})
                    self.container_failures += 1
                    self.logger.increment('failures')
                    return
                start_at = time()
                stop_at = start_at + self.container_time
                next_sync_point = None
                sync_stage_time = start_at
                try:
                    while time() < stop_at and sync_point2 < sync_point1:
                        rows = broker.get_items_since(sync_point2, 1)
                        if not rows:
                            break
                        row = rows[0]
                        if row['ROWID'] > sync_point1:
                            break
                        if not self.container_sync_row(
                                row, sync_to, user_key, broker, info, realm,
                                realm_key):
                            if not next_sync_point:
                                next_sync_point = sync_point2
                        sync_point2 = row['ROWID']
                        broker.set_x_container_sync_points(None, sync_point2)
                    if next_sync_point:
                        broker.set_x_container_sync_points(None,
                                                           next_sync_point)
                    else:
                        next_sync_point = sync_point2
                    sync_stage_time = time()
                    while sync_stage_time < stop_at:
                        rows = broker.get_items_since(sync_point1, 1)
                        if not rows:
                            break
                        row = rows[0]
                        key = hash_path(info['account'], info['container'],
                                        row['name'], raw_digest=True)
                        if unpack_from('>I', key)[0] % \
                                len(nodes) == ordinal:
                            self.container_sync_row(
                                row, sync_to, user_key, broker, info, realm,
                                realm_key)
                        sync_point1 = row['ROWID']
                        broker.set_x_container_sync_points(sync_point1, None)
                        sync_stage_time = time()
                    self.container_syncs += 1
                    self.logger.increment('syncs')
                finally:
                    self.container_report(start_at, sync_stage_time,
                                          sync_point1,
                                          next_sync_point,
                                          info, broker.get_max_row())
        except (Exception, Timeout):
            self.container_failures += 1
            self.logger.increment('failures')
            self.logger.exception('ERROR Syncing %s',
                                  broker if broker else path)

    def _update_sync_to_headers(self, name, sync_to, user_key,
                                realm, realm_key, method, headers):
        if realm and realm_key:
            nonce = uuid.uuid4().hex
            path = urlparse(sync_to).path + '/' + quote(name)
            sig = self.realms_conf.get_sig(method, path,
                                           headers.get('x-timestamp', 0),
                                           nonce, realm_key,
                                           user_key)
            headers['x-container-sync-auth'] = '%s %s %s' % (realm,
                                                             nonce,
                                                             sig)
        else:
            headers['x-container-sync-key'] = user_key

    def _object_in_remote_container(self, name, sync_to, user_key,
                                    realm, realm_key, timestamp):
        """
        idea-http-pool: head_object() 대신 _PooledSyncClient.head() 사용.
        """
        headers = {'x-timestamp': timestamp.internal}
        self._update_sync_to_headers(name, sync_to, user_key, realm,
                                     realm_key, 'HEAD', headers)
        try:
            client = _PooledSyncClient(sync_to, self.conn_timeout)
            remote_headers = client.head(name, headers)
            remote_ts = Timestamp(
                remote_headers.get('x-timestamp', Timestamp.zero()))
            self.logger.debug("remote obj timestamp %s local obj %s" %
                              (timestamp.internal, remote_ts.internal))
            if timestamp <= remote_ts:
                return True
            return False
        except ClientException as http_err:
            if http_err.http_status == 404:
                return False
            raise http_err

    def container_sync_row(self, row, sync_to, user_key, broker, info,
                           realm, realm_key):
        """
        idea-http-pool: delete_object() / put_object() 대신
        _PooledSyncClient.delete() / .put() 사용.
        """
        try:
            start_time = time()
            ts_data, ts_ctype, ts_meta = decode_timestamps(
                row['created_at'])
            client = _PooledSyncClient(sync_to, self.conn_timeout)
            if row['deleted']:
                try:
                    headers = {'x-timestamp': ts_data.internal}
                    self._update_sync_to_headers(row['name'], sync_to,
                                                 user_key, realm, realm_key,
                                                 'DELETE', headers)
                    client.delete(row['name'], headers)
                except ClientException as err:
                    if err.http_status not in (
                            HTTP_NOT_FOUND, HTTP_CONFLICT):
                        raise
                self.container_deletes += 1
                self.container_stats['deletes'] += 1
                self.logger.increment('deletes')
                self.logger.timing_since('deletes.timing', start_time)
            else:
                if self._object_in_remote_container(row['name'],
                                                    sync_to, user_key, realm,
                                                    realm_key, ts_meta):
                    return True
                exc = None
                headers_out = {'X-Newest': True,
                               'X-Backend-Storage-Policy-Index':
                               str(info['storage_policy_index'])}
                try:
                    source_obj_status, headers, body = \
                        self.swift.get_object(info['account'],
                                              info['container'], row['name'],
                                              headers=headers_out,
                                              acceptable_statuses=(2, 4),
                                              params={'symlink': 'get'})
                except (Exception, UnexpectedResponse, Timeout) as err:
                    headers = {}
                    body = None
                    exc = err

                if headers.get(SYSMETA_VERSIONS_SYMLINK):
                    self.logger.info(
                        'Skipping versioning symlink %s/%s/%s ' % (
                            info['account'], info['container'],
                            row['name']))
                    return True

                timestamp = Timestamp(
                    headers.get('x-timestamp', Timestamp.zero()))
                if timestamp < ts_meta:
                    if exc:
                        raise exc
                    raise Exception(
                        'Unknown exception trying to GET: '
                        '%(account)r %(container)r %(object)r' %
                        {'account': info['account'],
                         'container': info['container'],
                         'object': row['name']})
                for key in ('date', 'last-modified'):
                    if key in headers:
                        del headers[key]
                if 'etag' in headers:
                    headers['etag'] = normalize_etag(headers['etag'])
                if 'content-type' in headers:
                    headers['content-type'] = clean_content_type(
                        headers['content-type'])
                self._update_sync_to_headers(row['name'], sync_to, user_key,
                                             realm, realm_key, 'PUT', headers)
                # body를 bytes로 읽어서 urllib3에 전달
                body_bytes = b''.join(body) if body else b''
                client.put(row['name'], headers, body_bytes)
                self.container_puts += 1
                self.container_stats['puts'] += 1
                self.container_stats['bytes'] += row['size']
                self.logger.increment('puts')
                self.logger.timing_since('puts.timing', start_time)
        except ClientException as err:
            if err.http_status == HTTP_UNAUTHORIZED:
                self.logger.info(
                    'Unauth %(sync_from)r => %(sync_to)r',
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to})
            elif err.http_status == HTTP_NOT_FOUND:
                self.logger.info(
                    'Not found %(sync_from)r => %(sync_to)r \
                    - object %(obj_name)r',
                    {'sync_from': '%s/%s' %
                        (quote(info['account']), quote(info['container'])),
                     'sync_to': sync_to, 'obj_name': row['name']})
            else:
                self.logger.exception(
                    'ERROR Syncing %(db_file)s %(row)s',
                    {'db_file': str(broker), 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        except (Exception, Timeout):
            self.logger.exception(
                'ERROR Syncing %(db_file)s %(row)s',
                {'db_file': str(broker), 'row': row})
            self.container_failures += 1
            self.logger.increment('failures')
            return False
        return True

    def select_http_proxy(self):
        return choice(self.http_proxies) if self.http_proxies else None


def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerSync, conf_file, **options)


if __name__ == '__main__':
    main()
