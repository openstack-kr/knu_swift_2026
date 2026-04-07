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

# idea-batch-worker: Intra-Container Batch I/O (행 단위 병렬 처리)
#
# [문제]
#   기존 Phase 2의 while 루프는 get_items_since(point, 1)로 행을 1개씩
#   가져와 container_sync_row()를 호출한다.
#   container_sync_row() 내부에서 HEAD -> GET -> PUT 순으로 네트워크 I/O가
#   직렬 발생하므로 행당 ~1초(HEAD 300ms + GET 200ms + PUT 500ms)가 걸린다.
#
# [아이디어]
#   행을 batch_worker_count개씩 묶어 한 번에 가져온 뒤,
#   eventlet.GreenPool로 여러 행의 네트워크 I/O를 동시에 수행한다.
#   Swift는 현재 eventlet(Green Thread) 기반으로 동작하므로 OS 스레드를
#   생성하는 concurrent.futures.ThreadPoolExecutor 대신 GreenPool을 사용해야
#   eventlet의 I/O 루프(Hub)와 안전하게 통합된다.
#   Green Thread는 네트워크 대기 중에 제어권을 자동으로 양보(yield)하므로
#   I/O-bound 작업인 HEAD/PUT/DELETE를 효율적으로 병렬화할 수 있다.
#
#   [향후 교체 경로]
#   OpenStack Swift는 eventlet 제거를 진행 중이다. eventlet이 제거된 이후에는
#   아래 두 줄을 교체하면 동일한 효과를 얻을 수 있다:
#     현재: pool = eventlet.GreenPool(size=batch_size)
#           pool.spawn_n(...) / pool.waitall()
#     교체: executor = concurrent.futures.ThreadPoolExecutor(max_workers=...)
#           futures.append(executor.submit(...)) / executor.shutdown(wait=True)
#
# [구현 상세]
#   - get_items_since(sync_point1, batch_worker_count) 로 배치 조회
#   - 이 노드 담당 행(hash % len(nodes) == ordinal)만 pool.spawn_n()
#   - pool.waitall()로 배치 내 모든 Green Thread 완료 대기
#   - 배치의 마지막 ROWID로 sync_point1을 한 번에 전진
#   - 배치 완료 후 broker.set_x_container_sync_points() 호출
#   - container_sync_row() 내부의 카운터(container_puts 등)는
#     threading.Lock으로 보호 (eventlet monkey_patch 후에도 동작)
#
# [설정]
#   container-server.conf [container-sync]
#     batch_worker_count = 16  (기본값)

import collections
import errno
import os
import threading
import uuid
from time import ctime, time
from random import choice, random
from struct import unpack_from

import eventlet
from eventlet import sleep, Timeout
from urllib.parse import urlparse

import swift.common.db
from swift.common.db import DatabaseConnectionError
from swift.container.backend import ContainerBroker
from swift.container.sync_store import ContainerSyncStore
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.daemon import run_daemon
from swift.common.internal_client import (
    delete_object, put_object, head_object,
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

        # Phase 2 병렬 처리 스레드 수.
        # 한 번에 가져올 행 수(배치 크기)와 동일하게 사용한다.
        self.batch_worker_count = int(conf.get('batch_worker_count', 16))
        # container_sync_row()가 여러 스레드에서 동시에 실행될 때
        # self.container_puts 등의 카운터를 안전하게 갱신하기 위한 Lock.
        self._stats_lock = threading.Lock()

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
        """
        idea-batch-worker 변경:
        Phase 2에서 행을 batch_worker_count개씩 묶어 가져온 뒤
        eventlet.GreenPool으로 병렬 처리합니다.
        Phase 1은 원본과 동일 (직렬).
        """
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
                    # ── Phase 1: 원본과 동일 (직렬) ───────────────────────
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

                    # ── Phase 2: 배치 병렬 처리 ───────────────────────────
                    # GreenPool을 한 번 생성하고, 배치마다 spawn_n() + waitall()
                    # 패턴으로 배치 내 모든 Green Thread가 끝날 때까지 대기한다.
                    # eventlet 제거 이후에는 ThreadPoolExecutor로 교체 가능.
                    sync_stage_time = time()
                    batch_size = self.batch_worker_count
                    pool = eventlet.GreenPool(size=batch_size)
                    while sync_stage_time < stop_at:
                        # 기존 1개씩 조회 대신 batch_size개씩 한 번에 조회
                        rows = broker.get_items_since(sync_point1, batch_size)
                        if not rows:
                            break
                        # 이 노드가 담당하는 행만 Green Thread로 제출
                        # (hash % len(nodes) == ordinal)
                        for row in rows:
                            key = hash_path(
                                info['account'], info['container'],
                                row['name'], raw_digest=True)
                            if unpack_from('>I', key)[0] % \
                                    len(nodes) == ordinal:
                                pool.spawn_n(
                                    self.container_sync_row,
                                    row, sync_to, user_key,
                                    broker, info, realm, realm_key)
                        # 배치 내 모든 Green Thread 완료 대기.
                        # container_sync_row()가 내부에서 예외를 처리하므로
                        # 미처리 예외가 전파되지 않는다.
                        pool.waitall()
                        # sync_point1을 배치 마지막 ROWID로 한 번에 전진.
                        # 기존에는 행마다 전진했으나, 배치 완료 후 일괄 처리.
                        sync_point1 = rows[-1]['ROWID']
                        broker.set_x_container_sync_points(
                            sync_point1, None)
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
        headers = {'x-timestamp': timestamp.internal}
        self._update_sync_to_headers(name, sync_to, user_key, realm,
                                     realm_key, 'HEAD', headers)
        try:
            metadata, _ = head_object(sync_to, name=name,
                                      headers=headers,
                                      proxy=self.select_http_proxy(),
                                      logger=self.logger,
                                      retries=0)
            remote_ts = Timestamp(
                metadata.get('x-timestamp', Timestamp.zero()))
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
        try:
            start_time = time()
            ts_data, ts_ctype, ts_meta = decode_timestamps(
                row['created_at'])
            if row['deleted']:
                try:
                    headers = {'x-timestamp': ts_data.internal}
                    self._update_sync_to_headers(row['name'], sync_to,
                                                 user_key, realm, realm_key,
                                                 'DELETE', headers)
                    delete_object(sync_to, name=row['name'], headers=headers,
                                  proxy=self.select_http_proxy(),
                                  logger=self.logger,
                                  timeout=self.conn_timeout)
                except ClientException as err:
                    if err.http_status not in (
                            HTTP_NOT_FOUND, HTTP_CONFLICT):
                        raise
                with self._stats_lock:
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
                put_object(sync_to, name=row['name'], headers=headers,
                           contents=FileLikeIter(body),
                           proxy=self.select_http_proxy(), logger=self.logger,
                           timeout=self.conn_timeout)
                with self._stats_lock:
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
            with self._stats_lock:
                self.container_failures += 1
            self.logger.increment('failures')
            return False
        except (Exception, Timeout):
            self.logger.exception(
                'ERROR Syncing %(db_file)s %(row)s',
                {'db_file': str(broker), 'row': row})
            with self._stats_lock:
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
