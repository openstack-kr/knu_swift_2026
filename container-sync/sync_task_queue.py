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

# idea-task-queue: Adaptive Priority Scheduling + Zero-Pending Skip
#
# [문제]
#   기존 run_once()는 synced_containers_generator()의 모든 DB를 순서대로
#   순회하며, 이미 동기화 완료된 컨테이너에도 동일한 container_time(60초)을
#   할당한다. 3만 개 DB 중 pending이 0인 컨테이너가 대부분이어도
#   전부 열고 확인하는 낭비가 발생한다.
#
# [아이디어: 2-Pass 전략]
#   Pass 1 (Pre-scan):
#     모든 DB를 열어 get_info() + get_max_row()만 호출 (네트워크 I/O 없음).
#     sync lag = phase1_lag(SP1 - SP2) + phase2_rows(max_row - SP1) 계산.
#     lag == 0인 컨테이너는 스킵 (동기화 완료 상태).
#
#   Pass 2 (Weighted sync):
#     lag 내림차순으로 정렬해 가장 밀린 컨테이너부터 처리.
#     각 컨테이너의 시간 예산 = container_time * N * lag / total_lag
#     (N: 처리 대상 컨테이너 수, 최소 container_time 보장)
#
# [효과]
#   - zero-lag 컨테이너를 건너뛰어 불필요한 DB 쿼리 제거
#   - 가장 오래 밀린 컨테이너에 시간을 집중 배분
#   - self.container_time을 직접 변경하지 않고 time_budget 파라미터로
#     container_sync()에 전달하므로 다른 아이디어와 조합해도 안전

import collections
import errno
import os
import uuid
from time import ctime, time
from random import choice, random
from struct import unpack_from

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
        """
        Runs a single container sync scan.

        [idea-task-queue 변경점]

        Pass 1 - Pre-scan:
          모든 컨테이너 DB에 대해 get_info() + get_max_row()만 호출해
          sync lag를 계산한다. 네트워크 I/O가 없으므로 매우 빠르다.
            phase1_lag = max(0, SP1 - SP2)   # 재확인 구간 행 수
            phase2_rows = max(0, max_row - SP1)  # 신규 행 수
            lag = phase1_lag + phase2_rows
          SP1이 -1(초기 상태)인 경우를 max(sp1, 0)으로 처리한다.
          lag == 0이면 동기화 완료 상태이므로 스킵한다.

        Pass 2 - Weighted sync:
          lag 내림차순으로 정렬해 가장 밀린 컨테이너를 먼저 처리한다.
          각 컨테이너에 할당할 시간(time_budget)을 다음 공식으로 계산:
            time_budget = container_time * N * lag / total_lag
          단, 최솟값은 container_time (기본 할당량) 이상으로 보장한다.
        """
        self.logger.info('Begin container sync "once" mode')
        begin = time()

        # Pass 1: 경량 pre-scan -- 모든 컨테이너의 lag 수집
        tasks = []  # list of (lag, path), lag 기준으로 정렬 예정
        for path in self.sync_store.synced_containers_generator():
            try:
                broker = ContainerBroker(path, logger=self.logger)
                info = broker.get_info()
                sp1 = info['x_container_sync_point1']
                sp2 = info['x_container_sync_point2']
                max_row = broker.get_max_row()
                # SP1 < 0 은 아직 한 번도 sync가 완료되지 않은 초기 상태.
                # max(sp1, 0)으로 처리해 phase2_rows 계산 시 음수 방지.
                phase1_lag = max(0, sp1 - sp2)
                phase2_rows = max(0, max_row - max(sp1, 0))
                if phase1_lag == 0 and phase2_rows == 0:
                    # 동기화 완료 상태 -- DB 열기 비용 외 추가 작업 없음
                    self.container_skips += 1
                    self.logger.increment('skips')
                    continue
                lag = phase1_lag + phase2_rows
                tasks.append((lag, path))
            except Exception:
                # DB 오류 등 -- lag 계산 불가 시 container_sync에서 처리
                tasks.append((0, path))

        if not tasks:
            self.report()
            elapsed = time() - begin
            self.logger.info(
                'Container sync "once" mode completed: %.02fs', elapsed)
            return

        # Pass 2: lag 내림차순 정렬 -- 가장 밀린 컨테이너부터 처리
        tasks.sort(key=lambda x: x[0], reverse=True)

        # 비례 시간 분배
        # total_lag가 0이면 1로 대체해 ZeroDivisionError 방지
        total_lag = sum(lag for lag, _ in tasks) or 1
        n = len(tasks)

        for lag, path in tasks:
            if lag > 0:
                # lag 비율에 따라 시간 배정.
                # 최솟값은 self.container_time (기존 기본값) 이상으로 보장.
                time_budget = max(
                    self.container_time,
                    int(self.container_time * n * lag / total_lag))
            else:
                # lag 계산 실패한 컨테이너는 기본 시간 할당
                time_budget = self.container_time
            self.container_stats.clear()
            self.container_sync(path, time_budget=time_budget)
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

    def container_sync(self, path, time_budget=None):
        """
        Checks the given path for a container database, determines if syncing
        is turned on for that database and, if so, sends any updates to the
        other container.

        [idea-task-queue 변경점]
        stop_at 계산 시 self.container_time 대신 time_budget을 사용한다.
        run_once()에서 lag 비율로 계산한 값을 받아 컨테이너마다 다른
        시간을 할당할 수 있다. None이면 기존 self.container_time을 사용해
        run_forever() 등 다른 호출 경로와 호환된다.

        :param path: the path to a container db
        :param time_budget: 이 컨테이너에 할당할 최대 처리 시간(초).
                            None 이면 self.container_time 사용.
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
                budget = time_budget if time_budget is not None \
                    else self.container_time
                stop_at = start_at + budget
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
