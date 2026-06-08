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

import collections
import errno
import json
import os
import socket
import uuid
from datetime import datetime, timezone
from time import ctime, time
from random import choice, random
from struct import unpack_from

from swift.common.concurrency import sleep, Timeout
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
from swift.common.memcached import load_memcache
from swift.common.exceptions import ClientException
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.swob import normalize_etag
from swift.common.utils import (
    clean_content_type, config_true_value, dump_recon_cache,
    FileLikeIter, get_logger, hash_path, quote, validate_sync_to,
    # row 단위 병렬 작업에 ContextPool을 사용
    whataremyips, Timestamp, decode_timestamps, parse_options, ContextPool)
from swift.common.daemon import Daemon
from swift.common.http import HTTP_UNAUTHORIZED, HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.wsgi import ConfigString
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, RECON_CONTAINER_FILE
from swift.common.middleware.versioned_writes.object_versioning import (
    SYSMETA_VERSIONS_CONT, SYSMETA_VERSIONS_SYMLINK)


# The default internal client config body is to support upgrades without
# requiring deployment of the new /etc/swift/internal-client.conf
ic_conf_body = """
[DEFAULT]
[pipeline:main]
pipeline = catch_errors proxy-logging cache symlink proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true
allow_account_management = true

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
    """
    Daemon to sync syncable containers.

    This is done by scanning the local devices for container databases and
    checking for x-container-sync-to and x-container-sync-key metadata values.
    If they exist, newer rows since the last sync will trigger PUTs or DELETEs
    to the other container.

    The actual syncing is slightly more complicated to make use of the three
    (or number-of-replicas) main nodes for a container without each trying to
    do the exact same work but also without missing work if one node happens to
    be down.

    Two sync points are kept per container database. All rows between the two
    sync points trigger updates. Any rows newer than both sync points cause
    updates depending on the node's position for the container (primary nodes
    do one third, etc. depending on the replica count of course). After a sync
    run, the first sync point is set to the newest ROWID known and the second
    sync point is set to newest ROWID for which all updates have been sent.

    An example may help. Assume replica count is 3 and perfectly matching
    ROWIDs starting at 1.

       First sync run, database has 6 rows:

       * SyncPoint1 starts as -1.
       * SyncPoint2 starts as -1.
       * No rows between points, so no "all updates" rows.
       * Six rows newer than SyncPoint1, so a third of the rows are sent
         by node 1, another third by node 2, remaining third by node 3.
       * SyncPoint1 is set as 6 (the newest ROWID known).
       * SyncPoint2 is left as -1 since no "all updates" rows were synced.

       Next sync run, database has 12 rows:

       * SyncPoint1 starts as 6.
       * SyncPoint2 starts as -1.
       * The rows between -1 and 6 all trigger updates (most of which
         should short-circuit on the remote end as having already been
         done).
       * Six more rows newer than SyncPoint1, so a third of the rows are
         sent by node 1, another third by node 2, remaining third by node
         3.
       * SyncPoint1 is set as 12 (the newest ROWID known).
       * SyncPoint2 is set as 6 (the newest "all updates" ROWID).

    In this way, under normal circumstances each node sends its share of
    updates each run and just sends a batch of older updates to ensure nothing
    was missed.

    :param conf: The dict of configuration values from the [container-sync]
                 section of the container-server.conf
    :param container_ring: If None, the <swift_dir>/container.ring.gz will be
                           loaded. This is overridden by unit tests.
    """
    log_route = 'container-sync'

    def __init__(self, conf, container_ring=None, logger=None):
        #: The dict of configuration values from the [container-sync] section
        #: of the container-server.conf.
        self.conf = conf
        #: Logger to use for container-sync log lines.
        self.logger = logger or get_logger(conf, log_route=self.log_route)
        #: Path to the local device mount points.
        self.devices = conf.get('devices', '/srv/node')
        #: Indicates whether mount points should be verified as actual mount
        #: points (normally true, false for tests and SAIO).
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        #: Minimum time between full scans. This is to keep the daemon from
        #: running wild on near empty systems.
        self.interval = float(conf.get('interval', 300))
        #: Maximum amount of time to spend syncing a container before moving on
        #: to the next one. If a container sync hasn't finished in this time,
        #: it'll just be resumed next scan.
        self.container_time = int(conf.get('container_time', 60))
        #: ContainerSyncCluster instance for validating sync-to values.
        self.realms_conf = ContainerSyncRealms(
            os.path.join(
                conf.get('swift_dir', '/etc/swift'),
                'container-sync-realms.conf'),
            self.logger)
        #: The list of hosts we're allowed to send syncs to. This can be
        #: overridden by data in self.realms_conf
        self.allowed_sync_hosts = [
            h.strip()
            for h in conf.get('allowed_sync_hosts', '127.0.0.1').split(',')
            if h.strip()]
        self.http_proxies = [
            a.strip()
            for a in conf.get('sync_proxy', '').split(',')
            if a.strip()]
        #: ContainerSyncStore instance for iterating over synced containers
        self.sync_store = ContainerSyncStore(self.devices,
                                             self.logger,
                                             self.mount_check)
        #: Number of containers with sync turned on that were successfully
        #: synced.
        self.container_syncs = 0
        #: Number of successful DELETEs triggered.
        self.container_deletes = 0
        #: Number of successful PUTs triggered.
        self.container_puts = 0
        #: Number of containers whose sync has been turned off, but
        #: are not yet cleared from the sync store.
        self.container_skips = 0
        #: Number of containers that had a failure of some type.
        self.container_failures = 0

        #: Per container stats. These are collected per container.
        #: puts - the number of puts that were done for the container
        #: deletes - the number of deletes that were fot the container
        #: bytes - the total number of bytes transferred per the container
        self.container_stats = collections.defaultdict(int)
        self.container_stats.clear()

        self.recon_enabled = config_true_value(
            conf.get('container_sync_recon_enabled', 'true'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.rcache = os.path.join(self.recon_cache_path, RECON_CONTAINER_FILE)
        self.recon_interval = float(conf.get('recon_interval', 15))
        self.recon_last_flush = 0
        self.recon_hostname = socket.gethostname()
        self.recon_totals = collections.defaultdict(int)
        self.recon_containers = {}
        self.recon_scan = {
            'mode': 'init',
            'last_run_timestamp': 0,
            'last_run_finished_timestamp': 0,
            'last_run_duration_seconds': 0,
            'scanned_containers': 0,
            'synced_containers': 0,
            'skipped_containers': 0,
            'failed_containers': 0,
            'time_exhausted_containers': 0,
            'new_backlog_rows': 0,
            'retry_backlog_rows': 0,
            'max_new_backlog_rows': 0,
            'max_retry_backlog_rows': 0,
        }

        #: Time of last stats report.
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
        self.retry_memcache = load_memcache(conf, self.logger)
        # row 단위 동시성과 batch 조회 크기를 설정
        self.sync_row_concurrency = max(
            1, int(conf.get('sync_row_concurrency') or 8))
        self.sync_row_batch_size = max(
            1, int(conf.get('sync_row_batch_size') or 100))
        request_tries = int(conf.get('request_tries') or 3)

        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            internal_client_conf_path = os.path.join(
                self.swift_dir,
                'internal-client.conf')
            if os.path.exists(internal_client_conf_path):
                self.logger.warning(
                    'Configuration option internal_client_conf_path not '
                    'set, but %s exists and will be used.',
                    internal_client_conf_path)
                internal_client_conf = internal_client_conf_path
            else:
                self.logger.warning(
                    'Configuration option internal_client_conf_path not '
                    'defined. In a future release, this will be an error. '
                    'Using default configuration, See '
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
        """
        Runs container sync scans until stopped.
        """
        sleep(random() * self.interval)
        while True:
            begin = self._recon_begin_scan('forever')
            for path in self.sync_store.synced_containers_generator():
                self.container_stats.clear()
                self.container_sync(path)
                if time() - self.reported >= 3600:  # once an hour
                    self.report()
            elapsed = time() - begin
            self._recon_finish_scan(begin)
            if elapsed < self.interval:
                sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """
        Runs a single container sync scan.
        """
        self.logger.info('Begin container sync "once" mode')
        begin = self._recon_begin_scan('once')
        for path in self.sync_store.synced_containers_generator():
            self.container_sync(path)
            if time() - self.reported >= 3600:  # once an hour
                self.report()
        self.report()
        elapsed = time() - begin
        self._recon_finish_scan(begin)
        self.logger.info(
            'Container sync "once" mode completed: %.02fs', elapsed)

    def report(self):
        """
        Writes a report of the stats to the logger and resets the stats for the
        next report.
        """
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
                         max_row, outcome='success', time_exhausted=False):
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
        max_row = max(0, int(max_row or 0))
        sync_point2 = max(0, int(sync_point2 or 0))
        object_count = max(0, int(info.get('object_count') or 0))
        replication_rate = 1.0 if not max_row else min(
            1.0, float(sync_point2) / float(max_row))
        timestamp = datetime.fromtimestamp(
            end, timezone.utc).isoformat().replace('+00:00', 'Z')
        event = {
            'event_type': 'container_sync_container',
            'timestamp': timestamp,
            'host': self.recon_hostname,
            'program': self.log_route,
            'account': info.get('account', ''),
            'container': info.get('container', ''),
            'outcome': outcome,
            'sync_point1': max(0, int(sync_point1 or 0)),
            'sync_point2': sync_point2,
            'max_row': max_row,
            'object_count': object_count,
            'replication_rate': replication_rate,
            'puts': max(0, int(self.container_stats['puts'] or 0)),
            'deletes': max(0, int(self.container_stats['deletes'] or 0)),
            'bytes': max(0, int(self.container_stats['bytes'] or 0)),
            'request_time': max(0, end - start),
            'time_exhausted': 1 if time_exhausted else 0,
        }
        self.logger.info('container-sync-container-event %s',
                         json.dumps(event, sort_keys=True))

    def _recon_begin_scan(self, mode):
        now = time()
        if self.recon_enabled:
            self.recon_scan = {
                'mode': mode,
                'last_run_timestamp': now,
                'last_run_finished_timestamp': 0,
                'last_run_duration_seconds': 0,
                'scanned_containers': 0,
                'synced_containers': 0,
                'skipped_containers': 0,
                'failed_containers': 0,
                'time_exhausted_containers': 0,
                'new_backlog_rows': 0,
                'retry_backlog_rows': 0,
                'max_new_backlog_rows': 0,
                'max_retry_backlog_rows': 0,
            }
            self._recon_flush()
        return now

    def _recon_finish_scan(self, started_at):
        if not self.recon_enabled:
            return
        now = time()
        self.recon_scan['last_run_finished_timestamp'] = now
        self.recon_scan['last_run_duration_seconds'] = max(
            0, now - started_at)
        self._recon_flush(force=True)

    def _recon_record_container(self, info, status, sync_point1=None,
                                sync_point2=None, max_row=None,
                                time_exhausted=False):
        if not self.recon_enabled or not info:
            return

        sync_point1 = int(
            sync_point1 if sync_point1 is not None else
            info.get('x_container_sync_point1', -1))
        sync_point2 = int(
            sync_point2 if sync_point2 is not None else
            info.get('x_container_sync_point2', -1))
        max_row = int(max_row or 0)

        new_backlog_rows = max(0, max_row - max(sync_point1, 0))
        retry_backlog_rows = max(0, sync_point1 - max(sync_point2, 0))
        self.recon_scan['new_backlog_rows'] += new_backlog_rows
        self.recon_scan['retry_backlog_rows'] += retry_backlog_rows
        self.recon_scan['max_new_backlog_rows'] = max(
            self.recon_scan.get('max_new_backlog_rows', 0),
            new_backlog_rows)
        self.recon_scan['max_retry_backlog_rows'] = max(
            self.recon_scan.get('max_retry_backlog_rows', 0),
            retry_backlog_rows)

        if status == 'success':
            self.recon_scan['synced_containers'] += 1
        elif status == 'failure':
            self.recon_scan['failed_containers'] += 1
        elif status == 'skipped':
            self.recon_scan['skipped_containers'] += 1
        if time_exhausted:
            self.recon_scan['time_exhausted_containers'] += 1

        account = info.get('account', '')
        container = info.get('container', '')
        key = '%s/%s' % (account, container)
        self.recon_containers[key] = {
            'account': account,
            'container': container,
            'status': status,
            'last_status': status,
            'last_reason': status,
            'updated': time(),
            'sync_point1': max(0, sync_point1),
            'sync_point2': max(0, sync_point2),
            'max_row': max_row,
            'object_count': max(0, int(info.get('object_count') or 0)),
            'new_backlog_rows': new_backlog_rows,
            'retry_backlog_rows': retry_backlog_rows,
            'time_exhausted': 1 if time_exhausted else 0,
        }
        self._recon_flush()

    def _recon_flush(self, force=False):
        if not self.recon_enabled:
            return
        now = time()
        if not force and now - self.recon_last_flush < self.recon_interval:
            return

        last_run = (self.recon_scan.get('last_run_finished_timestamp') or
                    self.recon_scan.get('last_run_timestamp') or now)
        stats = dict(self.recon_totals)
        stats.update({
            'attempted': self.recon_scan.get('scanned_containers', 0),
            'syncs': self.recon_scan.get('synced_containers', 0),
            'failures': self.recon_scan.get('failed_containers', 0),
            'skips': self.recon_scan.get('skipped_containers', 0),
            'time_exhausted': self.recon_scan.get(
                'time_exhausted_containers', 0),
        })
        cache_update = {
            'container_sync_time': self.recon_scan.get(
                'last_run_duration_seconds', 0),
            'container_sync_last': last_run,
            'container_sync_stats': stats,
            'container_sync_daemon': dict(self.recon_scan),
            'container_sync_containers': dict(self.recon_containers),
            'container_sync_hostname': self.recon_hostname,
        }

        try:
            if not os.path.isdir(self.recon_cache_path):
                try:
                    os.makedirs(self.recon_cache_path)
                except OSError as err:
                    if err.errno != errno.EEXIST:
                        raise
            dump_recon_cache(cache_update, self.rcache, self.logger)
            self.recon_last_flush = now
        except (Exception, Timeout):
            self.logger.exception('ERROR writing container sync recon cache')

    # memcached에서 retry state를 읽어 온다. 없으면 초기값을 만든다
    def _read_retry_state(self, info, sync_point2, node_count):
        retry_state = {'rotation': 0, 'slots': {}}
        retry_cache_prefix = 'container-sync/slot/%s' % (
            hash_path(info['account'], info['container'], None),)
        if self.retry_memcache:
            retry_state['rotation'] = (
                self.retry_memcache.get('%s/rotation' %
                                        retry_cache_prefix) or 0
            ) % node_count
        for owner_index in range(node_count):
            cached_retry_slot = None
            if self.retry_memcache:
                cached_retry_slot = self.retry_memcache.get(
                    '%s/%s' % (retry_cache_prefix, owner_index))
            # 기존에 retry_slot이 없으면 생성
            cached_retry_slot = cached_retry_slot or {}
            point = max(
                sync_point2,
                cached_retry_slot.get('point', sync_point2))
            retry_state['slots'][str(owner_index)] = {'point': point}
        return retry_state

    # owner의 retry_point를 target_sync_point1까지 진행
    def _sync_retry_slot(self, owner_index, retry_slot, broker, info,
                         sync_to, user_key, realm, realm_key, stop_at,
                         target_sync_point1, node_count):
        retry_point = retry_slot['point']

        while time() < stop_at and retry_point < target_sync_point1:
            # 1. GET: 배치 단위로 row를 가져옴
            rows = broker.get_items_since(
                retry_point, self.sync_row_batch_size)
            rows = [row for row in rows
                    if row['ROWID'] <= target_sync_point1]
            if not rows:
                break

            # 2. Calculate Owner: batch마다 owner 계산해서 내 몫인 row만 처리
            rows_with_owner = [
                (row, unpack_from(
                    '>I', hash_path(
                        info['account'], info['container'], row['name'],
                        raw_digest=True))[0] % node_count == owner_index)
                for row in rows]
            owned_rows = [
                row for row, is_owned in rows_with_owner if is_owned]

            # 3. Sync: owner 몫인 row는 병렬로 작업을 던져서 처리
            with ContextPool(self.sync_row_concurrency) as pool:
                row_sync_waiters = [
                    pool.spawn(
                        self.container_sync_row, row, sync_to, user_key,
                        broker, info, realm, realm_key)
                    for row in owned_rows]
                # 순서대로 wait하며 결과를 회수
                owned_row_sync_results = iter([
                    row_sync_waiter.wait()
                    for row_sync_waiter in row_sync_waiters])

            # retry_point는 전체 row 순서를 따라감
            # owner 몫이면 결과를 확인하고, 아니면 point만 전진
            for row, is_owned in rows_with_owner:
                if is_owned:
                    success = next(owned_row_sync_results)
                    if not success:
                        retry_slot['point'] = retry_point
                        return retry_slot
                retry_point = row['ROWID']

            retry_slot['point'] = retry_point
            self._store_retry_slot(info, owner_index, retry_slot)
            broker.set_x_container_sync_points(None, retry_point)

        retry_slot['point'] = retry_point
        return retry_slot

    # 현재 노드의 point를 memcached에 저장
    def _store_retry_slot(self, info, owner_index, retry_slot):
        retry_cache_prefix = 'container-sync/slot/%s' % (
            hash_path(info['account'], info['container'], None),)
        if self.retry_memcache:
            self.retry_memcache.set(
                '%s/%s' % (retry_cache_prefix, owner_index),
                retry_slot)

    # retry slot 저장 후 memcahced의 retry state를 다시 읽고,
    # 완료면 rotation을 0으로 되돌리고 아니면 +1
    def _finalize_retry_state(self, info, retry_state, sync_point2,
                              target_sync_point1, node_count):
        # 먼저 memcached의 retry_state를 다시 읽는다
        retry_state = self._read_retry_state(info, sync_point2, node_count)
        # 1. Complete Check: 모든 노드가 target_sync_point1까지 끝났으면
        # sp2와 slot point를 모두 target_sync_point1로 맞추고
        # 다음 retry window를 위해 rotation을 0으로 되돌린다
        if all(state['point'] >= target_sync_point1
               for state in retry_state['slots'].values()):
            return self._complete_retry_state(
                info, retry_state, target_sync_point1, node_count)

        # 2. Advance: 아직 미완료 slot이 있으면 현재 공통 진행 지점은 min(point)
        sync_point2 = min(
            state['point'] for state in retry_state['slots'].values())
        if not self.retry_memcache:
            return sync_point2, retry_state

        # 3. Lock For Rotation: rotation 증가는 retry window마다 한 노드만
        # 하도록 lock으로 막는다.
        retry_cache_prefix = 'container-sync/slot/%s' % (
            hash_path(info['account'], info['container'], None),)
        lock_key = '%s/rotation-lock/%s' % (
            retry_cache_prefix, target_sync_point1)
        # run_forever의 시작 jitter까지 고려해 같은 retry window의 replica들이
        # 같은 lock key를 보도록 interval과 container_time 중 큰 값으로 둔다.
        lock_ttl = max(self.interval, self.container_time)
        lock_value = self.retry_memcache.incr(lock_key, time=lock_ttl)
        if lock_value != 1:
            return sync_point2, retry_state

        # 4. Recheck And Rotate: lock을 잡은 노드만 최신 상태를 다시 보고
        # 완료면 rotation을 0으로, 미완료면 rotation을 한 칸 올린다
        retry_state = self._read_retry_state(info, sync_point2, node_count)
        if all(state['point'] >= target_sync_point1
               for state in retry_state['slots'].values()):
            return self._complete_retry_state(
                info, retry_state, target_sync_point1, node_count)
        else:
            sync_point2 = min(
                state['point'] for state in retry_state['slots'].values())
            retry_state['rotation'] = (
                retry_state['rotation'] + 1) % node_count
            if self.retry_memcache:
                self.retry_memcache.set(
                    '%s/rotation' % retry_cache_prefix,
                    retry_state['rotation'])
        return sync_point2, retry_state
    
    # 모든 노드가 target_sync_point1까지 끝난 경우,
    # rotation을 0으로 되돌리고 slot point를 target_sync_point1로 정리
    def _complete_retry_state(self, info, retry_state,
                              target_sync_point1, node_count):
        retry_state['rotation'] = 0
        retry_cache_prefix = 'container-sync/slot/%s' % (
            hash_path(info['account'], info['container'], None),)
        if self.retry_memcache:
            self.retry_memcache.set(
                '%s/rotation' % retry_cache_prefix,
                retry_state['rotation'])
        for owner_index in range(node_count):
            retry_state['slots'][str(owner_index)] = {
                'point': target_sync_point1}
            self._store_retry_slot(
                info, owner_index, retry_state['slots'][str(owner_index)])
        return target_sync_point1, retry_state

    def container_sync(self, path):
        """
        Checks the given path for a container database, determines if syncing
        is turned on for that database and, if so, sends any updates to the
        other container.

        :param path: the path to a container db
        """
        broker = None
        info = None
        try:
            broker = ContainerBroker(path, logger=self.logger)
            # The path we pass to the ContainerBroker is a real path of
            # a container DB. If we get here, however, it means that this
            # path is linked from the sync_containers dir. In rare cases
            # of race or processes failures the link can be stale and
            # the get_info below will raise a DB doesn't exist exception
            # In this case we remove the stale link and raise an error
            # since in most cases the db should be there.
            try:
                info = broker.get_info()
            except DatabaseConnectionError as db_err:
                if str(db_err).endswith("DB doesn't exist"):
                    self.sync_store.remove_synced_container(broker)
                raise

            if self.recon_enabled:
                self.recon_scan['scanned_containers'] += 1

            x, nodes = self.container_ring.get_nodes(info['account'],
                                                     info['container'])
            for ordinal, node in enumerate(nodes):
                if is_local_device(self._myips, self._myport,
                                   node['ip'], node['port']):
                    break
            else:
                self._recon_record_container(
                    info, 'skipped', max_row=broker.get_max_row())
                return

            if broker.metadata.get(SYSMETA_VERSIONS_CONT):
                self.container_skips += 1
                self.logger.increment('skips')
                self.logger.warning('Skipping container %s/%s with '
                                    'object versioning configured' % (
                                        info['account'], info['container']))
                self._recon_record_container(
                    info, 'skipped', max_row=broker.get_max_row())
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
                    self._recon_record_container(
                        info, 'skipped', sync_point1=sync_point1,
                        sync_point2=sync_point2,
                        max_row=broker.get_max_row())
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
                    self._recon_record_container(
                        info, 'failure', sync_point1=sync_point1,
                        sync_point2=sync_point2,
                        max_row=broker.get_max_row())
                    return
                start_at = time()
                stop_at = start_at + self.container_time
                next_sync_point = None
                sync_stage_time = start_at
                sync_completed = False
                try:
                    # Phase 1 (sync_point2 < sync_point1)
                    if sync_point2 < sync_point1:
                        node_count = len(nodes)
                        # 1. Read: retry_state를 읽고 로컬 sp2를 최신으로 맞춘다
                        target_sync_point1 = sync_point1
                        retry_state = self._read_retry_state(
                            info, sync_point2, node_count)
                        rotation = retry_state['rotation']
                        sync_point2 = min(
                            state['point']
                            for state in retry_state['slots'].values())
                        broker.set_x_container_sync_points(None, sync_point2)

                        # 2. Sync: 이번 rotation에서 현재 노드가 맡은 owner slot만 처리
                        updated_owners = []
                        for owner_index in range(node_count):
                            retry_slot = retry_state['slots'][
                                str(owner_index)]
                            if (owner_index + rotation) % \
                                    node_count != ordinal:
                                continue
                            retry_state['slots'][str(owner_index)] = \
                                self._sync_retry_slot(
                                    owner_index, retry_slot, broker, info,
                                    sync_to, user_key, realm, realm_key,
                                    stop_at, target_sync_point1,
                                    node_count)
                            updated_owners.append(owner_index)

                        # 3. Store: 이번 sync에서 바뀐
                        # owner slot의 point를 memcached에 저장
                        for owner_index in updated_owners:
                            self._store_retry_slot(
                                info, owner_index,
                                retry_state['slots'][str(owner_index)])

                        # 4. Finalize: 최신 retry_state를 다시 보고
                        # rotation과 sp2를 최종 정리
                        sync_point2, retry_state = self._finalize_retry_state(
                            info, retry_state, sync_point2,
                            target_sync_point1, node_count)
                        broker.set_x_container_sync_points(None, sync_point2)
                    next_sync_point = sync_point2
                    sync_stage_time = time()

                    # Phase 2 (sync_point1 <= new row)
                    with ContextPool(self.sync_row_concurrency) as pool:
                        while sync_stage_time < stop_at:
                            rows = broker.get_items_since(sync_point1, 1)
                            if not rows:
                                break

                            row = rows[0]
                            # 1. Calculate Owner: 새 row의 owner가 현재 노드면
                            # 이번 sync에서 바로 처리한다
                            if unpack_from(
                                    '>I', hash_path(
                                        info['account'], info['container'],
                                        row['name'], raw_digest=True)
                            )[0] % len(nodes) == ordinal:
                                # 2. Sync: 작업은 pool에 맡기고 waitall에서 회수한다
                                pool.spawn(
                                    self.container_sync_row, row, sync_to,
                                    user_key, broker, info, realm, realm_key)
                            # 4. Advance: owner가 아니어도 새 row 영역은 모두
                            # 지나갔다는 뜻이므로 sync_point1은 현재 row까지 전진
                            sync_point1 = row['ROWID']

                            # 5. Flush: new row 진행 지점은 한 row씩 바로 DB에 반영
                            broker.set_x_container_sync_points(
                                sync_point1, None)
                            sync_stage_time = time()

                        pool.waitall()
                    sync_stage_time = time()
                    self.container_syncs += 1
                    self.logger.increment('syncs')
                    sync_completed = True
                finally:
                    max_row = broker.get_max_row()
                    self.container_report(
                        start_at, sync_stage_time, sync_point1,
                        next_sync_point, info, max_row,
                        outcome='success' if sync_completed else 'failure',
                        time_exhausted=sync_stage_time >= stop_at)
                    if sync_completed:
                        self._recon_record_container(
                            info, 'success', sync_point1=sync_point1,
                            sync_point2=next_sync_point, max_row=max_row,
                            time_exhausted=sync_stage_time >= stop_at)
            else:
                self._recon_record_container(
                    info, 'skipped',
                    sync_point1=info.get('x_container_sync_point1'),
                    sync_point2=info.get('x_container_sync_point2'),
                    max_row=broker.get_max_row())
        except (Exception, Timeout):
            self.container_failures += 1
            self.logger.increment('failures')
            if info:
                try:
                    max_row = broker.get_max_row() if broker else None
                except (Exception, Timeout):
                    max_row = None
                self._recon_record_container(
                    info, 'failure',
                    sync_point1=info.get('x_container_sync_point1'),
                    sync_point2=info.get('x_container_sync_point2'),
                    max_row=max_row)
            self.logger.exception('ERROR Syncing %s',
                                  broker if broker else path)

    def _log_object_sync_event(self, row, info, sync_to, method, outcome,
                               start_time, end_time=None, http_status=0,
                               reason='', bytes_transferred=0):
        end_time = end_time or time()
        object_name = row.get('name', '')
        account = info.get('account', '')
        container = info.get('container', '')
        parsed_sync_to = urlparse(sync_to)
        remote_container_path = parsed_sync_to.path.rstrip('/')
        http_status = int(http_status or 0)
        request_time = max(0, end_time - start_time)
        bytes_transferred = max(0, int(bytes_transferred or 0))
        object_bytes = max(0, int(row.get('size') or 0))
        timestamp = datetime.fromtimestamp(
            end_time, timezone.utc).isoformat().replace('+00:00', 'Z')

        event = {
            'event_type': 'container_sync_object',
            'timestamp': timestamp,
            'host': self.recon_hostname,
            'program': self.log_route,
            'account': account,
            'container': container,
            'object': object_name,
            'remote_container_path': remote_container_path,
            'method': method,
            'outcome': outcome,
            'reason': reason or '',
            'status': http_status,
            'request_time': request_time,
            'bytes': bytes_transferred,
            'object_bytes': object_bytes,
            'deleted': 1 if row.get('deleted') else 0,
        }
        self.logger.info('container-sync-object-event %s',
                         json.dumps(event, sort_keys=True))

    def _client_exception_reason(self, err):
        if err.http_status == HTTP_UNAUTHORIZED:
            return 'unauthorized'
        if err.http_status == HTTP_NOT_FOUND:
            return 'not_found'
        if err.http_status == HTTP_CONFLICT:
            return 'conflict'
        try:
            status_class = int(err.http_status) // 100
        except (TypeError, ValueError):
            return 'client_exception'
        if status_class == 4:
            return 'client_error'
        if status_class == 5:
            return 'server_error'
        return 'client_exception'

    def _update_sync_to_headers(self, name, sync_to, user_key,
                                realm, realm_key, method, headers):
        """
        Updates container sync headers

        :param name: The name of the object
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :param method: HTTP method to create sig with
        :param headers: headers to update with container sync headers
        """
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
        Performs head object on remote to eliminate extra remote put and
        local get object calls

        :param name: The name of the object in the updated row in the local
                     database triggering the sync update.
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :param timestamp: last modified date of local object
        :returns: True if object already exists in remote
        """
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
            # Object in remote should be updated
            return False
        except ClientException as http_err:
            # Object not in remote
            if http_err.http_status == 404:
                return False
            raise http_err

    def container_sync_row(self, row, sync_to, user_key, broker, info,
                           realm, realm_key):
        """
        Sends the update the row indicates to the sync_to container.
        Update can be either delete or put.

        :param row: The updated row in the local database triggering the sync
                    update.
        :param sync_to: The URL to the remote container.
        :param user_key: The X-Container-Sync-Key to use when sending requests
                         to the other container.
        :param broker: The local container database broker.
        :param info: The get_info result from the local container database
                     broker.
        :param realm: The realm from self.realms_conf, if there is one.
            If None, fallback to using the older allowed_sync_hosts
            way of syncing.
        :param realm_key: The realm key from self.realms_conf, if there
            is one. If None, fallback to using the older
            allowed_sync_hosts way of syncing.
        :returns: True on success
        """
        failure_reason = 'unexpected_exception'
        try:
            start_time = time()
            if self.recon_enabled:
                self.recon_totals['row_attempts'] += 1
            # extract last modified time from the created_at value
            ts_data, ts_ctype, ts_meta = decode_timestamps(
                row['created_at'])
            if row['deleted']:
                # when sync'ing a deleted object, use ts_data - this is the
                # timestamp of the source tombstone
                delete_status = 0
                delete_reason = ''
                try:
                    failure_reason = 'remote_delete_failed'
                    headers = {'x-timestamp': ts_data.internal}
                    self._update_sync_to_headers(row['name'], sync_to,
                                                 user_key, realm, realm_key,
                                                 'DELETE', headers)
                    delete_object(sync_to, name=row['name'], headers=headers,
                                  proxy=self.select_http_proxy(),
                                  logger=self.logger,
                                  timeout=self.conn_timeout)
                except ClientException as err:
                    delete_status = err.http_status
                    if err.http_status == HTTP_NOT_FOUND:
                        delete_reason = 'remote_not_found'
                    elif err.http_status == HTTP_CONFLICT:
                        delete_reason = 'remote_conflict'
                    if err.http_status not in (
                            HTTP_NOT_FOUND, HTTP_CONFLICT):
                        raise
                self._log_object_sync_event(
                    row, info, sync_to, 'DELETE', 'success', start_time,
                    http_status=delete_status, reason=delete_reason)
                self.container_deletes += 1
                self.container_stats['deletes'] += 1
                self.logger.increment('deletes')
                self.logger.timing_since('deletes.timing', start_time)
                if self.recon_enabled:
                    self.recon_totals['deletes'] += 1
                    self.recon_totals['row_successes'] += 1
            else:
                # when sync'ing a live object, use ts_meta - this is the time
                # at which the source object was last modified by a PUT or POST
                if self._object_in_remote_container(row['name'],
                                                    sync_to, user_key, realm,
                                                    realm_key, ts_meta):
                    self._log_object_sync_event(
                        row, info, sync_to, 'HEAD', 'skipped', start_time,
                        reason='remote_current')
                    if self.recon_enabled:
                        self.recon_totals['remote_head_skips'] += 1
                        self.recon_totals['row_successes'] += 1
                    return True
                exc = None
                # look up for the newest one; the symlink=get query-string has
                # no effect unless symlinks are enabled in the internal client
                # in which case it ensures that symlink objects retain their
                # symlink property when sync'd.
                headers_out = {'X-Newest': True,
                               'X-Backend-Storage-Policy-Index':
                               str(info['storage_policy_index'])}
                try:
                    failure_reason = 'source_get_failed'
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

                # skip object_versioning links; this is in case the container
                # metadata is out of date
                if headers.get(SYSMETA_VERSIONS_SYMLINK):
                    self.logger.info(
                        'Skipping versioning symlink %s/%s/%s ' % (
                            info['account'], info['container'],
                            row['name']))
                    self._log_object_sync_event(
                        row, info, sync_to, 'PUT', 'skipped', start_time,
                        reason='versioning_symlink')
                    if self.recon_enabled:
                        self.recon_totals['row_successes'] += 1
                    return True

                timestamp = Timestamp(
                    headers.get('x-timestamp', Timestamp.zero()))
                if timestamp < ts_meta:
                    failure_reason = 'source_timestamp_older_than_row'
                    if exc:
                        failure_reason = 'source_get_failed'
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
                failure_reason = 'remote_put_failed'
                self._update_sync_to_headers(row['name'], sync_to, user_key,
                                             realm, realm_key, 'PUT', headers)
                put_object(sync_to, name=row['name'], headers=headers,
                           contents=FileLikeIter(body),
                           proxy=self.select_http_proxy(), logger=self.logger,
                           timeout=self.conn_timeout)
                self._log_object_sync_event(
                    row, info, sync_to, 'PUT', 'success', start_time,
                    bytes_transferred=row['size'])
                self.container_puts += 1
                self.container_stats['puts'] += 1
                self.container_stats['bytes'] += row['size']
                self.logger.increment('puts')
                self.logger.timing_since('puts.timing', start_time)
                if self.recon_enabled:
                    self.recon_totals['puts'] += 1
                    self.recon_totals['bytes'] += row['size']
                    self.recon_totals['row_successes'] += 1
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
            reason = self._client_exception_reason(err)
            self._log_object_sync_event(
                row, info, sync_to, 'DELETE' if row['deleted'] else 'PUT',
                'failure', start_time, http_status=err.http_status,
                reason=reason)
            self.container_failures += 1
            self.logger.increment('failures')
            if self.recon_enabled:
                self.recon_totals['row_failures'] += 1
            return False
        except (Exception, Timeout):
            self.logger.exception(
                'ERROR Syncing %(db_file)s %(row)s',
                {'db_file': str(broker), 'row': row})
            self._log_object_sync_event(
                row, info, sync_to, 'DELETE' if row['deleted'] else 'PUT',
                'failure', start_time, reason=failure_reason)
            self.container_failures += 1
            self.logger.increment('failures')
            if self.recon_enabled:
                self.recon_totals['row_failures'] += 1
            return False
        return True

    def select_http_proxy(self):
        return choice(self.http_proxies) if self.http_proxies else None

def main():
    conf_file, options = parse_options(once=True)
    run_daemon(ContainerSync, conf_file, **options)

if __name__ == '__main__':
    main()
