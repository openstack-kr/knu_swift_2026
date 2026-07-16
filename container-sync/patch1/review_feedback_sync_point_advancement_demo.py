#!/usr/bin/env python3

"""
Standalone reproduction of the review-feedback expectation around
sync_point advancement.

This file is intentionally kept out of patch1/test_sync.py because the final
assertions model the reviewer's expectation, not the current mainline/patch1
algorithm. It is meant to be shared separately in review discussion.

Usage:
    SWIFT_REPO_ROOT=/path/to/swift ./review_feedback_sync_point_advancement_demo.py

If SWIFT_REPO_ROOT is unset, /home/ubuntu/swift is used.
"""

import importlib.util
import os
import sys
import unittest

from unittest import mock


SWIFT_REPO_ROOT = os.environ.get('SWIFT_REPO_ROOT', '/home/ubuntu/swift')
PATCH_DIR = os.path.dirname(__file__)

sys.path.insert(0, SWIFT_REPO_ROOT)

from swift.common.storage_policy import StoragePolicy
from test.debug_logger import debug_logger
from test.unit import patch_policies


def load_patch_sync():
    sync_path = os.path.join(PATCH_DIR, 'sync.py')
    spec = importlib.util.spec_from_file_location(
        'review_feedback_patch_sync', sync_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRing(object):

    def __init__(self):
        self.devs = [{'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}
                     for x in range(3)]

    def get_nodes(self, account, container=None, obj=None):
        return 1, list(self.devs)


class FakeContainerBroker(object):

    def __init__(self, path, metadata=None, info=None, deleted=False,
                 items_since=None):
        self.db_file = path
        self.db_dir = os.path.dirname(path)
        self.metadata = metadata if metadata else {}
        self.info = info if info else {}
        self.deleted = deleted
        self.items_since = items_since if items_since else []
        self.sync_point1 = -1
        self.sync_point2 = -1

    def get_max_row(self):
        return 1

    def get_info(self):
        return self.info

    def is_deleted(self):
        return self.deleted

    def get_items_since(self, sync_point, limit):
        if sync_point < 0:
            sync_point = 0
        return self.items_since[sync_point:sync_point + limit]

    def set_x_container_sync_points(self, sync_point1, sync_point2):
        self.sync_point1 = sync_point1
        self.sync_point2 = sync_point2


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestReviewFeedbackExpectation(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger('review-feedback-sync-point-demo')
        self.sync = load_patch_sync()

    def test_review_feedback_sync_point_advancement(self):
        cring = FakeRing()
        with mock.patch.object(self.sync, 'InternalClient'):
            cs = self.sync.ContainerSync({}, container_ring=cring,
                                         logger=self.logger)

        def fake_hash_path(account, container, obj, raw_digest=False):
            return b'\x00' * 16

        completed = []
        fcb = FakeContainerBroker(
            'path',
            info={'account': 'a', 'container': 'c',
                  'storage_policy_index': 0,
                  'x_container_sync_point1': -1,
                  'x_container_sync_point2': -1},
            metadata={'x-container-sync-to': ('http://127.0.0.1/a/c', 1),
                      'x-container-sync-key': ('key', 1)},
            items_since=[{'ROWID': 1, 'name': 'o1', 'created_at': '1.0',
                          'deleted': True},
                         {'ROWID': 2, 'name': 'o2', 'created_at': '2.0',
                          'deleted': True},
                         {'ROWID': 3, 'name': 'o3', 'created_at': '3.0',
                          'deleted': True},
                         {'ROWID': 4, 'name': 'o4', 'created_at': '4.0',
                          'deleted': True},
                         {'ROWID': 5, 'name': 'o5', 'created_at': '5.0',
                          'deleted': True}])
        sync_point_updates = []
        orig_set_sync_points = fcb.set_x_container_sync_points

        def tracking_set_sync_points(sync_point1, sync_point2):
            sync_point_updates.append((sync_point1, sync_point2))
            orig_set_sync_points(sync_point1, sync_point2)

        def fake_container_sync_row(row, sync_to, user_key, broker, info,
                                    realm, realm_key):
            if row['ROWID'] == 3:
                return False
            completed.append(row['ROWID'])
            return True

        fcb.set_x_container_sync_points = tracking_set_sync_points
        with mock.patch.object(self.sync, 'ContainerBroker',
                               lambda p, logger: fcb), \
                mock.patch.object(self.sync, 'hash_path', fake_hash_path), \
                mock.patch.object(cs, 'container_sync_row',
                                  fake_container_sync_row):
            cs._myips = ['10.0.0.0']
            cs._myport = 1000
            cs.allowed_sync_hosts = ['127.0.0.1']
            cs.container_sync('isa.db')

        self.assertEqual([1, 2], completed)
        self.assertEqual(1, cs.container_failures)
        self.assertEqual(2, fcb.sync_point1)
        self.assertIsNone(fcb.sync_point2)
        self.assertEqual((2, None), sync_point_updates[-1])


if __name__ == '__main__':
    unittest.main(verbosity=2)
