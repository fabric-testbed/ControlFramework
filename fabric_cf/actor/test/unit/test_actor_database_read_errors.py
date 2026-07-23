#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)
"""
Unit tests: ActorDatabase.get_components / get_links must NOT swallow DB/infra
errors. Returning None on failure would be read by the allocation logic as
"nothing in use", risking double-/over-allocation, so these must propagate.

The ActorDatabase is instantiated via __new__ to avoid constructing a real
PsqlDatabase (no running Postgres required); only the small surface used by the
methods under test is stubbed.
"""
import threading
import unittest
from unittest import mock

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.common.exceptions import DatabaseException


class TestActorDatabaseReadErrorPropagation(unittest.TestCase):
    def _make(self):
        db = ActorDatabase.__new__(ActorDatabase)
        db.db = mock.Mock()
        db.lock = threading.Lock()
        db.logger = mock.Mock()
        return db

    def test_get_components_success_returns_dict(self):
        db = self._make()
        db.db.get_components.return_value = {}
        self.assertEqual(db.get_components(node_id="n", states=[1], rsv_type=["t"]), {})

    def test_get_links_success_returns_dict(self):
        db = self._make()
        db.db.get_links.return_value = {"n": 10}
        self.assertEqual(db.get_links(node_id="n", states=[1], rsv_type=["t"]), {"n": 10})

    def test_get_components_reraises_database_exception(self):
        db = self._make()
        db.db.get_components.side_effect = DatabaseException("postgres down")
        with self.assertRaises(DatabaseException):
            db.get_components(node_id="n", states=[1], rsv_type=["t"])

    def test_get_links_reraises_database_exception(self):
        db = self._make()
        db.db.get_links.side_effect = DatabaseException("postgres down")
        with self.assertRaises(DatabaseException):
            db.get_links(node_id="n", states=[1], rsv_type=["t"])

    def test_get_components_propagates_generic_driver_error(self):
        db = self._make()
        db.db.get_components.side_effect = RuntimeError("connection reset")
        with self.assertRaises(RuntimeError):
            db.get_components(node_id="n", states=[1], rsv_type=["t"])


if __name__ == "__main__":
    unittest.main()
