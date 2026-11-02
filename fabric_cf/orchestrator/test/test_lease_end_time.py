"""
Unit tests for the lease end time the Orchestrator computes for create/renew.

Terms exchanged with the Broker and AM are carried in milliseconds, so a computed
end time must not carry a sub-second remainder. These tests need no
Kafka/Postgres/Neo4j.
"""
import logging
import unittest
from datetime import datetime, timedelta, timezone

from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler


class ComputeLeaseEndTimeTest(unittest.TestCase):
    def setUp(self):
        self.handler = OrchestratorHandler.__new__(OrchestratorHandler)
        self.handler.logger = logging.getLogger(self.__class__.__name__)
        self.handler.infrastructure_project_id = []
        self.compute = self.handler._OrchestratorHandler__compute_lease_end_time

    def test_capped_renew_end_is_whole_seconds(self):
        # Asking beyond the long-lived limit falls back to now + limit
        _, end = self.compute(lease_end_time=datetime.now(timezone.utc) + timedelta(days=400),
                              allow_long_lived=True, project_id="p")
        self.assertEqual(0, end.microsecond)
        self.assertLessEqual(end - datetime.now(timezone.utc), timedelta(weeks=26))

    def test_default_lifetime_end_is_whole_seconds(self):
        _, end = self.compute(project_id="p")
        self.assertEqual(0, end.microsecond)

    def test_explicit_end_kept(self):
        requested = (datetime.now(timezone.utc) + timedelta(days=10)).replace(microsecond=0)
        _, end = self.compute(lease_end_time=requested, project_id="p")
        self.assertEqual(requested, end)


if __name__ == '__main__':
    unittest.main()
