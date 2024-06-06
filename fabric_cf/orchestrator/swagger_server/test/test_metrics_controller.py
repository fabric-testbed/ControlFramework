# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.metrics import Metrics  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestMetricsController(BaseTestCase):
    """MetricsController integration test stubs"""

    def test_metrics_overview_get(self):
        """Test case for metrics_overview_get

        Control Framework metrics overview
        """
        query_string = [('excluded_projects', 'excluded_projects_example')]
        response = self.client.open(
            '//metrics/overview',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
