# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.resources import Resources  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestResourcesController(BaseTestCase):
    """ResourcesController integration test stubs"""

    def test_portalresources_get(self):
        """Test case for portalresources_get

        Retrieve a listing and description of available resources for portal
        """
        query_string = [('graph_format', 'GRAPHML'),
                        ('level', 1),
                        ('force_refresh', false),
                        ('start_date', 'start_date_example'),
                        ('end_date', 'end_date_example'),
                        ('includes', 'includes_example'),
                        ('excludes', 'excludes_example')]
        response = self.client.open(
            '//portalresources',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_resources_get(self):
        """Test case for resources_get

        Retrieve a listing and description of available resources. By default, a cached available resource information is returned. User can force to request the current available resources.
        """
        query_string = [('level', 1),
                        ('force_refresh', false),
                        ('start_date', 'start_date_example'),
                        ('end_date', 'end_date_example'),
                        ('includes', 'includes_example'),
                        ('excludes', 'excludes_example')]
        response = self.client.open(
            '//resources',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


    def test_resources_find_slot(self):
        """Test case for resources_find_slot

        Find available time slots for resources
        """
        body = {
            "start": "2025-07-01T00:00:00+00:00",
            "end": "2025-07-15T00:00:00+00:00",
            "duration": 24,
            "resources": [
                {"type": "compute", "site": "RENC", "cores": 2, "ram": 4, "disk": 10}
            ]
        }
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '//resources/find-slot',
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_resources_find_slot_multi_resource(self):
        """Test case for find-slot with multiple resource types"""
        body = {
            "start": "2025-07-01T00:00:00+00:00",
            "end": "2025-07-15T00:00:00+00:00",
            "duration": 48,
            "max_results": 5,
            "resources": [
                {"type": "compute", "site": "RENC", "cores": 32, "ram": 64, "disk": 100,
                 "components": {"GPU-A100": 1}},
                {"type": "link", "site_a": "RENC", "site_b": "CLEM", "bandwidth": 25},
                {"type": "facility_port", "name": "RENC-Chameleon", "site": "RENC", "vlans": 2}
            ]
        }
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer special-key',
        }
        response = self.client.open(
            '//resources/find-slot',
            method='POST',
            headers=headers,
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
