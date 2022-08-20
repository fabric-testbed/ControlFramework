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
        query_string = [('graph_format', 'GRAPHML')]
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
                        ('force_refresh', false)]
        response = self.client.open(
            '//resources',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
