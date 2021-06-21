# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestResourcesController(BaseTestCase):
    """ResourcesController integration test stubs"""

    def test_portalresources_get(self):
        """Test case for portalresources_get

        Retrieve a listing and description of available resources for portal
        """
        query_string = [('graph_format', 'JSON_NODELINK')]
        response = self.client.open(
            '//portalresources',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_resources_get(self):
        """Test case for resources_get

        Retrieve a listing and description of available resources
        """
        query_string = [('level', 1)]
        response = self.client.open(
            '//resources',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
