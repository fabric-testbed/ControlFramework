# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric.orchestrator.swagger_server.test import BaseTestCase


class TestResourcesController(BaseTestCase):
    """ResourcesController integration test stubs"""

    def test_resources_get(self):
        """Test case for resources_get

        Retrieve a listing and description of available resources
        """
        response = self.client.open(
            '//resources',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
