# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.poa import Poa  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.poa_post import PoaPost  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestPoasController(BaseTestCase):
    """PoasController integration test stubs"""

    def test_poas_create_sliver_id_post(self):
        """Test case for poas_create_sliver_id_post

        Perform an operational action on a sliver.
        """
        body = PoaPost()
        response = self.client.open(
            '//poas/create/{sliver_id}'.format(sliver_id='sliver_id_example'),
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_poas_get(self):
        """Test case for poas_get

        Request get the status of the POAs.
        """
        query_string = [('sliver_id', 'sliver_id_example'),
                        ('states', 'states_example'),
                        ('limit', 200),
                        ('offset', 1)]
        response = self.client.open(
            '//poas/',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_poas_poa_id_get(self):
        """Test case for poas_poa_id_get

        Perform an operational action on a sliver.
        """
        response = self.client.open(
            '//poas/{poa_id}'.format(poa_id='poa_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
