# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.slice_details import SliceDetails  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slices import Slices  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slivers import Slivers  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status200_ok_no_content import Status200OkNoContent  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status400_bad_request import Status400BadRequest  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status401_unauthorized import Status401Unauthorized  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status403_forbidden import Status403Forbidden  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status404_not_found import Status404NotFound  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.status500_internal_server_error import Status500InternalServerError  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestSlicesController(BaseTestCase):
    """SlicesController integration test stubs"""

    def test_slices_create_post(self):
        """Test case for slices_create_post

        Create slice
        """
        body = 'body_example'
        query_string = [('name', 'name_example'),
                        ('ssh_key', 'ssh_key_example'),
                        ('lease_end_time', 'lease_end_time_example')]
        response = self.client.open(
            '//slices/create',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_delete_slice_id_delete(self):
        """Test case for slices_delete_slice_id_delete

        Delete slice.
        """
        response = self.client.open(
            '//slices/delete/{slice_id}'.format(slice_id='slice_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_get(self):
        """Test case for slices_get

        Retrieve a listing of user slices
        """
        query_string = [('name', 'name_example'),
                        ('states', 'states_example'),
                        ('limit', 20),
                        ('offset', 1)]
        response = self.client.open(
            '//slices',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_modify_slice_id_accept_post(self):
        """Test case for slices_modify_slice_id_accept_post

        Accept the last modify an existing slice
        """
        response = self.client.open(
            '//slices/modify/{slice_id}/accept'.format(slice_id='slice_id_example'),
            method='POST')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_modify_slice_id_put(self):
        """Test case for slices_modify_slice_id_put

        Modify an existing slice
        """
        body = 'body_example'
        response = self.client.open(
            '//slices/modify/{slice_id}'.format(slice_id='slice_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_renew_slice_id_post(self):
        """Test case for slices_renew_slice_id_post

        Renew slice
        """
        query_string = [('lease_end_time', 'lease_end_time_example')]
        response = self.client.open(
            '//slices/renew/{slice_id}'.format(slice_id='slice_id_example'),
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_slice_id_get(self):
        """Test case for slices_slice_id_get

        slice properties
        """
        query_string = [('graph_format', 'GRAPHML')]
        response = self.client.open(
            '//slices/{slice_id}'.format(slice_id='slice_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
