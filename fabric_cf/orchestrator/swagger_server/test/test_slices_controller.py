# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestSlicesController(BaseTestCase):
    """SlicesController integration test stubs"""

    def test_slices_create_post(self):
        """Test case for slices_create_post

        Create slice
        """
        body = 'body_example'
        query_string = [('slice_name', 'slice_name_example'),
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

    def test_slices_delete_slice_iddelete(self):
        """Test case for slices_delete_slice_iddelete

        Delete slice.
        """
        response = self.client.open(
            '//slices/delete/{sliceID}'.format(slice_id='slice_id_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_get(self):
        """Test case for slices_get

        Retrieve a listing of user slices
        """
        query_string = [('states', 'states_example')]
        response = self.client.open(
            '//slices',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_modify_slice_idput(self):
        """Test case for slices_modify_slice_idput

        Modify slice
        """
        body = 'body_example'
        response = self.client.open(
            '//slices/modify/{sliceID}'.format(slice_id='slice_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_redeem_slice_idpost(self):
        """Test case for slices_redeem_slice_idpost

        Redeem resources reserved via Create API
        """
        response = self.client.open(
            '//slices/redeem/{sliceID}'.format(slice_id='slice_id_example'),
            method='POST')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_renew_slice_idpost(self):
        """Test case for slices_renew_slice_idpost

        Renew slice
        """
        query_string = [('new_lease_end_time', 'new_lease_end_time_example')]
        response = self.client.open(
            '//slices/renew/{sliceID}'.format(slice_id='slice_id_example'),
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_slice_idget(self):
        """Test case for slices_slice_idget

        slice properties
        """
        query_string = [('graph_format', 'GRAPHML')]
        response = self.client.open(
            '//slices/{sliceID}'.format(slice_id='slice_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_status_slice_idget(self):
        """Test case for slices_status_slice_idget

        slice status
        """
        response = self.client.open(
            '//slices/status/{sliceID}'.format(slice_id='slice_id_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
