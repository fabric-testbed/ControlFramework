# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.success import Success  # noqa: E501
from fabric_cf.orchestrator.swagger_server.test import BaseTestCase


class TestSliversController(BaseTestCase):
    """SliversController integration test stubs"""

    def test_slivers_get(self):
        """Test case for slivers_get

        Retrieve a listing of user slivers
        """
        query_string = [('slice_id', 'slice_id_example')]
        response = self.client.open(
            '//slivers',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slivers_modify_sliver_idput(self):
        """Test case for slivers_modify_sliver_idput

        Modify sliver
        """
        body = 'body_example'
        query_string = [('slice_id', 'slice_id_example')]
        response = self.client.open(
            '//slivers/modify/{sliver_id}'.format(sliver_id='sliver_id_example'),
            method='PUT',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slivers_poa_sliver_idpost(self):
        """Test case for slivers_poa_sliver_idpost

        Perform Operational Action
        """
        body = 'body_example'
        response = self.client.open(
            '//slivers/poa/{sliver_id}'.format(sliver_id='sliver_id_example'),
            method='POST',
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slivers_sliver_idget(self):
        """Test case for slivers_sliver_idget

        slivers properties
        """
        query_string = [('slice_id', 'slice_id_example')]
        response = self.client.open(
            '//slivers/{sliver_id}'.format(sliver_id='sliver_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slivers_status_sliver_idget(self):
        """Test case for slivers_status_sliver_idget

        slivers status
        """
        query_string = [('slice_id', 'slice_id_example')]
        response = self.client.open(
            '//slivers/status/{sliver_id}'.format(sliver_id='sliver_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
