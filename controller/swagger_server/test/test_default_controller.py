#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from controller.swagger_server.models.error import Error  # noqa: E501
from controller.swagger_server.models.success import Success  # noqa: E501
from controller.swagger_server.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_create_post(self):
        """Test case for create_post

        Create slice
        """
        body = 'body_example'
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/create',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_delete(self):
        """Test case for delete_delete

        Delete slice or sliver.
        """
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/delete',
            method='DELETE',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_manifest_get(self):
        """Test case for get_manifest_get

        Retrieve manifest for the slice or sliver
        """
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/getManifest',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_version_get(self):
        """Test case for get_version_get

        Retrieve the current Controller version and list of versions of the API supported. 
        """
        response = self.client.open(
            '/kthare10/controller/1.0.0/getVersion',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_resources_get(self):
        """Test case for list_resources_get

        Retrieve a listing and description of available resources at various sites. 
        """
        response = self.client.open(
            '/kthare10/controller/1.0.0/listResources',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_modify_post(self):
        """Test case for modify_post

        Modify slice or sliver
        """
        body = 'body_example'
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/modify',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_query_resources_post(self):
        """Test case for query_resources_post

        Query resource(s)
        """
        body = 'body_example'
        response = self.client.open(
            '/kthare10/controller/1.0.0/queryResources',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_renew_post(self):
        """Test case for renew_post

        Renew slice or sliver
        """
        query_string = [('resource_id', 'resource_id_example'),
                        ('new_lease_end_time', 'new_lease_end_time_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/renew',
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_status_get(self):
        """Test case for status_get

        Retrieve status of a slice or sliver(s)
        """
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/kthare10/controller/1.0.0/status',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
