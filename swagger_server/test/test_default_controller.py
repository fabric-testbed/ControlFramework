# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.actor_response import ActorResponse  # noqa: E501
from swagger_server.test import BaseTestCase


class TestDefaultController(BaseTestCase):
    """DefaultController integration test stubs"""

    def test_create_post(self):
        """Test case for create_post

        creates resource(s)
        """
        body = 'body_example'
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/fabric/actorbase/create',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_delete_delete(self):
        """Test case for delete_delete

        delete resource(s)
        """
        body = 'body_example'
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/fabric/actorbase/delete',
            method='DELETE',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_get(self):
        """Test case for get_get

        get manifest for the resource(s)
        """
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/fabric/actorbase/get',
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_modify_post(self):
        """Test case for modify_post

        modify resource(s)
        """
        body = 'body_example'
        query_string = [('resource_id', 'resource_id_example')]
        response = self.client.open(
            '/fabric/actorbase/modify',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
