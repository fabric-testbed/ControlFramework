# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from fabric_cf.orchestrator.swagger_server.models.slice_details import SliceDetails  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slices import Slices  # noqa: E501
from fabric_cf.orchestrator.swagger_server.models.slices_post import SlicesPost  # noqa: E501
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
    TOKEN = "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImI0MTUxNjcyMTExOTFlMmUwNWIyMmI1NGIxZDNiNzY2N2U3NjRhNzQ3NzIyMTg1ZTcyMmU1MmUxNDZmZTQzYWEiLCJ0eXAiOiJKV1QifQ.eyJhY3IiOiJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6YWM6Y2xhc3NlczpQYXNzd29yZFByb3RlY3RlZFRyYW5zcG9ydCIsImVtYWlsIjoia3RoYXJlMTBAZW1haWwudW5jLmVkdSIsImdpdmVuX25hbWUiOiJLb21hbCIsImZhbWlseV9uYW1lIjoiVGhhcmVqYSIsIm5hbWUiOiJLb21hbCBUaGFyZWphIiwiaXNzIjoiaHR0cHM6Ly9jaWxvZ29uLm9yZyIsInN1YiI6Imh0dHA6Ly9jaWxvZ29uLm9yZy9zZXJ2ZXJBL3VzZXJzLzExOTA0MTAxIiwiYXVkIjoiY2lsb2dvbjovY2xpZW50X2lkLzYxN2NlY2RkNzRlMzJiZTRkODE4Y2ExMTUxNTMxZGZmIiwianRpIjoiaHR0cHM6Ly9jaWxvZ29uLm9yZy9vYXV0aDIvaWRUb2tlbi8yNjg0N2EzODcwN2Y5YTk4Y2RmYjEwYjY5OThiNzBkLzE2ODkyNjkwNjU3OTIiLCJhdXRoX3RpbWUiOjE2ODkyNjc4MzgsImV4cCI6MTY4OTI4MzQ2OSwiaWF0IjoxNjg5MjY5MDY5LCJwcm9qZWN0cyI6W3sibmFtZSI6IkNGIFRlc3QiLCJ1dWlkIjoiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiIiwidGFncyI6WyJDb21wb25lbnQuR1BVIiwiQ29tcG9uZW50LlN0b3JhZ2UiXSwibWVtYmVyc2hpcHMiOnsiaXNfY3JlYXRvciI6dHJ1ZSwiaXNfbWVtYmVyIjp0cnVlLCJpc19vd25lciI6dHJ1ZX19XSwicm9sZXMiOlt7ImRlc2NyaXB0aW9uIjoiQ0YgVGVzdCBhbmQgdGVzdCBhbmQgdGVzdCIsIm5hbWUiOiIxMGMwMDk0YS1hYmFmLTRlZjktYTUzMi0yYmU1M2UyYTg5NmItcGMifSx7ImRlc2NyaXB0aW9uIjoiQ0YgVGVzdCBhbmQgdGVzdCBhbmQgdGVzdCIsIm5hbWUiOiIxMGMwMDk0YS1hYmFmLTRlZjktYTUzMi0yYmU1M2UyYTg5NmItcG0ifSx7ImRlc2NyaXB0aW9uIjoiQ0YgVGVzdCBhbmQgdGVzdCBhbmQgdGVzdCIsIm5hbWUiOiIxMGMwMDk0YS1hYmFmLTRlZjktYTUzMi0yYmU1M2UyYTg5NmItcG8ifSx7ImRlc2NyaXB0aW9uIjoiRkFCUklDIFByb2plY3QiLCJuYW1lIjoiOGIzYTJlYWUtYTBjMC00NzVhLTgwN2ItZTlhZjU4MWNlNGMwLXBtIn0seyJkZXNjcmlwdGlvbiI6IlByb2plY3QtVGFncyIsIm5hbWUiOiJiOGQ2NmZiMy1lN2FkLTRkZjktYTY1Mi0yZDhlMzIzMjM2ZTUtcGMifSx7ImRlc2NyaXB0aW9uIjoiUHJvamVjdC1UYWdzIiwibmFtZSI6ImI4ZDY2ZmIzLWU3YWQtNGRmOS1hNjUyLTJkOGUzMjMyMzZlNS1wbyJ9LHsiZGVzY3JpcHRpb24iOiJBY3RpdmUgVXNlcnMgb2YgRkFCUklDIC0gaW5pdGlhbGx5IHNldCBieSBlbnJvbGxtZW50IHdvcmtmbG93IiwibmFtZSI6ImZhYnJpYy1hY3RpdmUtdXNlcnMifSx7ImRlc2NyaXB0aW9uIjoiRmFjaWxpdHkgT3BlcmF0b3JzIGZvciBGQUJSSUMiLCJuYW1lIjoiZmFjaWxpdHktb3BlcmF0b3JzIn0seyJkZXNjcmlwdGlvbiI6IiBKdXB5dGVyaHViIGFjY2VzcyAtIGJhc2VkIG9uIHByb2plY3QgcGFydGljaXBhdGlvbiIsIm5hbWUiOiJKdXB5dGVyaHViIn0seyJkZXNjcmlwdGlvbiI6IlByb2plY3QgTGVhZHMgZm9yIEZBQlJJQyIsIm5hbWUiOiJwcm9qZWN0LWxlYWRzIn1dLCJzY29wZSI6ImFsbCIsInV1aWQiOiIyMjY1YTRkYS1lMWZjLTRmZmMtYmJiNy1kODZkNTY1NzViZmYifQ.OG_m48gCzMt8vcYv947cZduJSbEEzpMuBbbP59SgfD7q1q4_kMbBg36P_SmZMaRfZ6P65hfEdRSnX1x5i4OJQkE47sw1P0Uge6klcnfORIwUyMNphNoKgUcbLRZN9Kch6xKr6JPEfnXKnTdTUMS2-cKzmgLa5w2oIH8kipKisHey7PQhwfdZhwqR8hme5Q7Gwf4O3n45jA8HQQxaO38scIOy0NajQOMPvOkQvfzWOOv79mSztepN2jlJRMGWLqdBql5kSm_xtVRk3S3SdLuS_GTKyA4zfU1-ouT-NUlE9CM86mBHy4FYki5M-cnM3Us3RI7oAxal8kAGqB2I0LY-xMkhHY6k34FCCHiFWvH4WgYmWBEWO7U8uPUq1rbapX6NluQJaSxc54UEA33B9pC_Cgkod6UzB_7g_CNmS31wajjyulCEjIBtlBWTXdmblG8PEZA0_T8DrXh7NPPxF3XrRC74d-IqQ9WiTuH6qrOrMT3ivpeZqPFdriKOXB49EREmUV1iO-STWXRG1RDBLM-lGW7dWOfgsNmlEvW49A8ZO6-T7xVYyVbsVB0XF9oRKAr3uMt_N9ZgF_BP3CKzIi5bhvr4_0AyFifgbrkaKb4qIMOj0XD-Ds1cCDBASqiIj0peal9M8BlBDDMY_80MkRxH1i-XvLRYU5s9t3Oc3p1yd4U"

    def test_slices_create_post(self):
        """Test case for slices_create_post

        Create slice
        """
        body = 'body_example'
        query_string = [('name', 'name_example'),
                        ('ssh_key', 'ssh_key_example'),
                        ('lease_end_time', 'lease_end_time_example')]
        response = self.client.open(
            '/slices/create',
            method='POST',
            data=json.dumps(body),
            content_type='text/plain',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_creates_post(self):
        """Test case for slices_creates_post

        Create slice
        """
        body = SlicesPost()
        query_string = [('name', 'name_example'),
                        ('lease_end_time', 'lease_end_time_example')]
        response = self.client.open(
            '/slices/creates',
            method='POST',
            data=json.dumps(body),
            content_type='application/json',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_delete_delete(self):
        """Test case for slices_delete_delete

        Delete all slices for a User within a project.
        """
        response = self.client.open(
            '/slices/delete',
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_delete_slice_id_delete(self):
        """Test case for slices_delete_slice_id_delete

        Delete slice.
        """
        response = self.client.open(
            '/slices/delete/{slice_id}'.format(slice_id='slice_id_example'),
            method='DELETE', )
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_get(self):
        """Test case for slices_get

        Retrieve a listing of user slices
        """
        query_string = [('name', 'name_example'),
                        ('as_self', True),
                        #('states', 'states_example'),
                        ('limit', 200),
                        ('offset', 1),
                        # ('search', 'search_example'),
                        # ('exact_match', False),
                        ]
        response = self.client.open(
            '/slices',
            method='GET',
            query_string=query_string, headers={'AUTHORIZATION': self.TOKEN}
        )
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_modify_slice_id_accept_post(self):
        """Test case for slices_modify_slice_id_accept_post

        Accept the last modify an existing slice
        """
        response = self.client.open(
            '/slices/modify/{slice_id}/accept'.format(slice_id='slice_id_example'),
            method='POST')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_modify_slice_id_put(self):
        """Test case for slices_modify_slice_id_put

        Modify an existing slice
        """
        body = 'body_example'
        response = self.client.open(
            '/slices/modify/{slice_id}'.format(slice_id='slice_id_example'),
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
            '/slices/renew/{slice_id}'.format(slice_id='slice_id_example'),
            method='POST',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_slices_slice_id_get(self):
        """Test case for slices_slice_id_get

        slice properties
        """
        query_string = [('as_self', True),
                        ('graph_format', 'GRAPHML')]
        response = self.client.open(
            '/slices/{slice_id}'.format(slice_id='slice_id_example'),
            method='GET',
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
