#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)
"""
Unit tests for PdpAuth.check_access — the PDP authorization path.

These tests mock the outbound HTTP call (requests.post) and the request-builder
so they exercise only the response-handling / decision logic. They require no
running PDP or other infrastructure.
"""
import unittest
from unittest import mock

from fabric_cf.actor.security.pdp_auth import PdpAuth, PdpAuthException, ActionId

PDP_MODULE = "fabric_cf.actor.security.pdp_auth"


def _permit_response():
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {"Response": [{"Decision": "Permit"}]}
    return resp


def _deny_response(reason: str = "not allowed"):
    resp = mock.Mock()
    resp.status_code = 200
    resp.json.return_value = {
        "Response": [{
            "Decision": "Deny",
            "AssociatedAdvice": [{
                "AttributeAssignment": [{"Value": reason}]
            }]
        }]
    }
    return resp


class TestPdpAuthCheckAccess(unittest.TestCase):
    def _make(self, enable: bool = True):
        config = {"enable": enable, "url": "http://pdp.example.invalid/authorize"}
        return PdpAuth(config=config, logger=mock.Mock())

    @mock.patch(f"{PDP_MODULE}.requests.post")
    def test_disabled_skips_http_call(self, mock_post):
        """When PDP is disabled, check_access must return without any HTTP call."""
        pdp = self._make(enable=False)
        pdp.check_access(email="u@example.org", project="p", tags=[],
                         action_id=ActionId.create, resource=None, lease_end_time=None)
        mock_post.assert_not_called()

    @mock.patch(f"{PDP_MODULE}.requests.post")
    def test_permit_returns_without_raising(self, mock_post):
        mock_post.return_value = _permit_response()
        pdp = self._make(enable=True)
        with mock.patch.object(pdp, "build_pdp_request", return_value={}):
            # Should not raise
            pdp.check_access(email="u@example.org", project="p", tags=[],
                             action_id=ActionId.create, resource=None, lease_end_time=None)
        mock_post.assert_called_once()

    @mock.patch(f"{PDP_MODULE}.requests.post")
    def test_deny_raises_with_reason(self, mock_post):
        mock_post.return_value = _deny_response(reason="quota exceeded")
        pdp = self._make(enable=True)
        with mock.patch.object(pdp, "build_pdp_request", return_value={}):
            with self.assertRaises(PdpAuthException) as ctx:
                pdp.check_access(email="u@example.org", project="p", tags=[],
                                 action_id=ActionId.create, resource=None, lease_end_time=None)
        self.assertIn("quota exceeded", str(ctx.exception))

    @mock.patch(f"{PDP_MODULE}.requests.post")
    def test_non_json_response_raises(self, mock_post):
        resp = mock.Mock()
        resp.status_code = 200
        resp.json.side_effect = ValueError("no json")
        resp.text = "<html>gateway error</html>"
        mock_post.return_value = resp
        pdp = self._make(enable=True)
        with mock.patch.object(pdp, "build_pdp_request", return_value={}):
            with self.assertRaises(PdpAuthException):
                pdp.check_access(email="u@example.org", project="p", tags=[],
                                 action_id=ActionId.create, resource=None, lease_end_time=None)

    @mock.patch(f"{PDP_MODULE}.requests.post")
    def test_http_error_is_wrapped(self, mock_post):
        mock_post.side_effect = Exception("connection refused")
        pdp = self._make(enable=True)
        with mock.patch.object(pdp, "build_pdp_request", return_value={}):
            with self.assertRaises(PdpAuthException):
                pdp.check_access(email="u@example.org", project="p", tags=[],
                                 action_id=ActionId.create, resource=None, lease_end_time=None)


class TestPdpAuthBuildRequest(unittest.TestCase):
    def test_build_request_requires_project_and_email(self):
        pdp = PdpAuth(config={"enable": True, "url": "http://x.invalid"}, logger=mock.Mock())
        with self.assertRaises(PdpAuthException):
            pdp.build_pdp_request(email=None, project=None, tags=[],
                                  action_id=ActionId.create, resource=None, lease_end_time=None)


if __name__ == "__main__":
    unittest.main()
