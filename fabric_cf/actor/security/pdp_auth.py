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
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import List

import requests
from fim.slivers.base_sliver import BaseSliver
from fim.user.topology import ExperimentTopology
from fss_utils.jwt_validate import JWTValidator

from fim.authz.attribute_collector import ResourceAuthZAttributes

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.security.fabric_token import FabricToken


class PdpAuthException(Exception):
    """
    PDP Auth Exception
    """


class ResourceType(Enum):
    """
    Resource Type Enumeration
    """
    user = 1
    slice = 2
    sliver = 3
    resources = 4
    delegation = 5


class ActionId(Enum):
    """
    Action Id Enumeration
    """
    noop = 0
    query = 1
    status = 2
    create = 3
    redeem = 4
    delete = 5
    modify = 6
    POA = 7
    renew = 8
    demand = 9
    update = 10
    close = 11
    claim = 12
    reclaim = 13
    ticket = 14
    extend = 15
    relinquish = 16
    accept = 17

    def __str__(self):
        return self.name


class PdpAuth:
    """
    Responsible for Authorization against PDP
    """

    def __init__(self, *, config: dict, logger=None):
        self.config = config
        self.logger = logger

    @staticmethod
    def _headers() -> dict:
        """
        Returns Headers for REST APIs
        """
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        return headers

    def build_pdp_request(self, *, email: str, project: str, tags: List[str], action_id: ActionId,
                          resource: BaseSliver or ExperimentTopology, lease_end_time: datetime) -> dict:
        """
        Build PDP Request
        @param email email
        @param project project
        @param tags project tags
        @param action_id Action id
        @param resource: sliver of any type or slice (ExperimentTopology)
        @param lease_end_time lease end time
        @return PDP request
        """
        if project is None or email is None:
            raise PdpAuthException("No project found in fabric token")

        attrs = ResourceAuthZAttributes()
        # collect all resource attributes
        if resource is not None:
            attrs.collect_resource_attributes(source=resource)

        # additional attributes (slice end date in datetime format)
        if lease_end_time:
            attrs.set_lifetime(lease_end_time)

        # next we need to set the owner of the resource and their projects
        # generally only the id is needed. If action is create, it's not needed at all

        attrs.set_subject_attributes(subject_id=email, project=project, project_tag=tags)

        attrs.set_resource_subject_and_project(subject_id=email, project=project)

        # finally action
        # action can be any string matching ActionId enum
        attrs.set_action(action_id.name)

        # now you can produce the json
        request_json = attrs.transform_to_pdp_request(as_json=False)

        return request_json

    def check_access(self, *, email: str, project: str, tags: List[str], action_id: ActionId,
                     resource: BaseSliver or ExperimentTopology, lease_end_time: datetime):
        """
        Check Access
        @param email email
        @param project project
        @param tags project tags
        @param action_id action id
        @param resource sliver (of any type) or slice (ExperimentTopology)
        @param lease_end_time lease end time
        @raises PdpAuthException in case of denied access or failure
        """
        if not self.config['enable']:
            self.logger.debug("Skipping PDP Authorization check as configured")
            return

        pdp_request = self.build_pdp_request(email=email, project=project, tags=tags, action_id=action_id,
                                             resource=resource, lease_end_time=lease_end_time)

        self.logger.debug("PDP Auth Request: {}".format(pdp_request))

        # send request to PDP
        response = requests.post(url=self.config['url'], headers=self._headers(), json=pdp_request)
        response_json = response.json()

        if response.status_code == 200 and response_json["Response"][0]["Decision"] == "Permit":
            self.logger.debug("PDP response: {}".format(response_json))
        else:
            self.logger.error("PDP response: {}".format(response_json))
            msg = response_json["Response"][0]["AssociatedAdvice"][0]["AttributeAssignment"][0]["Value"]
            raise PdpAuthException(f"PDP Authorization check failed - {msg}")


if __name__ == '__main__':
    oauth_config = {
        "jwks-url": "https://alpha-2.fabric-testbed.net/certs",
        "key-refresh": "00:10:00",
        "verify-exp": False
    }
    CREDMGR_CERTS = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
    CREDMGR_KEY_REFRESH = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_KEY_REFRESH, None)
    t = datetime.strptime(CREDMGR_KEY_REFRESH, "%H:%M:%S")
    jwt_validator = JWTValidator(url=CREDMGR_CERTS,
                                 refresh_period=timedelta(hours=t.hour, minutes=t.minute, seconds=t.second))
    logger = logging.getLogger(__name__)
    token = FabricToken(oauth_config=oauth_config, jwt_validator=jwt_validator, logger=logger,
                        token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImI0MTUxNjcyMTExOTFlMmUwNWIyMmI1NGIxZDNiNzY2N2U3NjRhNzQ3NzIyMTg1ZTcyMmU1MmUxNDZmZTQzYWEifQ.eyJlbWFpbCI6Imt0aGFyZTEwQGVtYWlsLnVuYy5lZHUiLCJnaXZlbl9uYW1lIjoiS29tYWwiLCJmYW1pbHlfbmFtZSI6IlRoYXJlamEiLCJuYW1lIjoiS29tYWwgVGhhcmVqYSIsImlzcyI6Imh0dHBzOi8vY2lsb2dvbi5vcmciLCJzdWIiOiJodHRwOi8vY2lsb2dvbi5vcmcvc2VydmVyQS91c2Vycy8xMTkwNDEwMSIsImF1ZCI6ImNpbG9nb246L2NsaWVudF9pZC82MTdjZWNkZDc0ZTMyYmU0ZDgxOGNhMTE1MTUzMWRmZiIsImp0aSI6Imh0dHBzOi8vY2lsb2dvbi5vcmcvb2F1dGgyL2lkVG9rZW4vMjViM2ExMmM4YWIzODNhODcyMjNiOTA3YmRhNDA1MGMvMTY1MDM5NTI4ODA5OSIsImF1dGhfdGltZSI6MTY1MDM5NTI4NywiZXhwIjoxNjUwMzk4OTIxLCJpYXQiOjE2NTAzOTUzMjEsInByb2plY3RzIjp7IkNGIFRlc3QiOltdfSwicm9sZXMiOlsiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBjIiwiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBtIiwiMTBjMDA5NGEtYWJhZi00ZWY5LWE1MzItMmJlNTNlMmE4OTZiLXBvIiwiZmFicmljLWFjdGl2ZS11c2VycyIsInByb2plY3QtbGVhZHMiLCJwcm9qZWN0LWxlYWRzIl0sInNjb3BlIjoiYWxsIn0.v24LY2gfBjJPeWy-xXq0ViTguFRmZnv9NQUeqIEYvkWaL4V2qN9IKfatnDaoug7JBF8Xb2jQ0dQf_onnm2yYybWqXy-8ELZ8SZS8LBq3k0yyiE8vm6aAdmglMaLu6R3CIo3FncFBKNFeb_s0brEhngirsGA2lwNDf-Bi5ucHXFNSVDzmAcVopFSBcbwo78p3rRbzR5pjNpFrAT3CJRwRGv1-NvGUZvt10Z7s0KT2HEnNkanZWG02ck7H40HHr1O89svh8jl0ze4wgi9iYscYC0BZ74jBu9wntnty5hubowZ5sOuJZAFtYUB3Z3-W8sVeg_vHqMbPlpoIRYzwiby3SCGIJ7DgqNq8-18T6Z4ZxOAOB74PNJEArWq2Ti7nmL0zxI68wSGNqT0rLZo9p1UUYvf8qCsdYUKmVZD8xRea2FwyZEB8MyIQ5FRWOP2AKN3kCo1K89XpJY5iZrMxRtC7obc41wanZCiEhmEK1pLFDkIYrjNmpNQ0mQ9pMKIXCTKXRFgkMNN5vsz0uT797SNPKsFkKvBz7SBh2gAerpCDivCwoMpEPGTvJp_GqohyFjSkvjJ7n5vxWKiwqzU2wRG23tCi5xqqk30u4R6e7oU7IKBwrdpGHK23q-Laa1mvKL9CZ98Yngs3-S-rZVlTtT_y1UZHFWYOmPFzo0xlUTs5wak")
    token.validate()

    config = {'url': 'http://localhost:8080/services/pdp', 'enable': True}
    pdp = PdpAuth(config=config, logger=logger)
    projects = token.get_projects()
    for p in projects:
        pdp.check_access(email=token.get_email(), project=p.get(Constants.UUID), tags=p.get(Constants.TAGS),
                         action_id=ActionId.query, resource=None, lease_end_time=None)
