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
    poa = 18

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
        try:
            response = requests.post(url=self.config['url'], headers=self._headers(), json=pdp_request)

            try:
                response_json = response.json()
            except ValueError:
                # Handle non-JSON response
                self.logger.error(f"Non-JSON response from PDP: {response.text}")
                raise PdpAuthException("PDP returned a non-JSON response")

            if response.status_code == 200 and response_json.get("Response", [{}])[0].get("Decision") == "Permit":
                self.logger.debug("PDP response: {}".format(response_json))
            else:
                self.logger.error("PDP response: {}".format(response_json))
                msg = (
                    response_json.get("Response", [{}])[0]
                        .get("AssociatedAdvice", [{}])[0]
                        .get("AttributeAssignment", [{}])[0]
                        .get("Value", "Unknown reason")
                )
                raise PdpAuthException(f"PDP Authorization check failed - {msg}")

        except Exception as e:
            self.logger.error(f"Request to PDP failed: {e}")
            raise PdpAuthException(f"PDP Failure: {e}") from e
