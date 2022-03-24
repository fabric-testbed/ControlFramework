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
import os
from enum import Enum
from typing import List

import requests

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fim.authz.attribute_collector import ResourceAuthZAttributes


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


class PdpAuth:
    """
    Responsible for Authorization against PDP
    """
    co_manage_project_leads_project = 'project-leads'
    project_lead_role = 'projectLead'

    request = 'Request'
    attribute = 'Attribute'
    attribute_id = 'AttributeId'
    category = 'Category'
    category_id = 'CategoryId'
    value = 'Value'
    email = 'email'
    subject_id_urn = 'urn:oasis:names:tc:xacml:1.0:subject:subject-id'
    category_subject_urn = 'urn:oasis:names:tc:xacml:1.0:subject-category:access-subject'
    category_resource_urn = 'urn:oasis:names:tc:xacml:3.0:attribute-category:resource'
    resource_type_urn = 'urn:fabric:xacml:attributes:resource-type'
    resource_id_urn = 'urn:oasis:names:tc:xacml:1.0:resource:resource-id'
    category_action_urn = 'urn:oasis:names:tc:xacml:3.0:attribute-category:action'
    action_id_urn = 'urn:oasis:names:tc:xacml:1.0:action:action-id'
    category_environment_urn = 'urn:oasis:names:tc:xacml:3.0:attribute-category:environment'

    missing_parameter = "Missing {}"

    subject_fabric_role_attribute_json = {
        "IncludeInResult": False,
        "AttributeId": "urn:fabric:xacml:attributes:fabric-role",
        "DataType": "http://www.w3.org/2001/XMLSchema#string",
        "Value": ["projectMember:project-X"]
    }

    resource_id_attribute_json = {
        "IncludeInResult": False,
        "AttributeId": "urn:oasis:names:tc:xacml:1.0:resource:resource-id",
        "DataType": "http://www.w3.org/2001/XMLSchema#string",
        "Value": ["some-delegation"]
    }

    def __init__(self, *, config: dict, logger=None):
        self.roles_re = 'CO:COU:(.*):members:active'
        self.project_member = "projectMember:{}"
        self.config = config
        self.logger = logger

    @staticmethod
    def _headers() -> dict:
        """
        Returns Headers for REST APIs
        """
        headers = {
            'Content-Type': "application/json"
        }
        return headers

    def get_roles(self, *, fabric_token: dict) -> List[str]:
        """
        Get Roles from a fabric token
        @param fabric_token fabric token
        @return list of the roles
        """
        roles = fabric_token.get('roles', None)
        if roles is None:
            raise PdpAuthException(self.missing_parameter.format("roles"))

        return roles

    def build_pdp_request(self, *, fabric_token: dict, actor_type: ActorType,
                          action_id: ActionId, resource_type: ResourceType,
                          resource) -> dict:
        """
        Build PDP Request
        @param fabric_token fabric token
        @param actor_type action type
        @param action_id Action id
        @param resource_type resource_type
        @param resource: sliver of any type or slice (ExperimentTopology)
        @return PDP request
        """

        #
        # we ignore the actor type for now
        #

        # policies only deal with slivers/slices and this is the type to use
        assert resource_type == ResourceType.sliver

        attrs = ResourceAuthZAttributes()
        # collect all resource attributes
        attrs.collect_resource_attributes(source=resource)

        # FIXME: Komal here we need more attributes that I don't know where to get
        # FIXME: some of them not needed for anything other than create

        # additional attributes (slice end date in datetime format)
        # attrs.set_lifetime(end_date)

        # next we need to set the owner of the resource and their projects
        # generally only the id is needed. If action is create, it's not needed at all
        # attrs.set_resource_subject_and_project(subject_id="user@gmail.com", project="Project1")

        # next set subject attributes - their id, projects, project tags (tags should be collected
        # from the token)
        # attrs.set_subject_attributes(subject_id="user@gmail.com", project=["Project1", "Project2"],
        # project_tag=["Tag1", "Tag2"])

        # finally action
        # action can be any string matching ActionId enum
        attrs.set_action(action_id.name)

        # now you can produce the json
        request_json = attrs.transform_to_pdp_request()

        return request_json

    def check_access(self, *, fabric_token: dict, actor_type: ActorType,
                     action_id: ActionId, resource_type: ResourceType,
                     resource):
        """
        Check Access
        @param fabric_token fabric token
        @param actor_type actor type
        @param action_id action id
        @param resource_type resource type (should only be ResourceType.sliver)
        @param resource sliver (of any type) or slice (ExperimentTopology)
        @raises PdpAuthException in case of denied access or failure
        """
        if not self.config['enable']:
            self.logger.debug("Skipping PDP Authorization check as configured")
            return

        pdp_request = self.build_pdp_request(fabric_token=fabric_token, actor_type=actor_type,
                                             action_id=action_id, resource_type=resource_type,
                                             resource=resource)

        self.logger.debug("PDP Auth Request: {}".format(pdp_request))

        response = requests.post(url=self.config['url'], headers=self._headers(), json=pdp_request)

        if response.status_code != 200:
            raise PdpAuthException('Authorization check failure: {}'.format(response))

        if response.json()["Response"][0]["Decision"] == "Permit":
            if self.logger is not None:
                self.logger.debug("PDP response: {}".format(response.json()))
        else:
            if self.logger is not None:
                self.logger.debug("PDP response: {}".format(response.json()))
            raise PdpAuthException('Authorization check failure: {}'.format(response.json()))


if __name__ == '__main__':
    token = {"email": "kthare10@email.unc.edu",
             "given_name": "Komal",
             "family_name": "Thareja", "name": "Komal Thareja",
             "iss": "https://cilogon.org", "sub": "http://cilogon.org/serverA/users/11904101",
             "aud": "cilogon:/client_id/1253defc60a323fcaa3b449326476099",
             "token_id": "https://cilogon.org/oauth2/idToken/156747336e2a3fbc1d66cc8fe1571d91/1603986888019",
             "auth_time": "1603986887", "exp": 1603990493, "iat": 1603986893,
             "roles": ["CO:members:active", "CO:COU:Jupyterhub:members:active", "CO:COU:project-leads:members:active"],
             "projects": ["Project1"],
             "scope": "all", "project": "all"}

    config = {'url': 'http://localhost:8080/services/pdp'}
    pdp = PdpAuth(config=config)
    RESULT = pdp.check_access(fabric_token=token, actor_type=ActorType.Orchestrator,
                              action_id=ActionId.query, resource_type=ResourceType.sliver)
    print(RESULT)
