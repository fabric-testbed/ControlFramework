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

    def update_subject_category(self, *, subject: dict, token: dict) -> dict:
        """
        Update the Subject Category in PDP request
        @param subject subject
        @param token fabric token
        @return updated subject category
        """
        attributes = subject.get(PdpAuth.attribute, None)
        if attributes is None:
            raise PdpAuthException(self.missing_parameter.format("attributes"))

        roles = self.get_roles(fabric_token=token)

        if len(attributes) > 1:
            raise PdpAuthException("Should only have subject Id Attribute {}".format(subject))

        if attributes[0][PdpAuth.attribute_id] != PdpAuth.subject_id_urn:
            raise PdpAuthException("Should only have subject Id Attribute {}".format(subject))

        attributes[0][PdpAuth.value] = [token[PdpAuth.email]]

        if len(roles) < 1:
            raise PdpAuthException("No roles available in Token")

        for r in roles:
            if r != PdpAuth.co_manage_project_leads_project:
                attr = self.subject_fabric_role_attribute_json.copy()
                attr['Value'] = self.project_member.format(r)
                attributes.append(attr)
            else:
                attr = self.subject_fabric_role_attribute_json.copy()
                attr['Value'] = "projectLead"
                attributes.append(attr)

        return subject

    def update_resource_category(self, *, resource: dict, resource_type: ResourceType, resource_id: str = None) -> dict:
        """
        Update the Resource Category in PDP request
        @param resource resource
        @param resource_type resource type
        @param resource_id resource id
        @return updated Resource category
        """
        attributes = resource.get(PdpAuth.attribute, None)
        if attributes is None:
            raise PdpAuthException(self.missing_parameter.format("attributes"))

        if len(attributes) > 1:
            raise PdpAuthException("Should only have Resource Type Attribute {}".format(resource))

        if attributes[0][PdpAuth.attribute_id] != PdpAuth.resource_type_urn:
            raise PdpAuthException("Should only have Resource Type Attribute {}".format(resource))

        attributes[0][PdpAuth.value] = [resource_type.name]

        if resource_id is not None:
            attr = self.resource_id_attribute_json.copy()
            attr[PdpAuth.value] = resource_id
            attributes.append(attr)

        return resource

    def update_action_category(self, *, action: dict, action_id: ActionId) -> dict:
        """
        Update the Action Category in PDP request
        @param action action
        @param action_id action id
        @return updated Action category
        """
        attributes = action.get(PdpAuth.attribute, None)
        if attributes is None:
            raise PdpAuthException(self.missing_parameter.format("attributes"))

        if len(attributes) > 1:
            raise PdpAuthException("Should only have Action-Id Attribute {}".format(action))

        if attributes[0][PdpAuth.attribute_id] != PdpAuth.action_id_urn:
            raise PdpAuthException("Should only have Action-Id Attribute {}".format(action))

        attributes[0][PdpAuth.value] = [action_id.name]

        return action

    def build_pdp_request(self, *, fabric_token: dict, actor_type: ActorType,
                          action_id: ActionId, resource_type: ResourceType,
                          resource_id: str = None) -> dict:
        """
        Build PDP Request
        @param fabric_token fabric token
        @param actor_type action type
        @param action_id Action id
        @param resource_type resource_type
        @param resource_id resource_id
        @return PDP request
        """
        request_file = None
        if actor_type == ActorType.Orchestrator:
            request_file = os.path.dirname(__file__) + '/data/orchestrator-request.json'
        elif actor_type == ActorType.Broker:
            request_file = os.path.dirname(__file__) + '/data/broker-request.json'
        elif actor_type == ActorType.Authority:
            request_file = os.path.dirname(__file__) + '/data/am-request.json'
        else:
            raise PdpAuthException("Invalid Actor Type: {}".format(actor_type))

        request_json = None
        with open(request_file) as f:
            request_json = json.load(f)
            f.close()

        ## Subject
        categories = request_json[PdpAuth.request][PdpAuth.category]
        for c in categories:
            if c[PdpAuth.category_id] == PdpAuth.category_subject_urn:
                c = self.update_subject_category(subject=c, token=fabric_token)

            elif c[PdpAuth.category_id] == PdpAuth.category_resource_urn:
                c = self.update_resource_category(resource=c, resource_type=resource_type, resource_id=resource_id)

            elif c[PdpAuth.category_id] == PdpAuth.category_action_urn:
                c = self.update_action_category(action=c, action_id=action_id)

            elif c[PdpAuth.category_id] == PdpAuth.category_environment_urn:
                if self.logger is None:
                    print("Do nothing, ignore Environment category")
                else:
                    self.logger.debug("Do nothing, ignore Environment category")

            else:
                raise PdpAuthException("Invalid Category: {}".format(c))

        request_json[PdpAuth.request][PdpAuth.category] = categories

        return request_json

    def check_access(self, *, fabric_token: dict, actor_type: ActorType,
                     action_id: ActionId, resource_type: ResourceType,
                     resource_id: str = None):
        """
        Check Access
        @param fabric_token fabric token
        @param actor_type actor type
        @param action_id action id
        @param resource_type resource type
        @param resource_id resource id
        @raises PdpAuthException in case of denied access or failure
        """
        if not self.config['enable']:
            self.logger.debug("Skipping PDP Authorization check as configured")
            return

        pdp_request = self.build_pdp_request(fabric_token=fabric_token, actor_type=actor_type,
                                             action_id=action_id, resource_type=resource_type, resource_id=resource_id)

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
             "scope": "all", "project": "all"}

    config = {'url': 'http://localhost:8080/services/pdp'}
    pdp = PdpAuth(config=config)
    RESULT = pdp.check_access(fabric_token=token, actor_type=ActorType.Orchestrator,
                              action_id=ActionId.query, resource_type=ResourceType.resources)
    print(RESULT)
