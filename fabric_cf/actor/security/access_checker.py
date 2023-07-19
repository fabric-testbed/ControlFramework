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
from datetime import datetime
from typing import List, Union

from fim.slivers.base_sliver import BaseSliver
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.actor.security.pdp_auth import ActionId, PdpAuth


class AccessChecker:
    """
    Check access for Incoming operation against Policy Decision Point PDP
    """
    @staticmethod
    def validate_and_decode_token(*, token: str) -> Union[FabricToken, None]:
        if token is None:
            return token
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        oauth_config = GlobalsSingleton.get().get_config().get_global_config().get_oauth()
        token_validator = GlobalsSingleton.get().get_token_validator()
        verify_exp = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_VERIFY_EXP, True)
        return token_validator.validate_token(token=token, verify_exp=verify_exp)

    @staticmethod
    def check_access(*, action_id: ActionId, token: str,
                     resource: BaseSliver or ExperimentTopology = None,
                     lease_end_time: datetime = None, logger=None) -> Union[FabricToken, None]:
        """
        Validates Fabric Token and Check access for Incoming operation against Policy Decision Point PDP
        :param action_id action id
        :param token fabric token
        :param resource resource
        :param lease_end_time lease end time
        :param logger logger

        :returns decoded fabric token on success; throws exception in case of failure
        """
        if token is None:
            return token
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        oauth_config = GlobalsSingleton.get().get_config().get_global_config().get_oauth()
        token_validator = GlobalsSingleton.get().get_token_validator()

        verify_exp = oauth_config.get(Constants.PROPERTY_CONF_O_AUTH_VERIFY_EXP, True)
        decoded_token = token_validator.validate_token(token=token, verify_exp=verify_exp)

        for p in decoded_token.projects:
            AccessChecker.check_pdp_access(action_id=action_id, email=decoded_token.email,
                                           project=p.get(Constants.UUID), tags=p.get(Constants.TAGS),
                                           resource=resource, lease_end_time=lease_end_time, logger=logger)

        return decoded_token

    @staticmethod
    def check_pdp_access(*, action_id: ActionId, email: str, project: str, tags: List[str],
                         resource: BaseSliver or ExperimentTopology = None,
                         lease_end_time: datetime = None, logger=None):
        """
        Check access for Incoming operation against Policy Decision Point PDP
        :param action_id action id
        :param email email
        :param project project
        :param tags tags
        :param resource resource
        :param lease_end_time lease end time
        :param logger logger

        :throws exception in case of failure
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        pdp_config = GlobalsSingleton.get().get_config().get_global_config().get_pdp_config()

        pdp_auth = PdpAuth(config=pdp_config, logger=logger)
        pdp_auth.check_access(email=email, project=project, tags=tags,
                              lease_end_time=lease_end_time,
                              action_id=action_id, resource=resource)
