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

from fim.slivers.base_sliver import BaseSliver
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType, PdpAuth


class AccessChecker:
    """
    Check access for Incoming operation against Policy Decision Point PDP
    """
    @staticmethod
    def check_access(*, action_id: ActionId, token: str,
                     resource: BaseSliver or ExperimentTopology = None,
                     lease_end_time: datetime = None, logger=None) -> FabricToken or None:
        """
        Check access for Incoming operation against Policy Decision Point PDP
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
        pdp_config = GlobalsSingleton.get().get_config().get_global_config().get_pdp_config()
        oauth_config = GlobalsSingleton.get().get_config().get_global_config().get_oauth()
        jwt_validator = GlobalsSingleton.get().get_jwt_validator()

        fabric_token = FabricToken(oauth_config=oauth_config, jwt_validator=jwt_validator, logger=logger, token=token)
        fabric_token.validate()

        pdp_auth = PdpAuth(config=pdp_config, logger=logger)
        pdp_auth.check_access(fabric_token=fabric_token,
                              lease_end_time=lease_end_time,
                              action_id=action_id, resource=resource)

        return fabric_token
