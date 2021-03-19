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
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType, PdpAuth


class AccessChecker:
    """
    Check access for Incoming operation against Policy Decision Point PDP
    """
    @staticmethod
    def check_access(*, action_id: ActionId, resource_type: ResourceType, token: str,
                     resource_id: str = None, logger=None, actor_type: ActorType) -> FabricToken:
        """
        Check access for Incoming operation against Policy Decision Point PDP
        :param action_id action id
        :param resource_type resource type
        :param token fabric token
        :param resource_id resource id
        :param logger logger
        :param actor_type actor type

        :returns decoded fabric token on success; throws exception in case of failure
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        pdp_config = GlobalsSingleton.get().get_config().get_global_config().get_pdp_config()

        fabric_token = FabricToken(logger=logger, token=token)
        fabric_token.validate()

        pdp_auth = PdpAuth(config=pdp_config, logger=logger)
        pdp_auth.check_access(fabric_token=fabric_token.get_decoded_token(),
                              actor_type=actor_type,
                              action_id=action_id, resource_type=resource_type,
                              resource_id=resource_id)

        return fabric_token
