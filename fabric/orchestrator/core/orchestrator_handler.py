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
import traceback

import jwt

from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.common.constants import Constants
from fabric.orchestrator.core.orchestrator_state import OrchestratorStateSingleton


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorStateSingleton.get()
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.token_public_key = GlobalsSingleton.get().get_config().get_oauth_config().get(
            Constants.PropertyConfOAuthTokenPublicKey, None)

    def validate_credentials(self, token) -> str:
        try:
            with open(self.token_public_key) as f:
                key = f.read()

            options = {'verify_aud': False}
            verify = True
            payload = jwt.decode(token, key=key, algorithms='RS256', options=options, verify=verify)
            self.logger.debug(json.dumps(payload))
            return payload
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))

    def discover_types(self, controller: IMgmtActor) -> dict:
        return

    def list_resources(self):
        try:
            self.controller_state.close_dead_slices()
            self.logger.debug("list resources invoked")
            controller = self.controller_state.get_management_actor()

            try:
                abstract_models = self.discover_types(controller)
            except Exception as e:
                self.logger.error("Failed to populate abstract models e: {}".format(e))
                raise e

            if abstract_models is None:
                raise Exception("Failed to populate abstract models")

        except Exception as e:
            self.logger.error("Exception occurred processing list resource e: {}".format(e))
