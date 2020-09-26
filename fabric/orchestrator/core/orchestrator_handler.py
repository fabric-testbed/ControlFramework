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
from fabric.actor.core.manage.converter import Converter
from fabric.actor.core.util.id import ID
from fabric.orchestrator.core.orchestrator_state import OrchestratorStateSingleton
from fabric.orchestrator.core.site_resource_type import SiteResourceType
from fabric.orchestrator.core.site_resource_types import SiteResourceTypes


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorStateSingleton.get()
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.token_public_key = GlobalsSingleton.get().get_config().get_oauth_config().get(
            Constants.PropertyConfOAuthTokenPublicKey, None)
        self.types_map = {}

    def validate_credentials(self, *, token) -> str:
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

    def get_broker(self, *, controller: IMgmtActor) -> ID:
        try:
            brokers = controller.get_brokers()
            if brokers is not None:
                return ID(id=next(iter(brokers), None).get_guid())

        except Exception as e:
            self.logger.debug(traceback.format_exc())

        return None

    def discover_types(self, *, controller: IMgmtActor) -> dict:
        broker = self.get_broker(controller=controller)
        if broker is None:
            raise Exception("Unable to determine broker proxy for this controller. Please check SM container configuration and logs.")

        self.controller_state.set_broker(broker=str(broker))

        my_pools = controller.get_pool_info(broker=broker)
        if my_pools is None:
            raise Exception("Could not discover types: {}".format(controller.get_last_error()))

        pools = {}
        for p in my_pools:
            try:
                rpd = Converter.fill_resource_pool_descriptor(pool=p)
                rtype = rpd.get_resource_type()
                # TODO
                '''
                attr = rpd.get_attribute(key=Constants.ResourceDomain)
                if attr is None:
                    raise Exception("Missing domain information for resource pool: {}".format(rtype))
                pools[rtype] = rpd
                domain = attr.get_value()
                domain_resources = SiteResourceTypes(domain=domain)
                if domain not in self.types_map:
                    self.types_map[domain] = domain_resources
                '''

                drt = SiteResourceType(rtype=rtype)
                attr = rpd.get_attribute(key=Constants.ResourceAvailableUnits)
                total = 0
                if attr is not None:
                    total = attr.get_int_value()
                    drt.set_available_units(available_units=total)

                self.logger.debug("rt: {} available units: {}".format(drt.get_resource_type(),
                                                                      drt.get_available_units()))

                self.types_map[rtype] = drt
                # TODO
                #self.logger.debug("domain: {} rt: {} available units: {}".format(domain, drt.get_resource_type(),
                #                                                                 drt.get_available_units()))
                #domain_resources.add_resource(resource=drt)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.debug("Could not process discover types response {}".format(e))

        return self.resources_to_json()

    def resources_to_json(self):
        result = {}
        if self.types_map is not None:
            for drt in self.types_map.values():
                result[str(drt.get_resource_type())] = str(drt.get_available_units())

        return result

    def list_resources(self):
        try:
            self.controller_state.close_dead_slices()
            controller = self.controller_state.get_management_actor()
            self.logger.debug("list resources invoked controller:{}".format(controller))

            try:
                abstract_models = self.discover_types(controller=controller)
            except Exception as e:
                self.logger.error("Failed to populate abstract models e: {}".format(e))
                raise e

            if abstract_models is None:
                raise Exception("Failed to populate abstract models")

            return abstract_models

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred processing list resource e: {}".format(e))
            raise e