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
import traceback
from typing import Tuple

from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.neo4j_property_graph import Neo4jPropertyGraph

from fabric_cf.actor.boot.inventory.neo4j_resource_pool_factory import Neo4jResourcePoolFactory
from fabric_cf.actor.core.apis.i_actor import ActorType
from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.access_checker import AccessChecker
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice import OrchestratorSlice
from fabric_cf.orchestrator.core.orchestrator_state import OrchestratorStateSingleton


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorStateSingleton.get()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.jwks_url = GlobalsSingleton.get().get_config().get_oauth_config().get(
            Constants.property_conf_o_auth_jwks_url, None)
        self.pdp_config = GlobalsSingleton.get().get_config().get_global_config().get_pdp_config()

    def get_logger(self):
        return self.logger

    def validate_credentials(self, *, token) -> dict:
        try:
            fabric_token = FabricToken(logger=self.logger, token=token)

            return fabric_token.validate()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred while validating the token e: {}".format(e))

    def get_broker(self, *, controller: IMgmtController) -> ID:
        try:
            if self.controller_state.get_broker() is not None:
                return self.controller_state.get_broker()

            brokers = controller.get_brokers()
            self.logger.debug("Brokers: {}".format(brokers))
            self.logger.error("Last Error: {}".format(controller.get_last_error()))
            if brokers is not None:
                result = ID(uid=next(iter(brokers), None).get_guid())
                self.controller_state.set_broker(broker=result)
                return result
        except Exception:
            self.logger.error(traceback.format_exc())

        return None

    def discover_types(self, *, controller: IMgmtController, token: str,
                       delete_graph: bool = True) -> Tuple[dict, Neo4jPropertyGraph]:
        """
        Discover all the available resources by querying Broker
        :param controller Management Controller Object
        :param token Fabric Token
        :param delete_graph flag indicating if the loaded graph should be deleted or not
        :return tuple of dictionary containing the BQM and neo4j graph (if delete_graph = False)
        """
        broker = self.get_broker(controller=controller)
        if broker is None:
            raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                        "Please check Orchestrator container configuration and logs.")

        my_pools = controller.get_pool_info(broker=broker, id_token=token)
        if my_pools is None or len(my_pools) != 1:
            raise OrchestratorException("Could not discover types: {}".format(controller.get_last_error()))

        response = None
        graph = None
        for p in my_pools:
            try:
                bqm = p.properties.get(Constants.broker_query_model, None)
                if bqm is not None :
                    graph = Neo4jResourcePoolFactory.get_graph_from_string(graph_str=bqm)
                    graph.validate_graph()
                    if delete_graph:
                        Neo4jResourcePoolFactory.delete_graph(graph_id=graph.get_graph_id())
                response = bqm
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.debug("Could not process discover types response {}".format(e))

        return response, graph

    def list_resources(self, *, token: str) -> dict:
        try:
            AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.resources, token=token,
                                       actor_type=ActorType.Orchestrator, logger=self.logger)
            self.controller_state.close_dead_slices()
            controller = self.controller_state.get_management_actor()
            self.logger.debug("list resources invoked controller:{}".format(controller))

            try:
                broker_query_model, graph = self.discover_types(controller=controller, token=token)
            except Exception as e:
                self.logger.error("Failed to populate broker models e: {}".format(e))
                raise e

            if broker_query_model is None:
                raise OrchestratorException("Failed to populate abstract models")

            return broker_query_model

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred processing list resource e: {}".format(e))
            raise e

    def create_slice(self, *, token: str, slice_name: str, slice_graph: str) -> dict:
        orchestrator_slice = None
        try:
            fabric_token = AccessChecker.check_access(action_id=ActionId.create, resource_type=ResourceType.slice,
                                                      token=token, actor_type=ActorType.Orchestrator,
                                                      logger=self.logger)

            user_dn = fabric_token.get_decoded_token().get(Constants.claims_sub, None)

            if user_dn is None:
                raise OrchestratorException("Failed to determine Claim 'sub' from Fabric Token")

            self.controller_state.close_dead_slices()
            controller = self.controller_state.get_management_actor()
            self.logger.debug("Create slice invoked for Controller: {}".format(controller))

            bqm_string = None
            bqm_graph = None
            try:
                bqm_string, bqm_graph = self.discover_types(controller=controller, token=token)
            except Exception as e:
                self.logger.error("Exception occurred while listing resources e: {}".format(e))
                raise e

            if bqm_graph is None:
                self.logger.error("Could not get Broker Query Model")
                raise OrchestratorException("Broker Query Model not found!")

            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            slice_obj = SliceAvro()
            slice_obj.set_slice_name(slice_name)
            slice_obj.set_client_slice(True)

            slice_id = controller.add_slice(slice_obj=slice_obj)
            if slice_id is None:
                self.logger.error(controller.get_last_error())
                self.logger.error("Slice could not be added to Database")
                raise OrchestratorException("Slice could not be added to Database")

            slice_obj.set_slice_id(slice_id=slice_id)
            orchestrator_slice = OrchestratorSlice(controller=controller, broker=broker,
                                                   slice_obj=slice_obj, user_dn=user_dn, logger=self.logger)
            orchestrator_slice.lock()

            # Add slice to relational database
            self.controller_state.add_slice(controller_slice=orchestrator_slice)

            # Create Slivers from Slice Graph; Compute Reservations from Slivers;
            # Add Reservations to relational database;
            computed_reservations = orchestrator_slice.create(bqm_graph=bqm_graph, slice_graph=slice_graph)

            # Process the Slice i.e. Demand the computed reservations i.e. Add them to the policy
            # Once added to the policy; Actor Tick Handler will do following asynchronously:
            # 1. Ticket message exchange with broker and
            # 2. Redeem message exchange with AM once ticket is granted by Broker
            self.controller_state.get_sdt().process_slice(controller_slice=orchestrator_slice)

            return {"reservations": computed_reservations}
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred processing create slice e: {}".format(e))
            raise e
        finally:
            if orchestrator_slice is not None:
                orchestrator_slice.unlock()