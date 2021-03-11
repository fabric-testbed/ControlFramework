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

from fabric_cf.actor.neo4j.neo4j_helper import Neo4jHelper
from fabric_cf.actor.core.apis.i_mgmt_controller import IMgmtController
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper
from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
from fabric_cf.orchestrator.core.response_builder import ResponseBuilder


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorKernelSingleton.get()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()
        self.jwks_url = GlobalsSingleton.get().get_config().get_oauth_config().get(
            Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
        self.pdp_config = GlobalsSingleton.get().get_config().get_global_config().get_pdp_config()

    def get_logger(self):
        """
        Get Logger
        :return: logger
        """
        return self.logger

    def validate_credentials(self, *, token) -> dict:
        """
        Validate credentials
        :param token:
        :return:
        """
        try:
            fabric_token = FabricToken(logger=self.logger, token=token)

            return fabric_token.validate()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred while validating the token e: {e}")

    def get_broker(self, *, controller: IMgmtController) -> ID:
        """
        Get broker
        :param controller:
        :return:
        """
        try:
            if self.controller_state.get_broker() is not None:
                return self.controller_state.get_broker()

            brokers = controller.get_brokers()
            self.logger.debug(f"Brokers: {brokers}")
            self.logger.error(f"Last Error: {controller.get_last_error()}")
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
            raise OrchestratorException(f"Could not discover types: {controller.get_last_error()}")

        response = None
        graph = None
        for p in my_pools:
            try:
                status = p.properties.get(Constants.QUERY_RESPONSE_STATUS, "False")
                if status.lower() != "false":
                    bqm = p.properties.get(Constants.BROKER_QUERY_MODEL, None)
                    if bqm is not None:
                        graph = Neo4jHelper.get_graph_from_string_direct(graph_str=bqm)
                        graph.validate_graph()
                        if delete_graph:
                            Neo4jHelper.delete_graph(graph_id=graph.get_graph_id())
                    response = bqm
                else:
                    self.logger.error(p.properties.get(Constants.QUERY_RESPONSE_MESSAGE))
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.debug(f"Could not process discover types response {e}")

        return response, graph

    def list_resources(self, *, token: str) -> dict:
        """
        List Resources
        @param token Fabric Identity Token
        @throws Raises an exception in case of failure
        @returns Broker Query Model on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"list_resources invoked controller:{controller}")

            try:
                broker_query_model, graph = self.discover_types(controller=controller, token=token)
            except Exception as e:
                self.logger.error(f"Failed to populate broker models e: {e}")
                raise e

            if broker_query_model is None:
                raise OrchestratorException("Failed to populate abstract models")

            return ResponseBuilder.get_broker_query_model_summary(bqm=broker_query_model)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing list_resources e: {e}")
            raise e

    def create_slice(self, *, token: str, slice_name: str, slice_graph: str) -> dict:
        """
        Create a slice
        @param token Fabric Identity Token
        @param slice_name Slice Name
        @param slice_graph Slice Graph Model
        @throws Raises an exception in case of failure
        @returns List of reservations created for the Slice on success
        """
        slice_id = None
        controller = None
        orchestrator_slice = None
        bqm_graph = None
        neo4j_graph = None
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"create_slice invoked for Controller: {controller}")

            # Check if an Active slice exists already with the same name for the user
            existing_slices = controller.get_slices(id_token=token, slice_name=slice_name)

            if existing_slices is not None and len(existing_slices) != 0:
                for es in existing_slices:
                    if es.get_state() != SliceState.Dead.value and es.get_state() != SliceState.Closing.value:
                        raise OrchestratorException(f"Slice {slice_name} already exists")

            neo4j_graph = Neo4jHelper.load_slice_in_neo4j(slice_graph=slice_graph)

            try:
                bqm_string, bqm_graph = self.discover_types(controller=controller, token=token, delete_graph=False)
            except Exception as e:
                self.logger.error(f"Exception occurred while listing resources e: {e}")
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
            slice_obj.set_description("Description")
            slice_obj.graph_id = neo4j_graph.get_graph_id()

            self.logger.debug(f"Adding Slice {slice_name}")
            slice_id = controller.add_slice(slice_obj=slice_obj, id_token=token)
            if slice_id is None:
                self.logger.error(controller.get_last_error())
                self.logger.error("Slice could not be added to Database")
                raise OrchestratorException("Slice could not be added to Database")
            self.logger.debug(f"Slice {slice_name}/{slice_id} added successfully")

            slice_obj.set_slice_id(slice_id=str(slice_id))
            orchestrator_slice = OrchestratorSliceWrapper(controller=controller, broker=broker,
                                                          slice_obj=slice_obj, logger=self.logger)

            orchestrator_slice.lock()

            # Create Slivers from Slice Graph; Compute Reservations from Slivers;
            # Add Reservations to relational database;
            computed_reservations = orchestrator_slice.create(bqm_graph=bqm_graph, slice_graph=neo4j_graph)

            # Process the Slice i.e. Demand the computed reservations i.e. Add them to the policy
            # Once added to the policy; Actor Tick Handler will do following asynchronously:
            # 1. Ticket message exchange with broker and
            # 2. Redeem message exchange with AM once ticket is granted by Broker
            self.controller_state.get_sdt().process_slice(controller_slice=orchestrator_slice)

            return ResponseBuilder.get_reservation_summary(res_list=computed_reservations)
        except Exception as e:
            if slice_id is not None and controller is not None and neo4j_graph is not None:
                Neo4jHelper.delete_graph(graph_id=neo4j_graph.graph_id)
                controller.remove_slice(slice_id=slice_id, id_token=token)
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing create_slice e: {e}")
            raise e
        finally:
            if bqm_graph is not None:
                Neo4jHelper.delete_graph(graph_id=bqm_graph.get_graph_id())
            if orchestrator_slice is not None:
                orchestrator_slice.unlock()

    def get_slivers(self, *, token: str, slice_id: str, sliver_id: str = None, include_notices: bool = False) -> dict:
        """
        Get Slivers for a Slice
        @param token Fabric Identity Token
        @param slice_id Slice Id
        @param sliver_id Sliver Id
        @param include_notices include notices
        @throws Raises an exception in case of failure
        @returns List of reservations created for the Slice on success
        """
        include_sliver = False
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slivers invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)
            rid = None
            if sliver_id is not None:
                rid = ID(uid=sliver_id)
                include_sliver = True

            reservations = controller.get_reservations(id_token=token, slice_id=slice_guid, rid=rid)
            if reservations is None:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"Slice# {slice_id} has no reservations")

            return ResponseBuilder.get_reservation_summary(res_list=reservations, include_notices=include_notices,
                                                           include_sliver=True)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slivers e: {e}")
            raise e

    def get_slices(self, *, token: str, slice_id: str = None) -> dict:
        """
        Get User Slices
        @param token Fabric Identity Token
        @param slice_id Slice Id
=       @throws Raises an exception in case of failure
        @returns List of Slices on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slices invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices")

            return ResponseBuilder.get_slice_summary(slice_list=slice_list, slice_id=slice_id)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slices e: {e}")
            raise e

    def delete_slice(self, *, token: str, slice_id: str = None):
        """
        Delete User Slice
        @param token Fabric Identity Token
        @param slice_id Slice Id
=       @throws Raises an exception in case of failure
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"delete_slice invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)

            if slice_list is None or len(slice_list) == 0:
                raise OrchestratorException(f"Slice# {slice_id} not found")

            slice_object = next(iter(slice_list))

            slice_state = SliceState(slice_object.get_state())
            if slice_state == SliceState.Dead or slice_state == SliceState.Closing:
                raise OrchestratorException(f"Slice# {slice_id} already closed")

            if slice_state != SliceState.StableOK and slice_state != SliceState.StableError:
                self.logger.info(f"Unable to delete Slice# {slice_guid} that is not yet stable, try again later")
                raise OrchestratorException(f"Unable to delete Slice# {slice_guid} that is not yet stable, "
                                            f"try again later")

            controller.close_reservations(slice_id=slice_guid)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing delete_slice e: {e}")
            raise e

    def get_slice_graph(self, *, token: str, slice_id: str) -> dict:
        """
        Get User Slice
        @param token Fabric Identity Token
        @param slice_id Slice Id
=       @throws Raises an exception in case of failure
        @returns Slice Graph on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slice_graph invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices")

            slice_obj = next(iter(slice_list))

            if slice_obj.get_graph_id() is None:
                raise OrchestratorException(f"Slice# {slice_obj} does not have graph id")

            slice_model = Neo4jHelper.get_graph(graph_id=slice_obj.get_graph_id())

            if slice_model is None:
                raise OrchestratorException(f"Slice# {slice_obj} graph could not be loaded")

            return ResponseBuilder.get_slice_model_summary(slice_model=slice_model.serialize_graph())
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slice_graph e: {e}")
            raise e