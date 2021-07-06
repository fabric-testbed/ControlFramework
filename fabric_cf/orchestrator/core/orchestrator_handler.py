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
from datetime import datetime, timedelta
from http.client import NOT_FOUND, BAD_REQUEST
from typing import Tuple, List

from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.user import GraphFormat

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
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

    def get_broker(self, *, controller: ABCMgmtControllerMixin) -> ID:
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

    def discover_broker_query_model(self, *, controller: ABCMgmtControllerMixin, token: str = None,
                                    level: int = 10, delete_graph: bool = True,
                                    ignore_validation: bool = True,
                                    graph_format: GraphFormat = GraphFormat.GRAPHML) -> Tuple[str, ABCCBMPropertyGraph or None]:
        """
        Discover all the available resources by querying Broker
        :param controller Management Controller Object
        :param token Fabric Token
        :param level: level of details
        :param delete_graph flag indicating if the loaded graph should be deleted or not
        :param ignore_validation flag indicating to ignore validating the graph (only needed for ADs)
        :param graph_format: Graph format
        :return tuple of dictionary containing the BQM and ABCCBMPropertyGraph (if delete_graph = False)
        """
        broker = self.get_broker(controller=controller)
        if broker is None:
            raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                        "Please check Orchestrator container configuration and logs.")

        model = controller.get_broker_query_model(broker=broker, id_token=token, level=level,
                                                  graph_format=graph_format)
        if model is None:
            raise OrchestratorException(f"Could not discover types: {controller.get_last_error()}")

        graph_str = model.get_model()

        try:
            if graph_str is not None and graph_str != '':
                graph = None
                # Load graph only when GraphML
                if graph_format == GraphFormat.GRAPHML:
                    graph = FimHelper.get_neo4j_cbm_graph_from_string_direct(graph_str=graph_str,
                                                                             ignore_validation=ignore_validation)
                    if delete_graph:
                        FimHelper.delete_graph(graph_id=graph.get_graph_id())
                        graph = None
                return graph_str, graph
            else:
                raise OrchestratorException(http_error_code=NOT_FOUND,
                                            message="Resource(s) not found!")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            raise e

    def list_resources(self, *, token: str, level: int) -> dict:
        """
        List Resources
        :param token Fabric Identity Token
        :param level: level of details (default set to 1)
        :raises Raises an exception in case of failure
        :returns Broker Query Model on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"list_resources invoked controller:{controller}")

            broker_query_model, graph = self.discover_broker_query_model(controller=controller, token=token,
                                                                         level=level, ignore_validation=True)

            return ResponseBuilder.get_broker_query_model_summary(bqm=broker_query_model)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing list_resources e: {e}")
            raise e

    def portal_list_resources(self, *, graph_format_str: str) -> dict:
        """
        List Resources
        :param graph_format_str: Graph format
        :raises Raises an exception in case of failure
        :returns Broker Query Model on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"portal_list_resources invoked controller:{controller}")

            broker_query_model = None
            graph_format = self.__translate_graph_format(graph_format=graph_format_str)

            saved_bqm = self.controller_state.get_saved_bqm(graph_format=graph_format)
            if saved_bqm is not None and not saved_bqm.can_refresh():
                broker_query_model = saved_bqm.get_bqm()

            if broker_query_model is None:
                broker_query_model, graph = self.discover_broker_query_model(controller=controller,
                                                                             ignore_validation=True,
                                                                             level=1,
                                                                             graph_format=graph_format)
                if broker_query_model is not None:
                    self.controller_state.save_bqm(bqm=broker_query_model, graph_format=graph_format)

            return ResponseBuilder.get_broker_query_model_summary(bqm=broker_query_model)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing portal_list_resources e: {e}")
            raise e

    def create_slice(self, *, token: str, slice_name: str, slice_graph: str, ssh_key: str,
                     lease_end_time: str) -> dict:
        """
        Create a slice
        :param token Fabric Identity Token
        :param slice_name Slice Name
        :param slice_graph Slice Graph Model
        :param ssh_key: User ssh key
        :param lease_end_time: Lease End Time (UTC)
        :raises Raises an exception in case of failure
        :returns List of reservations created for the Slice on success
        """
        slice_id = None
        controller = None
        orchestrator_slice = None
        bqm_graph = None
        asm_graph = None
        try:
            end_time = self.__validate_lease_end_time(lease_end_time=lease_end_time)

            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"create_slice invoked for Controller: {controller}")

            # Check if an Active slice exists already with the same name for the user
            existing_slices = controller.get_slices(id_token=token, slice_name=slice_name)

            if existing_slices is not None and len(existing_slices) != 0:
                for es in existing_slices:
                    if es.get_state() != SliceState.Dead.value and es.get_state() != SliceState.Closing.value:
                        raise OrchestratorException(f"Slice {slice_name} already exists")

            asm_graph = FimHelper.get_neo4j_asm_graph(slice_graph=slice_graph)

            # FIXME : uncomment post testing
            #bqm_string, bqm_graph = self.discover_broker_query_model(controller=controller, token=token,
            #                                                         delete_graph=False)
            bqm_graph = None

            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            slice_obj = SliceAvro()
            slice_obj.set_slice_name(slice_name)
            slice_obj.set_client_slice(True)
            slice_obj.set_description("Description")
            slice_obj.graph_id = asm_graph.get_graph_id()
            slice_obj.set_config_properties(value={Constants.USER_SSH_KEY: ssh_key})
            slice_obj.set_lease_end(lease_end=end_time)

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
            computed_reservations = orchestrator_slice.create(slice_graph=asm_graph)

            # Process the Slice i.e. Demand the computed reservations i.e. Add them to the policy
            # Once added to the policy; Actor Tick Handler will do following asynchronously:
            # 1. Ticket message exchange with broker and
            # 2. Redeem message exchange with AM once ticket is granted by Broker
            self.controller_state.get_sdt().process_slice(controller_slice=orchestrator_slice)

            return ResponseBuilder.get_reservation_summary(res_list=computed_reservations)
        except Exception as e:
            if slice_id is not None and controller is not None and asm_graph is not None:
                FimHelper.delete_graph(graph_id=asm_graph.graph_id)
                controller.remove_slice(slice_id=slice_id, id_token=token)
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing create_slice e: {e}")
            raise e
        finally:
            if bqm_graph is not None:
                FimHelper.delete_graph(graph_id=bqm_graph.get_graph_id())
            if orchestrator_slice is not None:
                orchestrator_slice.unlock()

    def get_slivers(self, *, token: str, slice_id: str, sliver_id: str = None, include_notices: bool = False) -> dict:
        """
        Get Slivers for a Slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param sliver_id Sliver Id
        :param include_notices include notices
        :raises Raises an exception in case of failure
        :returns List of reservations created for the Slice on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slivers invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)
            rid = None
            if sliver_id is not None:
                rid = ID(uid=sliver_id)

            reservations = controller.get_reservations(id_token=token, slice_id=slice_guid, rid=rid)
            if reservations is None:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                    if controller.get_last_error().status.code == ErrorCodes.ErrorNoSuchSlice:
                        raise OrchestratorException(f"Slice# {slice_id} not found",
                                                    http_error_code=NOT_FOUND)
                    elif controller.get_last_error().status.code == ErrorCodes.ErrorNoSuchReservation:
                        raise OrchestratorException(f"Reservation# {rid} not found",
                                                    http_error_code=NOT_FOUND)

                raise OrchestratorException(f"Slice# {slice_id} has no reservations",
                                            http_error_code=NOT_FOUND)

            return ResponseBuilder.get_reservation_summary(res_list=reservations, include_notices=include_notices,
                                                           include_sliver=True)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slivers e: {e}")
            raise e

    def get_slices(self, *, token: str, slice_id: str = None, state: str = None) -> dict:
        """
        Get User Slices
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param state Slice state
=       :raises Raises an exception in case of failure
        :returns List of Slices on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slices invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_states = self.__translate_slice_state(state=state)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            return ResponseBuilder.get_slice_summary(slice_list=slice_list, slice_id=slice_id,
                                                     slice_states=slice_states)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slices e: {e}")
            raise e

    def delete_slice(self, *, token: str, slice_id: str = None):
        """
        Delete User Slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :raises Raises an exception in case of failure
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"delete_slice invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)

            if slice_list is None or len(slice_list) == 0:
                raise OrchestratorException(f"Slice# {slice_id} not found",
                                            http_error_code=NOT_FOUND)

            slice_object = next(iter(slice_list))

            slice_state = SliceState(slice_object.get_state())
            if slice_state == SliceState.Dead or slice_state == SliceState.Closing:
                raise OrchestratorException(f"Slice# {slice_id} already closed",
                                            http_error_code=BAD_REQUEST)

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
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :raises Raises an exception in case of failure
        :returns Slice Graph on success
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
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            slice_obj = next(iter(slice_list))

            if slice_obj.get_graph_id() is None:
                raise OrchestratorException(f"Slice# {slice_obj} does not have graph id")

            slice_model = FimHelper.get_graph(graph_id=slice_obj.get_graph_id())

            if slice_model is None:
                raise OrchestratorException(f"Slice# {slice_obj} graph could not be loaded")

            return ResponseBuilder.get_slice_model_summary(slice_model=slice_model.serialize_graph())
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slice_graph e: {e}")
            raise e

    def renew_slice(self, *, token: str, slice_id: str, new_lease_end_time: str):
        """
        Renew a slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param new_lease_end_time: New Lease End Time in UTC in '%Y-%m-%d %H:%M:%S' format
        :raises Raises an exception in case of failure
        :return:
        """
        failed_to_extend_rid_list = []
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"renew_slice invoked for Controller: {controller}")

            slice_guid = None
            if slice_id is not None:
                slice_guid = ID(uid=slice_id)

            slice_list = controller.get_slices(id_token=token, slice_id=slice_guid)

            if slice_list is None or len(slice_list) == 0:
                raise OrchestratorException(f"Slice# {slice_id} not found",
                                            http_error_code=NOT_FOUND)

            slice_object = next(iter(slice_list))

            slice_state = SliceState(slice_object.get_state())
            if slice_state == SliceState.Dead or slice_state == SliceState.Closing:
                raise OrchestratorException(f"Slice# {slice_id} already closed",
                                            http_error_code=BAD_REQUEST)

            if slice_state != SliceState.StableOK and slice_state != SliceState.StableError:
                self.logger.info(f"Unable to renew Slice# {slice_guid} that is not yet stable, try again later")
                raise OrchestratorException(f"Unable to renew Slice# {slice_guid} that is not yet stable, "
                                            f"try again later")

            new_end_time = self.__validate_lease_end_time(lease_end_time=new_lease_end_time)

            reservations = controller.get_reservations(id_token=token, slice_id=slice_id)
            if reservations is None or len(reservations) < 1:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"Slice# {slice_id} has no reservations")

            self.logger.debug(f"There are {len(reservations)} reservations in the slice# {slice_id}")

            for r in reservations:
                res_state = ReservationStates(r.get_state())
                if res_state == ReservationStates.Closed or res_state == ReservationStates.Failed or \
                        res_state == ReservationStates.CloseWait:
                    continue

                current_end_time = ActorClock.from_milliseconds(milli_seconds=r.get_end())

                if new_end_time < current_end_time:
                    raise OrchestratorException(f"Attempted new term end time is shorter than current slice end time")

                self.logger.debug(f"Extending reservation with reservation# {r.get_reservation_id()}")
                result = controller.extend_reservation(reservation=ID(uid=r.get_reservation_id()),
                                                       new_end_time=new_end_time)
                if not result:
                    failed_to_extend_rid_list.append(r.get_reservation_id())
                else:
                    slice_object.set_lease_end(lease_end=new_end_time)
                    if not controller.update_slice(slice_obj=slice_object):
                        self.logger.error(f"Failed to update lease end time: {new_end_time} in Slice: {slice_object}")

            return ResponseBuilder.get_response_summary(rid_list=failed_to_extend_rid_list)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing renew e: {e}")
            raise e

    def __validate_lease_end_time(self, lease_end_time: str) -> datetime:
        """
        Validate Lease End Time
        :param lease_end_time: New End Time
        :return End Time
        :raises Exception if new end time is in past
        """
        new_end_time = None
        if lease_end_time is None:
            new_end_time = datetime.utcnow() + timedelta(hours=Constants.DEFAULT_LEASE_IN_HOURS)
            return new_end_time
        try:
            new_end_time = datetime.strptime(lease_end_time, Constants.RENEW_TIME_FORMAT)
        except Exception as e:
            raise OrchestratorException(f"Lease End Time is not in format {Constants.RENEW_TIME_FORMAT}",
                                        http_error_code=BAD_REQUEST)

        now = datetime.utcnow()
        if new_end_time <= now:
            raise OrchestratorException(f"New term end time {new_end_time} is in the past! ",
                                        http_error_code=BAD_REQUEST)

        if (new_end_time - now) > Constants.DEFAULT_MAX_DURATION:
            self.logger.info(f"New term end time {new_end_time} exceeds system default "
                             f"{Constants.DEFAULT_MAX_DURATION}, setting to system default: ")

            new_end_time = now + Constants.DEFAULT_MAX_DURATION

        return new_end_time

    @staticmethod
    def __translate_graph_format(*, graph_format: str) -> GraphFormat:
        if graph_format == GraphFormat.GRAPHML.name:
            return GraphFormat.GRAPHML
        elif graph_format == GraphFormat.JSON_NODELINK.name:
            return GraphFormat.JSON_NODELINK
        elif graph_format == GraphFormat.CYTOSCAPE.name:
            return GraphFormat.CYTOSCAPE
        else:
            return GraphFormat.GRAPHML

    @staticmethod
    def __translate_slice_state(*, state: str) -> List[SliceState]:
        ret_val = []
        if state is not None:
            if state.upper() == SliceState.Dead.name.upper():
                ret_val.append(SliceState.Dead)
            elif state.upper() == Constants.STATE_ACTIVE:
                ret_val.append(SliceState.StableOK)
                ret_val.append(SliceState.StableError)
            elif state.upper() == Constants.STATE_ALL:
                ret_val.append(SliceState.StableOK)
                ret_val.append(SliceState.StableError)
                ret_val.append(SliceState.Dead)
                ret_val.append(SliceState.Closing)
                ret_val.append(SliceState.Configuring)
                ret_val.append(SliceState.Nascent)
        if len(ret_val) == 0:
            ret_val.append(SliceState.StableOK)
            ret_val.append(SliceState.StableError)
        return ret_val
