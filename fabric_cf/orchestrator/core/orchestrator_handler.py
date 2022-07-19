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
from datetime import datetime, timedelta, timezone
from http.client import NOT_FOUND, BAD_REQUEST, UNAUTHORIZED
from typing import Tuple, List

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.resources.abc_cbm import ABCCBMPropertyGraph
from fim.slivers.base_sliver import BaseSliver
from fim.user import GraphFormat
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.fabric_token import FabricToken
from fabric_cf.actor.security.pdp_auth import ActionId
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_slice_wrapper import OrchestratorSliceWrapper
from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
from fabric_cf.orchestrator.core.reservation_status_update import ReservationStatusUpdate
from fabric_cf.orchestrator.core.response_builder import ResponseBuilder


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorKernelSingleton.get()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.globals = GlobalsSingleton.get()
        self.logger = self.globals.get_logger()
        self.jwks_url = self.globals.get_config().get_oauth_config().get(Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
        self.pdp_config = self.globals.get_config().get_global_config().get_pdp_config()

    def get_logger(self):
        """
        Get Logger
        :return: logger
        """
        return self.logger

    def decode_token(self, *, token: str) -> FabricToken:
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        oauth_config = GlobalsSingleton.get().get_config().get_global_config().get_oauth()
        jwt_validator = GlobalsSingleton.get().get_jwt_validator()
        fabric_token = FabricToken(oauth_config=oauth_config, jwt_validator=jwt_validator,
                                   logger=self.logger, token=token)
        fabric_token.validate()
        return fabric_token

    def __authorize_request(self, *, id_token: str, action_id: ActionId,
                            resource: BaseSliver or ExperimentTopology = None,
                            lease_end_time: datetime = None) -> FabricToken:
        """
        Authorize request
        :param id_token:
        :param action_id:
        :param resource:
        :param lease_end_time:
        :return:
        """
        from fabric_cf.actor.security.access_checker import AccessChecker
        fabric_token = AccessChecker.check_access(action_id=action_id, token=id_token, logger=self.logger,
                                                  resource=resource, lease_end_time=lease_end_time)

        if fabric_token.get_subject() is None:
            raise OrchestratorException(http_error_code=UNAUTHORIZED,message="Invalid token")
        return fabric_token

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

            self.__authorize_request(id_token=token, action_id=ActionId.query)

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
        if self.globals.is_maintenance_mode_on():
            raise OrchestratorException(Constants.MAINTENANCE_MODE_ERROR)

        slice_id = None
        controller = None
        new_slice_object = None
        asm_graph = None
        try:
            end_time = self.__validate_lease_end_time(lease_end_time=lease_end_time)

            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"create_slice invoked for Controller: {controller}")

            # Validate the slice graph
            topology = ExperimentTopology(graph_string=slice_graph)
            topology.validate()

            asm_graph = FimHelper.get_neo4j_asm_graph(slice_graph=topology.serialize())
            asm_graph.validate_graph()

            # Authorize the slice
            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.create, resource=topology,
                                                    lease_end_time=end_time)

            # Check if an Active slice exists already with the same name for the user
            project, tags = fabric_token.get_project_and_tags()
            existing_slices = controller.get_slices(slice_name=slice_name,
                                                    email=fabric_token.get_email(), project=project)

            if existing_slices is not None and len(existing_slices) != 0:
                for es in existing_slices:
                    if es.get_state() != SliceState.Dead.value and es.get_state() != SliceState.Closing.value:
                        raise OrchestratorException(f"Slice {slice_name} already exists")

            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            slice_obj = SliceAvro()
            slice_obj.set_slice_name(slice_name)
            slice_obj.set_client_slice(True)
            slice_obj.set_description("Description")
            slice_obj.graph_id = asm_graph.get_graph_id()
            slice_obj.set_config_properties(value={Constants.USER_SSH_KEY: ssh_key,
                                                   Constants.PROJECT_ID: project})
            slice_obj.set_lease_end(lease_end=end_time)
            auth = AuthAvro()
            auth.oidc_sub_claim = fabric_token.get_subject()
            auth.email = fabric_token.get_email()
            auth.token = token
            slice_obj.set_owner(auth)
            slice_obj.set_project_id(project)

            self.logger.debug(f"Adding Slice {slice_name}")
            slice_id = controller.add_slice(slice_obj=slice_obj)
            if slice_id is None:
                self.logger.error(controller.get_last_error())
                self.logger.error("Slice could not be added to Database")
                raise OrchestratorException("Slice could not be added to Database")
            self.logger.debug(f"Slice {slice_name}/{slice_id} added successfully")

            slice_obj.set_slice_id(slice_id=str(slice_id))
            new_slice_object = OrchestratorSliceWrapper(controller=controller, broker=broker,
                                                        slice_obj=slice_obj, logger=self.logger)

            new_slice_object.lock()

            # Create Slivers from Slice Graph; Compute Reservations from Slivers;
            # Add Reservations to relational database;
            computed_reservations = new_slice_object.create(slice_graph=asm_graph)

            # Enqueue the slice on the demand thread
            # Demand thread is responsible for demanding the reservations
            # Helps improve the create response time
            self.controller_state.get_demand_thread().queue_slice(controller_slice=new_slice_object)

            return ResponseBuilder.get_reservation_summary(res_list=computed_reservations)
        except Exception as e:
            if slice_id is not None and controller is not None and asm_graph is not None:
                FimHelper.delete_graph(graph_id=asm_graph.graph_id)
                controller.remove_slice(slice_id=slice_id)
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing create_slice e: {e}")
            raise e
        finally:
            if new_slice_object is not None:
                new_slice_object.unlock()

    def get_slivers(self, *, token: str, slice_id: str, sliver_id: str = None, include_notices: bool = True) -> dict:
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

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            rid = ID(uid=sliver_id) if sliver_id is not None else None

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)

            reservations = controller.get_reservations(slice_id=slice_guid, rid=rid, email=fabric_token.get_email(),
                                                       oidc_claim_sub=fabric_token.get_subject())
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

    def get_slices(self, *, token: str, slice_id: str = None, states: List[str] = None) -> dict:
        """
        Get User Slices
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param states Slice states
        :raises Raises an exception in case of failure
        :returns List of Slices on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slices invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            slice_states = SliceState.str_list_to_state_list(states=states)

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)

            # TODO - uncomment once project based view is implemented on portal
            #project, tags = fabric_token.get_project_and_tags()
            project = None
            slice_list = controller.get_slices(slice_id=slice_guid, state=slice_states,
                                               email=fabric_token.get_email(), project=project)
            return ResponseBuilder.get_slice_summary(slice_list=slice_list)
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

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.delete)

            project, tags = fabric_token.get_project_and_tags()
            slice_list = controller.get_slices(slice_id=slice_guid, email=fabric_token.get_email())

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

            self.__authorize_request(id_token=token, action_id=ActionId.delete)
            controller.close_reservations(slice_id=slice_guid)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing delete_slice e: {e}")
            raise e

    def get_slice_graph(self, *, token: str, slice_id: str, graph_format_str: str) -> dict:
        """
        Get User Slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param graph_format_str
        :raises Raises an exception in case of failure
        :returns Slice Graph on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slice_graph invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None

            self.__authorize_request(id_token=token, action_id=ActionId.query)

            slice_list = controller.get_slices(slice_id=slice_guid)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            slice_obj = next(iter(slice_list))

            if slice_obj.get_graph_id() is None:
                raise OrchestratorException(f"Slice# {slice_obj} does not have graph id")

            slice_model = FimHelper.get_graph(graph_id=slice_obj.get_graph_id())

            graph_format = self.__translate_graph_format(graph_format=graph_format_str)
            if graph_format == GraphFormat.JSON_NODELINK:
                slice_model_str = slice_model.serialize_graph()
                slice_model = FimHelper.get_networkx_graph_from_string(graph_str=slice_model_str)

            if slice_model is None:
                raise OrchestratorException(f"Slice# {slice_obj} graph could not be loaded")

            slice_model_str = slice_model.serialize_graph(format=graph_format)
            return ResponseBuilder.get_slice_summary(slice_list=slice_list, slice_model=slice_model_str)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slice_graph e: {e}")
            raise e

    def renew_slice(self, *, token: str, slice_id: str, new_lease_end_time: str):
        """
        Renew a slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param new_lease_end_time: New Lease End Time in UTC in '%Y-%m-%d %H:%M:%S %z' format
        :raises Raises an exception in case of failure
        :return:
        """
        if self.globals.is_maintenance_mode_on():
            raise OrchestratorException(Constants.MAINTENANCE_MODE_ERROR)

        failed_to_extend_rid_list = []
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"renew_slice invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            slice_list = controller.get_slices(slice_id=slice_guid)

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

            reservations = controller.get_reservations(slice_id=slice_id)
            if reservations is None or len(reservations) < 1:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"Slice# {slice_id} has no reservations")

            self.logger.debug(f"There are {len(reservations)} reservations in the slice# {slice_id}")

            self.__authorize_request(id_token=token, action_id=ActionId.renew, lease_end_time=new_end_time)
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

            if len(failed_to_extend_rid_list) == 0:
                slice_object.set_lease_end(lease_end=new_end_time)
                if not controller.update_slice(slice_obj=slice_object):
                    self.logger.error(f"Failed to update lease end time: {new_end_time} in Slice: {slice_object}")
                    self.logger.error(controller.get_last_error())

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
            new_end_time = datetime.now(timezone.utc) + timedelta(hours=Constants.DEFAULT_LEASE_IN_HOURS)
            return new_end_time
        try:
            new_end_time = datetime.strptime(lease_end_time, Constants.LEASE_TIME_FORMAT)
        except Exception as e:
            raise OrchestratorException(f"Lease End Time is not in format {Constants.LEASE_TIME_FORMAT}",
                                        http_error_code=BAD_REQUEST)

        now = datetime.now(timezone.utc)
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
