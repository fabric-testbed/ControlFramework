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
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from http.client import NOT_FOUND, BAD_REQUEST, UNAUTHORIZED
from typing import List, Union

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.poa_avro import PoaAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.graph.networkx_property_graph_disjoint import NetworkXGraphImporterDisjoint
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.user import GraphFormat
from fim.user.topology import ExperimentTopology

from fabric_cf.actor.core.common.event_logger import EventLoggerSingleton
from fabric_cf.actor.core.kernel.poa import PoaStates
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
from fabric_cf.orchestrator.core.response_builder import ResponseBuilder


class OrchestratorHandler:
    def __init__(self):
        self.controller_state = OrchestratorKernelSingleton.get()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.globals = GlobalsSingleton.get()
        self.logger = self.globals.get_logger()
        self.jwks_url = self.globals.get_config().get_oauth_config().get(Constants.PROPERTY_CONF_O_AUTH_JWKS_URL, None)
        self.pdp_config = self.globals.get_config().get_global_config().get_pdp_config()
        self.config = self.globals.get_config()
        self.infrastructure_project_id = self.config.get_runtime_config().get(Constants.INFRASTRUCTURE_PROJECT_ID, None)
        self.total_slice_count_seed = self.config.get_runtime_config().get(Constants.TOTAL_SLICE_COUNT_SEED, 0)
        self.local_bqm = self.globals.get_config().get_global_config().get_bqm_config().get(
                    Constants.LOCAL_BQM, False)
        excluded_projects = self.config.get_runtime_config().get(Constants.EXCLUDED_PROJECTS, "")
        self.excluded_projects = [e.strip() for e in excluded_projects.split(",") if e.strip()]

    def get_logger(self):
        """
        Get Logger
        :return: logger
        """
        return self.logger

    def __authorize_request(self, *, id_token: str, action_id: ActionId,
                            resource: BaseSliver or ExperimentTopology = None,
                            lease_end_time: datetime = None,
                            poa_operation: str = None) -> FabricToken:
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

        if fabric_token.subject is None:
            raise OrchestratorException(http_error_code=UNAUTHORIZED, message="Invalid token")
        project_uuid, tags, project_name = fabric_token.first_project
        if action_id == ActionId.POA and poa_operation and poa_operation == "rescan" and project_uuid and \
                tags and "Component.FPGA" not in tags:
            raise OrchestratorException(http_error_code=UNAUTHORIZED,
                                        message="POA rescan not authorized - missing permissions Component.FPGA")
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
        except Exception as e:
            self.logger.error(f"Error occurred: {e}", stack_info=True)

    def discover_broker_query_model(self, *, controller: ABCMgmtControllerMixin, token: str = None,
                                    level: int = 10, graph_format: GraphFormat = GraphFormat.GRAPHML,
                                    force_refresh: bool = False, start: datetime = None,
                                    end: datetime = None, includes: str = None, excludes: str = None,
                                    email: str = None) -> str or None:
        """
        Discover all the available resources by querying Broker
        :param controller Management Controller Object
        :param token Fabric Token
        :param level: level of details
        :param graph_format: Graph format
        :param force_refresh: Force fetching a fresh model from Broker
        :param start: start time
        :param end: end time
        :param includes: comma separated lists of sites to include
        :param excludes: comma separated lists of sites to exclude
        :param email: Email of the user on whose behalf the request is initiated
        :return str or None
        """
        broker_query_model = None
        # Always get Fresh copy for advanced resource requests
        if not start and not end and not includes and not excludes:
            saved_bqm = self.controller_state.get_saved_bqm(graph_format=graph_format, level=level)
            if saved_bqm is not None:
                if not force_refresh and not saved_bqm.can_refresh() and not saved_bqm.refresh_in_progress:
                    broker_query_model = saved_bqm.get_bqm()
                else:
                    saved_bqm.start_refresh()

        # Request the model from Broker as a fallback
        if not broker_query_model:
            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            self.logger.info(f"Sending Query to broker on behalf of {email} Start: {start}, End: {end}, "
                             f"Force: {force_refresh}, Level: {level}, format: {graph_format}")

            model = controller.get_broker_query_model(broker=broker, id_token=token, level=level,
                                                      graph_format=graph_format, start=start, end=end,
                                                      includes=includes, excludes=excludes)
            if model is None or model.get_model() is None or model.get_model() == '':
                raise OrchestratorException(http_error_code=NOT_FOUND, message=f"Resource(s) not found for "
                                                                               f"level: {level} format: {graph_format}!")

            broker_query_model = model.get_model()

            # Do not update cache for advance requests
            if not start and not end and not includes and not excludes and level > 0:
                self.controller_state.save_bqm(bqm=broker_query_model, graph_format=graph_format, level=level)

        return broker_query_model

    def list_resources(self, *, level: int, force_refresh: bool = False, start: datetime = None,
                       end: datetime, includes: str = None, excludes: str = None, graph_format_str: str = None,
                       token: str = None, authorize: bool = True) -> str:
        """
        List Resources
        :param token Fabric Identity Token
        :param level: level of details (default set to 1)
        :param force_refresh: force fetching bqm from broker and override the cached model
        :param start: start time
        :param end: end time
        :param includes: comma separated lists of sites to include
        :param excludes: comma separated lists of sites to exclude
        :param graph_format_str: Graph format
        :param authorize: Authorize the request; Not authorized for Portal requests
        :raises Raises an exception in case of failure
        :returns Broker Query Model on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"list_resources invoked controller:{controller}")

            graph_format = self.__translate_graph_format(graph_format=graph_format_str) if graph_format_str else GraphFormat.GRAPHML

            if authorize:
                fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)
                email = fabric_token.email
            else:
                email = None
            broker_query_model = self.discover_broker_query_model(controller=controller, token=token, level=level,
                                                                  force_refresh=force_refresh, start=start,
                                                                  end=end, includes=includes, excludes=excludes,
                                                                  graph_format=graph_format, email=email)
            return broker_query_model

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing list_resources e: {e}")
            raise e

    def discover_broker_query_model_summary(self, *, controller: ABCMgmtControllerMixin, token: str = None,
                                             level: int = 2, force_refresh: bool = False,
                                             start: datetime = None, end: datetime = None,
                                             includes: str = None, excludes: str = None,
                                             email: str = None) -> str or None:
        """
        Discover resource summary (JSON) by querying Broker.
        :param controller: Management Controller Object
        :param token: Fabric Token
        :param level: level of details
        :param force_refresh: Force fetching a fresh model from Broker
        :param start: start time
        :param end: end time
        :param includes: comma separated lists of sites to include
        :param excludes: comma separated lists of sites to exclude
        :param email: Email of the user
        :return: JSON string or None
        """
        summary_json = None
        # Always get fresh copy for advanced resource requests
        if not start and not end and not includes and not excludes:
            saved = self.controller_state.get_saved_summary(level=level)
            if saved is not None:
                if not force_refresh and not saved.can_refresh() and not saved.refresh_in_progress:
                    summary_json = saved.get_bqm()
                else:
                    saved.start_refresh()

        if not summary_json:
            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            self.logger.info(f"Sending Summary Query to broker on behalf of {email} Start: {start}, End: {end}, "
                             f"Force: {force_refresh}, Level: {level}")

            model = controller.get_broker_query_model_summary(broker=broker, id_token=token, level=level,
                                                               start=start, end=end,
                                                               includes=includes, excludes=excludes)
            if model is None or model.get_model() is None or model.get_model() == '':
                raise OrchestratorException(http_error_code=NOT_FOUND,
                                            message=f"Resource summary not found for level: {level}!")

            summary_json = model.get_model()

            # Do not update cache for advance requests
            if not start and not end and not includes and not excludes and level > 0:
                self.controller_state.save_summary(summary=summary_json, level=level)

        return summary_json

    def list_resources_summary(self, *, level: int = 2, force_refresh: bool = False,
                               start: datetime = None, end: datetime = None,
                               includes: str = None, excludes: str = None,
                               token: str = None, authorize: bool = True,
                               resource_type: str = None) -> dict:
        """
        List Resources as a JSON summary dict.
        :param token: Fabric Identity Token
        :param level: level of details (default 2)
        :param force_refresh: force fetching from broker
        :param start: start time
        :param end: end time
        :param includes: comma separated lists of sites to include
        :param excludes: comma separated lists of sites to exclude
        :param authorize: Authorize the request
        :param resource_type: comma-separated resource types to include (sites,hosts,links,facility_ports)
        :raises OrchestratorException
        :returns dict with keys: sites, hosts, links, facility_ports
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"list_resources_summary invoked controller:{controller}")

            if authorize:
                fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)
                email = fabric_token.email
            else:
                email = None

            summary_json = self.discover_broker_query_model_summary(
                controller=controller, token=token, level=level,
                force_refresh=force_refresh, start=start, end=end,
                includes=includes, excludes=excludes, email=email
            )

            summary = json.loads(summary_json)

            # Filter by resource type if requested
            if resource_type:
                requested_types = {t.strip().lower() for t in resource_type.split(",")}
                all_types = {"sites", "hosts", "links", "facility_ports"}
                summary = {k: v for k, v in summary.items() if k in requested_types and k in all_types}

            return summary

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing list_resources_summary e: {e}")
            raise e

    def create_slice(self, *, token: str, slice_name: str, slice_graph: str, ssh_key: str,
                     lease_start_time: datetime = None, lease_end_time: datetime = None,
                     lifetime: int = 24) -> List[dict]:
        """
        Create a slice
        :param token Fabric Identity Token
        :param slice_name Slice Name
        :param slice_graph Slice Graph Model
        :param ssh_key: User ssh key
        :param lease_start_time: Lease Start Time (UTC)
        :param lease_end_time: Lease End Time (UTC)
        :param lifetime: Lifetime of the slice in hours
        :raises Raises an exception in case of failure
        :returns List of reservations created for the Slice on success
        """
        start = time.time()
        slice_id = None
        controller = None
        new_slice_object = None
        asm_graph = None
        topology = None
        try:
            from fabric_cf.actor.security.access_checker import AccessChecker
            fabric_token = AccessChecker.validate_and_decode_token(token=token)
            project, tags, project_name = fabric_token.first_project
            allow_long_lived = True if Constants.SLICE_NO_LIMIT_LIFETIME in tags else False
            start_time, end_time = self.__compute_lease_end_time(lease_end_time=lease_end_time, lifetime=lifetime,
                                                                 allow_long_lived=allow_long_lived, project_id=project)

            controller = self.controller_state.get_management_actor()

            # Validate the slice graph
            create_ts = time.time()
            topology = ExperimentTopology(graph_string=slice_graph, importer=NetworkXGraphImporterDisjoint())
            topology.validate()
            self.logger.info(f"TV validate: TIME= {time.time() - create_ts:.0f}")

            create_ts = time.time()
            asm_graph = FimHelper.get_neo4j_asm_graph(slice_graph=topology.serialize())
            self.logger.info(f"ASM load: TIME= {time.time() - create_ts:.0f}")

            # Authorize the slice
            create_ts = time.time()
            self.__authorize_request(id_token=token, action_id=ActionId.create, resource=topology,
                                     lease_end_time=end_time)
            self.logger.info(f"PDP authorize: TIME= {time.time() - create_ts:.0f}")

            # Check if an Active slice exists already with the same name for the user
            create_ts = time.time()
            if tags is not None and isinstance(tags, list):
                tags = ','.join(tags)
            existing_slices = controller.get_slices(slice_name=slice_name, user_id=fabric_token.uuid,
                                                    project=project)
            self.logger.info(f"GET slices: TIME= {time.time() - create_ts:.0f}")

            if existing_slices is not None and len(existing_slices) != 0:
                for es in existing_slices:
                    slice_state = SliceState(es.get_state())
                    if not SliceState.is_dead_or_closing(state=slice_state):
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
                                                   Constants.PROJECT_ID: project,
                                                   Constants.TAGS: tags,
                                                   Constants.CLAIMS_EMAIL: fabric_token.email,
                                                   Constants.TOKEN_HASH: fabric_token.token_hash})
            slice_obj.set_lease_start(lease_start=start_time)
            slice_obj.set_lease_end(lease_end=end_time)
            auth = AuthAvro()
            auth.name = self.controller_state.get_management_actor().get_name()
            auth.guid = self.controller_state.get_management_actor().get_guid()
            auth.oidc_sub_claim = fabric_token.uuid
            auth.email = fabric_token.email
            auth.token = token
            slice_obj.set_owner(auth)
            slice_obj.set_project_id(project)
            slice_obj.set_project_name(project_name)

            create_ts = time.time()
            self.logger.debug(f"Adding Slice {slice_name}")
            slice_id = controller.add_slice(slice_obj=slice_obj)
            self.logger.info(f"SLC add slices: TIME= {time.time() - create_ts:.0f}")
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
            computed_reservations = new_slice_object.create(slice_graph=asm_graph,
                                                            lease_start_time=lease_start_time,
                                                            lease_end_time=lease_end_time,
                                                            lifetime=lifetime)

            # Check if Testbed in Maintenance or Site in Maintenance
            self.check_maintenance_mode(token=fabric_token, reservations=computed_reservations)

            create_ts = time.time()
            #if (lease_start_time and lease_end_time and lifetime) or new_slice_object.ero:
            if lease_start_time and lease_end_time and lifetime:
                # Enqueue slices with ERO also on Advanced Scheduling Thread to determine the links
                # Enqueue future slices on Advanced Scheduling Thread to determine possible start time
                # Determining start time may take time so this is done asynchronously to avoid increasing response time
                # of create slice API
                new_slice_object.update_topology(topology=topology)
                self.controller_state.get_advance_scheduling_thread().queue_slice(controller_slice=new_slice_object)
            else:
                # Enqueue the slice on the demand thread
                # Demand thread is responsible for demanding the reservations
                # Helps improve the create response time

                # Add Reservations to relational database;
                new_slice_object.add_reservations()
                new_slice_object.update_topology(topology=topology)
                self.logger.info(f"OC wrapper: TIME= {time.time() - create_ts:.0f}")
                self.controller_state.get_defer_thread().queue_slice(controller_slice=new_slice_object)
                self.logger.info(f"QU queue: TIME= {time.time() - create_ts:.0f}")

            EventLoggerSingleton.get().log_slice_event(slice_object=slice_obj, action=ActionId.create,
                                                       topology=topology)

            controller.increment_metrics(project_id=project, oidc_sub=fabric_token.uuid)
            return ResponseBuilder.get_reservation_summary(res_list=computed_reservations)
        except Exception as e:
            if slice_id is not None and controller is not None and asm_graph is not None:
                FimHelper.delete_graph(graph_id=asm_graph.graph_id)
                controller.remove_slice(slice_id=slice_id)
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing create_slice e: {e}")
            raise e
        finally:
            if topology is not None and topology.graph_model is not None:
                topology.graph_model.delete_graph()
            if new_slice_object is not None:
                new_slice_object.unlock()
            self.logger.info(f"OH : TIME= {time.time() - start:.0f}")

    def get_slivers(self, *, token: str, slice_id: str, sliver_id: str = None, as_self: bool = True) -> List[dict]:
        """
        Get Slivers for a Slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param sliver_id Sliver Id
        :param as_self flag; True - return calling user's slivers otherwise, return all slivers in the project
        :raises Raises an exception in case of failure
        :returns List of reservations created for the Slice on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slivers invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            rid = ID(uid=sliver_id) if sliver_id is not None else None

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)

            # Filter slices based on user's user_id only when querying as_self
            user_id = fabric_token.uuid
            if not as_self:
                user_id = None

            reservations = controller.get_reservations(slice_id=slice_guid, rid=rid, oidc_claim_sub=user_id)
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

            return ResponseBuilder.get_reservation_summary(res_list=reservations)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slivers e: {e}")
            raise e

    def get_slices(self, *, token: str, states: List[str], name: str, limit: int, offset: int,
                   as_self: bool = True, search: str = None, exact_match: bool = False) -> List[dict]:
        """
        Get User Slices
        :param token Fabric Identity Token
        :param states Slice states
        :param name Slice name
        :param limit Number of slices to return
        :param offset Offset
        :param as_self flag; True - return calling user's slices otherwise, return all slices in the project
        :param search: search term applied
        :param exact_match: Exact Match for Search term
        :raises Raises an exception in case of failure
        :returns List of Slices on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slices invoked for Controller: {controller}")

            slice_states = SliceState.translate_list(states=states)

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)

            projects = fabric_token.projects
            project = None
            if len(projects) == 1:
                project, tags, project_name = fabric_token.first_project
            else:
                as_self = True

            # Filter slices based on user's user_id only when querying as_self
            user_id = fabric_token.uuid
            if not as_self:
                user_id = None
            slice_list = controller.get_slices(states=slice_states, user_id=user_id, project=project,
                                               slice_name=name, limit=limit, offset=offset)
            return ResponseBuilder.get_slice_summary(slice_list=slice_list)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slices e: {e}")
            raise e

    def modify_slice(self, *, token: str, slice_id: str, slice_graph: str) -> List[dict]:
        """
        Modify a slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param slice_graph Slice Graph Model
        :param ssh_key ssh_key
        :raises Raises an exception in case of failure
        :returns List of reservations created for the Slice on success
        """
        asm_graph = None
        topology = None
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"modify_slice invoked for Controller: {controller}")

            # Check if an Active slice exists already with the same name for the user
            slice_list = controller.get_slices(slice_id=slice_id)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            slice_obj = next(iter(slice_list))
            slice_obj.owner.token = token
            if slice_obj.get_graph_id() is None:
                raise OrchestratorException(f"Slice# {slice_obj} does not have graph id")

            slice_state = SliceState(slice_obj.get_state())

            if not SliceState.is_stable(state=slice_state):
                self.logger.info(f"Unable to modify Slice# {slice_id} that is not yet stable, try again later")
                raise OrchestratorException(f"Unable to modify Slice# {slice_id} that is not yet stable, "
                                            f"try again later")

            # Validate the slice graph
            topology = ExperimentTopology(graph_string=slice_graph, importer=NetworkXGraphImporterDisjoint())
            topology.validate()

            asm_graph = FimHelper.get_neo4j_asm_graph(slice_graph=slice_graph)

            # Authorize the slice
            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.modify, resource=topology)
            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.create, resource=topology)
            project, tags, project_name = fabric_token.first_project
            broker = self.get_broker(controller=controller)
            if broker is None:
                raise OrchestratorException("Unable to determine broker proxy for this controller. "
                                            "Please check Orchestrator container configuration and logs.")

            slice_object = OrchestratorSliceWrapper(controller=controller, broker=broker,
                                                    slice_obj=slice_obj, logger=self.logger)

            # Compute the reservations
            topology_diff, computed_reservations = slice_object.modify(new_slice_graph=asm_graph)

            # Check if Test Bed or site is in maintenance
            self.check_maintenance_mode(token=fabric_token, reservations=computed_reservations)

            # Add any new reservations to the database
            slice_object.add_reservations()

            # Slice has sliver modifications - add/remove/update for slivers requiring AM updates
            modify_state = slice_object.has_sliver_updates_at_authority()
            meta_data_updates = slice_object.has_meta_data_updates(topology_diff=topology_diff)

            if topology_diff is not None and (modify_state or meta_data_updates):
                slice_object.update_topology(topology=topology)
                FimHelper.delete_graph(graph_id=slice_obj.get_graph_id())
                graph_id = asm_graph.get_graph_id()
                slice_obj.graph_id = graph_id

            config_props = slice_obj.get_config_properties()
            config_props[Constants.PROJECT_ID] = project
            config_props[Constants.TAGS] = ','.join(tags)
            config_props[Constants.TOKEN_HASH] = fabric_token.token_hash
            slice_obj.set_config_properties(value=config_props)
            slice_obj.state = SliceState.Modifying.value

            if not controller.update_slice(slice_obj=slice_obj, modify_state=modify_state):
                self.logger.error(f"Failed to update slice: {slice_id} error: {controller.get_last_error()}")

            if modify_state:
                # Enqueue the slice on the demand thread
                # Demand thread is responsible for demanding the reservations
                # Helps improve the create response time
                self.controller_state.get_defer_thread().queue_slice(controller_slice=slice_object)
            # Sliver has meta data update
            else:
                self.logger.debug("Slice only has UserData updates")

            EventLoggerSingleton.get().log_slice_event(slice_object=slice_obj, action=ActionId.modify,
                                                       topology=topology)
            return ResponseBuilder.get_reservation_summary(res_list=computed_reservations)
        except Exception as e:
            if asm_graph is not None:
                FimHelper.delete_graph(graph_id=asm_graph.get_graph_id())

            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing modify_slice e: {e}")
            raise e
        finally:
            if topology is not None and topology.graph_model is not None:
                topology.graph_model.delete_graph()

    def delete_slices(self, *, token: str, slice_id: str = None):
        """
        Delete a user slice identified by slice_id if specified otherwise all user slices within a project
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :raises Raises an exception in case of failure
        """
        try:
            failed_to_delete_slice_ids = []
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"delete_slice invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.delete)
            project, tags, project_name = fabric_token.first_project

            self.logger.debug(f"Get Slices: {project} {fabric_token.email} {fabric_token.uuid}")
            states = None
            if slice_guid is None:
                states = [SliceState.Nascent.value,
                          SliceState.StableError.value,
                          SliceState.StableOK.value,
                          SliceState.ModifyOK.value,
                          SliceState.ModifyError.value,
                          SliceState.AllocatedError.value,
                          SliceState.AllocatedOK.value]
            slice_list = controller.get_slices(slice_id=slice_guid, user_id=fabric_token.uuid,
                                               project=project, states=states)

            if slice_list is None or len(slice_list) == 0:
                if slice_id is not None:
                    msg = f"Slice# {slice_id} not found"
                    raise OrchestratorException(msg, http_error_code=NOT_FOUND)

            self.__authorize_request(id_token=token, action_id=ActionId.delete)

            for slice_object in slice_list:
                slice_state = SliceState(slice_object.get_state())
                if SliceState.is_dead_or_closing(state=slice_state):
                    self.logger.debug(f"Slice# {slice_object.get_slice_id()} already closed")
                    continue

                if not SliceState.is_stable(state=slice_state) and not SliceState.is_modified(state=slice_state) and \
                        not SliceState.is_allocated(state=slice_state):
                    self.logger.info(f"Unable to delete Slice# {slice_object.get_slice_id()} that is not yet stable, "
                                     f"try again later")
                    failed_to_delete_slice_ids.append(slice_object.get_slice_id())
                    continue

                controller.close_reservations(slice_id=ID(uid=slice_object.get_slice_id()))
            if len(failed_to_delete_slice_ids) > 0:
                raise OrchestratorException(f"Unable to delete Slices {failed_to_delete_slice_ids} that are not yet "
                                            f"stable, try again later")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing delete_slice e: {e}")
            raise e

    def modify_accept(self, *, token: str, slice_id: str) -> dict:
        """
        Accept the last modify on the slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :raises Raises an exception in case of failure
        :returns Slice Graph on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"modify_accept invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None

            # TODO change this to accept
            self.__authorize_request(id_token=token, action_id=ActionId.modify)

            slice_list = controller.get_slices(slice_id=slice_guid)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            slice_obj = next(iter(slice_list))
            slice_state = SliceState(slice_obj.get_state())
            # Do not throw error if modify accept is received for a stable slice
            # Just return the success with slice topology
            #if not SliceState.is_modified(state=slice_state):
            #    self.logger.info(f"Unable to accept modify Slice# {slice_guid} that was not modified")
            #    raise OrchestratorException(f"Unable to accept modify Slice# {slice_guid} that was not modified")

            if slice_obj.get_graph_id() is None:
                raise OrchestratorException(f"Slice# {slice_obj} does not have graph id")

            if not SliceState.is_modified(state=slice_state):
                slice_topology = FimHelper.get_graph(graph_id=slice_obj.get_graph_id())
            # Prune the slice topology only if slice was modified
            else:
                slice_topology = FimHelper.prune_graph(graph_id=slice_obj.get_graph_id())

                controller.accept_update_slice(slice_id=ID(uid=slice_id))

            slice_model_str = slice_topology.serialize()
            return ResponseBuilder.get_slice_summary(slice_list=slice_list, slice_model=slice_model_str)[0]
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing modify_accept e: {e}")
            raise e

    def get_slice_graph(self, *, token: str, slice_id: str, graph_format_str: str, as_self: bool) -> dict:
        """
        Get User Slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param graph_format_str
        :param as_self flag; True - return calling user's slices otherwise, return all slices in the project
        :raises Raises an exception in case of failure
        :returns Slice Graph on success
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_slice_graph invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)

            # Filter slices based on user's user_id only when querying as_self
            user_id = fabric_token.uuid
            if not as_self:
                user_id = None

            slice_list = controller.get_slices(slice_id=slice_guid, user_id=user_id)
            if slice_list is None or len(slice_list) == 0:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"User# has no Slices",
                                            http_error_code=NOT_FOUND)

            slice_obj = next(iter(slice_list))

            slice_model_str = None
            if graph_format_str != "NONE":
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
            return ResponseBuilder.get_slice_summary(slice_list=slice_list, slice_model=slice_model_str)[0]
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_slice_graph e: {e}")
            raise e

    def renew_slice(self, *, token: str, slice_id: str, new_lease_end_time: datetime):
        """
        Renew a slice
        :param token Fabric Identity Token
        :param slice_id Slice Id
        :param new_lease_end_time: New Lease End Time in UTC in '%Y-%m-%d %H:%M:%S %z' format
        :raises Raises an exception in case of failure
        :return:
        """
        failed_to_extend_rid_list = []
        extend_rid_list = []
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"renew_slice invoked for Controller: {controller}")

            slice_guid = ID(uid=slice_id) if slice_id is not None else None
            slice_list = controller.get_slices(slice_id=slice_guid)

            if slice_list is None or len(slice_list) == 0:
                raise OrchestratorException(f"Slice# {slice_id} not found",
                                            http_error_code=NOT_FOUND)

            slice_object = next(iter(slice_list))
            slice_object.owner.token = token

            slice_state = SliceState(slice_object.get_state())
            if SliceState.is_dead_or_closing(state=slice_state):
                raise OrchestratorException(f"Slice# {slice_id} already closed",
                                            http_error_code=BAD_REQUEST)

            if not SliceState.is_stable(state=slice_state) and not SliceState.is_modified(state=slice_state):
                self.logger.info(f"Unable to renew Slice# {slice_guid} that is not yet stable, try again later")
                raise OrchestratorException(f"Unable to renew Slice# {slice_guid} that is not yet stable, "
                                            f"try again later")

            from fabric_cf.actor.security.access_checker import AccessChecker
            fabric_token = AccessChecker.validate_and_decode_token(token=token)
            project, tags, project_name = fabric_token.first_project
            allow_long_lived = True if Constants.SLICE_NO_LIMIT_LIFETIME in tags else False
            _, new_end_time = self.__compute_lease_end_time(lease_end_time=new_lease_end_time,
                                                            allow_long_lived=allow_long_lived,
                                                            project_id=project)

            reservations = controller.get_reservations(slice_id=slice_id)
            if reservations is None or len(reservations) < 1:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                raise OrchestratorException(f"Slice# {slice_id} has no reservations")

            self.logger.debug(f"There are {len(reservations)} reservations in the slice# {slice_id}")

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.renew,
                                                    lease_end_time=new_end_time)
            self.check_maintenance_mode(token=fabric_token, reservations=reservations)
            for r in reservations:
                res_state = ReservationStates(r.get_state())
                if res_state in [ReservationStates.Closed, ReservationStates.Failed, ReservationStates.CloseWait,
                                 ReservationStates.CloseFail]:
                    continue

                current_end_time = ActorClock.from_milliseconds(milli_seconds=r.get_end())

                if new_end_time < current_end_time:
                    raise OrchestratorException(f"Attempted new term end time is shorter than current slice end time")

                if new_end_time == current_end_time:
                    continue

                self.logger.debug(f"Extending reservation with reservation# {r.get_reservation_id()} "
                                  f"new_end_time: {new_end_time}")
                result = controller.extend_reservation(reservation=ID(uid=r.get_reservation_id()),
                                                       new_end_time=new_end_time,
                                                       sliver=None)
                if not result:
                    self.logger.error(f"Error: {controller.get_last_error()}")
                    failed_to_extend_rid_list.append(r.get_reservation_id())
                else:
                    extend_rid_list.append(r.get_reservation_id())

            '''
            if len(failed_to_extend_rid_list) == 0:
                slice_object.set_lease_end(lease_end=new_end_time)
                if not controller.update_slice(slice_obj=slice_object):
                    self.logger.error(f"Failed to update lease end time: {new_end_time} in Slice: {slice_object}")
                    self.logger.error(controller.get_last_error())
            '''

            if len(failed_to_extend_rid_list) > 0:
                raise OrchestratorException(f"Failed to extend reservation# {failed_to_extend_rid_list}")

            if len(extend_rid_list):
                slice_object.state = SliceState.Configuring.value
                if not controller.update_slice(slice_obj=slice_object, modify_state=True):
                    self.logger.error(f"Failed to update slice: {slice_id} error: {controller.get_last_error()}")

            EventLoggerSingleton.get().log_slice_event(slice_object=slice_object, action=ActionId.renew)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing renew e: {e}")
            raise e

    @staticmethod
    def validate_lease_time(lease_time: str) -> Union[datetime, None]:
        """
        Validate Lease Time
        :param lease_time: Lease Time
        :return Lease Time
        :raises Exception if new lease time is in past
        """
        if lease_time is None:
            return lease_time
        try:
            new_time = datetime.strptime(lease_time, Constants.LEASE_TIME_FORMAT)
        except Exception as e:
            raise OrchestratorException(f"Lease Time is not in format {Constants.LEASE_TIME_FORMAT}",
                                        http_error_code=BAD_REQUEST)

        now = datetime.now(timezone.utc)
        if new_time <= now:
            raise OrchestratorException(f"New lease time {new_time} is in the past! ",
                                        http_error_code=BAD_REQUEST)

        return new_time

    def __compute_lease_end_time(self, lease_end_time: datetime = None, allow_long_lived: bool = False,
                                 project_id: str = None,
                                 lifetime: int = Constants.DEFAULT_LEASE_IN_HOURS) -> tuple[datetime, datetime]:
        """
        Validate and compute Lease End Time.

        :param lease_end_time: The requested end time for the lease.
        :param allow_long_lived: If True, allows extended duration for leases.
        :param project_id: Project ID to check for special duration limits.
        :param lifetime: Requested lease duration in hours. Defaults to the system-defined lease duration.
        :return: A tuple containing the start time (current time) and the computed end time.
        :raises ValueError: If the lease end time is in the past.
        """
        base_time = datetime.now(timezone.utc)

        # Raise an error if lease_end_time is in the past
        if lease_end_time and lease_end_time < base_time:
            raise ValueError("Requested lease end time is in the past.")

        default_max_duration = (Constants.LONG_LIVED_SLICE_TIME_WEEKS
                                if allow_long_lived else Constants.DEFAULT_MAX_DURATION_IN_WEEKS).total_seconds()
        # Convert weeks to hours
        default_max_duration /= 3600

        # Calculate lifetime if not directly provided
        if lease_end_time:
            lifetime = (lease_end_time - base_time).total_seconds() / 3600
        else:
            if not lifetime:
                lifetime = Constants.DEFAULT_LEASE_IN_HOURS

        # Ensure the requested lifetime does not exceed allowed max duration for the project
        if project_id not in self.infrastructure_project_id and lifetime > default_max_duration:
            self.logger.info(f"Requested lifetime ({lifetime} hours) exceeds the allowed duration "
                             f"({default_max_duration} hours). Setting to maximum allowable.")
            lifetime = default_max_duration

        # Calculate the new end time
        new_end_time = base_time + timedelta(hours=lifetime)

        return base_time, new_end_time

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

    def check_maintenance_mode(self, *, token: FabricToken, reservations: List[ReservationMng] = None):
        controller = self.controller_state.get_management_actor()
        self.logger.debug(f"check_maintenance_mode invoked for Controller: {controller}")

        project, tags, project_name = token.first_project

        if not controller.is_slice_provisioning_allowed(project=project, email=token.email):
            raise OrchestratorException(Constants.MAINTENANCE_MODE_ERROR,
                                        http_error_code=Constants.INTERNAL_SERVER_ERROR_MAINT_MODE)

        if reservations is not None:
            for r in reservations:
                sliver = r.get_sliver()
                if not isinstance(sliver, NetworkServiceSliver):
                    worker = None
                    if sliver.get_labels() is not None and sliver.get_labels().instance_parent is not None:
                        worker = sliver.get_labels().instance_parent
                    status, message = controller.is_sliver_provisioning_allowed(project=project,
                                                                                site=sliver.get_site(),
                                                                                email=token.email,
                                                                                worker=worker)
                    if not status:
                        raise OrchestratorException(message=message,
                                                    http_error_code=Constants.INTERNAL_SERVER_ERROR_MAINT_MODE)

    def poa(self, *, token: str, sliver_id: str, poa: PoaAvro) -> tuple[str, str]:
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"poa invoked for Controller: {controller} pos: {poa}")

            rid = ID(uid=sliver_id) if sliver_id is not None else None

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.POA,
                                                    poa_operation=poa.operation)
            user_id = fabric_token.uuid
            project, tags, project_name = fabric_token.first_project

            auth = AuthAvro()
            auth.name = self.controller_state.get_management_actor().get_name()
            auth.guid = self.controller_state.get_management_actor().get_guid()
            auth.oidc_sub_claim = fabric_token.uuid
            auth.email = fabric_token.email
            poa.auth = auth
            poa.project_id = project
            poa.rid = sliver_id

            reservations = controller.get_reservations(rid=rid, oidc_claim_sub=user_id)
            if reservations is None or len(reservations) != 1:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                    if controller.get_last_error().status.code == ErrorCodes.ErrorNoSuchReservation:
                        raise OrchestratorException(f"Reservation# {rid} not found",
                                                    http_error_code=NOT_FOUND)

                raise OrchestratorException(f"Reservation# {rid} not found",
                                            http_error_code=NOT_FOUND)

            res_state = ReservationStates(reservations[0].get_state())

            if res_state != ReservationStates.Active:
                raise OrchestratorException(f"Cannot trigger POA; Reservation# {rid} is not {ReservationStates.Active}")

            if not controller.poa(poa=poa):
                raise OrchestratorException(f"Failed to trigger POA: "
                                            f"{controller.get_last_error().get_status().get_message()}")
            self.logger.debug(f"POA {poa.operation}/{sliver_id} added successfully")
            return poa.poa_id, reservations[0].get_slice_id()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing poa e: {e}")
            raise e

    def get_poas(self, *, token: str, sliver_id: str = None, poa_id: str = None, states: List[str] = None,
                 limit: int = 200, offset: int = 0):
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"poa invoked for Controller: {controller}")

            rid = ID(uid=sliver_id) if sliver_id is not None else None

            fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)
            email = fabric_token.email
            project, tags, project_name = fabric_token.first_project

            poa_states = PoaStates.translate_list(states=states)

            auth = AuthAvro()
            auth.name = self.controller_state.get_management_actor().get_name()
            auth.guid = self.controller_state.get_management_actor().get_guid()
            auth.oidc_sub_claim = fabric_token.uuid
            auth.email = fabric_token.email

            poa_list = controller.get_poas(rid=rid, poa_id=poa_id, project_id=project,
                                           states=poa_states, limit=limit, offset=offset)
            if poa_list is None:
                if controller.get_last_error() is not None:
                    self.logger.error(controller.get_last_error())
                    if controller.get_last_error().status.code == ErrorCodes.ErrorNoSuchPoa:
                        raise OrchestratorException(f"Reservation# {rid} not found",
                                                    http_error_code=NOT_FOUND)

                raise OrchestratorException(f"{controller.get_last_error()}")
            return ResponseBuilder.get_poa_summary(poa_list=poa_list)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing poa e: {e}")
            raise e

    def get_metrics_overview(self, *, token: str = None, excluded_projects: List[str] = None):
        """
        Get metrics overview
        :param token: token
        :param excluded_projects: list of projects to exclude
        """
        try:
            controller = self.controller_state.get_management_actor()
            self.logger.debug(f"get_metrics_overview invoked for Controller: {controller}")

            project = None
            user_id = None
            # Filter based on project_id and user_id when token is provided
            if token:
                fabric_token = self.__authorize_request(id_token=token, action_id=ActionId.query)
                projects = fabric_token.projects
                if len(projects) == 1:
                    project, tags, project_name = fabric_token.first_project
                user_id = fabric_token.uuid

            if excluded_projects:
                excluded_projects.extend(self.excluded_projects)
            else:
                excluded_projects = self.excluded_projects

            active_states = SliceState.list_values_ex_closing_dead()
            active_slice_count = controller.get_slice_count(states=active_states, user_id=user_id, project=project,
                                                            excluded_projects=excluded_projects)
            non_active_metrics = controller.get_metrics(oidc_sub=user_id, project_id=project,
                                                        excluded_projects=excluded_projects)
            total_slices = 0
            for m in non_active_metrics:
                total_slices += m.get("slice_count", 0)
            if not user_id and not project:
                # Get Seed value from config
                total_slices += self.total_slice_count_seed
            result = {
                "slices": {
                    "active_cumulative": active_slice_count,
                    "non_active_cumulative": total_slices
                }
            }
            return result
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing get_metrics_overview e: {e}")
            raise e

    def validate_token(self, *, token: str):
        """
        Validate and authorize token for query operations.
        :param token: Fabric Identity Token
        """
        self.__authorize_request(id_token=token, action_id=ActionId.query)

    @staticmethod
    def _reports_api_exception_to_orchestrator_exception(e: Exception) -> OrchestratorException:
        """
        Convert a Reports API exception into an OrchestratorException,
        preserving the HTTP status code and error details when possible.

        The Reports API client raises plain Exceptions with messages like:
          "Failed to find slot: 400 - {\"errors\": [{\"details\": \"...\"}]}"
        This method parses out the status code and detail text so the
        orchestrator can return a proper HTTP error to the caller.
        """
        msg = str(e)
        # Try to extract "NNN - {json}" from the message
        m = re.search(r':\s*(\d{3})\s*-\s*(\{.*)', msg, re.DOTALL)
        if m:
            status_code = int(m.group(1))
            body = m.group(2)
            # Try to pull the details text from the JSON body
            try:
                payload = json.loads(body)
                errors = payload.get("errors", [])
                if errors and isinstance(errors[0], dict):
                    detail = errors[0].get("details", msg)
                else:
                    detail = msg
            except (json.JSONDecodeError, IndexError):
                detail = msg
            return OrchestratorException(http_error_code=status_code, message=detail)
        return OrchestratorException(message=msg)

    def list_resources_calendar(self, *, token: str, start_date: str, end_date: str,
                                 interval: str = "day", site: list = None, host: list = None,
                                 exclude_site: list = None, exclude_host: list = None) -> dict:
        """
        Proxy resource availability calendar from reports API.
        :param token: Fabric Identity Token
        :param start_date: Start time (ISO 8601)
        :param end_date: End time (ISO 8601)
        :param interval: Time interval (day or week)
        :param site: Filter by site
        :param host: Filter by host
        :param exclude_site: Exclude sites
        :param exclude_host: Exclude hosts
        :returns dict with calendar data
        """
        try:
            reports_conf = self.config.get_reports_api()
            if not reports_conf or not reports_conf.get("enable", False):
                raise OrchestratorException(message="Reports API is not enabled")

            reports_host = reports_conf.get("host")
            reports_token = reports_conf.get("token")
            if not reports_host:
                raise OrchestratorException(message="Reports API host is not configured")

            from fabric_reports_client.reports_api import ReportsApi
            reports_api = ReportsApi(base_url=reports_host, token=reports_token)
            return reports_api.query_calendar(
                start_time=start_date, end_time=end_date,
                interval=interval, site=site, host=host,
                exclude_site=exclude_site, exclude_host=exclude_host
            )
        except OrchestratorException:
            raise
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing list_resources_calendar e: {e}")
            raise self._reports_api_exception_to_orchestrator_exception(e)

    def find_resource_slot(self, *, token: str, body: dict) -> dict:
        """
        Find time windows where requested resources are simultaneously available.

        When ``use_live_data`` is True, capacity data comes from the cached
        resource summary (BQM) and allocation data is queried live from the
        orchestrator DB.  Otherwise the request is proxied to the Reports API.

        :param token: Fabric Identity Token
        :param body: Request body with start, end, duration, resources, max_results, use_live_data
        :returns dict with find-slot results
        """
        try:
            self.__authorize_request(id_token=token, action_id=ActionId.query)

            if body.get("use_live_data", False):
                return self._find_slot_live(body=body, token=token)

            reports_conf = self.config.get_reports_api()
            if not reports_conf or not reports_conf.get("enable", False):
                raise OrchestratorException(message="Reports API is not enabled")

            reports_host = reports_conf.get("host")
            reports_token = reports_conf.get("token")
            if not reports_host:
                raise OrchestratorException(message="Reports API host is not configured")

            from fabric_reports_client.reports_api import ReportsApi
            reports_api = ReportsApi(base_url=reports_host, token=reports_token)
            return reports_api.find_slot(
                start_time=body.get("start"),
                end_time=body.get("end"),
                duration=body.get("duration"),
                resources=body.get("resources"),
                max_results=body.get("max_results", 1)
            )
        except OrchestratorException:
            raise
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(f"Exception occurred processing find_resource_slot e: {e}")
            raise self._reports_api_exception_to_orchestrator_exception(e)

    # ------------------------------------------------------------------
    # Live find-slot implementation
    # ------------------------------------------------------------------

    def _find_slot_live(self, *, body: dict, token: str) -> dict:
        """
        Compute find-slot using the cached resource summary for capacity data
        and the orchestrator DB for live allocation data.

        :param body: Request body with start, end, duration, resources, max_results
        :param token: Fabric Identity Token (used for resource summary query)
        :returns dict matching the Reports API find-slot response format
        """
        from collections import defaultdict
        from fim.user import ServiceType

        start_str = body.get("start")
        end_str = body.get("end")
        duration = body.get("duration")
        resources = body.get("resources", [])
        max_results = body.get("max_results", 1)

        start_time = datetime.fromisoformat(start_str)
        end_time = datetime.fromisoformat(end_str)

        total_hours = int((end_time - start_time).total_seconds() // 3600)
        if total_hours < duration:
            return self._empty_find_slot_result(start_time, end_time, duration)

        compute_requests = [r for r in resources if r.get("type") == "compute"]
        link_requests = [r for r in resources if r.get("type") == "link"]
        fp_requests = [r for r in resources if r.get("type") == "facility_port"]

        # --- Capacity data from cached resource summary ---
        summary = self.list_resources_summary(level=2, token=token, authorize=False)

        # Build host capacity map: {host_name: {site, cores_capacity, ram_capacity, disk_capacity, components}}
        host_cap_map = {}
        hosts_by_site = defaultdict(list)
        for h in summary.get("hosts", []):
            hname = h["name"]
            host_cap_map[hname] = {
                "site": h["site"],
                "cores_capacity": h.get("cores_capacity", 0),
                "ram_capacity": h.get("ram_capacity", 0),
                "disk_capacity": h.get("disk_capacity", 0),
                "components": {k.lower(): v.get("capacity", 0)
                               for k, v in h.get("components", {}).items()},
            }
            hosts_by_site[h["site"]].append(hname)

        # Build link capacity map: {(site_a, site_b) sorted: {bandwidth_capacity}}
        link_cap_map = {}
        for lnk in summary.get("links", []):
            sites = lnk.get("sites")
            if sites and len(sites) == 2:
                pair = tuple(sorted(sites))
                bw = lnk.get("bandwidth") or 0
                if pair not in link_cap_map or bw > link_cap_map[pair]["bandwidth_capacity"]:
                    link_cap_map[pair] = {"bandwidth_capacity": bw}

        # Build facility port capacity map: {(fp_name, site): {total_vlans}}
        fp_cap_map = {}
        for fp in summary.get("facility_ports", []):
            fp_name = fp.get("name")
            fp_site = fp.get("site")
            vlans_str = fp.get("vlans")
            total_vlans = 0
            if vlans_str:
                try:
                    import ast
                    vlan_list = ast.literal_eval(vlans_str)
                    if isinstance(vlan_list, list):
                        total_vlans = len(vlan_list)
                    elif isinstance(vlan_list, str) and "-" in vlan_list:
                        parts = vlan_list.split("-")
                        total_vlans = int(parts[1]) - int(parts[0]) + 1
                except Exception:
                    pass
            if fp_name and fp_site:
                fp_cap_map[(fp_name, fp_site)] = {"total_vlans": total_vlans}

        # Early exit if no capacity data for requested resource types
        if compute_requests and not host_cap_map:
            return self._empty_find_slot_result(start_time, end_time, duration)
        if link_requests and not link_cap_map:
            return self._empty_find_slot_result(start_time, end_time, duration)
        if fp_requests and not fp_cap_map:
            return self._empty_find_slot_result(start_time, end_time, duration)

        # --- Live allocation data from orchestrator DB ---
        controller = self.controller_state.get_management_actor()
        states = [ReservationStates.Active.value,
                  ReservationStates.ActiveTicketed.value,
                  ReservationStates.Ticketed.value]

        # Query compute reservations (NodeSlivers) — need full sliver for cores/ram/disk
        compute_reservations = []
        if compute_requests:
            existing = controller.get_reservations(states=states, start=start_time,
                                                   end=end_time, full=True) or []
            for r in existing:
                sliver = r.get_sliver()
                if isinstance(sliver, NodeSliver):
                    r_start = ActorClock.from_milliseconds(milli_seconds=r.get_start())
                    r_end = ActorClock.from_milliseconds(milli_seconds=r.get_end())
                    host = None
                    if sliver.get_labels() and sliver.get_labels().instance_parent:
                        host = sliver.get_labels().instance_parent
                    alloc = sliver.capacity_allocations
                    caps = sliver.capacities
                    cores = (getattr(alloc, 'core', 0) or 0) if alloc else 0
                    cores = cores or ((getattr(caps, 'core', 0) or 0) if caps else 0)
                    ram = (getattr(alloc, 'ram', 0) or 0) if alloc else 0
                    ram = ram or ((getattr(caps, 'ram', 0) or 0) if caps else 0)
                    disk = (getattr(alloc, 'disk', 0) or 0) if alloc else 0
                    disk = disk or ((getattr(caps, 'disk', 0) or 0) if caps else 0)
                    compute_reservations.append({
                        "host": host,
                        "lease_start": r_start,
                        "lease_end": r_end,
                        "cores": cores,
                        "ram": ram,
                        "disk": disk,
                        "components": self._extract_reservation_components(sliver),
                    })

        # Query network service reservations (for links and/or facility ports)
        net_reservations = []
        if link_requests or fp_requests:
            svc_types = ",".join(str(x) for x in ServiceType)
            existing = controller.get_reservations(states=states, type=svc_types,
                                                   start=start_time, end=end_time,
                                                   full=True) or []
            for r in existing:
                sliver = r.get_sliver()
                if isinstance(sliver, NetworkServiceSliver):
                    r_start = ActorClock.from_milliseconds(milli_seconds=r.get_start())
                    r_end = ActorClock.from_milliseconds(milli_seconds=r.get_end())
                    iface_sites = []
                    iface_vlans = []
                    fp_name = None
                    if sliver.interface_info and sliver.interface_info.interfaces:
                        for ifs in sliver.interface_info.interfaces.values():
                            if ifs.get_site():
                                iface_sites.append(ifs.get_site())
                            labels = ifs.get_label_allocations() or ifs.get_labels()
                            if labels:
                                vlan = getattr(labels, 'vlan', None)
                                if vlan is not None:
                                    iface_vlans.append(vlan)
                                parent = getattr(labels, 'device_name', None)
                                if parent:
                                    fp_name = parent
                    bw = 0
                    if sliver.capacities:
                        bw = getattr(sliver.capacities, 'bw', 0) or 0
                    elif sliver.capacity_allocations:
                        bw = getattr(sliver.capacity_allocations, 'bw', 0) or 0
                    net_reservations.append({
                        "lease_start": r_start,
                        "lease_end": r_end,
                        "bandwidth": bw,
                        "sites": sorted(set(iface_sites)),
                        "vlans": iface_vlans,
                        "fp_name": fp_name,
                        "site": sliver.get_site(),
                    })

        # --- Sliding window search ---
        windows = []
        for h in range(total_hours - duration + 1):
            window_start = start_time + timedelta(hours=h)
            window_end = window_start + timedelta(hours=duration)

            if self._check_live_window(
                window_start, window_end, duration,
                compute_requests, link_requests, fp_requests,
                host_cap_map, hosts_by_site, compute_reservations,
                link_cap_map, net_reservations,
                fp_cap_map,
            ):
                windows.append({
                    "start": window_start.isoformat(),
                    "end": window_end.isoformat(),
                })
                if len(windows) >= max_results:
                    break

        return {
            "windows": windows,
            "total": len(windows),
            "search_start": start_time.isoformat(),
            "search_end": end_time.isoformat(),
            "duration_hours": duration,
        }

    @staticmethod
    def _empty_find_slot_result(start_time: datetime, end_time: datetime, duration: int) -> dict:
        return {
            "windows": [],
            "total": 0,
            "search_start": start_time.isoformat(),
            "search_end": end_time.isoformat(),
            "duration_hours": duration,
        }

    @staticmethod
    def _extract_reservation_components(sliver: NodeSliver) -> list:
        """Extract component type-model keys from a NodeSliver's attached components."""
        comps = []
        info = sliver.attached_components_info
        if not info:
            return comps
        for comp_type, comp_list in info.by_type.items():
            for c in comp_list:
                units = 1
                if c.get_capacity_allocations() and getattr(c.get_capacity_allocations(), 'unit', None):
                    units = c.get_capacity_allocations().unit
                elif c.get_capacities() and getattr(c.get_capacities(), 'unit', None):
                    units = c.get_capacities().unit
                comp_key = f"{c.resource_type}-{c.resource_model}".lower()
                for _ in range(units):
                    comps.append(comp_key)
        return comps

    @staticmethod
    def _check_live_window(window_start, window_end, duration,
                           compute_requests, link_requests, fp_requests,
                           host_cap_map, hosts_by_site, compute_reservations,
                           link_cap_map, net_reservations,
                           fp_cap_map) -> bool:
        """Check if all resource requests can be satisfied in every hour of the window."""

        # --- Compute check (greedy bin-pack per hour) ---
        if compute_requests:
            for dh in range(duration):
                hour_start = window_start + timedelta(hours=dh)
                hour_end = hour_start + timedelta(hours=1)

                # Build remaining capacity for each host at this hour
                remaining = {}
                for host_name, cap in host_cap_map.items():
                    remaining[host_name] = {
                        "cores": cap["cores_capacity"],
                        "ram": cap["ram_capacity"],
                        "disk": cap["disk_capacity"],
                        "components": dict(cap["components"]),
                    }

                # Subtract live allocations overlapping this hour
                for rsv in compute_reservations:
                    if rsv["lease_start"] < hour_end and rsv["lease_end"] > hour_start:
                        h = rsv["host"]
                        if h and h in remaining:
                            remaining[h]["cores"] -= rsv["cores"]
                            remaining[h]["ram"] -= rsv["ram"]
                            remaining[h]["disk"] -= rsv["disk"]
                            for comp_key in rsv["components"]:
                                if comp_key in remaining[h]["components"]:
                                    remaining[h]["components"][comp_key] -= 1

                # Greedy bin-pack each compute request
                for req in compute_requests:
                    req_cores = req.get("cores", 0)
                    req_ram = req.get("ram", 0)
                    req_disk = req.get("disk", 0)
                    req_components = req.get("components", {})
                    req_site = req.get("site")

                    candidate_hosts = (
                        [hid for hid in hosts_by_site.get(req_site, []) if hid in remaining]
                        if req_site
                        else list(remaining.keys())
                    )

                    placed = False
                    for host_name in candidate_hosts:
                        rem = remaining[host_name]
                        if rem["cores"] < req_cores:
                            continue
                        if rem["ram"] < req_ram:
                            continue
                        if rem["disk"] < req_disk:
                            continue
                        comp_ok = True
                        for comp_key, comp_count in req_components.items():
                            if rem["components"].get(comp_key.lower(), 0) < comp_count:
                                comp_ok = False
                                break
                        if not comp_ok:
                            continue
                        # Place on this host
                        rem["cores"] -= req_cores
                        rem["ram"] -= req_ram
                        rem["disk"] -= req_disk
                        for comp_key, comp_count in req_components.items():
                            rem["components"][comp_key.lower()] -= comp_count
                        placed = True
                        break

                    if not placed:
                        return False

        # --- Link check ---
        for req in link_requests:
            pair = tuple(sorted([req["site_a"], req["site_b"]]))
            cap_entry = link_cap_map.get(pair)
            if not cap_entry:
                return False
            bw_cap = cap_entry["bandwidth_capacity"]
            req_bw = req["bandwidth"]

            for dh in range(duration):
                hour_start = window_start + timedelta(hours=dh)
                hour_end = hour_start + timedelta(hours=1)

                bw_used = 0
                for ns in net_reservations:
                    if ns["lease_start"] < hour_end and ns["lease_end"] > hour_start:
                        if len(ns["sites"]) == 2 and tuple(ns["sites"]) == pair:
                            bw_used += ns["bandwidth"]

                if bw_cap - bw_used < req_bw:
                    return False

        # --- Facility port check ---
        for req in fp_requests:
            req_name = req["name"]
            req_site = req["site"]
            req_vlans = req["vlans"]

            cap_entry = fp_cap_map.get((req_name, req_site))
            if not cap_entry:
                return False
            total_vlans = cap_entry["total_vlans"]

            for dh in range(duration):
                hour_start = window_start + timedelta(hours=dh)
                hour_end = hour_start + timedelta(hours=1)

                vlans_in_use = set()
                for ns in net_reservations:
                    if ns["lease_start"] < hour_end and ns["lease_end"] > hour_start:
                        ns_fp = ns.get("fp_name")
                        ns_site = ns.get("site")
                        if ns_fp == req_name and ns_site == req_site:
                            for v in ns["vlans"]:
                                vlans_in_use.add(v)

                if total_vlans - len(vlans_in_use) < req_vlans:
                    return False

        return True
