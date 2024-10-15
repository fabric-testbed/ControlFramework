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
import threading

from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates

from fabric_cf.actor.fim.fim_helper import FimHelper
from fim.user import GraphFormat, ServiceType, NodeType

from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
from fabric_cf.actor.core.apis.abc_mgmt_controller_mixin import ABCMgmtControllerMixin
from fabric_cf.actor.core.apis.abc_tick import ABCTick
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.event_processor import EventProcessor
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.bqm_wrapper import BqmWrapper
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.reservation_status_update_thread import ReservationStatusUpdateThread
from fabric_cf.orchestrator.core.slice_defer_thread import SliceDeferThread


class PollEvent(ABCActorEvent):
    def __init__(self, *, model_level_list: list):
        self.model_level_list = model_level_list

    def process(self):
        from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
        oh = OrchestratorHandler()
        for graph_format, level in self.model_level_list:
            oh.discover_broker_query_model(controller=oh.controller_state.controller,
                                           graph_format=graph_format, force_refresh=True,
                                           level=level)


class OrchestratorKernel(ABCTick):
    """
    Class responsible for starting Orchestrator Threads; also holds Management Actor and Broker information
    """

    def __init__(self):
        self.defer_thread = None
        self.sut = None
        self.broker = None
        self.logger = None
        self.controller = None
        self.lock = threading.Lock()
        self.bqm_cache = {}
        self.event_processor = None
        self.combined_broker_model_graph_id = None
        self.combined_broker_model = None
        
    def get_saved_bqm(self, *, graph_format: GraphFormat, level: int) -> BqmWrapper:
        """
        Get Saved BQM from cache
        """
        try:
            self.lock.acquire()
            key = f"{graph_format}-{level}"
            saved_bqm = self.bqm_cache.get(key, None)
            return saved_bqm
        finally:
            self.lock.release()

    def save_bqm(self, *, bqm: str, graph_format: GraphFormat, level: int):
        try:
            self.lock.acquire()
            key = f"{graph_format}-{level}"
            saved_bqm = self.bqm_cache.get(key, None)
            if saved_bqm is None:
                from fabric_cf.actor.core.container.globals import GlobalsSingleton
                refresh_interval = GlobalsSingleton.get().get_config().get_global_config().get_bqm_config().get(
                    Constants.REFRESH_INTERVAL, None)
                saved_bqm = BqmWrapper()
                saved_bqm.set_refresh_interval(refresh_interval=int(refresh_interval))
            saved_bqm.save(bqm=bqm, graph_format=graph_format, level=level)
            self.bqm_cache[key] = saved_bqm

            if level == 0:
                self.load_model(model=bqm)
        finally:
            self.lock.release()

    def set_broker(self, *, broker: ID):
        """
        Set Broker
        :param broker:
        :return:
        """
        self.broker = broker

    def get_broker(self) -> ID:
        """
        Get Broker
        :return:
        """
        return self.broker

    def get_defer_thread(self) -> SliceDeferThread:
        return self.defer_thread

    def get_sut(self) -> ReservationStatusUpdateThread:
        """
        Get SUT thread
        :return:
        """
        return self.sut

    def get_logger(self):
        """
        Get logger
        :return:
        """
        if self.logger is None:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            self.logger = GlobalsSingleton.get().get_logger()
        return self.logger

    def get_management_actor(self) -> ABCMgmtControllerMixin:
        """
        Get Management actor
        :return:
        """
        if self.controller is None:
            self.controller = ManagementUtils.get_local_actor()
        return self.controller

    def stop_threads(self):
        """
        Stop threads
        :return:
        """
        if self.defer_thread is not None:
            self.defer_thread.stop()
        if self.event_processor is not None:
            self.event_processor.stop()
        #if self.sut is not None:
        #    self.sut.stop()

    def load_model(self, model: str):
        if self.combined_broker_model_graph_id:
            FimHelper.delete_graph(graph_id=self.combined_broker_model_graph_id)

        self.logger.debug(f"Loading an existing Combined Broker Model Graph")
        self.combined_broker_model = FimHelper.get_neo4j_cbm_graph_from_string_direct(
            graph_str=model, ignore_validation=True)
        self.combined_broker_model_graph_id = self.combined_broker_model.get_graph_id()
        self.logger.debug(
            f"Successfully loaded an Combined Broker Model Graph: {self.combined_broker_model_graph_id}")

    def start_threads(self):
        """
        Start threads
        :return:
        """
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        GlobalsSingleton.get().get_container().register(tickable=self)
        from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler
        oh = OrchestratorHandler()
        model = oh.discover_broker_query_model(controller=self.get_management_actor(),
                                               graph_format=GraphFormat.GRAPHML,
                                               force_refresh=True, level=0)
        self.load_model(model=model)

        self.get_logger().debug("Starting SliceDeferThread")
        self.defer_thread = SliceDeferThread(kernel=self)
        self.defer_thread.start()
        self.event_processor = EventProcessor(name="PeriodicProcessor", logger=self.logger)
        self.event_processor.start()
        #self.get_logger().debug("Starting ReservationStatusUpdateThread")
        #self.sut = ReservationStatusUpdateThread()
        #self.sut.start()

    def external_tick(self, *, cycle: int):
        """
        External tick
        @param cycle
        """
        try:
            self.lock.acquire()
            model_level_list = []
            for cached_bqm in self.bqm_cache.values():
                if cached_bqm.can_refresh():
                    model_level_list.append((cached_bqm.get_graph_format(), cached_bqm.get_level()))
            if self.event_processor is not None and len(model_level_list) > 0:
                self.event_processor.enqueue(incoming=PollEvent(model_level_list=model_level_list))
        except Exception as e:
            self.logger.error(f"Error occurred while doing periodic processing: {e}")
        finally:
            self.lock.release()

    def get_name(self) -> str:
        return self.__class__.__name__

    def determine_start_time(self, computed_reservations: list[LeaseReservationAvro]):
        """
        Given set of reservations; check if the requested resources are available
        - if resources are not available find the next available start time for them based on the existing allocations
        @param computed_reservations:
        @return:
        """
        for r in computed_reservations:
            requested_sliver = r.get_sliver()
            # TODO remove
            requested_sliver = NodeSliver()
            # TODO remove

            if not isinstance(requested_sliver, NodeSliver):
                continue

            candidate_nodes = FimHelper.candidate_nodes(combined_broker_model=self.combined_broker_model,
                                                        sliver=requested_sliver)

            if not candidate_nodes or len(candidate_nodes):
                raise Exception("Bad request!")

            states = [ReservationStates.Active.value,
                      ReservationStates.ActiveTicketed.value,
                      ReservationStates.Ticketed.value]

            res_type = []
            for x in ServiceType:
                res_type.append(str(x))
            for x in NodeType:
                res_type.append(str(x))

            for c in candidate_nodes:
                cbm_node = self.combined_broker_model.build_deep_node_sliver(node_id=c)
                # TODO remove
                cbm_node = NodeSliver()
                # TODO remove
                # Skip if CBM node is not the specific host that is requested
                if requested_sliver.get_labels() and requested_sliver.get_labels().instance_parent and \
                        requested_sliver.get_labels().instance_parent != cbm_node.get_name():
                    continue
                existing = self.get_management_actor().get_reservations(node_id=c, states=states)

                components = self.get_management_actor().get_components(node_id=c, states=states,
                                                                        rsv_type=res_type)
                delegation_id = NetworkNodeInventory.check_capacities(
                    rid=ID(uid=r.get_reservation_id()),
                    requested_capacities=requested_sliver.get_capacities(),
                    delegated=cbm_node.get_capacity_delegations(),
                    existing_reservations=existing,
                    logger=self.logger,
                )


class OrchestratorKernelSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise OrchestratorException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = OrchestratorKernel()
        return self.__instance

    get = classmethod(get)
