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
from __future__ import annotations

import enum
import random
import threading
import traceback
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Tuple, List, Any, Dict

from fim.graph.abc_property_graph import ABCPropertyGraphConstants, GraphFormat, ABCPropertyGraph
from fim.graph.resources.abc_adm import ABCADMPropertyGraph
from fim.pluggable import PluggableRegistry, PluggableType
from fim.slivers.attached_components import ComponentSliver, ComponentType
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Labels, Capacities
from fim.slivers.interface_info import InterfaceType
from fim.slivers.network_node import NodeSliver, NodeType
from fim.slivers.network_service import NetworkServiceSliver, ServiceType, NSLayer
from fim.slivers.path_info import Path

from fabric_cf.actor.boot.configuration import ActorConfig
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.container.maintenance import Maintenance
from fabric_cf.actor.core.core.policy import AllocationAlgorithm
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicketFactory
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationOperation
from fabric_cf.actor.core.policy.broker_calendar_policy import BrokerCalendarPolicy
from fabric_cf.actor.core.policy.fifo_queue import FIFOQueue
from fabric_cf.actor.core.policy.network_node_inventory import NetworkNodeInventory
from fabric_cf.actor.core.policy.network_service_inventory import NetworkServiceInventory
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.bids import Bids
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.policy.inventory import Inventory
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.fim.plugins.broker.aggregate_bqm_plugin import AggregatedBQMPlugin
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fim.slivers.interface_info import InterfaceSliver

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin


class BrokerSimplerUnitsPolicy(BrokerCalendarPolicy):
    """
        BrokerSimplerUnitsPolicy is a simple implementation of the broker policy interface.
        It buffers requests for allocation periods and when it performs allocations of resources it does so
        in FIFO order giving preference to extending requests.
    """
    # The amount of time over specific policy decisions the broker
    # must add when communicating with other brokers as a client (e.g.
    # renew()). Clock skew must be at least one if the broker is ticked after
    # the upstream broker(s). At some point in time we may want this to not
    # be static and learn it from what we see in the system, but for now it
    # is static.
    CLOCK_SKEW = 1

    # Number of cycles between two consecutive allocations.
    CALL_INTERVAL = 1

    #  How far in the future is the broker allocating resources
    ADVANCE_TIME = 3

    def __init__(self, *, actor: ABCBrokerMixin = None):
        super().__init__(actor=actor)
        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.delegations = {}
        self.combined_broker_model = None
        self.combined_broker_model_graph_id = None
        self.query_cbm = None

        self.queue = FIFOQueue()
        self.inventory = Inventory()

        self.pluggable_registry = PluggableRegistry()
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['delegations']
        del state['combined_broker_model']
        del state['query_cbm']
        del state['lock']

        del state['calendar']

        del state['last_allocation']
        del state['allocation_horizon']
        del state['ready']
        del state['queue']
        del state['pluggable_registry']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.delegations = {}
        self.combined_broker_model = None
        self.query_cbm = None

        self.lock = threading.Lock()
        self.calendar = None

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.queue = FIFOQueue()
        self.pluggable_registry = PluggableRegistry()

    def load_combined_broker_model(self):
        if self.combined_broker_model_graph_id is None:
            self.logger.debug("Creating an empty Combined Broker Model Graph")
        else:
            self.logger.debug(f"Loading an existing Combined Broker Model Graph: {self.combined_broker_model_graph_id}")

        self.combined_broker_model = FimHelper.get_neo4j_cbm_graph(graph_id=self.combined_broker_model_graph_id)
        self.query_cbm = FimHelper.get_neo4j_cbm_graph(graph_id=self.combined_broker_model_graph_id)
        self.combined_broker_model_graph_id = self.combined_broker_model.get_graph_id()
        self.logger.debug(f"Successfully loaded an Combined Broker Model Graph: {self.combined_broker_model_graph_id}")
        self.pluggable_registry.register_pluggable(t=PluggableType.Broker, p=AggregatedBQMPlugin, actor=self.actor,
                                                   logger=self.logger)
        self.logger.debug(f"Registered AggregateBQMPlugin")

    def load_new_controls(self, *, config: ActorConfig):
        for i in config.get_controls():
            try:
                if i.get_module_name() is None or i.get_class_name() is None or i.get_type() is None or \
                        len(i.get_type()) == 0:
                    continue

                inventory = ReflectionUtils.create_instance(module_name=i.get_module_name(),
                                                            class_name=i.get_class_name())
                inventory.set_logger(logger=self.logger)

                for t in i.get_type():
                    self.logger.debug(f"Processing control type: {t}")
                    rtype = ResourceType(resource_type=t)
                    existing = self.inventory.get(resource_type=rtype)
                    if existing is None:
                        self.logger.debug(f"Registering control type: {t} inventory: {type(inventory)}")
                        self.register_inventory(resource_type=rtype, inventory=inventory)
                    else:
                        self.logger.debug(f"Exists control type: {t} inventory: {type(inventory)}")
            except Exception as e:
                self.logger.error(f"Exception occurred while loading new control: {e}")
                self.logger.error(traceback.format_exc())

    def initialize(self, *, config: ActorConfig):
        if not self.initialized:
            super().initialize(config=config)
            self.load_combined_broker_model()
            self.load_new_controls(config=config)
            self.initialized = True

    def register_inventory(self, *, resource_type: ResourceType, inventory: InventoryForType):
        """
        Registers the given inventory for the specified resource type. If the
        policy plugin has already been initialized, the inventory should be
        initialized.

        @param inventory: the inventory
        @param resource_type: resource_type
        """
        self.inventory.add_inventory_by_type(rtype=resource_type, inventory=inventory)

    def bind(self, *, reservation: ABCBrokerReservation) -> bool:
        term = reservation.get_requested_term()
        self.logger.info(f"SlottedAgent bind arrived at cycle {self.actor.get_current_cycle()} requested term {term}")

        bid_cycle = self.get_allocation(reservation=reservation)
        self.logger.info(f"SlottedAgent bind assigned cycle: {bid_cycle} requested term {term}")

        self.calendar.add_request(reservation=reservation, cycle=bid_cycle)

        return False

    def bind_delegation(self, *, delegation: ABCDelegation) -> bool:
        try:
            self.lock.acquire()
            self.delegations[delegation.get_delegation_id()] = delegation
        finally:
            self.lock.release()

        return False

    def extend_broker(self, *, reservation: ABCBrokerReservation) -> bool:
        requested_term = reservation.get_requested_term()
        self.logger.info("SlottedAgent extend arrived at cycle {} requested term {}".format(
            self.actor.get_current_cycle(), requested_term))

        source = reservation.get_source()

        if source is None:
            self.error(message="cannot find parent ticket for extend")

        if source.is_failed():
            self.error(message="parent ticket could not be renewed")

        bid_cycle = self.get_allocation(reservation=reservation)

        self.calendar.add_request(reservation=reservation, cycle=bid_cycle, source=source)
        self.calendar.add_request(reservation=reservation, cycle=bid_cycle)

        return False

    def formulate_bids(self, *, cycle: int) -> Bids:
        renewing = self.calendar.get_renewing(cycle=cycle)
        extending = self.process_renewing(renewing=renewing)
        return Bids(ticketing=ReservationSet(), extending=extending)

    def get_allocation(self, *, reservation: ABCBrokerReservation) -> int:
        if not self.ready:
            self.error(message="Agent not ready to accept bids")

        start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

        start -= self.ADVANCE_TIME

        intervals = int((start - self.last_allocation)/self.CALL_INTERVAL)

        if intervals <= 0:
            intervals = 1

        # Hack for Advanced Scheduling; force the advanced slivers to be scheduled in next cycle
        now = datetime.now(timezone.utc)
        diff = (reservation.get_requested_term().get_new_start_time() - now).total_seconds()
        if diff > 120:
            intervals = 2

        start = self.last_allocation + (intervals * self.CALL_INTERVAL) + self.ADVANCE_TIME

        return start

    @staticmethod
    def get_approved_term(*, reservation: ABCBrokerReservation) -> Term:
        return Term(start=reservation.get_requested_term().get_start_time(),
                    end=reservation.get_requested_term().get_end_time(),
                    new_start=reservation.get_requested_term().get_new_start_time())

    def get_next_allocation(self, *, cycle: int) -> int:
        return self.last_allocation + self.CALL_INTERVAL

    def get_renew(self, *, reservation: ABCClientReservation) -> int:
        new_start_cycle = self.actor.get_actor_clock().cycle(when=reservation.get_term().get_end_time()) + 1
        return new_start_cycle - self.ADVANCE_TIME - self.CLOCK_SKEW

    def get_start_for_allocation(self, *, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME

    def get_end_for_allocation(self, *, allocation_cycle: int) -> int:
        return allocation_cycle + self.ADVANCE_TIME + self.allocation_horizon

    def prepare(self, *, cycle: int):
        if not self.ready:
            self.last_allocation = cycle - self.CALL_INTERVAL
            self.ready = True

        try:
            self.check_pending()
        except Exception as e:
            self.logger.error("Exception in prepare {}".format(e))

    def process_renewing(self, *, renewing: ReservationSet) -> ReservationSet:
        """
        Performs checks on renewing reservations. Updates the terms to
        suggest new terms, stores the extend on the pending list. Returns a
        fresh ReservationSet of expiring reservations to try to renew in this
        bidding cycle.

        @param renewing collection of the renewing reservations

        @return non-null set of renewals
        """
        result = ReservationSet()
        if renewing is None:
            return None

        #self.logger.debug("Expiring = {}".format(renewing.size()))

        for reservation in renewing.values():
            self.logger.debug("Expiring res: {}".format(reservation))

            if reservation.is_renewable():
                self.logger.debug("This is a renewable expiring reservation")

                term = reservation.get_term()

                term = term.extend()

                reservation.set_approved(term=term, approved_resources=reservation.get_resources().abstract_clone())

                result.add(reservation=reservation)
                self.calendar.add_pending(reservation=reservation)
            else:
                self.logger.debug("This is not a renewable expiring reservation")

        return result

    def allocate(self, *, cycle: int):
        if self.get_next_allocation(cycle=cycle) != cycle:
            return

        self.last_allocation = cycle

        start_cycle = self.get_start_for_allocation(allocation_cycle=cycle)
        advance_cycle = self.get_end_for_allocation(allocation_cycle=cycle)
        requests = self.calendar.get_all_requests(cycle=advance_cycle)

        if (requests is None or requests.size() == 0) and (self.queue is None or self.queue.size() == 0):
            self.logger.debug(f"requests: {requests} queue: {self.queue}")
            self.logger.debug(f"no requests for auction start cycle {start_cycle}")
            return

        self.logger.debug(f"allocating resources for cycle {start_cycle}")

        self.allocate_extending_reservation_set(requests=requests)
        self.allocate_queue(start_cycle=start_cycle)
        self.allocate_ticketing(requests=requests)

    def get_default_resource_type(self) -> ResourceType:
        result = None

        if len(self.inventory.get_inventory().keys()) > 0:
            result = self.inventory.get_inventory().keys().__iter__().__next__()

        return result

    def allocate_extending_reservation_set(self, *, requests: ReservationSet):
        if requests is not None:
            # Holds the Node Id to List of Reservation Ids allocated
            # This is used to check on the reservations allocated during this cycle to compute available resources
            # as the reservations are not updated in the database yet
            node_id_to_reservations = {}
            for reservation in requests.values():
                if reservation.is_extending_ticket() and not reservation.is_closed():
                    start = reservation.get_requested_term().get_new_start_time()
                    end = self.align_end(when=reservation.get_requested_term().get_end_time())

                    resource_type = reservation.get_resources().get_type()

                    inv = self.inventory.get(resource_type=resource_type)

                    if inv is not None:
                        ext_term = Term(start=reservation.get_term().get_start_time(), end=end, new_start=start)
                        self.extend_private(reservation=reservation, inv=inv, term=ext_term,
                                            node_id_to_reservations=node_id_to_reservations)
                    else:
                        reservation.fail(message=Constants.NO_POOL)

    def allocate_ticketing(self, *, requests: ReservationSet):
        if requests is not None:
            # Holds the Node Id to List of Reservation Ids allocated
            # This is used to check on the reservations allocated during this cycle to compute available resources
            # as the reservations are not updated in the database yet
            node_id_to_reservations = {}
            for reservation in requests.values():
                if not reservation.is_ticketing():
                    continue

                status, node_id_to_reservations, error_msg = self.ticket(reservation=reservation,
                                                                         node_id_to_reservations=node_id_to_reservations)
                if status:
                    continue

                if self.queue is None and not reservation.is_failed():
                    fail_message = "Insufficient resources"
                    if error_msg is not None:
                        fail_message = error_msg
                    reservation.fail(message=fail_message)
                    continue

                if not reservation.is_failed():
                    fail_message = f"Insufficient resources for specified start time, Failing reservation: " \
                                   f"{reservation.get_reservation_id()}"
                    if error_msg is not None:
                        fail_message = error_msg
                    reservation.fail(message=fail_message)

    def allocate_queue(self, *, start_cycle: int):
        if self.queue is None:
            return

        # Holds the Node Id to List of Reservation Ids allocated
        # This is used to check on the reservations allocated during this cycle to compute available resources
        # as the reservations are not updated in the database yet
        node_id_to_reservations = {}
        for reservation in self.queue.values():
            status, node_id_to_reservations, error_msg = self.ticket(reservation=reservation,
                                                                     node_id_to_reservations=node_id_to_reservations)
            if not status:
                # TODO
                threshold = 100
                start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

                if threshold != 0 and ((start_cycle - start) > threshold):
                    reservation.fail_warn(message=f"Request has exceeded its threshold on the queue {reservation}")
                    self.queue.remove(reservation=reservation)
            else:
                self.queue.remove(reservation=reservation)

    def ticket(self, *, reservation: ABCBrokerReservation, node_id_to_reservations: dict) -> Tuple[bool, dict, Any]:
        self.logger.debug(f"cycle: {self.actor.get_current_cycle()} new ticket request: "
                          f"{reservation}/{type(reservation).__name__}")

        error_msg = None
        start = self.align_start(when=reservation.get_requested_term().get_new_start_time())
        end = self.align_end(when=reservation.get_requested_term().get_end_time())

        resource_type = reservation.get_requested_resources().get_type()
        if resource_type is None or not self.inventory.contains_type(resource_type=resource_type):
            resource_type = self.get_default_resource_type()

        if resource_type is not None:
            inv = self.inventory.get(resource_type=resource_type)

            if inv is not None:
                self.logger.debug(f"Inventory type: {type(inv)}")
                term = Term(start=start, end=end)
                return self.ticket_inventory(reservation=reservation, inv=inv, term=term,
                                             node_id_to_reservations=node_id_to_reservations,
                                             operation=ReservationOperation.Create)
            else:
                reservation.fail(message=Constants.NO_POOL)
        else:
            reservation.fail(message=Constants.NO_POOL)

        return False, node_id_to_reservations, error_msg

    def __candidate_nodes(self, *, sliver: NodeSliver) -> List[str]:
        """
        Identify candidate worker nodes in this site that have at least
        as many needed components as in the sliver.
        """
        # modify; return existing node map
        if sliver.get_node_map() is not None:
            graph_id, node_id = sliver.get_node_map()
            return [node_id]

        node_props = {ABCPropertyGraphConstants.PROP_SITE: sliver.site,
                      ABCPropertyGraphConstants.PROP_TYPE: str(NodeType.Server)}
        if sliver.get_type() == NodeType.Switch:
            node_props[ABCPropertyGraphConstants.PROP_TYPE] = str(NodeType.Switch)

        storage_components = []
        # remove storage components before the check
        if sliver.attached_components_info is not None:
            for name, c in sliver.attached_components_info.devices.items():
                if c.get_type() == ComponentType.Storage:
                    storage_components.append(c)
            for c in storage_components:
                sliver.attached_components_info.remove_device(name=c.get_name())

        result = self.combined_broker_model.get_matching_nodes_with_components(
            label=ABCPropertyGraphConstants.CLASS_NetworkNode,
            props=node_props,
            comps=sliver.attached_components_info)

        if sliver.get_type() == NodeType.Switch:
            exclude = []
            for n in result:
                if "p4" not in n:
                    exclude.append(n)
            for e in exclude:
                result.remove(e)

        # re-add storage components
        if len(storage_components) > 0:
            for c in storage_components:
                sliver.attached_components_info.add_device(device_info=c)

        return result

    def __prune_nodes_in_maintenance(self, node_id_list: List[str], site: str, reservation: ABCBrokerReservation):
        """
        Prune the candidate node list to exclude the workers in Maintenance
        @param node_id_list: Candidate Node List identified to allocate the reservation
        @param site: Site Name
        @param reservation: Reservation to be allocated
        """
        project_id = reservation.get_slice().get_project_id()
        email = reservation.get_slice().get_owner().get_email()

        nodes_to_remove = []
        for node_id in node_id_list:
            graph_node = self.get_network_node_from_graph(node_id=node_id)
            status, error_message = Maintenance.is_sliver_provisioning_allowed(database=self.actor.get_plugin().get_database(),
                                                                               project=project_id, site=site,
                                                                               worker=graph_node.get_name(),
                                                                               email=email)
            if not status:
                self.logger.info(f"Excluding {graph_node.get_name()} as allocation candidate due to {error_message}")
                nodes_to_remove.append(node_id)

        for x in nodes_to_remove:
            node_id_list.remove(x)

        return node_id_list

    def __reshuffle_nodes(self, node_id_list: List[str], node_id_to_reservations: dict,
                          term: Term) -> List[str]:
        """
        Reshuffles nodes based on their usage compared to a given threshold.

        @param: node_id_list (list): List of node_ids

        @return:  list: Reshuffled list of nodes, with nodes exceeding the threshold shuffled separately.
        """
        if len(node_id_list) == 1:
            return node_id_list

        enabled, threshold = self.get_core_capacity_threshold()
        if not enabled:
            return node_id_list

        # Separate nodes based on whether their usage exceeds the threshold
        above_threshold = []
        below_threshold = []

        for node_id in node_id_list:
            node, total, allocated = self.get_node_capacities(node_id=node_id,
                                                              node_id_to_reservations=node_id_to_reservations,
                                                              term=term)
            if total and allocated:
                self.logger.debug(f"Allocated: {allocated} Total: {total}")
                cpu_usage_percent = int(((allocated.core * 100)/ total.core))
                self.logger.debug(f"CPU Usage for {node.get_name()}: {cpu_usage_percent}; "
                                  f"threshold: {threshold}")
                if cpu_usage_percent < threshold:
                    below_threshold.append(node_id)
                else:
                    above_threshold.append(node_id)

        # Combine both shuffled lists (you can choose the order of combining)
        reshuffled_nodes = below_threshold + above_threshold

        return reshuffled_nodes

    def __find_first_fit(self, node_id_list: List[str], node_id_to_reservations: dict, inv: NetworkNodeInventory,
                         reservation: ABCBrokerReservation, term: Term, sliver: NodeSliver,
                         operation: ReservationOperation = ReservationOperation.Create) -> Tuple[str, BaseSliver, Any]:
        """
        Find First Available Node which can serve the reservation
        @param node_id_list: Candidate Nodes
        @param node_id_to_reservations:
        @param inv: Inventory
        @param reservation: Reservation
        @return tuple containing delegation id, sliver, error message if any
        """
        delegation_id = None
        error_msg = None
        self.logger.debug(f"Possible candidates to serve {reservation} candidates# {node_id_list}")
        for node_id in node_id_list:
            try:
                self.logger.debug(f"Attempting to allocate {reservation} via graph_node# {node_id}")
                graph_node = self.get_network_node_from_graph(node_id=node_id)

                if sliver.labels is not None and sliver.labels.instance_parent is not None:
                    self.logger.info(f"Sliver {sliver} is requested on worker: "
                                     f"{sliver.labels.instance_parent}")
                    if graph_node.get_name() != sliver.labels.instance_parent:
                        self.logger.info(f"Skipping candidate node: {graph_node}")
                        continue

                existing_reservations = self.get_existing_reservations(node_id=node_id,
                                                                       node_id_to_reservations=node_id_to_reservations,
                                                                       start=term.get_start_time(),
                                                                       end=term.get_end_time())

                include_ns = False if operation == ReservationOperation.Extend else True
                existing_components = self.get_existing_components(node_id=node_id, start=term.get_start_time(),
                                                                   end=term.get_end_time(),
                                                                   excludes=[str(reservation.get_reservation_id())],
                                                                   include_ns=include_ns)

                delegation_id, sliver = inv.allocate(rid=reservation.get_reservation_id(),
                                                     requested_sliver=sliver,
                                                     graph_id=self.combined_broker_model_graph_id,
                                                     graph_node=graph_node,
                                                     existing_reservations=existing_reservations,
                                                     existing_components=existing_components,
                                                     operation=operation)

                if delegation_id is not None and sliver is not None:
                    break
            except BrokerException as e:
                if e.error_code == ExceptionErrorCode.INSUFFICIENT_RESOURCES:
                    self.logger.error(f"Exception occurred: {e}")
                    error_msg = e.msg
                else:
                    raise e

        if delegation_id is None and sliver.labels is not None and \
                sliver.labels.instance_parent is not None:
            error_msg = f"Insufficient Resources: {sliver.labels.instance_parent} " \
                        f"cannot serve the requested sliver - {error_msg}"

        return delegation_id, sliver, error_msg

    def __allocate_nodes(self, *, reservation: ABCBrokerReservation, inv: NetworkNodeInventory, sliver: NodeSliver,
                         node_id_to_reservations: dict, term: Term,
                         operation: ReservationOperation = ReservationOperation.Create) -> Tuple[str or None, BaseSliver, Any]:
        """
        Allocate Network Node Slivers
        @param reservation Reservation
        @param inv Inventory
        @param sliver Requested sliver
        @param node_id_to_reservations
        @return tuple containing delegation id, sliver, error message if any
        """
        delegation_id = None
        node_id_list = FimHelper.candidate_nodes(combined_broker_model=self.combined_broker_model,
                                                 sliver=sliver)
        if self.get_algorithm_type(site=sliver.site) == AllocationAlgorithm.Random:
            random.shuffle(node_id_list)
        else:
            # Reshuffle Nodes based on CPU Threshold only for VMs when no specific host is specified
            if sliver.get_type() == NodeType.VM and (sliver.labels is None or
                                                     (sliver.labels and sliver.labels.instance_parent is None)):
                node_id_list = self.__reshuffle_nodes(node_id_list=node_id_list,
                                                      node_id_to_reservations=node_id_to_reservations,
                                                      term=term)

        if len(node_id_list) == 0 and sliver.site not in self.combined_broker_model.get_sites():
            error_msg = f'Unknown site {sliver.site} requested for {reservation}'
            self.logger.error(error_msg)
            return delegation_id, sliver, error_msg

        node_id_list = self.__prune_nodes_in_maintenance(node_id_list=node_id_list,
                                                         site=sliver.site,
                                                         reservation=reservation)

        # no candidate nodes found
        if len(node_id_list) == 0:
            error_msg = f'Insufficient resources: No hosts available to provision the {reservation}'
            self.logger.error(error_msg)
            return delegation_id, sliver, error_msg

        return self.__find_first_fit(node_id_list=node_id_list,
                                     node_id_to_reservations=node_id_to_reservations,
                                     inv=inv, reservation=reservation, term=term, sliver=sliver,
                                     operation=operation)

    def __can_extend_interface_sliver(self, rid: ID, inv: NetworkServiceInventory,
                                      ifs: InterfaceSliver, sliver: NetworkServiceSliver,
                                      node_id_to_reservations: dict, term: Term):
        """
        Checks if VLAN attached to an interface are assigned to any advanced reservations in this case
        @param rid
        @param inv
        @param ifs
        @param sliver
        @param node_id_to_reservations
        @param term

        @raises BrokerException in case VLAN is already assigned to any future sliver
        """
        ns_node_id, ns_bqm_node_id = sliver.get_node_map()
        node_id, bqm_node_id = ifs.get_node_map()
        bqm_cp = self.get_interface_sliver_from_graph(node_id=bqm_node_id)
        self.logger.debug(f"BQM IFS: {bqm_cp}")
        owner_switch, owner_mpls, owner_ns = self.get_owners(node_id=bqm_node_id, ns_type=sliver.get_type())
        self.logger.debug(f"Owner SWITCH: {owner_switch}")
        self.logger.debug(f"Owner MPLS: {owner_mpls}")
        self.logger.debug(f"Owner NS: {owner_ns}")

        # Handle IPV6Ext services
        ns_bqm_node_id = ns_bqm_node_id.node_id.replace('ipv6ext-ns',
                                                        'ipv6-ns') if 'ipv6ext-ns' in ns_bqm_node_id else ns_bqm_node_id

        existing_reservations = self.get_existing_reservations(
            node_id=ns_bqm_node_id,
            node_id_to_reservations=node_id_to_reservations,
            start=term.get_start_time(),
            end=term.get_end_time(),
        )

        inv.allocate_ifs(
            rid=rid,
            requested_ns=sliver,
            requested_ifs=ifs,
            owner_ns=owner_ns,
            bqm_ifs=bqm_cp,
            existing_reservations=existing_reservations,
            operation=ReservationOperation.Extend
        )

    def __allocate_services(self, *, rid: ID, inv: NetworkServiceInventory, sliver: NetworkServiceSliver,
                            node_id_to_reservations: dict, term: Term,
                            operation: ReservationOperation = ReservationOperation.Create) -> Tuple[str, BaseSliver, Any]:
        """
        Allocate Network Service Slivers
        @param rid Reservation Id
        @param inv Inventory
        @param sliver Requested sliver
        @param node_id_to_reservations
        @param operation
        @return tuple containing delegation id, sliver, error message if any
        """
        delegation_id = None
        error_msg = None
        try:
            self.logger.debug(f"Processing Network Service sliver: {sliver}")
            owner_ns = None
            owner_ns_id = None
            bqm_node = None
            is_vnic = False
            owner_mpls_ns = None
            owner_switch = None

            peered_ns_interfaces = []
            ero_source_end_info = []

            # For each Interface Sliver;
            for ifs in sliver.interface_info.interfaces.values():
                node_map_id = self.combined_broker_model_graph_id

                # Fetch Network Node Id and BQM Component Id
                node_id, bqm_node_id = ifs.get_node_map()

                # Skipping the already allocated interface on a modify
                if self.combined_broker_model_graph_id in node_id:

                    if operation == ReservationOperation.Extend:
                        self.__can_extend_interface_sliver(rid=rid, inv=inv, ifs=ifs, sliver=sliver,
                                                           node_id_to_reservations=node_id_to_reservations, term=term)
                    continue

                if node_id == str(NodeType.Facility):
                    bqm_node = self.get_facility_sliver(node_name=bqm_node_id)
                # Peered Interfaces are handled at the end
                elif node_id == str(Constants.PEERED):
                    peered_ns_interfaces.append(ifs)
                    continue
                elif node_id == str(NodeType.Switch):
                    bqm_node = self.get_network_node_from_graph(node_id=bqm_node_id)
                    node_map_id = f"{node_map_id}#{bqm_node.get_name()}#{bqm_node_id}#{ifs.get_labels().local_name}"
                else:
                    # For VM interfaces
                    bqm_node = self.get_component_sliver(node_id=bqm_node_id)
                    node_map_id = f"{node_map_id}:{node_id}:{bqm_node_id}:{ifs.get_labels().bdf}"

                if bqm_node is None:
                    raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES)

                # Get BQM Connection Point in Site Delegation (c)
                site_cp = FimHelper.get_site_interface_sliver(component=bqm_node,
                                                              local_name=ifs.get_labels().local_name,
                                                              region=ifs.get_labels().region,
                                                              device_name=ifs.get_labels().device_name)
                self.logger.debug(f"Interface Sliver [Site Delegation] (C): {site_cp}")

                # Get BQM Peer Connection Point in Site Delegation (a)
                net_cp = self.get_peer_interface_sliver(site_ifs_id=site_cp.node_id,
                                                        interface_type=InterfaceType.TrunkPort)

                if net_cp is None:
                    error_msg = "Peer Connection Point not found from Network AM"
                    raise BrokerException(msg=error_msg)

                self.logger.debug(f"Peer Interface Sliver [Network Delegation] (A): {net_cp}")

                # need to find the owner switch of the network service in CBM and take it's name or labels.local_name
                owner_switch, owner_mpls_ns, owner_ns = self.get_owners(node_id=net_cp.node_id,
                                                                        ns_type=sliver.get_type())

                # Hack for IPV6Ext services
                owner_ns_id = owner_ns.node_id
                if 'ipv6ext-ns' in owner_ns_id:
                    owner_ns_id = owner_ns_id.replace('ipv6ext-ns', 'ipv6-ns')

                bqm_cp = net_cp
                if bqm_node.get_type() == NodeType.Facility or \
                        (sliver.get_type() == ServiceType.L2Bridge and
                         bqm_node.get_model() == Constants.OPENSTACK_VNIC_MODEL):
                    bqm_cp = site_cp

                if bqm_node.get_type() == ComponentType.SharedNIC:
                    if bqm_node.get_model() == Constants.OPENSTACK_VNIC_MODEL:
                        is_vnic = True

                    # VLAN is already set by the Orchestrator using the information from the Node Sliver Parent Reservation
                    if ifs.get_labels().vlan is None and not is_vnic:
                        message = "Shared NIC VLAN cannot be None"
                        self.logger.error(message)
                        raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                              msg=f"{message}")
                else:
                    existing_reservations = self.get_existing_reservations(node_id=owner_ns_id,
                                                                           node_id_to_reservations=node_id_to_reservations,
                                                                           start=term.get_start_time(),
                                                                           end=term.get_end_time())
                    # Set vlan - source: (c) - only for dedicated NICs
                    ifs = inv.allocate_ifs(rid=rid, requested_ns=sliver, requested_ifs=ifs, owner_ns=owner_ns,
                                           bqm_ifs=bqm_cp, existing_reservations=existing_reservations)

                local_name = net_cp.get_name()
                device_name = owner_switch.get_name()

                if device_name == Constants.AL2S:
                    delegation_id, delegated_label = FimHelper.get_delegations(delegations=
                                                                               net_cp.get_label_delegations())
                    device_name = delegated_label.device_name
                    local_name = delegated_label.local_name

                # local_name source: (a)
                ifs_labels = ifs.get_labels()
                ifs_labels = Labels.update(ifs_labels, local_name=local_name)

                # NSO device name source: (a) - need to find the owner switch of the network service in CBM
                # and take its name or labels.local_name
                # Set the NSO device-name
                ifs_labels = Labels.update(ifs_labels, device_name=device_name)
                adm_ids = owner_switch.get_structural_info().adm_graph_ids
                site_adm_ids = bqm_node.get_structural_info().adm_graph_ids

                self.logger.debug(f"Owner Network Service: {owner_ns}")
                self.logger.debug(f"Owner Switch: {owner_switch}")
                if owner_switch.network_service_info is not None:
                    self.logger.debug(f"Owner Switch NS: {owner_switch.network_service_info.network_services.values()}")

                net_adm_ids = site_adm_ids
                if bqm_node.get_type() != NodeType.Facility and not is_vnic:
                    net_adm_ids = [x for x in adm_ids if not x in site_adm_ids or site_adm_ids.remove(x)]
                    # For sites like EDC which share switch with other sites like NCSA,
                    # the net_adm_ids also includes delegation id from the other side,
                    # this results in this list having more than one entry and no way for
                    # the code to know which delegation is from Network AM
                    # Using a hack here to pick the delegation id from one of the
                    # layer 3 network services in the owner switch
                    if len(net_adm_ids) > 1:
                        for x in owner_switch.network_service_info.network_services.values():
                            if x.get_layer() == NSLayer.L2:
                                continue
                            net_adm_ids = x.get_structural_info().adm_graph_ids
                            break
                else:
                    if bqm_cp.labels is not None and bqm_cp.labels.ipv4_subnet is not None:
                        ifs_labels = Labels.update(ifs_labels, ipv4_subnet=bqm_cp.labels.ipv4_subnet)
                    if bqm_cp.labels is not None and bqm_cp.labels.ipv6_subnet is not None:
                        ifs_labels = Labels.update(ifs_labels, ipv6_subnet=bqm_cp.labels.ipv6_subnet)
                if len(net_adm_ids) != 1:
                    error_msg = f"More than 1 or 0 Network Delegations found! net_adm_ids: {net_adm_ids}"
                    self.logger.error(error_msg)
                    raise BrokerException(msg=error_msg)

                if bqm_node.get_type() == NodeType.Facility:
                    node_map_id = f"{node_map_id}#{bqm_node.get_name()}#{bqm_cp.node_id}#{ifs_labels.vlan}"

                # Update the Interface Sliver Node Map to map to (a)
                ifs.set_node_map(node_map=(node_map_id, bqm_cp.node_id))
                #ifs.set_node_map(node_map=(self.combined_broker_model_graph_id, bqm_cp.node_id))

                delegation_id = net_adm_ids[0]

                ifs.labels = ifs_labels
                ifs.label_allocations = Labels.update(lab=ifs_labels)

                self.logger.info(f"Allocated Interface Sliver: {ifs} delegation: {delegation_id}")

                owner_v4_service = self.get_ns_from_switch(switch=owner_switch, ns_type=ServiceType.FABNetv4)
                self.logger.info(f"owner_v4_service: {owner_v4_service}")
                if owner_v4_service and owner_v4_service.get_labels():
                    ero_source_end_info.append((owner_switch.node_id, owner_v4_service.get_labels().ipv4))

            if not owner_ns:
                bqm_graph_id, bqm_node_id = sliver.get_node_map()
                owner_ns, owner_switch = self.get_network_service_from_graph(node_id=bqm_node_id,
                                                                             parent=True)
                # Hack for IPV6Ext services
                owner_ns_id = owner_ns.node_id
                if 'ipv6ext-ns' in owner_ns_id:
                    owner_ns_id = owner_ns_id.replace('ipv6ext-ns', 'ipv6-ns')

                owner_mpls_ns = None
                if owner_switch:
                    for ns in owner_switch.network_service_info.network_services.values():
                        if ServiceType.MPLS == ns.get_type():
                            owner_mpls_ns = ns
                            break
                if owner_ns and ServiceType.MPLS == owner_ns.get_type():
                    delegation_id, delegated_label = FimHelper.get_delegations(delegations=
                                                                               owner_switch.get_label_delegations())
                else:
                    delegation_id, delegated_label = FimHelper.get_delegations(delegations=
                                                                               owner_ns.get_label_delegations())

            # Set the Subnet and gateway from the Owner Switch (a)
            existing_reservations = self.get_existing_reservations(node_id=owner_ns_id,
                                                                   node_id_to_reservations=node_id_to_reservations,
                                                                   start=term.get_start_time(), end=term.get_end_time())

            # Allocate VLAN for the Network Service
            if is_vnic:
                site_adm_ids = bqm_node.get_structural_info().adm_graph_ids
                delegation_id = site_adm_ids[0]
                inv.allocate_vnic(rid=rid, requested_ns=sliver, owner_ns=owner_ns,
                                  existing_reservations=existing_reservations)
            else:
                sliver = inv.allocate(rid=rid, requested_ns=sliver, owner_ns=owner_ns,
                                      existing_reservations=existing_reservations)

            # Update the Network Service Sliver Node Map to map to parent of (a)
            sliver.set_node_map(node_map=(self.combined_broker_model_graph_id, owner_ns_id))

            self.__allocate_peered_interfaces(rid=rid, peered_interfaces=peered_ns_interfaces, owner_switch=owner_switch,
                                              owner_mpls=owner_mpls_ns, inv=inv, sliver=sliver, owner_ns=owner_ns,
                                              node_id_to_reservations=node_id_to_reservations, term=term)

            if sliver.ero and len(sliver.ero.get()) and len(ero_source_end_info) == 2:
                self.logger.info(f"Requested ERO: {sliver.ero} {ero_source_end_info}")
                ero_hops = []
                new_path = [ero_source_end_info[0][1]]
                type, path = sliver.ero.get()
                for hop in path.get()[0]:
                    # User passes the site names; Broker maps the sites names to the respective switch IP
                    hop_switch = self.get_switch_sliver(site=hop)
                    self.logger.debug(f"Switch information for {hop}: {hop_switch}")
                    if not hop_switch:
                        self.logger.error(f"Requested hop: {hop} in the ERO does not exist")
                        raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                              msg=f"Requested hop: {hop} in the ERO does not exist ")

                    hop_v4_service = self.get_ns_from_switch(switch=hop_switch, ns_type=ServiceType.FABNetv4)
                    if hop_v4_service and hop_v4_service.get_labels() and hop_v4_service.get_labels().ipv4:
                        self.logger.debug(f"Fabnetv4 information for {hop}: {hop_v4_service}")
                        ero_hops.append(f"{hop_switch.node_id}-ns")
                        new_path.append(hop_v4_service.get_labels().ipv4)

                new_path.append(ero_source_end_info[1][1])

                if len(new_path):
                    '''
                    if not self.validate_requested_ero_path(source_node=ero_source_end_info[0][0],
                                                            end_node=ero_source_end_info[1][0],
                                                            hops=ero_hops):
                        raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                              msg=f"Requested ERO path: {sliver.ero} is invalid!")
                    '''
                    ero_path = Path()
                    ero_path.set_symmetric(new_path)
                    sliver.ero.set(ero_path)
                    self.logger.info(f"Allocated ERO: {sliver.ero}")

        except BrokerException as e:
            delegation_id = None
            if e.error_code == ExceptionErrorCode.INSUFFICIENT_RESOURCES:
                self.logger.error(f"Exception occurred: {e}")
                error_msg = e.msg
            else:
                raise e
        self.logger.debug(f"Allocate Services returning: {delegation_id} {sliver} {error_msg}")
        return delegation_id, sliver, error_msg

    def __allocate_peered_interfaces(self, *, rid: ID, peered_interfaces: List[InterfaceSliver], owner_switch: NodeSliver,
                                     inv: NetworkServiceInventory, sliver: NetworkServiceSliver,
                                     owner_mpls: NetworkServiceSliver, owner_ns: NetworkServiceSliver,
                                     node_id_to_reservations: dict, term: Term):
        if not len(peered_interfaces):
            return
        for pfs in peered_interfaces:
            name, site_node_type_name = pfs.get_node_map()
            site = site_node_type_name
            node_type = None
            node_name = None
            res_list = site_node_type_name.split(",")
            if len(res_list) > 1:
                site = res_list[0]
                node_type = res_list[1]
                node_name = res_list[2]

            peer_sw = self.get_peer_node(site=site, node_type=node_type, node_name=node_name)

            nodes_on_path = self.get_shortest_path(src_node_id=owner_mpls.node_id,
                                                   dest_node_id=peer_sw.node_id)

            # Node ID of the switch connecting the L3VPN to the destination L3VPN
            interface_node_id = nodes_on_path[1]
            # In case of FABRIC L3VPN service connecting to the VMs, use the last switch connected to the AL2S
            if sliver.get_technology() != Constants.AL2S:
                # Update Switch
                index = 0
                peer_ns_id = None
                for x in nodes_on_path:
                    index += 1
                    if Constants.AL2S in x:
                        peer_ns_id = x
                        interface_node_id = nodes_on_path[index - 4]
                owner_switch, owner_mpls, owner_ns = self.get_owners(node_id=interface_node_id,
                                                                     ns_type=sliver.get_type())
                peer_mpls, peer_sw = self.get_network_service_from_graph(node_id=peer_ns_id, parent=True)

            peer_ns = self.get_ns_from_switch(switch=peer_sw, ns_type=sliver.get_type())

            bqm_interface = None
            for bifs in owner_mpls.interface_info.interfaces.values():
                if bifs.node_id == interface_node_id:
                    bqm_interface = bifs
                    break
            if bqm_interface is None:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Unable to find BQM interface for {pfs.get_name()}")

            existing_reservations = self.get_existing_reservations(node_id=owner_ns.node_id,
                                                                   node_id_to_reservations=node_id_to_reservations,
                                                                   start=term.get_start_time(),
                                                                   end=term.get_end_time())

            pfs = inv.allocate_peered_ifs(rid=rid, owner_switch=owner_switch, requested_ifs=pfs,
                                          bqm_interface=bqm_interface,
                                          existing_reservations=existing_reservations)

            pfs.set_node_map(node_map=(self.combined_broker_model_graph_id, bqm_interface.node_id))
            if pfs.peer_labels is None:
                pfs.peer_labels = Labels()
            if peer_ns and peer_ns.labels:
                pfs.peer_labels = Labels.update(pfs.peer_labels, asn=peer_ns.labels.asn)
            self.logger.info(f"Allocated Peered Interface Sliver: {pfs}")

        # Update the Network Service Sliver Node Map
        sliver.set_node_map(node_map=(self.combined_broker_model_graph_id, owner_ns.node_id))

    def ticket_inventory(self, *, reservation: ABCBrokerReservation, inv: InventoryForType, term: Term,
                         node_id_to_reservations: dict,
                         operation: ReservationOperation = ReservationOperation.Create) -> Tuple[bool, dict, Any]:
        error_msg = None
        try:
            if operation == ReservationOperation.Extend:
                rset = reservation.get_resources()
            else:
                rset = reservation.get_requested_resources()
            needed = rset.get_units()

            # for network node slivers
            # find a list of candidate worker nodes that satisfy the requirements based on delegated
            # capacities within the site
            # for network link slivers
            # orchestrator needs to provide a map to CBM guid of the node representing the
            # intended link (and possibly interfaces connected to it)

            res_sliver = rset.get_sliver()

            if isinstance(res_sliver, NodeSliver):
                delegation_id, sliver, error_msg = self.__allocate_nodes(reservation=reservation, inv=inv,
                                                                         sliver=res_sliver,
                                                                         node_id_to_reservations=node_id_to_reservations,
                                                                         term=term,
                                                                         operation=operation)

            elif isinstance(res_sliver, NetworkServiceSliver):
                delegation_id, sliver, error_msg = self.__allocate_services(rid=reservation.get_reservation_id(),
                                                                            inv=inv, sliver=res_sliver,
                                                                            node_id_to_reservations=node_id_to_reservations,
                                                                            term=term, operation=operation)
            else:
                self.logger.error(f'Reservation {reservation} sliver type is neither Node, nor NetworkServiceSliver')
                raise BrokerException(msg=f"Reservation sliver type is neither Node "
                                      f"nor NetworkLink for reservation# {reservation}")

            if delegation_id is not None:
                delegation = self.actor.get_delegation(did=delegation_id)
                reservation = self.issue_ticket(reservation=reservation, units=needed, rtype=rset.get_type(), term=term,
                                                source=delegation, sliver=sliver)

                node_map = sliver.get_node_map()
                node_id = node_map[1]
                if node_id_to_reservations.get(node_id, None) is None:
                    node_id_to_reservations[node_id] = ReservationSet()
                node_id_to_reservations[node_id].add(reservation=reservation)
                self.logger.debug(f"Ticket Inventory returning: True {error_msg}")
                return True, node_id_to_reservations, error_msg
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            reservation.fail(message=str(e))
        self.logger.debug(f"Ticket Inventory returning: False {error_msg}")
        return False, node_id_to_reservations, error_msg

    def __is_modify_on_openstack_vnic(self, *, sliver: BaseSliver) -> bool:
        if not isinstance(sliver, NetworkServiceSliver):
            return False

        if sliver.get_type() != ServiceType.L2Bridge:
            return False

        graph_id, bqm_ns_id = sliver.get_node_map()

        bqm_ns_sliver, bqm_sw = self.get_network_service_from_graph(node_id=bqm_ns_id)
        if bqm_ns_sliver.get_type() == ServiceType.VLAN:
            return True

        return False

    def extend_private(self, *, reservation: ABCBrokerReservation, inv: InventoryForType, term: Term,
                       node_id_to_reservations: dict):
        try:
            self.logger.debug(f"Extend private initiated for {reservation}")
            requested_resources = reservation.get_requested_resources()
            current_resources = reservation.get_resources()
            needed = requested_resources.get_units()
            sliver = current_resources.get_sliver()
            diff = sliver.diff(other_sliver=requested_resources.get_sliver())

            operation = ReservationOperation.Extend
            if diff is not None:
                sliver = requested_resources.get_sliver()
                operation = ReservationOperation.Modify

            #if diff is None or diff.added is None or \
            #        (len(diff.added.components) == 0 and len(diff.added.interfaces) == 0) or \
            #        self.__is_modify_on_openstack_vnic(sliver=sliver):

            if self.__is_modify_on_openstack_vnic(sliver=sliver):
                self.issue_ticket(reservation=reservation, units=needed, rtype=requested_resources.get_type(),
                                  term=term, source=reservation.get_source(), sliver=sliver)
            else:
                status, node_id_to_reservations, error_msg = self.ticket_inventory(reservation=reservation,
                                                                                   inv=inv, term=term,
                                                                                   node_id_to_reservations=node_id_to_reservations,
                                                                                   operation=operation)
                if not status and not reservation.is_failed():
                    fail_message = f"Insufficient resources for specified start time, Failing reservation: " \
                                   f"{reservation.get_reservation_id()}"
                    if error_msg is not None:
                        fail_message = error_msg
                    reservation.fail_extend(message=fail_message)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            reservation.fail_extend(message=str(e), exception=e)

    def issue_ticket(self, *, reservation: ABCBrokerReservation, units: int, rtype: ResourceType,
                     term: Term, source: ABCDelegation, sliver: BaseSliver) -> ABCBrokerReservation:

        # make the new delegation
        resource_delegation = ResourceTicketFactory.create(issuer=self.actor.get_identity().get_guid(), units=units,
                                                           term=term, rtype=rtype)

        # extract a new resource set
        mine = self.extract(source=source, delegation=resource_delegation)

        # attach the current request properties so that we can look at them in the future
        mine.set_sliver(sliver=sliver)

        if mine is not None and not reservation.is_failed():
            reservation.set_approved(term=term, approved_resources=mine)
            reservation.set_source(source=source)
            self.logger.debug(f"allocated: {mine.get_units()} for term: {term}")
            self.logger.debug(f"resource share= {units} mine= {mine.get_units()}")

            self.add_to_calendar(reservation=reservation)
            reservation.set_bid_pending(value=False)
        else:
            if mine is None:
                raise BrokerException(msg="There was an error extracting a ticket from the source delegation")
        return reservation

    def release(self, *, reservation):
        if isinstance(reservation, ABCBrokerReservation):
            self.logger.debug("Broker reservation")
            super().release(reservation=reservation)
        elif isinstance(reservation, ABCClientReservation):
            self.logger.debug("Client reservation")
            super().release(reservation=reservation)
            status = self.inventory.remove(source=reservation)
            self.logger.debug(f"Removing reservation: {reservation.get_reservation_id()} "
                              f"from inventory status: {status}")

    def align_end(self, *, when: datetime) -> datetime:
        """
        Aligns the specified date with the end of the closest cycle.

        @param when when to align

        @return date aligned with the end of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_end_in_millis(cycle=cycle)
        return ActorClock.from_milliseconds(milli_seconds=time)

    def align_start(self, *, when: datetime) -> datetime:
        """
        Aligns the specified date with the start of the closest cycle.

        @param when when to align

        @return date aligned with the start of the closes cycle
        """
        cycle = self.clock.cycle(when=when)
        time = self.clock.cycle_start_in_millis(cycle=cycle)
        return ActorClock.from_milliseconds(milli_seconds=time)

    def query(self, *, p: dict) -> dict:
        """
        Returns the Broker Query Model
        @params p : dictionary containing filters (not used currently)
        """
        result = {}
        self.logger.debug("Processing Query with properties: {}".format(p))

        query_action = self.get_query_action(properties=p)
        query_level = p.get(Constants.QUERY_DETAIL_LEVEL, None)
        if query_level is not None:
            query_level = int(query_level)

        if query_action is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"query_action {query_action}")

        if query_action != Constants.QUERY_ACTION_DISCOVER_BQM:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT,
                                  msg=f"query_action {query_action}")

        bqm_format = p.get(Constants.BROKER_QUERY_MODEL_FORMAT, None)
        if bqm_format is not None:
            bqm_format = GraphFormat(int(bqm_format))
        else:
            bqm_format = GraphFormat.GRAPHML

        start = p.get(Constants.START, None)
        if start:
            start = datetime.strptime(start, Constants.LEASE_TIME_FORMAT)
        end = p.get(Constants.END, None)
        if end:
            end = datetime.strptime(end, Constants.LEASE_TIME_FORMAT)

        excludes = p.get(Constants.EXCLUDES, None)
        includes = p.get(Constants.INCLUDES, None)

        try:
            if self.query_cbm is not None:
                graph = self.query_cbm.get_bqm(query_level=query_level, start=start, end=end, includes=includes,
                                               excludes=excludes)
                graph_string = None
                if graph is not None:
                    graph_string = graph.serialize_graph(format=bqm_format)
                if graph_string is not None:
                    result[Constants.BROKER_QUERY_MODEL] = graph_string
                    result[Constants.QUERY_RESPONSE_STATUS] = "True"
                else:
                    result[Constants.BROKER_QUERY_MODEL] = ""
                    result[Constants.QUERY_RESPONSE_STATUS] = "False"
                    result[Constants.QUERY_RESPONSE_MESSAGE] = "Resource(s) not found"
                graph.delete_graph()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            result[Constants.BROKER_QUERY_MODEL] = ""
            result[Constants.QUERY_RESPONSE_STATUS] = "False"
            result[Constants.QUERY_RESPONSE_MESSAGE] = str(e)

        self.logger.debug("Returning Query Result: {}".format(result))
        return result

    def donate_delegation(self, *, delegation: ABCDelegation):
        """
        Donate an incoming delegation by merging to CBM;
        We take snapshot of CBM before merge, and rollback to snapshot in case merge fails
        :param delegation:
        :return:
        :raises: Exception in case of failure
        """
        self.logger.debug("Donate Delegation")
        self.bind_delegation(delegation=delegation)
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.merge_adm(adm_graph=delegation.get_graph())
                self.logger.debug(f"Donated Delegation: {delegation.get_delegation_id()}")
            else:
                self.logger.warning(f"Delegation ignored: {delegation.get_delegation_id()}")
                self.logger.debug(f"Active delegations: {self.delegations}")
        except Exception as e:
            self.logger.error(f"Failed to merge ADM: {delegation}")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            self.lock.release()

    def remove_delegation(self, *, delegation: ABCDelegation):
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.unmerge_adm(graph_id=delegation.get_delegation_id())
                self.delegations.pop(delegation.get_delegation_id())
                self.logger.debug(f"Removed Delegation: {delegation.get_delegation_id()}")
            else:
                self.logger.warning(f"Delegation ignored: {delegation.get_delegation_id()}")
                self.logger.debug(f"Active delegations: {self.delegations}")
        except Exception as e:
            self.logger.error(f"Failed to un-merge ADM: {delegation}")
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            self.lock.release()

    def closed_delegation(self, *, delegation: ABCDelegation):
        """
        Close a delegation by un-merging from CBM
        We take snapshot of CBM before un-merge, and rollback to snapshot in case un-merge fails
        :param delegation:
        :return:
        """
        self.logger.debug("Close Delegation")
        self.remove_delegation(delegation=delegation)

    def reclaim_delegation(self, *, delegation: ABCDelegation):
        """
        Reclaim a delegation by un-merging from CBM
        We take snapshot of CBM before un-merge, and rollback to snapshot in case un-merge fails
        :param delegation:
        :return:
        """
        self.logger.debug("Reclaim Delegation")
        self.remove_delegation(delegation=delegation)

    def get_peer_interface_sliver(self, *, site_ifs_id: str, interface_type: InterfaceType) -> InterfaceSliver or None:
        """
        Get Peer Interface Sliver (child of Network Service Sliver) provided node id of Interface Sliver
        (child of Component Sliver)

        E.g: Provided Connection Point which is a child of Component i.e. renc-w3-nic2-p1,
        return Peer Connection Point i.e. HundredGigE 0/0/0/25.3

        renc-w3-nic2-p1      => l10  <= HundredGigE 0/0/0/25.3
        [Connection Point]      Link    [Connection Point]

        @param site_ifs_id Interface Sliver Id
        @param interface_type Interface Type
        @return Interface sliver
        """
        try:
            self.lock.acquire()
            peer_interfaces = FimHelper.get_peer_interfaces(ifs_node_id=site_ifs_id, graph=self.combined_broker_model,
                                                            interface_type=interface_type)

            if len(peer_interfaces) == 0:
                return None

            if len(peer_interfaces) != 1:
                raise BrokerException(msg=f"More than one Peer Interface Sliver of type {interface_type} found for "
                                          f"IFS: {site_ifs_id}")
            return next(iter(peer_interfaces))
        finally:
            self.lock.release()

    def get_component_sliver(self, *, node_id: str) -> ComponentSliver or None:
        """
        Get Component Sliver from BQM
        @param node_id: Node Id
        @return Component Sliver
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            return self.combined_broker_model.build_deep_component_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_shortest_path(self, *, src_node_id: str, dest_node_id: str):
        """
        Get Component Sliver from BQM
        @param src_node_id: Source node id
        @param dest_node_id: Destination node id
        @return Facility Sliver
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            node_list = self.combined_broker_model.get_nodes_on_shortest_path(node_a=src_node_id, node_z=dest_node_id)
            return node_list
        finally:
            self.lock.release()

    def get_peer_node(self, *, site: str, node_type: str, node_name: str) -> NodeSliver:
        if node_type == str(NodeType.Facility):
            peer_node = self.get_facility_sliver(node_name=f'{site},{node_name}')
            return peer_node
        else:
            return self.get_switch_sliver(site=site)

    @staticmethod
    def get_ns_from_switch(switch: NodeSliver, ns_type: ServiceType) -> NetworkServiceSliver:
        """
        Extract specific type of service from a switch
        :param switch: switch
        :param ns_type: type of service requested
        :return Network Service
        """
        if switch and switch.network_service_info:
            for service in switch.network_service_info.network_services.values():
                if service.get_type() == ns_type:
                    return service

    def validate_requested_ero_path(self, source_node: str, end_node: str, hops: List[str]) -> bool:
        try:
            self.lock.acquire()
            if self.combined_broker_model:
                path = self.combined_broker_model.get_nodes_on_path_with_hops(node_a=source_node,
                                                                              node_z=end_node, hops=hops, cut_off=200)
                self.logger.debug(f"Network path from source:{source_node} to end: {end_node} "
                                  f"with hops: {hops} is path: {path}")
                if len(path) and path[0] == source_node and path[-1] == end_node:
                    return True
        finally:
            self.lock.release()
        return False

    def get_switch_sliver(self, *, site: str, stitch: bool = True) -> NodeSliver:
        """
        Get Component Sliver from BQM
        @param site: Node Site Name
        @param stitch: Flag indicating if the StitchNode is being looked up
        @return Facility Sliver
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model:
                node_props = {ABCPropertyGraphConstants.PROP_SITE: site,
                              ABCPropertyGraphConstants.PROP_TYPE: str(NodeType.Switch)}
                              #ABCPropertyGraphConstants.PROP_STITCH_NODE: str(stitch).lower()}
                candidates = self.combined_broker_model.get_matching_nodes_with_components(
                    label=ABCPropertyGraphConstants.CLASS_NetworkNode,
                    props=node_props)

                if candidates is not None:
                    for c in candidates:
                        if stitch and "p4" in c:
                            continue
                        ns_sliver = self.combined_broker_model.build_deep_node_sliver(node_id=c)
                        return ns_sliver
        finally:
            self.lock.release()

    def get_facility_sliver(self, *, node_name: str) -> NodeSliver or None:
        """
        Get Component Sliver from BQM
        @param node_name: Node Name
        @return Facility Sliver
        """
        try:
            elems = node_name.split(",")
            site = elems[0]
            name = elems[1]
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            node_props = {ABCPropertyGraphConstants.PROP_SITE: site,
                          ABCPropertyGraphConstants.PROP_TYPE: str(NodeType.Facility)}
            candidates = self.combined_broker_model.get_matching_nodes_with_components(
                label=ABCPropertyGraphConstants.CLASS_NetworkNode,
                props=node_props)

            for c in candidates:
                ns_sliver = self.combined_broker_model.build_deep_node_sliver(node_id=c)
                if ns_sliver.get_name() == name:
                    return ns_sliver

            raise BrokerException(msg=f"Facility Port: {name} for site: {site} could not be found in CBM!")
        finally:
            self.lock.release()

    def get_owners(self, *, node_id: str, ns_type: ServiceType) -> Tuple[NodeSliver, NetworkServiceSliver,
                                                                         NetworkServiceSliver]:
        """
        Get owner switch and network service of a Connection Point from BQM
        @param node_id Node Id of the Connection Point
        @param ns_type Network Service Type
        """
        try:
            self.lock.acquire()
            return FimHelper.get_owners(bqm=self.combined_broker_model, node_id=node_id, ns_type=ns_type)
        finally:
            self.lock.release()

    def get_interface_sliver_from_graph(self, *, node_id: str) -> InterfaceSliver or None:
        """
        Get InterfaceSliver from CBM
        :param node_id:
        :return:
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            return self.combined_broker_model.build_deep_interface_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_network_node_from_graph(self, *, node_id: str) -> NodeSliver or None:
        """
        Get Node from CBM
        :param node_id:
        :return:
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None
            return self.combined_broker_model.build_deep_node_sliver(node_id=node_id)
        finally:
            self.lock.release()

    def get_network_service_from_graph(self, *, node_id: str,
                                       parent: bool = False) -> Tuple[NetworkServiceSliver or None, NodeSliver or None]:
        """
        Get Node from CBM
        :param node_id:
        :param parent:
        :return:
        """
        try:
            self.lock.acquire()
            if self.combined_broker_model is None:
                return None, None
            node_sliver = None
            ns_sliver = self.combined_broker_model.build_deep_ns_sliver(node_id=node_id)
            if parent:
                node_name, node_id = self.combined_broker_model.get_parent(node_id=node_id, rel=ABCPropertyGraph.REL_HAS,
                                                                           parent=ABCPropertyGraph.CLASS_NetworkNode)
                node_sliver = self.combined_broker_model.build_deep_node_sliver(node_id=node_id)

            return ns_sliver, node_sliver
        finally:
            self.lock.release()

    def get_existing_reservations(self, node_id: str, node_id_to_reservations: dict,
                                  start: datetime = None, end: datetime = None) -> List[ABCReservationMixin]:
        """
        Get existing reservations which are served by CBM node identified by node_id
        :param node_id:
        :param node_id_to_reservations:
        :param start
        :param end
        :return: list of reservations
        """
        states = [ReservationStates.Active.value,
                  ReservationStates.ActiveTicketed.value,
                  ReservationStates.Ticketed.value,
                  ReservationStates.Nascent.value]

        # Only get Active or Ticketing reservations
        existing_reservations = self.actor.get_plugin().get_database().get_reservations(graph_node_id=node_id,
                                                                                        states=states,
                                                                                        start=start,
                                                                                        end=end)

        reservations_allocated_in_cycle = node_id_to_reservations.get(node_id, None)

        if reservations_allocated_in_cycle is None:
            return existing_reservations

        if existing_reservations is None:
            return reservations_allocated_in_cycle.values()

        for e in existing_reservations.copy():
            if reservations_allocated_in_cycle.contains(rid=e.get_reservation_id()):
                existing_reservations.remove(e)

        for r in reservations_allocated_in_cycle.values():
            existing_reservations.append(r)

        return existing_reservations

    def get_existing_components(self, node_id: str, start: datetime = None, end: datetime = None,
                                excludes: List[str] = None, include_ns: bool = True,
                                include_node: bool = True) -> Dict[str, List[str]]:
        """
        Get existing components attached to Active/Ticketed Network Service Slivers
        :param node_id:
        :param start:
        :param end:
        :param excludes:
        :param include_node:
        :param include_ns:
        :return: list of components
        """
        states = [ReservationStates.Active.value,
                  ReservationStates.ActiveTicketed.value,
                  ReservationStates.Ticketed.value,
                  ReservationStates.Nascent.value,
                  ReservationStates.CloseFail.value]

        res_type = []
        if include_ns:
            for x in ServiceType:
                res_type.append(str(x))

        if include_node:
            for x in NodeType:
                res_type.append(str(x))

        # Only get Active or Ticketing reservations
        return self.actor.get_plugin().get_database().get_components(node_id=node_id, rsv_type=res_type, states=states,
                                                                     start=start, end=end, excludes=excludes)

    def set_logger(self, logger):
        """
        Set logger
        :param logger:
        :return:
        """
        super().set_logger(logger=logger)
        if self.inventory is not None:
            for inv in self.inventory.map.values():
                inv.set_logger(logger=logger)

    def merge_adm(self, *, adm_graph: ABCADMPropertyGraph):
        """
        Merge delegation model in CBM
        :param adm_graph: ADM
        :return:
        """
        snapshot_graph_id = None
        try:
            if self.combined_broker_model.graph_exists():
                snapshot_graph_id = self.combined_broker_model.snapshot()
            self.combined_broker_model.merge_adm(adm=adm_graph)
            self.combined_broker_model.validate_graph()
            # delete the snapshot
            if snapshot_graph_id is not None:
                self.combined_broker_model.importer.delete_graph(graph_id=snapshot_graph_id)
            # reload the query CBM
            self.query_cbm = FimHelper.get_neo4j_cbm_graph(graph_id=self.combined_broker_model_graph_id)
        except Exception as e:
            self.logger.error(f"Exception occurred: {e}")
            self.logger.error(traceback.format_exc())
            if snapshot_graph_id is not None:
                self.logger.info(f"CBM rollback due to merge failure")
                self.combined_broker_model.rollback(graph_id=snapshot_graph_id)
            raise e

    def unmerge_adm(self, *, graph_id: str):
        """
        Unmerge delegation model from CBM
        :param graph_id:
        :return:
        """
        snapshot_graph_id = None
        try:
            if self.combined_broker_model.graph_exists():
                snapshot_graph_id = self.combined_broker_model.snapshot()
            self.combined_broker_model.unmerge_adm(graph_id=graph_id)
            if self.combined_broker_model.graph_exists():
                self.combined_broker_model.validate_graph()

            if snapshot_graph_id is not None:
                # delete the snapshot
                self.combined_broker_model.importer.delete_graph(graph_id=snapshot_graph_id)
            # Reload the Query CBM
            self.query_cbm = FimHelper.get_neo4j_cbm_graph(graph_id=self.combined_broker_model_graph_id)
        except Exception as e:
            self.logger.error(f"Exception occurred: {e}")
            self.logger.error(traceback.format_exc())
            if snapshot_graph_id is not None:
                self.logger.info(f"CBM rollback due to un-merge failure")
                self.combined_broker_model.rollback(graph_id=snapshot_graph_id)
            raise e

    def get_algorithm_type(self, site: str) -> AllocationAlgorithm:
        if self.properties is not None:
            algorithms = self.properties.get(Constants.ALGORITHM, None)
            random_algo = algorithms.get(str(AllocationAlgorithm.Random))
            if random_algo and random_algo.get('enabled') and random_algo.get('sites') and \
                    site in random_algo.get('sites'):
                return AllocationAlgorithm.Random
            first_fit_algo = algorithms.get(AllocationAlgorithm.Random.name)
            if first_fit_algo and first_fit_algo.get('enabled'):
                return AllocationAlgorithm.FirstFit
        return AllocationAlgorithm.FirstFit

    def get_core_capacity_threshold(self) -> Tuple[bool, int]:
        if self.properties is not None:
            core_capacity_threshold = self.properties.get(Constants.CORE_CAPACITY_THRESHOLD, None)
            if core_capacity_threshold and core_capacity_threshold.get('enabled'):
                core_usage_threshold_percent = core_capacity_threshold.get('core_usage_threshold_percent', 75)
                return True, core_usage_threshold_percent
        return False, 0

    def get_node_capacities(self, node_id: str, node_id_to_reservations: dict,
                            term: Term) -> Tuple[NodeSliver, Capacities, Capacities]:
        """
        Get Node capacities - total as well as allocated capacities
        @param node_id: Node Id
        @param node_id_to_reservations: Reservations assigned as part of this bid
        @param term: Term
        @return: Tuple containing node, total and allocated capacity
        """
        try:
            graph_node = self.get_network_node_from_graph(node_id=node_id)
            existing_reservations = self.get_existing_reservations(node_id=node_id,
                                                                   node_id_to_reservations=node_id_to_reservations,
                                                                   start=term.get_start_time(),
                                                                   end=term.get_end_time())

            delegation_id, delegated_capacity = FimHelper.get_delegations(
                delegations=graph_node.get_capacity_delegations())

            allocated_capacity = Capacities()

            if existing_reservations:
                for reservation in existing_reservations:
                    # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                    resource_sliver = None
                    if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                        resource_sliver = reservation.get_approved_resources().get_sliver()

                    if (reservation.is_active() or reservation.is_ticketed()) and \
                            reservation.get_resources() is not None:
                        resource_sliver = reservation.get_resources().get_sliver()

                    if resource_sliver is not None and isinstance(resource_sliver, NodeSliver):
                        allocated_capacity += resource_sliver.get_capacity_allocations()

            return graph_node, delegated_capacity, allocated_capacity
        except Exception as e:
            self.logger.error(f"Failed to determine node capacities: {node_id}, error: {e}")

if __name__ == '__main__':
    policy = BrokerSimplerUnitsPolicy()
