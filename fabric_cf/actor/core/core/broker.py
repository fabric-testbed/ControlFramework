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

import queue
import threading
from typing import TYPE_CHECKING

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.boot.configuration import ActorConfig
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.delegation.broker_delegation_factory import BrokerDelegationFactory
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.broker_management_object import BrokerManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_broker_service import KafkaBrokerService
from fabric_cf.actor.core.proxies.kafka.services.broker_service import BrokerService
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.registry.peer_registry import PeerRegistry
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.security.auth_token import AuthToken

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
    from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.util.client import Client


class Broker(ActorMixin, ABCBrokerMixin):
    """
    Broker offers the base for all broker actors.
    """
    def __init__(self, *, identity: AuthToken = None, clock: ActorClock = None):
        super().__init__(auth=identity, clock=clock)
        # Recovered reservations that need to obtain tickets (both server and client roles).
        self.ticketing = ReservationSet()
        # Recovered reservations that need to extend tickets (both server and client roles).
        self.extending = ReservationSet()
        # The peer registry.
        self.registry = PeerRegistry()
        # Initialization status.
        self.initialized = False
        self.type = ActorType.Broker

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['recovered']
        del state['wrapper']
        del state['logger']
        del state['clock']
        del state['current_cycle']
        del state['first_tick']
        del state['stopped']
        del state['initialized']
        del state['closing']
        del state['message_service']

        del state['ticketing']
        del state['extending']
        del state['registry']
        del state['event_processors']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        self.recovered = False
        self.wrapper = None
        self.logger = None
        self.clock = None
        self.current_cycle = -1
        self.first_tick = True
        self.stopped = False
        self.initialized = False
        self.closing = ReservationSet()
        self.message_service = None

        self.ticketing = ReservationSet()
        self.extending = ReservationSet()
        self.registry = PeerRegistry()
        self.event_processors = {}

    def actor_added(self, *, config: ActorConfig):
        super().actor_added(config=config)
        self.registry.actor_added()

    def add_broker(self, *, broker: ABCBrokerProxy):
        self.registry.add_broker(broker=broker)

    def register_client_slice(self, *, slice_obj: ABCSlice):
        self.wrapper.register_slice(slice_object=slice_obj)

    def donate_delegation(self, *, delegation: ABCDelegation):
        ## This function is used by AUTs
        self.policy.donate_delegation(delegation=delegation)

    def bid(self, *, cycle: int):
        """
        Bids for resources as dictated by the bidding policy for the current cycle.

        @param cycle cycle

        @throws Exception in case of error
        """
        candidates = None
        candidates = self.policy.formulate_bids(cycle=cycle)
        if candidates is not None:
            for reservation in candidates.get_ticketing().values():
                try:
                    self.wrapper.ticket(reservation=reservation, destination=self)
                except Exception as e:
                    self.logger.error("unexpected ticket failure for #{} e: {}".format(
                        reservation.get_reservation_id(), e))

            for reservation in candidates.get_extending().values():
                try:
                    self.wrapper.extend_ticket(reservation=reservation)
                except Exception as e:
                    self.logger.error("unexpected extend failure for #{}".format(reservation.get_reservation_id()))

    def claim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                broker: ABCBrokerProxy = None) -> ABCDelegation:
        if delegation_id is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT, msg="delegation_id")

        if broker is None:
            broker = self.get_default_broker()

        if slice_object is None:
            slice_object = self.get_default_slice()
            if slice_object is None:
                slice_object = SliceFactory.create(slice_id=ID(), name=self.identity.get_name())
                slice_object.set_owner(owner=self.identity)
                slice_object.set_inventory(value=True)

        delegation = BrokerDelegationFactory.create(did=delegation_id, slice_id=slice_object.get_slice_id(),
                                                    broker=broker)
        delegation.set_exported(value=True)
        delegation.set_slice_object(slice_object=slice_object)

        self.wrapper.delegate(delegation=delegation, destination=self)
        return delegation

    def reclaim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                  broker: ABCBrokerProxy = None) -> ABCDelegation:
        if delegation_id is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT, msg="delegation_id")

        if broker is None:
            broker = self.get_default_broker()

        if slice_object is None:
            slice_object = self.get_default_slice()
            if slice_object is None:
                slice_object = SliceFactory.create(slice_id=ID(), name=self.identity.get_name())
                slice_object.set_owner(owner=self.identity)
                slice_object.set_inventory(value=True)

        delegation = BrokerDelegationFactory.create(did=delegation_id, slice_id=slice_object.get_slice_id(),
                                                    broker=broker)
        delegation.set_slice_object(slice_object=slice_object)
        delegation.set_exported(value=True)

        callback = ActorRegistrySingleton.get().get_callback(protocol=Constants.PROTOCOL_KAFKA,
                                                             actor_name=self.get_name())
        if callback is None:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_SUPPORTED, msg="callback is None")

        delegation.prepare(callback=callback, logger=self.logger)
        delegation.validate_outgoing()

        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=broker, callback=callback)

        return delegation

    def claim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.claim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def reclaim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def close_expiring(self, *, cycle: int):
        """
        Closes all expiring reservations.
        @param cycle cycle
        """
        expired = self.policy.get_closing(cycle=cycle)
        if expired is not None:
            self.logger.info("Broker expiring for cycle {} = {}".format(cycle, expired))
            self.close_reservations(reservations=expired)

    def demand(self, *, rid: ID):
        if rid is None:
            raise BrokerException(error_code=ExceptionErrorCode.INVALID_ARGUMENT, msg="rid")
        reservation = self.get_reservation(rid=rid)
        if reservation is None:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_FOUND, msg=f"rid={rid}")

        self.policy.demand(reservation=reservation)
        reservation.set_policy(policy=self.policy)

    def advertise(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> ID:
        slice_obj = SliceFactory.create(slice_id=ID(), name=client.get_name())
        slice_obj.set_owner(owner=client)
        slice_obj.set_broker_client()

        dlg_obj = DelegationFactory.create(did=delegation.get_graph_id(), slice_id=slice_obj.get_slice_id(),
                                           delegation_name=delegation_name)
        dlg_obj.set_slice_object(slice_object=slice_obj)
        dlg_obj.set_graph(graph=delegation)
        self.wrapper.advertise(delegation=dlg_obj, client=client)
        return dlg_obj.get_delegation_id()

    def extend_ticket_client(self, *, reservation: ABCClientReservation):
        if not self.recovered:
            self.extending.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket(reservation=reservation)

    def extend_tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, ABCBrokerReservation):
                    self.extend_ticket_broker(reservation=reservation)
                elif isinstance(reservation, ABCClientReservation):
                    self.extend_ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for # {} e: {}".format(reservation.get_reservation_id(), e))

    def extend_ticket_broker(self, *, reservation: ABCBrokerReservation):
        if not self.recovered:
            self.extending.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                               compare_sequence_numbers=False)

    def extend_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy, caller: AuthToken):
        if not self.recovered or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.extend_ticket_request(reservation=reservation, caller=caller,
                                           compare_sequence_numbers=True, callback=callback)

    def get_broker(self, *, guid: ID) -> ABCBrokerProxy:
        return self.registry.get_broker(guid=guid)

    def get_brokers(self) -> list:
        return self.registry.get_brokers()

    def get_default_broker(self) -> ABCBrokerProxy:
        return self.registry.get_default_broker()

    def get_default_slice(self) -> ABCSlice:
        """
        Get default inventory slice for the broker
        @return inventory slice for broker
        """
        slice_list = self.get_inventory_slices()
        if slice_list is not None and len(slice_list) > 0:
            return slice_list[0]
        return None

    def initialize(self, *, config: ActorConfig):
        if not self.initialized:
            super().initialize(config=config)
            self.registry.set_slices_plugin(plugin=self.plugin)
            self.registry.initialize()
            self.initialized = True

    def issue_delayed(self):
        super().issue_delayed()

        self.extend_tickets_client(rset=self.extending)
        self.extending.clear()

        self.tickets_client(rset=self.ticketing)
        self.ticketing.clear()

    def ticket_client(self, *, reservation: ABCClientReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket(reservation=reservation, destination=self)

    def tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, ABCBrokerReservation):
                    self.ticket_broker(reservation=reservation)
                elif isinstance(reservation, ABCClientReservation):
                    self.ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{} e: {}".format(reservation.get_reservation_id(), e))

    def ticket_broker(self, *, reservation: ABCBrokerReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                        callback=reservation.get_callback(), compare_seq_numbers=False)

    def ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")

        self.wrapper.ticket_request(reservation=reservation, caller=caller, callback=callback,
                                    compare_seq_numbers=True)

    def relinquish(self, *, reservation: ABCReservationMixin, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.relinquish_request(reservation=reservation, caller=caller)

    def tick_handler(self):
        self.policy.allocate(cycle=self.current_cycle)
        self.bid(cycle=self.current_cycle)
        self.close_expiring(cycle=self.current_cycle)

    def update_lease(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(msg="This actor cannot receive calls")

        self.wrapper.update_lease(reservation=reservation, update_data=update_data, caller=caller)

    def update_ticket(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.update_ticket(reservation=reservation, update_data=update_data, caller=caller)

    def update_delegation(self, *, delegation: ABCDelegation, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE, msg="of actor")
        self.wrapper.update_delegation(delegation=delegation, update_data=update_data, caller=caller)

    def register_client(self, *, client: Client):
        database = self.plugin.get_database()

        try:
            database.get_client(guid=client.get_guid())
        except Exception as e:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_FOUND, msg=f"client: {client.get_guid()} e: {e}")

        try:
            database.add_client(client=client)
        except Exception as e:
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE, msg=f"client: {client.get_guid()} e: {e}")

    def unregister_client(self, *, guid: ID):
        database = self.plugin.get_database()

        try:
            database.get_client(guid=guid)
        except Exception as e:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_FOUND, msg=f"client: {guid} e: {e}")

        try:
            database.remove_client(guid=guid)
        except Exception as e:
            raise BrokerException(error_code=ExceptionErrorCode.FAILURE, msg=f"client: {guid} e: {e}")

    def get_client(self, *, guid: ID) -> Client:
        database = self.plugin.get_database()
        ret_val = None
        try:
            ret_val = database.get_client(guid=guid)
        except Exception as e:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_FOUND, msg=f"client: {guid} e: {e}")

        return ret_val

    def modify(self, *, reservation_id: ID, modified_sliver: BaseSliver):
        return

    @staticmethod
    def get_management_object_class() -> str:
        return BrokerManagementObject.__name__

    @staticmethod
    def get_management_object_module() -> str:
        return BrokerManagementObject.__module__

    @staticmethod
    def get_kafka_service_class() -> str:
        return BrokerService.__name__

    @staticmethod
    def get_kafka_service_module() -> str:
        return BrokerService.__module__

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        return KafkaBrokerService.__name__

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        return KafkaBrokerService.__module__
