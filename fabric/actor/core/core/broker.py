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

import pickle
import queue
import threading
from typing import TYPE_CHECKING

from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.delegation.broker_delegation_factory import BrokerDelegationFactory
from fabric.actor.core.delegation.delegation_factory import DelegationFactory
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.manage.broker_management_object import BrokerManagementObject
from fabric.actor.core.manage.kafka.services.kafka_broker_service import KafkaBrokerService
from fabric.actor.core.proxies.kafka.services.broker_service import BrokerService
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.id import ID
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_broker import IBroker
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.actor import Actor
from fabric.actor.core.registry.peer_registry import PeerRegistry
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.util.reservation_set import ReservationSet
from fabric.actor.security.auth_token import AuthToken

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.util.client import Client


class Broker(Actor, IBroker):
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
        del state['thread_lock']
        del state['thread']
        del state['timer_queue']
        del state['event_queue']
        del state['reservation_tracker']
        del state['subscription_id']
        del state['actor_main_lock']
        del state['closing']
        del state['message_service']

        del state['ticketing']
        del state['extending']
        del state['registry']
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
        self.thread = None
        self.thread_lock = threading.Lock()
        self.timer_queue = queue.Queue()
        self.event_queue = queue.Queue()
        self.reservation_tracker = None
        self.subscription_id = None
        self.actor_main_lock = threading.Condition()
        self.closing = ReservationSet()
        self.message_service = None

        self.ticketing = ReservationSet()
        self.extending = ReservationSet()
        self.registry = PeerRegistry()

    def actor_added(self):
        super().actor_added()
        self.registry.actor_added()

    def add_broker(self, *, broker: IBrokerProxy):
        self.registry.add_broker(broker=broker)

    def register_client_slice(self, *, slice_obj: ISlice):
        self.wrapper.register_slice(slice_object=slice_obj)

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

    def claim_delegation_client(self, *, delegation_id: str = None, slice_object: ISlice = None,
                                broker: IBrokerProxy = None, id_token: str = None) -> IDelegation:
        if delegation_id is None:
            raise Exception("Invalid arguments")

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

        self.wrapper.delegate(delegation=delegation, destination=self, id_token=id_token)
        return delegation

    def reclaim_delegation_client(self, *, delegation_id: str = None, slice_object: ISlice = None,
                                  broker: IBrokerProxy = None, id_token: str = None) -> IDelegation:
        if delegation_id is None:
            raise Exception("Invalid arguments")

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

        callback = ActorRegistrySingleton.get().get_callback(protocol=Constants.protocol_kafka,
                                                             actor_name=self.get_name())
        if callback is None:
            raise Exception("Unsupported")

        delegation.prepare(callback=callback, logger=self.logger)
        delegation.validate_outgoing()

        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=broker, callback=callback,
                                                id_token=id_token)

        return delegation

    def claim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken,
                         id_token: str = None):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.claim_delegation_request(delegation=delegation, caller=caller, callback=callback,
                                              id_token=id_token)

    def reclaim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken,
                           id_token: str = None):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=caller, callback=callback,
                                                id_token=id_token)

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
            raise Exception("Invalid arguments")
        reservation = self.get_reservation(rid=rid)
        if reservation is None:
            raise Exception("unknown reservation #{}".format(rid))

        self.policy.demand(reservation=reservation)
        reservation.set_policy(policy=self.policy)

    def donate_reservation(self, *, reservation: IClientReservation):
        self.policy.donate_reservation(reservation=reservation)

    def advertise(self, *, delegation: ABCPropertyGraph, client: AuthToken) -> ID:
        slice_obj = SliceFactory.create(slice_id=ID(), name=client.get_name(), data=ResourceData())
        slice_obj.set_owner(owner=client)
        slice_obj.set_broker_client()

        dlg_obj = DelegationFactory.create(did=delegation.get_graph_id(), slice_id=slice_obj.get_slice_id())
        dlg_obj.set_slice_object(slice_object=slice_obj)
        dlg_obj.set_graph(graph=delegation)
        self.wrapper.advertise(delegation=dlg_obj, client=client)
        return dlg_obj.get_delegation_id()

    def extend_ticket_client(self, *, reservation: IClientReservation):
        if not self.recovered:
            self.extending.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket(reservation=reservation)

    def extend_tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, IBrokerReservation):
                    self.extend_ticket_broker(reservation=reservation)
                elif isinstance(reservation, IClientReservation):
                    self.extend_ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for # {}".format(reservation.get_reservation_id()))

    def extend_ticket_broker(self, *, reservation: IBrokerReservation):
        if not self.recovered:
            self.extending.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                               compare_sequence_numbers=False)

    def extend_ticket(self, *, reservation: IReservation, caller: AuthToken):
        if not self.recovered or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.extend_ticket_request(reservation=reservation, caller=caller, compare_sequence_numbers=True)

    def get_broker(self, *, guid: ID) -> IBrokerProxy:
        return self.registry.get_broker(guid=guid)

    def get_brokers(self) -> list:
        return self.registry.get_brokers()

    def get_default_broker(self) -> IBrokerProxy:
        return self.registry.get_default_broker()

    def get_default_slice(self) -> ISlice:
        """
        Get default inventory slice for the broker
        @return inventory slice for broker
        """
        slice_list = self.get_inventory_slices()
        if slice_list is not None and len(slice_list) > 0:
            return slice_list[0]
        return None

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.registry.set_slices_plugin(plugin=self.plugin)
            self.registry.initialize()
            self.initialized = True

    def issue_delayed(self):
        super().issue_delayed()

        self.extend_tickets_client(rset=self.extending)
        self.extending.clear()

        self.tickets_client(rset=self.ticketing)
        self.ticketing.clear()

    def ticket_client(self, *, reservation: IClientReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket(reservation=reservation, destination=self)

    def tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, IBrokerReservation):
                    self.ticket_broker(reservation=reservation)
                elif isinstance(reservation, IClientReservation):
                    self.ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{}".format(reservation.get_reservation_id()))

    def ticket_broker(self, *, reservation: IBrokerReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                        callback=reservation.get_callback(), compare_seq_numbers=False)

    def ticket(self, *, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")

        self.wrapper.ticket_request(reservation=reservation, caller=caller, callback=callback, compare_seq_numbers=True)

    def relinquish(self, *, reservation: IReservation, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.relinquish_request(reservation=reservation, caller=caller)

    def tick_handler(self):
        self.policy.allocate(cycle=self.current_cycle)
        self.bid(cycle=self.current_cycle)
        self.close_expiring(cycle=self.current_cycle)

    def update_ticket(self, *, reservation: IReservation, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.update_ticket(reservation=reservation, update_data=update_data, caller=caller)

    def update_delegation(self, *, delegation: IDelegation, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")
        self.wrapper.update_delegation(delegation=delegation, update_data=update_data, caller=caller)

    def register_client(self, *, client: Client):
        database = self.plugin.get_database()

        try:
            database.get_client(guid=client.get_guid())
        except Exception as e:
            raise Exception("Failed to check if client is present in the database {}".format(e))

        try:
            database.add_client(client=client)
        except Exception as e:
            raise Exception("Failed to add client to the database{}".format(e))

    def unregister_client(self, *, guid: ID):
        database = self.plugin.get_database()

        try:
            database.get_client(guid=guid)
        except Exception as e:
            raise Exception("Failed to check if client is present in the database {}".format(e))

        try:
            database.remove_client(guid=guid)
        except Exception as e:
            raise Exception("Failed to add client to the database{}".format(e))

    def get_client(self, *, guid: ID) -> Client:
        database = self.plugin.get_database()
        ret_val = None
        try:
            client_obj = database.get_client(guid=guid)
            ret_val = pickle.loads(client_obj.get(Constants.property_pickle_properties))
        except Exception as e:
            raise Exception("Failed to check if client is present in the database {}".format(e))

        return ret_val

    def modify(self, *, reservation_id: ID, modify_properties: dict):
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
