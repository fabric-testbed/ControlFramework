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

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.manage.controller_management_object import ControllerManagementObject
from fabric.actor.core.manage.kafka.services.kafka_controller_service import KafkaControllerService
from fabric.actor.core.proxies.kafka.services.controller_service import ControllerService
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.apis.i_controller import IController
from fabric.actor.core.core.actor import Actor
from fabric.actor.core.registry.peer_registry import PeerRegistry
from fabric.actor.core.util.reservation_set import ReservationSet

if TYPE_CHECKING:
    from fabric.actor.core.time.actor_clock import ActorClock
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.apis.i_slice import ISlice
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.apis.i_controller_reservation import IControllerReservation


class Controller(Actor, IController):
    """
    Implements Controller
    """
    saved_extended_renewable = ReservationSet()

    def __init__(self, *, identity: AuthToken = None, clock: ActorClock = None):
        super().__init__(auth=identity, clock=clock)
        # Recovered reservations that need to obtain tickets.
        self.ticketing = ReservationSet()
        # Recovered reservations that need to extend tickets.
        self.extending_ticket = ReservationSet()
        # Recovered reservations that need to be redeemed.
        self.redeeming = ReservationSet()
        # Recovered reservations that need to extend leases.
        self.extending_lease = ReservationSet()
        # Recovered reservations that need to modify leases
        self.modifying_lease = ReservationSet()
        # Peer registry.
        self.registry = PeerRegistry()
        # initialization status
        self.initialized = False
        self.type = ActorType.Orchestrator

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
        del state['extending_ticket']
        del state['redeeming']
        del state['extending_lease']
        del state['modifying_lease']
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
        self.extending_ticket = ReservationSet()
        self.redeeming = ReservationSet()
        self.extending_lease = ReservationSet()
        self.modifying_lease = ReservationSet()
        self.registry = PeerRegistry()

    def actor_added(self):
        super().actor_added()
        self.registry.actor_added()

    def add_broker(self, *, broker: IBrokerProxy):
        self.registry.add_broker(broker=broker)

    def bid(self):
        """
        Bids for resources as dictated by the plugin bidding policy for the
        current cycle.

        @throws Exception in case of error
        """
        # Invoke policy module to select candidates for ticket and extend. Note
        # that candidates structure is discarded when we're done.
        candidates = self.policy.formulate_bids(cycle=self.current_cycle)
        if candidates is not None:
            # Issue new ticket requests.
            ticketing = candidates.get_ticketing()
            if ticketing is not None:
                for ticket in ticketing.values():
                    try:
                        self.wrapper.ticket(reservation=ticket, destination=self)
                    except Exception as e:
                        self.logger.error("unexpected ticket failure for #{} {}".format(ticket.get_reservation_id(), e))
                        ticket.fail(message="unexpected ticket failure {}".format(e))

            extending = candidates.get_extending()
            if extending is not None:
                for extend in extending.values():
                    try:
                        self.wrapper.extend_ticket(reservation=extend)
                    except Exception as e:
                        self.logger.error("unexpected extend failure for #{} {}".format(extend.get_reservation_id(), e))
                        extend.fail(message="unexpected extend failure {}".format(e))

    def claim_delegation_client(self, *, delegation_id: str = None, slice_object: ISlice = None,
                                broker: IBrokerProxy = None, id_token: str = None) -> IDelegation:
        raise Exception("Not implemented")

    def reclaim_delegation_client(self, *, delegation_id: str = None, slice_object: ISlice = None,
                                  broker: IBrokerProxy = None, id_token: str = None) -> IDelegation:
        raise Exception("Not implemented")

    def close_expiring(self):
        """
        Issues close requests on all reservations scheduled for closing on the
        current cycle
        """
        rset = self.policy.get_closing(cycle=self.current_cycle)

        if rset is not None and rset.size() > 0:
            self.logger.info("SlottedSM close expiring for cycle {} expiring {}".format(self.current_cycle, rset))
            self.close_reservations(reservations=rset)

    def demand(self, *, rid: ID):
        if rid is None:
            raise Exception("Invalid argument")

        reservation = self.get_reservation(rid=rid)

        if reservation is None:
            raise Exception("Unknown reservation {}".format(rid))

        self.policy.demand(reservation=reservation)
        reservation.set_policy(policy=self.policy)

    def extend_lease_reservation(self, *, reservation: IControllerReservation):
        """
        Extend Lease for a reservation
        @param reservation reservation
        """
        if not self.recovered:
            self.extending_lease.add(reservation=reservation)
        else:
            self.wrapper.extend_lease(reservation=reservation)

    def extend_lease(self, *, reservation: IControllerReservation = None, rset: ReservationSet = None):
        if reservation is not None and rset is not None:
            raise Exception("Invalid Arguments: reservation and rset can not be both not None")
        if reservation is None and rset is None:
            raise Exception("Invalid Arguments: reservation and rset can not be both None")

        if reservation is not None:
            self.extend_lease_reservation(reservation=reservation)

        if rset is not None:
            for r in rset.values():
                try:
                    if isinstance(r, IControllerReservation):
                        self.extend_lease_reservation(reservation=r)
                    else:
                        self.logger.warning("Reservation #{} cannot extendLease".format(
                            r.get_reservation_id()))
                except Exception as e:
                    self.logger.error("Could not extend_lease for #{} e={}".format(
                        r.get_reservation_id(), e))

    def extend_ticket_client(self, *, reservation: IClientReservation):
        if not self.recovered:
            self.extending_ticket.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket(reservation=reservation)

    def extend_tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, IClientReservation):
                    self.extend_ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation # {} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{} e: {}".format(reservation.get_reservation_id(), e))

    def get_broker(self, *, guid: ID) -> IBrokerProxy:
        return self.registry.get_broker(guid=guid)

    def get_brokers(self) -> list:
        return self.registry.get_brokers()

    def get_default_broker(self) -> IBrokerProxy:
        return self.registry.get_default_broker()

    def initialize(self):
        if not self.initialized:
            super().initialize()

            self.registry.set_slices_plugin(plugin=self.plugin)
            self.registry.initialize()

            self.initialized = True

    def process_redeeming(self):
        """
        Issue redeem requests on all reservations scheduled for redeeming on the current cycle
        """
        rset = self.policy.get_redeeming(cycle=self.current_cycle)

        if rset is not None and rset.size() > 0:
            self.logger.info("SlottedController redeem for cycle {} redeeming {}".format(self.current_cycle, rset))

            self.redeem_reservations(rset=rset)

    def redeem(self, *, reservation: IControllerReservation):
        if not self.recovered:
            self.redeeming.add(reservation=reservation)
        else:
            self.wrapper.redeem(reservation=reservation)

    def redeem_reservations(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, IControllerReservation):
                    self.redeem(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be redeemed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not redeem for #{} {}".format(reservation.get_reservation_id(), e))

    def ticket_client(self, *, reservation: IClientReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket(reservation=reservation, destination=self)

    def tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, IClientReservation):
                    self.ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{} e: {}".format(reservation.get_reservation_id(), e))

    def tick_handler(self):
        self.close_expiring()
        self.process_redeeming()
        self.bid()

    def update_lease(self, *, reservation: IReservation, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")

        self.wrapper.update_lease(reservation=reservation, update_data=update_data, caller=caller)

    def update_ticket(self, *, reservation: IReservation, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")

        self.wrapper.update_ticket(reservation=reservation, update_data=update_data, caller=caller)

    def update_delegation(self, *, delegation: IDelegation, update_data, caller: AuthToken):
        raise Exception("Not supported in controller")

    def modify(self, *, reservation_id: ID, modify_properties: dict):
        if reservation_id is None or modify_properties is None:
            self.logger.error("modifyProperties argument is null or non-existing reservation")

        rc = None
        try:
            rc = self.get_reservation(rid=reservation_id)
        except Exception as e:
            self.logger.error("Could not find reservation #{} e: {}".format(reservation_id, e))

        if rc is None:
            raise Exception("Unknown reservation: {}".format(reservation_id))

        if rc.get_resources() is not None:
            curr_config_props = rc.get_resources().get_config_properties()
            curr_config_props = PropList.merge_properties(incoming=modify_properties, outgoing=curr_config_props)
            rc.get_resources().set_config_properties(p=curr_config_props)

            if rc.get_leased_resources() is not None:
                curr_config_props = rc.get_leased_resources().get_config_properties()
                curr_config_props = PropList.merge_properties(incoming=modify_properties, outgoing=curr_config_props)
                rc.get_leased_resources().set_config_properties(p=curr_config_props)
            else:
                self.logger.warning("There were no leased resources for {}, no modify properties will be added".format(
                    reservation_id))
        else:
            self.logger.warning("There are no approved resources for {}, no modify properties will be added".format(
                reservation_id))

        if not self.recovered:
            self.modifying_lease.add(reservation=rc)
        else:
            self.wrapper.modify_lease(reservation=rc)

    def save_extending_renewable(self):
        """
        For recovery, mark extending reservations renewable or the opposite
        and save this, then restore afterwards
        """
        for reservation in self.extending_ticket.values():
            try:
                if isinstance(reservation, IClientReservation):
                    if not reservation.get_renewable():
                        reservation.set_renewable(renewable=True)
                        self.saved_extended_renewable.add(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be remarked".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not mark ticket renewable for #{} e: {}".format(
                    reservation.get_reservation_id(), e))

    def restore_extending_renewable(self):
        """
        Restore the value of renewable field after recovery if we changed it
        """
        for reservation in self.saved_extended_renewable.values():
            try:
                reservation.set_renewable(renewable=False)
            except Exception as e:
                self.logger.error("Could not remark ticket non renewable for #{} e: {}".format(
                    reservation.get_reservation_id(), e))

    def issue_delayed(self):
        super().issue_delayed()
        self.tickets_client(rset=self.ticketing)
        self.ticketing.clear()

        self.save_extending_renewable()
        self.extend_tickets_client(rset=self.extending_ticket)
        self.extending_ticket.clear()

        self.redeem_reservations(rset=self.redeeming)
        self.redeeming.clear()

        self.extend_lease(rset=self.extending_lease)
        self.extending_lease.clear()

    @staticmethod
    def get_management_object_class() -> str:
        return ControllerManagementObject.__name__

    @staticmethod
    def get_management_object_module() -> str:
        return ControllerManagementObject.__module__

    @staticmethod
    def get_kafka_service_class() -> str:
        return ControllerService.__name__

    @staticmethod
    def get_kafka_service_module() -> str:
        return ControllerService.__module__

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        return KafkaControllerService.__name__

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        return KafkaControllerService.__module__
