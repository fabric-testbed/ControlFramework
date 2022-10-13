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
import traceback
from typing import TYPE_CHECKING

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.exceptions import ControllerException
from fabric_cf.actor.core.manage.controller_management_object import ControllerManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_controller_service import KafkaControllerService
from fabric_cf.actor.core.proxies.kafka.services.controller_service import ControllerService
from fabric_cf.actor.core.apis.abc_controller import ABCController
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.registry.peer_registry import PeerRegistry
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.fim.asm_update_thread import AsmUpdateThread

if TYPE_CHECKING:
    from fabric_cf.actor.core.time.actor_clock import ActorClock
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.util.id import ID


class Controller(ActorMixin, ABCController):
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
        self.asm_update_thread = AsmUpdateThread(name=f"{self.get_name()}-asm-thread", logger=self.logger)

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
        del state['extending_ticket']
        del state['redeeming']
        del state['extending_lease']
        del state['modifying_lease']
        del state['registry']

        del state['asm_update_thread']
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
        self.extending_ticket = ReservationSet()
        self.redeeming = ReservationSet()
        self.extending_lease = ReservationSet()
        self.modifying_lease = ReservationSet()
        self.registry = PeerRegistry()
        self.asm_update_thread = AsmUpdateThread(name=f"{self.get_name()}-asm-thread", logger=self.logger)
        self.event_processors = {}

    def set_logger(self, logger):
        super(Controller, self).set_logger(logger=logger)
        self.asm_update_thread.set_logger(logger=logger)

    def start(self):
        self.asm_update_thread.set_logger(logger=self.logger)
        self.asm_update_thread.start()
        super(Controller, self).start()

    def actor_added(self):
        super().actor_added()
        self.registry.actor_added()

    def add_broker(self, *, broker: ABCBrokerProxy):
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

    def claim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                broker: ABCBrokerProxy = None) -> ABCDelegation:
        raise ControllerException("Not implemented")

    def reclaim_delegation_client(self, *, delegation_id: str = None, slice_object: ABCSlice = None,
                                  broker: ABCBrokerProxy = None) -> ABCDelegation:
        raise ControllerException("Not implemented")

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
            raise ControllerException("Invalid argument")

        reservation = self.get_reservation(rid=rid)

        if reservation is None:
            raise ControllerException("Unknown reservation {}".format(rid))

        self.policy.demand(reservation=reservation)
        reservation.set_policy(policy=self.policy)

    def extend_lease_reservation(self, *, reservation: ABCControllerReservation):
        """
        Extend Lease for a reservation
        @param reservation reservation
        """
        if not self.recovered:
            self.extending_lease.add(reservation=reservation)
        else:
            self.wrapper.extend_lease(reservation=reservation)

    def extend_lease(self, *, reservation: ABCControllerReservation = None, rset: ReservationSet = None):
        if reservation is not None and rset is not None:
            raise ControllerException("Invalid Arguments: reservation and rset can not be both not None")
        if reservation is None and rset is None:
            raise ControllerException("Invalid Arguments: reservation and rset can not be both None")

        if reservation is not None:
            self.extend_lease_reservation(reservation=reservation)

        if rset is not None:
            for r in rset.values():
                try:
                    if isinstance(r, ABCControllerReservation):
                        self.extend_lease_reservation(reservation=r)
                    else:
                        self.logger.warning("Reservation #{} cannot extendLease".format(
                            r.get_reservation_id()))
                except Exception as e:
                    self.logger.error("Could not extend_lease for #{} e={}".format(
                        r.get_reservation_id(), e))

    def extend_ticket_client(self, *, reservation: ABCClientReservation):
        if not self.recovered:
            self.extending_ticket.add(reservation=reservation)
        else:
            self.wrapper.extend_ticket(reservation=reservation)

    def extend_tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, ABCClientReservation):
                    self.extend_ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation # {} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{} e: {}".format(reservation.get_reservation_id(), e))

    def get_broker(self, *, guid: ID) -> ABCBrokerProxy:
        return self.registry.get_broker(guid=guid)

    def get_brokers(self) -> list:
        return self.registry.get_brokers()

    def get_default_broker(self) -> ABCBrokerProxy:
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

    def redeem(self, *, reservation: ABCControllerReservation):
        if not self.recovered:
            self.redeeming.add(reservation=reservation)
        else:
            self.wrapper.redeem(reservation=reservation)

    def redeem_reservations(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, ABCControllerReservation):
                    self.redeem(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be redeemed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("Could not redeem for #{} {}".format(reservation.get_reservation_id(), e))

    def ticket_client(self, *, reservation: ABCClientReservation):
        if not self.recovered:
            self.ticketing.add(reservation=reservation)
        else:
            self.wrapper.ticket(reservation=reservation, destination=self)

    def tickets_client(self, *, rset: ReservationSet):
        for reservation in rset.values():
            try:
                if isinstance(reservation, ABCClientReservation):
                    self.ticket_client(reservation=reservation)
                else:
                    self.logger.warning("Reservation #{} cannot be ticketed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not ticket for #{} e: {}".format(reservation.get_reservation_id(), e))

    def tick_handler(self):
        self.close_expiring()
        self.process_redeeming()
        self.bid()

    def update_lease(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise ControllerException("This actor cannot receive calls")

        self.wrapper.update_lease(reservation=reservation, update_data=update_data, caller=caller)

    def update_ticket(self, *, reservation: ABCReservationMixin, update_data, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise ControllerException("This actor cannot receive calls")

        self.wrapper.update_ticket(reservation=reservation, update_data=update_data, caller=caller)

    def update_delegation(self, *, delegation: ABCDelegation, update_data, caller: AuthToken):
        raise ControllerException("Not supported in controller")

    def modify(self, *, reservation_id: ID, modified_sliver: BaseSliver):
        if reservation_id is None or modified_sliver is None:
            self.logger.error(f"rid {reservation_id} modified_sliver {modified_sliver}")

        reservation = None
        try:
            reservation = self.get_reservation(rid=reservation_id)
        except Exception as e:
            self.logger.error("Could not find reservation #{} e: {}".format(reservation_id, e))

        if reservation is None:
            raise ControllerException("Unknown reservation: {}".format(reservation_id))

        if reservation.get_leased_resources() is not None:
            reservation.get_resources().set_sliver(sliver=modified_sliver)
        else:
            self.logger.warning(f"There are no approved resources for {reservation_id}, do nothin!")
            return

        if not self.recovered:
            self.modifying_lease.add(reservation=reservation)
        else:
            self.wrapper.modify_lease(reservation=reservation)

    def save_extending_renewable(self):
        """
        For recovery, mark extending reservations renewable or the opposite
        and save this, then restore afterwards
        """
        for reservation in self.extending_ticket.values():
            try:
                if isinstance(reservation, ABCClientReservation):
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

    def get_asm_thread(self) -> AsmUpdateThread:
        return self.asm_update_thread
