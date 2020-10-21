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
import queue
import threading

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.apis.i_authority import IAuthority
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.core.actor import Actor
from fabric.actor.core.kernel.broker_reservation_factory import BrokerReservationFactory
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.manage.authority_management_object import AuthorityManagementObject
from fabric.actor.core.manage.kafka.services.kafka_authority_service import KafkaAuthorityService
from fabric.actor.core.proxies.kafka.services.authority_service import AuthorityService
from fabric.actor.core.delegation.delegation_factory import DelegationFactory
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.client import Client
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.reservation_set import ReservationSet
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.security.auth_token import AuthToken
from fim.graph.abc_property_graph import ABCPropertyGraph


class Authority(Actor, IAuthority):
    """
    Authority is the base implementation for a site authority actor.
    """
    def __init__(self, *, identity: AuthToken = None, clock: ActorClock = None):
        super().__init__(auth=identity, clock=clock)
        self.type = ActorType.Authority
        # Initialization status.
        self.initialized = False
        # Reservations to redeem once the actor recovers.
        self.redeeming = ReservationSet()
        # Reservations to extendLease for once the actor recovers.
        self.extending_lease = ReservationSet()
        # Reservations to modifyLease for once the actor recovers
        self.modifying_lease = ReservationSet()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['recovered']
        del state['wrapper']
        del state['logger']
        del state['clock']
        del state['monitor']
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

        del state['redeeming']
        del state['extending_lease']
        del state['modifying_lease']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.recovered = False
        self.wrapper = None
        self.logger = None
        self.clock = None
        self.monitor = None
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

        self.redeeming = ReservationSet()
        self.extending_lease = ReservationSet()
        self.modifying_lease = ReservationSet()

    def register_client_slice(self, *, slice_obj:ISlice):
        self.wrapper.register_slice(slice_object=slice_obj)

    def available(self, *, resources: ResourceSet):
        self.policy.available(resources=resources)

    def claim(self, *, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.claim_request(reservation=reservation, caller=caller, callback=callback)

    def reclaim(self, *, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.reclaim_request(reservation=reservation, caller=caller, callback=callback)

    def claim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken):
        slice_obj = delegation.get_slice_object()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.claim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def reclaim_delegation(self, *, delegation: IDelegation, callback: IClientCallbackProxy, caller: AuthToken):
        slice_obj = delegation.get_slice_object()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def close_by_caller(self, *, reservation:IReservation, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise Exception("This actor cannot receive calls")

        self.wrapper.close_request(reservation=reservation, caller=caller, compare_sequence_numbers=True)

    def close_expiring(self, *, cycle: int):
        """
        Closes the expiring reservations for the specified cycle.

        @param cycle
                   cycle number
        """
        expired = self.policy.get_closing(cycle=cycle)
        if expired is not None:
            # self.logger.info("Authority expiring for cycle {} = {}".format(cycle, expired))
            self.close_reservations(reservations=expired)

    def donate_delegation(self, *, delegation: IDelegation):
        self.policy.donate_delegation(delegation=delegation)

    def donate(self, *, resources: ResourceSet):
        self.policy.donate(resources=resources)

    def donate_reservation(self, *, reservation: IClientReservation):
        self.policy.donate_reservation(reservation=reservation)

    def eject(self, *, resources: ResourceSet):
        self.policy.eject(resources=resources)

    def export(self, *, reservation: IBrokerReservation = None, resources: ResourceSet = None,
               term: Term = None, client: AuthToken = None) -> ID:
        if reservation is None:
            slice_obj = SliceFactory.create(slice_id=ID(), name=client.get_name(), data=ResourceData())
            slice_obj.set_owner(owner=client)
            slice_obj.set_broker_client()

            reservation = BrokerReservationFactory.create(rid=ID(), resources=resources, term=term, slice_obj=slice_obj)
            reservation.set_owner(owner=self.identity)

        self.wrapper.export(reservation=reservation, client=client)
        return reservation.get_reservation_id()

    def advertise(self, *, delegation: ABCPropertyGraph, client: AuthToken) -> ID:
        slice_obj = SliceFactory.create(slice_id=ID(), name=client.get_name(), data=ResourceData())
        slice_obj.set_owner(owner=client)
        slice_obj.set_broker_client()

        dlg_obj = DelegationFactory.create(did=delegation.get_graph_id(), slice_id=slice_obj.get_slice_id())
        dlg_obj.set_slice_object(slice_object=slice_obj)
        dlg_obj.set_graph(graph=delegation)
        self.wrapper.advertise(delegation=dlg_obj, client=client)
        return dlg_obj.get_delegation_id()

    def extend_lease(self, *, reservation:IAuthorityReservation, caller: AuthToken):
        if caller is None:
            if not self.recovered:
                self.extending_lease.add(reservation=reservation)
            else:
                self.wrapper.extend_lease_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                                  compare_sequence_numbers=False)
        else:
            if not self.is_recovered() or self.is_stopped():
                raise Exception("This actor cannot receive calls")
            self.wrapper.extend_lease_request(reservation=reservation, caller=caller, compare_sequence_numbers=True)

    def modify_lease(self, *, reservation:IAuthorityReservation, caller: AuthToken):
        if caller is None:
            if not self.recovered:
                self.modifying_lease.add(reservation=reservation)
            else:
                self.wrapper.modify_lease_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                                  compare_sequence_numbers=False)
        else:
            if not self.is_recovered() or self.stopped:
                raise Exception("This actor cannot receive calls")
            self.wrapper.modify_lease_request(reservation=reservation, caller=caller, compare_sequence_numbers=True)

    def extend_ticket(self, *, reservation: IReservation, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.extend_ticket_request(reservation=reservation, caller=caller, compare_sequence_numbers=True)

    def relinquish(self, *, reservation: IReservation, caller: AuthToken):
        if not self.is_recovered() or self.stopped:
            raise Exception("This actor cannot receive calls")
        self.wrapper.relinquish_request(reservation=reservation, caller=caller)

    def freed(self, *, resources: ResourceSet):
        self.policy.freed(resources=resources)

    def redeem(self, *, reservation: IReservation, callback: IControllerCallbackProxy, caller: AuthToken):
        if callback is None and caller is None:
            if not self.recovered:
                self.redeeming.add(reservation=reservation)
            else:
                self.wrapper.redeem_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                            callback=reservation.get_callback(), compare_sequence_numbers=False)
        else:
            if not self.is_recovered() or self.is_stopped():
                raise Exception("This actor cannot receive calls")

            if self.plugin.validate_incoming(reservation=reservation, auth=caller):
                self.wrapper.redeem_request(reservation=reservation, caller=caller, callback=callback,
                                            compare_sequence_numbers=True)
            else:
                self.logger.error("the redeem request is invalid")
        self.logger.debug("Completed processing Redeem Request")

    def ticket(self, *, reservation: IReservation, callback: IClientCallbackProxy, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.ticket_request(reservation=reservation, caller=caller, callback=callback, compare_seq_numbers=True)

    def tick_handler(self):
        # close expired reservations
        self.close_expiring(cycle=self.current_cycle)
        # process all requests for the current cycle
        self.policy.assign(cycle=self.current_cycle)

    def unavailable(self, *, resources: ResourceSet) -> int:
        return self.policy.unavailable(resources=resources)

    def register_client(self, *, client: Client):
        db = self.plugin.get_database()

        try:
            db.get_client(guid=client.get_guid())
        except Exception as e:
            self.logger.debug("Client does not exist")

        try:
            db.add_client(client=client)
        except Exception as e:
            raise e

    def unregister_client(self, *, guid:ID):
        db = self.plugin.get_database()
        db.remove_client(guid=guid)

    def get_client(self, *, guid: ID) -> Client:
        db = self.plugin.get_database()

        return db.get_client(guid=guid)

    def redeem_reservations(self, *, rset: ReservationSet):
        """
        Redeem all reservations:
        @param rset: reservation set
        """
        for reservation in rset.values():
            try:
                if isinstance(reservation, IAuthorityReservation):
                    self.redeem(reservation=reservation, callback=None, caller=None)
                else:
                    self.logger.warning("Reservation # {} cannot be redeemed".format(reservation.get_reservation_id()))
            except Exception as e:
                self.logger.error("Could not redeem for # {} {}".format(reservation.get_reservation_id(), e))

    def extend_lease_reservations(self, *, rset: ReservationSet):
        """
        Extend all reservations:
        @param rset: reservation set
        """
        for reservation in rset.values():
            try:
                self.extend_lease(reservation=reservation, caller=None)
            except Exception as e:
                self.logger.error("Could not redeem for # {} {}".format(reservation.get_reservation_id(), e))

    def issue_delayed(self):
        super().issue_delayed()
        self.redeem_reservations(rset=self.redeeming)
        self.redeeming.clear()
        self.extend_lease_reservations(rset=self.extending_lease)
        self.extending_lease.clear()

    @staticmethod
    def get_management_object_class() -> str:
        return AuthorityManagementObject.__name__

    @staticmethod
    def get_management_object_module() -> str:
        return AuthorityManagementObject.__module__

    @staticmethod
    def get_kafka_service_class() -> str:
        return AuthorityService.__name__

    @staticmethod
    def get_kafka_service_module() -> str:
        return AuthorityService.__module__

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        return KafkaAuthorityService.__name__

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        return KafkaAuthorityService.__module__
