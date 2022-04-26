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

from fim.graph.abc_property_graph import ABCPropertyGraph
from fim.graph.resources.neo4j_arm import Neo4jARMGraph

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.authority_management_object import AuthorityManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_authority_service import KafkaAuthorityService
from fabric_cf.actor.core.proxies.kafka.services.authority_service import AuthorityService
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.client import Client
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.security.auth_token import AuthToken


class Authority(ActorMixin, ABCAuthority):
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
        del state['current_cycle']
        del state['first_tick']
        del state['stopped']
        del state['initialized']
        del state['thread_lock']
        del state['thread']
        del state['timer_queue']
        del state['event_queue']
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
        self.current_cycle = -1
        self.first_tick = True
        self.stopped = False
        self.initialized = False
        self.thread = None
        self.thread_lock = threading.Lock()
        self.timer_queue = queue.Queue()
        self.event_queue = queue.Queue()
        self.actor_main_lock = threading.Condition()
        self.closing = ReservationSet()
        self.message_service = None

        self.redeeming = ReservationSet()
        self.extending_lease = ReservationSet()
        self.modifying_lease = ReservationSet()

    def register_client_slice(self, *, slice_obj: ABCSlice):
        self.wrapper.register_slice(slice_object=slice_obj)

    def claim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy, caller: AuthToken):
        slice_obj = delegation.get_slice_object()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.claim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def reclaim_delegation(self, *, delegation: ABCDelegation, callback: ABCClientCallbackProxy, caller: AuthToken):

        slice_obj = delegation.get_slice_object()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.reclaim_delegation_request(delegation=delegation, caller=caller, callback=callback)

    def close_by_caller(self, *, reservation: ABCReservationMixin, caller: AuthToken):
        if not self.is_recovered() or self.is_stopped():
            raise AuthorityException(Constants.INVALID_ACTOR_STATE)

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

    def donate_delegation(self, *, delegation: ABCDelegation):
        self.policy.donate_delegation(delegation=delegation)

    def eject(self, *, resources: ResourceSet):
        self.policy.eject(resources=resources)

    def advertise(self, *, delegation: ABCPropertyGraph, delegation_name: str, client: AuthToken) -> str:
        slice_obj = SliceFactory.create(slice_id=ID(), name=client.get_name())
        slice_obj.set_owner(owner=client)
        slice_obj.set_broker_client()

        dlg_obj = DelegationFactory.create(did=delegation.get_graph_id(), slice_id=slice_obj.get_slice_id(),
                                           delegation_name=delegation_name)
        dlg_obj.set_slice_object(slice_object=slice_obj)
        dlg_obj.set_graph(graph=delegation)
        self.wrapper.advertise(delegation=dlg_obj, client=client)
        return dlg_obj.get_delegation_id()

    def extend_lease(self, *, reservation: ABCAuthorityReservation, caller: AuthToken = None,
                     callback: ABCControllerCallbackProxy = None,):
        if caller is None:
            if not self.recovered:
                self.extending_lease.add(reservation=reservation)
            else:
                self.wrapper.extend_lease_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                                  compare_sequence_numbers=False, callback=callback)
        else:
            if not self.is_recovered() or self.is_stopped():
                raise AuthorityException(Constants.INVALID_ACTOR_STATE)
            self.wrapper.extend_lease_request(reservation=reservation, caller=caller, compare_sequence_numbers=True,
                                              callback=callback)

    def modify_lease(self, *, reservation: ABCAuthorityReservation, caller: AuthToken,
                     callback: ABCControllerCallbackProxy = None,):
        if caller is None:
            if not self.recovered:
                self.modifying_lease.add(reservation=reservation)
            else:
                self.wrapper.modify_lease_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                                  compare_sequence_numbers=False, callback=callback)
        else:
            if not self.is_recovered() or self.stopped:
                raise AuthorityException(Constants.INVALID_ACTOR_STATE)
            self.wrapper.modify_lease_request(reservation=reservation, caller=caller, compare_sequence_numbers=True,
                                              callback=callback)

    def extend_ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.extend_ticket_request(reservation=reservation, caller=caller, compare_sequence_numbers=True,
                                           callback=callback)

    def relinquish(self, *, reservation: ABCReservationMixin, caller: AuthToken):
        if not self.is_recovered() or self.stopped:
            raise AuthorityException(Constants.INVALID_ACTOR_STATE)
        self.wrapper.relinquish_request(reservation=reservation, caller=caller)

    def freed(self, *, resources: ResourceSet):
        self.policy.freed(resources=resources)

    def redeem(self, *, reservation: ABCReservationMixin, callback: ABCControllerCallbackProxy = None,
               caller: AuthToken = None):
        if callback is None and caller is None:
            if not self.recovered:
                self.redeeming.add(reservation=reservation)
            else:
                self.wrapper.redeem_request(reservation=reservation, caller=reservation.get_client_auth_token(),
                                            callback=reservation.get_callback(), compare_sequence_numbers=False)
        else:
            if not self.is_recovered() or self.is_stopped():
                raise AuthorityException(Constants.INVALID_ACTOR_STATE)

            if self.plugin.validate_incoming(reservation=reservation, auth=caller):
                self.wrapper.redeem_request(reservation=reservation, caller=caller, callback=callback,
                                            compare_sequence_numbers=True)
            else:
                self.logger.error("the redeem request is invalid")
        self.logger.debug("Completed processing Redeem Request")

    def ticket(self, *, reservation: ABCReservationMixin, callback: ABCClientCallbackProxy, caller: AuthToken):
        slice_obj = reservation.get_slice()
        if slice_obj is not None:
            slice_obj.set_broker_client()

        self.wrapper.ticket_request(reservation=reservation, caller=caller, callback=callback,
                                    compare_seq_numbers=True)

    def tick_handler(self):
        # close expired reservations
        self.close_expiring(cycle=self.current_cycle)
        # process all requests for the current cycle
        self.policy.assign(cycle=self.current_cycle)

    def register_client(self, *, client: Client):
        db = self.plugin.get_database()

        try:
            db.get_client(guid=client.get_guid())
        except Exception as e:
            self.logger.debug("Client does not exist e:{}".format(e))

        db.add_client(client=client)

    def unregister_client(self, *, guid: ID):
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
                if isinstance(reservation, ABCAuthorityReservation):
                    self.redeem(reservation=reservation)
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
                self.extend_lease(reservation=reservation)
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

    def set_aggregate_resource_model(self, aggregate_resource_model: Neo4jARMGraph):
        """
        Set aggregate resource model
        :param aggregate_resource_model: resource model
        :return:
        """
        self.policy.set_aggregate_resource_model(aggregate_resource_model=aggregate_resource_model)

    def load_model(self, *, graph_id: str):
        self.policy.set_aggregate_resource_model_graph_id(graph_id=graph_id)
        self.policy.load_aggregate_resource_model()
