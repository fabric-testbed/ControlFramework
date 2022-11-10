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

import json
import threading
import time
import traceback
from typing import TYPE_CHECKING, List

from fim.slivers.attached_components import ComponentType
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Labels
from fim.slivers.network_node import NodeSliver, NodeType
from fim.slivers.network_service import ServiceType, NetworkServiceSliver

from fabric_cf.actor.core.apis.abc_authority_policy import ABCAuthorityPolicy
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ReservationException
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.util.rpc_exception import RPCError
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin, ReservationCategory
from fabric_cf.actor.core.kernel.predecessor_state import PredecessorState
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.kernel.reservation import Reservation
from fabric_cf.actor.core.kernel.reservation_states import ReservationPendingStates, ReservationStates, JoinState
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_state import ReservationState
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.core.util.utils import sliver_to_str
from fabric_cf.actor.fim.fim_helper import FimHelper

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
    from fabric_cf.actor.core.apis.abc_client_policy import ABCClientPolicy
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.resource_type import ResourceType


class ReservationClient(Reservation, ABCControllerReservation):
    """
    Reservation state machine for a client-side reservation. Role: orchestrator,
    or an agent requesting tickets from an upstream agent. This class
    includes support for client-side handling of leases as well as tickets; lease
    handling is relevant only to the orchestrator.

    Implementation note on terms. One complication in ReservationClient is that
    acquiring or renewing a lease is a two-step process (first the ticket, then
    the lease), thus there are some intermediate states and corner cases, and
    multiple terms to keep track of. Problem: we could get confused if we try to
    extend a ticket before the redeem() or extendLease() for the previously
    awarded ticket completes. So we do not allow it: if the ticket term is
    shorter than the time to redeem it, the reservation is forced to expire.

    Implementation note: When we receive a new lease, new resources may require
    some join processing. The current approach is to enter an (Active, None)
    state immediately. Resources are presumed to automatically enter service
    (e.g., by joining a collective) as they join: if any subset of the resources
    could be active, then the ReservationClient is considered active. The
    joinstate tracks joining for the reservation's first lease only, just so that
    we can sequence/time the join and/or fail if all resources fail to prime. The
    primary purpose of joinstate is to implement reservation groups with
    sequenced priming or joining.
    """

    CLOSE_COMPLETE = "close complete"

    def __init__(self, *, rid: ID, resources: ResourceSet = None, term: Term = None,
                 slice_object: ABCSlice = None, broker: ABCBrokerProxy = None):
        super().__init__(rid=rid, resources=resources, term=term, slice_object=slice_object)
        self.service_pending = JoinState.None_
        # Proxy to the broker that serves tickets for this reservation.
        self.broker = broker
        # Proxy to the site authority that serves leases for this reservation.
        self.authority = None
        # Sequence number for incoming updateTicket messages.
        self.sequence_ticket_in = 0
        # Sequence number for outgoing ticket/extend ticket messages. Increases with every new message.
        self.sequence_ticket_out = 0
        # Sequence number for incoming updateLease messages.
        self.sequence_lease_in = 0
        # Sequence number for outgoing redeem/extend lease messages. Increases with every new message.
        self.sequence_lease_out = 0
        # Does this reservation represent resources exported by a broker?
        self.exported = False
        # The most recent granted term for a ticket. If the reservation has
        # obtained/extended a ticket but has not yet redeemed or extended its
        # lease, this field will have the same value as term. However, once the
        # site sends the update lease message, term may change and may no longer
        # equal ticketTerm. Use ticketTerm if you want to make sure that you refer
        # to the term reflected in the latest ticket.
        self.ticket_term = None
        # The previous ticket term. Some policy decisions, e.g., updating internal
        # calendar structures, may require access to the previous ticket term.
        self.previous_ticket_term = None
        # The most recent granted term for a lease. Similarly to ticketTerm, term
        # equals lease term after the reservation has completed a redeem or extend
        # lease operation, but before it has extended its ticket. If you require
        # access to the latest lease term use this field.
        self.lease_term = None
        # The previous lease term.
        self.previous_lease_term = None
        # The leased resources. Will be null if no resource have yet been leased.
        self.leased_resources = None
        # The most recently recommended term for new requests/extensions for this
        # reservation. This field will be set by the programmer/controllers to pass
        # information to the resource policy. The policy must examine this field
        # and decide what to do. Once a decision is made, the term chosen by the
        # policy will be in approvedTerm.
        self.suggested_term = term
        # The most recently recommended resources for new requests/extensions for
        # this reservation. This field will be set by the programmer/orchestrator to
        # pass information to the resource policy. The policy must examine this
        # field and decide what to do. Once a decision is made, the resources
        # chosen by the policy will be in approvedResources.
        self.suggested_resources = resources
        # On the orchestrator, ReservationClient has an additional joinstate
        # variable to track and sequence join/redeem operations. Reservations may
        # be "blocked" from redeeming or joining the guest (i.e.,
        # configuration/post-install) until their "predecessor" reservations have
        # completed. There is at most one predecessor for joining and another for
        # redeeming: these may be the same, or either may be specified without the
        # other.
        self.joinstate = JoinState.NoJoin
        # Join predecessors for this reservation (orchestrator only).
        self.join_predecessors = {}
        # Redeem predecessors for this reservation (orchestrator only).
        self.redeem_predecessors = {}
        # The status of the last ticket update.
        self.last_ticket_update = UpdateData()
        # The status of the last lease update.
        self.last_lease_update = UpdateData()
        # The cycle in which we have to issue ticket update request for this
        # reservation. The value is a cache of the the response we received from
        # the broker. This field is needed primarily for recovery: especially in
        # the case when the broker had also failed.
        self.renew_time = 0
        # Callback object for callbacks on operations issued from this class.
        self.callback = None
        # Relinquish status.
        self.relinquished = False
        # Set to true if a close is received while redeem is in progress.
        self.closed_during_redeem = False
        # The policy in control of this reservation. Some reservation operations
        # require interacting with the policy.
        self.policy = None
        # True if the programmer has set new suggestedTerm or suggestedResources
        # since the last policy decision. This field is cleared when we receive an
        # updateTicket.
        self.suggested = True

        self.renewable = False
        self.approved_resources = resources
        self.approved_term = term
        self.approved = True
        self.category = ReservationCategory.Client

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['actor']
        del state['logger']
        del state['slice']
        del state['approved']
        del state['previous_resources']
        del state['bid_pending']
        del state['dirty']
        del state['expired']
        del state['pending_recover']
        del state['state_transition']
        del state['service_pending']

        del state['suggested']
        del state['thread_lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.actor = None
        self.logger = None
        self.slice = None
        self.approved = False
        self.previous_resources = None
        self.bid_pending = False
        self.dirty = False
        self.expired = False
        self.pending_recover = False
        self.state_transition = False
        self.service_pending = ReservationPendingStates.None_

        self.suggested = True
        self.thread_lock = threading.Lock()

    def restore(self, *, actor: ABCActorMixin, slice_obj: ABCSlice):
        """
        Must be invoked after creating reservation from unpickling
        """
        super().restore(actor=actor, slice_obj=slice_obj)
        if actor is not None:
            if self.leased_resources is not None:
                self.leased_resources.restore(plugin=actor.get_plugin(), reservation=self)
            if self.suggested_resources is not None:
                self.suggested_resources.restore(plugin=actor.get_plugin(), reservation=self)
            if self.broker is not None:
                self.broker.set_logger(logger=actor.get_logger())
            if self.authority is not None:
                self.authority.set_logger(logger=actor.get_logger())
            if self.callback is not None:
                self.callback.set_logger(logger=actor.get_logger())
            self.policy = actor.get_policy()

        self.suggested = False

    def absorb_lease_update(self, *, incoming: ABCReservationMixin, update_date: UpdateData):
        """
        Absorbs and incoming lease update.

        @param incoming
                   incoming update
        @param update_date
                   update data
        @throws Exception
        """
        if self.leased_resources is None:
            self.leased_resources = self.resources.abstract_clone()

        if self.state == ReservationStates.CloseWait and incoming.get_resources().get_concrete_units() != 0:
            # We are waiting for a FIN, and this is not it. Minor hack: do not
            # incorporate changes into the resource set, since there may be new
            # resources and we do not want to process the joins. Just keep
            # waiting for the FIN. Essentially rejects the update without
            # transition to Failed.
            update_date.post(event="reservation is closing, rejected lease is non-empty")
            self.logger.warning("non-empty lease update received in CloseWait: waiting for FIN")
        else:
            self.leased_resources.update(reservation=self, resource_set=incoming.get_resources())

        # Remember the current term and lease term and absorb the incoming ones
        self.previous_term = self.term
        self.previous_lease_term = self.lease_term
        self.term = incoming.get_term()
        self.lease_term = self.term

    def absorb_ticket_update(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Absorbs an incoming ticket update.

        @param incoming
                   incoming ticket update
        @param update_data
                   update data
        @throws Exception
        """
        site_authority = incoming.get_resources().get_site_proxy()
        if self.authority is None:
            self.authority = site_authority

        self.logger.debug(f"Authority {self.authority} Site Authority {site_authority}")
        assert self.authority.get_name() == site_authority.get_name()

        # Remember the current term and ticket term and absorb the incoming term
        self.previous_term = self.term
        self.previous_lease_term = self.ticket_term
        self.ticket_term = incoming.get_term()
        self.term = self.ticket_term

        self.resources.update(reservation=self, resource_set=incoming.get_resources())
        self.logger.debug("absorb_update: {}".format(incoming))

        self.policy.update_ticket_complete(reservation=self)

    def accept_lease_update(self, *, incoming: ABCReservationMixin, update_data: UpdateData) -> bool:
        """
        Determines whether the incoming lease update is acceptable and if so
        accepts it.

        @param incoming
                   incoming lease update
        @param update_data
                   update data
        @return true if the update was successful
        """
        # should we absorb this. What if decide not to accept the update?
        self.last_lease_update.absorb(other=update_data)

        # Policy: if this lease update fails, then transition to Failed.
        # Alternative: could transition to (state, None) to allow retry of the
        # redeem/extend by a higher level.
        if update_data.failed:
            self.fail(message=f"failed lease update- {update_data.get_message()}",
                      sliver=incoming.get_resources().get_sliver())
            #self.transition(prefix="failed lease update", state=ReservationStates.Failed,
            #                pending=ReservationPendingStates.None_)
        else:
            try:
                self.lease_update_satisfies(incoming=incoming, update_data=update_data)
                self.absorb_lease_update(incoming=incoming, update_date=update_data)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.transition(prefix="rejected lease update",
                                state=ReservationStates.Failed,
                                pending=ReservationPendingStates.None_)
                update_data.post_error(event=str(e))
                self.logger.error("accept_lease_update e:{}".format(e))
        return update_data.successful()

    def accept_ticket_update(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Determines whether the incoming ticket update is acceptable and if so
        accepts it.

        @param incoming
                   incoming ticket update
        @param update_data
                   update data
        @return true if the update was successful
        """
        self.last_ticket_update.absorb(other=update_data)
        success = True
        if update_data.is_failed():
            success = False
        else:
            try:
                self.ticket_update_satisfies(incoming=incoming, update_data=update_data)
                self.absorb_ticket_update(incoming=incoming, update_data=update_data)
            except Exception as e:
                success = False
                update_data.error(message=str(e))
                self.logger.error(traceback.format_exc())
                self.logger.error("accept_ticket_update e:{}".format(e))

        if not success:
            if self.state == ReservationStates.Nascent:
                self.transition(prefix="failed ticket reserve",
                                state=ReservationStates.Failed, pending=ReservationPendingStates.None_)
            else:
                self.transition(prefix="failed ticket update", state=self.state,
                                pending=ReservationPendingStates.None_)
        return success

    def approve_join(self):
        """
        Join predicate: invoked internally to determine if reservation
        post-install actions can take place. This gives subclasses an opportunity
        sequence post-install configuration actions.

        If false, the reservation enters a "BlockedJoin" sub-state until a
        subsequent approveJoin returns true. When true, the reservation can
        manipulate the current reservation's property lists and attributes to
        facilitate configuration. Note that approveJoin may be invoked multiple
        times, and should be idempotent.
        @returns true if approved; false otherwise
        """
        approved = True
        for pred_state in self.join_predecessors.values():
            if pred_state.get_reservation().is_failed() or pred_state.get_reservation().is_closed():
                self.logger.error("join predecessor reservation is in a terminal state. ignoring it: {}".
                                  format(pred_state.get_reservation()))
                continue
            if not pred_state.get_reservation().is_active_joined():
                approved = False
                break

        if approved:
            self.prepare_join()

        return approved

    def approve_redeem(self):
        """
        Redeem predicate: invoked internally to determine if the reservation
        should be redeemed. This gives subclasses an opportunity sequence install
        configuration actions at the authority side.

        If false, the reservation enters a "BlockedRedeem" sub-state until a
        subsequent approveRedeem returns true. When true, the reservation can
        manipulate the current reservation's properly lists and attributes to
        facilitate configuration. Note that approveRedeem may be polled multiple
        times, and should be idempotent.


        @return true if approved; false otherwise
        """
        approved = True

        for pred_state in self.redeem_predecessors.values():
            if pred_state.get_reservation() is None or \
                    pred_state.get_reservation().is_failed() or \
                    pred_state.get_reservation().is_closed():
                self.logger.error("redeem predecessor reservation is in a terminal state or reservation is null."
                                  " ignoring it: {}".format(pred_state.get_reservation()))
                continue

            if not pred_state.get_reservation().is_active():
                approved = False
                break

        if approved:
            self.prepare_redeem()

        return approved

    def can_redeem(self) -> bool:
        ticketed_states_to_redeem = [ReservationStates.ActiveTicketed, ReservationStates.Ticketed]
        is_valid_state = self.state in ticketed_states_to_redeem or \
                         (self.state == ReservationStates.Active and self.pending_recover)
        if is_valid_state and self.pending_state == ReservationPendingStates.None_:
            assert self.resources is not None
            c = self.resources.get_resources()
            assert c is not None and c.get_units() > 0
            return True
        return False

    def prepare_ticket(self, extend: bool = False):
        # Parent reservations have been Ticketed; Update BQM Node and Component Id in Node Map
        # Used by Broker to set vlan - source: (c)
        # local_name source: (a)
        # NSO device name source: (a) - need to find the owner switch of the network service in CBM
        # and take its .name or labels.local_name

        assert self.resources is not None
        assert self.resources.sliver is not None
        sliver = self.resources.sliver

        if extend:
            assert self.requested_resources is not None
            assert self.requested_resources.sliver is not None
            sliver = self.requested_resources.sliver

        if not isinstance(sliver, NetworkServiceSliver):
            return

        for ifs in sliver.interface_info.interfaces.values():
            component_name, rid = ifs.get_node_map()

            # Skip Facility Port Interfaces for Create or Modify
            # Allocated interfaces on Modify i.e. ExtendTicket
            if component_name == str(NodeType.Facility) or ifs.label_allocations is not None:
                continue

            pred_state = self.redeem_predecessors.get(ID(uid=rid))
            if not pred_state:
                self.logger.error(f"Redeem predecessors not found {rid} for {self.get_reservation_id()}")
                continue
            parent_res = pred_state.get_reservation()
            if parent_res is not None and (parent_res.is_ticketed() or parent_res.is_active()):
                node_sliver = parent_res.get_resources().get_sliver()
                component = node_sliver.attached_components_info.get_device(name=component_name)
                graph_id, bqm_component_id = component.get_node_map()
                graph_id, node_id = node_sliver.get_node_map()
                ifs.set_node_map(node_map=(node_id, bqm_component_id))

                # For shared NICs grab the MAC & VLAN from corresponding Interface Sliver
                # maintained in the Parent Reservation Sliver
                if component.get_type() == ComponentType.SharedNIC:
                    parent_res_ifs_sliver = FimHelper.get_site_interface_sliver(component=component,
                                                                                local_name=ifs.get_labels().local_name)
                    parent_labs = parent_res_ifs_sliver.get_label_allocations()

                    if component.get_model() == Constants.OPENSTACK_VNIC_MODEL:
                        ifs.labels = Labels.update(ifs.labels, mac=parent_labs.mac,
                                                   instance_parent=f"{parent_res.get_reservation_id()}-{node_sliver.get_name()}")
                    else:
                        ifs.labels = Labels.update(ifs.labels, mac=parent_labs.mac, vlan=parent_labs.vlan)

            self.logger.trace(f"Updated Network Res# {self.get_reservation_id()} {sliver}")

    def approve_ticket(self, extend: bool = False):
        """
        Ticket predicate: invoked internally to determine if the reservation
        should be ticketed. This gives subclasses an opportunity sequence actions at the orchestrator side.

        If false, the reservation enters a "BlockedTicket" sub-state until a subsequent approve_ticket returns true.
        When true, the reservation can manipulate the current reservation's attributes to
        facilitate ticketing from the broker. Note that approve_ticket may be polled multiple
        times, and should be idempotent.

        @return true if approved; false otherwise
        """
        approved = True
        for pred_state in self.redeem_predecessors.values():
            if pred_state.get_reservation() is None:
                self.logger.error(f"redeem predecessor reservation is null, ignoring it: {pred_state.get_reservation()}")
                continue
            if pred_state.get_reservation().is_failed() or pred_state.get_reservation().is_closed() or \
                    pred_state.get_reservation().is_closing():
                self.logger.error(f"redeem predecessor reservation# {pred_state.get_reservation().get_reservation_id()}"
                                  f" is in a terminal state, failing the reservation# {self.get_reservation_id()}")
                self.fail(message=f"redeem predecessor reservation# {pred_state.get_reservation().get_reservation_id()}"
                                  f" is in a terminal state")

            if not (pred_state.get_reservation().is_ticketed() or pred_state.get_reservation().is_active()):
                approved = False
                break

        if approved:
            self.prepare_ticket(extend=extend)

        return approved

    def can_ticket(self, extend: bool = False) -> bool:
        supported_ns = [str(ServiceType.L2STS), str(ServiceType.L2Bridge), str(ServiceType.L2PTP),
                        str(ServiceType.FABNetv4), str(ServiceType.FABNetv6), str(ServiceType.PortMirror)]

        ret_val = False
        if self.get_type() is not None:
            resource_type_str = str(self.get_type())
            if resource_type_str in supported_ns:
                ret_val = self.approve_ticket(extend=extend)
            else:
                ret_val = True

        return ret_val

    def can_renew(self) -> bool:
        """
        The reservation cannot be renewed if a previous renew attempt failed, or
        if the reservation is terminal (closed, failed, closing), or if a renew
        is currently in progress, or if the reservation has not yet been
        ticketed.
        """
        if not self.renewable:
            return False

        if self.last_ticket_update is None:
            return False

        if self.is_terminal():
            return False

        if self.is_extending_ticket():
            return False

        if self.is_extending_lease() or self.is_redeeming():
            return False

        return self.last_ticket_update.successful()

    def clear_notice(self):
        self.last_ticket_update.clear()
        self.last_lease_update.clear()

    def do_relinquish(self):
        """
        Perform needed steps to relinquish/cleanup a reservation
        """
        if not self.relinquished:
            self.relinquished = True
            try:
                if self.policy is not None:
                    self.policy.closed(reservation=self)
                else:
                    self.logger.warning("doRelinquish(): policy not set in reservation {}, "
                                        "unable to call policy.closed(), continuing".format(self.rid))
            except Exception as e:
                self.logger.error("close with policy e:{}".format(e))
            if self.get_requested_resources() is not None:
                try:
                    self.sequence_ticket_out += 1
                    RPCManagerSingleton.get().relinquish(reservation=self)
                except Exception as e:
                    self.logger.error("broker reports relinquish error: e: {}".format(e))
                    self.logger.error(traceback.format_exc())
            else:
                self.logger.info("Reservation #{} has not requested any resource yet. Nothing to relinquish.".
                                 format(self.rid))

    def close(self):
        if self.state == ReservationStates.Nascent or self.state == ReservationStates.Failed:
            self.logger.debug(f"Reservation in state: {self.state}, transition to {ReservationStates.Closed}")
            self.transition(prefix="close", state=ReservationStates.Closed, pending=self.pending_state)
            if self.broker is not None:
                self.logger.debug("Triggering relinquish")
                self.do_relinquish()
        elif self.state == ReservationStates.Ticketed:
            if self.pending_state != ReservationPendingStates.Redeeming:
                self.logger.debug("Reservation in ticketed")
                self.transition(prefix="close", state=ReservationStates.Closed, pending=self.pending_state)
                self.logger.debug("Triggering relinquish")
                self.do_relinquish()
            else:
                self.logger.info("Received close for a redeeming reservation. Deferring close until redeem completes.")
                self.closed_during_redeem = True
        elif self.state == ReservationStates.Active or self.state == ReservationStates.ActiveTicketed:
            if self.pending_state == ReservationPendingStates.Redeeming:
                self.logger.info("Received close for a redeeming reservation. Deferring close until redeem completes.")
                self.closed_during_redeem = True
            else:
                if self.joinstate == JoinState.BlockedJoin:
                    # no join operations have taken place, so no need for local leave operations
                    self.transition(prefix="close", state=ReservationStates.CloseWait,
                                    pending=ReservationPendingStates.None_)
                    try:
                        self.sequence_lease_out += 1
                        RPCManagerSingleton.get().close(reservation=self)
                    except Exception as e:
                        self.logger.error("authority reports close error: e: {}".format(e))
                        self.logger.error(traceback.format_exc())
                        self.transition(prefix="close", state=ReservationStates.Closed,
                                        pending=ReservationPendingStates.None_)
                        self.do_relinquish()
                else:
                    self.transition_with_join(prefix="close", state=ReservationStates.Active,
                                              pending=ReservationPendingStates.Closing,
                                              join_state=JoinState.NoJoin)

    def extend_lease(self):
        # Not permitted if there is a pending operation.
        self.nothing_pending()
        self.requested_term.enforce_extends_term(old_term=self.lease_term)

        if self.state == ReservationStates.ActiveTicketed:
            self.transition(prefix="extend lease", state=ReservationStates.ActiveTicketed,
                            pending=ReservationPendingStates.ExtendingLease)
            self.sequence_lease_out += 1
            RPCManagerSingleton.get().extend_lease(proxy=self.authority, reservation=self,
                                                   caller=self.slice.get_owner())
        else:
            self. error(err="Wrong state to initiate extend lease: {}".format(ReservationStates(self.state).name))

    def modify_lease(self):
        # Not permitted if there is a pending operation.
        self.nothing_pending()

        if self.state == ReservationStates.Active:
            self.transition(prefix="modify lease", state=ReservationStates.Active,
                            pending=ReservationPendingStates.ModifyingLease)
            self.sequence_lease_out += 1
            RPCManagerSingleton.get().modify_lease(proxy=self.authority, reservation=self,
                                                   caller=self.slice.get_owner())
        else:
            self.error(err="Wrong state to initiate modify lease: {}".format(ReservationStates(self.state).name))

    def extend_ticket(self, *, actor: ABCActorMixin):
        # Not permitted if there is a pending operation: cannot renew while a
        # previous renew or redeem is in progress (see note above).
        self.nothing_pending()
        assert self.broker is not None

        self.approved_term.enforce_extends_term(old_term=self.term)
        self.requested_term = self.approved_term
        self.requested_resources = self.approved_resources

        if self.state == ReservationStates.Ticketed:
            if self.is_controller(actor=actor):
                self.transition(prefix=Constants.EXTEND_TICKET, state=ReservationStates.Ticketed,
                                pending=ReservationPendingStates.ExtendingTicket)
            else:
                raise ReservationException("Cannot extend ticket while in Ticketed")
        elif self.state == ReservationStates.Active:
            self.transition(prefix=Constants.EXTEND_TICKET, state=ReservationStates.Active,
                            pending=ReservationPendingStates.ExtendingTicket)
        else:
            self.error(err="Wrong state to initiate extend ticket: {}".format(ReservationStates(self.state).name))

        # Extend Ticket is invoked by Probe
        if not self.can_ticket(extend=True):
            self.transition_with_join(prefix="Extend ticket blocked", state=self.state,
                                      pending=self.pending_state, join_state=JoinState.BlockedExtendTicket)
            self.logger.info("Reservation has to wait for the dependencies to be extended!")
            print("Reservation has to wait for the dependencies to be extended!")
            return

        self.sequence_ticket_out += 1
        RPCManagerSingleton.get().extend_ticket(reservation=self)

    def get_authority(self) -> ABCAuthorityProxy:
        return self.authority

    def get_broker(self) -> ABCBrokerProxy:
        return self.broker

    def get_join_state(self) -> JoinState:
        return self.joinstate

    def get_join_state_name(self) -> str:
        return JoinState(self.joinstate).name

    def get_leased_abstract_units(self) -> int:
        if self.leased_resources is not None:
            return self.leased_resources.get_units()
        return 0

    def get_leased_resources(self) -> ResourceSet:
        return self.leased_resources

    def get_leased_units(self) -> int:
        if self.leased_resources is not None:
            cs = self.leased_resources.get_resources()
            if cs is not None:
                return cs.get_units()
        return 0

    def get_lease_sequence_in(self) -> int:
        return self.sequence_lease_in

    def get_lease_sequence_out(self) -> int:
        return self.sequence_lease_out

    def get_lease_term(self) -> Term:
        return self.lease_term

    def get_notices_dict(self) -> str:
        notices = {}
        if self.get_error_message() is not None and len(self.get_error_message()) > 0:
            notices["error_message"] = self.get_error_message()

        if self.get_last_lease_update() is not None and len(self.get_last_lease_update()) > 0:
            notices["last_lease_update"] = self.get_last_lease_update()

        if self.get_last_ticket_update() is not None and len(self.get_last_ticket_update()) > 0:
            notices["last_ticket_update"] = self.get_last_ticket_update()

        return json.dumps(notices)

    def get_notices(self) -> str:
        s = super().get_notices()
        notices = self.get_update_notices()
        if notices is not None:
            s += " {}".format(notices)
        return s

    def get_previous_lease_term(self) -> Term:
        return self.previous_lease_term

    def get_previous_ticket_term(self) -> Term:
        return self.previous_ticket_term

    def get_renew_time(self) -> int:
        return self.renew_time

    def get_reservation_state(self) -> ReservationState:
        return ReservationState(state=self.state, pending=self.pending_state, joining=self.joinstate)

    def get_suggested_resources(self) -> ResourceSet:
        return self.suggested_resources

    def get_suggested_term(self) -> Term:
        return self.suggested_term

    def get_suggested_type(self) -> ResourceType:
        if self.suggested_resources is not None:
            return self.suggested_resources.type
        return None

    def get_ticket_sequence_in(self) -> int:
        return self.sequence_ticket_in

    def get_ticket_sequence_out(self) -> int:
        return self.sequence_ticket_out

    def get_ticket_term(self) -> Term:
        return self.ticket_term

    def get_type(self) -> ResourceType:
        if self.resources is not None:
            return self.resources.type
        elif self.requested_resources is not None:
            return self.requested_resources.type
        elif self.approved_resources is not None:
            return self.approved_resources.get_type()
        elif self.suggested_resources is not None:
            return self.suggested_resources.type
        else:
            return None

    def get_update_notices(self) -> str:
        result = ""
        if self.last_ticket_update is not None:
            if self.last_ticket_update.get_message() is not None and self.last_ticket_update.get_message() != "":
                result += f" (Last ticket update: {self.last_ticket_update.get_message()})"
            ev = self.last_ticket_update.get_events()
            if ev is not None and ev != "":
                result += f" (Ticket events: {ev})"

        if self.last_lease_update is not None:
            if self.last_lease_update.get_message() is not None and self.last_lease_update.get_message() != "":
                result += f" (Last lease update: {self.last_lease_update.get_message()})"
            ev = self.last_lease_update.get_events()
            if ev is not None and ev != "":
                result += f" (Lease events: {ev})"
        return result

    def get_last_ticket_update(self) -> str:
        result = ""
        if self.last_ticket_update is not None:
            if self.last_ticket_update.get_message() is not None and self.last_ticket_update.get_message() != "":
                result += f"{self.last_ticket_update.get_message()}, "
            ev = self.last_ticket_update.get_events()
            if ev is not None and ev != "":
                result += f"events: {ev}, "
            result = result[:-2]
        return result

    def get_last_lease_update(self) -> str:
        result = ""
        if self.last_lease_update is not None:
            if self.last_lease_update.get_message() is not None and self.last_lease_update.get_message() != "":
                result += f"{self.last_lease_update.get_message()}, "
            ev = self.last_lease_update.get_events()
            if ev is not None and ev != "":
                result += f"{ev}, "
            result = result[:-2]
        return result

    def is_active_joined(self) -> bool:
        return self.is_active() and self.joinstate == JoinState.NoJoin

    def is_exported(self) -> bool:
        return self.exported

    @staticmethod
    def is_controller(*, actor: ABCActorMixin):
        """
        Check if the actor is Controller
        @return true if actor is a controller, false otherwise
        """
        from fabric_cf.actor.core.core.controller import Controller
        return isinstance(actor, Controller)

    def lease_update_satisfies(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Check if lease update can be satisfied
        @return true if update is acceptable, false otherwise
        """
        try:
            self.policy.lease_satisfies(request_resources=self.resources, actual_resources=incoming.get_resources(),
                                        requested_term=self.term, actual_term=incoming.get_term())

            if self.is_active_ticketed():
                assert incoming.get_term().get_new_start_time() == self.term.get_new_start_time()
        except Exception as e:
            self.logger.warning("lease update does not satisfy ticket term (ignored) e: {}".format(e))
            update_data.post(event="lease update does not satisfy ticket term (ignored)")

    def prepare(self, *, callback: ABCCallbackProxy, logger):
        self.set_logger(logger=logger)
        self.callback = callback

    def prepare_join(self):
        return

    def prepare_probe(self):
        if self.leased_resources is not None and self.get_join_state() != JoinState.BlockedJoin:
            self.leased_resources.prepare_probe()

    def prepare_redeem(self):
        assert self.resources is not None
        assert self.resources.sliver is not None
        sliver = self.resources.sliver

        self.logger.info(f"Redeem prepared for Sliver: {sliver}")
        if isinstance(sliver, NetworkServiceSliver) and sliver.interface_info is not None:
            for ifs in sliver.interface_info.interfaces.values():
                self.logger.info(f"Interface Sliver: {ifs}")

        if isinstance(sliver, NodeSliver) and sliver.attached_components_info is not None:
            for c in sliver.attached_components_info.devices.values():
                self.logger.info(f"Component: {c}")

    def probe_join_state(self):
        """
        Called from a probe to monitor asynchronous processing related to the joinstate for controller.
        @raises Exception passed through from prepareJoin or prepareRedeem
        """
        if self.state == ReservationStates.Closed or self.state == ReservationStates.Failed:
            self.transition_with_join(prefix="clearing join state for terminal reservation", state=self.state,
                                      pending=self.pending_state,
                                      join_state=JoinState.NoJoin)
            return

        if self.joinstate == JoinState.BlockedTicket:
            # this is new reservation, and the ticket is
            # blocked for a predecessor: see if we can get it going now.
            assert self.state == ReservationStates.Nascent

            if self.approve_ticket():
                self.transition_with_join(prefix="unblock ticket", state=ReservationStates.Nascent,
                                          pending=ReservationPendingStates.Ticketing, join_state=JoinState.NoJoin)

                # This is a regular request for network resources to an upstream broker.
                self.sequence_ticket_out += 1
                RPCManagerSingleton.get().ticket(reservation=self)

                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.resources.sliver)

        elif self.joinstate == JoinState.BlockedExtendTicket:
            # this is existing reservation, and the extend ticket is
            # blocked for a predecessor: see if we can get it going now.
            assert self.state == ReservationStates.Active

            if self.approve_ticket(extend=True):
                self.transition_with_join(prefix="unblock ticket", state=self.state,
                                          pending=self.pending_state, join_state=JoinState.NoJoin)

                # This is a regular request for modifying network resources to an upstream broker.
                self.sequence_ticket_out += 1
                print(f"Issuing an extend ticket {sliver_to_str(sliver=self.get_requested_resources().get_sliver())}")
                RPCManagerSingleton.get().extend_ticket(reservation=self)

                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.resources.sliver)

        elif self.joinstate == JoinState.BlockedRedeem:
            # this reservation has a ticket to redeem, and the redeem is
            # blocked for a predecessor: see if we can get it going now.
            assert self.state == ReservationStates.Ticketed

            if self.approve_redeem():
                self.transition_with_join(prefix="unblock redeem", state=ReservationStates.Ticketed,
                                          pending=ReservationPendingStates.Redeeming, join_state=JoinState.NoJoin)
                self.sequence_lease_out += 1
                # If redeem fails we should not fail the reservation!!! The
                # failure may be due to the authority being unavailable
                RPCManagerSingleton.get().redeem(reservation=self)

                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.resources.sliver)

        elif self.joinstate == JoinState.BlockedExtendLease:
            # this reservation has a ticket to extend, and the extend is
            # blocked for a predecessor: see if we can get it going now.
            assert self.state == ReservationStates.ActiveTicketed

            if self.approve_redeem():
                self.transition_with_join(prefix="unblock extend lease", state=self.state,
                                          pending=self.pending_state, join_state=JoinState.NoJoin)
                self.extend_lease()

                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.resources.sliver)

        elif self.joinstate == JoinState.BlockedJoin:
            # This reservation has a lease whose join processing was blocked
            # for a predecessor: see if we can get it going now. Note: if
            # pendingRecover is true the reservation cannot be unblocked, since
            # it may not actually have its leased resources. If state is
            # ActiveTicketed we will also not unblock, because we may be
            # recovering a reservation in Active, ExtendingTicket, BlockedJoin.
            # For reservations in this state the pendingRecover flag will be
            # cleared by the updateTicket message. Since the reservation does
            # not actually complete recovery until the lease comes back from
            # the site, unblocking the reservation will result in an error.
            if not self.pending_recover and self.state != ReservationStates.ActiveTicketed and self.approve_join():
                self.transition_with_join(prefix="unblocked join", state=self.state, pending=self.pending_state,
                                          join_state=JoinState.Joining)
                self.service_pending = JoinState.Joining

                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.resources.sliver)

        elif self.joinstate == JoinState.Joining and self.service_pending == JoinState.None_ and \
                self.leased_resources.is_active() and not self.pending_recover and\
                self.state != ReservationStates.ActiveTicketed:
            # Tracking initial join processing for first lease on a service
            # manager. The reservation is already "active", but we log
            # completion of the join here and Fail if it failed completely.
            # Recovery note: if pendingRecover is true it is dangerous to allow
            # a transition to NoJoin, since the local lease may be different
            # from the lease at the site. If we fail during recovery, it is
            # possible to end up in Active, None, NoJoin and we will not be
            # able to tell if the lease is good or not. For the same reason,
            # when we are in ActiveTicketed we should not allow a transition to
            # NoJoin: pendingRecover may have already been cleared by an
            # updateTicket message but we still must get the lease from the
            # site.
            # join completed: go ahead and transition to NoJoin
            self.transition_with_join(prefix="join complete", state=self.state, pending=self.pending_state,
                                      join_state=JoinState.NoJoin)
            if self.leased_resources.get_concrete_units() == 0:
                if self.leased_resources.get_notices().get_notice() is not None:
                    self.fail(message=f"resources failed to join: {self.leased_resources.get_notices().get_notice()}")
                else:
                    self.fail(message="resources failed to join: (no details)")
            else:
                # Update ASM with Reservation Info
                self.update_slice_graph(sliver=self.leased_resources.sliver)

    def probe_pending(self):
        # Process join state to complete or restart join-related operations for Controller
        if self.joinstate != JoinState.NoJoin:
            self.probe_join_state()
        else:
            # Handle pending response for a Redeem or Ticket from AM or broker respectively
            # This happens usually in case of Kafka Message Timeout
            # To avoid the slice from being stuck in Configuring state
            # Close the reservation - send Close to AM and relinquish to Broker
            # Timeout is configurable
            if self.pending_state == ReservationPendingStates.Redeeming or \
                    self.pending_state == ReservationPendingStates.Ticketing:
                from fabric_cf.actor.core.container.globals import GlobalsSingleton
                if self.exceeds_timeout(timeout=GlobalsSingleton.get().RPC_TIMEOUT):
                    self.logger.info(f"Res# {self.get_reservation_id()} Redeem/Ticket timeout! "
                                     f"No response received in 900 seconds!")

                    self.transition(prefix="Redeem/Ticket timeout", state=ReservationStates.CloseWait,
                                    pending=ReservationPendingStates.None_)

                    # Send close to Authority;
                    try:
                        self.sequence_lease_out += 1
                        RPCManagerSingleton.get().close(reservation=self)
                    except Exception as e:
                        self.logger.error("authority reports close error: {}".format(e))
                        self.logger.error(traceback.format_exc())

                    # Send close to Broker;
                    self.transition(prefix=self.CLOSE_COMPLETE, state=ReservationStates.Closed,
                                    pending=ReservationPendingStates.None_)
                    self.do_relinquish()

                    # Update ASM with Reservation Info
                    self.update_slice_graph(sliver=self.resources.sliver)
                    update_data = UpdateData()
                    update_data.failed = True
                    update_data.message = "Redeem/Ticket timeout"
                    self.mark_close_by_ticket_review(update_data=update_data)

        if self.leased_resources is None:
            return

        # Handling for close completion. Note that this reservation could
        # "stick" once we enter the CloseWait state, if we never hear back from
        # the authority. There is no harm to purging a CloseWait reservation,
        # but we just leave them for now.
        if self.pending_state == ReservationPendingStates.Closing and self.leased_resources.is_closed():
            self.logger.debug("LEASED RESOURCES are closed")

            self.transition(prefix="local close complete", state=ReservationStates.CloseWait,
                            pending=ReservationPendingStates.None_)

            try:
                self.sequence_lease_out += 1
                RPCManagerSingleton.get().close(reservation=self)
            except Exception as e:
                self.logger.error("authority reports close error: {}".format(e))
                self.logger.error(traceback.format_exc())
                # If the authority is unreachable or rejects the request,
                # then purge it. This is useful because the authority may
                # close first and reject this request, which could lead to
                # large numbers of stuck CloseWaits hanging around if we
                # don't complete close here. But if the authority is merely
                # unreachable, it might be better to retry.
                self.transition(prefix=self.CLOSE_COMPLETE, state=ReservationStates.Closed,
                                pending=ReservationPendingStates.None_)
                # Note: the broker does not have information to ensure we
                # are not cheating
                self.do_relinquish()

    def set_policy(self, *, policy: ABCClientPolicy):
        self.policy = policy

    def reserve(self, *, policy: ABCPolicy):
        assert self.slice is not None

        self.nothing_pending()
        self.policy = policy

        if self.state == ReservationStates.Nascent:
            # We are a broker or orchestrator initiating a new ticket
            # request to an upstream agent.
            assert self.broker is not None

            self.requested_term = self.approved_term
            self.requested_resources = self.approved_resources

            self.transition(prefix="ticket", state=ReservationStates.Nascent,
                            pending=ReservationPendingStates.Ticketing)

            if not self.exported:
                if not self.can_ticket():
                    self.transition_with_join(prefix="ticket blocked", state=ReservationStates.Nascent,
                                              pending=self.pending_state, join_state=JoinState.BlockedTicket)
                else:
                    # This is a regular request for new resources to an upstream broker.
                    self.sequence_ticket_out += 1
                    RPCManagerSingleton.get().ticket(reservation=self)

        elif self.state == ReservationStates.Ticketed:
            self.transition_with_join(prefix="redeem blocked", state=ReservationStates.Ticketed,
                                      pending=self.pending_state, join_state=JoinState.BlockedRedeem)

        elif self.state == ReservationStates.Active:
            self.sequence_lease_out += 1
            RPCManagerSingleton.get().redeem(reservation=self)

        elif self.state == ReservationStates.ActiveTicketed:
            self.transition_with_join(prefix="extend lease blocked", state=self.state,
                                      pending=self.pending_state, join_state=JoinState.BlockedExtendLease)

        elif self.state == ReservationStates.Closed or self.state == ReservationStates.CloseWait or \
                self.state == ReservationStates.Failed:
            self.error(err="initiating reserve on defunct reservation")

    def setup(self):
        super().setup()
        if self.leased_resources is not None:
            self.leased_resources.setup(reservation=self)

        if self.suggested_resources is not None:
            self.suggested_resources.setup(reservation=self)

    def service_close(self):
        if self.leased_resources is not None:
            self.logger.debug("Closing leased resources: {}".format(type(self.leased_resources)))
            self.leased_resources.close()
            self.update_slice_graph(sliver=self.leased_resources.sliver)
        elif self.resources is not None:
            self.update_slice_graph(sliver=self.resources.sliver)
        else:
            self.update_slice_graph(sliver=self.requested_resources.sliver)

    def service_probe(self):
        # An exception in one of these service routines should mean some
        # unrecoverable, reservation-wide failure. It should not occur, e.g.,
        # if some subset of the resources fail.
        try:
            if self.service_pending == JoinState.Joining:
                # The reservation state may have changed by the time we reach
                # here (e.g., Closing/Closed). However, even if we check here,
                # there is no guarantee that the update initiated by
                # leasedResources.serviceUpdate will be applied before a
                # potential close request (We are not holding locks here, and
                # we should not). So the concrete resource implementation must
                # ensure that it will not honor the update if the reservation
                # state has changed.
                assert self.leased_resources is not None
                self.leased_resources.service_update(reservation=self)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Controller failed service update e:{}".format(e))

        self.service_pending = JoinState.None_

    def service_update_lease(self):
        if self.leased_resources is not None and self.last_lease_update.successful():
            if self.joinstate != JoinState.BlockedJoin:
                # An update() was called above, so we must clear it. Update()
                # must be called in every success case, and must not be called
                # in any failure case. But: if the reservation is in
                # BlockedJoin, then leave the update unserviced until a future
                # probePending.
                self.leased_resources.service_update(reservation=self)
                self.set_dirty()

                # If subsequent lease updates come in (e.g., for an extend)
                # before we have cleared the initial one, then
                # rset.serviceUpdate should now do the right thing.

                self.update_slice_graph(sliver=self.leased_resources.sliver)

    def service_update_ticket(self):
        if self.last_ticket_update.successful():
            self.resources.service_update(reservation=self)
        if self.resources is not None and self.resources.sliver is not None:
            self.update_slice_graph(sliver=self.resources.sliver)

    def set_broker(self, *, broker: ABCBrokerProxy) -> bool:
        if self.state != ReservationStates.Nascent:
            self.error(err="setBroker on reservation while in use")
            return False

        self.broker = broker
        return True

    def set_exported(self, *, exported: bool):
        self.exported = exported

    def set_lease_sequence_in(self, *, sequence: int):
        self.sequence_lease_in = sequence

    def set_lease_sequence_out(self, *, sequence: int):
        self.sequence_lease_out = sequence

    def set_renewable(self, *, renewable: bool):
        self.renewable = renewable

    def get_renewable(self) -> bool:
        return self.renewable

    def set_renew_time(self, *, time: int):
        self.renew_time = time

    def set_suggested(self, *, term: Term, resources: ResourceSet):
        self.suggested_resources = resources
        self.suggested_term = term
        self.suggested = True

    def set_suggested_resources(self, *, resources: ResourceSet):
        self.suggested_resources = resources

    def set_suggested_term(self, *, term: Term):
        self.suggested_term = term

    def set_ticket_sequence_in(self, *, sequence: int):
        self.sequence_ticket_in = sequence

    def set_ticket_sequence_out(self, *, sequence: int):
        self.sequence_lease_out = sequence

    def ticket_update_satisfies(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        """
        Enforce minimum standards for an arriving ticket update.
        @param incoming  incoming ticket update
        @param update_data update data
        @throws Exception thrown if resources do not satisfy request
        @throws Exception thrown if term does not satisfy request
        """
        try:
            # Call the policy to determine if we can apply the incoming update.
            self.policy.ticket_satisfies(requested_resources=self.requested_resources,
                                         actual_resources=incoming.get_resources(),
                                         requested_term=self.requested_term,
                                         actual_term=incoming.get_term())

            # If the policy was careless about the term, make sure that the
            # incoming term extends the current one.
            if self.pending_state == ReservationPendingStates.ExtendingTicket:
                incoming.get_term().enforce_extends_term(old_term=self.term)
        except Exception as e:
            self.error(err="incoming ticket does not satisfy our request: {}".format(e))

    def transition_with_join(self, *, prefix: str, state: ReservationStates, pending: ReservationPendingStates,
                             join_state: JoinState):
        """
        State transition along with join state
        @param prefix prefix string
        @param state state to be transitioned to
        @param pending pending state
        @param join_state join state
        """
        self.logger.debug("Reservation # {} {} transition for joinstate: {} -> {}".format(self.rid, prefix,
                                                                                          self.joinstate.name,
                                                                                          join_state.name))
        self.joinstate = join_state

        self.transition(prefix=prefix, state=state, pending=pending)

    def update_lease(self, *, incoming: ABCReservationMixin, update_data):
        if self.state == ReservationStates.Nascent:
            self.error(err="Lease update for a reservation without a ticket")

        elif self.state == ReservationStates.Ticketed:
            if self.pending_state != ReservationPendingStates.Redeeming:
                self.logger.warning("unsolicited lease update: Ticketed/None. Details: {}".format(incoming))
                return

            if self.accept_lease_update(incoming=incoming, update_data=update_data):
                self.pending_recover = False

                self.transition_with_join(prefix="lease arrival blocked join", state=ReservationStates.Active,
                                          pending=ReservationPendingStates.None_,
                                          join_state=JoinState.BlockedJoin)

            if self.closed_during_redeem:
                self.logger.info("Received updateLease for a reservation closed in the Redeeming state. Issuing close.")
                self.close()

        elif self.state == ReservationStates.Active:
            if self.accept_lease_update(incoming=incoming, update_data=update_data) and \
                    self.pending_state == ReservationPendingStates.ModifyingLease:
                if self.joinstate == JoinState.Joining:
                    self.logger.warning("Received LeaseUpdate while in Joining")

                self.transition(prefix="modified lease", state=ReservationStates.Active,
                                pending=ReservationPendingStates.None_)

        elif self.state == ReservationStates.ActiveTicketed:
            if self.accept_lease_update(incoming=incoming, update_data=update_data):
                # Tricky transition: take this lease as an extension if we
                # already issued the lease extend request, else accept it as an
                # unsolicited and stay in ActiveTicketed.
                if self.pending_state == ReservationPendingStates.ExtendingLease:
                    if self.joinstate == JoinState.Joining:
                        self.logger.warning("Received LeaseUpdate while in Joining")

                    self.transition_with_join(prefix="extended lease",
                                              state=ReservationStates.Active,
                                              pending=ReservationPendingStates.None_,
                                              join_state=JoinState.Joining)
                self.pending_recover = False

            if self.closed_during_redeem:
                self.logger.info("Received updateLease for a reservation closed in the Redeeming state. Issuing close.")
                self.close()

        elif self.state == ReservationStates.CloseWait:
            close_wait_temp = self.accept_lease_update(incoming=incoming, update_data=update_data)

            if not close_wait_temp:
                self.logger.warning("incoming lease update is not FIN or indicates a remote error. "
                                    "Transitioning to close nevertheless.")

            self.pending_recover = False

            self.transition(prefix=self.CLOSE_COMPLETE, state=ReservationStates.Closed,
                            pending=ReservationPendingStates.None_)

            self.do_relinquish()

        elif self.state == ReservationStates.Closed:
            self.logger.error("Lease update on closed reservation")

        elif self.state == ReservationStates.Failed:
            self.logger.error("Lease update on failed reservation")

    def update_ticket(self, *, incoming: ABCReservationMixin, update_data: UpdateData):
        if self.state == ReservationStates.Nascent or self.state == ReservationStates.Ticketed:
            if self.pending_state != ReservationPendingStates.Ticketing and \
                    self.pending_state != ReservationPendingStates.ExtendingTicket:
                self.logger.warning("unsolicited ticket update. Ignoring it. Details: {}".format(incoming))
                return

            if self.accept_ticket_update(incoming=incoming, update_data=update_data):
                self.transition(prefix="ticket update", state=ReservationStates.Ticketed,
                                pending=ReservationPendingStates.None_)
                self.suggested = False
                self.approved = False
                self.pending_recover = False

        elif self.state == ReservationStates.Active or self.state == ReservationStates.ActiveTicketed:
            if self.pending_state != ReservationPendingStates.ExtendingTicket:
                self.logger.warning("unsolicited ticket update. Ignoring it. Details: {}".format(incoming))
                return

            if self.accept_ticket_update(incoming=incoming, update_data=update_data):
                self.extended = True
                self.transition(prefix="ticket update", state=ReservationStates.ActiveTicketed,
                                pending=ReservationPendingStates.None_)
                self.suggested = False
                self.approved = False
                self.pending_recover = False

        elif self.state == ReservationStates.Closed or self.state == ReservationStates.CloseWait:
            self.logger.warning("Ticket update after close")

        elif self.state == ReservationStates.Failed:
            self.logger.error("Ticket update on failed reservation")

    def validate_incoming(self):
        if self.slice is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("slice"))

        if self.resources is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("resource set"))

        if self.term is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("term"))

        self.term.validate()

        self.resources.validate_incoming()

    def validate_incoming_lease(self):
        """
        Validate incoming lease
        """
        self.validate_incoming()

    def validate_incoming_ticket(self):
        """
        Validate incoming ticket
        """
        self.validate_incoming()
        self.resources.validate_incoming_ticket(term=self.term)

    def validate_outgoing(self):
        if self.slice is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("slice"))

        if self.resources is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("resource set"))

        if self.term is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("term"))

        self.approved_term.validate()

        self.approved_resources.validate_outgoing()

    def validate_redeem(self):
        if self.authority is None:
            self.internal_error(err="no authority proxy for redeem")

        if self.resources.get_units() == 0:
            self.internal_error(err="redeeming a reservation for 0 resources!")

        if self.slice is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("slice"))

        if self.resources is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("resource set"))

        if self.term is None:
            self.error(err=Constants.NOT_SPECIFIED_PREFIX.format("term"))

    def add_redeem_predecessor(self, *, reservation: ABCReservationMixin, filters: dict = None):
        if reservation.get_reservation_id() not in self.redeem_predecessors:
            state = PredecessorState(reservation=reservation)
            self.redeem_predecessors[reservation.get_reservation_id()] = state

    def add_join_predecessor(self, *, predecessor):
        if predecessor.get_reservation_id() not in self.redeem_predecessors:
            state = PredecessorState(reservation=predecessor)
            self.join_predecessors[predecessor.get_reservation_id()] = state

    def get_redeem_predecessors(self) -> List[PredecessorState]:
        result = []
        for v in self.redeem_predecessors.values():
            result.append(v)
        return result

    def get_join_predecessors(self) -> List[PredecessorState]:
        result = []
        for v in self.join_predecessors.values():
            result.append(v)
        return result

    def get_client_callback_proxy(self) -> ABCClientCallbackProxy:
        return self.callback

    def handle_failed_rpc(self, *, failed: FailedRPC):
        if failed.get_error_type() == RPCError.NetworkError:
            if self.is_failed() or self.is_closed():
                return

            if self.is_closing():
                if self.leased_resources is None or self.leased_resources.is_closed():
                    self.transition(prefix=self.CLOSE_COMPLETE, state=ReservationStates.Closed,
                                    pending=ReservationPendingStates.None_)
                    self.do_relinquish()
                return

        self.fail(message=f"Failing reservation due to non-recoverable RPC error {failed.get_error_type()}",
                  exception=failed.get_error())

    def print_state(self):
        """
        Print reservation state
        """
        result = "[{}, {}, {}]({}/{})({}/{})".format(self.get_state_name(),
                                                     self.get_pending_state_name(), self.get_join_state_name(),
                                                     self.get_ticket_sequence_out(), self.get_ticket_sequence_in(),
                                                     self.get_lease_sequence_out(), self.get_lease_sequence_in())
        return result

    def recover_nascent(self):
        """
        Recover the reservation post stateful restart in nascent state
        """
        if self.pending_state == ReservationPendingStates.None_:
            self.actor.ticket(reservation=self)
            self.logger.debug("Issued ticket request for reservation #{} State={}".format(
                self.get_reservation_id(), self.print_state()))

        elif self.pending_state == ReservationPendingStates.Ticketing:
            self.set_pending_recover(pending_recover=True)
            self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
            self.set_ticket_sequence_out(sequence=self.get_ticket_sequence_out() - 1)
            self.actor.ticket(reservation=self)
            self.logger.debug(Constants.ISSUE_OPERATION.format("ticket", self.get_reservation_id(), self.print_state()))

    def recover_ticketed(self):
        """
        Recover the reservation post stateful restart in ticketed state
        """
        if self.pending_state == ReservationPendingStates.None_:
            self.logger.debug("No recovery necessary for reservation #{} State={}".format(
                self.get_reservation_id(), self.print_state()))

        elif self.pending_state == ReservationPendingStates.Redeeming:
            self.set_pending_recover(pending_recover=True)
            self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
            self.set_lease_sequence_out(sequence=self.get_lease_sequence_out() - 1)
            self.actor.redeem(reservation=self)
            self.logger.debug(Constants.ISSUE_OPERATION.format("redeem", self.get_reservation_id(),
                                                               self.print_state()))

        elif self.pending_state == ReservationPendingStates.ExtendingTicket:
            self.set_pending_recover(pending_recover=True)
            self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
            self.set_ticket_sequence_out(sequence=self.get_ticket_sequence_in() - 1)
            self.actor.extend_ticket(reservation=self)
            self.logger.debug(Constants.ISSUE_OPERATION.format(Constants.EXTEND_TICKET,
                                                               self.get_reservation_id(), self.print_state()))

        else:
            raise ReservationException(Constants.INVALID_PENDING_STATE)

    def recover_active_none(self):
        """
        Recover the reservation post stateful restart in active state
        """
        # If we were in Joining, restart the handlers actions and re-request the
        # lease. If we are in BlockedJoin, set joinstate to NoJoin and
        # re-request the lease.
        if self.joinstate == JoinState.NoJoin:
            self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))

        elif self.joinstate == JoinState.Joining:
            self.logger.debug(Constants.RESTARTING_ACTIONS.format(self.get_reservation_id()))
            self.actor.get_plugin().restart_configuration_actions(reservation=self)
            self.logger.debug(Constants.RESTARTING_ACTIONS_COMPLETE.format(
                self.get_reservation_id()))
            self.set_pending_recover(pending_recover=True)
            self.set_lease_sequence_out(sequence=self.get_lease_sequence_out() - 1)
            self.actor.redeem(reservation=self)
            self.logger.debug(Constants.ISSUE_OPERATION.format("redeem", self.get_reservation_id(), self.print_state()))

        elif self.joinstate == JoinState.BlockedJoin:
            # Do not clear the join state. If we fail here before issuing the
            # redeem request and the reservation gets committed to the
            # database, we will end in [Active, None, NoJoin], and then when we
            # try to recover we will assume (incorrectly) that the reservation
            # requires no recovery operations.
            self.set_pending_recover(pending_recover=True)
            self.set_lease_sequence_out(sequence=self.get_lease_sequence_out() - 1)
            self.actor.redeem(reservation=self)
            self.logger.debug(Constants.ISSUE_OPERATION.format("redeem", self.get_reservation_id(), self.print_state()))

        else:
            raise ReservationException("Invalid join state")

    def recover_active_redeeming(self):
        """
        Recover the reservation post stateful restart in active redeeming state
        """
        self.logger.debug("It seems that we have failed earlier while recovering reservation #{}".format(
            self.get_reservation_id()))

        if self.joinstate == JoinState.Joining:
            self.logger.debug(Constants.RESTARTING_ACTIONS.format(self.get_reservation_id()))
            self.actor.get_plugin().restart_configuration_actions(reservation=self)
            self.logger.debug(Constants.RESTARTING_ACTIONS_COMPLETE.format(self.get_reservation_id()))

        self.set_pending_recover(pending_recover=True)
        self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
        self.set_lease_sequence_out(sequence=self.get_lease_sequence_out() - 1)
        self.actor.redeem(reservation=self)
        self.logger.debug(Constants.ISSUE_OPERATION.format("redeem", self.get_reservation_id(), self.print_state()))

    def recover_active_extending_ticket(self):
        """
        Recover the reservation post stateful restart in active extending ticket state
        """
        if self.joinstate == JoinState.Joining:
            self.logger.debug(Constants.RESTARTING_ACTIONS.format(self.get_reservation_id()))
            self.actor.get_plugin().restart_configuration_actions(reservation=self)
            self.logger.debug(Constants.RESTARTING_ACTIONS_COMPLETE.format(self.get_reservation_id()))

        self.set_pending_recover(pending_recover=True)
        self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
        self.set_ticket_sequence_out(sequence=self.get_ticket_sequence_out() - 1)
        self.actor.extend_ticket(reservation=self)
        self.logger.debug(Constants.ISSUE_OPERATION.format(Constants.EXTEND_TICKET, self.get_reservation_id(),
                                                           self.print_state()))

    def recover_active_ticketed_extending_lease(self):
        """
        Recover the reservation post stateful restart in active ticketed extending lease state
        """
        self.set_pending_recover(pending_recover=True)
        self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
        self.set_lease_sequence_out(sequence=self.get_lease_sequence_out() - 1)
        self.actor.extend_lease(reservation=self)
        self.logger.debug(Constants.ISSUE_OPERATION.format("extend lease", self.get_reservation_id(),
                                                           self.print_state()))

    def recover_closing(self):
        """
        Recover the reservation post stateful restart in closing state
        """
        self.transition(prefix=Constants.RECOVERY, state=self.state, pending=ReservationPendingStates.None_)
        self.actor.close(reservation=self)
        self.logger.debug(Constants.ISSUE_OPERATION.format("close", self.get_reservation_id(), self.print_state()))

    def recover_active(self):
        """
        Recover the reservation post stateful restart in active state
        """
        if self.pending_state == ReservationPendingStates.None_:
            self.recover_active_none()

        elif self.pending_state == ReservationPendingStates.Redeeming:
            self.recover_active_redeeming()

        elif self.pending_state == ReservationPendingStates.ExtendingTicket:
            self.recover_active_extending_ticket()

        elif self.pending_state == ReservationPendingStates.Closing:
            self.recover_closing()

        else:
            raise ReservationException(Constants.INVALID_PENDING_STATE)

    def recover_active_ticketed(self):
        """
        Recover the reservation post stateful restart in active ticketed state
        """
        if self.pending_state == ReservationPendingStates.None_:
            self.recover_active_none()

        elif self.pending_state == ReservationPendingStates.Redeeming:
            self.recover_active_redeeming()

        elif self.pending_state == ReservationPendingStates.ExtendingLease:
            self.recover_active_ticketed_extending_lease()

        elif self.pending_state == ReservationPendingStates.Closing:
            self.recover_closing()

        else:
            raise ReservationException(Constants.INVALID_PENDING_STATE)

    def recover(self):
        """
        Recover the reservation post stateful restart
        """
        if isinstance(self.policy, ABCAuthorityPolicy):
            self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))
            return

        if self.state == ReservationStates.Nascent:
            self.recover_nascent()
        elif self.state == ReservationStates.Ticketed:
            self.recover_ticketed()
        elif self.state == ReservationStates.Active:
            self.recover_active()
        elif self.state == ReservationStates.ActiveTicketed:
            self.recover_active_ticketed()
        elif self.state == ReservationStates.CloseWait:
            self.recover_closing()
        elif self.state == ReservationStates.Failed:
            self.logger.warning("Reservation #{} has failed".format(self.get_reservation_id()))

    def fail(self, *, message: str, exception: Exception = None, sliver: BaseSliver = None):
        super().fail(message=message, exception=exception)
        if sliver is None and self.requested_resources is not None and self.requested_resources.sliver is not None:
            sliver = self.requested_resources.sliver

        if sliver is not None:
            self.update_slice_graph(sliver=self.requested_resources.sliver)

    def update_slice_graph(self, *, sliver: BaseSliver):
        """
        Update ASM with Sliver information
        :param sliver: sliver
        :return:
        """
        begin = time.time()
        try:
            self.logger.debug(f"Updating ASM for  Reservation# {self.rid} State# {self.get_reservation_state()} "
                              f"Slice Graph# {self.slice.get_graph_id()}")
            error_message = self.get_error_message()
            if error_message is None:
                error_message = self.get_last_ticket_update()
            if error_message is None:
                error_message = self.get_last_lease_update()
            '''
            self.slice.update_slice_graph(sliver=sliver, rid=str(self.rid),
                                          reservation_state=self.state.name,
                                          error_message=error_message)
            '''
            asm_thread = self.actor.get_asm_thread()
            if asm_thread is not None:
                asm_thread.enqueue(graph_id=self.slice.get_graph_id(),
                                   sliver=sliver, rid=str(self.rid),
                                   reservation_state=self.state.name,
                                   error_message=error_message)
            self.logger.debug(f"Update ASM completed for  Reservation# {self.rid} State# {self.get_reservation_state()} "
                              f"Slice Graph# {self.slice.get_graph_id()}")

        except Exception as e:
            self.logger.error(f"Failed to update the ASM Graph: {e}")
            self.logger.error(traceback.format_exc())
        self.logger.info(f"ASM TIME: {time.time() - begin:.0f}")

    def mark_close_by_ticket_review(self, *, update_data: UpdateData):
        if self.last_ticket_update is not None:
            self.last_ticket_update.absorb(other=update_data)


class ClientReservationFactory:
    """
    Factory class for creating client reservations
    """
    @staticmethod
    def create(*, rid: ID, resources: ResourceSet = None, term: Term = None, slice_object: ABCSlice = None,
               broker: ABCBrokerProxy = None, actor: ABCActorMixin = None):
        """
        Create Client reservation
        :param rid:
        :param resources:
        :param term:
        :param slice_object:
        :param broker:
        :param actor:
        :return:
        """
        result = ReservationClient(rid=rid, resources=resources, term=term, slice_object=slice_object, broker=broker)
        if actor is not None:
            result.restore(actor=actor, slice_obj=slice_object)
        return result
