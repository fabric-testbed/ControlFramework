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

import traceback
from datetime import datetime
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.apis.abc_authority_policy import ABCAuthorityPolicy
from fabric_cf.actor.core.apis.abc_broker_policy_mixin import ABCBrokerPolicyMixin
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory, ABCReservationMixin
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.kernel.request_types import RequestTypes
from fabric_cf.actor.core.kernel.reservation_server import ReservationServer
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
    from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term


class BrokerReservation(ReservationServer, ABCBrokerReservation):
    """
    A note on exported "will call" reservations. An export() operation may be
    locally initiated on an agent. It binds and forms a ticket in the same way as
    if the request came from a client, but there is no client rid (remoteRid) and
    no callback object. The prepare method in AgentReservation and
    register/unregister in ReservationServer handle these cases: the export
    proceeds as a normal reserve request, but it leaves the callback and
    remoteRid null, does not register the reservation with its slice (since there
    is no remoteRid), and does not issue an updateTicket (since there is no
    callback).
    """
    updated_absorbed = "update absorbed"

    def __init__(self, *, rid: ID, resources: ResourceSet, term: Term, slice_obj: ABCSlice):
        super().__init__(rid=rid, resources=resources, term=term, slice_object=slice_obj)
        # Delegation backing the ticket granted to this reservation. For now only
        # one source delegation can be used to issue a ticket to satisfy a client
        # request.
        self.source = None
        # If this flag is true, then the reservation represents a request to export
        # resources to a client.
        self.exporting = False
        # The authority in control of the resources.
        self.authority = None
        # True if an updateTicket() must be sent on the next service probe.
        self.must_send_update = None
        # True if we notified the client about the fact that the reservation had failed
        self.notified_failed = False
        # True if the reservation was closed in the priming state.
        self.closed_in_priming = False
        self.category = ReservationCategory.Broker

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

        del state['policy']

        del state['source']
        del state['notified_failed']
        del state['closed_in_priming']
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

        self.policy = None

        self.source = None
        self.notified_failed = False
        self.closed_in_priming = False

    def restore(self, *, actor: ABCActorMixin, slice_obj: ABCSlice):
        """
        Must be invoked after creating reservation from unpickling
        """
        super().restore(actor=actor, slice_obj=slice_obj)
        self.source = None
        if actor is not None and self.resources is not None and self.resources.get_resources() is not None:
            delegation_id = self.resources.get_resources().get_delegation_id()
            delegation = self.actor.get_delegation(did=delegation_id)
            self.source = delegation
        self.notified_failed = False
        self.closed_in_priming = False

    def print_state(self):
        """
        Converts the reservation to a state string.
        @return state string representing the reservation
        """
        return "[{},{}] ({})({})".format(self.get_state_name(), self.get_pending_state_name(), self.get_sequence_in(),
                                         self.get_sequence_out())

    def recover(self):
        """
        Recover the reservation post stateful restart
        """
        if isinstance(self.policy, ABCAuthorityPolicy):
            self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))
            return

        if not isinstance(self.policy, ABCBrokerPolicyMixin):
            raise BrokerException(msg=f"Do not know how to recover: policy={self.policy}")

        if self.state == ReservationStates.Nascent:
            if self.pending_state == ReservationPendingStates.None_:
                self.actor.ticket(self)
                self.logger.info("Added reservation #{} to the ticketing list. State={}".format(
                    self.get_reservation_id(), self.print_state()))

            elif self.pending_state == ReservationPendingStates.Ticketing:
                self.set_pending_recover(pending_recover=True)
                self.transition(prefix="[recovery]", state=self.state, pending=ReservationPendingStates.None_)
                self.actor.ticket(self)
                self.logger.info(
                    "Added reservation #{} to the ticketing list. State={}".format(self.get_reservation_id(),
                                                                                   self.print_state()))

            else:
                raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE,
                                      msg=f"pending_state={self.pending_state}")

        elif self.state == ReservationStates.Ticketed:
            if self.pending_state == ReservationPendingStates.None_ or \
                    self.pending_state == ReservationPendingStates.Priming:
                self.set_service_pending(code=ReservationPendingStates.None_)
                self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))

            elif self.pending_state == ReservationPendingStates.ExtendingTicket:
                self.set_pending_recover(pending_recover=True)
                self.transition(prefix="[recovery]", state=self.state,
                                pending=ReservationPendingStates.None_)
                self.actor.extend_ticket(reservation=self)
                self.logger.info(
                    "Added reservation #{} to the extending list. State={}".format(self.get_reservation_id(),
                                                                                   self.print_state()))
            else:
                raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE,
                                      msg=f"pending_state={self.pending_state}")

        elif self.state == ReservationStates.Failed:
            self.logger.warning("Reservation #{} has failed".format(self.get_reservation_id()))
        else:
            raise BrokerException(error_code=ExceptionErrorCode.UNEXPECTED_STATE,
                                  msg=f"state={self.state}")

    def prepare(self, *, callback: ABCCallbackProxy, logger):
        self.set_logger(logger=logger)
        self.callback = callback

        # Null callback indicates a locally initiated request to create an
        # exported reservation. Else the request is from a client and must have
        # a client-specified RID.

        if self.callback is not None and self.rid is None:
            self.error(err="no reservation ID specified for request")

        self.set_dirty()

    def reserve(self, *, policy: ABCPolicy):
        # These handlers may need to be slightly more sophisticated, since a
        # client may bid multiple times on a ticket as part of an auction
        # protocol: so we may receive a reserve or extend when there is already
        # a request pending.
        self.incoming_request()

        if self.pending_state != ReservationPendingStates.None_ and \
                self.pending_state != ReservationPendingStates.Ticketing:
            # We do not want to fail the reservation simply log a warning and exit from reserve
            self.logger.warning("Duplicate ticket request")
            return

        self.policy = policy
        self.approved = False
        self.bid_pending = True
        self.map_and_update(ticketed=False)

    def service_reserve(self):
        # resources is null initially. It becomes non-null once the
        # policy completes its allocation.
        if self.resources is not None:
            self.resources.service_update(reservation=self)
            if not self.is_failed():
                self.transition(prefix=self.updated_absorbed, state=ReservationStates.Ticketed,
                                pending=ReservationPendingStates.None_)
                self.generate_update()

    def extend_ticket(self, *, actor: ABCActorMixin):
        self.incoming_request()

        # State must be ticketed. The reservation may be active, but the agent wouldn't know that
        if self.state != ReservationStates.Ticketed:
            self.error(err="extending unticketed reservation")

        if self.pending_state != ReservationPendingStates.None_ and self.pending_state != \
                ReservationPendingStates.ExtendingTicket:
            self.error(err="extending reservation with another pending request")

        if not self.requested_term.extends_term(old_term=self.term):
            self.error(err=f"new term {self.requested_term} does not extend current term {self.term}")

        self.approved = False
        self.bid_pending = True
        self.pending_recover = False
        self.map_and_update(ticketed=True)

    def service_extend_ticket(self):
        if self.pending_state == ReservationPendingStates.None_:
            self.resources.service_update(self)
            if not self.is_failed():
                self.transition(prefix=self.updated_absorbed, state=ReservationStates.Ticketed,
                                pending=ReservationPendingStates.None_)
                self.generate_update()

    def close(self):
        send_notification = False
        if self.state == ReservationStates.Nascent or self.pending_state != ReservationPendingStates.None_:
            self.logger.warning("Closing a reservation in progress")
            send_notification = True

        if self.state != ReservationStates.Closed:
            if self.pending_state == ReservationPendingStates.Priming or \
                    (self.pending_state == ReservationPendingStates.Ticketing and not self.bid_pending):
                # Close in Priming is a special case: when processing the close
                # event inside the policy we cannot rely on resources to
                # represent the resources allocated to the reservation. They
                # may either represent the previous resources or a mixture of
                # both. So here we will mark the reservation that it was closed
                # while it was in the Priming state. When processing the close
                # event the policy must free previousResources (if any) and
                # approvedResources. The policy should not free resources.
                self.logger.debug("closing reservation #{} while in Priming".format(self.rid))
                self.closed_in_priming = True

            self.transition(prefix="closed", state=ReservationStates.Closed, pending=ReservationPendingStates.None_)
            self.policy.closed(reservation=self)

        if send_notification:
            self.update_data.error(message="Closed while allocating ticket")
            self.generate_update()

    def probe_pending(self):
        if self.service_pending != ReservationPendingStates.None_:
            self.internal_error(err="service overrun in probePending")

        if self.is_failed() and not self.notified_failed:
            self.generate_update()
            self.notified_failed = True
        else:
            if self.pending_state == ReservationPendingStates.Ticketing:
                # Check for a pending ticket operation that may have completed
                if not self.bid_pending and self.map_and_update(ticketed=False):
                    self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.ExtendingTicket:
                # Check for a pending extendTicket operation
                if not self.bid_pending and self.map_and_update(ticketed=True):
                    self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.Redeeming:
                self.logger.error("AgentReservation in unexpected state")

            elif self.pending_state == ReservationPendingStates.Priming:
                self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.None_ and self.must_send_update:
                # for exported reservations that have been claimed, we need to
                # schedule a ticketUpdate
                self.service_pending = ReservationPendingStates.SendUpdate
                self.must_send_update = False

    def service_probe(self):
        try:
            if self.service_pending == ReservationPendingStates.AbsorbUpdate:
                self.resources.service_update(reservation=self)
                if not self.is_failed():
                    self.transition(prefix=self.updated_absorbed, state=ReservationStates.Ticketed,
                                    pending=ReservationPendingStates.None_)
                    self.generate_update()

            elif self.service_pending == ReservationPendingStates.SendUpdate:
                self.generate_update()

        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("failed while servicing probe e:{}".format(e))
            self.fail_notify(message=str(e))

        self.service_pending = ReservationPendingStates.None_

    def handle_duplicate_request(self, *, operation: RequestTypes):
        # The general idea is to do nothing if we are in the process of
        # performing a pending operation or about to reissue a
        # ticket/extendTicket after recovery. If there is nothing pending for
        # this reservation, we resend the last update.
        if operation == RequestTypes.RequestTicket:
            if self.pending_state == ReservationPendingStates.None_ and self.state != ReservationStates.Nascent and \
                    not self.pending_recover:
                self.generate_update()

        elif operation == RequestTypes.RequestExtendTicket:
            if self.pending_state == ReservationPendingStates.None_ and not self.pending_recover:
                self.generate_update()

        elif operation == RequestTypes.RequestRelinquish:
            self.logger.debug("no op")

        else:
            raise BrokerException(error_code=ExceptionErrorCode.NOT_SUPPORTED,
                                  msg=f"operation {RequestTypes(operation).name}")

    def generate_update(self):
        self.logger.debug("Generating update")
        if self.callback is None:
            self.logger.warning("Cannot generate update: no callback.")
            return

        self.logger.debug("Generating update: update count={}".format(self.update_count))
        try:
            self.update_count += 1
            self.sequence_out += 1
            RPCManagerSingleton.get().update_ticket(reservation=self)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            # Note that this may result in a "stuck" reservation... not much we
            # can do if the receiver has failed or rejects our update. We will
            # regenerate on any user-initiated probe.
            self.logger.error("callback failed e:{}".format(e))

    def map_and_update(self, *, ticketed: bool):
        """
        Call the policy to fill a request, with associated state transitions.
        Catch exceptions and report all errors using callback mechanism.

        @param ticketed
                   true iff this is ticketed (i.e., request is extend)
        @return boolean success
        """
        success = False
        granted = False

        if self.state == ReservationStates.Failed:
            # Must be a previous failure, or policy marked as failed. Send
            # update to reset client. Note: this might be the wrong thing if a
            # bidding protocol allows the caller to retry a denied request,
            # e.g., to bid higher after losing in an auction.
            self.generate_update()
        elif self.state == ReservationStates.Nascent:
            if ticketed:
                self.fail_notify(message="reservation is not yet ticketed")
            else:
                self.logger.debug("Using policy {} to bind reservation".format(self.policy.__class__.__name__))
                try:
                    granted = False
                    # If the policy has processed this reservation, granted should
                    # be set true so that we can send the result back to the
                    # client. If the policy has not yet processed this reservation
                    # (binPending is true) then call the policy. The policy may
                    # choose to process the request immediately (true) or to defer
                    # it (false). In case of a deferred request, we will eventually
                    # come back to this method after the policy has done its job.
                    if self.is_bid_pending():
                        if not self.is_exporting():
                            granted = self.policy.bind(reservation=self)
                        else:
                            self.internal_error(err="Exporting reservations not implemented")
                    else:
                        granted = True
                    self.transition(prefix="ticket request", state=ReservationStates.Nascent,
                                    pending=ReservationPendingStates.Ticketing)
                except Exception as e:
                    self.logger.error(f"mapAndUpdate bindTicket failed for ticketRequest: {e}")
                    self.fail_notify(message=str(e))
                    return success

                if granted:
                    self.logger.debug("Reservation {} has been granted".format(self.get_reservation_id()))
                    try:
                        success = True
                        self.term = self.approved_term
                        self.resources = self.approved_resources.abstract_clone()
                        self.resources.update(reservation=self, resource_set=self.approved_resources)
                        self.transition(prefix="ticketed", state=ReservationStates.Ticketed,
                                        pending=ReservationPendingStates.Priming)
                    except Exception as e:
                        self.logger.error("mapAndUpdate ticket failed for ticketRequest")
                        self.logger.error(e)
                        self.logger.error(traceback.format_exc())
                        self.fail_notify(message=str(e))
        elif self.state == ReservationStates.Ticketed:
            if not ticketed:
                self.fail_notify(message="reservation is already ticketed")
            else:
                try:
                    self.transition(prefix="extending ticket", state=ReservationStates.Ticketed,
                                    pending=ReservationPendingStates.ExtendingTicket)

                    # If the policy has processed this reservation, set granted to
                    # true so that we can send the ticket back to the client. If
                    # the policy has not yet processed this reservation (bid_pending
                    # is true) then call the policy. The plugin may choose to
                    # process the request immediately (true) or to defer it
                    # (false). In case of a deferred request, we will eventually
                    # come back to this method after the policy has done its job.

                    granted = False

                    if self.is_bid_pending():
                        granted = self.policy.extend_broker(reservation=self)
                    else:
                        granted = True
                except Exception as e:
                    self.logger.error(f"mapAndUpdate extendTicket failed for ExtendTicket: {e}")
                    self.fail_notify(message=str(e))
                    return success

                if granted:
                    try:
                        success = True
                        self.extended = True
                        self.transition(prefix="extended ticket", state=ReservationStates.Ticketed,
                                        pending=ReservationPendingStates.Priming)
                        self.previous_term = self.term
                        self.previous_resources = self.resources.clone()
                        self.term = self.approved_term
                        self.resources.update(reservation=self, resource_set=self.approved_resources)
                    except Exception as e:
                        self.logger.error(f"mapAndUpdate extend ticket failed for ExtendTicket: {e}")
                        self.fail_notify(message=str(e))
        else:
            self.logger.error("broker mapAndUpdate: unexpected state")
            self.fail_notify(message="invalid operation for the current reservation state")

        return success

    def get_authority(self) -> ABCAuthorityProxy:
        return self.authority

    def get_source(self) -> ABCDelegation:
        return self.source

    def get_units(self, *, when: datetime = None) -> int:
        hold = 0
        if not self.is_terminal():
            hold = self.resources.get_concrete_units(when=when)

        return hold

    def is_closed_in_priming(self) -> bool:
        return self.closed_in_priming

    def is_exporting(self) -> bool:
        return self.exporting

    def set_exporting(self):
        self.exporting = True

    def set_source(self, *, source: ABCDelegation):
        """
        Set source
        @param source source
        """
        self.source = source

    def set_authority(self, *, authority: ABCAuthorityProxy):
        self.authority = authority

    def update_lease(self, *, incoming: ABCReservationMixin, update_data):
        self.logger.info(f"Received Update Lease: {incoming} at Broker")
        # TODO add any processing if needed
        self.logger.info(f"Do Nothing!")

    def handle_failed_rpc(self, *, failed: FailedRPC):
        if failed.get_request_type() == RPCRequestType.UpdateTicket and \
                self.last_pending_state == ReservationPendingStates.ExtendingTicket:
            return
        super().handle_failed_rpc(failed=failed)


class BrokerReservationFactory:
    @staticmethod
    def create(*, rid: ID, resources: ResourceSet, term: Term, slice_obj: ABCSlice, actor: ABCActorMixin = None):
        """
        Create Broker Reservation
        :param rid:
        :param resources:
        :param term:
        :param slice_obj:
        :param actor:
        :return:
        """
        reservation = BrokerReservation(rid=rid, resources=resources, term=term, slice_obj=slice_obj)
        reservation.restore(actor=actor, slice_obj=slice_obj)
        return reservation
