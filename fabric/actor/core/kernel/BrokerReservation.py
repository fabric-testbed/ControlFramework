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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fabric.actor.core.apis.IActor import IActor
    from fabric.actor.core.apis.IAuthorityProxy import IAuthorityProxy
    from fabric.actor.core.apis.ICallbackProxy import ICallbackProxy
    from fabric.actor.core.apis.IClientReservation import IClientReservation
    from fabric.actor.core.apis.IPolicy import IPolicy
    from fabric.actor.core.apis.ISlice import ISlice
    from fabric.actor.core.kernel.FailedRPC import FailedRPC
    from fabric.actor.core.apis.IKernelSlice import IKernelSlice
    from fabric.actor.core.kernel.ResourceSet import ResourceSet
    from fabric.actor.core.time.Term import Term
    from fabric.actor.core.util.ID import ID

from datetime import datetime
from fabric.actor.core.apis.IAuthorityPolicy import IAuthorityPolicy
from fabric.actor.core.apis.IBrokerPolicy import IBrokerPolicy
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.IKernelBrokerReservation import IKernelBrokerReservation
from fabric.actor.core.kernel.RPCManagerSingleton import RPCManagerSingleton
from fabric.actor.core.kernel.RPCRequestType import RPCRequestType
from fabric.actor.core.kernel.RequestTypes import RequestTypes
from fabric.actor.core.kernel.ReservationServer import ReservationServer
from fabric.actor.core.kernel.ReservationStates import ReservationStates, ReservationPendingStates


class BrokerReservation(ReservationServer, IKernelBrokerReservation):
    """
    A note on exported "will call" reservations. An export() operation may be
    locally initiated on an agent. It binds and forms a ticket in the same way as
    if the request came from a client, but there is no client rid (remoteRid) and
    no callback object. The prepare method in AgentReservation and
    register/unregister in ReservationServer handle these cases: the export
    proceeds as a normal reserve request, but it leaves the callback and
    remoteRid null, does not register the reservation with its slice (since there
    is no remoteRid), and does not issue an updateTicket (since there is no
    callback). The client claims the ticket with a claim request, passing the
    exportedRid, and a remoteRid and callback in the usual fashion. At this time,
    prepareClaim() below sets the callback and remoteRid, then claim() registers
    the reservation with its slice and issues the ticket. It would be irregular
    for an export request to not be satisfied immediately, or for an extend
    request to arrive on an exported ticket that has not yet been claimed. Even
    so, all code in AgentReservation checks against a null callback before
    attempting to issue an updateTicket. Implementation note: once any request
    fails, this version marks the reservation as Failed and disallows any
    subsequent operations.
    """
    PropertySource = "AgentReservationSource"
    PropertyExporting = "AgentReservationExporting"
    PropertyAuthority = "AgentReservationAuthority"
    PropertyMustSendUpdate = "AgentReservationMustSendUpdate"

    def __init__(self, rid: ID, resources: ResourceSet, term: Term, slice_obj: IKernelSlice):
        super().__init__(rid, resources, term, slice_obj)
        # Reservation backing the ticket granted to this reservation. For now only
        # one source reservation can be used to issue a ticket to satisfy a client
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
        self.category = IReservation.CategoryBroker

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

    def restore(self, actor: IActor, slice_obj: ISlice, logger):
        """
        Must be invoked after creating reservation from unpickling
        """
        super().restore(actor, slice_obj, logger)
        self.source = None
        self.notified_failed = False
        self.closed_in_priming = False

    def print_state(self):
        """
        Converts the reservation to a state string.
        @return state string representing the reservation
        """
        return "[{},{}] ({})({})".format(self.get_state_name(), self.get_pending_state_name(), self.get_sequence_in(),
                                         self.get_sequence_out())

    def recover(self, parent, saved_state):
        if isinstance(self.policy, IAuthorityPolicy):
            self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))
            return

        if not isinstance(self.policy, IBrokerPolicy):
            raise Exception("Do not know how to recover: policy={}".format(self.policy))

        try:
            if self.state == ReservationStates.Nascent:
                if self.pending_state == ReservationPendingStates.None_:
                    self.actor.ticket(self)
                    self.logger.info("Added reservation #{} to the ticketing list. State={}".format(self.get_reservation_id(), self.print_state()))

                elif self.pending_state == ReservationPendingStates.Ticketing:
                    self.set_pending_recover(True)
                    self.transition("[recovery]", self.state, ReservationPendingStates.None_)
                    self.actor.ticket(self)
                    self.logger.info(
                        "Added reservation #{} to the ticketing list. State={}".format(self.get_reservation_id(),
                                                                                       self.print_state()))

                else:
                    raise Exception("Unexpected pending state")

            elif self.state == ReservationStates.Ticketed:
                if self.pending_state == ReservationPendingStates.None_ or self.pending_state == ReservationPendingStates.Priming:
                    self.set_service_pending(ReservationPendingStates.None_)
                    self.logger.debug("No recovery necessary for reservation #{}".format(self.get_reservation_id()))

                elif self.pending_state == ReservationPendingStates.ExtendingTicket:
                    self.set_pending_recover(True)
                    self.transition("[recovery]", self.state, ReservationPendingStates.None_)
                    self.actor.extend_ticket(self)
                    self.logger.info(
                        "Added reservation #{} to the extending list. State={}".format(self.get_reservation_id(),
                                                                                       self.print_state()))
                else:
                    raise Exception("Unexpected pending state")

            elif self.state == ReservationStates.Failed:
                self.logger.warning("Reservation #{} has failed".format(self.get_reservation_id()))
            else:
                raise Exception("Unexpected reservation state")

        except Exception as e:
            raise e

    def handle_failed_rpc(self, failed: FailedRPC):
        # make sure that the failed RPC came from the callback identity
        remote_auth = failed.get_remote_auth()
        if failed.get_request_type() == RPCRequestType.UpdateTicket:
            if self.callback is None or self.callback.get_identity() != remote_auth:
                raise Exception("Unauthorized Failed reservation RPC: expected={}, but was: {}".format(self.callback.get_identity(), remote_auth))
        else:
            Exception("Unexpected FailedRPC for BrokerReservation. RequestType={}".format(failed.get_request_type()))

        super().handle_failed_rpc(failed)

    def prepare(self, callback: ICallbackProxy, logger):
        self.set_logger(logger)
        self.callback = callback

        # Null callback indicates a locally initiated request to create an
        # exported reservation. Else the request is from a client and must have
        # a client-specified RID.

        if self.callback is not None:
            if self.rid is None:
                self.error("no reservation ID specified for request")

        self.set_dirty()

    def reserve(self, policy: IPolicy):
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
        self.map_and_update(False)

    def service_reserve(self):
        # resources is null initially. It becomes non-null once the
        # policy completes its allocation.
        if self.resources is not None:
            self.resources.service_update(self)
            if not self.is_failed():
                self.transition("update absorbed", ReservationStates.Ticketed, ReservationPendingStates.None_)
                self.generate_update()

    def claim(self):
        self.approved = False
        if self.state == ReservationStates.Ticketed:
            # We are an agent asked to return a pre-reserved "will call" ticket
            # to a client. Set mustSendUpdate so that the update will be sent
            # on the next probe.
            self.must_send_update = True
        else:
            self.error("Wrong reservation state for ticket claim")

    def extend_ticket(self, actor: IActor):
        self.incoming_request()

        # State must be ticketed. The reservation may be active, but the agent wouldn't know that
        if self.state != ReservationStates.Ticketed:
            self.error("extending unticketed reservation")

        if self.pending_state != ReservationPendingStates.None_ and self.pending_state != ReservationPendingStates.ExtendingTicket:
            self.error("extending reservation with another pending request")

        if not self.requested_term.extends_term(self.term):
            self.error("new term does not extend current term")

        self.approved = False
        self.bid_pending = True
        self.pending_recover = False
        self.map_and_update(True)

    def service_extend_ticket(self):
        if self.pending_state == ReservationPendingStates.None_:
            self.resources.service_update(self)
            if not self.is_failed():
                self.transition("update absorbed", ReservationStates.Ticketed, ReservationPendingStates.None_)
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

            self.transition("closed", ReservationStates.Closed, ReservationPendingStates.None_)
            self.policy.close(self)

        if send_notification:
            self.update_data.error("Closed while allocating ticket")
            self.generate_update()

    def probe_pending(self):
        if self.service_pending != ReservationPendingStates.None_:
            self.internal_error("service overrun in probePending")

        if self.is_failed() and not self.notified_failed:
            self.generate_update()
            self.notified_failed = True
        else:
            if self.pending_state == ReservationPendingStates.Ticketing:
                # Check for a pending ticket operation that may have completed
                if not self.bid_pending and self.map_and_update(False):
                    self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.ExtendingTicket:
                # Check for a pending extendTicket operation
                if not self.bid_pending and self.map_and_update(True):
                    self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.Redeeming:
                self.logger.error("AgentReservation in unexpected state")

            elif self.pending_state == ReservationPendingStates.Priming:
                self.service_pending = ReservationPendingStates.AbsorbUpdate

            elif self.pending_state == ReservationPendingStates.None_:
                # for exported reservations that have been claimed, we need to
                # schedule a ticketUpdate
                if self.must_send_update:
                    self.service_pending = ReservationPendingStates.SendUpdate
                    self.must_send_update = False

    def service_probe(self):
        try:
            if self.service_pending == ReservationPendingStates.AbsorbUpdate:
                self.resources.service_update(self)
                if not self.is_failed():
                    self.transition("update absorbed", ReservationStates.Ticketed, ReservationPendingStates.None_)
                    self.generate_update()

            elif self.service_pending == ReservationPendingStates.SendUpdate:
                self.generate_update()

        except Exception as e:
            self.log_error("failed while servicing probe", e)
            self.fail_notify(str(e))

        self.service_pending = ReservationPendingStates.None_

    def handle_duplicate_request(self, operation: RequestTypes):
        # The general idea is to do nothing if we are in the process of
        # performing a pending operation or about to reissue a
        # ticket/extendTicket after recovery. If there is nothing pending for
        # this reservation, we resend the last update.
        if operation == RequestTypes.RequestTicket:
            if self.pending_state == ReservationPendingStates.None_ and self.state != ReservationStates.Nascent and not self.pending_recover:
                self.generate_update()

        elif operation == RequestTypes.RequestExtendTicket:
            if self.pending_state == ReservationPendingStates.None_ and not self.pending_recover:
                self.generate_update()

        elif operation == RequestTypes.RequestRelinquish:
            self.log_debug("no op")

        else:
            raise Exception("unsupported operation {}".format(RequestTypes(operation).name))

    def generate_update(self):
        self.log_debug("Generating update")
        if self.callback is None:
            self.logger.warning("Cannot generate update: no callback.")
            return

        self.logger.debug("Generating update: update count={}".format(self.update_count))
        try:
            self.update_count += 1
            self.sequence_out += 1
            RPCManagerSingleton.get().update_ticket(self)
        except Exception as e:
            # Note that this may result in a "stuck" reservation... not much we
            # can do if the receiver has failed or rejects our update. We will
            # regenerate on any user-initiated probe.
            self.log_remote_error("callback failed", e)

    def map_and_update(self, ticketed: bool):
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
                self.fail_notify("reservation is not yet ticketed")
            else:
                self.log_debug("Using policy {} to bind reservation".format(self.policy.__class__.__name__))
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
                            granted = self.policy.bind(self)
                        else:
                            self.internal_error("Exporting reservations not implemented")
                    else:
                        granted = True
                    self.transition("ticket request", ReservationStates.Nascent, ReservationPendingStates.Ticketing)
                except Exception as e:
                    self.log_error("mapAndUpdate bindTicket failed for ticketRequest:", e)
                    self.fail_notify(str(e))
                    return success

                if granted:
                    self.logger.debug("Reservation {} has been granted".format(self.get_reservation_id()))
                    try:
                        success = True
                        self.term = self.approved_term
                        self.resources = self.approved_resources.abstract_clone()
                        self.resources.update(self, self.approved_resources)
                        self.transition("ticketed", ReservationStates.Ticketed, ReservationPendingStates.Priming)
                    except Exception as e:
                        self.log_error("mapAndUpdate ticket failed for ticketRequest", e)
                        self.fail_notify(str(e))
        elif self.state == ReservationStates.Ticketed:
            if not ticketed:
                self.fail_notify("reservation is already ticketed")
            else:
                try:
                    self.transition("ticket request", ReservationStates.Ticketed, ReservationPendingStates.Priming)

                    # If the policy has processed this reservation, set granted to
                    # true so that we can send the ticket back to the client. If
                    # the policy has not yet processed this reservation (binPending
                    # is true) then call the policy. The plugin may choose to
                    # process the request immediately (true) or to defer it
                    # (false). In case of a deferred request, we will eventually
                    # come back to this method after the policy has done its job.

                    granted = False

                    if self.is_bid_pending():
                        granted = self.policy.extend_broker(self)
                    else:
                        granted = True
                except Exception as e:
                    self.log_error("mapAndUpdate extendTicket failed for ticketRequest:", e)
                    self.fail_notify(str(e))
                    return success

                if granted:
                    try:
                        success = True
                        self.extended = True
                        self.transition("extended ticket", ReservationStates.Ticketed, ReservationPendingStates.Priming)
                        self.previous_term = self.term
                        self.previous_resources = self.resources.clone()
                        self.term = self.approved_term
                        self.resources.update(self, self.approved_resources)
                    except Exception as e:
                        self.log_error("mapAndUpdate ticket failed for ticketRequest", e)
                        self.fail_notify(str(e))
        else:
            self.log_error("broker mapAndUpdate: unexpected state", None)
            self.fail_notify("invalid operation for the current reservation state")

        return success

    def get_authority(self) -> IAuthorityProxy:
        return self.authority

    def get_source(self) -> IClientReservation:
        return self.source

    def get_units(self, when: datetime = None) -> int:
        hold = 0
        if not self.is_terminal():
            hold = self.resources.get_concrete_units(when)

        return hold

    def is_closed_in_priming(self) -> bool:
        return self.closed_in_priming

    def is_exporting(self) -> bool:
        return self.exporting

    def set_exporting(self):
        self.exporting = True

    def set_source(self, source: IClientReservation):
        self.source = source

    def set_authority(self, authority: IAuthorityProxy):
        self.authority = authority