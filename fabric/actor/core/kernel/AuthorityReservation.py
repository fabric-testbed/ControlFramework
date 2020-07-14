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
    from fabric.actor.core.apis.ICallbackProxy import ICallbackProxy
    from fabric.actor.core.apis.IPolicy import IPolicy
    from fabric.actor.core.apis.ISlice import ISlice
    from fabric.actor.core.kernel.FailedRPC import FailedRPC
    from fabric.actor.core.apis.IKernelSlice import IKernelSlice
    from fabric.actor.core.kernel.ResourceSet import ResourceSet
    from fabric.actor.core.time.Term import Term
    from fabric.actor.core.util.ID import ID

from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.IKernelAuthorityReservation import IKernelAuthorityReservation
from fabric.actor.core.kernel.RPCRequestType import RPCRequestType
from fabric.actor.core.kernel.RequestTypes import RequestTypes
from fabric.actor.core.kernel.ReservationServer import ReservationServer
from fabric.actor.core.kernel.ReservationStates import ReservationStates, ReservationPendingStates


class AuthorityReservation(ReservationServer, IKernelAuthorityReservation):
    """
    AuthorityReservation controls the state machine for a reservation on the
    authority side. It coordinates resource allocation, lease generation,
    priming, and shutdown of reservations.
    """
    def __init__(self, rid: ID, resources: ResourceSet, term: Term, slice_object: IKernelSlice):
        super().__init__(rid, resources, term, slice_object)
        # The ticket.
        self.ticket = None
        # Policies use this flag to instruct the core to send reservations to the client even if they have deficit.
        self.send_with_deficit = True
        # True if we notified the client about the fact that the reservation had failed
        self.notified_about_failure = False
        # Creates a new "blank" reservation instance. Used during recovery.
        self.category = IReservation.CategoryAuthority

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

        del state['notified_about_failure']

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

        self.notified_about_failure = False

    def restore(self, actor: IActor, slice_obj: ISlice, logger):
        """
        Must be invoked after creating reservation from unpickling
        """
        super().restore(actor, slice_obj, logger)
        self.notified_about_failure = False

    def prepare(self, callback: ICallbackProxy, logger):
        self.set_logger(logger)
        self.callback = callback
        self.requested_resources.validate_incoming_ticket(self.requested_term)

        if self.rid is None:
            self.error("no reservation ID specified for request")

        self.state = ReservationStates.Ticketed

    def reserve(self, policy: IPolicy):
        self.nothing_pending()
        self.incoming_request()
        if self.is_active():
            self.error("reservation already holds a lease")

        self.policy = policy
        self.approved = False
        self.bid_pending = True
        self.pending_recover = False
        self.map_and_update(False)

    def service_reserve(self):
        try:
            if self.resources is not None:
                self.resources.service_reserve_site()
        except Exception as e:
            self.log_error("authority failed servicing reserve", e)
            self.fail_notify(str(e))

    def extend_lease(self):
        self.nothing_pending()
        self.incoming_request()

        if not self.is_active():
            self.error("reservation does not yet hold a lease")

        if not self.requested_term.extends_term(self.term):
            self.error("requested term does not extend current term for extendLease")

        self.approved = False
        self.bid_pending = True
        self.pending_recover = False
        self.map_and_update(True)

    def modify_lease(self):
        self.nothing_pending()
        self.incoming_request()

        if not self.is_active():
            self.error("reservation does not yet hold a lease")

        self.approved = True
        self.bid_pending = True
        self.pending_recover = False
        self.map_and_update_modify_lease()

    def service_extend_lease(self):
        assert (self.state == ReservationStates.Failed and self.pending_state == ReservationPendingStates.None_) or \
                self.pending_state == ReservationPendingStates.ExtendingLease or \
                self.pending_state == ReservationPendingStates.Priming

        try:
            if self.pending_state == ReservationPendingStates.Priming:
                self.resources.service_extend()
        except Exception as e:
            self.log_error("authority failed servicing extendLease", e)
            self.fail_notify(str(e))

    def service_modify_lease(self):
        assert (self.state == ReservationStates.Failed and self.pending_state == ReservationPendingStates.None_) or \
               self.pending_state == ReservationPendingStates.ModifyingLease or \
               self.pending_state == ReservationPendingStates.Priming

        try:
            if self.pending_state == ReservationPendingStates.Priming:
                self.resources.service_modify()
        except Exception as e:
            self.log_error("authority failed servicing modifylease", e)
            self.fail_notify(str(e))

    def close(self):
        self.logger.debug("Processing  close for #{}".format(self.rid))
        self.transition("external close", self.state, ReservationPendingStates.Closing)

    def service_close(self):
        if self.resources is not None:
            self.resources.close()

    def handle_duplicate_request(self, operation: RequestTypes):
        # The general idea is to do nothing if we are in the process of
        # performing a pending operation or about to reissue a
        # ticket/extendTicket after recovery. If there is nothing pending for
        # this reservation, we resend the last update.

        if operation == RequestTypes.RequestRedeem:
            if self.pending_state == ReservationPendingStates.None_ and not self.bid_pending and \
                    not self.pending_recover:
                self.generate_update()
        elif operation == RequestTypes.RequestExtendLease:
            if self.pending_state == ReservationPendingStates.None_ and not self.bid_pending and not \
                    self.pending_recover:
                self.generate_update()
        elif operation == RequestTypes.RequestModifyLease:
            if self.pending_state == ReservationPendingStates.None_ and not self.bid_pending and not \
                    self.pending_recover:
                self.generate_update()
        else:
            raise Exception("Unsupported operation: {}".format(operation))

    def map_and_update(self, extend: bool) -> bool:
        """
        Calls the policy to fill a request, with associated state transitions.
        
        @param extend
                   true if this request is an extend
        @return boolean success
        """
        success = False
        granted = False
        if self.state == ReservationStates.Failed:
            # Must be a previous failure, or policy marked it as failed. Send update to reset client.
            self.generate_update()
        elif self.state == ReservationStates.Ticketed:
            assert not extend
            try:
                self.transition("redeeming", ReservationStates.Ticketed, ReservationPendingStates.Redeeming)
                # If the policy has processed this reservation, set granted to
                # true so that we can start priming the resources. If the
                # policy has not yet processed this reservation (binPending is
                # true) then call the policy. The policy may choose to process
                # the request immediately (true) or to defer it (false). In
                # case of a deferred request, we will eventually come back to
                # this method after the policy has done its job.
                if self.is_bid_pending():
                    granted = self.policy.bind(self)
                else:
                    granted = True
            except Exception as e:
                self.log_error("authority policy bind", e)
                self.fail_notify(str(e))

            if granted:
                try:
                    success = True
                    self.ticket = self.requested_resources
                    self.term = self.approved_term
                    self.resources = self.requested_resources.abstract_clone()
                    self.resources.units = 0
                    self.resources.update(self, self.approved_resources)
                    self.transition("redeem", ReservationStates.Ticketed, ReservationPendingStates.Priming)
                except Exception as e:
                    self.log_error("authority redeem", e)
                    self.fail_notify(str(e))
        elif self.state == ReservationStates.Active:
            assert extend
            try:
                self.transition("extending lease", ReservationStates.Active, ReservationPendingStates.ExtendingLease)
                # If the policy has processed this reservation, set granted to
                # true so that we can start priming the resources. If the
                # policy has not yet processed this reservation (binPending is
                # true) then call the policy. The policy may choose to process
                # the request immediately (true) or to defer it (false). In
                # case of a deferred request, we will eventually come back to
                # this method after the policy has done its job.
                if self.is_bid_pending():
                    granted = self.policy.bind(self)
                else:
                    granted = True

                if granted:
                    success = True
                    self.extended = True
                    self.previous_term = self.term
                    self.ticket = self.requested_resources
                    self.term = self.approved_term
                    if self.requested_resources.get_config_properties() is not None:
                        self.approved_resources.set_config_properties(self.requested_resources.get_config_properties())

                    self.resources.update(self, self.approved_resources)
                    self.transition("extend lease", ReservationStates.Active, ReservationPendingStates.Priming)
            except Exception as e:
                self.log_error("authority mapper extend", e)
                self.fail_notify(str(e))
        else:
            self.fail("mapAndUpdate: unexpected state", None)
        return success

    def map_and_update_modify_lease(self) -> bool:
        """
        Calls the policy to fill a request, with associated state transitions.
        
        @return boolean success
        """
        success = False
        granted = False
        if self.state == ReservationStates.Failed:
            # Must be a previous failure, or policy marked it as failed. Send update to reset client.
            self.generate_update()
        elif self.state == ReservationStates.Active:
            try:
                self.transition("modifying lease", ReservationStates.Active, ReservationPendingStates.ModifyingLease)
                granted = True
                if granted:
                    success = True
                    self.ticket = self.requested_resources
                    self.logger.debug("requestedResources.getConfigurationProperties() = {}".format(self.requested_resources.get_config_properties()))
                    self.logger.debug("approvedResources.getConfigurationProperties() = {}".format(self.approved_resources.get_config_properties()))
                    if self.requested_resources.get_config_properties() is not None:
                        if self.approved_resources.get_config_properties() is not None:
                            self.approved_resources.set_config_properties(self.requested_resources.get_config_properties())
                        else:
                            # TODO merge
                            self.logger.debug("merge properties")
                    self.logger.debug("approvedResources.getConfigurationProperties() = {}".format(self.approved_resources.get_config_properties()))
                    self.resources.update_properties(self, self.approved_resources)
                    self.transition("modify lease", ReservationStates.Active, ReservationPendingStates.Priming)
            except Exception as e:
                self.log_error("authority mapper modify", e)
                self.fail_notify(str(e))
        else:
            self.fail("mapAndUpdateModifyLease: unexpected state")

        return success

    def generate_update(self):
        if self.callback is None:
            self.log_warning("cannot generate update: no callback")
            return

        try:
            self.update_count += 1
            self.sequence_out += 1
            from fabric.actor.core.kernel.RPCManagerSingleton import RPCManagerSingleton
            RPCManagerSingleton.get().update_lease(self)
        except Exception as e:
            self.log_remote_error("callback failed", e)

    def handle_failed_rpc(self, failed: FailedRPC):
        remote_auth = failed.get_remote_auth()
        if failed.get_request_type() == RPCRequestType.UpdateLease:
            if self.callback is None or self.callback.get_identity() != remote_auth:
                raise Exception("Unauthorized Failed reservation RPC: expected={}, but was: {}".format(self.callback.get_identity(), remote_auth))
        else:
            raise Exception("Unexpected FailedRPC for BrokerReservation. RequestType={}".format(failed.get_request_type()))

    def prepare_extend_lease(self):
        self.requested_resources.validate_incoming_ticket(self.requested_term)

    def prepare_modify_lease(self):
        self.requested_resources.validate_incoming_ticket(self.requested_term)

    def prepare_probe(self):
        try:
            if self.resources is not None:
                self.resources.prepare_probe()
        except Exception as e:
            self.log_error("exception in authority prepareProbe", e)

    def probe_pending(self):
        if self.service_pending != ReservationPendingStates.None_:
            self.log_error("service overrun in probePending", None)
            return

        self.reap()

        if self.pending_state == ReservationPendingStates.None_:
            if self.is_failed() and not self.notified_about_failure:
                self.generate_update()
                self.notified_about_failure = True

        elif self.pending_state == ReservationPendingStates.Redeeming:
            # We are an authority trying to satisfy a ticket redeem on behalf of a client. Retry policy bind.
            assert self.state == ReservationStates.Ticketed
            if not self.bid_pending and self.map_and_update(False):
                self.log_debug("Resource assignment (redeem) for #{} completed".format(self.rid))
                self.service_pending = ReservationPendingStates.Redeeming

        elif self.pending_state == ReservationPendingStates.ExtendingLease:
            assert self.state == ReservationStates.Active
            if not self.bid_pending and self.map_and_update(True):
                self.log_debug("Resource assignment (extend) for #{} completed".format(self.rid))
                self.service_pending = ReservationPendingStates.ExtendingLease

        elif self.pending_state == ReservationPendingStates.ModifyingLease:
            assert self.state == ReservationStates.Active
            if not self.bid_pending and self.map_and_update_modify_lease():
                self.log_debug("Resource assignment (modify) for #{} completed".format(self.rid))
                self.service_pending = ReservationPendingStates.ModifyingLease

        elif self.pending_state == ReservationPendingStates.Closing:
            if self.resources is None or self.resources.is_closed():
                self.transition("close complete", ReservationStates.Closed, ReservationPendingStates.None_)
                self.pending_recover = False
                self.generate_update()

        elif self.pending_state == ReservationPendingStates.Priming:
            # We are an authority filling a ticket claim. Got resources? Note
            # that active() just means no primes/closes/modifies are still in
            # progress. The primes/closes/modifies could have failed. If
            # something succeeded, then we report what we got as active, else
            # it's a complete bust.
            if self.resources.is_active():
                # If something failed or we are recovering, we need to correct
                # the deficit. For a recovering reservation we need to call
                # correctDeficit regardless of whether there is a real deficit,
                # since the individual nodes may be inconsistent with what the
                # client/broker wanted. For example, they may have the wrong
                # logical ids and resource shares.
                if self.pending_recover or self.get_deficit() != 0:
                    # The abstract and the concrete units may be different. We
                    # need to adjust the abstract to equal concrete so that
                    # future additions of resources will not result in
                    # inconsistent abstract unit count.
                    self.resources.fix_abstract_units()
                    # Policies can instruct us to let go a reservation with a
                    # deficit. For example, a policy failed adding resources to
                    # the reservation multiple times and it wants to prevent
                    # exhausting its inventory from servicing this particular
                    # request: probably something is wrong with the request.
                    if not self.send_with_deficit:
                        # Call the policy to correct the deficit
                        self.policy.correct_deficit(self)
                        # XXX: be careful here. we are reusing extending for
                        # the purpose of triggering configuration actions on
                        # the newly assigned nodes. If this is not appropriate,
                        # we may need a new servicePending value
                        self.service_pending = ReservationPendingStates.ExtendingLease
                    else:
                        self.pending_recover = False
                        if self.resources.get_units() == 0:
                            message = "no information available"
                            if self.update_data.get_events() is not None:
                                message = self.update_data.get_events()
                            self.fail("all units failed priming: {}".format(message))
                            self.update_data.clear()
                        else:
                            self.transition("prime complete1", ReservationStates.Active, ReservationPendingStates.None_)
                        self.generate_update()
                else:
                    self.pending_recover = False
                    self.transition("prime complete2", ReservationStates.Active, ReservationPendingStates.None_)
                    self.generate_update()

    def service_probe(self):
        # An exception in one of these service routines should mean some
        # unrecoverable, reservation-wide failure. It should not occur, e.g.,
        # if some subset of the resources fail.
        try:
            if self.service_pending == ReservationPendingStates.Redeeming:
                self.service_reserve()

            elif self.service_pending == ReservationPendingStates.ExtendingLease:
                self.service_extend_lease()

            elif self.service_pending == ReservationPendingStates.ModifyingLease:
                self.service_modify_lease()

        except Exception as e:
            self.log_error("authority failed servicing probe", e)
            self.fail_notify("post-op exception: {}".format(e))
        self.service_pending = ReservationPendingStates.None_

    def reap(self):
        """
        Reaps any failed or closed resources. Need more here: if reservation has
        a deficit due to failures then we need to find some replacements.
        @throws Exception
        """
        try:
            if self.resources is not None:
                released = self.resources.collect_released()
                if released is not None:
                    if not released.get_notices().is_empty():
                        self.update_data.post(released.get_notices().get_notice())
                    self.policy.release(released)
        except Exception as e:
            self.log_error("exception in authority reap", e)

    def recover(self, parent, saved_state: dict):
        try:
            if self.state == ReservationStates.Ticketed:
                if self.pending_state == ReservationPendingStates.None_:
                    self.actor.redeem(self, None, None)

                elif self.pending_state == ReservationPendingStates.Redeeming:
                    self.transition("[recover]", self.state, ReservationPendingStates.None_)
                    self.actor.redeem(self, None, None)

                elif self.pending_state == ReservationPendingStates.Priming:
                    self.pending_recover = True
                    self.actor.close(reservation=self)

                elif self.pending_state == ReservationPendingStates.Closing:
                    self.actor.close(reservation=self)

                else:
                    raise Exception("Unexpected reservation state: state={} pending={}".format(self.state, self.pending_state))

            elif self.state == ReservationStates.Active:
                if self.pending_state == ReservationPendingStates.None_:
                    self.log_debug("No op")

                elif self.pending_state == ReservationPendingStates.ExtendingLease:
                    self.transition("[recover]", self.state, ReservationPendingStates.None_)
                    self.actor.extend_lease(self, None)

                elif self.pending_state == ReservationPendingStates.Priming:
                    self.pending_recover = True
                    self.actor.close(reservation=self)

                elif self.pending_state == ReservationPendingStates.Closing:
                    self.actor.close(reservation=self)

                else:
                    raise Exception(
                        "Unexpected reservation state: state={} pending={}".format(self.state, self.pending_state))
            elif self.state == ReservationStates.Failed:
                self.log_debug("No op")
            else:
                raise Exception(
                    "Unexpected reservation state: state={} pending={}".format(self.state, self.pending_state))
        except Exception as e:
            raise e

    def set_send_with_deficit(self, value: bool):
        self.send_with_deficit = value

    def get_deficit(self):
        result = 0
        if self.requested_resources is not None:
            result = self.requested_resources.get_units()

        if self.resources is not None:
            cs = self.resources.get_resources()
            if cs is not None:
                result -= cs.get_units()

        return result

    def get_leased_units(self) -> int:
        if self.resources is not None:
            cs = self.resources.get_resources()
            if cs is not None:
                return cs.get_units()
        return 0

    def get_notices(self) -> str:
        s = super().get_notices()
        if self.resources is not None and self.resources.get_resources() is not None:
            cs = self.resources.get_resources()
            notices = cs.get_notices()
            if notices is not None:
                s += "\n{}".format(notices)

        return s

    def get_ticket(self) -> ResourceSet:
        return self.ticket
