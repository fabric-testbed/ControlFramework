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
from abc import abstractmethod

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.kernel.reservation import Reservation
from fabric_cf.actor.core.kernel.reservation_states import ReservationPendingStates, ReservationStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.rpc_exception import RPCError
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class ReservationServer(Reservation, ABCServerReservation):
    """
    Implementation note on error handling. There are several kinds of errors,
    all of which are logged: - Error on an incoming operation, caught with no
    side effects: these are reflected as exceptions. - Reservation failure.
    Mark reservation as failed, report failure in next update, accept no
    further operations except probe and close, close when ready. - Resource
    failure: log as an event, report notification in next update, release
    failed resources, leave reservation state unchanged, complete operations
    with reduced resource set. Someday obtain new resources to fill the
    deficit. - Probe errors. Log and report. For externally initiated probes,
    always send a lease update. - Asserts. Throw exception if asserts are
    enabled. Errors should generally not throw exceptions up into the calling
    code, with a few exceptions: asserts, and incoming call errors with no
    side effects. Should update exception signatures on all reservation
    classes to reflect this convention.
    """

    def __init__(self, *, rid: ID, resources: ResourceSet, term: Term, slice_object: ABCSlice):
        super().__init__(rid=rid, slice_object=slice_object)
        # Sequence number for incoming messages.
        self.sequence_in = 0
        # Sequence number for outgoing messages.
        self.sequence_out = 0
        # Callback proxy.
        self.callback = None
        # Status of the last server-side operation for the reservation.
        self.update_data = UpdateData()
        # How many update messages have been sent to the client.
        self.update_count = 0
        # Identity of the client actor.
        self.client = None
        # Identity of the server actor.
        self.owner = None
        # Policy in control of the reservation.
        self.policy = None
        self.service_pending = ReservationPendingStates.None_
        self.approved = False
        self.requested_resources = resources
        self.requested_term = term

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

    def restore(self, *, actor: ABCActorMixin, slice_obj: ABCSlice):
        """
        Must be invoked after creating reservation from unpickling
        """
        super().restore(actor=actor, slice_obj=slice_obj)
        if actor is not None:
            if self.callback is not None:
                self.callback.set_logger(logger=actor.get_logger())
            self.policy = actor.get_policy()

    def prepare(self, *, callback: ABCCallbackProxy, logger):
        self.internal_error(err="abstract method")

    def validate_incoming(self):
        if self.slice is None:
            self.error(err="Missing slice")

        if self.requested_resources is None:
            self.error(err="Missing resource set")

        if self.requested_term is None:
            self.error(err="Missing term")

        self.requested_resources.validate_incoming()
        self.requested_term.validate()

    def incoming_request(self):
        """
        Checks reservation state prior to handling an incoming request. These
        checks are not applied to probes or closes.

        @throws Exception
        """
        assert self.slice is not None

        # Disallow a request on a failed reservation, but always send an update to reset the client.
        if self.is_failed():
            self.generate_update()
            self.error(err="server cannot satisfy request (marked failed)")

        # Disallow any further requests on a closing reservation. Generate and update to reset the client.
        if self.is_closed() or self.pending_state == ReservationPendingStates.Closing:
            self.generate_update()
            self.error(err="server cannot satisfy request closing")

    def update_lease(self, *, incoming: ABCReservationMixin, update_data):
        self.internal_error(err="Cannot update a server-side reservation")

    def update_ticket(self, *, incoming: ABCReservationMixin, update_data):
        self.internal_error(err="Cannot update a server-side reservation")

    def handle_failed_rpc(self, *, failed: FailedRPC):
        if failed.get_error_type() == RPCError.NetworkError:
            if self.is_failed() or self.is_closed():
                return

        self.fail(message=f"Failing reservation due to non-recoverable RPC error {failed.get_error_type()}",
                  exception=failed.get_error())

    def clear_notice(self):
        self.update_data.clear()

    def fail(self, *, message: str, exception: Exception = None):
        self.update_data.error(message=message)
        super().fail(message=message, exception=exception)

    def fail_notify(self, *, message: str):
        """
        Fail and notify
        @param message message
        """
        self.fail(message=message)
        self.generate_update()

    def fail_warn(self, *, message: str):
        self.update_data.error(message=message)
        super().fail_warn(message=message)

    @abstractmethod
    def generate_update(self):
        """
        Generates an update to the callback object (if any) for this reservation.
        """

    def get_callback(self) -> ABCCallbackProxy:
        return self.callback

    def get_sequence_in(self) -> int:
        return self.sequence_in

    def get_sequence_out(self) -> int:
        return self.sequence_out

    def get_type(self) -> ResourceType:
        if self.resources is not None:
            return self.resources.type
        else:
            if self.requested_resources is not None:
                return self.requested_resources.type
        return None

    def get_update_data(self) -> UpdateData:
        result = UpdateData()
        result.absorb(other=self.update_data)
        return result

    def get_notices(self) -> str:
        s = super().get_notices()
        notices = self.update_data.get_events()
        if notices is not None:
            s += " {}".format(notices)

        notices = self.update_data.get_message()
        if notices is not None:
            s += " {}".format(notices)

        return s

    def set_owner(self, *, owner: AuthToken):
        self.owner = owner

    def set_requested_resources(self, *, resources: ResourceSet):
        self.requested_resources = resources

    def set_requested_term(self, *, term: Term):
        self.requested_term = term

    def set_sequence_in(self, *, sequence: int):
        self.sequence_in = sequence

    def set_sequence_out(self, *, sequence: int):
        self.sequence_out = sequence

    def get_client_auth_token(self) -> AuthToken:
        if self.callback is not None:
            return self.callback.get_identity()
        else:
            return self.client
