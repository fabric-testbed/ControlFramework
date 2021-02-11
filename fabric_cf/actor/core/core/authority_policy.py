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
import traceback

from fabric_cf.actor.core.apis.i_authority import IAuthority
from fabric_cf.actor.core.apis.i_authority_policy import IAuthorityPolicy
from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation
from fabric_cf.actor.core.apis.i_delegation import IDelegation
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.core.policy import Policy
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.delegation.resource_delegation import ResourceDelegation
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_data import ResourceData


class AuthorityPolicy(Policy, IAuthorityPolicy):
    def __init__(self, *, actor: IAuthority = None):
        super().__init__(actor=actor)
        self.initialized = False
        self.delegations = None

    def initialize(self):
        if not self.initialized:
            super().initialize()
            self.initialized = True
            self.delegations = {}

    def revisit(self, *, reservation: IReservation):
        return

    def revisit_delegation(self, *, delegation: IDelegation):
        self.donate_delegation(delegation=delegation)

    def donate_delegation(self, *, delegation: IDelegation):
        self.delegations[delegation.get_delegation_id()] = delegation

    def eject(self, *, resources: ResourceSet):
        return

    def failed(self, *, resources: ResourceSet):
        return

    def freed(self, *, resources: ResourceSet):
        return

    def recovered(self, *, resources: ResourceSet):
        return

    def release(self, *, resources: ResourceSet):
        return

    def bind_delegation(self, *, delegation: IDelegation) -> bool:
        result = False

        if delegation.get_delegation_id() not in self.delegations:
            self.delegations[delegation.get_delegation_id()] = delegation
            result = True

        return result

    def allocate(self, *, cycle: int):
        return

    def assign(self, *, cycle: int):
        return

    def correct_deficit(self, *, reservation: IAuthorityReservation):
        if reservation.get_resources() is None:
            return

        self.finish_correct_deficit(reservation=reservation)

    def finish_correct_deficit(self, *, reservation: IAuthorityReservation, rset: ResourceSet = None):
        """
        Finishes correcting a deficit.
        @param rset correction
        @param reservation reservation
        @raises Exception in case of error
        """
        # We could have a partial set if there's a shortage. Go ahead and
        # install it: we'll come back later for the rest if we return a null
        # term. Alternatively, we could release them and throw an error.
        if rset is None:
            self.log_warn(message="we either do not have resources to satisfy the request or "
                                  "the reservation has/will have a pending operation")
            return

        if rset.is_empty():
            reservation.set_pending_recover(pending_recover=False)
        else:
            reservation.get_resources().update(reservation=reservation, resource_set=rset)

    def extend_authority(self, *, reservation: IAuthorityReservation) -> bool:
        return False

    def extend_broker(self, *, reservation: IBrokerReservation) -> bool:
        return False

    def extend(self, *, reservation: IReservation, resources: ResourceSet, term: Term):
        return False
