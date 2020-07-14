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
from fabric.actor.core.apis.IClientReservation import IClientReservation
from fabric.actor.core.kernel.ReservationStates import JoinState

if TYPE_CHECKING:
    from fabric.actor.core.time.Term import Term
    from fabric.actor.core.kernel.ResourceSet import ResourceSet


class IControllerReservation(IClientReservation):
    def get_join_state(self) -> JoinState:
        """
        Returns the join state.
        @returns join state
        """
        raise NotImplementedError("Should have implemented this")

    def get_join_state_name(self) -> str:
        """
        Returns the name of the join state.
        @returns name of the join state
        """
        raise NotImplementedError("Should have implemented this")

    def get_leased_resources(self) -> ResourceSet:
        """
        Returns the resources leased by the reservation. If the reservation has
        not yet issued a redeem request, returns null.
        @returns resources leased by the reservation. Can be null.
        """
        raise NotImplementedError("Should have implemented this")

    def get_lease_sequence_in(self) -> int:
        """
        Returns the reservation sequence number for incoming ticket/extend ticket messages.
        @returns reservation sequence number for incoming ticket/extend ticket messages
        """
        raise NotImplementedError("Should have implemented this")

    def get_lease_sequence_out(self) -> int:
        """
        Returns the reservation sequence number for outgoing ticket/extend ticket messages.
        @returns reservation sequence number for outgoing ticket/extend ticket messages
        """
        raise NotImplementedError("Should have implemented this")

    def get_lease_term(self) -> Term:
        """
        Returns the term of the current lease.
        @returns current lease term
        """
        raise NotImplementedError("Should have implemented this")

    def get_previous_lease_term(self) -> Term:
        """
        Returns the previous lease term.
        @returns previous lease term
        """
        raise NotImplementedError("Should have implemented this")

    def is_active_joined(self) -> bool:
        """
        Returns true if this reservation is currently active, and has completed
        joining the guest, i.e., successor reservations (with join dependencies
        on this reservation) may now join. Note: if this reservation is closed or
        failed, activeJoined returns false and successors will remain blocked,
        i.e, the caller must close them.
        @returns true or false as explained above
        """
        raise NotImplementedError("Should have implemented this")

    def set_exported(self, exported: bool):
        """
        Indicates whether the reservation represents exported resources.
        @params exported value for the exported flag
        """
        raise NotImplementedError("Should have implemented this")

    def add_join_predecessor(self, predecessor, filter: dict = None):
        """
        Sets the join predecessor: the reservation, for which the kernel must
        issue a join before a join may be issued for the current reservation.
        @params predecessor predecessor reservation
        @params filter: filter
        """
        raise NotImplementedError("Should have implemented this")

    def set_lease_sequence_in(self, sequence: int):
        """
        Sets the reservation sequence number for incoming lease messages.
        @params sequence sequence number
        """
        raise NotImplementedError("Should have implemented this")

    def set_lease_sequence_out(self, sequence: int):
        """
        Sets the reservation sequence number for outgoing lease messages.
        @params sequence sequence number
        """
        raise NotImplementedError("Should have implemented this")

    def add_redeem_predecessor(self, reservation, filter: dict = None):
        """
        Adds a redeem predecessor to this reservation: the passed in reservation
        must be redeemed before this reservation.
        @params reservation: predecessor reservation
        @params filter: filter
        """
        raise NotImplementedError("Should have implemented this")

    def get_redeem_predecessors(self) -> list:
        """
        Returns the redeem predecessors list for the reservation.
        @returns redeem predecessors list for the reservation
        """
        raise NotImplementedError("Should have implemented this")

    def get_join_predecessors(self) -> list:
        """
        Returns the join predecessors list for the reservation.
        @returns join predecessors list for the reservation
        """
        raise NotImplementedError("Should have implemented this")

    def set_config_property(self, key: str, value: str):
        """
        Sets a configuration property.
        @params key: key
        @params value: value
        """
        raise NotImplementedError("Should have implemented this")

    def set_request_property(self, key: str, value: str):
        """
        Sets a request property.
        @params key: key
        @params value: value
        """
        raise NotImplementedError("Should have implemented this")
