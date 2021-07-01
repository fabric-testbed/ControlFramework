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

from abc import abstractmethod
from typing import TYPE_CHECKING, List
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.kernel.reservation_states import JoinState

if TYPE_CHECKING:
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.kernel.predecessor_state import PredecessorState


class ABCControllerReservation(ABCClientReservation):
    """
    Interface for Controller/Orchestrator Reservations
    """
    @abstractmethod
    def get_join_state(self) -> JoinState:
        """
        Returns the join state.
        @returns join state
        """

    @abstractmethod
    def get_join_state_name(self) -> str:
        """
        Returns the name of the join state.
        @returns name of the join state
        """

    @abstractmethod
    def get_leased_resources(self) -> ResourceSet:
        """
        Returns the resources leased by the reservation. If the reservation has
        not yet issued a redeem request, returns null.
        @returns resources leased by the reservation. Can be null.
        """

    @abstractmethod
    def get_lease_sequence_in(self) -> int:
        """
        Returns the reservation sequence number for incoming ticket/extend ticket messages.
        @returns reservation sequence number for incoming ticket/extend ticket messages
        """

    @abstractmethod
    def get_lease_sequence_out(self) -> int:
        """
        Returns the reservation sequence number for outgoing ticket/extend ticket messages.
        @returns reservation sequence number for outgoing ticket/extend ticket messages
        """

    @abstractmethod
    def get_lease_term(self) -> Term:
        """
        Returns the term of the current lease.
        @returns current lease term
        """

    @abstractmethod
    def get_previous_lease_term(self) -> Term:
        """
        Returns the previous lease term.
        @returns previous lease term
        """

    @abstractmethod
    def is_active_joined(self) -> bool:
        """
        Returns true if this reservation is currently active, and has completed
        joining the guest, i.e., successor reservations (with join dependencies
        on this reservation) may now join. Note: if this reservation is closed or
        failed, activeJoined returns false and successors will remain blocked,
        i.e, the caller must close them.
        @returns true or false as explained above
        """

    @abstractmethod
    def set_exported(self, *, exported: bool):
        """
        Indicates whether the reservation represents exported resources.
        @params exported value for the exported flag
        """

    @abstractmethod
    def add_join_predecessor(self, *, predecessor: ABCReservationMixin, filters: dict = None):
        """
        Sets the join predecessor: the reservation, for which the kernel must
        issue a join before a join may be issued for the current reservation.
        @params predecessor predecessor reservation
        @params filters: filters
        """

    @abstractmethod
    def set_lease_sequence_in(self, *, sequence: int):
        """
        Sets the reservation sequence number for incoming lease messages.
        @params sequence sequence number
        """

    @abstractmethod
    def set_lease_sequence_out(self, *, sequence: int):
        """
        Sets the reservation sequence number for outgoing lease messages.
        @params sequence sequence number
        """

    @abstractmethod
    def add_redeem_predecessor(self, *, reservation: ABCReservationMixin, filters: dict = None):
        """
        Adds a redeem predecessor to this reservation: the passed in reservation
        must be redeemed before this reservation.
        @params reservation: predecessor reservation
        @params filters: filters
        """

    @abstractmethod
    def get_redeem_predecessors(self) -> List[PredecessorState]:
        """
        Returns the redeem predecessors list for the reservation.
        @returns redeem predecessors list for the reservation
        """

    @abstractmethod
    def get_join_predecessors(self) -> List[PredecessorState]:
        """
        Returns the join predecessors list for the reservation.
        @returns join predecessors list for the reservation
        """
