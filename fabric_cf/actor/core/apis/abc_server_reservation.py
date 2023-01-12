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
from typing import TYPE_CHECKING

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
    from fabric_cf.actor.core.util.update_data import UpdateData
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.kernel.resource_set import ResourceSet
    from fabric_cf.actor.core.time.term import Term


class ABCServerReservation(ABCReservationMixin):
    """
    IServerReservation defines the reservation interface for actors acting as servers for other actors.
    """

    @abstractmethod
    def get_callback(self) -> ABCCallbackProxy:
        """
        Returns the callback proxy.
        @return callback proxy
        """

    @abstractmethod
    def get_client_auth_token(self) -> AuthToken:
        """
        Returns the identity of the user represented by the reservation.
        @returns identity of user
        """

    @abstractmethod
    def get_sequence_in(self):
        """
        Returns the sequence number of the last received message.
        @returns sequence number of the last received message
        """

    @abstractmethod
    def get_sequence_out(self):
        """
        Returns the sequence number of the last sent message.
        @returns sequence number of the last sent message
        """

    @abstractmethod
    def is_bid_pending(self) -> bool:
        """
        Checks if the reservation is in progress of being allocated resources
        @returns true if the reservation is in the process of being allocated resources
        """

    @abstractmethod
    def set_bid_pending(self, *, value: bool):
        """
        Indicates whether the reservation is in the progress of
        obtaining resources. This flag should be set (true) when the actor
        policy starts processing a reservation and  should be cleared(false)
        when the policy completes the allocation process.
        @params value : value for the bid pending flag
        """

    @abstractmethod
    def set_sequence_in(self, *, sequence: int):
        """
        Sets the sequence number of the last received message.
        @params sequence : incoming message sequence number
        """

    @abstractmethod
    def set_sequence_out(self, *, sequence: int):
        """
        Sets the sequence number of the last sent message.
        @params sequence : outgoing message sequence number
        """

    @abstractmethod
    def get_update_data(self) -> UpdateData:
        """
        Returns data to be sent back to the client in an update message.
        @return data to be sent back to the client in an update message
        """

    @abstractmethod
    def set_owner(self, *, owner: AuthToken):
        """
        Sets the identity of the server actor that controls the reservation.
        @param owner identity of server actor
        """

    @abstractmethod
    def set_requested_resources(self, *, resources: ResourceSet):
        """
        Sets the requested resources.

        @param resources requested resources
        """

    @abstractmethod
    def set_requested_term(self, *, term: Term):
        """
        Sets the requested term.

        @param term requested term
        """

    @abstractmethod
    def post_message(self, *, message: str):
        """
        Post an update when some of the user data was overwritten as part of Extend
        """
    @abstractmethod
    def clear_notice(self, clear_fail: bool = False):
        """
        Clear the Notices
        """