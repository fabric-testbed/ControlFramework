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

from fabric.actor.core.apis.i_reservation import IReservation

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
    from fabric.actor.core.util.update_data import UpdateData
    from fabric.actor.security.auth_token import AuthToken


class IServerReservation(IReservation):
    """
    IServerReservation defines the reservation interface for actors acting as servers for other actors.
    """
    def get_callback(self) -> ICallbackProxy:
        """
        Returns the callback proxy.
        @return callback proxy
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_client_auth_token(self) -> AuthToken:
        """
        Returns the identity of the user represented by the reservation.
        @returns identity of user
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_sequence_in(self):
        """
        Returns the sequence number of the last received message.
        @returns sequence number of the last received message
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_sequence_out(self):
        """
        Returns the sequence number of the last sent message.
        @returns sequence number of the last sent message
        """
        raise NotImplementedError( "Should have implemented this" )

    def is_bid_pending(self) -> bool:
        """
        Checks if the reservation is in progress of being allocated resources
        @returns true if the reservation is in the process of being allocated resources
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_bid_pending(self, value: bool):
        """
        Indicates whether the reservation is in the progress of
        obtaining resources. This flag should be set (true) when the actor
        policy starts processing a reservation and  should be cleared(false)
        when the policy completes the allocation process.
        @params value : value for the bid pending flag
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_sequence_in(self, sequence: int):
        """
        Sets the sequence number of the last received message.
        @params sequence : incoming message sequence number
        """
        raise NotImplementedError( "Should have implemented this" )

    def set_sequence_out(self, sequence: int):
        """
        Sets the sequence number of the last sent message.
        @params sequence : outgoing message sequence number
        """
        raise NotImplementedError( "Should have implemented this" )

    def get_update_data(self) -> UpdateData:
        """
        Returns data to be sent back to the client in an update message.
        @return data to be sent back to the client in an update message
        """
        raise NotImplementedError("Should have implemented this")