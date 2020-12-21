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

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.i_reservation import IReservation
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.i_delegation import IDelegation


class IClientPublic:
    """
    IClientPublic represents the public cross-actor interface for actors acting as clients of other actors.
    """

    @abstractmethod
    def update_ticket(self, *, reservation: IReservation, update_data, caller: AuthToken):
        """
        Handles an incoming ticket update.

        @param reservation reservation represented by this update. The
               reservation object will contain the ticket (if any) as well
               information about the actually allocated resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller

        @throws Exception in case of error
        """

    @abstractmethod
    def update_delegation(self, *, delegation: IDelegation, update_data, caller: AuthToken):
        """
        Handles an incoming delegation update.

        @param delegation delegation represented by this update. The
               delegation object will contain the delegation (if any) as well
               information about the actually allocated resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller

        @throws Exception in case of error
        """
