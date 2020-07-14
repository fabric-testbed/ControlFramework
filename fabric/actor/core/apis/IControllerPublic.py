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
    from fabric.actor.core.apis.IReservation import IReservation
    from fabric.actor.security.AuthToken import AuthToken


class IControllerPublic:
    """
    IControllerPublic represents the public cross-actor interface for actors acting in the controller role.
    """
    def update_lease(self, reservation: IReservation, update_data, caller: AuthToken):
        """
        Handles an incoming lease update.
       
        @param reservation reservation represented by this update. The
               reservation object will contain the lease (if any) as well
               information about the actually leased resources.
        @param update_data status of the remote operation.
        @param caller identity of the caller
       
        @throws Exception in case of error
        """
        raise NotImplementedError("Should have implemented this")
