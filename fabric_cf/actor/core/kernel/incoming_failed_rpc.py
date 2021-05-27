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

from fabric_cf.actor.core.kernel.incoming_rpc import IncomingRPC

if TYPE_CHECKING:
    from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.security.auth_token import AuthToken


class IncomingFailedRPC(IncomingRPC):
    """
    Represents an incoming failed rpc message
    """
    def __init__(self, message_id: ID, failed_request_type: RPCRequestType, request_id: str,
                 failed_reservation_id: ID, error_details: str, caller: AuthToken):
        super().__init__(message_id=message_id, request_type=RPCRequestType.FailedRPC, caller=caller,
                         callback=None)
        self.failed_request_type = failed_request_type
        self.failed_reservation_id = failed_reservation_id
        self.error_details = error_details
        self.request_id = request_id

    def get_error_details(self) -> str:
        """
        Get Error details
        @return error details
        """
        return self.error_details

    def get_failed_request_type(self) -> RPCRequestType:
        """
        Get failed request type
        @return failed request type
        """
        return self.failed_request_type

    def get_failed_reservation_id(self) -> ID():
        """
        Get failed reservation id
        @return failed reservation id
        """
        return self.failed_reservation_id

    def get(self):
        return None
