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
from fabric.actor.core.apis.IActor import IActor
from fabric.actor.core.apis.IProxy import IProxy
from fabric.actor.core.apis.IRPCRequestState import IRPCRequestState
from fabric.actor.core.apis.IRPCResponseHandler import IRPCResponseHandler
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.kernel.RPCRequestType import RPCRequestType


class RPCRequest:
    def __init__(self, request: IRPCRequestState, actor: IActor, proxy: IProxy,
                 reservation: IReservation, sequence: int, handler: IRPCResponseHandler):
        self.request = request
        self.actor = actor
        self.proxy = proxy
        self.reservation = reservation
        self.sequence = sequence
        self.handler = handler
        self.retry_count = 0
        self.timer = None

    def get_actor(self) -> IActor:
        return self.actor

    def get_reservation(self) -> IReservation:
        return self.reservation

    def get_handler(self) -> IRPCResponseHandler:
        return self.handler

    def get_request_type(self) -> RPCRequestType:
        return self.request.get_type()

    def cancel_timer(self):
        ### TODO
        if self.timer is not None:
            self.timer.cancel()
