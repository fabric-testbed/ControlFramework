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

from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_actor_identity import ABCActorIdentity
if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_rpc_request_state import ABCRPCRequestState


class ABCProxy(ABCActorIdentity):
    """
    IProxy defines the base interface each actor proxy must implement.
    """

    @abstractmethod
    def get_type(self) -> str:
        """
        Returns the type of proxy

        Returns:
            proxy type
        """

    @abstractmethod
    def execute(self, *, request: ABCRPCRequestState, producer: AvroProducerApi):
        """
        Executes the specified request

        Args:
            request: request
            producer: AVRO AvroProducerApi

        Raises:
            Exception in case of error
        """

    @abstractmethod
    def get_logger(self):
        """
        Returns the logger
        """

    @abstractmethod
    def set_logger(self, *, logger):
        """
        Sets the logger
        """
