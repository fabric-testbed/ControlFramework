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

from fabric_cf.actor.core.util.id import ID
from .broker_delegation import BrokerDelegation
from ..apis.abc_broker_proxy import ABCBrokerProxy
from ..apis.abc_delegation import ABCDelegation


class BrokerDelegationFactory:
    """
    Factory class to create broker delegation instances
    """
    @staticmethod
    def create(did: str, slice_id: ID, broker: ABCBrokerProxy) -> ABCDelegation:
        """
        Create a broker delegation
        @param did delegation id
        @param slice_id slice id
        @param broker broker
        @return delegation
        """
        delegation = BrokerDelegation(dlg_graph_id=did, slice_id=slice_id, broker=broker)
        return delegation
