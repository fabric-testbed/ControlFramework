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
from ctypes import Union
from typing import Tuple

from fim.slivers.capacities_labels import Labels, Capacities
from fim.slivers.delegations import DelegationFormat, Delegations


class InventoryForType:
    def __init__(self):
        self.logger = None

    def set_logger(self, *, logger):
        self.logger = logger

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None

    def _get_delegations(self, *, lab_cap_delegations: Delegations) -> Tuple[str, Union[Labels, Capacities]]:
        # Grab Label Delegations
        delegation_id, deleg = lab_cap_delegations.get_sole_delegation()
        #self.logger.debug(f"Available label/capacity delegations: {deleg} format {deleg.get_format()}")
        # ignore pool definitions and references for now
        if deleg.get_format() != DelegationFormat.SinglePool:
            return None, None
        # get the Labels/Capacities object
        delegated_label_capacity = deleg.get_details()
        return delegation_id, delegated_label_capacity

    @abstractmethod
    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        """
        Frees the specified number of resource units.
        @param count number of units
        @param request request properties
        @param resource resource properties
        @return new resource properties
        """
