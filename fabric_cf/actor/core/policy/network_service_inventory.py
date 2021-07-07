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
from typing import Tuple, List

from fim.slivers.attached_components import AttachedComponentsInfo, ComponentSliver
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.capacities_labels import Capacities, Labels
from fim.slivers.delegations import Delegations, DelegationFormat
from fim.slivers.instance_catalog import InstanceCatalog
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID


class NetworkServiceInventory(InventoryForType):
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

    def allocate(self, *, rid: ID, requested_sliver: BaseSliver, graph_id: str, graph_node: BaseSliver,
                 existing_reservations: List[ABCReservationMixin]) -> BaseSliver:

        if existing_reservations is not None:
            # FIXME - needs to be completed and tested
            self.logger.debug("Shared NIC - Ignore")
        else:
            vlans = graph_node.get_labels().vlan_range.split("-")
            vlan_tag = int(vlans[0]) + 10
            requested_sliver.get_labels().set_fields(vlan=str(vlan_tag))

        return requested_sliver

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        pass
