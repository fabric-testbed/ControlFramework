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
# Author Komal Thareja (kthare10@renci.org)
from bisect import bisect_left

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.user import ComponentType
from fim.user.topology import TopologyDiff, TopologyDiffTuple

from fabric_cf.actor.security.pdp_auth import ActionId


def binary_search(*, a, x):
    """
    Binary search element x in list a
    """
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    else:
        return -1


def sliver_to_str(*, sliver: BaseSliver):
    if isinstance(sliver, NodeSliver):
        return node_sliver_to_str(sliver=sliver)

    if isinstance(sliver, NetworkServiceSliver):
        return ns_sliver_to_str(sliver=sliver)


def node_sliver_to_str(*, sliver: NodeSliver):
    result = str(sliver)
    nic_types = [ComponentType.SharedNIC, ComponentType.SmartNIC]
    if sliver.attached_components_info is not None:
        for c in sliver.attached_components_info.devices.values():
            result += f"\nComponent: {c}"
            if c.get_type() not in nic_types:
                continue
            if c.network_service_info is not None and c.network_service_info.network_services is not None:
                for ns in c.network_service_info.network_services.values():
                    result += f"\nNS: {ns}"
                    if ns.interface_info is not None and ns.interface_info.interfaces is not None:
                        for i in ns.interface_info.interfaces.values():
                            result += f"\nIFS: {i}"
    return result


def ns_sliver_to_str(*, sliver: NetworkServiceSliver):
    result = str(sliver)
    for interface in sliver.interface_info.interfaces.values():
        result += f"\nIFS: {interface}"
    return result


def translate_avro_message_type_pdp_action_id(*, message_name: str) -> ActionId:
    if message_name == AbcMessageAvro.claim_resources:
        return ActionId.claim
    elif message_name == AbcMessageAvro.reclaim_resources:
        return ActionId.reclaim
    elif message_name == AbcMessageAvro.get_slices_request or message_name == AbcMessageAvro.get_reservations_request \
        or message_name == AbcMessageAvro.get_reservations_state_request or \
        message_name == AbcMessageAvro.get_delegations or message_name == AbcMessageAvro.get_reservation_units_request \
            or message_name == AbcMessageAvro.get_unit_request or message_name == AbcMessageAvro.get_broker_query_model_request:
        return ActionId.query
    elif message_name == AbcMessageAvro.remove_slice:
        return ActionId.delete
    elif message_name == AbcMessageAvro.close_reservations:
        return ActionId.close
    elif message_name == AbcMessageAvro.remove_reservation:
        return ActionId.delete
    elif message_name == AbcMessageAvro.extend_reservation:
        return ActionId.renew
    else:
        return ActionId.noop
