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
import hashlib
from bisect import bisect_left
from typing import Tuple, Dict

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.user import ComponentType, InstanceCatalog

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
                            if i.interface_info and i.interface_info.interfaces:
                                for ch_ifc in i.interface_info.interfaces.values():
                                    result += f"\nSub IFS: {ch_ifc}"
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


def generate_sha256(*, token: str):
    """
    Generate SHA 256 for a token
    @param token token string
    """
    # Create a new SHA256 hash object
    sha256_hash = hashlib.sha256()

    # Convert the string to bytes and update the hash object
    sha256_hash.update(token.encode('utf-8'))

    # Get the hexadecimal representation of the hash
    sha256_hex = sha256_hash.hexdigest()

    return sha256_hex


def extract_quota_usage(sliver, duration: float) -> Dict[Tuple[str, str], float]:
    """
    Extract quota usage from a sliver

    @param sliver: The sliver object from which resources are extracted.
    @param duration: Number of hours the resources are requested for.
    @return: A dictionary of resource type/unit tuples to requested amounts.
    """
    unit = "HOURS"
    requested_resources = {}

    # Check if the sliver is a NodeSliver
    if not isinstance(sliver, NodeSliver):
        return requested_resources

    allocations = sliver.get_capacity_allocations()
    if not allocations and sliver.get_capacity_hints():
        catalog = InstanceCatalog()
        allocations = catalog.get_instance_capacities(instance_type=sliver.get_capacity_hints().instance_type)
    else:
        allocations = sliver.get_capacities()

    # Extract Core, Ram, Disk Hours
    requested_resources[("Core", unit)] = requested_resources.get(("Core", unit), 0) + (duration * allocations.core)
    requested_resources[("RAM", unit)] = requested_resources.get(("Core", unit), 0) + (duration * allocations.ram)
    requested_resources[("Disk", unit)] = requested_resources.get(("Core", unit), 0) + (duration * allocations.disk)

    # Extract component hours (e.g., GPU, FPGA, SmartNIC)
    if sliver.attached_components_info:
        for c in sliver.attached_components_info.devices.values():
            component_type = str(c.get_type())
            requested_resources[(component_type, unit)] = (
                requested_resources.get((component_type, unit), 0) + duration
            )

    return requested_resources


def enforce_quota_limits(quota_lookup: dict, computed_reservations: list[LeaseReservationAvro],
                         duration: float) -> Tuple[bool, str]:
    """
    Check if the requested resources for multiple reservations are within the project's quota limits.

    @param quota_lookup: Quota Limits for various resource types.
    @param computed_reservations: List of slivers requested.
    @param duration: Number of hours the reservations are requested for.
    @return: Tuple (True, None) if resources are within quota, or (False, message) if denied.
    @throws: Exception if there is an error during the database interaction.
    """
    try:
        requested_resources = {}

        # Accumulate resource usage for all reservations
        for r in computed_reservations:
            sliver = r.get_sliver()
            sliver_resources = extract_quota_usage(sliver, duration)
            for key, value in sliver_resources.items():
                requested_resources[key] = requested_resources.get(key, 0) + value

        # Check each accumulated resource usage against its quota
        for quota_key, total_requested_duration in requested_resources.items():
            if quota_key not in quota_lookup:
                return False, f"Quota not defined for resource: {quota_key[0]} ({quota_key[1]})."

            quota_info = quota_lookup[quota_key]
            available_quota = quota_info["quota_limit"] - quota_info["quota_used"]

            if total_requested_duration > available_quota:
                return False, (
                    f"Requested {total_requested_duration} {quota_key[1]} of {quota_key[0]}, "
                    f"but only {available_quota} is available."
                )

        # If all checks pass
        return True, None
    except Exception as e:
        raise Exception(f"Error while checking reservation: {str(e)}")
