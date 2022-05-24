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
import ipaddress
import traceback
from ipaddress import IPv6Network, IPv4Network
from typing import List

from fim.slivers.capacities_labels import Labels
from fim.slivers.gateway import Gateway
from fim.slivers.interface_info import InterfaceSliver, InterfaceType
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver, NSLayer, ServiceType

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID


class NetworkServiceInventory(InventoryForType):
    @staticmethod
    def __extract_vlan_range(*, labels: Labels) -> List[int] or None:
        vlan_range = None
        if labels is None:
            return vlan_range
        if labels.vlan_range is not None:
            vlans = labels.vlan_range.split("-")
            vlan_range = list(range(int(vlans[0]), int(vlans[1])))
        elif labels.vlan is not None:
            vlan_range = [int(labels.vlan)]
        return vlan_range

    def __exclude_allocated_vlans(self, *, available_vlan_range: List[int], bqm_ifs: InterfaceSliver,
                                  existing_reservations: List[ABCReservationMixin]) -> List[int]:
        # Exclude the already allocated VLANs and subnets
        if existing_reservations is None:
            return available_vlan_range

        for reservation in existing_reservations:
            # For Active or Ticketed or Ticketing reservations; reduce the counts from available
            allocated_sliver = None
            if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                allocated_sliver = reservation.get_approved_resources().get_sliver()

            if (reservation.is_active() or reservation.is_ticketed()) and \
                    reservation.get_resources() is not None:
                allocated_sliver = reservation.get_resources().get_sliver()

            self.logger.debug(
                f"Existing res# {reservation.get_reservation_id()} state:{reservation.get_state()} "
                f"allocated: {allocated_sliver}")

            if allocated_sliver is None:
                continue

            if allocated_sliver.interface_info is None:
                continue

            for allocated_ifs in allocated_sliver.interface_info.interfaces.values():
                # Ignore the Interface Slivers not on the same port
                if allocated_ifs.get_node_map()[1] != bqm_ifs.node_id:
                    continue

                self.logger.debug(f"Excluding already allocated VLAN: "
                                  f"{allocated_ifs.label_allocations.vlan} to "
                                  f"res# {reservation.get_reservation_id()}")

                # Exclude VLANs on the allocated on the same port
                if allocated_ifs.label_allocations.vlan is not None:
                    allocated_vlan = int(allocated_ifs.label_allocations.vlan)

                    if allocated_vlan in available_vlan_range:
                        available_vlan_range.remove(allocated_vlan)

        return available_vlan_range

    def allocate_ifs(self, *, requested_ns: NetworkServiceSliver, requested_ifs: InterfaceSliver,
                     owner_switch: NodeSliver, mpls_ns: NetworkServiceSliver, bqm_ifs: InterfaceSliver,
                     existing_reservations: List[ABCReservationMixin]) -> InterfaceSliver:
        """
        Allocate Interface Sliver
        - For L2 services, validate the VLAN tag specified is within the allowed range
        - For L3 services,
            - grab the VLAN from BQM Site specific NetworkService
            - exclude the VLAN already assigned to other Interface Sliver on the same port
            - allocate the first available VLAN to the Interface Sliver
        :param requested_ns: Requested NetworkService
        :param requested_ifs: Requested Interface Sliver
        :param owner_switch: BQM Owner site switch identified to serve the InterfaceSliver
        :param mpls_ns: BQM MPLS NetworkService identified to serve the InterfaceSliver
        :param bqm_ifs: BQM InterfaceSliver identified to serve the InterfaceSliver
        :param existing_reservations: Existing Reservations which also are served by the owner switch
        :return Interface Sliver updated with the allocated VLAN tag for FABNetv4 and FABNetv6 services
        :raises Exception if vlan tag range is not in the valid range for L2 services
        Return the sliver updated with the VLAN
        """
        if requested_ns.get_layer() == NSLayer.L2:
            requested_vlan = None
            if requested_ifs.labels is not None and requested_ifs.labels.vlan is not None:
                requested_vlan = int(requested_ifs.labels.vlan)

            if requested_vlan is None:
                return requested_ifs

            # Validate the requested VLAN is in range specified on MPLS Network Service in BQM
            # Only do this for Non FacilityPorts
            if bqm_ifs.get_type() != InterfaceType.FacilityPort:
                if mpls_ns.get_label_delegations() is None:
                    if 1 > requested_vlan > 4095:
                        raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                              msg=f"Vlan for L2 service {requested_vlan} "
                                                  f"is outside the allowed range 1-4095")
                    else:
                        return requested_ifs

                delegation_id, delegated_label = self._get_delegations(lab_cap_delegations=mpls_ns.get_label_delegations())
                vlan_range = self.__extract_vlan_range(labels=delegated_label)

                if vlan_range is not None and requested_vlan not in vlan_range:
                    raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                          msg=f"Vlan for L2 service {requested_vlan} is outside the allowed range "
                                              f"{vlan_range}")

            # Validate the VLANs
            vlan_range = self.__extract_vlan_range(labels=bqm_ifs.labels)
            if vlan_range is not None:
                vlan_range = self.__exclude_allocated_vlans(available_vlan_range=vlan_range, bqm_ifs=bqm_ifs,
                                                            existing_reservations=existing_reservations)

                if requested_vlan not in vlan_range:
                    raise BrokerException(error_code=ExceptionErrorCode.FAILURE,
                                          msg=f"Vlan for L2 service {requested_vlan} is outside the available range "
                                              f"{vlan_range}")

        else:
            for ns in owner_switch.network_service_info.network_services.values():
                if requested_ns.get_type() == ns.get_type():
                    # Grab Label Delegations
                    delegation_id, delegated_label = self._get_delegations(
                        lab_cap_delegations=ns.get_label_delegations())

                    # Get the VLAN range
                    vlan_range = self.__extract_vlan_range(labels=delegated_label)
                    vlan_range = self.__exclude_allocated_vlans(available_vlan_range=vlan_range, bqm_ifs=bqm_ifs,
                                                                existing_reservations=existing_reservations)

                    # Allocate the first available VLAN
                    if vlan_range is not None:
                        requested_ifs.labels.vlan = str(vlan_range[0])
                        requested_ifs.label_allocations = Labels(vlan=str(vlan_range[0]))
                    break
        return requested_ifs

    def __allocate_ip_address_to_ifs(self, *, requested_ns: NetworkServiceSliver) -> NetworkServiceSliver:
        if requested_ns.gateway is None:
            return requested_ns

        if requested_ns.get_type() == ServiceType.FABNetv4:
            start_ip_str = requested_ns.gateway.lab.ipv4
        else:
            start_ip_str = requested_ns.gateway.lab.ipv6

        start_ip = ipaddress.ip_address(start_ip_str)
        start_ip += 1

        for ifs in requested_ns.interface_info.interfaces.values():
            if requested_ns.get_type() == ServiceType.FABNetv4:
                ifs.labels.ipv4 = str(start_ip)
                ifs.label_allocations.ipv4 = str(start_ip)

            elif requested_ns.get_type() == ServiceType.FABNetv6:
                ifs.labels.ipv6 = str(start_ip)
                ifs.label_allocations.ipv6 = str(start_ip)
            self.logger.info("Allocated IP address to interface %s", ifs)
            start_ip += 1

        return requested_ns

    def allocate(self, *, rid: ID, requested_ns: NetworkServiceSliver, owner_switch: NodeSliver,
                 existing_reservations: List[ABCReservationMixin]) -> NetworkServiceSliver:
        """
        Allocate Network Service Sliver (Only for L3 Service)
            - grab the /17 or /48 from BQM Site specific NetworkService
            - divide it into /24 or /64 subnets
            - exclude the 1st subnet (reserved for control plane)
            - exclude the subnets already assigned to other V4/V6 NetworkService on the same owner switch
            - allocate the first available subnet to the NetworkService
        :param rid: Reservation ID
        :param requested_ns: Requested NetworkService
        :param owner_switch: BQM Owner site switch identified to serve the NetworkService
        :param existing_reservations: Existing Reservations which also are served by the owner switch
        :return NetworkService updated with the allocated subnet for FABNetv4 and FABNetv6 services
        Return the sliver updated with the subnet
        """
        try:
            if requested_ns.get_type() != ServiceType.FABNetv4 and requested_ns.get_type() != ServiceType.FABNetv6:
                return requested_ns

            for ns in owner_switch.network_service_info.network_services.values():
                if requested_ns.get_type() != ns.get_type():
                    continue

                # Grab Label Delegations
                delegation_id, delegated_label = self._get_delegations(lab_cap_delegations=ns.get_label_delegations())

                subnet_list = None
                # Get Subnet
                if ns.get_type() == ServiceType.FABNetv6:
                    ip_network = IPv6Network(delegated_label.ipv6_subnet)
                    subnet_list = list(ip_network.subnets(new_prefix=64))

                elif ns.get_type() == ServiceType.FABNetv4:
                    ip_network = IPv4Network(delegated_label.ipv4_subnet)
                    subnet_list = list(ip_network.subnets(new_prefix=24))

                # Exclude the 1st subnet as it is reserved for control plane
                subnet_list.pop(0)

                # Exclude the already allocated subnets
                for reservation in existing_reservations:
                    if rid == reservation.get_reservation_id():
                        continue
                    # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                    allocated_sliver = None
                    if reservation.is_ticketing() and reservation.get_approved_resources() is not None:
                        allocated_sliver = reservation.get_approved_resources().get_sliver()

                    if (reservation.is_active() or reservation.is_ticketed()) and \
                            reservation.get_resources() is not None:
                        allocated_sliver = reservation.get_resources().get_sliver()

                    self.logger.debug(f"Existing res# {reservation.get_reservation_id()} "
                                      f"allocated: {allocated_sliver}")

                    if allocated_sliver is None:
                        continue

                    if allocated_sliver.get_type() != requested_ns.get_type():
                        continue

                    if allocated_sliver.get_type() == ServiceType.FABNetv4:
                        subnet_to_remove = IPv4Network(allocated_sliver.get_gateway().lab.ipv4_subnet)
                        subnet_list.remove(subnet_to_remove)
                        self.logger.debug(
                            f"Excluding already allocated IP4Subnet: "
                            f"{allocated_sliver.get_gateway().lab.ipv4_subnet}"
                            f" to res# {reservation.get_reservation_id()}")

                    elif allocated_sliver.get_gateway().lab.ipv6_subnet is not None:
                        subnet_to_remove = IPv6Network(allocated_sliver.get_gateway().lab.ipv6_subnet)
                        subnet_list.remove(subnet_to_remove)
                        self.logger.debug(
                            f"Excluding already allocated IPv6Subnet: "
                            f"{allocated_sliver.get_gateway().lab.ipv6_subnet}"
                            f" to res# {reservation.get_reservation_id()}")

                gateway_labels = Labels()
                if requested_ns.get_type() == ServiceType.FABNetv4:
                    gateway_labels.ipv4_subnet = subnet_list[0].with_prefixlen
                    gateway_labels.ipv4 = str(next(subnet_list[0].hosts()))

                elif requested_ns.get_type() == ServiceType.FABNetv6:
                    gateway_labels.ipv6_subnet = subnet_list[0].with_prefixlen
                    gateway_labels.ipv6 = str(next(subnet_list[0].hosts()))

                requested_ns.gateway = Gateway(lab=gateway_labels)
                break
            # Allocate the IP Addresses for the requested NS
            requested_ns = self.__allocate_ip_address_to_ifs(requested_ns=requested_ns)
        except Exception as e:
            self.logger.error(f"Error in allocate_gateway_for_ns: {e}")
            self.logger.error(traceback.format_exc())
        return requested_ns

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        pass
