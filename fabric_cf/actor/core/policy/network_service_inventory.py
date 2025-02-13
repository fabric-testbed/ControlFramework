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
import random
import traceback
from ipaddress import IPv6Network, IPv4Network
from typing import List, Tuple, Union

from fabric_cf.actor.fim.fim_helper import FimHelper
from fim.slivers.capacities_labels import Labels
from fim.slivers.gateway import Gateway
from fim.slivers.interface_info import InterfaceSliver, InterfaceType
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver, NSLayer, ServiceType

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import BrokerException, ExceptionErrorCode
from fabric_cf.actor.core.kernel.reservation_states import ReservationOperation
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_cf.actor.core.util.id import ID


class NetworkServiceInventory(InventoryForType):
    @staticmethod
    def __extract_vlan_range(*, labels: Labels) -> List[int] or None:
        vlan_range = None
        if labels is None:
            return vlan_range
        if labels.vlan_range is not None:
            if isinstance(labels.vlan_range, list):
                vlan_range = []
                for v_r in labels.vlan_range:
                    vlans = v_r.split("-")
                    for x in list(range(int(vlans[0]), int(vlans[1]) + 1)):
                        if x not in vlan_range:
                            vlan_range.append(x)
            else:
                vlans = labels.vlan_range.split("-")
                vlan_range = list(range(int(vlans[0]), int(vlans[1]) + 1))
        elif labels.vlan is not None:
            vlan_range = [int(labels.vlan)]
        return vlan_range

    def __exclude_allocated_vlans(self, *, rid: ID, available_vlan_range: List[int], bqm_ifs: InterfaceSliver,
                                  existing_reservations: List[ABCReservationMixin]) -> List[int]:
        # Exclude the already allocated VLANs and subnets
        if existing_reservations is None:
            return available_vlan_range

        for reservation in existing_reservations:
            if rid == reservation.get_reservation_id():
                continue

            # For Active or Ticketed or Ticketing reservations; reduce the counts from available
            allocated_sliver = self.get_allocated_sliver(reservation=reservation)

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

        if available_vlan_range is None or len(available_vlan_range) == 0:
            raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                  msg=f"No VLANs available!")
        return available_vlan_range

    def allocate_ifs(self, *, rid: ID, requested_ns: NetworkServiceSliver, requested_ifs: InterfaceSliver,
                     owner_ns: NetworkServiceSliver, bqm_ifs: InterfaceSliver,
                     existing_reservations: List[ABCReservationMixin],
                     operation: ReservationOperation = ReservationOperation.Create) -> InterfaceSliver:
        """
        Allocate Interface Sliver
        - For L2 services, validate the VLAN tag specified is within the allowed range
        - For L3 services,
            - grab the VLAN from BQM Site specific NetworkService
            - exclude the VLAN already assigned to other Interface Sliver on the same port
            - allocate the first available VLAN to the Interface Sliver
        :param rid: Reservation ID
        :param requested_ns: Requested NetworkService
        :param requested_ifs: Requested Interface Sliver
        :param owner_ns: BQM NetworkService identified to serve the InterfaceSliver
        :param bqm_ifs: BQM InterfaceSliver identified to serve the InterfaceSliver
        :param existing_reservations: Existing Reservations which also are served by the owner switch
        :param operation: Extend/Create/Modify Operation
        :return Interface Sliver updated with the allocated VLAN tag for FABNetv4 and FABNetv6 services
        :raises Exception if vlan tag range is not in the valid range for L2 services
        """
        requested_vlan = None
        if requested_ifs.labels and requested_ifs.labels.vlan:
            requested_vlan = int(requested_ifs.labels.vlan)

        if requested_ns.get_layer() == NSLayer.L2:
            # Validate the requested VLAN is in range specified on MPLS Network Service in BQM
            # Only do this for Non FacilityPorts
            if bqm_ifs.get_type() != InterfaceType.FacilityPort:
                if requested_vlan is None:
                    return requested_ifs

                if owner_ns.get_label_delegations() is None:
                    if not (1 <= requested_vlan <= 4095):
                        raise BrokerException(
                            error_code=ExceptionErrorCode.FAILURE,
                            msg=f"Vlan for L2 service {requested_vlan} is outside the allowed range 1-4095"
                        )
                    return requested_ifs

                delegation_id, delegated_label = FimHelper.get_delegations(delegations=owner_ns.get_label_delegations())
                vlan_range = self.__extract_vlan_range(labels=delegated_label)

                if vlan_range and requested_vlan not in vlan_range:
                    raise BrokerException(
                        error_code=ExceptionErrorCode.FAILURE,
                        msg=f"Vlan for L2 service {requested_vlan} is outside the available range {vlan_range}"
                    )

            vlan_range = self.__extract_vlan_range(labels=bqm_ifs.labels)
            if vlan_range:
                vlan_range = self.__exclude_allocated_vlans(rid=rid, available_vlan_range=vlan_range,
                                                            bqm_ifs=bqm_ifs,
                                                            existing_reservations=existing_reservations)

                if operation == ReservationOperation.Extend:
                    if requested_vlan and requested_vlan not in vlan_range:
                        raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                              msg=f"Renew failed: VLAN {requested_vlan} for Interface : "
                                                  f"{requested_ifs.get_name()/bqm_ifs.node_id} already in "
                                                  f"use by another reservation")
                    return requested_ifs

                if requested_vlan is None:
                    requested_ifs.labels.vlan = str(random.choice(vlan_range))
                    return requested_ifs

                if requested_vlan not in vlan_range:
                    raise BrokerException(
                        error_code=ExceptionErrorCode.FAILURE,
                        msg=f"Vlan for L2 service {requested_vlan} is outside the available range {vlan_range}"
                    )
        else:
            if bqm_ifs.get_type() != InterfaceType.FacilityPort:
                delegation_id, delegated_label = FimHelper.get_delegations(
                    delegations=owner_ns.get_label_delegations())
                vlan_range = self.__extract_vlan_range(labels=delegated_label)
            else:
                vlan_range = self.__extract_vlan_range(labels=bqm_ifs.labels)

            if vlan_range:
                vlan_range = self.__exclude_allocated_vlans(rid=rid, available_vlan_range=vlan_range,
                                                            bqm_ifs=bqm_ifs,
                                                            existing_reservations=existing_reservations)

                if operation == ReservationOperation.Extend:
                    if requested_vlan and requested_vlan not in vlan_range:
                        raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                              msg=f"Renew failed: VLAN {requested_vlan} for Interface : "
                                                  f"{requested_ifs.get_name()}/{bqm_ifs.node_id} already in "
                                                  f"use by another reservation")
                    return requested_ifs

                if bqm_ifs.get_type() != InterfaceType.FacilityPort:
                    requested_ifs.labels.vlan = str(random.choice(vlan_range))
                    requested_ifs.label_allocations = Labels(vlan=requested_ifs.labels.vlan)
                else:
                    if not requested_ifs.labels:
                        return requested_ifs

                    if requested_ifs.labels.vlan is None:
                        requested_ifs.labels.vlan = str(random.choice(vlan_range))

                    if int(requested_ifs.labels.vlan) not in vlan_range:
                        raise BrokerException(
                            error_code=ExceptionErrorCode.FAILURE,
                            msg=f"Vlan for L3 service {requested_ifs.labels.vlan} is outside the "
                                f"available range {vlan_range}"
                        )

        return requested_ifs

    def __allocate_ip_address_to_ifs(self, *, requested_ns: NetworkServiceSliver) -> NetworkServiceSliver:
        """
        Allocate IP addresses to the interfaces of the requested network service sliver.

        :param requested_ns: The requested network service sliver.
        :return: The updated network service sliver with allocated IP addresses.
        """
        if requested_ns.gateway is None:
            return requested_ns

        if requested_ns.get_type() in Constants.L3_FABNET_EXT_SERVICES:
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

    def allocate_vnic(self, *, rid: ID, requested_ns: NetworkServiceSliver, owner_ns: NetworkServiceSliver,
                      existing_reservations: List[ABCReservationMixin]) -> NetworkServiceSliver:
        """
        Allocate Network Service Sliver (Only for L2Bridge Service for OpenStackvNIC)
        :param rid: Reservation ID
        :param requested_ns: Requested NetworkService
        :param owner_ns: BQM Network Service Sliver
        :param existing_reservations: Existing Reservations which also are served by the owner switch
        :return NetworkService updated with the allocated vlan
        """
        try:
            if requested_ns.get_type() != ServiceType.L2Bridge:
                return requested_ns

            vlans_range = self.__extract_vlan_range(labels=owner_ns.labels)

            # Exclude the already allocated VLANs
            for reservation in existing_reservations:
                if rid == reservation.get_reservation_id():
                    continue

                # For Active or Ticketed or Ticketing reservations; reduce the counts from available
                allocated_sliver = self.get_allocated_sliver(reservation=reservation)

                self.logger.debug(f"Existing res# {reservation.get_reservation_id()} "
                                  f"allocated: {allocated_sliver}")

                if allocated_sliver is None:
                    continue

                if allocated_sliver.get_type() != requested_ns.get_type():
                    continue

                if allocated_sliver.label_allocations is not None and allocated_sliver.label_allocations.vlan is not None:
                    vlans_range.remove(int(allocated_sliver.label_allocations.vlan))

            if requested_ns.label_allocations is None:
                requested_ns.label_allocations = Labels()
            #requested_ns.label_allocations.vlan = str(random.choice(vlans_range))
            requested_ns.label_allocations.vlan = str(vlans_range[0])
        except Exception as e:
            self.logger.error(f"Error in allocate_vNIC: {e}")
            self.logger.error(traceback.format_exc())
            raise BrokerException(msg=f"Allocation failure for Openstack VNIC: {e}")
        return requested_ns

    def allocate(self, *, rid: ID, requested_ns: NetworkServiceSliver, owner_ns: NetworkServiceSliver,
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
        :param owner_ns: BQM Network Service identified to serve the NetworkService
        :param existing_reservations: Existing Reservations which also are served by the owner switch
        :return: NetworkService updated with the allocated subnet for FABNetv4 and FABNetv6 services
        """
        try:
            if requested_ns.get_type() not in Constants.L3_SERVICES:
                return requested_ns

            # Grab Label Delegations
            delegation_id, delegated_label = FimHelper.get_delegations(delegations=owner_ns.get_label_delegations())

            # HACK to use FabNetv6 for FabNetv6Ext as both have the same range
            requested_ns_type = requested_ns.get_type()
            if requested_ns_type == ServiceType.FABNetv6Ext:
                requested_ns_type = ServiceType.FABNetv6

            # Handle L3VPN type specifically
            if requested_ns_type == ServiceType.L3VPN:
                if requested_ns.labels is not None:
                    requested_ns.labels = Labels.update(requested_ns.labels, asn=delegated_label.asn)
                else:
                    requested_ns.labels = Labels(asn=delegated_label.asn)
                return requested_ns

            ip_network, subnet_list = self._generate_subnet_list(owner_ns=owner_ns, delegated_label=delegated_label)

            # Exclude the already allocated subnets
            subnet_list = self._exclude_allocated_subnets(subnet_list=subnet_list, requested_ns_type=requested_ns_type,
                                                          rid=rid, existing_reservations=existing_reservations)

            # Extend Case
            if requested_ns.get_node_map():
                self._can_extend(subnet_list=subnet_list, requested_ns=requested_ns)
                return requested_ns

            gateway_labels = self._assign_gateway_labels(ip_network=ip_network, subnet_list=subnet_list,
                                                         requested_ns=requested_ns)

            self.logger.debug(f"Gateway Labels: {gateway_labels}")

            requested_ns.gateway = Gateway(lab=gateway_labels)

            # Allocate the IP Addresses for the requested NS
            requested_ns = self.__allocate_ip_address_to_ifs(requested_ns=requested_ns)
        except BrokerException as e:
            raise e
        except Exception as e:
            self.logger.error(f"Error in allocate_gateway_for_ns: {e}")
            self.logger.error(traceback.format_exc())
            raise BrokerException(msg=f"Allocation failure for Requested Network Service: {e}")

        return requested_ns

    def _generate_subnet_list(self, *, owner_ns: NetworkServiceSliver,
                              delegated_label: Labels) -> Tuple[Union[IPv4Network, IPv6Network], List]:
        """
        Generate the list of subnets based on the owner network service type.

        :param owner_ns: The NetworkServiceSliver representing the owner network service.
        :param delegated_label: The Labels object containing the delegated subnet information.
        :return: A tuple containing the IP network and the list of generated subnets.
        """
        subnet_list = None
        ip_network = None
        if owner_ns.get_type() in Constants.L3_FABNETv6_SERVICES:
            ip_network = IPv6Network(delegated_label.ipv6_subnet)
            subnet_list = list(ip_network.subnets(new_prefix=64))
            # Exclude the 1st subnet as it is reserved for control plane
            subnet_list.pop(0)
            # Exclude the last subnet for FABRIC STAR Bastion Host Allocation
            subnet_list.pop(-1)

        elif owner_ns.get_type() in [ServiceType.FABNetv4, ServiceType.FABNetv4Ext]:
            ip_network = IPv4Network(delegated_label.ipv4_subnet)
            if owner_ns.get_type() == ServiceType.FABNetv4:
                subnet_list = list(ip_network.subnets(new_prefix=24))
                subnet_list.pop(0)

            elif owner_ns.get_type() == ServiceType.FABNetv4Ext:
                subnet_list = list(ip_network.hosts())

        self.logger.debug(f"Available Subnets: {subnet_list}")

        return ip_network, subnet_list

    def _exclude_allocated_subnets(self, *, subnet_list: List, requested_ns_type: str, rid: ID,
                                   existing_reservations: List[ABCReservationMixin]) -> List:
        """
        Exclude the subnets that are already allocated.

        :param subnet_list: A list of available subnets to be allocated.
        :param requested_ns_type: The type of the requested network service.
        :param rid: The reservation ID of the current request.
        :param existing_reservations: A list of existing reservations that may contain allocated subnets.
        :return: A list of subnets excluding those that have already been allocated.
        """
        for reservation in existing_reservations:
            if rid == reservation.get_reservation_id():
                continue

            allocated_sliver = self.get_allocated_sliver(reservation)
            if allocated_sliver is None:
                continue

            # HACK to use FabNetv6 for FabNetv6Ext as both have the same range
            # Needs to be removed if FabNetv6/FabNetv6Ext are configured with different ranges
            allocated_sliver_type = allocated_sliver.get_type()
            if allocated_sliver_type == ServiceType.FABNetv6Ext:
                allocated_sliver_type = ServiceType.FABNetv6
            # HACK End

            if allocated_sliver_type != requested_ns_type:
                continue

            if allocated_sliver.get_type() == ServiceType.FABNetv4:
                subnet_to_remove = IPv4Network(allocated_sliver.get_gateway().subnet)
                self.logger.debug(
                    f"Excluding already allocated IP4Subnet: "
                    f"{allocated_sliver.get_gateway().subnet}"
                    f" to res# {reservation.get_reservation_id()}")
                if subnet_to_remove in subnet_list:
                    subnet_list.remove(subnet_to_remove)

            elif allocated_sliver.get_type() == ServiceType.FABNetv4Ext:
                if allocated_sliver.labels is not None and allocated_sliver.labels.ipv4 is not None:
                    for x in allocated_sliver.labels.ipv4:
                        subnet_to_remove = ipaddress.IPv4Address(x)
                        self.logger.debug(
                            f"Excluding already allocated IP4: "
                            f"{x}"
                            f" to res# {reservation.get_reservation_id()}")
                        if subnet_to_remove in subnet_list:
                            subnet_list.remove(subnet_to_remove)

            elif allocated_sliver.get_type() in Constants.L3_FABNETv6_SERVICES:
                subnet_to_remove = IPv6Network(allocated_sliver.get_gateway().subnet)
                self.logger.debug(
                    f"Excluding already allocated IP6Subnet: "
                    f"{allocated_sliver.get_gateway().subnet}"
                    f" to res# {reservation.get_reservation_id()}")

                if subnet_to_remove in subnet_list:
                    subnet_list.remove(subnet_to_remove)

            self.logger.debug(f"Excluding already allocated subnet for reservation {reservation.get_reservation_id()}")

        return subnet_list

    def _assign_gateway_labels(self, *, ip_network: Union[IPv4Network, IPv6Network], subnet_list: List,
                               requested_ns: NetworkServiceSliver) -> Labels:
        """
        Assign gateway labels based on the requested network service type.

        :param ip_network: The IP network from which subnets are derived, either IPv4Network or IPv6Network.
        :param subnet_list: A list of subnets derived from the ip_network.
        :param requested_ns: Network Service sliver.
        :return: Gateway labels populated with the appropriate subnet and IP address.
        """
        if len(subnet_list) == 0:
            raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                  msg=f"No subnets available for {requested_ns.get_site()}")
        gateway_labels = Labels()
        if requested_ns.get_type() == ServiceType.FABNetv4:
            # Allocate the requested network if available else allocate new network
            if requested_ns.gateway and requested_ns.gateway.lab and requested_ns.gateway.lab.ipv4_subnet:
                requested_subnet = IPv4Network(requested_ns.gateway.lab.ipv4_subnet)
                if requested_subnet in subnet_list:
                    gateway_labels.ipv4_subnet = requested_subnet.with_prefixlen
                    gateway_labels.ipv4 = str(next(requested_subnet.hosts()))
                    return gateway_labels

            gateway_labels.ipv4_subnet = subnet_list[0].with_prefixlen
            gateway_labels.ipv4 = str(list(subnet_list[0].hosts())[0])

        elif requested_ns.get_type() == ServiceType.FABNetv4Ext:
            gateway_labels.ipv4_subnet = ip_network.with_prefixlen
            gateway_labels.ipv4 = str(subnet_list[0])

        elif requested_ns.get_type() in Constants.L3_FABNETv6_SERVICES:
            # Allocate the requested network if available else allocate new network
            if requested_ns.gateway and requested_ns.gateway.lab and requested_ns.gateway.lab.ipv6_subnet:
                requested_subnet = IPv6Network(requested_ns.gateway.lab.ipv6_subnet)
                if requested_subnet in subnet_list:
                    gateway_labels.ipv6_subnet = requested_subnet.with_prefixlen
                    gateway_labels.ipv6 = str(next(requested_subnet.hosts()))
                    return gateway_labels

            gateway_labels.ipv6_subnet = subnet_list[0].with_prefixlen
            gateway_labels.ipv6 = str(next(subnet_list[0].hosts()))

        self.logger.debug(f"Allocated Gateway Labels for Network Service: {gateway_labels}")

        return gateway_labels

    def _can_extend(self, *, subnet_list: List, requested_ns: NetworkServiceSliver):
        if requested_ns.get_type() == ServiceType.FABNetv4:
            allocated_subnet = ipaddress.IPv4Network(requested_ns.gateway.subnet)
            if allocated_subnet not in subnet_list:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Renew failed: Subnet {requested_ns.gateway.subnet} for Network Service : "
                                          f"{requested_ns.get_type()} already in use by another reservation")
        elif requested_ns.get_type() == ServiceType.FABNetv4Ext:
            if requested_ns.gateway.gateway in subnet_list:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Renew failed: Subnet {requested_ns.gateway.subnet} for Network Service : "
                                          f"{requested_ns.get_type()} already in use by another reservation")

        elif requested_ns.get_type() in Constants.L3_FABNETv6_SERVICES:
            allocated_subnet = ipaddress.IPv6Network(requested_ns.gateway.subnet)
            if allocated_subnet not in subnet_list:
                raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                      msg=f"Renew failed: Subnet {requested_ns.gateway.subnet} for Network Service : "
                                          f"{requested_ns.get_type()} already in use by another reservation")

    def free(self, *, count: int, request: dict = None, resource: dict = None) -> dict:
        pass

    def allocate_peered_ifs(self, *, rid: ID, owner_switch: NodeSliver,
                            requested_ifs: InterfaceSliver, bqm_interface: InterfaceSliver,
                            existing_reservations: List[ABCReservationMixin]) -> InterfaceSliver:
        """
        Update Labels for a Peered Interface
        @param
        """
        ifs_labels = requested_ifs.get_labels()
        if ifs_labels is None:
            ifs_labels = Labels()

        if owner_switch.get_name() == Constants.AL2S:
            delegation_id, delegated_label = FimHelper.get_delegations(delegations=bqm_interface.get_label_delegations())
            local_name = delegated_label.local_name
            device_name = delegated_label.device_name
        else:
            local_name = bqm_interface.get_name()
            device_name = owner_switch.get_name()

        ifs_labels = Labels.update(ifs_labels, local_name=local_name, device_name=device_name)

        if bqm_interface.labels.vlan_range is not None:
            vlan_range = self.__extract_vlan_range(labels=bqm_interface.labels)
            available_vlans = self.__exclude_allocated_vlans(rid=rid, available_vlan_range=vlan_range, bqm_ifs=bqm_interface,
                                                             existing_reservations=existing_reservations)

            # Extend case
            if requested_ifs.get_node_map() and requested_ifs.labels and requested_ifs.labels.vlan:
                if int(requested_ifs.labels.vlan) in available_vlans:
                    raise BrokerException(error_code=ExceptionErrorCode.INSUFFICIENT_RESOURCES,
                                          msg=f"Renew failed: VLAN {requested_ifs.labels.vlan} for Interface : "
                                              f"{requested_ifs.get_name()} already in use by another reservation")

            vlan = str(random.choice(available_vlans))
            #vlan = str(available_vlans[0])
            ifs_labels = Labels.update(ifs_labels, vlan=vlan)

        requested_ifs.labels = ifs_labels
        requested_ifs.label_allocations = Labels.update(lab=ifs_labels)

        return requested_ifs
