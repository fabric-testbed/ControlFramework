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

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.resource_pool_descriptor import ResourcePoolDescriptor
from fabric.actor.core.core.unit_set import UnitSet
from fabric.actor.core.policy.resource_control import ResourceControl
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.ipv4_set import IPv4Set
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.core.unit import Unit, UnitState

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_client_reservation import IClientReservation
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
    from fabric.actor.core.apis.i_reservation import IReservation


class Vmm:
    def __init__(self, host: Unit, capacity: int):
        if host is None:
            raise Exception("host cannot be None")
        if capacity < 1:
            raise Exception("capacity must be at least 1")
        self._host = host
        self.hosted = set()
        self.capacity = capacity

    def __str__(self):
        return "{} {}".format(self._host, self.capacity)

    def __eq__(self, other):
        if not isinstance(other, Vmm):
            return False
        return self._host.get_id() == other._host.get_id()

    def __hash__(self):
        return self._host.get_id().__hash__()

    def release(self, vm: Unit):
        if vm is None:
            raise Exception("vm cannot be None")

        if vm not in self.hosted:
            raise Exception("the specified node is not hosted on this vmm")

        self.hosted.remove(vm)

    def host(self, vm: Unit):
        if vm is None:
            raise Exception("vm cannot be None")

        if vm in self.hosted:
            raise Exception("the specified node is already hosted on this vmm")

        self.hosted.add(vm)

    def get_hosted_count(self) -> int:
        return len(self.hosted)

    def get_capacity(self) -> int:
        return self.capacity

    def get_available(self) -> int:
        return self.capacity - len(self.hosted)

    def get_host(self) -> Unit:
        return self._host

    def get_hosted_vms(self) -> set:
        return self.hosted.copy()


class VmmPool:
    def __init__(self, rtype: ResourceType, properties: dict):
        self.rtype = rtype
        self.properties = properties
        self.vmms = {}
        self.memory = 0
        self.cpu = 0
        self.bandwidth = 0
        self.disk = 0
        self.capacity = 0

    def donate(self, vm: Vmm):
        if vm is None:
            raise Exception("vm cannot be None")

        if vm.get_host().get_actor_id() in self.vmms:
            raise Exception("the specified vm already in the pool")

        self.vmms[vm.get_host().get_id()] = vm


    def get_vmm_set(self) -> list:
        result = []
        for v in self.vmms.values():
            result.append(v)
        return result

    def get_vmm(self, uid: ID) -> Vmm:
        if uid in self.vmms:
            return self.vmms[uid]
        return None

    def get_vmms_count(self) -> int:
        return len(self.vmms)

    def get_properties(self):
        return self.properties

    def get_memory(self) -> int:
        return self.memory

    def set_memory(self, memory: int):
        self.memory = memory

    def get_cpu(self) -> int:
        return self.cpu

    def set_cpu(self, cpu: int):
        self.cpu = cpu

    def get_bandwidth(self) -> int:
        return self.bandwidth

    def set_bandwidth(self, bandwidth: int):
        self.bandwidth = bandwidth

    def get_disk(self) -> int:
        return self.disk

    def set_disk(self, disk: int):
        self.disk = disk

    def get_capacity(self) -> int:
        return self.capacity

    def set_capacity(self, capacity: int):
        self.capacity = capacity

    def get_type(self) -> ResourceType:
        return self.rtype


class VMControl(ResourceControl):
    PropertyCapacity = "capacity"
    PropertyIPList = "ip.list"
    PropertyIPSubnet = "ip.subnet"
    PropertyIPGateway = "ip.gateway"
    PropertyDataSubnet = "data.subnet"

    def __init__(self):
        super().__init__()
        self.inventory = {}
        self.ipset = IPv4Set()
        self.subnet = None
        self.gateway = None
        self.use_ip_set = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['authority']
        del state['logger']
        del state['initialized']

        del state['inventory']
        del state['ipset']
        del state['subnet']
        del state['gateway']
        del state['use_ip_set']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

        self.authority = None
        self.logger = None
        self.initialized = False

        self.inventory = {}
        self.ipset = IPv4Set()
        self.subnet = None
        self.gateway = None
        self.use_ip_set = False

    def donate_reservation(self, reservation: IClientReservation):
        return

    def donate(self, resource_set: ResourceSet):
        rtype = resource_set.get_type()
        resource = resource_set.get_resource_properties()
        local = resource_set.get_local_properties()

        pool = self.inventory.get(rtype)
        if pool is None:
            pool = VmmPool(rtype, resource)
            rd = ResourcePoolDescriptor()
            rd.reset(resource, None)
            memory = int(rd.get_attribute(Constants.ResourceMemory).get_value())
            capacity = 0
            if self.PropertyCapacity in local:
                capacity = int(local[self.PropertyCapacity])
            if self.PropertyIPSubnet in local:
                self.subnet = local[self.PropertyIPSubnet]
            if self.PropertyIPGateway in local:
                self.gateway = local[self.PropertyIPGateway]

            if self.PropertyIPList in local:
                temp = local[self.PropertyIPList]
                self.ipset.add(temp)
                self.use_ip_set = True

            pool.set_memory(memory)
            pool.set_capacity(capacity)
            self.inventory[rtype] = pool

        unit_set = resource_set.get_resources()
        for unit in unit_set.get_set().values():
            vmm = Vmm(unit, pool.get_capacity())
            pool.donate(vmm)

    def assign(self, reservation: IAuthorityReservation) -> ResourceSet:
        reservation.set_send_with_deficit(True)

        if len(self.inventory) == 0:
            raise Exception("no inventory")

        requested = reservation.get_requested_resources()
        request_properties = requested.get_request_properties()
        rtype = requested.get_type()
        current = reservation.get_resources()
        ticket = requested.get_resources()

        gained = None
        lost = None

        if current is None:
            pool = self.inventory.get(rtype)
            if pool is None:
                raise Exception("no resources of the specified pool")

            needed = ticket.get_units()
            gained = self.get_vms(pool, needed)

            if gained is None or gained.get_units() == 0:
                self.logger.warning("Could not allocate any units for r: {}".format(reservation.get_reservation_id()))
                return None
        else:
            rtype = current.get_type()
            pool = self.inventory.get(rtype)
            current_units = current.get_units()
            difference = ticket.get_units() - current_units

            if difference > 0:
                gained = self.get_vms(pool, difference)
            elif difference < 0:
                unit_set = current.get_resources()
                victims = request_properties[Constants.ConfigVictims]
                to_take = unit_set.select_extract(-difference, victims)
                lost = UnitSet(self.authority.get_plugin(), to_take)

        return ResourceSet(gained=gained, lost=lost, rtype=rtype)

    def get_vms(self, pool: VmmPool, needed: int) -> UnitSet:
        uset = UnitSet(self.authority.get_plugin())
        vmms = pool.get_vmm_set()
        allocated = 0

        for vmm in vmms:
            available = vmm.get_available()
            if self.use_ip_set:
                available = min(available, self.ipset.get_free_count())

            if available > 0:
                to_allocate = min(available, needed - allocated)
                for i in range(to_allocate):
                    vm = Unit(id=ID())
                    vm.set_resource_type(pool.get_type())
                    vm.set_parent_id(vmm.get_host().get_id())
                    vm.set_property(Constants.UnitParentHostName, vmm.get_host().get_property(Constants.UnitHostName))
                    vm.set_property(Constants.UnitControl, vmm.get_host().get_property(Constants.UnitControl))
                    vm.set_property(Constants.UnitMemory, str(pool.get_memory()))
                    if self.use_ip_set:
                        vm.set_property(Constants.UnitManagementIP, self.ipset.allocate())

                    if self.subnet is not None:
                        vm.set_property(Constants.UnitManageSubnet, self.subnet)

                    if self.gateway is not None:
                        vm.set_property(Constants.UnitManageGateway, self.gateway)

                    vmm.host(vm)
                    uset.add_unit(vm)
                allocated += to_allocate
            if allocated == needed or self.ipset.get_free_count() == 0:
                break

        return uset

    def free(self, uset: dict):
        if uset is not None:
            for u in uset.values():
                try:
                    rtype = u.get_resource_type()
                    pool = self.inventory.get(rtype)
                    host = u.get_parent_id()
                    vmm = pool.get_vmm(host)
                    vmm.release(u)
                    if self.use_ip_set:
                        self.ipset.free(u.get_property(Constants.UnitManagementIP))
                    self.logger.debug("Released unit: {}".format(e))
                except Exception as e:
                    self.logger.error("Failed to release unit: {} exception:{}".format(u, e))

    def revisit(self, reservation: IReservation):
        unit_set = reservation.get_resources().get_resources()
        for u in unit_set.get_set().values:
            try:
                if u.get_state() == UnitState.DEFAULT or \
                        u.get_state() == UnitState.FAILED or \
                        u.get_state() == UnitState.CLOSING or \
                        u.get_state() == UnitState.PRIMING or \
                        u.get_state() == UnitState.ACTIVE or \
                        u.get_state() == UnitState.MODIFYING:
                    rtype = u.get_resource_type()
                    pool = self.inventory.get(rtype)
                    id = u.get_parent_id()
                    vmm = pool.get_vmm(id)
                    vmm.host(u)
                    self.logger.debug("VMControl.revisit(); recovering management IP {}".format(u.get_property(Constants.UnitManagementIP)))
                    self.ipset.reserve(u.get_property(Constants.UnitManagementIP))
            except Exception as e:
                self.fail(u, "revisit with vmcontrol", e)

    def recovery_starting(self):
        self.logger.info("Beginning VMControl recovery")

    def recovery_ended(self):
        self.logger.info("Completing VMControl recovery")
        self.logger.debug("Restored VMControl with subnet {} gateway {} ipset {} and inventory {}"
                          .format(self.subnet, self.gateway, self.ipset, self.inventory))
