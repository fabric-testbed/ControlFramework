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
from typing import List

from fim.slivers.base_sliver import BaseSliver
from fim.slivers.network_service import NetworkServiceSliver
from fim.slivers.topology_diff import WhatsModifiedFlag

from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.policy.resource_control import ResourceControl
from fabric_cf.actor.core.util.resource_type import ResourceType


class NetworkServiceControl(ResourceControl):
    """
    Resource Control for Network Service
    """
    @staticmethod
    def __has_sliver_changed(current: NetworkServiceSliver, requested: NetworkServiceSliver):
        diff = current.diff(other_sliver=requested)
        if diff is None:
            return False

        if (diff.added.interfaces is not None and len(diff.added.interfaces)) or \
                (diff.removed.interfaces is not None and len(diff.removed.interfaces)) or \
                (diff.modified.interfaces is not None and len(diff.modified.interfaces)):
            return True

        if diff.modified is not None and diff.modified.services is not None:
            for new_ns, flag in diff.modified.services:
                if flag & WhatsModifiedFlag.LABELS or flag & WhatsModifiedFlag.CAPACITIES:
                    return True
        return False

    def assign(self, *, reservation: ABCAuthorityReservation, delegation_name: str,
               graph_node: BaseSliver, existing_reservations: List[ABCReservationMixin]) -> ResourceSet:
        """
        Assign a reservation
        :param reservation: reservation
        :param delegation_name: Name of delegation serving the request
        :param graph_node: ARM Graph Node serving the reservation
        :param existing_reservations: Existing Reservations served by the same ARM node
        :return: ResourceSet with updated sliver annotated with properties
        :raises: AuthorityException in case the request cannot be satisfied
        """
        reservation.set_send_with_deficit(value=True)

        requested = reservation.get_requested_resources().get_sliver()
        self.__dump_sliver(sliver=requested)
        if not isinstance(requested, NetworkServiceSliver):
            raise AuthorityException(f"Invalid resource type {requested.get_type()}")

        current = reservation.get_resources()

        resource_type = ResourceType(resource_type=str(requested.get_type()))

        gained = None
        modified = None
        lost = None
        if current is None:
            self.logger.debug("check if sliver can be provisioned")
            # FIXME Add validation to check ticketed sliver against ARM
            # Refer Network Node Control for reference

            self.logger.debug(f"Slice properties: {reservation.get_slice().get_config_properties()}")
            unit = Unit(rid=reservation.get_reservation_id(), slice_id=reservation.get_slice_id(),
                        actor_id=self.authority.get_guid(), sliver=requested, rtype=resource_type,
                        properties=reservation.get_slice().get_config_properties())
            gained = UnitSet(plugin=self.authority.get_plugin(), units={unit.reservation_id: unit})
        else:
            current_sliver = current.get_sliver()
            if self.__has_sliver_changed(current=current_sliver, requested=requested):
                unit = Unit(rid=reservation.get_reservation_id(), slice_id=reservation.get_slice_id(),
                            actor_id=self.authority.get_guid(), sliver=requested, rtype=resource_type,
                            properties=reservation.get_slice().get_config_properties())
                modified = UnitSet(plugin=self.authority.get_plugin(), units={unit.reservation_id: unit})
            else:
                return current

        result = ResourceSet(gained=gained, modified=modified, lost=lost, rtype=resource_type)
        result.set_sliver(sliver=requested)
        return result

    def revisit(self, *, reservation: ABCReservationMixin):
        return

    def free(self, *, uset: dict):
        if uset is not None:
            for u in uset.values():
                self.logger.debug(f"Freeing 1 unit {u}")

    def __dump_sliver(self, *, sliver: NetworkServiceSliver):
        self.logger.debug(f"Network Service Sliver# {sliver}")
        if sliver.interface_info is not None:
            for ifs in sliver.interface_info.interfaces.values():
                self.logger.debug(f"Ifs# {ifs}")