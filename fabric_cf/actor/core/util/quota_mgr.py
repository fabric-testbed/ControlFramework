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
import logging

from fabric_cf.actor.core.time.term import Term
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabrictestbed.external_api.core_api import CoreApi
from fabrictestbed.slice_editor import InstanceCatalog
from fim.slivers.network_node import NodeSliver

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType


class QuotaMgr:
    def __init__(self, *, core_api_host: str, token: str, logger: logging.Logger):
        self.core_api = CoreApi(core_api_host=core_api_host, token=token)
        self.logger = logger

    def list_quotas(self, project_uuid: str, offset: int = 0, limit: int = 200) -> dict[tuple[str, str], dict]:
        quota_list = self.core_api.list_quotas(project_uuid=project_uuid, offset=offset, limit=limit)
        quotas = {}
        for q in quota_list:
            quotas[(q.get("resource_type").lower(), q.get("resource_unit").lower())] = q
        return quotas

    def update_quota(self, reservation: ABCReservationMixin, term: Term):
        try:
            slice_object = reservation.get_slice()
            if not slice_object:
                return
            project_id = slice_object.get_project_id()
            if not project_id:
                return

            sliver = InventoryForType.get_allocated_sliver(reservation=reservation)
            '''
            from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
            if isinstance(reservation, ReservationClient) and reservation.get_leased_resources() and \
                    reservation.get_leased_resources().get_sliver():
                sliver = reservation.get_leased_resources().get_sliver()
            if not sliver and reservation.get_resources() and reservation.get_resources().get_sliver():
                sliver = reservation.get_resources().get_sliver()
            '''

            if not sliver:
                return

            if reservation.is_closed() or reservation.is_closing():
                duration = term.get_remaining_length()
            else:
                duration = term.get_length()

            if duration < 60:
                return

            duration /= 3600000
            existing_quotas = self.list_quotas(project_uuid=project_id)

            sliver_quota_usage = self.extract_quota_usage(sliver=sliver, duration=duration)

            self.logger.debug(f"Existing: {existing_quotas}")
            self.logger.debug(f"Updated by: {sliver_quota_usage}")

            # Check each accumulated resource usage against its quota
            for quota_key, total_duration in sliver_quota_usage.items():
                existing = existing_quotas.get(quota_key)
                self.logger.debug(f"Quota update requested for: prj:{project_id} quota_key:{quota_key}: quota: {existing}")
                if not existing:
                    self.logger.debug("Existing not found so skipping!")
                    continue

                # Return resource hours for a sliver deleted before expiry
                if reservation.is_closing() or reservation.is_closed():
                    usage = existing.get("quota_used") - total_duration
                    if usage < 0:
                        usage = 0
                # Account for resource hours used for a new or extended sliver
                else:
                    usage = total_duration + existing.get("quota_used")

                self.core_api.update_quota(uuid=existing.get("uuid"), project_uuid=project_id,
                                           resource_type=existing.get("resource_type"),
                                           resource_unit=existing.get("resource_unit"),
                                           quota_used=usage, quota_limit=existing.get("quota_limit"))
        except Exception as e:
            self.logger.error(f"Failed to update Quota: {e}")
        finally:
            self.logger.debug("done")

    @staticmethod
    def extract_quota_usage(sliver, duration: float) -> dict[tuple[str, str], float]:
        """
        Extract quota usage from a sliver

        @param sliver: The sliver object from which resources are extracted.
        @param duration: Number of hours the resources are requested for.
        @return: A dictionary of resource type/unit tuples to requested amounts.
        """
        unit = "HOURS".lower()
        requested_resources = {}

        # Check if the sliver is a NodeSliver
        if not isinstance(sliver, NodeSliver):
            return requested_resources

        allocations = sliver.get_capacity_allocations()
        if not allocations and sliver.get_capacity_hints():
            catalog = InstanceCatalog()
            allocations = catalog.get_instance_capacities(instance_type=
                                                          sliver.get_capacity_hints().instance_type)
        else:
            allocations = sliver.get_capacities()

        # Extract Core, Ram, Disk Hours
        requested_resources[("core", unit)] = requested_resources.get(("core", unit), 0) + \
                                              (duration * allocations.core)
        requested_resources[("ram", unit)] = requested_resources.get(("ram", unit), 0) +\
                                             (duration * allocations.ram)
        requested_resources[("disk", unit)] = requested_resources.get(("disk", unit), 0) + \
                                              (duration * allocations.disk)

        # Extract component hours (e.g., GPU, FPGA, SmartNIC)
        if sliver.attached_components_info:
            for c in sliver.attached_components_info.devices.values():
                component_type = str(c.get_type()).lower()
                requested_resources[(component_type, unit)] = (
                    requested_resources.get((component_type, unit), 0) + duration
                )

        return requested_resources

    def enforce_quota_limits(self, quotas: dict, computed_reservations: list[LeaseReservationAvro],
                             duration: float) -> tuple[bool, str]:
        """
        Check if the requested resources for multiple reservations are within the project's quota limits.

        @param quotas: Quota Limits for various resource types.
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
                sliver_resources = self.extract_quota_usage(sliver, duration)
                for key, value in sliver_resources.items():
                    requested_resources[key] = requested_resources.get(key, 0) + value

            # Check each accumulated resource usage against its quota
            for quota_key, total_requested_duration in requested_resources.items():
                if quota_key not in quotas:
                    return False, f"Quota not defined for resource: {quota_key[0]} ({quota_key[1]})."

                quota_info = quotas[quota_key]
                available_quota = quota_info["quota_limit"] - quota_info["quota_used"]

                if total_requested_duration > available_quota:
                    return False, (
                        f"Requested {total_requested_duration} {quota_key[1]} of {quota_key[0]}, "
                        f"but only {available_quota} is available."
                    )

            # If all checks pass
            return True, None
        except Exception as e:
            self.logger.error(f"Error while checking reservation: {str(e)}")
