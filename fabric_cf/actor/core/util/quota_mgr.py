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
import re
import threading
from typing import Any

from fabrictestbed.external_api.core_api import CoreApi
from fabrictestbed.slice_editor import InstanceCatalog
from fim.slivers.network_node import NodeSliver
from fim.user import NodeType

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType


class QuotaMgr:
    """
    Manages resource quotas for projects, including listing, updating, and enforcing limits.
    """
    def __init__(self, *, core_api_host: str, token: str, logger: logging.Logger):
        """
        Initialize the Quota Manager.

        @param core_api_host: The API host for core services.
        @param token: Authentication token for API access.
        @param logger: Logger instance for logging messages.
        """
        self.core_api = CoreApi(core_api_host=core_api_host, token=token)
        self.logger = logger
        self.lock = threading.Lock()

    def list_quotas(self, project_uuid: str, offset: int = 0, limit: int = 200) -> dict[tuple[str, str], dict]:
        """
        Retrieve the list of quotas for a given project.

        @param project_uuid: Unique identifier for the project.
        @param offset: Pagination offset for results (default: 0).
        @param limit: Maximum number of quotas to fetch (default: 200).
        @return: A dictionary mapping resource type/unit pairs to their quota details.
        """
        quota_list = self.core_api.list_quotas(project_uuid=project_uuid, offset=offset, limit=limit)
        quotas = {}
        for q in quota_list:
            quotas[(q.get("resource_type").get("name").lower(), q.get("resource_unit").lower())] = q
        return quotas

    def update_quota(self, reservation: ABCReservationMixin, duration: float):
        """
        Update the quota usage for a given reservation.

        @param reservation: Reservation object containing resource usage details.
        @param duration: Duration in milliseconds for which the reservation was held.
        """
        try:
            with self.lock:  # Locking critical section
                self.logger.debug("Acquired lock for quota update.")

                slice_object = reservation.get_slice()
                if not slice_object:
                    return
                project_id = slice_object.get_project_id()
                if not project_id:
                    return

                sliver = InventoryForType.get_allocated_sliver(reservation=reservation)
                if not sliver:
                    return

                if duration < 60:
                    return

                existing_quotas = self.list_quotas(project_uuid=project_id)

                sliver_quota_usage = self.extract_quota_usage(sliver=sliver, duration=duration)

                self.logger.debug(f"Existing: {existing_quotas}")
                self.logger.debug(f"Updated by: {sliver_quota_usage}")

                # Check each accumulated resource usage against its quota
                for quota_key, total_duration in sliver_quota_usage.items():
                    existing = existing_quotas.get(quota_key)
                    if not existing:
                        self.logger.debug("Existing not found, skipping!")
                        continue

                    # Return resource hours for a sliver deleted before expiry
                    usage = -total_duration if reservation.is_closing() or reservation.is_closed() else total_duration

                    self.core_api.update_quota_usage(uuid=existing.get("uuid"), project_uuid=project_id,
                                                     quota_used=usage)

        except Exception as e:
            self.logger.error(f"Failed to update Quota: {e}")
        finally:
            self.logger.debug("Released lock for quota update.")

    @staticmethod
    def __massage_name(name: str) -> str:
        """
        Massage to make it python friendly
        :param name:
        :return:
        """
        return re.sub(r'[ -]', '_', name)

    @staticmethod
    def extract_quota_usage(sliver: NodeSliver, duration: float) -> dict[tuple[str, str], float]:
        """
        Extract resource usage details from a given sliver.

        @param sliver: The sliver object representing allocated resources.
        @param duration: Duration in milliseconds for which resources are requested.
        @return: A dictionary mapping resource type/unit pairs to usage amounts.
        """
        unit = "HOURS".lower()
        requested_resources = {}

        duration /= 3600000

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

        if allocations:
            # Extract Core, Ram, Disk Hours
            requested_resources[("core", unit)] = requested_resources.get(("core", unit), 0) + \
                                                  (duration * allocations.core)
            requested_resources[("ram", unit)] = requested_resources.get(("ram", unit), 0) +\
                                                 (duration * allocations.ram)
            requested_resources[("disk", unit)] = requested_resources.get(("disk", unit), 0) + \
                                                  (duration * allocations.disk)

        if sliver.get_type() == NodeType.Switch and allocations:
            requested_resources["p4", unit] = requested_resources.get(("p4", unit), 0) + \
                                                  (duration * allocations.unit)

        # Extract component hours (e.g., GPU, FPGA, SmartNIC)
        if sliver.attached_components_info:
            for c in sliver.attached_components_info.devices.values():
                type_model_name = '_'.join([QuotaMgr.__massage_name(str(c.get_type())),
                                            QuotaMgr.__massage_name(str(c.get_model()))])
                requested_resources[(type_model_name.lower(), unit)] = (
                    requested_resources.get((type_model_name.lower(), unit), 0) + duration
                )

        return requested_resources

    def enforce_quota_limits(self, reservation: ABCReservationMixin, duration: float) -> tuple[bool, Any]:
        """
        Verify whether a reservation's requested resources fit within the project's quota limits.

        @param reservation: The reservation to check against available quotas.
        @param duration: Duration in milliseconds for the reservation request.
        @return: Tuple (True, None) if within limits, (False, message) if quota is exceeded.
        @throws: Exception if an error occurs during database interaction.
        """
        try:
            slice_object = reservation.get_slice()
            if not slice_object:
                return False, None
            project_uuid = slice_object.get_project_id()
            if not project_uuid:
                return False, None

            sliver = InventoryForType.get_allocated_sliver(reservation=reservation)

            if not sliver and reservation.is_ticketing() and reservation.get_requested_resources():
                sliver = reservation.get_requested_resources().get_sliver()

            if not sliver:
                self.logger.info("No sliver found!")
                return False, None

            sliver_resources = self.extract_quota_usage(sliver, duration)
            quotas = self.list_quotas(project_uuid=project_uuid)

            # Check each accumulated resource usage against its quota
            for quota_key, total_requested_duration in sliver_resources.items():
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
        return False, None
