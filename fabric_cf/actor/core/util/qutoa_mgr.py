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
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro

from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin


class QuotaMgr:
    def __init__(self, *, core_api_host: str):
        self.core_api_host = core_api_host

    def update_quota(self, reservation: ABCReservationMixin):
        print("Update Quota")
        try:
            slice_object = reservation.get_slice()
            if not slice_object:
                return
            project_id = slice_object.get_project_id()
            if not project_id:
                return

            sliver = None
            from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
            if isinstance(reservation, ReservationClient) and reservation.get_leased_resources() and \
                    reservation.get_leased_resources().get_sliver():
                sliver = reservation.get_leased_resources().get_sliver()
            if not sliver and reservation.get_resources() and reservation.get_resources().get_sliver():
                sliver = reservation.get_resources().get_sliver()

            if not sliver:
                return

            if reservation.is_closed() or reservation.is_closing():
                duration = reservation.get_term().get_remaining_length()
            else:
                duration = reservation.get_term().get_length()

            if duration < 60:
                return

            duration /= 3600000
            existing_quota = self.db.get_quota_lookup(project_id=project_id)

            sliver_quota_usage = self.extract_quota_usage(sliver=sliver, duration=duration)

            print(f"Existing: {existing_quota}")
            print(f"Updated by: {sliver_quota_usage}")

            # Check each accumulated resource usage against its quota
            for quota_key, total_duration in sliver_quota_usage.items():
                print(f"Iteration: {quota_key}")
                current_duration = 0
                if quota_key in existing_quota:
                    current_duration = existing_quota.get(quota_key)
                (resource_type, resource_unit) = quota_key

                # Return resource hours for a sliver deleted before expiry
                if reservation.is_closing() or reservation.is_closed():
                    usage = current_duration["quota_used"] - total_duration
                    if usage < 0:
                        usage = 0
                    self.db.update_quota(project_id=project_id,
                                         resource_type=resource_type,
                                         resource_unit=resource_unit, quota_used=usage)
                # Account for resource hours used for a new or extended sliver
                else:
                    usage = total_duration + current_duration["quota_used"]
                    self.db.update_quota(project_id=project_id,
                                         resource_type=resource_type,
                                         resource_unit=resource_unit, quota_used=usage)
        finally:
            if self.lock.locked():
                self.lock.release()

    def extract_quota_usage(self, sliver, duration: float) -> dict[tuple[str, str], float]:
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

    def enforce_quota_limits(self, quota_lookup: dict, computed_reservations: list[LeaseReservationAvro],
                             duration: float) -> tuple[bool, str]:
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
                sliver_resources = self.extract_quota_usage(sliver, duration)
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
