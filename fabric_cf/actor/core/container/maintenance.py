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
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Tuple, Union

from fim.slivers.maintenance_mode import MaintenanceEntry, MaintenanceInfo, MaintenanceState

from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.common.constants import Constants


class Site:
    def __init__(self, *, name: str, maint_info: MaintenanceInfo):
        """
        Represents a Site in maintenance
        """
        self.name = name
        self.maintenance_info = maint_info
        if self.maintenance_info is not None:
            self.maintenance_info.finalize()
        # Contains allowed projects/users
        self.properties = {}

    def get_name(self) -> str:
        return self.name

    def get_maintenance_info(self) -> MaintenanceInfo:
        return self.maintenance_info

    def set_properties(self, *, properties: dict):
        self.properties = properties

    def get_properties(self) -> dict:
        return self.properties

    def is_in_maintenance(self) -> bool:
        if self.maintenance_info is None:
            return False
        return Maintenance.is_maintenance_blocking(entry=self.maintenance_info.get(self.name))

    def get_state(self) -> MaintenanceState:
        site_info = self.maintenance_info.get(self.name)
        if site_info is not None:
            return site_info.state
        else:
            self.maintenance_info.finalize()
            for name, entry in self.maintenance_info.iter():
                return entry.state

    def get_blocked_vlans(self, *, worker: str = None) -> Set[str]:
        """
        VLANs that must not be handed out for Shared NIC allocations on this site, typically
        because stale switch configuration would make provisioning fail. Sourced from the site
        properties: 'blocked-vlans' applies site wide, 'blocked-vlans.<worker>' to a single
        worker; both hold comma separated VLAN tags and are combined.
        @param worker worker (host) name; only the site wide VLANs are returned when omitted
        @return set of blocked VLAN tags as strings; empty set when none are blocked
        """
        result = set()
        if not self.properties:
            return result
        keys = [Constants.BLOCKED_VLANS]
        if worker is not None:
            keys.append(f"{Constants.BLOCKED_VLANS}.{worker}")
        for key in keys:
            value = self.properties.get(key)
            if value:
                result.update(v.strip() for v in str(value).split(",") if v.strip())
        return result

    def is_worker_in_maintenance(self, *, worker: str) -> bool:
        if self.maintenance_info is None:
            return False
        entry = Maintenance.worker_maintenance_entry(maintenance_info=self.maintenance_info,
                                                     site_name=self.name, worker_name=worker)
        return Maintenance.is_maintenance_blocking(entry=entry)

    def __str__(self):
        return f"Name: {self.name} MaintInfo: {self.maintenance_info} Properties: {self.properties}"

    def clone_maintenance_info(self) -> Union[MaintenanceInfo or None]:
        if self.maintenance_info is not None:
            return self.maintenance_info.copy()
        return None

    def update_maintenance_info(self, maint_info: MaintenanceInfo):
        self.maintenance_info = maint_info


class Maintenance:
    @staticmethod
    def derive_site_entry(*, site_name: str, maint_info: MaintenanceInfo) -> MaintenanceEntry:
        """
        Derive the site level maintenance entry from the worker level entries
        - PartMaint if at least one worker is in Maint or in PreMaint whose deadline has become current
        - Active otherwise; a worker in PreMaint with a future deadline leaves the site Active so that
          provisioning is only restricted on the specific worker, not the entire site
        @param site_name site name
        @param maint_info maintenance information containing the worker entries
        @return site level maintenance entry
        """
        site_state = MaintenanceState.Active
        now = datetime.now(timezone.utc)
        for name, entry in maint_info.list_details():
            if name == site_name:
                continue
            if entry.state == MaintenanceState.Maint or entry.state == MaintenanceState.PartMaint or \
                    (entry.state == MaintenanceState.PreMaint and entry.deadline is not None and
                     entry.deadline <= now):
                site_state = MaintenanceState.PartMaint
                break
        return MaintenanceEntry(state=site_state)

    @staticmethod
    def is_maintenance_blocking(*, entry: MaintenanceEntry) -> bool:
        """
        Determine whether a maintenance entry prevents new provisioning:
        Maint always blocks, PreMaint blocks once its deadline has passed.
        @param entry Maintenance entry to inspect
        @return True if provisioning is blocked; False otherwise
        """
        if entry is None or entry.state is None:
            return False
        if entry.state == MaintenanceState.Maint:
            return True
        if entry.state == MaintenanceState.PreMaint and entry.deadline is not None:
            deadline = entry.deadline
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return deadline <= datetime.now(timezone.utc)
        return False

    # Relative severity of the maintenance states, used to pick between a site and a worker entry
    __STATE_SEVERITY = {MaintenanceState.Active: 0,
                        MaintenanceState.Unknown: 1,
                        MaintenanceState.PartMaint: 2,
                        MaintenanceState.PreMaint: 3,
                        MaintenanceState.Maint: 4}

    @classmethod
    def __entry_severity(cls, entry: MaintenanceEntry) -> Optional[tuple]:
        """
        Sort key ranking how restrictive a maintenance entry is: entries that already block
        provisioning outrank those that do not, then the state itself, then the deadline (an
        earlier deadline is more restrictive; entries without one rank last).
        """
        if entry is None or entry.state is None:
            return None

        deadline = entry.deadline
        if deadline is not None and deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)

        return (cls.is_maintenance_blocking(entry=entry),
                cls.__STATE_SEVERITY.get(entry.state, cls.__STATE_SEVERITY[MaintenanceState.Unknown]),
                -deadline.timestamp() if deadline is not None else float('-inf'))

    @classmethod
    def worker_maintenance_entry(cls, *, maintenance_info: MaintenanceInfo, site_name: str,
                                 worker_name: str) -> Optional[MaintenanceEntry]:
        """
        Determine the effective maintenance entry for a worker.

        Maintenance is tracked per site, with the site's MaintenanceInfo holding one entry keyed
        by the site name (site level maintenance) and one entry per worker in maintenance. Both
        can apply to a worker at once - a site scheduled for maintenance next week does not stop a
        worker from being in maintenance right now - so the more restrictive of the two wins.

        @param maintenance_info Maintenance information for the site
        @param site_name Site name
        @param worker_name Worker (host) name
        @return Effective MaintenanceEntry or None if unknown
        """
        if maintenance_info is None:
            return None

        site_entry = maintenance_info.get(site_name)
        worker_entry = maintenance_info.get(worker_name)

        # A site level PartMaint entry is an aggregate derived from the worker states (some
        # workers are in maintenance); it does not restrict the other workers on the site
        if site_entry is not None and site_entry.state == MaintenanceState.PartMaint:
            site_entry = MaintenanceEntry(state=MaintenanceState.Active)

        site_severity = cls.__entry_severity(site_entry)
        worker_severity = cls.__entry_severity(worker_entry)

        if worker_severity is not None and (site_severity is None or worker_severity > site_severity):
            return worker_entry

        return site_entry

    @classmethod
    def effective_site_maintenance_info(cls, *, site_name: str,
                                        maint_info: MaintenanceInfo = None) -> MaintenanceInfo:
        """
        Build the maintenance information to report for a site, guaranteeing a site level entry.

        The stored site level entry is derived at update time; re-derive it at read time unless
        maintenance was set explicitly at the site level (Maint/PreMaint), so that a PreMaint
        worker whose deadline has passed since the last update is reported as PartMaint.

        @param site_name Site name
        @param maint_info Stored maintenance information for the site; None if the site is unknown
        @return finalized MaintenanceInfo always containing a site level entry
        """
        if maint_info is not None:
            result = maint_info.copy()
        else:
            result = MaintenanceInfo()
        site_entry = result.get(site_name)
        if site_entry is None or site_entry.state in (MaintenanceState.Active, MaintenanceState.PartMaint):
            if site_entry is not None:
                result.rem(site_name)
            result.add(site_name, cls.derive_site_entry(site_name=site_name, maint_info=result))
        result.finalize()
        return result

    @staticmethod
    def merge_properties(*, existing: Dict[str, str], updates: Dict[str, str]) -> Dict[str, str]:
        """
        Merge a properties update into the stored site properties. Only the keys present in the
        update are touched, so e.g. setting blocked VLANs does not drop the allowed projects/users
        or the blocked VLANs of another worker; an empty value removes the key.
        @param existing stored site properties
        @param updates properties passed with the maintenance request
        @return merged properties
        """
        result = dict(existing) if existing else {}
        for key, value in updates.items():
            if value is None or str(value).strip() == "":
                result.pop(key, None)
            else:
                result[key] = value
        return result

    @staticmethod
    def update_maintenance_mode(*, database: ABCDatabase, properties: Dict[str, str], sites: List[Site] = None):
        """
        Update Maintenance Mode at Testbed/Site/Worker Level
        - Tesbed level Maintenance - single Site object is passed with Name = ALL
        - Site level Maintenance - single Site object per site is passed with Name = SiteName
        - Worker level Maintenance - single Site object per site with one entry per worker;
          a site level entry is always maintained and derived from the worker level states
        Site level and worker level updates are merged - a site level update preserves the
        worker level entries and a worker level update preserves an explicitly set site level
        maintenance (Maint/PreMaint)
        Properties (allowed projects/users, blocked VLANs) are merged per key - keys absent from
        the update are preserved and an empty value removes the key
        @param database database
        @param properties properties containing project ids/user emails/blocked VLANs
        @param sites Maintenance information for the sites
        """
        for s in sites:
            # Set the list of allowed projects/users at the site level
            if properties is not None:
                s.set_properties(properties=Maintenance.merge_properties(existing=s.get_properties(),
                                                                          updates=properties))

            # Get Current Maintenance mode for the Site
            existing_site = database.get_site(site_name=s.get_name())
            # Site entry exists
            if existing_site is not None:
                # Update the Properties {project id/user email/blocked VLAN information)
                if properties is not None:
                    existing_site.set_properties(
                        properties=Maintenance.merge_properties(existing=existing_site.get_properties(),
                                                                updates=properties))
                # Site level Maintenance Update - preserve the worker level entries
                if s.get_maintenance_info().get(s.get_name()) is not None:
                    new_maint_info = existing_site.clone_maintenance_info()
                    if new_maint_info.get(s.get_name()):
                        new_maint_info.rem(s.get_name())
                    site_entry = s.get_maintenance_info().get(s.get_name())
                    # Taking the site out of maintenance falls back to the state derived from
                    # the workers still in maintenance
                    if site_entry.state == MaintenanceState.Active:
                        site_entry = Maintenance.derive_site_entry(site_name=s.get_name(),
                                                                   maint_info=new_maint_info)
                    new_maint_info.add(s.get_name(), site_entry)
                    new_maint_info.finalize()
                    existing_site.update_maintenance_info(maint_info=new_maint_info)
                    database.update_site(site=existing_site)
                # Worker level Maintenance Update - preserve an explicit site level maintenance
                else:
                    new_maint_info = existing_site.clone_maintenance_info()
                    existing_site_entry = new_maint_info.get(s.get_name())
                    if existing_site_entry is not None:
                        new_maint_info.rem(s.get_name())
                    for worker_name, entry in s.get_maintenance_info().list_details():
                        # Remove existing entry
                        if new_maint_info.get(worker_name):
                            new_maint_info.rem(worker_name)

                        # Add worker entry using the new information only if worker is in Maintenance
                        if entry.state != MaintenanceState.Active:
                            new_maint_info.add(worker_name, entry)

                    # Always maintain a site level entry: keep an explicitly set site level
                    # maintenance (Maint/PreMaint), otherwise derive it from the worker level states
                    if existing_site_entry is not None and existing_site_entry.state in (
                            MaintenanceState.Maint, MaintenanceState.PreMaint):
                        site_entry = existing_site_entry
                    else:
                        site_entry = Maintenance.derive_site_entry(site_name=s.get_name(),
                                                                   maint_info=new_maint_info)
                    new_maint_info.add(s.get_name(), site_entry)
                    new_maint_info.finalize()
                    existing_site.update_maintenance_info(maint_info=new_maint_info)
                    database.update_site(site=existing_site)
            # Adding Maintenance State First Time
            else:
                # Worker level Maintenance - add a site level entry derived from the worker level states
                if s.get_maintenance_info().get(s.get_name()) is None:
                    new_maint_info = s.clone_maintenance_info()
                    for worker_name, entry in new_maint_info.list_details():
                        # Keep worker entries only if worker is in Maintenance
                        if entry.state == MaintenanceState.Active:
                            new_maint_info.rem(worker_name)
                    site_entry = Maintenance.derive_site_entry(site_name=s.get_name(), maint_info=new_maint_info)
                    new_maint_info.add(s.get_name(), site_entry)
                    new_maint_info.finalize()
                    s.update_maintenance_info(maint_info=new_maint_info)
                database.add_site(site=s)

    @staticmethod
    def is_testbed_in_maintenance(*, database: ABCDatabase) -> Tuple[bool, Dict[str, str] or None]:
        test_bed = database.get_site(site_name=Constants.ALL)
        if test_bed is not None:
            return test_bed.is_in_maintenance(), test_bed.get_properties()

        return False, None

    @staticmethod
    def is_site_in_maintenance(*, database: ABCDatabase, site_name: str) -> Tuple[bool, Site or None]:
        site = database.get_site(site_name=site_name)
        if site is None:
            return False, None

        return site.is_in_maintenance(), site

    @staticmethod
    def is_sliver_provisioning_allowed(*, database: ABCDatabase, project: str, email: str, site: str,
                                       worker: str) -> Tuple[bool, str or None]:
        """
        Determine if sliver can be provisioned
        Sliver provisioning can be prohibited if Testbed or Site or Worker is in maintenance mode
        Sliver provisioning in maintenance mode may be allowed for specific projects/users
        @param database database
        @param project project
        @param email user's email
        @param site site name
        @param worker worker name
        @return True if allowed; False otherwise
        """
        status, site = Maintenance.is_site_in_maintenance(database=database, site_name=site)

        if not status and site is None:
            return True, None

        projects = site.get_properties().get(Constants.PROJECT_ID)
        users = site.get_properties().get(Constants.USERS)

        if project is not None and projects is not None and project in projects:
            return True, None

        if email is not None and users is not None and email in users:
            return True, None

        if status:
            return False, f"Site {site.get_name()} in {site.get_state()}"

        if worker is not None and site.is_worker_in_maintenance(worker=worker):
            return False, f"Worker {worker} on {site.get_name()} in {site.get_state()}"

        return True, None

    @staticmethod
    def is_slice_provisioning_allowed(*, database: ABCDatabase, project: str, email: str) -> bool:
        """
        Determine if slice can be provisioned
        Slice provisioning can be prohibited if Testbed is in maintenance mode
        Slice provisioning in maintenance mode may be allowed for specific projects/users
        @param database database
        @param project project
        @param email user's email
        @return True if allowed; False otherwise
        """

        status, properties = Maintenance.is_testbed_in_maintenance(database=database)

        if not status:
            return True

        users = properties.get(Constants.USERS)
        projects = properties.get(Constants.PROJECT_ID)

        if project is not None and projects is not None and project in projects:
            return True

        if users is not None and email is not None and email in users:
            return True

        return False
