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
from typing import List, Dict, Tuple, Union

from fim.slivers.maintenance_mode import MaintenanceInfo, MaintenanceState

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
        if self.maintenance_info is not None:
            now = datetime.now(timezone.utc)
            site_info = self.maintenance_info.get(self.name)
            if site_info is not None and (site_info.state == MaintenanceState.Maint or
                                         (site_info.state == MaintenanceState.PreMaint and site_info.deadline <= now)):
                return True
        return False

    def get_state(self) -> MaintenanceState:
        site_info = self.maintenance_info.get(self.name)
        if site_info is not None:
            return site_info.state
        else:
            self.maintenance_info.finalize()
            for name, entry in self.maintenance_info.iter():
                return entry.state

    def is_worker_in_maintenance(self, *, worker: str) -> bool:
        # Whole site is in Maintenance
        if self.is_in_maintenance():
            return True

        # Check if the specific worker is in Maintenance
        entry = self.maintenance_info.get(name=worker)
        now = datetime.now(timezone.utc)

        if entry is not None and (entry.state == MaintenanceState.Maint or
                                  (entry.state == MaintenanceState.PreMaint and entry.deadline <= now)):
            return True

        return False

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
    def update_maintenance_mode(*, database: ABCDatabase, properties: Dict[str, str], sites: List[Site] = None):
        """
        Update Maintenance Mode at Testbed/Site/Worker Level
        - Tesbed level Maintenance - single Site object is passed with Name = ALL
        - Site level Maintenance - single Site object per site is passed with Name = SiteName
        - Worker level Maintenance - single Site object per site with one entry per worker
        @param database database
        @param properties properties container project ids/ user emails
        @param sites Maintenance information for the sites
        """
        for s in sites:
            # Set the list of allowed projects/users at the site level
            if properties is not None:
                s.set_properties(properties=properties)

            # Get Current Maintenance mode for the Site
            existing_site = database.get_site(site_name=s.get_name())
            # Site entry exists
            if existing_site is not None:
                # Update the Properties {project id/user email information)
                if properties is not None:
                    existing_site.set_properties(properties=properties)
                # Site level Maintenance Update
                if s.get_maintenance_info().get(s.get_name()) is not None:
                    database.update_site(site=s)
                # Worker level Maintenance Update
                else:
                    new_maint_info = existing_site.clone_maintenance_info()
                    if new_maint_info.get(s.get_name()):
                        new_maint_info.rem(s.get_name())
                    for worker_name, entry in s.get_maintenance_info().list_details():
                        # Remove existing entry
                        if new_maint_info.get(worker_name):
                            new_maint_info.rem(worker_name)

                        # Add worker entry using the new information only if worker is in Maintenance
                        if entry.state != MaintenanceState.Active:
                            new_maint_info.add(worker_name, entry)
                    new_maint_info.finalize()
                    existing_site.update_maintenance_info(maint_info=new_maint_info)
                    database.update_site(site=existing_site)
            # Adding Maintenance State First Time
            else:
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
