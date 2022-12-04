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
from enum import Enum
from typing import List, Dict, Tuple

from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.common.constants import Constants


class MaintenanceState(Enum):
    PreMaint = 1
    Maint = 2
    NoMaint = 3

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

    @staticmethod
    def translate_string_to_state(*, state_string: str):
        if state_string.lower() == MaintenanceState.PreMaint.name.lower():
            return MaintenanceState.PreMaint
        elif state_string.lower() == MaintenanceState.Maint.name.lower():
            return MaintenanceState.Maint
        elif state_string.lower() == MaintenanceState.NoMaint.name.lower():
            return MaintenanceState.NoMaint
        else:
            raise Exception("Invalid state!")


class Site:
    def __init__(self, *, name: str, state: MaintenanceState):
        """
        Represents a Site in maintenance
        """
        # Site Name
        self.name = name
        # Specific workers marked in maintenance
        self.workers = []
        # Maintenance State
        self.state = state
        # Contains allowed projects/users
        self.properties = {}
        # Deadline; only used for the PreMaint State
        self.deadline = None

    def get_deadline(self):
        return self.deadline

    def get_deadline_str(self) -> str or None:
        if self.deadline is None:
            return None
        return datetime.strftime(self.deadline, Constants.LEASE_TIME_FORMAT)

    def set_properties(self, *, properties: dict):
        self.properties = properties

    def get_properties(self) -> dict:
        return self.properties

    def get_name(self) -> str:
        return self.name

    def get_state(self) -> MaintenanceState:
        return self.state

    def add_worker(self, *, worker: str):
        """
        Mark a specific worker in Maintenance mode; if the workers list is empty,
        the whole site is considered in maintenance
        """
        if worker not in self.workers:
            self.workers.append(worker)

    def remove_worker(self, *, worker: str):
        """
        Remove a specific worker from Maintenance mode
        """
        if worker in self.workers:
            self.workers.remove(worker)

    def has_specific_workers(self) -> bool:
        if self.workers is None or len(self.workers) == 0:
            return False
        return True

    def get_workers(self) -> List[str]:
        return self.workers

    def get_workers_str(self) -> str:
        if self.workers is not None and len(self.workers) > 0:
            return ','.join(self.workers)
        return ""

    def is_in_maintenance(self) -> bool:
        if self.state is not None:
            now = datetime.now(timezone.utc)
            if self.state == MaintenanceState.Maint or \
                    (self.state == MaintenanceState.PreMaint and self.deadline <= now):
                return True
        return False

    def is_worker_in_maintenance(self, *, worker: str) -> bool:
        if not self.is_in_maintenance():
            return False

        if len(self.workers) == 0:
            return True

        if worker in self.workers:
            return True

        return False

    def __str__(self):
        return f"Name: {self.name} State: {self.state} Workers: {self.workers} Properties: {self.properties}"


class Maintenance:
    @staticmethod
    def update_maintenance_mode(*, database: ABCDatabase, properties: Dict[str, str], sites: List[Site] = None):
        if sites is not None:
            for s in sites:
                if properties is not None:
                    s.set_properties(properties=properties)

                if s.get_state() == MaintenanceState.NoMaint:
                    database.remove_site(site_name=s.get_name())
                else:
                    if database.get_site(site_name=s.get_name()) is not None:
                        database.update_site(site=s)
                    else:
                        database.add_site(site=s)
        else:
            if database.get_maintenance_properties() is None:
                database.add_maintenance_properties(properties=properties)
            else:
                database.update_maintenance_properties(properties=properties)

    @staticmethod
    def is_testbed_in_maintenance(*, database: ABCDatabase) -> Tuple[bool, Dict[str, str] or None]:
        properties = database.get_maintenance_properties()
        if properties is not None and properties.get(Constants.MODE) is not None:
            maint_mode = MaintenanceState(int(properties.get(Constants.MODE)))
            if maint_mode == MaintenanceState.Maint or maint_mode == MaintenanceState.PreMaint:
                return True, properties

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
        if not status:
            return True, None

        projects = site.get_properties().get(Constants.PROJECT_ID)
        users = site.get_properties().get(Constants.USERS)

        if project is not None and projects is not None and project in projects:
            return True, None

        if email is not None and users is not None and email in users:
            return True, None

        message = f"Site {site.get_name()} in {site.get_state()}"
        if worker is not None:
            if not site.is_worker_in_maintenance(worker=worker):
                return True, None
            else:
                message = f"Worker {worker} on {site.get_name()} in {site.get_state()}"
        else:
            if site.has_specific_workers():
                return True, None

        return False, message

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
