#!/usr/bin/env python3
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
import argparse
import logging
import traceback
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from fim.slivers.interface_info import InterfaceSliver
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver

from fabric_cf.actor.core.kernel.slice import SliceTypes, Slice
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from export.db_manager import DatabaseManager


LAST_EXPORT_FILE = "./last_export_time.txt"


class ExportScript:
    """
    CLI interface to fetch data from Postgres and insert it into the new SQLAlchemy database via DatabaseManager.
    """

    def __init__(self, src_user, src_password, src_db, src_host,
                 dest_user, dest_password, dest_db, dest_host, batch_size=1000):
        """
        Initializes connections to both source (Postgres) and destination (DatabaseManager).
        """
        self.logger = logging.getLogger("export")
        file_handler = RotatingFileHandler('./export.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        # Connect to the source Postgres database
        Globals.config_file = '/etc/fabric/actor/config/config.yaml'
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True

        self.src_db = ActorDatabase(user=src_user, password=src_password, database=src_db,
                                    db_host=src_host, logger=self.logger)

        # Initialize the destination database manager
        self.dest_db = DatabaseManager(user=dest_user, password=dest_password, database=dest_db,
                                       db_host=dest_host)

        self.batch_size = batch_size
        self.last_export_time = self.get_last_export_time()

    def get_last_export_time(self):
        """
        Reads the last export timestamp from a file.
        """
        if os.path.exists(LAST_EXPORT_FILE):
            with open(LAST_EXPORT_FILE, "r") as f:
                timestamp_str = f.read().strip()
                try:
                    return datetime.fromisoformat(timestamp_str)
                except ValueError:
                    self.logger.warning("Invalid timestamp in last export file, defaulting to epoch.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)  # Default to epoch

    def update_last_export_time(self, new_timestamp):
        """
        Updates the last export timestamp file with the current run time.
        """
        with open(LAST_EXPORT_FILE, "w") as f:
            f.write(new_timestamp.isoformat())

    def export(self):
        """
        Exports only the slices updated after the last execution timestamp.
        """
        try:
            self.logger.info(f"Starting export process... Last export was at {self.last_export_time}")

            offset = 0
            new_timestamp = datetime.now(timezone.utc)

            while True:
                self.logger.info(f"Fetching slices from offset {offset} (batch size: {self.batch_size})")
                slices = self.src_db.get_slices(offset=offset, limit=self.batch_size, slc_type=[SliceTypes.ClientSlice],
                                                updated_after=self.last_export_time)  # Fetch only updated slices

                if not slices or len(slices) == 0:
                    self.logger.info("No more slices to process. Export complete.")
                    break  # Stop when no more slices are found

                for slice_object in slices:
                    try:
                        project_id = self.dest_db.add_or_update_project(project_uuid=slice_object.get_project_id(),
                                                                        project_name=slice_object.get_project_name())
                        user_id = self.dest_db.add_or_update_user(user_uuid=slice_object.get_owner().get_oidc_sub_claim(),
                                                                  user_email=slice_object.get_owner().get_email())

                        slice_id = self.dest_db.add_or_update_slice(project_id=project_id, user_id=user_id,
                                                                    slice_guid=str(slice_object.get_slice_id()),
                                                                    slice_name=slice_object.get_name(),
                                                                    state=slice_object.get_state().value,
                                                                    lease_start=slice_object.get_lease_start(),
                                                                    lease_end=slice_object.get_lease_end())
                        for reservation in self.src_db.get_reservations(slice_id=slice_object.get_slice_id()):
                            error_message = reservation.get_error_message()
                            sliver = InventoryForType.get_allocated_sliver(reservation=reservation)
                            site_name = None
                            host_name = None
                            site_id = None
                            host_id = None
                            ip_subnet = None
                            core = None
                            ram = None
                            disk = None
                            image = None
                            bw = None

                            if isinstance(sliver, NodeSliver):
                                site_name = sliver.get_site()
                                if sliver.label_allocations and sliver.label_allocations.instance_parent:
                                    host_name = sliver.label_allocations.instance_parent
                                ip_subnet = str(sliver.management_ip)
                                image = sliver.image_ref

                                if sliver.capacity_allocations:
                                    core = sliver.capacity_allocations.core
                                    ram = sliver.capacity_allocations.ram
                                    disk = sliver.capacity_allocations.disk

                            elif isinstance(sliver, NetworkServiceSliver):
                                site_name = sliver.get_site()
                                if sliver.get_gateway():
                                    ip_subnet = str(sliver.get_gateway().subnet)
                                if sliver.capacities:
                                    bw = sliver.capacities.bw

                            if site_name:
                                site_id = self.dest_db.add_or_update_site(site_name=site_name.upper())
                                if host_name:
                                    host_id = self.dest_db.add_or_update_host(host_name=host_name.lower(), site_id=site_id)

                            sliver_id = self.dest_db.add_or_update_sliver(project_id=project_id,
                                                                          user_id=user_id,
                                                                          slice_id=slice_id,
                                                                          site_id=site_id,
                                                                          host_id=host_id,
                                                                          sliver_guid=str(reservation.get_reservation_id()),
                                                                          lease_start=reservation.get_term().get_start_time(),
                                                                          lease_end=reservation.get_term().get_end_time(),
                                                                          state=reservation.get_state().value,
                                                                          ip_subnet=ip_subnet,
                                                                          core=core,
                                                                          ram=ram,
                                                                          disk=disk,
                                                                          image=image,
                                                                          bandwidth=bw,
                                                                          sliver_type=str(reservation.get_type()).lower(),
                                                                          error=error_message)

                            if isinstance(sliver, NodeSliver) and sliver.attached_components_info:
                                for component in sliver.attached_components_info.devices.values():
                                    bdfs = component.labels.bdf if component.labels and component.labels.bdf else None
                                    if bdfs and not isinstance(bdfs, list):
                                        bdfs = [bdfs]
                                    self.dest_db.add_or_update_component(sliver_id=sliver_id,
                                                                         component_guid=component.node_id,
                                                                         component_type=str(component.get_type()).lower(),
                                                                         model=str(component.get_model()).lower(),
                                                                         bdfs=bdfs)

                            if isinstance(sliver, NetworkServiceSliver) and sliver.interface_info:
                                for ifs in sliver.interface_info.interfaces.values():
                                    vlan = ifs.labels.vlan if ifs.labels else None
                                    if not vlan and ifs.label_allocations:
                                        vlan = ifs.label_allocations.vlan

                                    bdf = ifs.labels.bdf if ifs.labels else None
                                    if not bdf and ifs.label_allocations:
                                        bdf = ifs.label_allocations.bdf

                                    local_name = ifs.labels.local_name if ifs.labels else None
                                    if not local_name and ifs.label_allocations:
                                        local_name = ifs.label_allocations.local_name

                                    device_name = ifs.labels.device_name if ifs.labels else None
                                    if not device_name and ifs.label_allocations:
                                        device_name = ifs.label_allocations.device_name

                                    self.dest_db.add_or_update_interface(sliver_id=sliver_id,
                                                                         interface_guid=ifs.node_id,
                                                                         vlan=vlan,
                                                                         name=ifs.get_name(),
                                                                         bdf=bdf,
                                                                         local_name=local_name,
                                                                         device_name=device_name)

                    except Exception as slice_error:
                        self.logger.error(f"Error processing slice {slice_object.get_slice_id()}: {slice_error}")
                        traceback.print_exc()
                    offset += self.batch_size  # Move to the next batch

            self.logger.info(f"Updating last export time to {new_timestamp}")
            self.update_last_export_time(new_timestamp)
            self.logger.info("Export process completed successfully!")

        except Exception as e:
            self.logger.error(f"Exception occurred during export: {e}")
            traceback.print_exc()

        finally:
            self.dest_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export data from Postgres to SQLAlchemy DB via DatabaseManager")
    parser.add_argument("--src_user", default="fabric", help="Source database username")
    parser.add_argument("--src_password", default="fabric", help="Source database password")
    parser.add_argument("--src_db", default="orchestrator", help="Source database name")
    parser.add_argument("--src_host", default="orchestrator-db:5432", help="Source database host")
    parser.add_argument("--dest_user", default="fabric", help="Destination database username")
    parser.add_argument("--dest_password", default="fabric", help="Destination database password")
    parser.add_argument("--dest_db", default="analytics", help="Destination database name")
    parser.add_argument("--dest_host", default="analytics-db:5432", help="Destination database host")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of slices to process per batch")

    args = parser.parse_args()

    exporter = ExportScript(
        src_user=args.src_user, src_password=args.src_password, src_db=args.src_db, src_host=args.src_host,
        dest_user=args.dest_user, dest_password=args.dest_password, dest_db=args.dest_db, dest_host=args.dest_host,
        batch_size=args.batch_size
    )

    exporter.export()
