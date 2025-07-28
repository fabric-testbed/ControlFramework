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
import re
import tempfile
import traceback
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fim.slivers.network_node import NodeSliver
from fim.slivers.network_service import NetworkServiceSliver

from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
from fabric_cf.actor.core.policy.inventory_for_type import InventoryForType
from fabric_reports_client.reports_api import ReportsApi


LAST_EXPORT_FILE = "./last_export_time.txt"


class ExportScript:
    """
    CLI interface to fetch data from Postgres and push to reports db via reportsApi.
    """

    def __init__(self, config_file: str, batch_size=1000):
        self.logger = logging.getLogger("export")
        file_handler = RotatingFileHandler('/var/log/actor/export.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        Globals.config_file = config_file
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True
        self.config = GlobalsSingleton.get().get_config()

        db_conf = self.config.get_global_config().get_database()
        self.src_db = ActorDatabase(user=db_conf.get("db-user"),
                                    password=db_conf.get("db-password"),
                                    database=db_conf.get("db-name"),
                                    db_host=db_conf.get("db-host"),
                                    logger=self.logger)

        self.reports_conf = self.config.get_reports_api()
        self.temp_token_file = self._create_temp_token_file(self.reports_conf.get("token"))
        self.reports_api = ReportsApi(base_url=self.reports_conf.get("host"), token_file=self.temp_token_file)

        self.actor_config = self.config.get_actor_config()
        self.batch_size = batch_size
        self.last_export_time = self.get_last_export_time()

    def _create_temp_token_file(self, token: str) -> str:
        fd, path = tempfile.mkstemp(prefix="token_", suffix=".json")
        with os.fdopen(fd, 'w') as f:
            f.write(f'{{"id_token": "{token}"}}')
        return path

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
            if not self.reports_conf.get("enable", False):
                self.logger.info(f"Exiting export process... not enabled {self.reports_conf}")
                return
            actor_type = self.actor_config.get_type()
            if actor_type.lower() != ActorType.Orchestrator.name.lower():
                self.logger.info(f"Exiting export process... not orchestrator")
                return

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
                        slice_guid = str(slice_object.get_slice_id())
                        self.reports_api.post_slice(slice_id=slice_guid,
                                                    slice_payload={
                                                        "project_id": slice_object.get_project_id(),
                                                        "project_name": slice_object.get_project_name(),
                                                        "user_id": slice_object.get_owner().get_oidc_sub_claim(),
                                                        "user_email": slice_object.get_owner().get_email(),
                                                        "slice_id": slice_guid,
                                                        "slice_name": slice_object.get_name(),
                                                        "state": slice_object.get_state().name,
                                                        "lease_start": slice_object.get_lease_start().isoformat(),
                                                        "lease_end": slice_object.get_lease_end().isoformat()

                                                    })

                        for reservation in self.src_db.get_reservations(slice_id=slice_object.get_slice_id()):
                            error_message = reservation.get_error_message()
                            sliver_guid = str(reservation.get_reservation_id())
                            sliver = InventoryForType.get_allocated_sliver(reservation=reservation)
                            site_name = None
                            host_name = None
                            ip_subnet = None
                            core = None
                            ram = None
                            disk = None
                            image = None
                            bw = None
                            node_id = None

                            if isinstance(sliver, NodeSliver):
                                site_name = sliver.get_site()
                                if sliver.label_allocations and sliver.label_allocations.instance_parent:
                                    host_name = sliver.label_allocations.instance_parent
                                ip_subnet = str(sliver.management_ip)
                                image = sliver.image_ref
                                node_id = str(reservation.get_graph_node_id())

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

                            sliver_payload = {
                                "project_id": slice_object.get_project_id(),
                                "project_name": slice_object.get_project_name(),
                                "slice_id": slice_guid,
                                "slice_name": slice_object.get_name(),
                                "user_id": slice_object.get_owner().get_oidc_sub_claim(),
                                "user_email": slice_object.get_owner().get_email(),
                                "host": host_name,
                                "site": site_name,
                                "sliver_id": sliver_guid,
                                "node_id": node_id,
                                "state": reservation.get_state().name.lower(),
                                "sliver_type": str(reservation.get_type()).lower(),
                                "ip_subnet": ip_subnet,
                                "error": error_message,
                                "image": image,
                                "core": core,
                                "ram": ram,
                                "disk": disk,
                                "bandwidth": bw,
                                "lease_start": reservation.get_term().get_start_time().isoformat(),
                                "lease_end": reservation.get_term().get_end_time().isoformat(),
                                "interfaces": {
                                    "data": []
                                },
                                "components": {
                                    "data": []
                                }
                            }
                            if isinstance(sliver, NodeSliver) and sliver.attached_components_info:
                                components = []
                                for component in sliver.attached_components_info.devices.values():
                                    bdfs = component.labels.bdf if component.labels and component.labels.bdf else None
                                    if bdfs and not isinstance(bdfs, list):
                                        bdfs = [bdfs]
                                    node_id = None
                                    component_node_id = None

                                    sliver_map = sliver.get_node_map()
                                    if sliver_map:
                                        _, node_id = sliver_map

                                    component_map = component.get_node_map()
                                    if component_map:
                                        _, component_node_id = component_map

                                    components.append({
                                        "component_id": component.node_id,
                                        "node_id": node_id,
                                        "component_node_id": component_node_id,
                                        "type": str(component.get_type()).lower(),
                                        "model": str(component.get_model()).lower(),
                                        "bdfs": bdfs
                                    })
                                if len(components):
                                    sliver_payload["components"]["data"] = components

                            if isinstance(sliver, NetworkServiceSliver) and sliver.interface_info:
                                interfaces = []
                                for ifs in sliver.interface_info.interfaces.values():
                                    site = None
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
                                        if device_name:
                                            result = re.findall(r'\b([\w]+)-data-sw\b', device_name)
                                            if result and len(result) > 0:
                                                site = result[0]

                                    interfaces.append({
                                        "interface_id": ifs.node_id,
                                        "site": site,
                                        "vlan": vlan,
                                        "bdf": bdf,
                                        "local_name": local_name,
                                        "device_name": device_name,
                                        "name": ifs.get_name()
                                    })
                                if len(interfaces):
                                    sliver_payload["interfaces"]["data"] = interfaces

                            self.reports_api.post_sliver(slice_id=slice_guid,
                                                         sliver_id=sliver_guid,
                                                         sliver_payload=sliver_payload)

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
            if os.path.exists(self.temp_token_file):
                os.remove(self.temp_token_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export data from Postgres to SQLAlchemy DB via DatabaseManager")
    parser.add_argument("--config_file", default="/etc/fabric/actor/config/config.yaml", help="Path to config file")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of slices to process per batch")

    args = parser.parse_args()
    exporter = ExportScript(config_file=args.config_file, batch_size=args.batch_size)
    exporter.export()
