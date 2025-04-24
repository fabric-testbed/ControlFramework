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
import json
import logging
import os
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_reports_client.reports_api import ReportsApi


class ImportScript:
    """
    CLI interface to load slice JSON files from a directory and push to reports db via ReportsApi.
    """

    def __init__(self, config_file: str, slices_dir: str):
        self.logger = logging.getLogger("import")
        file_handler = RotatingFileHandler('./import.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton

        Globals.config_file = config_file
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True
        self.config = GlobalsSingleton.get().get_config()
        reports_conf = self.config.get_reports_api()
        self.reports_api = ReportsApi(base_url=reports_conf.get("host"), token_file=self._create_temp_token_file(reports_conf.get("token")))

        self.slices_dir = Path(slices_dir)
        if not self.slices_dir.exists():
            raise FileNotFoundError(f"Slice directory not found: {slices_dir}")

    def _create_temp_token_file(self, token: str) -> str:
        import tempfile
        fd, path = tempfile.mkstemp(prefix="token_", suffix=".json")
        with os.fdopen(fd, 'w') as f:
            f.write(f'{{"id_token": "{token}"}}')
        return path

    def import_slices(self):
        ignored_slices = 0
        imported_slices = 0
        try:
            for slice_file in self.slices_dir.glob("slice_*.json"):
                with open(slice_file, 'r') as f:
                    slice_data = json.load(f)
                    slice_guid = slice_data.get("slice_id")
                    slivers = slice_data.get("slivers")
                    if not slice_data.get("lease_end") or not slice_data.get("lease_start") or not slivers:
                        ignored_slices += 1
                        self.logger.info(f"Ignoring slice: {slice_guid}")
                        continue

                    self.logger.info(f"Importing slice: {slice_guid}")
                    state = SliceState(slice_data.get("state")).name
                    slice_payload = {
                        "project_id": slice_data.get("project_id"),
                        "project_name": slice_data.get("project_name"),
                        "user_id": slice_data.get("user_id"),
                        "user_email": slice_data.get("user_email"),
                        "slice_id": slice_guid,
                        "slice_name": slice_data.get("slice_name"),
                        "state": state,
                        "lease_start": slice_data.get("lease_start"),
                        "lease_end": slice_data.get("lease_end")
                    }

                    cleaned_slice_dict = {k: v for k, v in slice_payload.items() if v is not None}

                    self.reports_api.post_slice(slice_id=slice_guid, slice_payload=cleaned_slice_dict)

                    for sliver in slice_data.get("slivers", []):
                        sliver["lease_end"] = slice_data.get("lease_end")
                        sliver["sliver_type"] = sliver["type"]
                        sliver.pop("type")
                        if "components" in sliver:
                            sliver["components"] = {
                                "total": len(sliver["components"]),
                                "data": sliver["components"]
                            }
                        sliver_payload = {k: v for k, v in sliver.items() if v is not None}
                        self.reports_api.post_sliver(slice_id=slice_guid, sliver_id=sliver.get("sliver_id"),
                                                     sliver_payload=sliver_payload)

                    imported_slices += 1

                self.logger.info(f"Total slices ignored: {ignored_slices}")
                self.logger.info(f"Total slices imported: {imported_slices}")

        except Exception as e:
            self.logger.error(f"Exception occurred during import: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import JSON slice data to Reports API")
    parser.add_argument("--config_file", default="/etc/fabric/actor/config/config.yaml", help="Path to config file")
    parser.add_argument("--slices_dir", default="./exported_slices", help="Directory containing exported slice JSON files")

    args = parser.parse_args()
    importer = ImportScript(config_file=args.config_file, slices_dir=args.slices_dir)
    importer.import_slices()
