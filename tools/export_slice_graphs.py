#!/usr/bin/env python3
#
# Copyright (c) 2026 FABRIC Testbed
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
"""
Export the slice ASM graphs stored in the Orchestrator's Neo4j database as GraphML files.

Slices are enumerated from the Orchestrator's Postgres database (which holds the
slice name/guid to Neo4j graph id mapping); each slice's graph is then serialized
from Neo4j and written to <output_dir>/<slice_name>-<slice_guid>.graphml.

Intended to be run inside the orchestrator container:
    python3 export_slice_graphs.py --config_file /etc/fabric/actor/config/config.yaml \
        --output_dir /var/log/actor/slice-graphs
"""
import argparse
import logging
import os
import re
import traceback

import yaml

from fim.graph.abc_property_graph import GraphFormat

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.fim.fim_helper import FimHelper


class SliceGraphExporter:
    """
    Exports slice ASM graphs from Neo4j to GraphML files, one file per slice.
    """

    def __init__(self, config_file: str, output_dir: str, batch_size: int = 100):
        with open(config_file) as f:
            config_dict = yaml.safe_load(f)

        self.logger = logging.getLogger("slice-graph-export")
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s")

        self.neo4j_config = config_dict[Constants.CONFIG_SECTION_NEO4J]
        database_config = config_dict[Constants.CONFIG_SECTION_DATABASE]

        self.db = ActorDatabase(user=database_config[Constants.PROPERTY_CONF_DB_USER],
                                password=database_config[Constants.PROPERTY_CONF_DB_PASSWORD],
                                database=database_config[Constants.PROPERTY_CONF_DB_NAME],
                                db_host=database_config[Constants.PROPERTY_CONF_DB_HOST],
                                logger=self.logger)

        self.output_dir = output_dir
        self.batch_size = batch_size
        os.makedirs(self.output_dir, exist_ok=True)

    @staticmethod
    def _safe_file_name(name: str) -> str:
        return re.sub(r'[^A-Za-z0-9._-]+', '_', name)

    def export_slice(self, *, slice_object) -> bool:
        """
        Serialize a single slice's graph from Neo4j and write it as GraphML.
        :return: True if the graph was exported, False if it was skipped.
        """
        slice_guid = str(slice_object.get_slice_id())
        slice_name = slice_object.get_name()
        graph_id = slice_object.get_graph_id()

        if graph_id is None:
            self.logger.warning(f"Slice {slice_name}/{slice_guid} has no graph id; skipping")
            return False

        graph = FimHelper.get_graph(graph_id=graph_id, neo4j_config=self.neo4j_config)
        if not graph.graph_exists():
            self.logger.warning(f"Graph {graph_id} for slice {slice_name}/{slice_guid} "
                                f"not found in Neo4j (possibly closed slice); skipping")
            return False

        graph_ml = graph.serialize_graph(format=GraphFormat.GRAPHML)
        if graph_ml is None:
            self.logger.warning(f"Graph {graph_id} for slice {slice_name}/{slice_guid} "
                                f"serialized to empty output; skipping")
            return False

        file_name = f"{self._safe_file_name(slice_name)}-{slice_guid}.graphml"
        file_path = os.path.join(self.output_dir, file_name)
        with open(file_path, 'w') as f:
            f.write(graph_ml)

        state = slice_object.get_state().name if slice_object.get_state() else "Unknown"
        self.logger.info(f"Exported slice {slice_name}/{slice_guid} [{state}] graph {graph_id} -> {file_path}")
        return True

    def export(self, *, slice_id: str = None, states: list[int] = None, email: str = None,
               project_id: str = None):
        """
        Export the graphs of all matching slices.
        """
        exported = 0
        skipped = 0
        failed = 0
        offset = 0

        while True:
            slices = self.db.get_slices(slice_id=ID(uid=slice_id) if slice_id else None,
                                        states=states, email=email, project_id=project_id,
                                        slc_type=[SliceTypes.ClientSlice],
                                        offset=offset, limit=self.batch_size)
            if not slices:
                break

            for slice_object in slices:
                try:
                    if self.export_slice(slice_object=slice_object):
                        exported += 1
                    else:
                        skipped += 1
                except Exception as e:
                    failed += 1
                    self.logger.error(f"Failed to export slice {slice_object.get_slice_id()}: {e}")
                    self.logger.error(traceback.format_exc())

            offset += self.batch_size

        self.logger.info(f"Export complete: {exported} exported, {skipped} skipped, {failed} failed; "
                         f"output directory: {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(description="Export slice graphs from the Orchestrator Neo4j "
                                                 "database as GraphML files")
    parser.add_argument("--config_file", default="/etc/fabric/actor/config/config.yaml",
                        help="Path to the actor config file (for database and neo4j sections)")
    parser.add_argument("--output_dir", default="./slice-graphs",
                        help="Directory where GraphML files are written")
    parser.add_argument("--slice_id", default=None,
                        help="Export only the slice with this guid")
    parser.add_argument("--states", default=None,
                        help="Comma-separated slice states to include, e.g. StableOK,StableError "
                             "(default: all slices)")
    parser.add_argument("--email", default=None, help="Only slices owned by this user email")
    parser.add_argument("--project_id", default=None, help="Only slices in this project")
    parser.add_argument("--batch_size", type=int, default=100,
                        help="Number of slices fetched from the database per batch")

    args = parser.parse_args()

    states = None
    if args.states:
        try:
            states = [SliceState[name.strip()].value for name in args.states.split(",")]
        except KeyError as e:
            parser.error(f"Unknown slice state {e}; valid states: "
                         f"{', '.join(s.name for s in SliceState)}")

    exporter = SliceGraphExporter(config_file=args.config_file, output_dir=args.output_dir,
                                  batch_size=args.batch_size)
    exporter.export(slice_id=args.slice_id, states=states, email=args.email,
                    project_id=args.project_id)


if __name__ == "__main__":
    main()
