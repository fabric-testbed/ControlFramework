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
import argparse
import logging
import traceback
from logging.handlers import RotatingFileHandler

from fim.user import ServiceType

from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fim.graph.neo4j_property_graph import Neo4jGraphImporter, Neo4jPropertyGraph

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton


class MainClass:
    """
    CLI interface to directly fetch information from postgres Database
    """
    Globals.config_file = '/etc/fabric/actor/config/config.yaml'
    GlobalsSingleton.get().load_config()
    GlobalsSingleton.get().initialized = True

    def __init__(self, user: str, password: str, db: str, host: str = 'orchestrator-db:5432'):
        self.logger = logging.getLogger("db-cli")
        file_handler = RotatingFileHandler('./db_cli.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        self.db = ActorDatabase(user=user, password=password, database=db, db_host=host, logger=self.logger)
        self.neo4j_config = {"url": "neo4j://orchestrator-neo4j:9687",
                             "user": "neo4j",
                             "pass": "password",
                             "import_host_dir": "/Users/kthare10/renci/code/fabric/ControlFramework/neo4j1/imports/",
                             "import_dir": "/imports"}

    def get_slices(self, email: str = None, slice_id: str = None, slice_name: str = None):
        try:
            if slice_id is not None:
                slice_obj = self.db.get_slices(slice_id=ID(uid=slice_id))
                slice_list = [slice_obj]
            elif email is not None:
                slice_list = self.db.get_slices(email=email)
            else:
                slice_list = self.db.get_slices()

            if slice_list is not None and len(slice_list) > 0:
                for s in slice_list:
                    show_slice = slice_name is None
                    if slice_name is not None:
                        show_slice = slice_name in s.get_name()
                    if show_slice:
                        print(s)
                        print()
            else:
                print(f"No slices found: {slice_list}")
        except Exception as e:
            print(f"Exception occurred while fetching slices: {e}")
            traceback.print_exc()

    def get_slice_topology(self, graph_id: str):
        try:
            neo4j_graph_importer = Neo4jGraphImporter(url=self.neo4j_config["url"],
                                                      user=self.neo4j_config["user"],
                                                      pswd=self.neo4j_config["pass"],
                                                      import_host_dir=self.neo4j_config["import_host_dir"],
                                                      import_dir=self.neo4j_config["import_dir"],
                                                      logger=self.logger)

            slice_model = Neo4jPropertyGraph(graph_id=graph_id, importer=neo4j_graph_importer)
            slice_model_str = slice_model.serialize_graph()

            with open("slice_model_str.txt", "w") as file:
                file.write(slice_model_str)

            print(f"Slice Model: {slice_model}")
        except Exception as e:
            print(f"Exception occurred while fetching slices: {e}")
            traceback.print_exc()

    def get_delegations(self, dlg_id: str = None):
        try:
            if dlg_id is not None:
                del_list = self.db.get_delegation(dlg_graph_id=dlg_id)
            else:
                del_list = self.db.get_delegations()
            if del_list is not None and len(del_list) > 0:
                for d in del_list:
                    print(d)
                    print()
            else:
                print(f"No delegations found: {del_list}")
        except Exception as e:
            print(f"Exception occurred while fetching delegations: {e}")
            traceback.print_exc()

    def get_sites(self):
        try:
            sites = self.db.get_sites()
            if sites is not None:
                for s in sites:
                    print(s)
        except Exception as e:
            print(f"Exception occurred while fetching sites: {e}")
            traceback.print_exc()

    def get_components(self, node_id: str):
        states = [ReservationStates.Active.value,
                  ReservationStates.ActiveTicketed.value,
                  ReservationStates.Ticketed.value,
                  ReservationStates.Nascent.value]

        res_type = []
        for x in ServiceType:
            res_type.append(str(x))

        try:
            components = self.db.get_components(node_id=node_id, states=states, rsv_type=res_type)
            if components is not None:
                for c in components:
                    print(c)
        except Exception as e:
            print(f"Exception occurred while fetching sites: {e}")
            traceback.print_exc()

    def get_reservations(self, slice_id: str = None, res_id: str = None, email: str = None):
        try:
            res_list = self.db.get_reservations(slice_id=slice_id, rid=res_id, email=email)
            if res_list is not None and len(res_list) > 0:
                for r in res_list:
                    print(r)
                    print(type(r))
                    print(f"RES Sliver: {r.get_resources().get_sliver()}")
                    print(f"REQ RES Sliver: {r.get_requested_resources()} {r.get_requested_resources().get_sliver()}")
                    print(f"APPR RES Sliver: {r.get_approved_resources()} {r.get_approved_resources().get_sliver()}")
                    from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
                    if isinstance(r, ReservationClient):
                        print(r.get_leased_resources().get_sliver())
                    print()
            else:
                print(f"No reservations found: {res_list}")
        except Exception as e:
            print(f"Exception occurred while fetching delegations: {e}")
            traceback.print_exc()

    def remove_reservation(self, sliver_id: str):
        try:
            self.db.remove_reservation(rid=ID(uid=sliver_id))
        except Exception as e:
            print(f"Exception occurred while fetching delegations: {e}")
            traceback.print_exc()

    def remove_slice(self, slice_id: str):
        try:
            self.db.remove_slice(slice_id=ID(uid=slice_id))
        except Exception as e:
            print(f"Exception occurred while fetching delegations: {e}")
            traceback.print_exc()

    def handle_command(self, args):
        if args.command == "slices":
            if args.operation is not None and args.operation == "remove":
                self.remove_slice(slice_id=args.slice_id)
            elif args.operation is not None and args.operation == "topology":
                self.get_slice_topology(graph_id=args.graph_id)
            else:
                self.get_slices(slice_id=args.slice_id, email=args.email, slice_name=args.slice_name)
        elif args.command == "slivers":
            if args.operation is not None and args.operation == "remove":
                self.remove_reservation(sliver_id=args.sliver_id)
            else:
                self.get_reservations(slice_id=args.slice_id, res_id=args.sliver_id, email=args.email)
        elif args.command == "delegations":
            self.get_delegations(dlg_id=args.delegation_id)
        elif args.command == "sites":
            self.get_sites()
        elif args.command == "components":
            self.get_components(node_id=args.node_id)
        else:
            print(f"Unsupported command: {args.command}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", dest='user', required=True, type=str)
    parser.add_argument("-p", dest='password', required=True, type=str)
    parser.add_argument("-d", dest='database', required=True, type=str)
    parser.add_argument("-c", dest='command', required=True, type=str)
    parser.add_argument("-s", dest='slice_id', required=False, type=str)
    parser.add_argument("-g", dest='graph_id', required=False, type=str)
    parser.add_argument("-r", dest='sliver_id', required=False, type=str)
    parser.add_argument("-i", dest='delegation_id', required=False, type=str)
    parser.add_argument("-e", dest='email', required=False, type=str)
    parser.add_argument("-n", dest='slice_name', required=False, type=str)
    parser.add_argument("-o", dest='operation', required=False, type=str)
    parser.add_argument("-nid", dest='node_id', required=False, type=str)
    args = parser.parse_args()

    mc = MainClass(user=args.user, password=args.password, db=args.database)
    mc.handle_command(args)

