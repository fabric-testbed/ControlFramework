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
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

import yaml

from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.fim.fim_helper import FimHelper


class MainClass:
    """
    CLI interface to cleanup Dead/Closing slices
    """
    def __init__(self, config_file: str):
        with open(config_file) as f:
            config_dict = yaml.safe_load(f)
        self.log_config = config_dict["logging"]

        self.logger = logging.getLogger(self.log_config["logger"])
        file_handler = RotatingFileHandler(f"{self.log_config['log-directory']}/cleanup.log",
                                           backupCount=self.log_config["log-retain"],
                                           maxBytes=self.log_config["log-size"])
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        self.neo4j_config = config_dict["neo4j"]
        self.database_config = config_dict["database"]

    def delete_dead_closing_slice(self, *, days: int):
        actor_db = ActorDatabase(user=self.database_config["db-user"],
                                 password=self.database_config["db-password"],
                                 database=self.database_config["db-name"],
                                 db_host=self.database_config["db-host"],
                                 logger=self.logger)
        states = [SliceState.Dead.value, SliceState.Closing.value]
        lease_end = datetime.now(timezone.utc) - timedelta(days=days)
        slices = actor_db.get_slices(state=states, lease_end=lease_end)
        for s in slices:
            try:
                try:
                    FimHelper.delete_graph(graph_id=s.get_graph_id(), neo4j_config=self.neo4j_config)
                except Exception as e:
                    self.logger.error(f"Failed to delete graph {s.get_graph_id()} for Slice# {s.get_slice_id()}: e: {e}")
                    self.logger.error(traceback.format_exc())
                reservations = actor_db.get_reservations(slice_id=s.get_slice_id())
                for r in reservations:
                    try:
                        actor_db.remove_reservation(rid=r.get_reservation_id())
                    except Exception as e:
                        self.logger.error(
                            f"Failed to delete reservation {r.get_reservation_id()} for Slice# {s.get_slice_id()}: e: {e}")
                        self.logger.error(traceback.format_exc())
                actor_db.remove_slice(slice_id=s.get_slice_id())
            except Exception as e:
                self.logger.error(f"Failed to delete slice: {s.get_slice_id()}: e: {e}")
                self.logger.error(traceback.format_exc())

    def handle_command(self, args):
        if args.command == "slices":
            if args.operation is not None and args.operation == "remove":
                self.delete_dead_closing_slice(days=args.days)
            else:
                print(f"Unsupported operation: {args.operation}")
        else:
            print(f"Unsupported command: {args.command}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", dest='config', required=True, type=str)
    parser.add_argument("-d", dest='days', required=True, type=int, default=30)
    parser.add_argument("-c", dest='command', required=True, type=str)
    parser.add_argument("-o", dest='operation', required=True, type=str)
    args = parser.parse_args()

    mc = MainClass(config_file=args.config)
    mc.handle_command(args)

