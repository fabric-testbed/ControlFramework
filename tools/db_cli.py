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

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.util.id import ID


class MainClass:
    """
    CLI interface to directly fetch information from postgres Database
    """
    def __init__(self, user: str, password: str, db: str, host: str = '127.0.0.1:5432'):
        self.logger = logging.getLogger("db-cli")
        file_handler = RotatingFileHandler('./db_cli.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        self.db = ActorDatabase(user=user, password=password, database=db, db_host=host, logger=self.logger)

    def get_slices(self, email: str = None, slice_id: str = None, slice_name: str = None):
        try:
            if slice_id is not None:
                slice_list = self.db.get_slice(slice_id=ID(uid=slice_id))
            elif email is not None:
                slice_list = self.db.get_slice_by_email(email=email)
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

    def get_reservations(self, slice_id: str = None, res_id: str = None, email: str = None):
        try:
            res_list = []
            if slice_id is not None:
                res_list = self.db.get_reservations_by_slice_id(slice_id=ID(uid=slice_id))
            elif res_id is not None:
                res_list = self.db.get_reservation(rid=ID(uid=res_id))
            elif email is not None:
                res_list = self.db.get_reservations_by_email(email=email)
            else:
                res_list = self.db.get_reservations()

            if res_list is not None and len(res_list) > 0:
                for r in res_list:
                    print(r)
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

    def handle_command(self, args):
        if args.command == "slices":
            self.get_slices(slice_id=args.slice_id, email=args.email, slice_name=args.slice_name)
        elif args.command == "slivers":
            if args.operation is not None:
                if args.operation == "remove":
                    self.remove_reservation(sliver_id=args.sliver_id)
            else:
                self.get_reservations(slice_id=args.slice_id, res_id=args.sliver_id, email=args.email)
        elif args.command == "delegations":
            self.get_delegations(dlg_id=args.delegation_id)
        else:
            print(f"Unsupported command: {args.command}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", dest='user', required=True, type=str)
    parser.add_argument("-p", dest='password', required=True, type=str)
    parser.add_argument("-d", dest='database', required=True, type=str)
    parser.add_argument("-c", dest='command', required=True, type=str)
    parser.add_argument("-s", dest='slice_id', required=False, type=str)
    parser.add_argument("-r", dest='sliver_id', required=False, type=str)
    parser.add_argument("-i", dest='delegation_id', required=False, type=str)
    parser.add_argument("-e", dest='email', required=False, type=str)
    parser.add_argument("-n", dest='slice_name', required=False, type=str)
    parser.add_argument("-o", dest='operation', required=False, type=str)
    args = parser.parse_args()

    mc = MainClass(user=args.user, password=args.password, db=args.database)
    mc.handle_command(args)

