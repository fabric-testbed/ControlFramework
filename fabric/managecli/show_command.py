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
import traceback

from fabric.actor.core.util.id import ID
from fabric.managecli.command import Command


class ShowCommand(Command):
    def get_slices(self, actor_name: str, callback_topic: str, slice_id: str):
        try:
            slices, error = self.do_get_slices(actor_name, callback_topic, slice_id)
            if slices is not None:
                for s in slices:
                    s.print()
            else:
                print("Status: {}".format(error.get_status()))
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing get_slices {}".format(e))

    def do_get_slices(self, actor_name: str, callback_topic: str, slice_id: str):
        actor = self.get_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            if slice_id is None:
                return actor.get_slices(), actor.get_last_error()
            else:
                slice_list = []
                slice_obj = actor.get_slice(ID(slice_id))
                if slice_obj is not None:
                    slice_list.append(slice_obj)
                return slice_list, actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
        return None, None

    def get_reservations(self, actor_name: str, callback_topic: str, rid: str):
        try:
            reservations, error = self.do_get_reservations(actor_name, callback_topic, rid)
            if reservations is not None:
                for r in reservations:
                    r.print()
            else:
                print("Status: {}".format(error.get_status()))
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing get_reservations {}".format(e))

    def do_get_reservations(self, actor_name: str, callback_topic: str, rid: str):
        actor = self.get_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            if rid is None:
                return actor.get_reservations(), actor.get_last_error()
            else:
                rid_list = []
                r = actor.get_reservation(rid)
                if r is not None:
                    rid_list.append(r)
                return rid_list, actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
