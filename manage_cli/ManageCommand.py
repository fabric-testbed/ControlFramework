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

from fabric.actor.core.util import ID
from message_bus.messages.GetReservationsResponse import GetReservationsResponseAvro
from message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro


class ManageCommand:
    @staticmethod
    def get_actor(actor_name: str):
        from manage_cli.MainShell import MainShellSingleton
        actor = MainShellSingleton.get().get_mgmt_actor(actor_name)
        return actor

    def get_slices(self, actor_name: str, callback_topic: str):
        result = self.do_get_slices(actor_name, callback_topic)
        print("Get Slices Response Status: {}".format(result.status))
        if result.status.get_code() == 0 and result.slices is not None:
            for s in result.slices:
                s.print()

    def do_get_slices(self, actor_name: str, callback_topic: str) -> GetSlicesResponseAvro:
        actor = self.get_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            return actor.get_slices()
        except Exception as e:
            traceback.print_exc()

    def get_slice(self, actor_name: str, slice_id: str, callback_topic: str):
        result = self.do_get_slice(actor_name, slice_id, callback_topic)
        print("Get Slice Response Status: {}".format(result.status))
        if result.status.get_code() == 0 and result.slices is not None:
            for s in result.slices:
                s.print()

    def do_get_slice(self, actor_name: str, slice_id: str, callback_topic: str) -> GetSlicesResponseAvro:
        actor = self.get_actor(actor_name)

        if actor is None or slice_id is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            return actor.get_slice(ID(slice_id))
        except Exception as e:
            traceback.print_exc()
            print(e)

    def get_reservations(self, actor_name: str, callback_topic: str):
        result = self.do_get_reservations(actor_name, callback_topic)
        print("Get Reservations Response Status: {}".format(result.status))
        if result.status.get_code() == 0 and result.reservations is not None:
            for r in result.reservations:
                r.print()

    def do_get_reservations(self, actor_name: str, callback_topic: str) -> GetReservationsResponseAvro:
        actor = self.get_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            return actor.get_reservations()
        except Exception as e:
            traceback.print_exc()

    def get_reservation(self, actor_name: str, rid: str, callback_topic: str):
        result = self.do_get_reservations(actor_name, rid, callback_topic)
        print("Get Reservations Response Status: {}".format(result.status))
        if result.status.get_code() == 0 and result.reservations is not None:
            for r in result.reservations:
                r.print()

    def do_get_reservation(self, actor_name: str, rid: str, callback_topic: str) -> GetReservationsResponseAvro:
        actor = self.get_actor(actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic)
            return actor.get_reservation(rid)
        except Exception as e:
            traceback.print_exc()

    def claim_resources(self, broker: str, am: str, callback_topic: str):
        am_actor = self.get_actor(am)
        broker_actor = self.get_actor(broker)

        if am_actor is None or broker_actor:
            raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor, broker_actor))

        broker_slice_id_list = None

        result = self.do_get_slices(am_actor.get_guid(), callback_topic)
        if result.status.get_code() == 0 and result.slices is not None:
            for s in result.slices:
                if s.get_slice_name() == broker:
                    broker_slice_id_list.append(s.get_slice_id())

        claim_rid_list = {}
        result = self.do_get_reservations(am, callback_topic)
        if result.status.get_code() == 0 and result.reservations is not None:
            for r in result.reservations:
                if r.get_slice_id() in broker_slice_id_list:
                    claim_rid_list[r.get_reservation_id()] = r.get_resource_type()

        print("List of reservations to be claimed from {} by {}: {}".format(am, broker, claim_rid_list))

        for k, v in claim_rid_list.items():
            print("Claiming Reservation# {} for resource_type: {}".format(k, v))
            result = self.do_claim_resources(broker, am_actor.get_guid(), k, callback_topic)
            print("Claim Response: {}".format(result))

    def do_claim_resources(self, broker: str, am_guid: str, rid: str, callback_topic: str):
        actor = self.get_actor(broker)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(broker))
        try:
            actor.prepare(callback_topic)
            return actor.claim_resources(ID(am_guid), ID(rid))
        except Exception as e:
            traceback.print_exc()