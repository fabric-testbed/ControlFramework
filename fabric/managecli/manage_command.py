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
from fabric.managecli.show_command import ShowCommand


class ManageCommand(ShowCommand):
    def claim_resources(self, *, broker: str, am: str, callback_topic: str, rid: str):
        try:
            am_actor = self.get_actor(actor_name=am)
            broker_actor = self.get_actor(actor_name=broker)

            if am_actor is None or broker_actor is None:
                raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor,
                                                                                                    broker_actor))

            claim_rid_list = {}
            broker_slice_id_list = []
            if rid is None:
                slices, error = self.do_get_slices(actor_name=am, callback_topic=callback_topic, slice_id=None)
                if slices is None:
                    print("Error occurred while getting slices for actor: {}".format(am))
                    self.print_result(status=error.get_status())
                    return

                for s in slices:
                    if s.get_slice_name() == broker:
                        broker_slice_id_list.append(s.get_slice_id())

            reservations, error = self.do_get_reservations(actor_name=am, callback_topic=callback_topic, rid=rid)
            if reservations is None:
                print("Error occurred while getting reservations for actor: {}".format(am))
                self.print_result(status=error.get_status())
                return

            if reservations is not None:
                for r in reservations:
                    if rid is not None or r.get_slice_id() in broker_slice_id_list:
                        claim_rid_list[r.get_reservation_id()] = r.get_resource_type()

            if len(claim_rid_list) == 0:
                print("No reservations to be claimed from {} by {}:".format(am, broker))

            for reservation_id, resource_type in claim_rid_list.items():
                print("Claiming Reservation# {} for resource_type: {}".format(reservation_id, resource_type))
                reservation, error = self.do_claim_resources(broker=broker, am_guid=am_actor.get_guid(),
                                                             rid=reservation_id, callback_topic=callback_topic)
                if reservation is not None:
                    print("Reservation claimed: {} ".format(reservation.get_reservation_id()))
                else:
                    self.print_result(status=error.get_status())
        except Exception as e:
            traceback.print_exc()
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing claim {}".format(e))

    def reclaim_resources(self, *, broker: str, am: str, callback_topic: str, rid: str):
        try:
            am_actor = self.get_actor(actor_name=am)
            broker_actor = self.get_actor(actor_name=broker)

            if am_actor is None or broker_actor is None:
                raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor,
                                                                                                    broker_actor))

            reclaim_rid_list = {}
            broker_slice_id_list = []
            if rid is None:
                slices, error = self.do_get_slices(actor_name=am, callback_topic=callback_topic, slice_id=None)
                if slices is None:
                    print("Error occurred while getting slices for actor: {}".format(am))
                    self.print_result(status=error.get_status())
                    return

                for s in slices:
                    if s.get_slice_name() == broker:
                        broker_slice_id_list.append(s.get_slice_id())

            reservations, error = self.do_get_reservations(actor_name=am, callback_topic=callback_topic, rid=rid)
            if reservations is None:
                print("Error occurred while getting reservations for actor: {}".format(am))
                self.print_result(status=error.get_status())
                return

            if reservations is not None:
                for r in reservations:
                    if rid is not None or r.get_slice_id() in broker_slice_id_list:
                        reclaim_rid_list[r.get_reservation_id()] = r.get_resource_type()

            if len(reclaim_rid_list) == 0:
                print("No reservations to be claimed from {} by {}:".format(am, broker))

            for reservation_id, resource_type in reclaim_rid_list.items():
                print("Reclaiming Reservation# {} for resource_type: {}".format(reservation_id, resource_type))
                reservation, error = self.do_reclaim_resources(broker=broker, am_guid=am_actor.get_guid(),
                                                               rid=reservation_id, callback_topic=callback_topic)
                if reservation is not None:
                    print("Reservation reclaimed: {} ".format(reservation.get_reservation_id()))
                else:
                    self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing claim {}".format(e))

    def do_claim_resources(self, *, broker: str, am_guid: str, callback_topic: str, rid: str = None, did: str = None):
        actor = self.get_actor(actor_name=broker)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(broker))
        try:
            actor.prepare(callback_topic=callback_topic)
            if rid is not None:
                res = actor.claim_resources(broker=ID(id=am_guid), rid=ID(id=rid))
                return res, actor.get_last_error()
            elif did is not None:
                dlg = actor.claim_delegations(broker=ID(id=am_guid), did=did)
                return dlg, actor.get_last_error()
            else:
                raise Exception("Invalid arguments: reservation id and delegation id; both cannot be empty")
        except Exception as e:
            traceback.print_exc()
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            return None, actor.get_last_error()

    def do_reclaim_resources(self, *, broker: str, am_guid: str, callback_topic: str, rid: str = None, did: str = None):
        actor = self.get_actor(actor_name=broker)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(broker))
        try:
            actor.prepare(callback_topic=callback_topic)
            if rid is not None:
                return actor.reclaim_resources(broker=ID(id=am_guid), rid=ID(id=rid)), actor.get_last_error()
            elif did is None:
                return actor.reclaim_delegations(broker=ID(id=am_guid), did=did), actor.get_last_error()
            else:
                raise Exception("Invalid arguments: reservation id and delegation id; both cannot be empty")
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def do_close_reservation(self, *, rid: str, actor_name: str, callback_topic: str):
        actor = self.get_actor(actor_name=actor_name)
        if actor is None:
            raise Exception("Invalid arguments actor_name {} not found".format(actor_name))

        try:
            actor.prepare(callback_topic=callback_topic)
            return actor.close_reservation(rid=ID(id=rid)), actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def close_reservation(self, *, rid: str, actor_name: str, callback_topic: str):
        try:
            result, error = self.do_close_reservation(rid=rid, actor_name=actor_name, callback_topic=callback_topic)
            print(result)
            if result is False:
                self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing close_reservation {}".format(e))

    def close_slice(self, *, slice_id: str, actor_name: str, callback_topic: str):
        try:
            result, error = self.do_close_slice(slice_id=slice_id, actor_name=actor_name, callback_topic=callback_topic)
            print(result)
            if result is False:
                self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing close_slice {}".format(e))

    def do_close_slice(self, *, slice_id: str, actor_name: str, callback_topic: str):
        actor = self.get_actor(actor_name=actor_name)
        if actor is None:
            raise Exception("Invalid arguments actor_name {} not found".format(actor_name))

        try:
            actor.prepare(callback_topic=callback_topic)
            return actor.close_reservations(slice_id=slice_id), actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def remove_reservation(self, *, rid: str, actor_name: str, callback_topic: str):
        try:
            result, error = self.do_remove_reservation(rid=rid, actor_name=actor_name, callback_topic=callback_topic)
            print(result)
            if result is False:
                self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing remove_reservation {}".format(e))

    def do_remove_reservation(self, *, rid: str, actor_name: str, callback_topic: str):
        actor = self.get_actor(actor_name=actor_name)
        if actor is None:
            raise Exception("Invalid arguments actor_name {} not found".format(actor_name))

        try:
            actor.prepare(callback_topic=callback_topic)
            return actor.remove_reservation(rid=ID(id=rid)), actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def remove_slice(self, *, slice_id: str, actor_name: str, callback_topic: str):
        try:
            result, error = self.do_remove_slice(slice_id=slice_id, actor_name=actor_name,
                                                 callback_topic=callback_topic)
            print(result)
            if result is False:
                self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing remove_reservation {}".format(e))

    def do_remove_slice(self, *, slice_id: str, actor_name: str, callback_topic: str):
        actor = self.get_actor(actor_name=actor_name)
        if actor is None:
            raise Exception("Invalid arguments actor_name {} not found".format(actor_name))

        try:
            actor.prepare(callback_topic=callback_topic)
            return actor.remove_slice(slice_id=ID(id=slice_id)), actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def claim_delegations(self, *, broker: str, am: str, callback_topic: str, did: str):
        try:
            am_actor = self.get_actor(actor_name=am)
            broker_actor = self.get_actor(actor_name=broker)

            if am_actor is None or broker_actor is None:
                raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor,
                                                                                                    broker_actor))

            broker_slice_id_list = []
            if did is None:
                slices, error = self.do_get_slices(actor_name=am, callback_topic=callback_topic, slice_id=None)
                if slices is None:
                    print("Error occurred while getting slices for actor: {}".format(am))
                    self.print_result(status=error.get_status())
                    return

                for s in slices:
                    if s.get_slice_name() == broker:
                        broker_slice_id_list.append(s.get_slice_id())

            delegations, error = self.do_get_delegations(actor_name=am, callback_topic=callback_topic, did=did)
            if delegations is None:
                print("Error occurred while getting delegations for actor: {}".format(am))
                self.print_result(status=error.get_status())
                return

            if delegations is None or len(delegations) == 0:
                print("No delegations to be claimed from {} by {}:".format(am, broker))
                return

            for d in delegations:
                print("Claiming Delegation# {}".format(d.get_delegation_id()))
                delegation, error = self.do_claim_resources(broker=broker, am_guid=am_actor.get_guid(),
                                                             did=d.get_delegation_id(), callback_topic=callback_topic)
                if delegation is not None:
                    print("Delegation claimed: {} ".format(delegation.get_delegation_id()))
                else:
                    self.print_result(status=error.get_status())
        except Exception as e:
            traceback.print_exc()
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing claim {}".format(e))

    def reclaim_delegations(self, *, broker: str, am: str, callback_topic: str, did: str):
        try:
            am_actor = self.get_actor(actor_name=am)
            broker_actor = self.get_actor(actor_name=broker)

            if am_actor is None or broker_actor is None:
                raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor,
                                                                                                    broker_actor))

            reclaim_rid_list = {}
            broker_slice_id_list = []
            if did is None:
                slices, error = self.do_get_slices(actor_name=am, callback_topic=callback_topic, slice_id=None)
                if slices is None:
                    print("Error occurred while getting slices for actor: {}".format(am))
                    self.print_result(status=error.get_status())
                    return

                for s in slices:
                    if s.get_slice_name() == broker:
                        broker_slice_id_list.append(s.get_slice_id())

            delegations, error = self.do_get_delegations(actor_name=am, callback_topic=callback_topic, did=did)
            if delegations is None or len(reclaim_rid_list) == 0:
                print("No delegations to be claimed from {} by {}:".format(am, broker))
                return

            for d in delegations:
                print("Reclaiming Delegation# {}".format(d.get_delegation_id()))
                delegation, error = self.do_reclaim_resources(broker=broker, am_guid=am_actor.get_guid(),
                                                             did=d.get_delegation_id(), callback_topic=callback_topic)
                if delegation is not None:
                    print("Delegation reclaimed: {} ".format(delegation.get_delegation_id()))
                else:
                    self.print_result(status=error.get_status())
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            print("Exception occurred while processing claim {}".format(e))