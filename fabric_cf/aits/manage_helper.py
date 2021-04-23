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
from typing import Tuple

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro

from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.util.id import ID


class ManageHelper:
    def __init__(self, *, logger):
        self.logger = logger

    @staticmethod
    def print_result(*, status: ResultAvro):
        print("Code={}".format(status.get_code()))
        if status.message is not None:
            print("Message={}".format(status.message))
        if status.details is not None:
            print("Details={}".format(status.details))

    @staticmethod
    def get_actor(*, actor_name: str) -> KafkaActor:
        from .kafka_processor import KafkaProcessorSingleton
        actor = KafkaProcessorSingleton.get().get_mgmt_actor(name=actor_name)
        return actor

    def do_get_slices(self, *, actor_name: str, callback_topic: str, slice_id: str = None, id_token: str):
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            if slice_id is None:
                return actor.get_slices(id_token=id_token), actor.get_last_error()
            else:
                slice_list = []
                slice_obj = actor.get_slice(slice_id=ID(uid=slice_id), id_token=id_token)
                if slice_obj is not None:
                    slice_list.append(slice_obj)
                return slice_list, actor.get_last_error()
        except Exception:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
        return None, None

    def do_get_reservations(self, *, actor_name: str, callback_topic: str, rid: str, id_token: str):
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            if rid is None:
                return actor.get_reservations(id_token=id_token), actor.get_last_error()
            else:
                rid_list = []
                r = actor.get_reservation(rid=rid, id_token=id_token)
                if r is not None:
                    rid_list.append(r)
                return rid_list, actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def do_get_delegations(self, *, actor_name: str, callback_topic: str, did: str, id_token: str):
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            if did is None:
                return actor.get_delegations(id_token=id_token), actor.get_last_error()
            else:
                did_list = []
                d = actor.get_delegations(delegation_id=did, id_token=id_token)
                if d is not None:
                    did_list.append(d)
                return did_list, actor.get_last_error()
        except Exception as e:
            traceback.print_exc()
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)

    def do_claim_resources(self, *, broker: str, am_guid: ID, callback_topic: str, id_token: str,
                           did: str) -> Tuple[DelegationAvro, Error]:
        """
        Claim delegations by invoking Management Actor Claim Delegations API
        @param broker broker guid
        @param am_guid am guid
        @param callback_topic callback topic
        @param id_token id token
        @param did delegation id
        @return Tuple[Delegation, Error] Delegation on success and Error in case of failure
        """
        actor = self.get_actor(actor_name=broker)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(broker))
        try:
            actor.prepare(callback_topic=callback_topic)

            dlg = actor.claim_delegations(broker=am_guid, did=did, id_token=id_token)
            return dlg, actor.get_last_error()
        except Exception as e:
            self.logger.error(f"Exception occurred e: {e}")
            self.logger.error(traceback.format_exc())

        return None, actor.get_last_error()

    def claim_delegations(self, *, broker: str, am: str, callback_topic: str, did: str, id_token: str):
        """
        Claim delegations
        @param broker broker name
        @param am am name
        @param callback_topic callback topic
        @param id_token id token
        @param did delegation id
        """
        try:
            am_actor = self.get_actor(actor_name=am)
            broker_actor = self.get_actor(actor_name=broker)

            if am_actor is None or broker_actor is None:
                raise Exception("Invalid arguments am_actor {} or broker_actor {} not found".format(am_actor,
                                                                                                    broker_actor))

            broker_slice_id_list = []
            if did is None:
                slices, error = self.do_get_slices(actor_name=am, callback_topic=callback_topic, slice_id=None,
                                                   id_token=id_token)
                if slices is None:
                    print("Error occurred while getting slices for actor: {}".format(am))
                    self.print_result(status=error.get_status())
                    return

                for s in slices:
                    if s.get_slice_name() == broker:
                        broker_slice_id_list.append(s.get_slice_id())

            delegations, error = self.do_get_delegations(actor_name=am, callback_topic=callback_topic, did=did,
                                                         id_token=id_token)
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
                                                             did=d.get_delegation_id(), callback_topic=callback_topic,
                                                            id_token=id_token)
                if delegation is not None:
                    print("Delegation claimed: {} ".format(delegation.get_delegation_id()))
                else:
                    self.print_result(status=error.get_status())
        except Exception as e:
            self.logger.error(f"Exception occurred e: {e}")
            self.logger.error(traceback.format_exc())