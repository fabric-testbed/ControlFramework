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
from typing import Tuple, List

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_delegation import DelegationState
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.util.id import ID


class ManageHelper:
    def __init__(self, *, logger):
        self.logger = logger

    def print_result(self, *, status: ResultAvro):
        self.logger.debug("Code={}".format(status.get_code()))
        if status.message is not None:
            self.logger.debug("Message={}".format(status.message))
        if status.details is not None:
            self.logger.debug("Details={}".format(status.details))

    @staticmethod
    def get_actor(*, actor_name: str) -> KafkaActor:
        from .kafka_processor import KafkaProcessorSingleton
        actor = KafkaProcessorSingleton.get().get_mgmt_actor(name=actor_name)
        return actor

    def do_get_slices(self, *, actor_name: str, callback_topic: str, slice_id: str = None, slice_name: str = None,
                      id_token: str = None, email: str = None, states: str = None) -> Tuple[
        List[SliceAvro] or None, Error]:
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            sid = ID(uid=slice_id) if slice_id is not None else None
            slice_states = None
            if states is not None:
                states_list = states.split(",")
                for x in states_list:
                    if slice_states is None:
                        slice_states = []
                    slice_states.append(SliceState.translate(state_name=x).value)

            result = actor.get_slices(slice_id=sid, slice_name=slice_name, email=email, states=slice_states)
            return result, actor.get_last_error()
        except Exception:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
        return None, actor.get_last_error()

    def do_get_reservations(self, *, actor_name: str, callback_topic: str, slice_id: str = None, rid: str = None,
                            states: str = None, id_token: str = None, email: str = None, site: str = None,
                            type: str = None) -> Tuple[List[ReservationMng] or None, Error]:
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            sid = ID(uid=slice_id) if slice_id is not None else None
            reservation_id = ID(uid=rid) if rid is not None else None
            reservation_states = None
            if states is not None:
                states_list = states.split(",")
                for x in states_list:
                    if reservation_states is None:
                        reservation_states = []
                    reservation_states.append(ReservationStates.translate(state_name=x).value)
            return actor.get_reservations(slice_id=sid, rid=reservation_id, states=reservation_states, email=email,
                                          site=site, type=type), actor.get_last_error()
        except Exception as e:
            ex_str = traceback.format_exc()
            self.logger.error(ex_str)
            traceback.print_exc()
        return None, actor.get_last_error()

    def do_get_delegations(self, *, actor_name: str, callback_topic: str, slice_id: str = None, did: str = None,
                           states: str = None, id_token: str = None) -> Tuple[List[DelegationAvro] or None, Error]:
        actor = self.get_actor(actor_name=actor_name)

        if actor is None:
            raise Exception("Invalid arguments actor {} not found".format(actor_name))
        try:
            actor.prepare(callback_topic=callback_topic)
            sid = None
            if slice_id is not None:
                sid = ID(uid=slice_id)
            delegation_states = None
            if states is not None:
                for x in states:
                    if delegation_states is None:
                        delegation_states = []
                    delegation_states.append(DelegationState.translate(state_name=x).value)
            return actor.get_delegations(delegation_id=did, slice_id=sid,
                                         states=delegation_states), actor.get_last_error()
        except Exception as e:
            self.logger.error(f"Exception occurred while fetching delegations: e {e}")
            self.logger.error(traceback.format_exc())
            traceback.print_exc()
        return None, actor.get_last_error()

    def do_claim_delegations(self, *, broker: str, am_guid: ID, callback_topic: str, id_token: str = None,
                             did: str = None) -> Tuple[DelegationAvro, Error]:
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

            dlg = actor.claim_delegations(broker=am_guid, did=did)
            return dlg, actor.get_last_error()
        except Exception as e:
            self.logger.error(f"Exception occurred e: {e}")
            self.logger.error(traceback.format_exc())

        return None, actor.get_last_error()

    def do_reclaim_delegations(self, *, broker: str, am_guid: ID, callback_topic: str, id_token: str = None,
                               did: str = None) -> Tuple[DelegationAvro, Error]:
        """
        Reclaim delegations by invoking Management Actor Claim Delegations API
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

            dlg = actor.reclaim_delegations(broker=am_guid, did=did)
            return dlg, actor.get_last_error()
        except Exception as e:
            self.logger.error(f"Exception occurred e: {e}")
            self.logger.error(traceback.format_exc())

        return None, actor.get_last_error()
