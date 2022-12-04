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
from __future__ import annotations

from typing import List

from fabric_mb.message_bus.messages.close_delegations_avro import CloseDelegationsAvro
from fabric_mb.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric_mb.message_bus.messages.maintenance_request_avro import MaintenanceRequestAvro
from fabric_mb.message_bus.messages.remove_delegation_avro import RemoveDelegationAvro
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric_mb.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric_mb.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.site_avro import SiteAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fabric_mb.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric_mb.message_bus.messages.update_slice_avro import UpdateSliceAvro

from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.apis.abc_mgmt_actor import ABCMgmtActor
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.kafka.kafka_proxy import KafkaProxy


class KafkaActor(KafkaProxy, ABCMgmtActor):
    def get_guid(self) -> ID:
        return self.management_id

    def prepare(self, *, callback_topic: str):
        self.callback_topic = callback_topic

    def toggle_maintenance_mode(self, actor_guid: str, callback_topic: str, mode: int, sites: List[SiteAvro] = None,
                                projects: str = None, users: str = None):
        props = {}
        if mode is not None:
            props = {Constants.MODE: str(mode)}
        if projects is not None:
            props[Constants.PROJECT_ID] = projects
        if users is not None:
            props[Constants.USERS] = users
        request = MaintenanceRequestAvro(properties=props, actor_guid=actor_guid,
                                         callback_topic=callback_topic, sites=sites)

        status, response = self.send_request(request)

        return response.status.code == 0

    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, email: str = None, project: str = None,
                   state: List[int] = None, limit: int = None, offset: int = None) -> List[SliceAvro] or None:
        request = GetSlicesRequestAvro()
        reservation_state = None
        if state is not None and len(state) == 1:
            reservation_state = state[0]
        request = self.fill_request_by_id_message(request=request, email=email, slice_id=slice_id,
                                                  slice_name=slice_name, reservation_state=reservation_state)
        status, response = self.send_request(request)
        if response is not None:
            return response.slices

        return None

    def remove_slice(self, *, slice_id: ID) -> bool:
        request = RemoveSliceAvro()
        request = self.fill_request_by_id_message(request=request, slice_id=slice_id)
        status, response = self.send_request(request)

        return status.code == 0

    def add_slice(self, *, slice_obj: SliceAvro) -> ID:
        ret_val = None
        request = AddSliceAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.slice_obj = slice_obj

        status, response = self.send_request(request)

        if status.code == 0:
            ret_val = ID(uid=response.get_result())

        return ret_val

    def update_slice(self, *, slice_obj: SliceAvro, modify_state: bool = False) -> bool:
        request = UpdateSliceAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.slice_obj = slice_obj

        status, response = self.send_request(request)

        return status.code == 0

    def accept_update_slice(self, *, slice_id: ID) -> bool:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def get_reservations(self, *, state: int = None, slice_id: ID = None,
                         rid: ID = None, oidc_claim_sub: str = None, email: str = None,
                         rid_list: List[str] = None, type: str = None, site: str = None) -> List[ReservationMng]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, slice_id=slice_id,
                                                  reservation_state=state, email=email, rid=rid, type=type, site=site)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservations
        return None

    def get_delegations(self, *, slice_id: ID = None, state: int = None,
                        delegation_id: str = None) -> List[DelegationAvro]:
        request = GetDelegationsAvro()
        request = self.fill_request_by_id_message(request=request, slice_id=slice_id,
                                                  reservation_state=state, delegation_id=delegation_id)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.delegations
        return None

    def remove_reservation(self, *, rid: ID) -> bool:
        request = RemoveReservationAvro()
        request = self.fill_request_by_id_message(request=request, rid=rid)
        status, response = self.send_request(request)

        return status.code == 0

    def close_reservation(self, *, rid: ID) -> bool:
        request = CloseReservationsAvro()
        request = self.fill_request_by_id_message(request=request, rid=rid)
        status, response = self.send_request(request)

        return status.code == 0

    def close_reservations(self, *, slice_id: ID) -> bool:
        request = CloseReservationsAvro()
        request = self.fill_request_by_id_message(request=request, slice_id=slice_id)
        status, response = self.send_request(request)

        return status.code == 0

    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        request = UpdateReservationAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.reservation = reservation

        status, response = self.send_request(request)

        return status.code == 0

    def get_reservation_state_for_reservations(self, *, reservation_list: List[str]) -> List[ReservationStateAvro]:
        request = GetReservationsStateRequestAvro()
        request.guid = str(self.management_id)
        request.auth = self.auth
        request.callback_topic = self.callback_topic
        request.message_id = str(ID())
        request.reservation_ids = []
        for r in reservation_list:
            request.reservation_ids.append(str(r))

        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservation_states

        return None

    def clone(self):
        return KafkaActor(guid=self.management_id,
                          kafka_topic=self.kafka_topic,
                          auth=self.auth, logger=self.logger,
                          message_processor=self.message_processor,
                          producer=self.producer)

    def get_name(self) -> str:
        raise ManageException(Constants.NOT_IMPLEMENTED)

    def remove_delegation(self, *, did: str) -> bool:
        request = RemoveDelegationAvro()
        request = self.fill_request_by_id_message(request=request, delegation_id=did)
        status, response = self.send_request(request)

        return status.code == 0

    def close_delegation(self, *, did: str) -> bool:
        request = CloseDelegationsAvro()
        request = self.fill_request_by_id_message(request=request, delegation_id=did)
        status, response = self.send_request(request)

        return status.code == 0
