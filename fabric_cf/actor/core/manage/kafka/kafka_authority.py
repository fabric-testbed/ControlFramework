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

from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_reservation_units_request_avro import GetReservationUnitsRequestAvro
from fabric_mb.message_bus.messages.get_unit_request_avro import GetUnitRequestAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.unit_avro import UnitAvro

from fabric_cf.actor.core.apis.abc_mgmt_authority import ABCMgmtAuthority
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.manage.kafka.kafka_server_actor import KafkaServerActor
from fabric_cf.actor.core.util.id import ID


class KafkaAuthority(KafkaServerActor, ABCMgmtAuthority):
    def get_authority_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        request = GetReservationsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token)
        request.reservation_type = ReservationCategory.Authority.name
        status, response = self.send_request(request)

        if status.code == 0:
            return response.reservations
        return None

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        request = GetReservationUnitsRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token, rid=rid)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.units
        return None

    def get_reservation_unit(self, *, uid: ID, id_token: str = None) -> UnitAvro:
        request = GetUnitRequestAvro()
        request = self.fill_request_by_id_message(request=request, id_token=id_token, rid=uid)
        status, response = self.send_request(request)

        if status.code == 0:
            return response.units
        return None

    def clone(self):
        return KafkaAuthority(guid=self.management_id,
                              kafka_topic=self.kafka_topic,
                              auth=self.auth, logger=self.logger,
                              message_processor=self.message_processor,
                              producer=self.producer)
