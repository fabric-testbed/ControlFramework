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

import traceback
from typing import List

from fabric_cf.actor.core.apis.i_mgmt_authority import IMgmtAuthority
from fabric_cf.actor.core.apis.i_reservation import ReservationCategory
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.manage.kafka.kafka_server_actor import KafkaServerActor
from fabric_cf.actor.core.util.id import ID
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_reservation_units_request_avro import GetReservationUnitsRequestAvro
from fabric_mb.message_bus.messages.get_unit_request_avro import GetUnitRequestAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro


class KafkaAuthority (KafkaServerActor, IMgmtAuthority):
    def get_authority_reservations(self, *, id_token: str = None) -> List[ReservationMng]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.reservation_type = ReservationCategory.Authority.name
            request.id_token = id_token

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.management_inter_actor_outbound_message.format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.management_api_timeout_in_seconds)

                if not message_wrapper.done:
                    self.logger.debug(Constants.management_api_timeout_occurred)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug(Constants.management_inter_actor_inbound_message.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.reservations
            else:
                self.logger.debug(Constants.management_inter_actor_message_failed.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_reservation_units(self, *, rid: ID, id_token: str = None) -> List[UnitAvro]:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetReservationUnitsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.reservation_id = str(rid)
            request.id_token = id_token

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.management_inter_actor_outbound_message.format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.management_api_timeout_in_seconds)

                if not message_wrapper.done:
                    self.logger.debug(Constants.management_api_timeout_occurred)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug(Constants.management_inter_actor_inbound_message.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.units
            else:
                self.logger.debug(Constants.management_inter_actor_message_failed.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_reservation_unit(self, *, uid: ID, id_token: str = None) -> UnitAvro:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetUnitRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.message_id = str(ID())
            request.callback_topic = self.callback_topic
            request.reservation_id = str(uid)
            request.id_token = id_token

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug(Constants.management_inter_actor_outbound_message.format(
                request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.management_api_timeout_in_seconds)

                if not message_wrapper.done:
                    self.logger.debug(Constants.management_api_timeout_occurred)
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug(Constants.management_inter_actor_inbound_message.format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.units
            else:
                self.logger.debug(Constants.management_inter_actor_message_failed.format(
                    request.name, self.kafka_topic))
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        if rret_val is not None:
            return rret_val.__iter__().__next__()

        return rret_val

    def clone(self):
        return KafkaAuthority(guid=self.management_id,
                              kafka_topic=self.kafka_topic,
                              auth=self.auth, logger=self.logger,
                              message_processor=self.message_processor,
                              producer=self.producer)