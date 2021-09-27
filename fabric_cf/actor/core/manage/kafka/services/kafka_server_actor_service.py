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

from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.common.constants import ErrorCodes
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.util.id import ID


class KafkaServerActorService(KafkaActorService):
    def process(self, *, message: AbcMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == AbcMessageAvro.get_reservations_request and \
                message.get_type() is not None and \
                message.get_type() == ReservationCategory.Broker.name:
            result = self.get_reservations_by_category(request=message, category=ReservationCategory.Broker)

        elif message.get_message_name() == AbcMessageAvro.get_slices_request and \
                message.get_type() is not None and \
                message.get_type() == SliceTypes.InventorySlice.name:
            result = self.get_slices_by_slice_type(request=message, slice_type=SliceTypes.InventorySlice)

        elif message.get_message_name() == AbcMessageAvro.get_reservations_request and \
                message.get_type() is not None and \
                message.get_type() == ReservationCategory.Inventory.name:
            result = self.get_reservations_by_category(request=message, category=ReservationCategory.Inventory)

        elif message.get_message_name() == AbcMessageAvro.get_slices_request and \
                message.get_type() is not None and \
                message.get_type() == SliceTypes.ClientSlice.name:
            result = self.get_slices_by_slice_type(request=message, slice_type=SliceTypes.ClientSlice)

        elif message.get_message_name() == AbcMessageAvro.add_slice and message.slice_obj is not None and \
                (message.slice_obj.is_client_slice() or message.slice_obj.is_broker_client_slice()):
            result = self.add_client_slice(request=message)
        else:
            super().process(message=message)
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce(callback_topic, result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def add_client_slice(self, *, request: AddSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.add_client_slice(caller=auth, slice_obj=request.slice_obj)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_reservations_by_category(self, *, request: GetReservationsRequestAvro,
                                     category: ReservationCategory) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.get_reservations_by_category(caller=auth, category=category, id_token=request.get_id_token(),
                                                     slice_id=request.slice_id)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_slices_by_slice_type(self, *, request: GetSlicesRequestAvro, slice_type: SliceTypes) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.get_slices_by_slice_type(caller=auth, slice_type=slice_type, id_token=request.get_id_token())

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result
