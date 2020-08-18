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

from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.manage.kafka.services.kafka_service import KafkaService
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric.message_bus.messages.message import IMessageAvro


class KafkaActorService(KafkaService):
    def __init__(self):
        super().__init__()

    def process(self, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == IMessageAvro.GetSlicesRequest:
            if message.get_slice_id() is not None:
                result = self.get_slice(message)
            else:
                result = self.get_slices(message)

        elif message.get_message_name() == IMessageAvro.RemoveSlice:
            result = self.remove_slice(message)

        elif message.get_message_name() == IMessageAvro.AddSlice:
            result = self.add_slice(message)

        elif message.get_message_name() == IMessageAvro.UpdateSlice:
            result = self.update_slice(message)

        elif message.get_message_name() == IMessageAvro.GetReservationsRequest:
            result = self.get_reservations(message)

        elif message.get_message_name() == IMessageAvro.RemoveReservation:
            result = self.remove_reservation(message)

        elif message.get_message_name() == IMessageAvro.CloseReservations:
            result = self.close_reservations(message)

        elif message.get_message_name() == IMessageAvro.UpdateReservation:
            result = self.update_reservation(message)
        elif message.get_message_name() == IMessageAvro.GetReservationsStateRequest:
            result = self.get_reservation_state(message)

        else:
            self.logger.debug("Unsupported Message, discarding it!")
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(callback_topic, result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def get_actor_mo(self, guid: ID):
        try:
            from fabric.actor.core.container.globals import GlobalsSingleton
            return GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(guid)
        except Exception as e:
            raise Exception("Invalid actor id={}! e={}".format(guid, e))

    def get_slices(self, request: GetSlicesRequestAvro) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))
            result = mo.get_slices(auth)
            result.message_id = request.message_id

        except Exception as e:
            traceback.print_exc()
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_slice(self, request: GetSlicesRequestAvro) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result = mo.get_slice(ID(request.slice_id), auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_slice(self, request:RemoveSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result.status = mo.remove_slice(ID(request.slice_id), auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_slice(self, request:AddSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result = mo.add_slice(request.slice_obj, auth)
            result.message_id = request.message_id
        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def update_slice(self, request:AddSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result.status = mo.update_slice(request.slice_obj, auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations(self, request: GetReservationsRequestAvro) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if request.get_reservation_id() is not None:
                result = mo.get_reservation(auth, ID(request.get_reservation_id()))

            elif request.get_slice_id() is not None:

                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.AllReservationStates:

                    result = mo.get_reservations_by_slice_id_state(auth, ID(request.get_slice_id()),
                                                                                 request.get_reservation_state())

                else:
                    result = mo.get_reservations_by_slice_id(auth, ID(request.get_slice_id()))

            else:
                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.AllReservationStates:

                    result = mo.get_reservations_by_state(auth, request.get_reservation_state())

                else:
                    result = mo.get_reservations(auth)

            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_reservation(self, request: RemoveReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.get_reservation_id() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))
            result.status = mo.remove_reservation(auth, ID(request.reservation_id))
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def close_reservations(self, request: CloseReservationsAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or (request.get_slice_id() is None and request.get_reservation_id() is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if request.get_slice_id() is not None:
                result.status = mo.close_slice_reservations(auth, ID(request.slice_id))
            else:
                result.status = mo.close_reservation(auth, ID(request.reservation_id))

            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def update_reservation(self, request:UpdateReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.get_reservation() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result.status = mo.update_reservation(ID(request.reservation_obj), auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservation_state(self, request:GetReservationsStateRequestAvro) -> ResultReservationStateAvro:
        result = ResultReservationStateAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.get_reservation_ids() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result = mo.get_reservation_state_for_reservations(auth, request.get_reservation_ids())
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

