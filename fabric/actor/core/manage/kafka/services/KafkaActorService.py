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

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.ManagementObject import ManagementObject
from fabric.actor.core.manage.kafka.services.KafkaService import KafkaService
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.message_bus.messages.AddSliceRequestAvro import AddSliceRequestAvro
from fabric.message_bus.messages.GetReservationsRequest import GetReservationsRequestAvro
from fabric.message_bus.messages.GetReservationsResponse import GetReservationsResponseAvro
from fabric.message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro
from fabric.message_bus.messages.GetSlicesRequestAvro import GetSlicesRequestAvro
from fabric.message_bus.messages.RemoveSliceAvro import RemoveSliceAvro
from fabric.message_bus.messages.ResultAvro import ResultAvro
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.StatusResponseAvro import StatusResponseAvro
from fabric.message_bus.messages.message import IMessageAvro


class KafkaActorService(KafkaService):
    def __init__(self, producer_conf, key_schema, val_schema):
        super().__init__(producer_conf, key_schema, val_schema)

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
            from fabric.actor.core.container.Globals import GlobalsSingleton
            return GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(guid)
        except Exception as e:
            raise Exception("Invalid actor id={}! e={}".format(guid, e))

    def get_slices(self, request: GetSlicesRequestAvro) -> GetSlicesResponseAvro:
        result = GetSlicesResponseAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))
            temp_result = mo.get_slices(auth)

            result.message_id = request.message_id

            if temp_result.status.get_code() == 0 and temp_result.result is not None:
                result.slices = temp_result.result.copy()

            result.status = temp_result.status
        except Exception as e:
            traceback.print_exc()
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_slice(self, request: GetSlicesRequestAvro):
        result = GetSlicesResponseAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_id is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            temp_result = mo.get_slice(ID(request.slice_id), auth)
            result.status = temp_result.status
            result.message_id = request.message_id

            if temp_result.status.get_code() == 0 and temp_result.result is not None:
                result.slices = temp_result.result.copy()

        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_slice(self, request:RemoveSliceAvro):
        result = StatusResponseAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_id is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            temp_result = mo.remove_slice(ID(request.slice_id), auth)
            result.status = temp_result.status
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_slice(self, request:AddSliceRequestAvro) -> StatusResponseAvro:
        result = StatusResponseAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            temp_result = mo.add_slice(request.slice_obj, auth)
            result.status = temp_result.status
            result.message_id = request.message_id
            if temp_result.status.get_code() == 0:
                result.result = temp_result.result

        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def update_slice(self, request:AddSliceRequestAvro) -> StatusResponseAvro:
        result = StatusResponseAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            temp_result = mo.update_slice(request.slice_obj, auth)
            result.status = temp_result.status
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations(self, request: GetReservationsRequestAvro):
        result = GetReservationsResponseAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))
            temp_result = None

            if request.get_reservation_id() is not None:
                temp_result = mo.get_reservation(auth, ID(request.get_reservation_id()))

            elif request.get_slice_id() is not None:

                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.AllReservationStates:

                    temp_result = mo.get_reservations_by_slice_id_state(auth, ID(request.get_slice_id()),
                                                                                 request.get_reservation_state())

                else:
                    temp_result = mo.get_reservations_by_slice_id(auth, ID(request.get_slice_id()))

            else:
                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.AllReservationStates:

                    temp_result = mo.get_reservations_by_state(auth, request.get_reservation_state())

                else:
                    temp_result = mo.get_reservations(auth)

            result.status = temp_result.status
            result.message_id = request.message_id

            if temp_result is not None and temp_result.status.get_code() == 0 and temp_result.result is not None:
                result.reservations = temp_result.result.copy()

        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result




