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
from typing import TYPE_CHECKING

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.apis.IMgmtActor import IMgmtActor
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.kafka.KafkaProxy import KafkaProxy
from fabric.message_bus.messages.CloseReservationsAvro import CloseReservationsAvro
from fabric.message_bus.messages.GetReservationsResponseAvro import GetReservationsResponseAvro
from fabric.message_bus.messages.GetReservationsStateRequestAvro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.GetReservationsStateResponseAvro import GetReservationsStateResponseAvro
from fabric.message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro
from fabric.message_bus.messages.AddSliceAvro import AddSliceAvro
from fabric.message_bus.messages.GetReservationsRequestAvro import GetReservationsRequestAvro
from fabric.message_bus.messages.GetSlicesRequestAvro import GetSlicesRequestAvro
from fabric.message_bus.messages.RemoveReservationAvro import RemoveReservationAvro
from fabric.message_bus.messages.RemoveSliceAvro import RemoveSliceAvro
from fabric.message_bus.messages.ReservationMng import ReservationMng
from fabric.message_bus.messages.ResultAvro import ResultAvro
from fabric.message_bus.messages.SliceAvro import SliceAvro
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.StatusResponseAvro import StatusResponseAvro
from fabric.message_bus.messages.UpdateReservationAvro import UpdateReservationAvro
from fabric.message_bus.messages.UpdateSliceAvro import UpdateSliceAvro

if TYPE_CHECKING:
    from fabric.message_bus.messages.AuthAvro import AuthAvro


class KafkaActor(KafkaProxy, IMgmtActor):
    def __init__(self, guid: ID, kafka_topic: str, auth: AuthAvro, kafka_config:dict, logger,
                 message_processor: KafkaMgmtMessageProcessor):
        super().__init__(guid, kafka_topic, auth, kafka_config, logger, message_processor)

    def prepare(self, callback_topic:str):
        self.callback_topic = callback_topic

    def get_slices(self) -> GetSlicesResponseAvro:
        self.clear_last()

        response = GetSlicesResponseAvro()
        response.status = ResultAvro()

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def get_slice(self, slice_id: ID) -> GetSlicesResponseAvro:
        self.clear_last()
        response = GetSlicesResponseAvro()
        response.status = ResultAvro()

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)
            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def remove_slice(self, slice_id: ID) -> StatusResponseAvro:
        self.clear_last()
        response = StatusResponseAvro()
        response.status = ResultAvro()
        try:
            request = RemoveSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)
            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id
            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def add_slice(self, slice_obj: SliceAvro) -> StatusResponseAvro:
        self.clear_last()
        response = StatusResponseAvro()
        response.status = ResultAvro()

        try:
            request = AddSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_obj = slice_obj
            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def update_slice(self, slice_obj: SliceAvro) -> StatusResponseAvro:
        self.clear_last()
        response = StatusResponseAvro()
        response.status = ResultAvro()
        try:
            request = UpdateSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_obj = slice_obj
            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def do_get_reservations(self, slice_id: ID, state: int, reservation_id: ID = None) -> GetReservationsResponseAvro:
        self.clear_last()
        response = GetReservationsResponseAvro()
        response.status = ResultAvro()
        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_state = state
            request.slice_id = str(slice_id)
            request.reservation_id = str(reservation_id)

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    return message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response

    def get_reservations(self) -> GetReservationsResponseAvro:
        return self.do_get_reservations(None, Constants.AllReservationStates)

    def get_reservations_by_state(self, state: int) -> GetReservationsResponseAvro:
        return self.do_get_reservations(None, state)

    def get_reservations_by_slice_id(self, slice_id: ID) -> GetReservationsResponseAvro:
        self.clear_last()
        if slice_id is None:
            self.last_exception = Exception("Invalid arguments")
            return None

        return self.do_get_reservations(slice_id, Constants.AllReservationStates)

    def get_reservations_by_slice_id_and_state(self, slice_id: ID, state: int) -> GetReservationsResponseAvro:
        return self.do_get_reservations(slice_id, state)

    def get_reservation(self, rid: ID) -> GetReservationsResponseAvro:
        return self.do_get_reservations(None, None, reservation_id=rid)

    def remove_reservation(self, rid: ID) -> bool:
        response = StatusResponseAvro()
        response.status = ResultAvro()
        self.clear_last()
        if rid is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            request = RemoveReservationAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_id = str(rid)

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response.status.code == 0

    def close_reservation(self, rid: ID) -> bool:
        response = StatusResponseAvro()
        response.status = ResultAvro()
        self.clear_last()
        if rid is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            request = CloseReservationsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_id = str(rid)

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response.status.code == 0

    def close_reservations(self, slice_id: ID) -> bool:
        response = StatusResponseAvro()
        response.status = ResultAvro()
        self.clear_last()
        if slice_id is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            request = CloseReservationsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response.status.code == 0

    def update_reservation(self, reservation: ReservationMng) -> bool:
        response = StatusResponseAvro()
        response.status = ResultAvro()
        self.clear_last()
        if reservation is None:
            self.last_exception = Exception("Invalid arguments")
            return False

        try:
            request = UpdateReservationAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation = reservation

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response.status.code == 0

    def get_reservation_state_for_reservations(self, reservation_list: list) -> list:
        response = GetReservationsStateResponseAvro()
        response.status = ResultAvro()
        self.clear_last()
        if reservation_list is None:
            self.last_exception = Exception("Invalid arguments")
            return None

        try:
            request = GetReservationsStateRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_ids = []
            for r in reservation_list:
                request.reservation_ids.append(str(r))

            ret_val = self.producer.produce_sync(self.kafka_topic, request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(request.get_message_id())
                    response.status.code = Constants.ErrorInternalError
                    response.status.message = "Timeout occurred"
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = Constants.ErrorTransportFailure
                response.status.message = "Failed to send the message"

        except Exception as e:
            self.last_exception = e
            response.status.code = Constants.ErrorInternalError
            response.status.details = traceback.format_exc()
            response.status.message = "ErrorInternalError"

        return response.reservation_states