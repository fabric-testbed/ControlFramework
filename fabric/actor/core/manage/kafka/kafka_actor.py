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
from typing import TYPE_CHECKING, List

from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.apis.i_mgmt_actor import IMgmtActor
from fabric.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric.actor.core.manage.kafka.kafka_proxy import KafkaProxy
from fabric.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric.message_bus.messages.delegation_avro import DelegationAvro
from fabric.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.slice_avro import SliceAvro
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric.message_bus.messages.update_slice_avro import UpdateSliceAvro

if TYPE_CHECKING:
    from fabric.message_bus.messages.auth_avro import AuthAvro
    from fabric.message_bus.producer import AvroProducerApi


class KafkaActor(KafkaProxy, IMgmtActor):
    def __init__(self, *, guid: ID, kafka_topic: str, auth: AuthAvro, logger,
                 message_processor: KafkaMgmtMessageProcessor, producer: AvroProducerApi = None):
        super().__init__(guid=guid, kafka_topic=kafka_topic, auth=auth, logger=logger,
                         message_processor=message_processor, producer=producer)

    def get_guid(self) -> ID:
        return self.management_id

    def prepare(self, *, callback_topic:str):
        self.callback_topic = callback_topic

    def get_slices(self) -> List[SliceAvro]:
        self.clear_last()

        status = ResultAvro()
        rret_val = None

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = message_wrapper.response.slices

            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def get_slice(self, *, slice_id: ID) -> SliceAvro:
        self.clear_last()
        status = ResultAvro()
        rret_val = None

        try:
            request = GetSlicesRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)
            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0 and message_wrapper.response.slices is not None and \
                            len(message_wrapper.response.slices) > 0:
                        rret_val = message_wrapper.response.slices.__iter__().__next__()
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def remove_slice(self, *, slice_id: ID) -> bool:
        self.clear_last()
        status = ResultAvro()
        try:
            request = RemoveSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)
            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.code == 0

    def add_slice(self, *, slice_obj: SliceAvro) -> ID:
        rret_val = None
        self.clear_last()
        status = ResultAvro()

        try:
            request = AddSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_obj = slice_obj
            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.code == 0:
                        rret_val = ID(id=message_wrapper.response.get_result())
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return rret_val

    def update_slice(self, *, slice_obj: SliceAvro) -> bool:
        self.clear_last()
        status = ResultAvro()
        try:
            request = UpdateSliceAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_obj = slice_obj
            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.code == 0

    def do_get_reservations(self, *, slice_id: ID = None, state: int = None,
                            reservation_id: ID = None) -> List[ReservationMng]:
        self.clear_last()
        response = ResultReservationAvro()
        response.status = ResultAvro()
        try:
            request = GetReservationsRequestAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_state = state

            if slice_id is not None:
                request.slice_id = str(slice_id)

            if reservation_id is not None:
                request.reservation_id = str(reservation_id)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    response.status.code = ErrorCodes.ErrorTransportTimeout.value
                    response.status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    response = message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = ErrorCodes.ErrorTransportFailure.value
                response.status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            response.status.code = ErrorCodes.ErrorInternalError.value
            response.status.message = ErrorCodes.ErrorInternalError.name
            response.status.details = traceback.format_exc()

        self.last_status = response.status

        return response.reservations

    def get_reservations(self) -> List[ReservationMng]:
        return self.do_get_reservations(slice_id=None, state=Constants.AllReservationStates, reservation_id=None)

    def get_reservations_by_state(self, *, state: int) -> List[ReservationMng]:
        return self.do_get_reservations(slice_id=None, state=state, reservation_id=None)

    def get_reservations_by_slice_id(self, *, slice_id: ID) -> List[ReservationMng]:
        status = ResultAvro()

        self.clear_last()
        if slice_id is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return None

        return self.do_get_reservations(slice_id=slice_id, state=Constants.AllReservationStates, reservation_id=None)

    def get_reservations_by_slice_id_and_state(self, *, slice_id: ID, state: int) -> List[ReservationMng]:
        return self.do_get_reservations(slice_id=slice_id, state=state, reservation_id=None)

    def get_reservation(self, *, rid: ID) -> ReservationMng:
        reservation_list = self.do_get_reservations(slice_id=None, state=None, reservation_id=rid)
        if reservation_list is not None and len(reservation_list) > 0:
            return reservation_list.__iter__().__next__()
        return None

    def do_get_delegations(self, *, slice_id: ID = None, state: int = None,
                           delegation_id: ID = None) -> List[DelegationAvro]:
        self.clear_last()
        response = ResultDelegationAvro()
        response.status = ResultAvro()
        try:
            request = GetDelegationsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.delegation_state = state

            if slice_id is not None:
                request.slice_id = str(slice_id)

            if delegation_id is not None:
                request.delegation_id = str(delegation_id)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))
            response.message_id = request.message_id

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    response.status.code = ErrorCodes.ErrorTransportTimeout.value
                    response.status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    response = message_wrapper.response
            else:
                self.logger.debug("Failed to send the message")
                response.status.code = ErrorCodes.ErrorTransportFailure.value
                response.status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            response.status.code = ErrorCodes.ErrorInternalError.value
            response.status.message = ErrorCodes.ErrorInternalError.name
            response.status.details = traceback.format_exc()

        self.last_status = response.status

        return response.delegations

    def get_delegations(self) -> List[DelegationAvro]:
        return self.do_get_delegations(slice_id=None, state=Constants.AllReservationStates, delegation_id=None)

    def get_delegations_by_state(self, *, state: int) -> List[DelegationAvro]:
        return self.do_get_delegations(slice_id=None, state=state, delegation_id=None)

    def get_delegations_by_slice_id(self, *, slice_id: ID) -> List[DelegationAvro]:
        status = ResultAvro()

        self.clear_last()
        if slice_id is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return None

        return self.do_get_delegations(slice_id=slice_id, state=Constants.AllReservationStates, delegation_id=None)

    def get_delegations_by_slice_id_and_state(self, *, slice_id: ID, state: int) -> List[DelegationAvro]:
        return self.do_get_delegations(slice_id=slice_id, state=state, delegation_id=None)

    def get_delegation(self, *, rid: ID) -> DelegationAvro:
        reservation_list = self.do_get_delegations(slice_id=None, state=None, delegation_id=rid)
        if reservation_list is not None and len(reservation_list) > 0:
            return next(iter(reservation_list))
        return None

    def remove_reservation(self, *, rid: ID) -> bool:
        status = ResultAvro()
        self.clear_last()
        if rid is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return False

        try:
            request = RemoveReservationAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_id = str(rid)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.code == 0

    def close_reservation(self, *, rid: ID) -> bool:
        status = ResultAvro()
        self.clear_last()
        if rid is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return False

        try:
            request = CloseReservationsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation_id = str(rid)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.code == 0

    def close_reservations(self, *, slice_id: ID) -> bool:
        status = ResultAvro()
        self.clear_last()
        if slice_id is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return False

        try:
            request = CloseReservationsAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.slice_id = str(slice_id)

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.code == 0

    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        status = ResultAvro()
        self.clear_last()
        if reservation is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
            return False

        try:
            request = UpdateReservationAvro()
            request.guid = str(self.management_id)
            request.auth = self.auth
            request.callback_topic = self.callback_topic
            request.message_id = str(ID())
            request.reservation = reservation

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return status.get_code() == 0

    def get_reservation_state_for_reservations(self, *, reservation_list: list) -> List[ReservationMng]:
        status = ResultAvro()
        self.clear_last()
        if reservation_list is None:
            self.last_exception = Exception("Invalid arguments")
            status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            self.last_status = status
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

            ret_val = self.producer.produce_sync(topic=self.kafka_topic, record=request)

            self.logger.debug("Message {} written to {}".format(request.name, self.kafka_topic))

            if ret_val:
                message_wrapper = self.message_processor.add_message(message=request)

                with message_wrapper.condition:
                    message_wrapper.condition.wait(Constants.ManagementApiTimeoutInSeconds)

                if not message_wrapper.done:
                    self.logger.debug("Timeout occurred!")
                    self.message_processor.remove_message(msg_id=request.get_message_id())
                    status.code = ErrorCodes.ErrorTransportTimeout.value
                    status.message = ErrorCodes.ErrorTransportTimeout.name
                else:
                    self.logger.debug("Received response {}".format(message_wrapper.response))
                    status = message_wrapper.response.status
                    if status.get_code() == 0:
                        return message_wrapper.response.reservation_states
            else:
                self.logger.debug("Failed to send the message")
                status.code = ErrorCodes.ErrorTransportFailure.value
                status.message = ErrorCodes.ErrorTransportFailure.name

        except Exception as e:
            self.last_exception = e
            status.code = ErrorCodes.ErrorInternalError.value
            status.message = ErrorCodes.ErrorInternalError.name
            status.details = traceback.format_exc()

        self.last_status = status

        return None
