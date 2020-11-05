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

from fabric.actor.core.apis.i_reservation import ReservationCategory
from fabric.actor.core.common.constants import ErrorCodes
from fabric.actor.core.manage.kafka.services.kafka_client_actor_service import KafkaClientActorService
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.get_reservation_units_avro import GetReservationUnitsAvro
from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.result_unit_avro import ResultUnitAvro


class KafkaControllerService(KafkaClientActorService):
    def __init__(self):
        super().__init__()

    def process(self, *, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        if message.get_message_name() == IMessageAvro.GetReservationUnitsRequest:
            result = self.get_reservation_units(request=message)
        else:
            super().process(message=message)

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(topic=callback_topic, record=result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def get_reservation_units(self, *, request:GetReservationUnitsAvro) -> ResultUnitAvro:
        result = ResultUnitAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.reservation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(id=request.guid))

            result = mo.get_reservation_units(caller=auth, rid=ID(id=request.reservation_id),
                                              id_token=request.get_id_token())
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result