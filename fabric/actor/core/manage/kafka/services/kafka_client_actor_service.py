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
from datetime import datetime

from fabric.actor.core.apis.i_actor import ActorType
from fabric.actor.core.common.constants import ErrorCodes
from fabric.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.util.resource_type import ResourceType
from fabric.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric.message_bus.messages.get_actors_avro import GetActorsAvro
from fabric.message_bus.messages.get_pool_info_avro import GetPoolInfoAvro
from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.messages.reclaim_resources_avro import ReclaimResourcesAvro
from fabric.message_bus.messages.result_pool_info_avro import ResultPoolInfoAvro
from fabric.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric.actor.core.util.id import ID
from fabric.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.message_bus.messages.result_strings_avro import ResultStringsAvro


class KafkaClientActorService(KafkaActorService):
    def __init__(self):
        super().__init__()

    def process(self, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == IMessageAvro.ClaimResources:
            result = self.claim_resources(message)
        elif message.get_message_name() == IMessageAvro.AddReservation:
            result = self.add_reservation(message)
        elif message.get_message_name() == IMessageAvro.AddReservations:
            result = self.add_reservations(message)
        elif message.get_message_name() == IMessageAvro.DemandReservation:
            result = self.demand_reservation(message)
        elif message.get_message_name() == IMessageAvro.GetActorsRequest:
            result = self.get_brokers(message)
        elif message.get_message_name() == IMessageAvro.GetPoolInfoRequest:
            result = self.get_pool_info(message)
        elif message.get_message_name() == IMessageAvro.ExtendReservation:
            result = self.extend_reservation(message)
        else:
            super().process(message)
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(callback_topic, result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def claim_resources(self, request: ClaimResourcesAvro) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.broker_id is None or request.reservation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if mo is None:
                print("Management object could not be found: guid: {} auth: {}".format(request.guid, auth))
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
                return result

            if request.slice_id is not None:
                result = mo.claim_resources_slice(ID(request.broker_id), ID(request.slice_id), ID(request.reservation_id), auth)
            else:
                result = mo.claim_resources(ID(request.broker_id), ID(request.reservation_id), auth)

            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            result.status.set_message(str(e))
            result.status.set_details(traceback.format_exc())

        return result

    def reclaim_resources(self, request: ReclaimResourcesAvro) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.broker_id is None or request.reservation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if mo is None:
                print("Management object could not be found: guid: {} auth: {}".format(request.guid, auth))
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
                return result

            result = mo.reclaim_resources(ID(request.broker_id), ID(request.reservation_id), auth)

            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            result.status.set_message(str(e))
            result.status.set_details(traceback.format_exc())

        return result

    def add_reservation(self, request: AddReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.get_reservation() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result.status = mo.add_reservation(request.reservation_obj, auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_reservations(self, request: AddReservationsAvro) -> ResultStringsAvro:
        result = ResultStringsAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.get_reservation() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result.status = mo.add_reservations(request.reservation_list, auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def demand_reservation(self, request: DemandReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        try:
            if request.guid is None or (request.reservation_id is None and request.reservation_obj is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)

                return result

            if request.reservation_obj is not None and request.reservation_id is not None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)

                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if request.reservation_id is not None:
                result.status = mo.demand_reservation_rid(ID(request.reservation_id), auth)
            else:
                result.status = mo.demand_reservation(request.reservation_obj, auth)

            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_brokers(self, request: GetActorsAvro) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.type != ActorType.Broker.name:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result = mo.get_brokers(auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_pool_info(self, request: GetPoolInfoAvro) -> ResultPoolInfoAvro:
        result = ResultPoolInfoAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            result = mo.get_pool_info(ID(request.broker_id), auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def extend_reservation(self, request: ExtendReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        try:
            if request.guid is None or (request.reservation_id is None and request.reservation_id is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)

                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            end_time = ActorClock.from_milliseconds(request.end_time)
            rtype = None
            if request.new_resource_type is not None:
                rtype = ResourceType(request.new_resource_type)

            result.status = mo.extend_reservation(ID(request.reservation_id), end_time, request.new_units, rtype,
                                           request.request_properties, request.config_properties, auth)
            result.message_id = request.message_id

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result