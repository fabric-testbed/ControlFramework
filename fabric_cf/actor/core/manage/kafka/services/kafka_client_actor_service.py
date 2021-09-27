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

from fabric_mb.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.get_broker_query_model_request_avro import GetBrokerQueryModelRequestAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.reclaim_resources_avro import ReclaimResourcesAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_broker_query_model_avro import ResultBrokerQueryModelAvro
from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric_mb.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric_mb.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric_mb.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric_mb.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_strings_avro import ResultStringsAvro
from fim.user import GraphFormat

from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.common.constants import ErrorCodes
from fabric_cf.actor.core.manage.kafka.services.kafka_actor_service import KafkaActorService
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.util.id import ID


class KafkaClientActorService(KafkaActorService):
    def process(self, *, message: AbcMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == AbcMessageAvro.claim_resources:
            result = self.claim(request=message)

        elif message.get_message_name() == AbcMessageAvro.reclaim_resources:
            result = self.reclaim(request=message)

        elif message.get_message_name() == AbcMessageAvro.add_reservation:
            result = self.add_reservation(request=message)

        elif message.get_message_name() == AbcMessageAvro.add_reservations:
            result = self.add_reservations(request=message)

        elif message.get_message_name() == AbcMessageAvro.demand_reservation:
            result = self.demand_reservation(request=message)

        elif message.get_message_name() == AbcMessageAvro.get_actors_request:
            result = self.get_brokers(request=message)

        elif message.get_message_name() == AbcMessageAvro.get_broker_query_model_request:
            result = self.get_broker_query_model(request=message)

        elif message.get_message_name() == AbcMessageAvro.extend_reservation:
            result = self.extend_reservation(request=message)
        else:
            super().process(message=message)
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce(callback_topic, result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def add_reservation(self, *, request: AddReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.get_reservation() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result.status = mo.add_reservation(reservation=request.reservation_obj, caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def add_reservations(self, *, request: AddReservationsAvro) -> ResultStringsAvro:
        result = ResultStringsAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.get_reservation() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result.status = mo.add_reservations(reservations=request.reservation_list, caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def demand_reservation(self, *, request: DemandReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or (request.reservation_id is None and request.reservation_obj is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())

                return result

            if request.reservation_obj is not None and request.reservation_id is not None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())

                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            if request.reservation_id is not None:
                result.status = mo.demand_reservation_rid(rid=ID(uid=request.reservation_id), caller=auth)
            else:
                result.status = mo.demand_reservation(reservation=request.reservation_obj, caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_brokers(self, *, request: GetActorsRequestAvro) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.type != ActorType.Broker.name:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.get_brokers(caller=auth, id_token=request.get_id_token(), broker_id=request.broker_id)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_broker_query_model(self, *, request: GetBrokerQueryModelRequestAvro) -> ResultBrokerQueryModelAvro:
        result = ResultBrokerQueryModelAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            graph_format = GraphFormat(request.graph_format)

            result = mo.get_broker_query_model(broker=ID(uid=request.broker_id), caller=auth,
                                               id_token=request.get_id_token(), graph_format=graph_format)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def extend_reservation(self, *, request: ExtendReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.reservation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())

                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            end_time = ActorClock.from_milliseconds(milli_seconds=request.end_time)
            rtype = None
            if request.new_resource_type is not None:
                rtype = ResourceType(resource_type=request.new_resource_type)

            result.status = mo.extend_reservation(rid=ID(uid=request.reservation_id), new_end_time=end_time,
                                                  new_units=request.new_units, new_resource_type=rtype,
                                                  request_properties=request.request_properties,
                                                  config_properties=request.config_properties, caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def claim(self, *, request: ClaimResourcesAvro):
        result = ResultDelegationAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.broker_id is None or request.delegation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            if mo is None:
                self.logger.debug("Management object could not be found: guid: {} auth: {}".format(request.guid, auth))
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
                return result

            result = mo.claim_delegations(broker=ID(uid=request.broker_id), did=request.delegation_id, caller=auth,
                                          id_token=request.id_token)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            result.status.set_message(str(e))
            result.status.set_details(traceback.format_exc())

        result.message_id = request.message_id
        return result

    def reclaim(self, *, request: ReclaimResourcesAvro):
        result = ResultDelegationAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.broker_id is None or request.delegation_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            if mo is None:
                self.logger.debug("Management object could not be found: guid: {} auth: {}".format(request.guid, auth))
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
                return result

            result = mo.reclaim_delegations(broker=ID(uid=request.broker_id), did=request.delegation_id, caller=auth,
                                            id_token=request.id_token)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            result.status.set_message(str(e))
            result.status.set_details(traceback.format_exc())

        result.message_id = request.message_id
        return result
