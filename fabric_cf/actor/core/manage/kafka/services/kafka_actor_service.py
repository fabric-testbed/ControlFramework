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

from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric_mb.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric_mb.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric_mb.message_bus.messages.message import IMessageAvro

from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_service import KafkaService
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.util.id import ID


class KafkaActorService(KafkaService):
    def process(self, *, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == IMessageAvro.get_slices_request:
            result = self.get_slices(request=message)

        elif message.get_message_name() == IMessageAvro.remove_slice:
            result = self.remove_slice(request=message)

        elif message.get_message_name() == IMessageAvro.add_slice:
            result = self.add_slice(request=message)

        elif message.get_message_name() == IMessageAvro.update_slice:
            result = self.update_slice(request=message)

        elif message.get_message_name() == IMessageAvro.get_reservations_request:
            result = self.get_reservations(request=message)

        elif message.get_message_name() == IMessageAvro.get_delegations:
            result = self.get_delegations(request=message)

        elif message.get_message_name() == IMessageAvro.remove_reservation:
            result = self.remove_reservation(request=message)

        elif message.get_message_name() == IMessageAvro.close_reservations:
            result = self.close_reservations(request=message)

        elif message.get_message_name() == IMessageAvro.update_reservation:
            result = self.update_reservation(request=message)

        elif message.get_message_name() == IMessageAvro.get_reservations_state_request:
            result = self.get_reservation_state(request=message)

        else:
            self.logger.debug("Unsupported Message, discarding it!")
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(topic=callback_topic, record=result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

    def get_actor_mo(self, *, guid: ID):
        try:
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            return GlobalsSingleton.get().get_container().get_management_object_manager().get_management_object(key=guid)
        except Exception as e:
            raise ManageException("Invalid actor id={}! e={}".format(guid, e))

    def get_slices(self, *, request: GetSlicesRequestAvro) -> ResultSliceAvro:
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
            slice_id = None
            if request.slice_id is not None:
                slice_id = ID(uid=request.slice_id)
            result = mo.get_slices(slice_id=slice_id, caller=auth, id_token=request.get_id_token(),
                                   slice_name=request.slice_name, email=request.get_email())

        except Exception as e:
            self.logger.error(traceback.format_exc())
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def add_slice(self, *, request: AddSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.add_slice(slice_obj=request.slice_obj, caller=auth, id_token=request.id_token)
        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def update_slice(self, *, request: AddSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None and request.slice_obj is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result.status = mo.update_slice(request.slice_obj, auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def remove_slice(self, *, request: RemoveSliceAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None and request.slice_id is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result.status = mo.remove_slice(slice_id=ID(uid=request.slice_id), caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_reservations(self, *, request: GetReservationsRequestAvro) -> ResultReservationAvro:
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

            slice_id = None
            if request.slice_id is not None:
                slice_id = ID(uid=request.slice_id)

            rid = None
            if request.get_reservation_id() is not None:
                rid = ID(uid=request.get_reservation_id())

            if rid is not None:
                result = mo.get_reservations(caller=auth, rid=rid, id_token=request.get_id_token())

            elif slice_id is not None:

                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.ALL_RESERVATION_STATES:

                    result = mo.get_reservations(caller=auth, slice_id=slice_id,
                                                 state=request.get_reservation_state(),
                                                 id_token=request.get_id_token())

                else:
                    result = mo.get_reservations(caller=auth, slice_id=slice_id,
                                                 id_token=request.get_id_token())

            else:
                if request.get_reservation_state() is not None and \
                        request.get_reservation_state() != Constants.ALL_RESERVATION_STATES:

                    result = mo.get_reservations(caller=auth, state=request.get_reservation_state(),
                                                 id_token=request.get_id_token(), email=request.get_email())

                else:
                    result = mo.get_reservations(caller=auth, id_token=request.get_id_token(),
                                                 email=request.get_email())

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def update_reservation(self, *, request: UpdateReservationAvro) -> ResultStringAvro:
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

            result.status = mo.update_reservation(reservation=request.reservation_obj, caller=auth)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def remove_reservation(self, *, request: RemoveReservationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.get_reservation_id() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))
            result.status = mo.remove_reservation(caller=auth, rid=ID(uid=request.reservation_id),
                                                  id_token=request.id_token)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def close_reservations(self, *, request: CloseReservationsAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or (request.get_slice_id() is None and request.get_reservation_id() is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            if request.get_slice_id() is not None:
                result.status = mo.close_slice_reservations(caller=auth, slice_id=ID(uid=request.slice_id),
                                                            id_token=request.id_token)
            else:
                result.status = mo.close_reservation(caller=auth, rid=ID(uid=request.reservation_id),
                                                     id_token=request.id_token)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_reservation_state(self, *, request: GetReservationsStateRequestAvro) -> ResultReservationStateAvro:
        result = ResultReservationStateAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.get_reservation_ids() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result = mo.get_reservation_state_for_reservations(caller=auth, rids=request.get_reservation_ids(),
                                                               id_token=request.get_id_token())

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def get_delegations(self, *, request: GetDelegationsAvro) -> ResultDelegationAvro:
        result = ResultDelegationAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            if request.get_delegation_id() is not None:
                result = mo.get_delegations(caller=auth, did=ID(uid=request.get_delegation_id()),
                                            id_token=request.get_id_token(), state=request.delegation_state)

            elif request.get_slice_id() is not None:
                result = mo.get_delegations(caller=auth, slice_id=ID(uid=request.get_slice_id()),
                                            id_token=request.get_id_token(), state=request.delegation_state)

            else:
                result = mo.get_delegations(caller=auth, id_token=request.get_id_token(),
                                            state=request.delegation_state)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result
