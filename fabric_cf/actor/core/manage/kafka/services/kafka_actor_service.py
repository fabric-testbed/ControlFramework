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

import json
import traceback
from datetime import datetime

from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.close_delegations_avro import CloseDelegationsAvro
from fabric_mb.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric_mb.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.maintenance_request_avro import MaintenanceRequestAvro
from fabric_mb.message_bus.messages.remove_delegation_avro import RemoveDelegationAvro
from fabric_mb.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric_mb.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.update_slice_avro import UpdateSliceAvro
from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_service import KafkaService
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.utils import translate_avro_message_type_pdp_action_id
from fabric_cf.actor.security.pdp_auth import ActionId


class KafkaActorService(KafkaService):
    def authorize_request(self, *, id_token: str, message_name: str, resource: BaseSliver = None,
                          lease_end_time: datetime = None) -> ResultStringAvro or None:
        """
        Authorize request
        :param id_token:
        :param message_name:
        :param resource:
        :param lease_end_time:
        :return:
        """
        result = None
        action_id = translate_avro_message_type_pdp_action_id(message_name=message_name)

        # Internal messages do not require authorization
        if action_id is ActionId.noop:
            return result

        try:
            from fabric_cf.actor.security.access_checker import AccessChecker
            AccessChecker.check_access(action_id=action_id, token=id_token, logger=self.logger,
                                       resource=resource, lease_end_time=lease_end_time)
        except Exception as e:
            self.logger.exception(e)
            result = ResultStringAvro()
            status = ResultAvro()
            status.status = ErrorCodes.unauthorized
            status.message = str(e)
            result.status = status
        return result

    def process(self, *, message: AbcMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        result = self.authorize_request(id_token=message.get_id_token(), message_name=message.get_message_name())

        # If authorization failed, return the result
        if result is None:
            if message.get_message_name() == AbcMessageAvro.get_slices_request:
                result = self.get_slices(request=message)

            elif message.get_message_name() == AbcMessageAvro.remove_slice:
                result = self.remove_slice(request=message)

            elif message.get_message_name() == AbcMessageAvro.add_slice:
                result = self.add_slice(request=message)

            elif message.get_message_name() == AbcMessageAvro.update_slice:
                result = self.update_slice(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_reservations_request:
                result = self.get_reservations(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_delegations:
                result = self.get_delegations(request=message)

            elif message.get_message_name() == AbcMessageAvro.remove_reservation:
                result = self.remove_reservation(request=message)

            elif message.get_message_name() == AbcMessageAvro.close_reservations:
                result = self.close_reservations(request=message)

            elif message.get_message_name() == AbcMessageAvro.update_reservation:
                result = self.update_reservation(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_reservations_state_request:
                result = self.get_reservation_state(request=message)

            elif message.get_message_name() == AbcMessageAvro.maintenance_request:
                result = self.maintenance_request(request=message)

            else:
                self.logger.debug("Unsupported Message, discarding it!")
                return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce(topic=callback_topic, record=result):
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
            states = SliceState.list_values_ex_closing_dead()
            if request.reservation_state is not None:
                if request.reservation_state == SliceState.All.value:
                    states = SliceState.list_values()
                else:
                    states = [request.reservation_state]

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))
            slice_id = ID(uid=request.get_slice_id()) if request.slice_id is not None else None
            result = mo.get_slices(slice_id=slice_id, caller=auth, slice_name=request.get_slice_name(),
                                   email=request.get_email(), state=states)

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

            result = mo.add_slice(slice_obj=request.slice_obj, caller=auth)
        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def update_slice(self, *, request: UpdateSliceAvro) -> ResultStringAvro:
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
            state = None
            if request.get_reservation_state() is not None and \
                    request.get_reservation_state() != Constants.ALL_RESERVATION_STATES:
                state = [request.get_reservation_state()]

            slice_id = ID(uid=request.slice_id) if request.slice_id is not None else None
            rid = ID(uid=request.reservation_id) if request.reservation_id is not None else None

            result = mo.get_reservations(caller=auth, state=state, slice_id=slice_id,
                                         rid=rid, email=request.get_email(), type=request.get_type(),
                                         site=request.get_site())

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
            result.status = mo.remove_reservation(caller=auth, rid=ID(uid=request.reservation_id))

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
                result.status = mo.close_slice_reservations(caller=auth, slice_id=ID(uid=request.slice_id))
            else:
                result.status = mo.close_reservation(caller=auth, rid=ID(uid=request.reservation_id))

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

            result = mo.get_reservation_state_for_reservations(caller=auth, rids=request.get_reservation_ids())

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

            slice_id = ID(uid=request.slice_id) if request.slice_id is not None else None
            result = mo.get_delegations(caller=auth, did=request.get_delegation_id(),
                                        state=request.get_delegation_state(), slice_id=slice_id)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def maintenance_request(self, *, request: MaintenanceRequestAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        try:
            if request.actor_guid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            mo = self.get_actor_mo(guid=ID(uid=request.actor_guid))
            sites = None
            if request.get_sites() is not None:
                sites = []
                for s in request.get_sites():
                    sites.append(Translate.translate_site_from_avro(site_avro=s))
            mo.update_maintenance_mode(properties=request.get_properties(), sites=sites)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def remove_delegation(self, *, request: RemoveDelegationAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or request.get_delegation_id() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))
            result.status = mo.remove_delegation(caller=auth, did=request.delegation_id)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result

    def close_delegations(self, *, request: CloseDelegationsAvro) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()
        result.message_id = request.message_id

        try:
            if request.guid is None or (request.get_slice_id() is None and request.get_delegation_id() is None):
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

            auth = Translate.translate_auth_from_avro(auth_avro=request.auth)
            mo = self.get_actor_mo(guid=ID(uid=request.guid))

            result.status = mo.close_delegation(caller=auth, did=request.delegation_id)

        except Exception as e:
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        result.message_id = request.message_id
        return result