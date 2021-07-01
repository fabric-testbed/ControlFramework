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

from datetime import date, datetime
from typing import TYPE_CHECKING, List

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ReservationNotFoundException, SliceNotFoundException, \
    DelegationNotFoundException, ManageException
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric_cf.actor.core.apis.abc_actor_management_object import ABCActorManagementObject
from fabric_cf.actor.security.access_checker import AccessChecker
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.security.auth_token import AuthToken

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice


class ActorManagementObject(ManagementObject, ABCActorManagementObject):
    def __init__(self, *, actor: ABCActorMixin = None):
        super().__init__()
        self.actor = actor
        self.db = None
        if actor is not None:
            self.set_actor(actor=actor)

    def register_protocols(self):
        from fabric_cf.actor.core.manage.local.local_actor import LocalActor
        local = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_LOCAL, proxy_class=LocalActor.__name__,
                                        proxy_module=LocalActor.__module__)

        from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
        kafka = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_KAFKA, proxy_class=KafkaActor.__name__,
                                        proxy_module=KafkaActor.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kafka)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PROPERTY_CLASS_NAME] = ActorManagementObject.__name__
        properties[Constants.PROPERTY_MODULE_NAME] = ActorManagementObject.__module__

        return properties

    def recover(self):
        actor_name = None
        if Constants.PROPERTY_ACTOR_NAME in self.serial:
            actor_name = self.serial[Constants.PROPERTY_ACTOR_NAME]
        else:
            raise ManageException(Constants.NOT_SPECIFIED_PREFIX.format("actor name"))

        actor = ActorRegistrySingleton.get().get_actor(actor_name_or_guid=actor_name)

        if actor is None:
            raise ManageException(Constants.OBJECT_NOT_FOUND.format("Managed Object", actor_name))

        self.set_actor(actor=actor)

    def set_actor(self, *, actor: ABCActorMixin):
        if self.actor is None:
            self.actor = actor
            self.db = actor.get_plugin().get_database()
            self.logger = actor.get_logger()
            self.id = actor.get_guid()

    def get_slices(self, *, slice_id: ID, caller: AuthToken, id_token: str = None,
                   slice_name: str = None, email: str = None) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
        else:
            slice_list = None
            user_dn = None
            user_email = email
            try:
                if id_token is not None:
                    fabric_token = AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.slice,
                                                              token=id_token, logger=self.logger,
                                                              actor_type=self.actor.get_type(),
                                                              resource_id=str(slice_id))
                    user_dn = fabric_token.get_decoded_token().get(Constants.CLAIMS_SUB, None)
                    user_email = fabric_token.get_decoded_token().get(Constants.CLAIMS_EMAIL, None)

                    if user_dn is None:
                        result.status.set_code(ErrorCodes.ErrorInvalidToken.value)
                        result.status.set_message(ErrorCodes.ErrorInvalidToken.interpret())
                        return result

                try:
                    slice_list = None
                    if slice_id is not None:
                        slice_obj = self.db.get_slice(slice_id=slice_id)
                        if slice_obj is not None:
                            slice_list = [slice_obj]

                    elif slice_name is not None:
                        slice_list = self.db.get_slice_by_name(slice_name=slice_name, oidc_claim_sub=user_dn,
                                                               email=user_email)
                    elif user_dn is not None:
                        slice_list = self.db.get_slice_by_oidc_claim_sub(oidc_claim_sub=user_dn)
                    elif user_email is not None:
                        slice_list = self.db.get_slice_by_email(email=user_email)
                    else:
                        slice_list = self.db.get_slices()

                except Exception as e:
                    self.logger.error("getSlices:db access {}".format(e))
                    result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                    result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                    result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                if slice_list is not None:
                    result.slices = Translate.fill_slices(slice_list=slice_list, full=True)
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchSlice.interpret())
            except Exception as e:
                self.logger.error("getSlices {}".format(e))
                result.status.set_code(ErrorCodes.ErrorInternalError.value)
                result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
        return result

    def add_slice(self, *, slice_obj: SliceAvro, caller: AuthToken, id_token: str = None) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if slice_obj is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())

        else:
            try:
                user_dn = None
                user_email = None
                if id_token is not None:
                    fabric_token = AccessChecker.check_access(action_id=ActionId.query,
                                                              resource_type=ResourceType.slice,
                                                              token=id_token, logger=self.logger,
                                                              actor_type=self.actor.get_type(),
                                                              resource_id=str(slice_obj.slice_name))
                    user_dn = fabric_token.get_decoded_token().get(Constants.CLAIMS_SUB, None)
                    user_email = fabric_token.get_decoded_token().get(Constants.CLAIMS_EMAIL, None)

                    if user_dn is None:
                        result.status.set_code(ErrorCodes.ErrorInvalidToken.value)
                        result.status.set_message(ErrorCodes.ErrorInvalidToken.interpret())
                        return result

                slice_obj_new = SliceFactory.create(slice_id=ID(), name=slice_obj.get_slice_name())

                slice_obj_new.set_description(description=slice_obj.get_description())
                slice_obj_new.set_owner(owner=self.actor.get_identity())
                slice_obj_new.get_owner().set_oidc_sub_claim(oidc_sub_claim=user_dn)
                slice_obj_new.get_owner().set_email(email=user_email)
                slice_obj_new.set_graph_id(graph_id=slice_obj.graph_id)
                slice_obj_new.set_config_properties(value=slice_obj.get_config_properties())
                slice_obj_new.set_lease_end(lease_end=slice_obj.get_lease_end())
                slice_obj_new.set_lease_start(lease_start=datetime.utcnow())

                if slice_obj.get_inventory():
                    slice_obj_new.set_inventory(value=True)

                elif slice_obj.is_broker_client_slice():
                    slice_obj.set_broker_client_slice(value=True)

                elif slice_obj.is_client_slice():
                    slice_obj.set_client_slice(value=True)

                class Runner(ABCActorRunnable):
                    def __init__(self, *, actor: ABCActorMixin):
                        self.actor = actor

                    def run(self):
                        try:
                            self.actor.register_slice(slice_object=slice_obj_new)
                        except Exception as e:
                            self.actor.get_plugin().release_slice(slice_obj=slice_obj_new)
                            raise e

                        return None

                self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

                result.set_result(str(slice_obj_new.get_slice_id()))
            except Exception as e:
                self.logger.error("add_slice: {}".format(e))
                result.status.set_code(ErrorCodes.ErrorInternalError.value)
                result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def remove_slice(self, *, slice_id: ID, caller: AuthToken, id_token: str = None) -> ResultAvro:
        result = ResultAvro()
        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.delete,
                                           resource_type=ResourceType.slice,
                                           token=id_token, logger=self.logger,
                                           actor_type=self.actor.get_type(),
                                           resource_id=str(slice_id))

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.remove_slice_by_slice_id(slice_id=slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except Exception as e:
            self.logger.error("remove_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)
        return result

    def update_slice(self, *, slice_mng: SliceAvro, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_mng is None or slice_mng.get_slice_id() is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            slice_id = ID(uid=slice_mng.get_slice_id())

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    slice_obj = self.actor.get_slice(slice_id=slice_id)
                    if slice_obj is None:
                        result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                        result.set_message(ErrorCodes.ErrorNoSuchSlice.interpret())
                        return result
                    slice_obj = ManagementUtils.update_slice(slice_obj=slice_obj, slice_mng=slice_mng)
                    slice_obj.set_dirty()

                    try:
                        self.actor.get_plugin().get_database().update_slice(slice_object=slice_obj)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                        result = ManagementObject.set_exception_details(result=result, e=e)

                    return result

            result = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except Exception as e:
            self.logger.error("update_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_slice_by_guid(self, *, guid: str, id_token: str = None) -> ABCSlice:
        return self.db.get_slice(slice_id=guid)

    def get_reservations(self, *, caller: AuthToken, id_token: str = None, state: int = None,
                         slice_id: ID = None, rid: ID = None, oidc_claim_sub: str = None,
                         email: str = None, rid_list: List[str] = None) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            user_dn = oidc_claim_sub
            user_email = email
            if id_token is not None:
                fabric_token = AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.sliver,
                                                          token=id_token, logger=self.logger,
                                                          actor_type=self.actor.get_type(),
                                                          resource_id=str(rid))
                user_dn = fabric_token.get_decoded_token().get(Constants.CLAIMS_SUB, None)
                user_email = fabric_token.get_decoded_token().get(Constants.CLAIMS_EMAIL, None)

                if user_dn is None:
                    result.status.set_code(ErrorCodes.ErrorInvalidToken.value)
                    result.status.set_message(ErrorCodes.ErrorInvalidToken.interpret())
                    return result

            res_list = None
            try:
                if rid is not None:
                    res = self.db.get_reservation(rid=rid)
                    if res is not None:
                        res_list = [res]
                    else:
                        result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.status.set_message(ErrorCodes.ErrorNoSuchReservation.interpret())
                elif slice_id is not None and state is not None:
                    res_list = self.db.get_reservations_by_slice_id_state(slice_id=slice_id, state=state)
                elif slice_id is not None:
                    res_list = self.db.get_reservations_by_slice_id(slice_id=slice_id)
                elif state is not None:
                    res_list = self.db.get_reservations_by_state(state=state)
                elif user_dn is not None:
                    res_list = self.db.get_reservations_by_oidc_claim_sub(oidc_claim_sub=user_dn)
                elif user_email is not None:
                    if state is None:
                        res_list = self.db.get_reservations_by_email(email=user_email)
                    else:
                        res_list = self.db.get_reservations_by_email_state(email=user_email, state=state)
                elif rid_list is not None:
                    res_list = self.db.get_reservations_by_rids(rid=rid_list)
                else:
                    res_list = self.db.get_reservations()
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get_slice_id()
                    slice_obj = self.get_slice_by_guid(guid=slice_id)
                    r.restore(actor=self.actor, slice_obj=slice_obj)
                    rr = Converter.fill_reservation(reservation=r, full=True)
                    result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def remove_reservation(self, *, caller: AuthToken, rid: ID, id_token: str = None) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.delete,
                                           resource_type=ResourceType.sliver,
                                           token=id_token, logger=self.logger,
                                           actor_type=self.actor.get_type(),
                                           resource_id=str(rid))

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.remove_reservation(rid=rid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("remove_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("remove_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def close_reservation(self, *, caller: AuthToken, rid: ID, id_token: str = None) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.close,
                                           resource_type=ResourceType.sliver,
                                           token=id_token, logger=self.logger,
                                           actor_type=self.actor.get_type(),
                                           resource_id=str(rid))

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.close_by_rid(rid=rid)
                    return True

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("close_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("close_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def close_slice_reservations(self, *, caller: AuthToken, slice_id: ID, id_token: str = None) -> ResultAvro:
        result = ResultAvro()

        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.close,
                                           resource_type=ResourceType.slice,
                                           token=id_token, logger=self.logger,
                                           actor_type=self.actor.get_type(),
                                           resource_id=str(slice_id))

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.close_slice_reservations(slice_id=slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except SliceNotFoundException as e:
            self.logger.error("close_slice_reservations: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
            result.set_message(e.text)
            result = ManagementObject.set_exception_details(result=result, e=e)
        except Exception as e:
            self.logger.error("close_slice_reservations: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)
        return result

    def get_actor(self) -> ABCActorMixin:
        return self.actor

    def get_actor_name(self) -> str:
        if self.actor is not None:
            return self.actor.get_name()

        return None

    def update_reservation(self, *, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if reservation is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            rid = ID(uid=reservation.get_reservation_id())

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    r = self.actor.get_reservation(rid=rid)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.interpret())
                        return result
                    r = ManagementUtils.update_reservation(res_obj=r, rsv_mng=reservation)
                    r.set_dirty()

                    try:
                        self.actor.get_plugin().get_database().update_reservation(reservation=r)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                        result = ManagementObject.set_exception_details(result=result, e=e)
                    return result

            result = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("update_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("update_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_reservation_state_for_reservations(self, *, caller: AuthToken, rids: List[str],
                                               id_token: str = None) -> ResultReservationStateAvro:
        result = ResultReservationStateAvro()
        result.status = ResultAvro()

        if rids is None or len(rids) == 0 or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        for rid in rids:
            if rid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.sliver,
                                           token=id_token, logger=self.logger, actor_type=self.actor.get_type())

            res_list = None
            try:
                res_list = self.db.get_reservations_by_rids(rid=rids)
            except Exception as e:
                self.logger.error("get_reservation_state_for_reservations db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            result.reservation_states = []
            for r in res_list:
                result.reservation_states.append(Converter.fill_reservation_state(res=r))

        except ReservationNotFoundException as e:
            self.logger.error("get_reservation_state_for_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservation_state_for_reservations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_delegations(self, *, caller: AuthToken, id_token: str = None, slice_id: ID = None,
                        did: str = None, state: int = None) -> ResultDelegationAvro:
        result = ResultDelegationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            if id_token is not None:
                AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.delegation,
                                           token=id_token, logger=self.logger, actor_type=self.actor.get_type())
            dlg_list = None
            try:
                if did is not None:
                    dlg = self.db.get_delegation(dlg_graph_id=did)
                    if dlg is not None:
                        dlg_list = [dlg]
                    else:
                        result.status.set_code(ErrorCodes.ErrorNoSuchDelegation.value)
                        result.status.set_message(ErrorCodes.ErrorNoSuchDelegation.interpret())
                elif slice_id is not None:
                    dlg_list = self.db.get_delegations_by_slice_id(slice_id=slice_id, state=state)
                else:
                    dlg_list = self.db.get_delegations(state=state)
            except Exception as e:
                self.logger.error("get_delegations db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret())
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)

            if dlg_list is not None:
                result.delegations = []
                for d in dlg_list:
                    slice_id = d.get_slice_id()
                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_guid(guid=slice_id)
                    d.restore(actor=self.actor, slice_obj=slice_obj)
                    dd = Translate.translate_delegation_to_avro(delegation=d)
                    result.delegations.append(dd)
        except DelegationNotFoundException as e:
            self.logger.error("get_delegations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchDelegation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("getDelegations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result
