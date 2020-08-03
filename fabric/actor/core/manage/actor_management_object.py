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
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_actor_runnable import IActorRunnable
from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.common.exceptions import ReservationNotFoundException
from fabric.actor.core.kernel.reservation_factory import ReservationFactory
from fabric.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.manage.converter import Converter
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.manage.management_utils import ManagementUtils
from fabric.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric.actor.core.apis.i_actor_management_object import IActorManagementObject
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric.actor.core.manage.messages.result_event_mng import ResultEventMng
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.util.all_actor_events_filter import AllActorEventsFilter
from fabric.actor.core.util.id import ID
from fabric.actor.security.auth_token import AuthToken
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric.message_bus.messages.slice_avro import SliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_slice import ISlice


class ActorManagementObject(ManagementObject, IActorManagementObject):
    def __init__(self, actor: IActor = None):
        super().__init__()
        self.actor = actor
        self.db = None
        if actor is not None:
            self.set_actor(actor)

    def register_protocols(self):
        from fabric.actor.core.manage.local.local_actor import LocalActor
        local = ProxyProtocolDescriptor(Constants.ProtocolLocal, LocalActor.__name__, LocalActor.__module__)

        from fabric.actor.core.manage.kafka.kafka_actor import KafkaActor
        kakfa = ProxyProtocolDescriptor(Constants.ProtocolKafka, KafkaActor.__name__, KafkaActor.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kakfa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PropertyClassName] = ActorManagementObject.__name__,
        properties[Constants.PropertyModuleName] = ActorManagementObject.__name__

        return properties

    def recover(self):
        actor_name = None
        if Constants.PropertyActorName in self.serial:
            actor_name = self.serial[Constants.PropertyActorName]
        else:
            raise Exception("Missing actor name")

        actor = ActorRegistrySingleton.get().get_actor(actor_name)

        if actor is None:
            raise Exception("The managed actor does not exist")

        self.set_actor(actor)

    def set_actor(self, actor: IActor):
        if self.actor is None:
            self.actor = actor
            self.db = actor.get_plugin().get_database()
            self.logger = actor.get_logger()
            self.id = actor.get_guid()

    def get_slices(self, caller: AuthToken) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
        else:
            slice_list = None
            try:
                try:
                    slice_list = self.db.get_slices()
                except Exception as e:
                    self.logger.error("getSlices:db access {}".format(e))
                    result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                    result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                    result.status = ManagementObject.set_exception_details(result.status, e)
                if slice_list is not None:
                    result.slices = Translate.fill_slices(slice_list, True)
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchSlice.name)
            except Exception as e:
                self.logger.error("getSlices {}".format(e))
                result.status.set_code(ErrorCodes.ErrorInternalError.value)
                result.status.set_message(ErrorCodes.ErrorInternalError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def get_slice(self, slice_id: ID, caller: AuthToken) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if slice_id is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
        else:
            slice_obj = None
            try:
                try:
                    slice_obj = self.db.get_slice(slice_id)
                except Exception as e:
                    self.logger.error("getSlice:db access {}".format(e))
                    result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                    result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                    result.status = ManagementObject.set_exception_details(result.status, e)

                if slice_obj is not None:
                    slice_list = [slice_obj]
                    result.slices = Translate.fill_slices(slice_list, True)
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchSlice.name)
            except Exception as e:
                self.logger.error("getSlice {}".format(e))
                result.status.set_code(ErrorCodes.ErrorInternalError.value)
                result.status.set_message(ErrorCodes.ErrorInternalError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def add_slice(self, slice_mng: SliceAvro, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if slice_mng is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)

        else:
            try:
                data = Converter.get_resource_data(slice_mng)
                slice_obj = SliceFactory.create(ID(), name=slice_mng.get_slice_name(), data=data)

                slice_obj.set_description(slice_mng.get_description())
                slice_obj.set_owner(self.actor.get_identity())
                slice_obj.set_inventory(True)

                class Runner(IActorRunnable):
                    def __init__(self, actor: IActor):
                        self.actor = actor

                    def run(self):
                        try:
                            self.actor.register_slice(slice_obj)
                        except Exception as e:
                            self.actor.get_plugin().release_slice(slice_obj)
                            raise e

                        return None

                self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))

                result.set_result(str(slice_obj.get_slice_id()))
            except Exception as e:
                self.logger.error("addslice: {}".format(e))
                result.status.set_code(ErrorCodes.ErrorInternalError.value)
                result.status.set_message(ErrorCodes.ErrorInternalError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_slice(self, slice_id: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.remove_slice_by_slice_id(slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("remove_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def update_slice(self, slice_mng: SliceAvro, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_mng is None or slice_mng.get_slice_id() is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            slice_id = ID(slice_mng.get_slice_id())

            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    slice_obj = self.actor.get_slice(slice_id=slice_id)
                    if slice_obj is None:
                        result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                        result.set_message(ErrorCodes.ErrorNoSuchSlice.name)
                        return result
                    slice_obj = ManagementUtils.update_slice(slice_obj, slice_mng)

                    try:
                        self.actor.get_plugin().get_database().update_slice(slice_obj)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.name)
                        result = ManagementObject.set_exception_details(result, e)

                    return result

            result = self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("update_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def get_slice_by_id(self, id: int) -> ISlice:
        ss = self.db.get_slice_by_id(id)
        slice_obj = SliceFactory.create_instance(ss)
        return slice_obj

    def get_slice_by_guid(self, guid: str) -> ISlice:
        ss = self.db.get_slice(guid)
        slice_obj = SliceFactory.create_instance(ss)
        return slice_obj

    def get_reservations(self, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations()
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get('rsv_slc_id', None)
                    if slice_id is None:
                        self.logger.error("Inconsistent state reservation does not belong to a slice: {}".format(r))

                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_id(slice_id)
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj, self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_state(self, caller: AuthToken, state: int) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or state is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_state(state)
            except Exception as e:
                self.logger.error("get_reservations_by_state:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get('rsv_slc_id', None)
                    if slice_id is None:
                        self.logger.error("Inconsistent state reservation does not belong to a slice: {}".format(r))

                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_id(slice_id)
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj, self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("get_reservations_by_state: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservations_by_state: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_slice_id(self, caller: AuthToken, slice_id: ID) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or slice_id is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_slice_id(slice_id)
            except Exception as e:
                self.logger.error("get_reservations_by_slice_id:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get('rsv_slc_id', None)
                    if slice_id is None:
                        self.logger.error("Inconsistent state reservation does not belong to a slice: {}".format(r))

                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_id(slice_id)
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj, self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("get_reservations_by_slice_id: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservations_by_slice_id: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_slice_id_state(self, caller: AuthToken, slice_id: ID, state: int) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or slice_id is None or state is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_slice_id_state(slice_id, state)
            except Exception as e:
                self.logger.error("get_reservations_by_slice_id_state:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get('rsv_slc_id', None)
                    if slice_id is None:
                        self.logger.error("Inconsistent state reservation does not belong to a slice: {}".format(r))

                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_id(slice_id)
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj, self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("get_reservations_by_slice_id_state: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservations_by_slice_id_state: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservation(self, caller: AuthToken, rid: ID) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or rid is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_list = None
            try:
                res = self.db.get_reservation(rid)
                if res is not None:
                    res_list = [res]
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchReservation.name)
            except Exception as e:
                self.logger.error("get_reservation:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_id = r.get('rsv_slc_id', None)
                    if slice_id is None:
                        self.logger.error("Inconsistent state reservation does not belong to a slice: {}".format(r))

                    slice_obj = None
                    if slice_id is not None:
                        slice_obj = self.get_slice_by_id(slice_id)
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj, self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("get_reservation: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservation: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_reservation(self, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.remove_reservation(rid=rid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("remove_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("remove_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def close_reservation(self, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.close_by_rid(rid)
                    return True

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("close_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("close_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def close_slice_reservations(self, caller: AuthToken, slice_id: ID) -> ResultAvro:
        result = ResultAvro()

        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.close_slice_reservations(slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("close_slice_reservations: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def get_actor(self) -> IActor:
        return self.actor

    def get_actor_name(self) -> str:
        if self.actor is not None:
            return self.actor.get_name()

        return None

    def create_event_subscription(self, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            from fabric.actor.core.container.globals import GlobalsSingleton
            id = GlobalsSingleton.get().event_manager.create_subscription(caller, AllActorEventsFilter(self.actor.get_guid()))
            result.set_result(str(id))
        except Exception as e:
            self.logger.error("createEventSubscription {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def delete_event_subscription(self, caller: AuthToken, id: ID) -> ResultAvro:
        result = ResultAvro()

        if caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            from fabric.actor.core.container.globals import GlobalsSingleton
            GlobalsSingleton.get().event_manager.delete_subscription(id, caller)
        except Exception as e:
            self.logger.error("createEventSubscription {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def drain_events(self, caller: AuthToken, id: ID, timeout: int) -> ResultEventMng:
        return None

    def update_reservation(self, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if reservation is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            rid = ID(reservation.get_reservation_id())

            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    r = self.actor.get_reservation(rid)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.name)
                        return result
                    r = ManagementUtils.update_reservation(r, reservation)
                    r.set_dirty()

                    try:
                        self.actor.get_plugin().get_database().update_reservation(r)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.name)
                        result = ManagementObject.set_exception_details(result, e)
                    return result

            result = self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("update_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("update_reservation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def get_reservation_state(self, caller: AuthToken, rid: ID) -> ResultReservationStateAvro:
        result = ResultReservationStateAvro()
        result.status = ResultAvro()

        if rid is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            res_dict = None
            try:
                res_dict = self.db.get_reservation(rid)
            except Exception as e:
                self.logger.error("get_reservation_state:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            result.reservation_states = Converter.fill_reservation_state(res_dict)
        except ReservationNotFoundException as e:
            self.logger.error("get_reservation_state: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservation_state {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def get_reservation_state_for_reservations(self, caller: AuthToken, rids: list) -> ResultReservationStateAvro:
        result = ResultReservationStateAvro()
        result.status = ResultAvro()

        if rids is None or len(rids) == 0 or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        for rid in rids:
            if rid is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_rids(rids)
            except Exception as e:
                self.logger.error("get_reservation_state_for_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if len(res_list) == len(rids):
                result.reservation_states = Converter.fill_reservation_state(res_list)
            elif len(res_list) > len(rids):
                raise Exception("The database provided too many records")
            else:
                i = 0
                j = 0
                while i < len(rids):
                    properties = res_list[j]
                    if rids[i] == ReservationFactory.get_reservation_id(properties):
                        result.reservation_states.append(Converter.fill_reservation_state(res_list))
                        j += 1

                    else:
                        state = ReservationStateAvro()
                        state.set_state(ReservationStates.Unknown.value)
                        state.set_pending_state(ReservationPendingStates.Unknown.value)
                        result.reservation_states.append(state)
                    i += 1
        except ReservationNotFoundException as e:
            self.logger.error("get_reservation_state_for_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_reservation_state_for_reservations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result
