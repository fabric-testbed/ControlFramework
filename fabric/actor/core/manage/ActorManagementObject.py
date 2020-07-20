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

from fabric.actor.core.apis.IActorRunnable import IActorRunnable
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.kernel.ReservationFactory import ReservationFactory
from fabric.actor.core.kernel.ReservationStates import ReservationStates, ReservationPendingStates
from fabric.actor.core.kernel.SliceFactory import SliceFactory
from fabric.actor.core.manage.Converter import Converter
from fabric.actor.core.manage.ManagementObject import ManagementObject
from fabric.actor.core.manage.ManagementUtils import ManagementUtils
from fabric.actor.core.manage.ProxyProtocolDescriptor import ProxyProtocolDescriptor
from fabric.actor.core.apis.IActorManagementObject import IActorManagementObject
from fabric.message_bus.messages.ReservationMng import ReservationMng
from fabric.message_bus.messages.ReservationStateAvro import ReservationStateMng
from fabric.actor.core.manage.messages.ResultEventMng import ResultEventMng
from fabric.actor.core.manage.messages.ResultReservationMng import ResultReservationMng
from fabric.actor.core.manage.messages.ResultReservationStateMng import ResultReservationStateMng
from fabric.actor.core.manage.messages.ResultSliceMng import ResultSliceMng
from fabric.actor.core.manage.messages.ResultStringMng import ResultStringMng
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.actor.core.registry.ActorRegistry import ActorRegistrySingleton
from fabric.actor.core.util.AllActorEventsFilter import AllActorEventsFilter
from fabric.actor.core.util.ID import ID
from fabric.actor.security.AuthToken import AuthToken
from fabric.message_bus.messages.ResultAvro import ResultAvro
from fabric.message_bus.messages.SliceAvro import SliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.IActor import IActor
    from fabric.actor.core.apis.ISlice import ISlice


class ActorManagementObject(ManagementObject, IActorManagementObject):
    def __init__(self, actor: IActor = None):
        super().__init__()
        self.actor = actor
        self.db = None
        if actor is not None:
            self.set_actor(actor)

    def register_protocols(self):
        from fabric.actor.core.manage.local.LocalActor import LocalActor
        local = ProxyProtocolDescriptor(Constants.ProtocolLocal, LocalActor.__name__, LocalActor.__module__)

        from fabric.actor.core.manage.kafka.KafkaActor import KafkaActor
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

    def get_slices(self, caller: AuthToken) -> ResultSliceMng:
        result = ResultSliceMng()
        result.set_status(ResultAvro())

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
        else:
            slice_list = None
            try:
                try:
                    slice_list = self.db.get_slices()
                except Exception as e:
                    self.logger.error("getSlices:db access {}".format(e))
                    result.status.set_code(Constants.ErrorDatabaseError)
                    result.status = ManagementObject.set_exception_details(result.status, e)
                if slice_list is not None:
                    result.result = Translate.fill_slices(slice_list, True)
            except Exception as e:
                self.logger.error("getSlices {}".format(e))
                result.status.set_code(Constants.ErrorInternalError)
                result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def get_slice(self, slice_id: ID, caller: AuthToken):
        result = ResultSliceMng()
        result.set_status(ResultAvro())

        if slice_id is None or caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
        else:
            slice_obj = None
            try:
                try:
                    slice_obj = self.db.get_slice(slice_id)
                except Exception as e:
                    self.logger.error("getSlice:db access {}".format(e))
                    result.status.set_code(Constants.ErrorDatabaseError)
                    result.status = ManagementObject.set_exception_details(result.status, e)

                if slice_obj is not None:
                    slice_list = [slice_obj]
                    result.result = Translate.fill_slices(slice_list, True)
            except Exception as e:
                self.logger.error("getSlice {}".format(e))
                result.status.set_code(Constants.ErrorInternalError)
                result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def add_slice(self, slice_mng: SliceAvro, caller: AuthToken) -> ResultStringMng:
        result = ResultStringMng()
        result.set_status(ResultAvro())

        if slice_mng is None or caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)

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
                result.status.set_code(Constants.ErrorInternalError)
                result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_slice(self, slice_id: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_id is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.remove_slice(slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("remove: {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def update_slice(self, slice_mng: SliceAvro, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_mng is None or slice_mng.get_slice_id() is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
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
                        result.set_code(Constants.ErrorNoSuchSlice)
                        return result
                    slice_obj = ManagementUtils.update_slice(slice_obj, slice_mng)

                    try:
                        self.actor.get_plugin().get_database().update_slice(slice_obj)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(Constants.ErrorDatabaseError)
                        result = ManagementObject.set_exception_details(result, e)

                    return result

            result = self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("update Slice: {}".format(e))
            result.set_code(Constants.ErrorInternalError)
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

    def get_reservations(self, caller: AuthToken) -> ResultReservationMng:
        result = ResultReservationMng()
        result.set_status(ResultAvro())

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations()
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.result = []
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
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_state(self, caller: AuthToken, state: int) -> ResultReservationMng:
        result = ResultReservationMng()
        result.set_status(ResultAvro())

        if caller is None or state is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_state(state)
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.result = []
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
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_slice_id(self, caller: AuthToken, slice_id: ID) -> ResultReservationMng:
        result = ResultReservationMng()
        result.set_status(ResultAvro())

        if caller is None or slice_id is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_slice_id(slice_id)
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.result = []
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
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservations_by_slice_id_state(self, caller: AuthToken, slice_id: ID, state: int) -> ResultReservationMng:
        result = ResultReservationMng()
        result.set_status(ResultAvro())

        if caller is None or slice_id is None or state is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_slice_id_state(slice_id, state)
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.result = []
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
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservation(self, caller: AuthToken, rid: ID) -> ResultReservationMng:
        result = ResultReservationMng()
        result.set_status(ResultAvro())

        if caller is None or rid is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res = self.db.get_reservation(rid)
                res_list = [res]
            except Exception as e:
                self.logger.error("getReservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)

            if res_list is not None:
                result.result = []
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
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def remove_reservation(self, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.remove_reservation(rid=rid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("remove reservation {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def close_reservation(self, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.close_by_rid(rid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("close reservation {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def close_slice_reservations(self, caller: AuthToken, slice_id: ID) -> ResultAvro:
        result = ResultAvro()

        if slice_id is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
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
            self.logger.error("close reservation {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def get_actor(self) -> IActor:
        return self.actor

    def get_actor_name(self) -> str:
        if self.actor is not None:
            return self.actor.get_name()

        return None

    def create_event_subscription(self, caller: AuthToken) -> ResultStringMng:
        result = ResultStringMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            from fabric.actor.core.container.Globals import GlobalsSingleton
            id = GlobalsSingleton.get().event_manager.create_subscription(caller, AllActorEventsFilter(self.actor.get_guid()))
            result.set_result(str(id))
        except Exception as e:
            self.logger.error("createEventSubscription {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def delete_event_subscription(self, caller: AuthToken, id: ID) -> ResultAvro:
        result = ResultAvro()

        if caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            from fabric.actor.core.container.Globals import GlobalsSingleton
            GlobalsSingleton.get().event_manager.delete_subscription(id, caller)
        except Exception as e:
            self.logger.error("createEventSubscription {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)
        return result

    def drain_events(self, caller: AuthToken, id: ID, timeout: int) -> ResultEventMng:
        return None

    def update_reservation(self, reservation: ReservationMng, caller: AuthToken):
        result = ResultAvro()
        if reservation is None or caller is None:
            result.set_code(Constants.ErrorInvalidArguments)
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
                        result.set_code(Constants.ErrorNoSuchReservation)
                        return result
                    r = ManagementUtils.update_reservation(r, reservation)
                    r.set_dirty()

                    try:
                        self.actor.get_plugin().get_database().update_reservation(r)
                    except Exception as e:
                        print("Could not commit slice update {}".format(e))
                        result.set_code(Constants.ErrorDatabaseError)
                        result = ManagementObject.set_exception_details(result, e)

                    return result

            result = self.actor.execute_on_actor_thread_and_wait(Runner(self.actor))
        except Exception as e:
            self.logger.error("update Slice: {}".format(e))
            result.set_code(Constants.ErrorInternalError)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def get_reservation_state(self, caller: AuthToken, rid: ID) -> ResultReservationStateMng:
        result = ResultReservationStateMng()
        result.status = ResultAvro()

        if rid is None or caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        try:
            res_list = None
            try:
                res = self.db.get_reservation(rid)
                res_list = [res]
            except Exception as e:
                self.logger.error("getReservationState:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            result.result = Converter.fill_reservation_state(res_list)
        except Exception as e:
            self.logger.error("getReservationState {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)
        return result

    def get_reservation_state_for_reservations(self, caller: AuthToken, rids: list) -> ResultReservationStateMng:
        result = ResultReservationStateMng()
        result.status = ResultAvro()

        if rids is None or len(rids) == 0 or caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result

        for rid in rids:
            if rid is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

        try:
            res_list = None
            try:
                res_list = self.db.get_reservations_by_rids(rids)
            except Exception as e:
                self.logger.error("get_reservation_state_for_reservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if len(res_list) == len(rids):
                result.result = Converter.fill_reservation_state(res_list)
            elif len(res_list) > len(rids):
                raise Exception("The database provided too many records")
            else:
                i = 0
                j = 0
                while i < len(rids):
                    properties = res_list[j]
                    if rids[i] == ReservationFactory.get_reservation_id(properties):
                        result.result.append(Converter.fill_reservation_state(res_list))
                        j += 1

                    else:
                        state = ReservationStateMng()
                        state.set_state(ReservationStates.Unknown.value)
                        state.set_pending_state(ReservationPendingStates.Unknown.value)
                        result.result.append(state)
                    i += 1

        except Exception as e:
            self.logger.error("get_reservation_state_for_reservations {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result
