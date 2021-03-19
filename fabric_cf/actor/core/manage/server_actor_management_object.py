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

from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro

from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.actor_management_object import ActorManagementObject
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.messages.result_client_mng import ResultClientMng
from fabric_cf.actor.core.proxies.kafka.translate import Translate

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro

    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.apis.abc_server_actor import ABCServerActor
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.manage.messages.client_mng import ClientMng


class ServerActorManagementObject(ActorManagementObject):
    def __init__(self, *, sa: ABCServerActor = None):
        super().__init__(actor=sa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PROPERTY_CLASS_NAME] = ServerActorManagementObject.__name__
        properties[Constants.PROPERTY_MODULE_NAME] = ServerActorManagementObject.__module__
        return properties

    def get_reservations_by_category(self, *, caller: AuthToken, category: ReservationCategory, slice_id: ID = None,
                                     id_token: str = None) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result
        try:
            res_list = None
            try:
                if category == ReservationCategory.Client:
                    if slice_id is None:
                        res_list = self.db.get_client_reservations()
                    else:
                        res_list = self.db.get_client_reservations_by_slice_id(slice_id=slice_id)
                elif category == ReservationCategory.Broker:
                    res_list = self.db.get_broker_reservations()
                elif category == ReservationCategory.Inventory:
                    if slice_id is None:
                        res_list = self.db.get_holdings()
                    else:
                        res_list = self.db.get_holdings_by_slice_id(slice_id=slice_id)
            except Exception as e:
                self.logger.error("do_get_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                for r in res_list:
                    slice_obj = self.get_slice_by_guid(guid=r.get_slice_id())
                    r.restore(actor=self.actor, slice_obj=slice_obj, logger=self.logger)
                    rr = Converter.fill_reservation(reservation=r, full=True)
                    result.result.append(rr)
        except Exception as e:
            self.logger.error("do_get_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_slices_by_slice_type(self, *, caller: AuthToken, slice_type: SliceTypes,
                                 id_token: str = None) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            slc_list = None

            try:
                if slice_type == SliceTypes.ClientSlice:
                    slc_list = self.db.get_client_slices()
                elif slice_type == SliceTypes.InventorySlice:
                    slc_list = self.db.get_inventory_slices()

            except Exception as e:
                self.logger.error("get_slices_by_slice_type:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if slc_list is not None:
                result.result = Translate.fill_slices(slice_list=slc_list, full=True)

        except Exception as e:
            self.logger.error("get_slices_by_slice_type: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def add_client_slice(self, *, caller: AuthToken, slice_mng: SliceAvro) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None or slice_mng is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            owner_mng = slice_mng.get_owner()
            owner = None
            owner_is_ok = False

            if owner_mng is not None:
                owner = Translate.translate_auth_from_avro(auth_avro=owner_mng)
                if owner is not None and owner.get_name() is not None and owner.get_guid() is not None:
                    owner_is_ok = True

            if not owner_is_ok:
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret())
                return result

            slice_obj = SliceFactory.create(slice_id=ID(), name=slice_mng.get_slice_name())
            slice_obj.set_description(description=slice_mng.get_description())
            slice_obj.set_inventory(value=False)

            assert owner is not None
            slice_obj.set_owner(owner=owner)

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    try:
                        self.actor.register_slice(slice_object=slice_obj)
                    except Exception as e:
                        self.actor.get_plugin().release_slice(slice_obj=slice_obj)
                        raise e

                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

            result.result = str(slice_obj.get_slice_id())

        except Exception as e:
            self.logger.error("addClientSlice: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def register_client(self, *, client: ClientMng, kafka_topic: str, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if client is None or kafka_topic is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            client_obj = Converter.fill_client(client_mng=client)
            client_obj.set_kafka_topic(kafka_topic=kafka_topic)

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.register_client(client=client_obj)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

        except Exception as e:
            self.logger.error("register_client: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_clients(self, *, caller: AuthToken, guid: ID = None, id_token: str = None) -> ResultClientMng:
        result = ResultClientMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            cl_list = None
            if guid is None:
                cl_list = self.db.get_clients()
            else:
                cl = self.db.get_client(guid=guid)
                if cl is not None:
                    cl_list = [cl]
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchActor.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchActor.interpret())

            if cl_list is not None:
                result.result = Converter.fill_clients(client_list=cl_list)
        except Exception as e:
            self.logger.error("get_clients: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

    def unregister_client(self, *, guid: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if guid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.unregister_client(guid=guid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

        except Exception as e:
            self.logger.error("unregister_client: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def advertise_resources(self, *, delegation: ABCPropertyGraph, delegation_name: str,
                            client: AuthToken, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if client is None or delegation is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    return self.actor.advertise(delegation=delegation, delegation_name=delegation_name, client=client)

            self.logger.debug("Executing advertise on actor {} {} ({})".format(self.actor.get_name(),
                                                                               self.actor.get_name(),
                                                                               self.actor.__class__.__name__))

            advertised = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
            result.set_result(str(advertised))
        except Exception as e:
            self.logger.error("advertise_resources: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result
