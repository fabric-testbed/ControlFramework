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

from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_actor_runnable import IActorRunnable
from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.core.authority_policy import AuthorityPolicy
from fabric.actor.core.kernel.broker_reservation_factory import BrokerReservationFactory
from fabric.actor.core.kernel.reservation_factory import ReservationFactory
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.manage.actor_management_object import ActorManagementObject
from fabric.actor.core.manage.converter import Converter
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.manage.messages.result_client_mng import ResultClientMng
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.actor.core.proxies.kafka.translate import Translate
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.resource_data import ResourceData
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.result_slice_avro import ResultSliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.actor.core.apis.i_server_actor import IServerActor
    from fabric.actor.security.auth_token import AuthToken
    from fabric.actor.core.util.id import ID
    from fabric.actor.core.manage.messages.client_mng import ClientMng
    from fabric.message_bus.messages.slice_avro import SliceAvro
    from fabric.actor.core.util.resource_type import ResourceType


class ServerActorManagementObject(ActorManagementObject):
    def __init__(self, *, sa: IServerActor = None):
        super().__init__(actor=sa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PropertyClassName] = ServerActorManagementObject.__name__
        properties[Constants.PropertyModuleName] = ServerActorManagementObject.__module__
        return properties

    def get_broker_reservations(self, *, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_broker_reservations()
            except Exception as e:
                self.logger.error("get_broker_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                for r in res_list:
                    slice_obj = self.get_slice_by_id(id=r['slc_id'])
                    rsv_obj = ReservationFactory.create_instance(properties=r, actor=self.actor,
                                                                 slice_obj=slice_obj,
                                                                 logger=self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(reservation=rsv_obj, full=False)
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("get_broker_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_inventory_reservations(self, *, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_holdings()
            except Exception as e:
                self.logger.error("get_inventory_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                for r in res_list:
                    slice_obj = self.get_slice_by_id(id=r['slc_id'])
                    rsv_obj = ReservationFactory.create_instance(properties=r, actor=self.actor,
                                                                 slice_obj=slice_obj,
                                                                 logger=self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(reservation=rsv_obj, full=False)
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("get_inventory_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_inventory_reservations_by_slice_id(self, *, caller: AuthToken, slice_id: ID) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or slice_id is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_holdings_by_slice_id(slice_id)
            except Exception as e:
                self.logger.error("get_holdings:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                slice_obj = self.get_slice_by_guid(guid=str(slice_id))
                for r in res_list:
                    rsv_obj = ReservationFactory.create_instance(properties=r, actor=self.actor, slice_obj=slice_obj,
                                                                 logger=self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(reservation=rsv_obj, full=False)
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("get_holdings: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_inventory_slices(self, *, caller: AuthToken) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            slc_list = None

            try:
                slc_list = self.db.get_inventory_slices()
            except Exception as e:
                self.logger.error("get_inventory_slices:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if slc_list is not None:
                result.result = Translate.fill_slices(slice_list=slc_list, full=True)

        except Exception as e:
            self.logger.error("get_inventory_slices: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_client_slices(self, *, caller: AuthToken) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            slc_list = None

            try:
                slc_list = self.db.get_client_slices()
            except Exception as e:
                self.logger.error("get_inventory_slices:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if slc_list is not None:
                result.result = Translate.fill_slices(slice_list=slc_list, full=True)

        except Exception as e:
            self.logger.error("get_inventory_slices: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def add_client_slice(self, *, caller: AuthToken, slice_mng: SliceAvro) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None or slice_mng is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            owner_mng = slice_mng.get_owner()
            owner = None
            owner_is_ok = False

            if owner_mng is not None:
                owner = Translate.translate_auth_from_avro(auth_avro=owner_mng)
                if owner is not None:
                    if owner.get_name() is not None and owner.get_guid() is not None:
                        owner_is_ok = True

            if not owner_is_ok:
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                return result

            slice_obj = SliceFactory.create(slice_id=ID(), name=slice_mng.get_slice_name(),
                                            data=Converter.get_resource_data(slice_mng=slice_mng))
            slice_obj.set_description(description=slice_mng.get_description())
            slice_obj.set_inventory(value=False)

            assert owner is not None
            slice_obj.set_owner(owner=owner)

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
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
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def register_client(self, *, client: ClientMng, kafka_topic: str, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if client is None or kafka_topic is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            client_obj = Converter.fill_client(client_mng=client)
            client_obj.set_kafka_topic(kafka_topic=kafka_topic)

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.register_client(client=client_obj)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

        except Exception as e:
            self.logger.error("register_client: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_clients(self, *, caller: AuthToken) -> ResultClientMng:
        result = ResultClientMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            cl_list = self.db.get_clients()
            result.result = Converter.fill_clients(client_list=cl_list)
        except Exception as e:
            self.logger.error("get_clients: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

    def get_client(self, *, caller: AuthToken, guid: ID) -> ResultClientMng:
        result = ResultClientMng()
        result.status = ResultAvro()

        if caller is None or guid is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            cl = self.db.get_client(guid=guid)
            if cl is not None:
                cl_list = [cl]
                result.result = Converter.fill_clients(client_list=cl_list)
            else:
                result.status.set_code(ErrorCodes.ErrorNoSuchActor.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchActor.name)
        except Exception as e:
            self.logger.error("get_clients: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)
        return result

    def unregister_client(self, *, guid: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if guid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.unregister_client(guid=guid)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

        except Exception as e:
            self.logger.error("unregister_client: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_client_reservations(self, *, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_client_reservations()
            except Exception as e:
                self.logger.error("get_client_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                for r in res_list:
                    slice_obj = self.get_slice_by_id(id=r['slc_id'])
                    rsv_obj = ReservationFactory.create_instance(properties=r, actor=self.actor,
                                                                 slice_obj=slice_obj,
                                                                 logger=self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(reservation=rsv_obj, full=False)
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("get_client_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_client_reservations_by_slice_id(self, *, caller: AuthToken, slice_id: ID):
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or slice_id is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_client_reservations_by_slice_id(slice_id=slice_id)
            except Exception as e:
                self.logger.error("get_client_reservations_by_slice_id:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.name)
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.result = []
                slice_obj = self.get_slice_by_guid(guid=str(slice_id))
                for r in res_list:
                    rsv_obj = ReservationFactory.create_instance(properties=r, actor=self.actor, slice_obj=slice_obj,
                                                                 logger=self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(reservation=rsv_obj, full=False)
                        result.result.append(rr)
        except Exception as e:
            self.logger.error("get_client_reservations_by_slice_id: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def export_resources_pool_client_slice(self, *, client_slice_id: ID, pool_id: ID, start: datetime, end: datetime,
                                           units: int, ticket_properties: dict, resource_properties: dict,
                                           source_ticket_id: ID, caller: AuthToken) -> ResultStringAvro:

        result = ResultStringAvro()
        result.status = ResultAvro()

        if client_slice_id is None or pool_id is None or start is None or end is None or units < 1 or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            client_slice = self.actor.get_slice(slice_id=client_slice_id)
            if client_slice is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchSlice.name)
                return result

            pool = self.actor.get_slice(slice_id=pool_id)
            if pool is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchResourcePool.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchResourcePool.name)
                return result

            term = Term(start=start, end=end)
            rdata = ResourceData()
            if source_ticket_id is not None:
                rdata.request_properties[AuthorityPolicy.PropertySourceTicket] = str(source_ticket_id)

            rset = ResourceSet(units=units, rtype=pool.get_resource_type(), rdata=rdata)
            broker_reservation = BrokerReservationFactory.create(rid=ID(), resources=rset, term=term,
                                                                 slice_obj=client_slice)
            broker_reservation.set_owner(owner=client_slice.get_owner())

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.export(reservation=broker_reservation, client=client_slice.get_owner())
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

            result.result = str(broker_reservation.get_reservation_id())
        except Exception as e:
            self.logger.error("export_resources: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def export_resources_pool(self, *, pool_id: ID, start: datetime, end: datetime, units: int,
                              ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                              client: AuthToken, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if client is None or pool_id is None or start is None or end is None or units < 1 or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            pool = self.actor.get_slice(slice_id=pool_id)
            if pool is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchResourcePool.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchResourcePool.name)
                return result

            term = Term(start=start, end=end)
            rdata = ResourceData()
            if source_ticket_id is not None:
                rdata.request_properties[AuthorityPolicy.PropertySourceTicket] = str(source_ticket_id)

            rset = ResourceSet(units=units, rtype=pool.get_resource_type(), rdata=rdata)

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    return self.actor.export(resources=rset, term=term, client=client)

            exported = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

            result.result = str(exported)
        except Exception as e:
            self.logger.error("export_resources: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def export_resources_client_slice(self, *, client_slice_id: ID, rtype: ResourceType, start: datetime, end: datetime,
                                      units: int, ticket_properties: dict, resource_properties: dict,
                                      source_ticket_id: ID, caller: AuthToken) -> ResultStringAvro:

        result = ResultStringAvro()
        result.status = ResultAvro()

        if client_slice_id is None or rtype is None or start is None or end is None or units < 1 or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            client_slice = self.actor.get_slice(slice_id=client_slice_id)
            if client_slice is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchSlice.name)
                return result

            term = Term(start=start, end=end)
            rdata = ResourceData()
            if source_ticket_id is not None:
                rdata.request_properties[AuthorityPolicy.PropertySourceTicket] = str(source_ticket_id)

            rset = ResourceSet(units=units, rtype=rtype, rdata=rdata)
            broker_reservation = BrokerReservationFactory.create(rid=ID(), resources=rset, term=term,
                                                                 slice_obj=client_slice)
            broker_reservation.set_owner(owner=client_slice.get_owner())

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    self.actor.export(reservation=broker_reservation, client=client_slice.get_owner())
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))

            result.result = str(broker_reservation.get_reservation_id())
        except Exception as e:
            self.logger.error("export_resources: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def export_resources(self, *, rtype: ResourceType, start: datetime, end: datetime, units: int,
                         ticket_properties: dict, resource_properties: dict, source_ticket_id: ID,
                         client: AuthToken, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if client is None or rtype is None or start is None or end is None or units < 1 or client is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            term = Term(start=start, end=end)
            rdata = ResourceData()
            if source_ticket_id is not None:
                rdata.request_properties[AuthorityPolicy.PropertySourceTicket] = str(source_ticket_id)

            rset = ResourceSet(units=units, rtype=rtype, rdata=rdata)

            class Runner(IActorRunnable):
                def __init__(self, *, actor: IActor):
                    self.actor = actor

                def run(self):
                    return self.actor.export(resources=rset, term=term, client=client)

            self.logger.debug("Executing export on actor {} {} ({}) {}".format(self.actor.get_name(),
                                                                               self.actor.get_name(),
                                                                               self.actor.__class__.__name__,
                                                                               rset.get_reservation_id()))

            exported = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
            result.result = str(exported)
        except Exception as e:
            self.logger.error("export_resources: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result
