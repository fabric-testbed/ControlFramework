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

from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Dict, Tuple

from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_sites_avro import ResultSitesAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro

from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ReservationNotFoundException, SliceNotFoundException, \
    DelegationNotFoundException, ManageException
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric_cf.actor.core.apis.abc_actor_management_object import ABCActorManagementObject

from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.container.maintenance import Site, Maintenance
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

    def get_slices(self, *, slice_id: ID, caller: AuthToken, slice_name: str = None, email: str = None,
                   states: List[int] = None, project: str = None, limit: int = None,
                   offset: int = None) -> ResultSliceAvro:
        result = ResultSliceAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
        else:
            slice_list = None

            try:
                try:
                    slice_list = self.db.get_slices(slice_id=slice_id, slice_name=slice_name, email=email,
                                                    states=states, project_id=project, limit=limit, offset=offset)
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

    def add_slice(self, *, slice_obj: SliceAvro, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if slice_obj is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())

        else:
            try:

                slice_obj_new = SliceFactory.create(slice_id=ID(), name=slice_obj.get_slice_name(),
                                                    project_id=slice_obj.get_project_id(),
                                                    project_name=slice_obj.get_project_name())
                slice_obj_new.set_description(description=slice_obj.get_description())
                auth_token = Translate.translate_auth_from_avro(auth_avro=slice_obj.get_owner())
                slice_obj_new.set_owner(owner=auth_token)
                slice_obj_new.set_graph_id(graph_id=slice_obj.graph_id)
                slice_obj_new.set_config_properties(value=slice_obj.get_config_properties())
                slice_obj_new.set_lease_end(lease_end=slice_obj.get_lease_end())
                slice_obj_new.set_lease_start(lease_start=datetime.now(timezone.utc))

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

    def accept_update_slice(self, *, slice_id: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.modify_accept(slice_id=slice_id)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except Exception as e:
            self.logger.error("accept_update_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)
        return result

    def remove_slice(self, *, slice_id: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()
        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

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

    def update_slice(self, *, slice_mng: SliceAvro, caller: AuthToken, modify_state: bool = False) -> ResultAvro:
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
                    runner_result = ResultAvro()
                    try:
                        if modify_state:
                            slice_object = Translate.translate_slice(slice_avro=slice_mng)
                            self.actor.modify_slice(slice_object=slice_object)
                        else:
                            slice_obj = self.actor.get_slice(slice_id=slice_id)
                            if slice_obj is None:
                                runner_result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                                runner_result.set_message(ErrorCodes.ErrorNoSuchSlice.interpret())
                                return runner_result

                            slice_obj = ManagementUtils.update_slice(slice_obj=slice_obj, slice_mng=slice_mng)
                            slice_obj.set_dirty()

                            self.actor.get_plugin().get_database().update_slice(slice_object=slice_obj)
                    except Exception as e:
                        self.actor.get_logger().error("Failed to modify/update slice {}".format(e))
                        runner_result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        runner_result.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                        runner_result = ManagementObject.set_exception_details(result=result, e=e)

                    return runner_result

            result = self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except Exception as e:
            self.logger.error("update_slice: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_slice_by_guid(self, *, guid: str) -> ABCSlice or None:
        slices = self.db.get_slices(slice_id=guid)
        if len(slices) == 0:
            return None
        return slices[0]

    def get_sites(self, *, caller: AuthToken, site: str) -> ResultSitesAvro:
        result = ResultSitesAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            sites = site.split(",")
            sites_list = None
            try:
                if len(sites) == 1:
                    site_info = self.db.get_site(site_name=sites[0])
                    if site_info is not None:
                        sites_list = [site_info]
                else:
                    sites_list = self.db.get_sites()

            except Exception as e:
                self.logger.error("get_sites:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)

            if sites_list is not None:
                result.sites = []
                for s in sites_list:
                    site_avro = Translate.translate_site_to_avro(site=s)
                    result.sites.append(site_avro)
        except ReservationNotFoundException as e:
            self.logger.error("get_sites: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_sites: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result


    def get_reservations(self, *, caller: AuthToken, states: List[int] = None,
                         slice_id: ID = None, rid: ID = None, oidc_claim_sub: str = None,
                         email: str = None, rid_list: List[str] = None, type: str = None,
                         site: str = None, node_id: str = None) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            res_list = None
            try:
                if rid_list is not None:
                    res_list = self.db.get_reservations_by_rids(rid=rid_list)
                else:
                    res_list = self.db.get_reservations(slice_id=slice_id, rid=rid, email=email,
                                                        states=states, rsv_type=type, site=site,
                                                        graph_node_id=node_id)
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

    def remove_reservation(self, *, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

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

    def close_reservation(self, *, caller: AuthToken, rid: ID) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
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

    def close_slice_reservations(self, *, caller: AuthToken, slice_id: ID) -> ResultAvro:
        result = ResultAvro()

        if slice_id is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.close_slice_reservations(slice_id=slice_id)
                    return None

            self.actor.execute_on_actor_thread(runnable=Runner(actor=self.actor))
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

    def get_actor_name(self) -> str or None:
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
                        self.actor.get_logger().error("Could not commit slice update {}".format(e))
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

    def get_reservation_state_for_reservations(self, *, caller: AuthToken,
                                               rids: List[str]) -> ResultReservationStateAvro:
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

    def get_delegations(self, *, caller: AuthToken, slice_id: ID = None, did: str = None,
                        states: List[int] = None) -> ResultDelegationAvro:
        result = ResultDelegationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            dlg_list = None
            try:
                if did is not None:
                    dlg = self.db.get_delegation(dlg_graph_id=did)
                    if dlg is not None:
                        dlg_list = [dlg]
                    else:
                        result.status.set_code(ErrorCodes.ErrorNoSuchDelegation.value)
                        result.status.set_message(ErrorCodes.ErrorNoSuchDelegation.interpret())
                else:
                    dlg_list = self.db.get_delegations(slice_id=slice_id, states=states)
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

    def update_maintenance_mode(self, *, properties: Dict[str, str], sites: List[Site] = None):
        result = ResultAvro()

        if properties is None and sites is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.update_maintenance_mode(properties=properties, sites=sites)
                    return True

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("update_maintenance_mode: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("update_maintenance_mode: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def is_testbed_in_maintenance(self) -> Tuple[bool, Dict[str, str] or None]:
        return Maintenance.is_testbed_in_maintenance(database=self.db)

    def is_site_in_maintenance(self, *, site_name: str) -> Tuple[bool, Site or None]:
        return Maintenance.is_site_in_maintenance(database=self.db, site_name=site_name)

    def is_sliver_provisioning_allowed(self, *, project: str, email: str, site: str,
                                       worker: str) -> Tuple[bool, str or None]:
        """
        Determine if sliver can be provisioned
        Sliver provisioning can be prohibited if Testbed or Site or Worker is in maintenance mode
        Sliver provisioning in maintenance mode may be allowed for specific projects/users
        @param project project
        @param email user's email
        @param site site name
        @param worker worker name
        @return True if allowed; False otherwise
        """
        return Maintenance.is_sliver_provisioning_allowed(database=self.db, project=project, email=email, site=site,
                                                          worker=worker)

    def is_slice_provisioning_allowed(self, *, project: str, email: str) -> bool:
        """
        Determine if slice can be provisioned
        Slice provisioning can be prohibited if Testbed is in maintenance mode
        Slice provisioning in maintenance mode may be allowed for specific projects/users
        @param project project
        @param email user's email
        @return True if allowed; False otherwise
        """
        return Maintenance.is_slice_provisioning_allowed(database=self.db, project=project, email=email)

    def close_delegation(self, *, caller: AuthToken, did: str) -> ResultAvro:
        result = ResultAvro()

        if did is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.close_delegation(did=did)
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

    def remove_delegation(self, *, caller: AuthToken, did: str) -> ResultAvro:
        result = ResultAvro()

        if did is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    self.actor.remove_delegation(did=did)
                    return None

            self.actor.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.actor))
        except ReservationNotFoundException as e:
            self.logger.error("remove_delegation: {}".format(e))
            result.set_code(ErrorCodes.ErrorNoSuchDelegation.value)
            result.set_message(e.text)
        except Exception as e:
            self.logger.error("remove_delegation: {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result
