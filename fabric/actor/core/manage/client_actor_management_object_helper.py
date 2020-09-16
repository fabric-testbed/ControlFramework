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
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_actor_runnable import IActorRunnable
from fabric.actor.core.apis.i_controller_reservation import IControllerReservation
from fabric.actor.core.common.constants import Constants, ErrorCodes
from fabric.actor.core.kernel.controller_reservation_factory import ControllerReservationFactory
from fabric.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.manage.converter import Converter
from fabric.actor.core.manage.management_object import ManagementObject
from fabric.actor.core.manage.management_utils import ManagementUtils
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric.message_bus.messages.result_pool_info_avro import ResultPoolInfoAvro
from fabric.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric.actor.core.apis.i_client_actor_management_object import IClientActorManagementObject
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.actor.core.core.broker_policy import BrokerPolicy

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_client_actor import IClientActor
    from fabric.actor.security.auth_token import AuthToken
    from fabric.message_bus.messages.proxy_avro import ProxyMng
    from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric.actor.core.apis.i_actor import IActor
    from fabric.message_bus.messages.reservation_mng import ReservationMng


class ClientActorManagementObjectHelper(IClientActorManagementObject):
    def __init__(self, client: IClientActor):
        self.client = client
        from fabric.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def get_brokers(self, caller: AuthToken) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            brokers = self.client.get_brokers()
            result.result = Converter.fill_proxies(brokers)
        except Exception as e:
            self.logger.error("get_brokers {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_broker(self, broker_id: ID, caller: AuthToken) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()

        if broker_id is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            broker = self.client.get_broker(broker_id)
            if broker is not None:
                brokers = [broker]
                result.result = Converter.fill_proxies(brokers)
            else:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
        except Exception as e:
            traceback.print_exc()
            self.logger.error("get_broker {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_broker(self, broker_proxy: ProxyMng, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if broker_proxy is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            proxy = Converter.get_agent_proxy(broker_proxy)
            if proxy is None:
                result.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            else:
                self.client.add_broker(proxy)
        except Exception as e:
            self.logger.error("add_broker {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def get_pool_info(self, broker: ID, caller: AuthToken) -> ResultPoolInfoAvro:
        result = ResultPoolInfoAvro()
        result.status = ResultAvro()

        if broker is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            b = self.client.get_broker(broker)
            if b is not None:
                request = BrokerPolicy.get_resource_pools_query()
                response = ManagementUtils.query(self.client, b, request)
                pools = BrokerPolicy.get_resource_pools(response)

                for rd in pools:
                    temp = {}
                    temp = rd.save(temp, None)
                    pi = PoolInfoAvro()
                    pi.set_type(str(rd.get_resource_type()))
                    pi.set_name(rd.get_resource_type_label())
                    pi.set_properties(temp)

                    result.result.append(pi)
            else:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
        except Exception as e:
            self.logger.error("get_pool_info {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_reservation_private(self, reservation: TicketReservationAvro):
        result = ResultAvro()
        slice_id = ID(reservation.get_slice_id())
        rset = Converter.get_resource_set(reservation)
        term = Term(start=ActorClock.from_milliseconds(reservation.get_start()),
                    end=ActorClock.from_milliseconds(reservation.get_end()))

        broker = None

        if reservation.get_broker() is not None:
            broker = ID(reservation.get_broker())

        rc = ControllerReservationFactory.create(rid=ID(), resources=rset, term=term)
        rc.set_renewable(reservation.is_renewable())

        if rc.get_state() != ReservationStates.Nascent or rc.get_pending_state() != ReservationPendingStates.None_:
            result.set_code(ErrorCodes.ErrorInvalidReservation.value)
            result.set_message("Only reservations in Nascent.None can be added")
            return None, result

        slice_obj = self.client.get_slice(slice_id)

        if slice_obj is None:
            result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
            result.set_message(ErrorCodes.ErrorNoSuchSlice.name)
            return None, result

        rc.set_slice(slice_obj)

        proxy = None

        if broker is None:
            proxy = self.client.get_default_broker()
        else:
            proxy = self.client.get_broker(broker)

        if proxy is None:
            result.set_code(ErrorCodes.ErrorNoSuchBroker.value)
            result.set_message(ErrorCodes.ErrorNoSuchBroker.name)
            return None, result

        rc.set_broker(proxy)
        self.client.register(rc)
        return rc.get_reservation_id(), result

    def add_reservation(self, reservation: TicketReservationAvro, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if reservation is None or reservation.get_slice_id() is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, parent):
                    self.parent = parent

                def run(self):
                    return self.parent.add_reservation_private(reservation)

            rid, result.status = self.client.execute_on_actor_thread_and_wait(Runner(self))

            if rid is not None:
                result.result_str = str(rid)
        except Exception as e:
            self.logger.error("add_reservation {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def add_reservations(self, reservations: list, caller: AuthToken) -> ResultStringsAvro:
        result = ResultStringsAvro()
        result.status = ResultAvro()

        if reservations is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        for r in reservations:
            if r.get_slice_id() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
                return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, parent):
                    self.parent = parent

                def run(self):
                    result = []
                    try:
                        for r in reservations:
                            rr, status = self.parent.add_reservation_private(r)
                            if rr is not None:
                                result.append(str(rr))
                            else:
                                raise Exception("Could not add reservation")
                    except Exception as e:
                        for r in reservations:
                            self.parent.client.unregister(r)
                        result.clear()

                    return result

            rids, result.status = self.client.execute_on_actor_thread_and_wait(Runner(self))

            if result.status.get_code() == 0:
                for r in rids:
                    result.result.append(r)
        except Exception as e:
            self.logger.error("add_reservations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def demand_reservation_rid(self, rid: ID, caller: AuthToken) -> ResultAvro:
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
                    self.actor.demand(rid)
                    return None

            self.client.execute_on_actor_thread_and_wait(Runner(self.client))
        except Exception as e:
            self.logger.error("demand_reservation_rid {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def demand_reservation(self, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if reservation is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor, logger):
                    self.actor = actor
                    self.logger = logger

                def run(self):
                    result = ResultAvro()
                    rid = ID(reservation.get_reservation_id())
                    r = self.actor.get_reservation(rid)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.name)
                        return result

                    ManagementUtils.update_reservation(r, reservation)
                    if isinstance(reservation, LeaseReservationAvro):
                        predecessors = reservation.get_redeem_predecessors()
                        for pred in predecessors:
                            if pred.get_reservation_id() is None:
                                self.logger.warning("Redeem predecessor specified for rid={} but missing reservation id of predecessor".format(rid))
                                continue

                            predid = ID(pred.get_reservation_id())
                            pr = self.actor.get_reservation(predid)

                            if pr is None:
                                self.logger.warning("Redeem predecessor for rid={} with rid={} does not exist. Ignoring it!".format(rid, predid))
                                continue

                            if not isinstance(pr, IControllerReservation):
                                self.logger.warning("Redeem predecessor for rid={} is not an IControllerReservation: class={}".format(rid, type(pr)))
                                continue

                            ff = pred.get_filter()
                            if ff is not None:
                                self.logger.debug("Setting redeem predecessor on reservation # {} pred={} filter={}".format(r.get_reservation_id(), pr.get_reservation_id(), ff))
                                r.add_redeem_predecessor(pr, ff)
                            else:
                                self.logger.debug(
                                    "Setting redeem predecessor on reservation # {} pred={} filter=none".format(r.get_reservation_id(),
                                                                                                              pr.get_reservation_id()))
                                r.add_redeem_predecessor(pr)

                    try:
                        self.actor.get_plugin().get_database().update_reservation(r)
                    except Exception as e:
                        self.logger.error("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.name)

                    self.actor.demand(rid)

                    return result

            result = self.client.execute_on_actor_thread_and_wait(Runner(self.client, self.logger))
        except Exception as e:
            self.logger.error("demand_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def claim_resources_slice(self, broker: ID, slice_id: ID, rid: ID, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or broker is None or slice_id is None or rid is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            rtype = ResourceType(str(ID()))
            rdata = ResourceData()
            rset = ResourceSet(units=0, rtype=rtype, rdata=rdata)

            my_broker = self.client.get_broker(broker)

            if my_broker is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
                return result

            slice_obj = self.client.get_slice(slice_id)
            if slice_obj is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchSlice.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchSlice.name)
                return result

            if not slice_obj.is_inventory():
                result.status.set_code(ErrorCodes.ErrorInvalidSlice.value)
                result.status.set_message(ErrorCodes.ErrorInvalidSlice.name)
                return result

            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    return self.actor.claim_client(reservation_id=rid, resources=rset, slice_obj=slice_obj, broker=my_broker)

            rc = self.client.execute_on_actor_thread_and_wait(Runner(self.client))

            if rc is not None:
                result.reservations = []
                reservation = Converter.fill_reservation(rc, True)
                result.reservations.append(reservation)
            else:
                raise Exception("Internal Error")
        except Exception as e:
            self.logger.error("claim_resources_slice {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def claim_resources(self, broker: ID, rid: ID, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or rid is None or broker is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            rtype = ResourceType(str(ID()))
            rdata = ResourceData()
            rset = ResourceSet(units=0, rtype=rtype, rdata=rdata)

            my_broker = self.client.get_broker(broker)

            if my_broker is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
                return result

            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    return self.actor.claim_client(reservation_id=rid, resources=rset, broker=my_broker)

            rc = self.client.execute_on_actor_thread_and_wait(Runner(self.client))

            if rc is not None:
                result.reservations = []
                reservation = Converter.fill_reservation(rc, True)
                result.reservations.append(reservation)
            else:
                raise Exception("Internal Error")
        except Exception as e:
            traceback.print_exc()
            self.logger.error("claim_resources {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def reclaim_resources(self, broker: ID, rid: ID, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None or rid is None or broker is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            rtype = ResourceType(str(ID()))
            rdata = ResourceData()
            rset = ResourceSet(units=0, rtype=rtype, rdata=rdata)

            my_broker = self.client.get_broker(broker)

            if my_broker is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.name)
                return result

            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    return self.actor.reclaim_client(reservation_id=rid, resources=rset, broker=my_broker, caller=caller)

            rc = self.client.execute_on_actor_thread_and_wait(Runner(self.client))

            if rc is not None:
                result.reservations = []
                reservation = Converter.fill_reservation(rc, True)
                result.reservations.append(reservation)
            else:
                raise Exception("Internal Error")
        except Exception as e:
            traceback.print_exc()
            self.logger.error("claim_resources {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.name)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def extend_reservation(self, reservation: id, new_end_time: datetime, new_units: int,
                           new_resource_type: ResourceType, request_properties: dict,
                           config_properties: dict, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if reservation is None or caller is None or new_end_time is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        try:
            class Runner(IActorRunnable):
                def __init__(self, actor: IActor):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    r = self.actor.get_reservation(ID(reservation.get_reservation_id()))
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.name)
                        return result

                    temp = PropList.merge_properties(r.get_resources().get_config_properties(), config_properties)
                    r.get_resources().set_config_properties(temp)

                    temp = PropList.merge_properties(r.get_resources().get_request_properties(), request_properties)
                    r.get_resources().set_request_properties(temp)

                    rset = ResourceSet()
                    if new_units == Constants.ExtendSameUnits:
                        rset.set_units(r.get_resources().get_units())
                    else:
                        rset.set_units(new_units)

                    if new_resource_type is None:
                        rset.set_type(r.get_resources().get_type())

                    rset.set_config_properties(config_properties)
                    rset.set_request_properties(request_properties)

                    tmp_start_time = r.get_term().get_start_time()
                    new_term = r.get_term().extend()

                    new_term.set_end_time(new_end_time)
                    new_term.set_new_start_time(tmp_start_time)
                    new_term.set_start_time(tmp_start_time)

                    self.actor.extend(r.get_reservation_id(), rset, new_term)

                    return result

            result = self.client.execute_on_actor_thread_and_wait(Runner(self.client))

        except Exception as e:
            self.logger.error("extend_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result

    def modify_reservation(self, rid: ID, modify_properties: dict, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if rid is None or modify_properties is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.name)
            return result

        self.logger.debug("reservation: {} | modifyProperties= {}".format(rid, modify_properties))
        try:

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

                    self.actor.modify(rid, modify_properties)

                    return result
            result = self.client.execute_on_actor_thread_and_wait(Runner(self.client))
        except Exception as e:
            self.logger.error("modify_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.name)
            result = ManagementObject.set_exception_details(result, e)

        return result