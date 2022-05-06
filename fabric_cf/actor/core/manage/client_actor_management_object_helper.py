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
from typing import TYPE_CHECKING, List, Tuple

from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_broker_query_model_avro import ResultBrokerQueryModelAvro
from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fim.slivers.base_sliver import BaseSliver
from fim.user import GraphFormat

from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ManageException
from fabric_cf.actor.core.kernel.reservation_client import ClientReservationFactory
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates, ReservationPendingStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.management_utils import ManagementUtils
from fabric_cf.actor.core.proxies.kafka.translate import Translate
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.security.access_checker import AccessChecker
from fabric_cf.actor.security.pdp_auth import ActionId
from fabric_cf.actor.core.apis.abc_client_actor_management_object import ABCClientActorManagementObject
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.core.broker_policy import BrokerPolicy
from fabric_cf.actor.security.pdp_auth import ResourceType as AuthResourceType

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
    from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_cf.actor.core.apis.abc_client_actor import ABCClientActor
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin


class ClientActorManagementObjectHelper(ABCClientActorManagementObject):
    def __init__(self, *, client: ABCClientActor):
        self.client = client
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.logger = GlobalsSingleton.get().get_logger()

    def get_brokers(self, *, caller: AuthToken, broker_id: ID = None) -> ResultProxyAvro:
        result = ResultProxyAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            brokers = None
            if broker_id is None:
                brokers = self.client.get_brokers()
            else:
                broker = self.client.get_broker(guid=broker_id)
                if broker is not None:
                    brokers = [broker]
                    result.proxies = Converter.fill_proxies(proxies=brokers)
                else:
                    result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                    result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
            if brokers is not None:
                result.proxies = Converter.fill_proxies(proxies=brokers)
        except Exception as e:
            self.logger.error("get_brokers {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def add_broker(self, *, broker: ProxyAvro, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if broker is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            proxy = Converter.get_agent_proxy(mng=broker)
            if proxy is None:
                result.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            else:
                self.client.add_broker(broker=proxy)
        except Exception as e:
            self.logger.error("add_broker {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def get_broker_query_model(self, *, broker: ID, caller: AuthToken, id_token: str,
                               level: int, graph_format: GraphFormat) -> ResultBrokerQueryModelAvro:
        result = ResultBrokerQueryModelAvro()
        result.status = ResultAvro()

        if broker is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            b = self.client.get_broker(guid=broker)
            if b is not None:
                request = BrokerPolicy.get_broker_query_model_query(level=level, bqm_format=graph_format)
                response = ManagementUtils.query(actor=self.client, actor_proxy=b, query=request)
                result.model = Translate.translate_to_broker_query_model(query_response=response, level=level)
            else:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
        except Exception as e:
            self.logger.error("get_broker_query_model {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def add_reservation_private(self, *, reservation: TicketReservationAvro):
        result = ResultAvro()
        slice_id = ID(uid=reservation.get_slice_id())
        rset = Converter.get_resource_set(res_mng=reservation)
        term = Term(start=ActorClock.from_milliseconds(milli_seconds=reservation.get_start()),
                    end=ActorClock.from_milliseconds(milli_seconds=reservation.get_end()))

        broker = None

        if reservation.get_broker() is not None:
            broker = ID(uid=reservation.get_broker())

        rid = None
        if reservation.get_reservation_id() is not None:
            rid = ID(uid=reservation.get_reservation_id())
        else:
            rid = ID()
        rc = ClientReservationFactory.create(rid=rid, resources=rset, term=term)
        rc.set_renewable(renewable=reservation.is_renewable())

        if rc.get_state() != ReservationStates.Nascent or rc.get_pending_state() != ReservationPendingStates.None_:
            result.set_code(ErrorCodes.ErrorInvalidReservation.value)
            result.set_message("Only reservations in Nascent.None can be added")
            return None, result

        slice_obj = self.client.get_slice(slice_id=slice_id)

        if slice_obj is None:
            result.set_code(ErrorCodes.ErrorNoSuchSlice.value)
            result.set_message(ErrorCodes.ErrorNoSuchSlice.interpret())
            return None, result

        rc.set_slice(slice_object=slice_obj)

        proxy = None

        if broker is None:
            proxy = self.client.get_default_broker()
        else:
            proxy = self.client.get_broker(guid=broker)

        if proxy is None:
            result.set_code(ErrorCodes.ErrorNoSuchBroker.value)
            result.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
            return None, result

        rc.set_broker(broker=proxy)
        self.client.register(reservation=rc)
        return rc.get_reservation_id(), result

    def add_reservation(self, *, reservation: TicketReservationAvro, caller: AuthToken) -> ResultStringAvro:
        result = ResultStringAvro()
        result.status = ResultAvro()

        if reservation is None or reservation.get_slice_id() is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, parent):
                    self.parent = parent

                def run(self):
                    return self.parent.add_reservation_private(reservation=reservation)

            rid, result.status = self.client.execute_on_actor_thread_and_wait(runnable=Runner(parent=self))

            if rid is not None:
                result.set_result(str(rid))
        except Exception as e:
            self.logger.error("add_reservation {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def add_reservations(self, *, reservations: List[TicketReservationAvro], caller: AuthToken) -> ResultStringsAvro:
        result = ResultStringsAvro()
        result.status = ResultAvro()

        if reservations is None or caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        for r in reservations:
            if r.get_slice_id() is None:
                result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
                result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
                return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, parent):
                    self.parent = parent

                def run(self):
                    result = []
                    try:
                        for r in reservations:
                            rr, status = self.parent.add_reservation_private(reservation=r)
                            if rr is not None:
                                result.append(str(rr))
                            else:
                                raise ManageException("Could not add reservation")
                    except Exception:
                        for r in reservations:
                            self.parent.client.unregister(reservation=r)
                        result.clear()

                    return result

            rids, result.status = self.client.execute_on_actor_thread_and_wait(runnable=Runner(parent=self))

            if result.status.get_code() == 0:
                for r in rids:
                    result.result.append(r)
        except Exception as e:
            self.logger.error("add_reservations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def demand_reservation_rid(self, *, rid: ID, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if rid is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCClientActor):
                    self.actor = actor

                def run(self):
                    self.actor.demand(rid=rid)
                    return None

            self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client))
        except Exception as e:
            self.logger.error("demand_reservation_rid {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def demand_reservation(self, *, reservation: ReservationMng, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if reservation is None or caller is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCClientActor, logger):
                    self.actor = actor
                    self.logger = logger

                def run(self):
                    result = ResultAvro()
                    rid = ID(uid=reservation.get_reservation_id())
                    r = self.actor.get_reservation(rid=rid)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.interpret())
                        return result

                    ManagementUtils.update_reservation(res_obj=r, rsv_mng=reservation)
                    if isinstance(reservation, LeaseReservationAvro):
                        predecessors = reservation.get_redeem_predecessors()
                        for pred in predecessors:
                            if pred.get_reservation_id() is None:
                                self.logger.warning("Redeem predecessor specified for rid={} "
                                                    "but missing reservation id of predecessor".format(rid))
                                continue

                            predid = ID(uid=pred.get_reservation_id())
                            pr = self.actor.get_reservation(rid=predid)

                            if pr is None:
                                self.logger.warning("Redeem predecessor for rid={} with rid={} does not exist. "
                                                    "Ignoring it!".format(rid, predid))
                                continue

                            if not isinstance(pr, ABCControllerReservation):
                                self.logger.warning("Redeem predecessor for rid={} is not an IControllerReservation: "
                                                    "class={}".format(rid, type(pr)))
                                continue

                            self.logger.debug("Setting redeem predecessor on reservation # {} pred={}".
                                              format(r.get_reservation_id(), pr.get_reservation_id()))
                            r.add_redeem_predecessor(reservation=pr)

                    try:
                        self.actor.get_plugin().get_database().update_reservation(reservation=r)
                    except Exception as e:
                        self.logger.error("Could not commit slice update {}".format(e))
                        result.set_code(ErrorCodes.ErrorDatabaseError.value)
                        result.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))

                    self.actor.demand(rid=rid)

                    return result

            result = self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client,
                                                                                  logger=self.logger))
        except Exception as e:
            self.logger.error("demand_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def extend_reservation(self, *, reservation: id, new_end_time: datetime, new_units: int,
                           caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if reservation is None or caller is None or new_end_time is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCActorMixin):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    r = self.actor.get_reservation(rid=reservation)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.interpret())
                        return result

                    rset = ResourceSet()
                    if new_units == Constants.EXTEND_SAME_UNITS:
                        rset.set_units(units=r.get_resources().get_units())
                    else:
                        rset.set_units(units=new_units)

                    rset.set_type(rtype=r.get_resources().get_type())

                    tmp_start_time = r.get_term().get_start_time()
                    new_term = r.get_term().extend()

                    new_term.set_end_time(date=new_end_time)
                    new_term.set_new_start_time(date=tmp_start_time)
                    new_term.set_start_time(date=tmp_start_time)

                    self.actor.extend(rid=r.get_reservation_id(), resources=rset, term=new_term)

                    return result

            result = self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client))

        except Exception as e:
            self.logger.error("extend_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def modify_reservation(self, *, rid: ID, modified_sliver: BaseSliver, caller: AuthToken) -> ResultAvro:
        result = ResultAvro()

        if rid is None or modified_sliver is None:
            result.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        self.logger.debug("reservation: {} | modified_sliver= {}".format(rid, modified_sliver))
        try:

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCClientActor):
                    self.actor = actor

                def run(self):
                    result = ResultAvro()
                    r = self.actor.get_reservation(rid=rid)
                    if r is None:
                        result.set_code(ErrorCodes.ErrorNoSuchReservation.value)
                        result.set_message(ErrorCodes.ErrorNoSuchReservation.interpret())
                        return result

                    self.actor.modify(reservation_id=rid, modified_sliver=modified_sliver)

                    return result
            result = self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client))
        except Exception as e:
            self.logger.error("modify_reservation {}".format(e))
            result.set_code(ErrorCodes.ErrorInternalError.value)
            result.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result = ManagementObject.set_exception_details(result=result, e=e)

        return result

    def claim_delegations(self, *, broker: ID, did: str, caller: AuthToken) -> ResultDelegationAvro:
        result = ResultDelegationAvro()
        result.status = ResultAvro()

        if caller is None or did is None or broker is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:
            my_broker = self.client.get_broker(guid=broker)

            if my_broker is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
                return result

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCClientActor):
                    self.actor = actor

                def run(self):
                    return self.actor.claim_delegation_client(delegation_id=did, broker=my_broker)

            rc = self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client))

            if rc is not None:
                result.delegations = []
                delegation = Translate.translate_delegation_to_avro(delegation=rc)
                result.delegations.append(delegation)
            else:
                raise ManageException("Internal Error")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("claim_delegations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def reclaim_delegations(self, *, broker: ID, did: str, caller: AuthToken) -> ResultDelegationAvro:
        result = ResultDelegationAvro()
        result.status = ResultAvro()

        if caller is None or did is None or broker is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result

        try:

            my_broker = self.client.get_broker(guid=broker)

            if my_broker is None:
                result.status.set_code(ErrorCodes.ErrorNoSuchBroker.value)
                result.status.set_message(ErrorCodes.ErrorNoSuchBroker.interpret())
                return result

            class Runner(ABCActorRunnable):
                def __init__(self, *, actor: ABCClientActor):
                    self.actor = actor

                def run(self):
                    return self.actor.reclaim_delegation_client(delegation_id=did, broker=my_broker)

            rc = self.client.execute_on_actor_thread_and_wait(runnable=Runner(actor=self.client))

            if rc is not None:
                result.delegations = []
                delegation = Translate.translate_delegation_to_avro(delegation=rc)
                result.delegations.append(delegation)
            else:
                raise ManageException("Internal Error")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("reclaim_delegations {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInternalError.value)
            result.status.set_message(ErrorCodes.ErrorInternalError.interpret(exception=e))
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result
