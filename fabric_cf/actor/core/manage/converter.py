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
from typing import TYPE_CHECKING, List

from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.lease_reservation_state_avro import LeaseReservationStateAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.core.actor_identity import ActorIdentity
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.core.unit import Unit
from fabric_cf.actor.core.kernel.predecessor_state import PredecessorState
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.proxies.actor_location import ActorLocation
from fabric_cf.actor.core.proxies.kafka.kafka_proxy import KafkaProxy
from fabric_cf.actor.core.proxies.local.local_proxy import LocalProxy
from fabric_cf.actor.core.util.client import Client
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.manage.messages.client_mng import ClientMng

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.apis.abc_proxy import ABCProxy
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin


class Converter:
    @staticmethod
    def absorb_res_properties(*, rsv_mng: ReservationMng, res_obj: ABCReservationMixin):
        res_obj.get_resources().set_sliver(sliver=rsv_mng.get_sliver())
        return res_obj

    @staticmethod
    def fill_reservation_predecessor(*, pred: PredecessorState) -> ReservationPredecessorAvro:
        pred_avro = ReservationPredecessorAvro()
        pred_avro.set_reservation_id(value=str(pred.get_reservation().get_reservation_id()))
        return pred_avro

    @staticmethod
    def fill_reservation(*, reservation: ABCReservationMixin, full: bool) -> ReservationMng:
        rsv_mng = None
        if isinstance(reservation, ABCControllerReservation):
            rsv_mng = LeaseReservationAvro()
            if reservation.get_redeem_predecessors() is not None and len(reservation.get_redeem_predecessors()) > 0:
                rsv_mng.redeem_processors = []
                for p in reservation.get_redeem_predecessors():
                    pred = Converter.fill_reservation_predecessor(pred=p)
                    rsv_mng.redeem_processors.append(pred)

        elif isinstance(reservation, ABCClientReservation):
            rsv_mng = TicketReservationAvro()
        else:
            rsv_mng = ReservationMng()

        rsv_mng.set_reservation_id(str(reservation.get_reservation_id()))
        rsv_mng.set_slice_id(str(reservation.get_slice_id()))

        if reservation.get_type() is not None:
            rsv_mng.set_resource_type(str(reservation.get_type()))

        rsv_mng.set_units(reservation.get_units())
        rsv_mng.set_state(reservation.get_state().value)
        rsv_mng.set_pending_state(reservation.get_pending_state().value)

        if isinstance(reservation, ABCControllerReservation):
            rsv_mng.set_leased_units(reservation.get_leased_abstract_units())
            rsv_mng.set_join_state(reservation.get_join_state().value)
            authority = reservation.get_authority()

            if authority is not None:
                rsv_mng.set_authority(str(authority.get_guid()))

        if isinstance(reservation, ABCClientReservation):
            broker = reservation.get_broker()
            if broker is not None:
                rsv_mng.set_broker(str(broker.get_guid()))
            rsv_mng.set_renewable(reservation.is_renewable())
            rsv_mng.set_renew_time(reservation.get_renew_time())

        if reservation.get_term() is not None:
            rsv_mng.set_start(ActorClock.to_milliseconds(when=reservation.get_term().get_start_time()))
            rsv_mng.set_end(ActorClock.to_milliseconds(when=reservation.get_term().get_end_time()))
        else:
            if reservation.get_requested_term() is not None:
                rsv_mng.set_start(ActorClock.to_milliseconds(when=reservation.get_requested_term().get_start_time()))
                rsv_mng.set_end(ActorClock.to_milliseconds(when=reservation.get_requested_term().get_end_time()))

        if reservation.get_requested_term() is not None:
            rsv_mng.set_requested_end(ActorClock.to_milliseconds(when=reservation.get_requested_term().get_end_time()))

        rsv_mng.set_notices(reservation.get_notices())

        if full:
            rsv_mng = Converter.attach_res_properties(mng=rsv_mng, reservation=reservation)

        return rsv_mng

    @staticmethod
    def attach_res_properties(*, mng: ReservationMng, reservation: ABCReservationMixin):
        sliver = None
        if isinstance(reservation, ABCControllerReservation):
            if reservation.is_active():
                sliver = reservation.get_leased_resources().get_sliver()
            else:
                sliver = reservation.get_resources().get_sliver()
        else:
            rset = reservation.get_resources()
            if rset is not None:
                sliver = rset.get_sliver()

        ticket = None
        rset = reservation.get_resources()

        if rset is not None:
            cs = rset.get_resources()

            if cs is not None and isinstance(cs, Ticket):
                ticket = cs.get_properties()

        mng.set_sliver(sliver=sliver)

        if isinstance(mng, TicketReservationAvro):
            mng.set_ticket_properties(ticket)

        return mng

    @staticmethod
    def fill_reservation_state(*, res: ABCReservationMixin) -> ReservationStateAvro:
        result = None
        if isinstance(res, ABCControllerReservation):
            result = LeaseReservationStateAvro()
            result.set_reservation_id(rid=str(res.get_reservation_id()))
            result.set_state(res.get_state().value)
            result.set_pending_state(res.get_pending_state().value)
            result.set_joining(res.get_join_state().value)
        else:
            result = ReservationStateAvro()
            result.set_reservation_id(rid=str(res.get_reservation_id()))
            result.set_state(res.get_state().value)
            result.set_pending_state(res.get_pending_state().value)

        return result

    @staticmethod
    def fill_reservation_states(*, res_list: list) -> List[ReservationStateAvro]:
        result = []
        for r in res_list:
            rstate = Converter.fill_reservation_state(res=r)
            result.append(rstate)

        return result

    @staticmethod
    def fill_client(*, client_mng: ClientMng) -> Client:
        result = Client()
        result.set_name(name=client_mng.get_name())
        result.set_guid(guid=ID(uid=client_mng.get_guid()))
        return result

    @staticmethod
    def fill_client_mng(*, client: dict) -> ClientMng:
        result = ClientMng()
        result.set_name(name=client['clt_name'])
        result.set_guid(guid=client['clt_guid'])
        return result

    @staticmethod
    def fill_clients(*, client_list: list) -> List[ClientMng]:
        result = []
        for c in client_list:
            mng = Converter.fill_client_mng(client=c)
            result.append(mng)

        return result

    @staticmethod
    def fill_unit_mng(*, unit: Unit) -> UnitAvro:
        result = UnitAvro()
        result.properties = unit.properties
        return result

    @staticmethod
    def fill_units(*, unit_list: List[Unit]) -> List[UnitAvro]:
        result = []
        for u in unit_list:
            mng = Converter.fill_unit_mng(unit=u)
            result.append(mng)

        return result

    @staticmethod
    def fill_proxy(*, proxy: ABCProxy) -> ProxyAvro:
        result = ProxyAvro()
        result.set_name(proxy.get_name())
        result.set_guid(str(proxy.get_guid()))

        if isinstance(proxy, LocalProxy):
            result.set_protocol(Constants.PROTOCOL_LOCAL)
        elif isinstance(proxy, KafkaProxy):
            result.set_protocol(Constants.PROTOCOL_KAFKA)
            result.set_kafka_topic(proxy.get_kafka_topic())

        return result

    @staticmethod
    def fill_proxies(*, proxies: list) -> List[ProxyAvro]:
        result = []
        for p in proxies:
            proxy = Converter.fill_proxy(proxy=p)
            result.append(proxy)

        return result

    @staticmethod
    def get_agent_proxy(*, mng: ProxyAvro):
        try:
            location = ActorLocation(location=mng.get_kafka_topic())
            identity = ActorIdentity(name=mng.get_name(), guid=ID(uid=mng.get_guid()))
            from fabric_cf.actor.core.container.container import Container
            return Container.get_proxy(protocol=mng.get_protocol(), identity=identity, location=location,
                                       proxy_type=mng.get_type())
        except Exception:
            traceback.format_exc()
            return None

    @staticmethod
    def get_resource_set(*, res_mng: ReservationMng) -> ResourceSet:
        return ResourceSet(units=res_mng.get_units(), rtype=ResourceType(resource_type=res_mng.get_resource_type()),
                           sliver=res_mng.get_sliver())

    @staticmethod
    def fill_actor(*, actor: ABCActorMixin) -> ActorAvro:
        result = ActorAvro()
        result.set_name(actor.get_name())
        result.set_description(actor.get_description())
        result.set_type(actor.get_type().value)
        result.set_online(True)
        result.set_id(str(actor.get_guid()))
        result.set_policy_guid(str(actor.get_plugin().get_guid()))
        return result

    @staticmethod
    def fill_actors(*, act_list: list) -> List[ActorAvro]:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor(actor=a)
            result.append(act_mng)

        return result

    @staticmethod
    def fill_actor_from_db(*, actor: ABCActorMixin) -> ActorAvro:
        result = ActorAvro()
        result.set_description(actor.get_description())
        result.set_name(actor.get_name())
        result.set_type(actor.get_type().value)

        from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
        aa = ActorRegistrySingleton.get().get_actor(actor_name_or_guid=actor.get_name())
        result.set_online(aa is not None)

        return result

    @staticmethod
    def fill_actors_from_db(*, act_list: List[ABCActorMixin]) -> List[ActorAvro]:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor_from_db(actor=a)
            result.append(act_mng)

        return result

    @staticmethod
    def fill_actors_from_db_status(*, act_list: List[ABCActorMixin], status: int) -> List[ActorAvro]:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor_from_db(actor=a)

            if status == 0 or (status == 1 and act_mng.get_online()) or (status == 2 and not act_mng.get_online()):
                result.append(act_mng)

        return result
