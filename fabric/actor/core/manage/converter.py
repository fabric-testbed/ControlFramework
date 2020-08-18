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
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_controller_reservation import IControllerReservation

from fabric.actor.core.common.constants import Constants
from fabric.actor.core.core.actor_identity import ActorIdentity
from fabric.actor.core.core.ticket import Ticket
from fabric.actor.core.core.unit import Unit
from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.message_bus.messages.actor_avro import ActorAvro
from fabric.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric.message_bus.messages.proxy_avro import ProxyAvro
from fabric.message_bus.messages.lease_reservation_state_avro import LeaseReservationStateAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric.message_bus.messages.unit_avro import UnitAvro
from fabric.actor.core.proxies.actor_location import ActorLocation
from fabric.actor.core.proxies.kafka.kafka_proxy import KafkaProxy
from fabric.actor.core.proxies.local.local_proxy import LocalProxy
from fabric.actor.core.util.client import Client
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.manage.messages.client_mng import ClientMng
from fabric.message_bus.messages.slice_avro import SliceAvro

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_reservation import IReservation
    from fabric.actor.core.apis.i_proxy import IProxy
    from fabric.actor.core.apis.i_actor import IActor


class Converter:
    @staticmethod
    def get_resource_data(slice_mng: SliceAvro) -> ResourceData:
        rd = ResourceData()

        rd.request_properties = slice_mng.get_request_properties()
        rd.resource_properties = slice_mng.get_resource_properties()
        rd.config_properties = slice_mng.get_config_properties()
        rd.local_properties = slice_mng.get_local_properties()
        return rd

    @staticmethod
    def absorb_res_properties(rsv_mng: ReservationMng, res_obj: IReservation):
        res_obj.get_resources().set_local_properties(
            PropList.merge_properties(rsv_mng.get_local_properties(), res_obj.get_resources().get_local_properties()))

        res_obj.get_resources().set_config_properties(
            PropList.merge_properties(rsv_mng.get_config_properties(), res_obj.get_resources().get_config_properties()))

        res_obj.get_resources().set_request_properties(
            PropList.merge_properties(rsv_mng.get_request_properties(), res_obj.get_resources().get_request_properties()))

        res_obj.get_resources().set_resource_properties(
            PropList.merge_properties(rsv_mng.get_resource_properties(), res_obj.get_resources().get_resource_properties()))

        return res_obj

    @staticmethod
    def fill_reservation(reservation: IReservation, full: bool) -> ReservationMng:
        rsv_mng = None

        if isinstance(reservation, IControllerReservation):
            rsv_mng = LeaseReservationAvro()
        elif isinstance(reservation, IClientReservation):
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

        if isinstance(reservation, IControllerReservation):
            rsv_mng.set_leased_units(reservation.get_leased_abstract_units())
            rsv_mng.set_join_state(reservation.get_join_state().value)
            authority = reservation.get_authority()

            if authority is not None:
                rsv_mng.set_authority(str(authority.get_guid()))

        if isinstance(reservation, IClientReservation):
            broker = reservation.get_broker()
            if broker is not None:
                rsv_mng.set_broker(str(broker.get_guid()))
            rsv_mng.set_renewable(reservation.is_renewable())
            rsv_mng.set_renew_time(reservation.get_renew_time())

        if reservation.get_term() is not None:
            rsv_mng.set_start(int(reservation.get_term().get_start_time().timestamp() * 1000))
            rsv_mng.set_end(int(reservation.get_term().get_end_time().timestamp() * 1000))
        else:
            if reservation.get_requested_term() is not None:
                rsv_mng.set_start(int(reservation.get_requested_term().get_start_time().timestamp() * 1000))
                rsv_mng.set_end(int(reservation.get_requested_term().get_end_time().timestamp() * 1000))

        if reservation.get_requested_term() is not None:
            rsv_mng.set_requested_end(int(reservation.get_requested_term().get_end_time().timestamp() * 1000))

        rsv_mng.set_notices(reservation.get_notices())

        if full:
            rsv_mng = Converter.attach_res_properties(rsv_mng, reservation)

        return rsv_mng

    @staticmethod
    def attach_res_properties(mng: ReservationMng, reservation: IReservation):
        resource = None
        config = None
        local = None
        request = None

        if isinstance(reservation, IControllerReservation):
            if reservation.is_active():
                resource = reservation.get_leased_resources().get_resource_properties()
                config = reservation.get_leased_resources().get_config_properties()
                request = reservation.get_leased_resources().get_request_properties()
            else:
                resource = reservation.get_resources().get_resource_properties()
                config = reservation.get_resources().get_config_properties()
                request = reservation.get_resources().get_request_properties()
            local = reservation.get_resources().get_local_properties()
        else:
            rset = reservation.get_resources()
            if rset is not None:
                resource = rset.get_resource_properties()
                config = rset.get_config_properties()
                local = rset.get_local_properties()
                request = rset.get_request_properties()

        ticket = None
        rset = reservation.get_resources()

        if rset is not None:
            cs = rset.get_resources()

            if cs is not None and isinstance(cs, Ticket):
                ticket = cs.get_properties()

        mng.set_config_properties(config)
        mng.set_request_properties(request)
        mng.set_local_properties(local)
        mng.set_resource_properties(resource)

        if isinstance(mng, TicketReservationAvro):
            mng.set_ticket_properties(ticket)

        return mng

    @staticmethod
    def fill_reservation_state(res: dict) -> ReservationStateAvro:
        result = None
        if 'rsv_joining' in res:
            result = LeaseReservationStateAvro()
            result.set_joining(res['rsv_joining'])
            result.set_state(res['rsv_state'])
            result.set_pending_state(res['rsv_pending'])
        else:
            result = ReservationStateAvro()
            result.set_state(res['rsv_state'])
            result.set_pending_state(res['rsv_pending'])

        return result

    @staticmethod
    def fill_reservation_states(res_list: list) -> list:
        result = []
        for r in res_list:
            rstate = Converter.fill_reservation_state(r)
            result.append(rstate)

        return result

    @staticmethod
    def fill_client(client_mng: ClientMng) -> Client:
        result = Client()
        result.set_name(client_mng.get_name())
        result.set_guid(ID(client_mng.get_guid()))
        return result

    @staticmethod
    def fill_client_mng(client: dict) -> ClientMng:
        result = ClientMng()
        result.set_name(client['clt_name'])
        result.set_guid(client['clt_guid'])
        return result

    @staticmethod
    def fill_clients(client_list: list) -> list:
        result = []
        for c in client_list:
            mng = Converter.fill_client_mng(c)
            result.append(mng)

        return result

    @staticmethod
    def fill_unit_mng(properties: dict) -> UnitAvro:
        result = UnitAvro()
        unit = Unit.create_instance(properties)
        result.properties = unit.properties
        return result

    @staticmethod
    def fill_units(unit_list: list) -> list:
        result = []
        for u in unit_list:
            mng = Converter.fill_unit_mng(u)
            result.append(mng)

        return result

    @staticmethod
    def fill_proxy(proxy: IProxy) -> ProxyAvro:
        result = ProxyAvro()
        result.set_name(proxy.get_name())
        result.set_guid(str(proxy.get_guid()))

        if isinstance(proxy, LocalProxy):
            result.set_protocol(Constants.ProtocolLocal)
        elif isinstance(proxy, KafkaProxy):
            result.set_protocol(Constants.ProtocolKafka)
            result.set_kafka_topic(proxy.get_kafka_topic())

        return result

    @staticmethod
    def fill_proxies(proxies: list) -> list:
        result = []
        for p in proxies:
            proxy = Converter.fill_proxy(p)
            result.append(proxy)

        return result

    @staticmethod
    def get_agent_proxy(mng: ProxyAvro):
        try:
            location = ActorLocation(mng.get_kafka_topic())
            identity = ActorIdentity(mng.get_name(), ID(mng.get_guid()))
            from fabric.actor.core.container.container import Container
            return Container.get_proxy(mng.get_protocol(), identity, location, mng.get_type())
        except Exception as e:
            traceback.print_exc()
            return None

    @staticmethod
    def get_resource_data_from_res(res_mng: ReservationMng) -> ResourceData:
        rd = ResourceData()
        rd.request_properties = res_mng.get_request_properties()
        rd.resource_properties = res_mng.get_resource_properties()
        rd.local_properties = res_mng.get_local_properties()
        rd.configuration_properties = res_mng.get_config_properties()

        return rd

    @staticmethod
    def get_resource_set(res_mng: ReservationMng) -> ResourceSet:
        rd = Converter.get_resource_data_from_res(res_mng)

        return ResourceSet(units=res_mng.get_units(), rtype=ResourceType(res_mng.get_resource_type()), rdata=rd)

    @staticmethod
    def fill_actor(actor: IActor) -> ActorAvro:
        result = ActorAvro()
        result.set_name(actor.get_name())
        result.set_description(actor.get_description())
        result.set_type(actor.get_type().value)
        result.set_online(True)
        result.set_id(str(actor.get_guid()))
        result.set_policy_guid(str(actor.get_plugin().get_guid()))
        return result

    @staticmethod
    def fill_actors(act_list: list) -> list:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor(a)
            result.append(act_mng)

        return result

    @staticmethod
    def fill_actor_from_db(properties: dict) -> ActorAvro:
        from fabric.actor.core.core.actor import Actor
        actor = Actor.create_instance(properties)
        result = ActorAvro()
        result.set_description(actor.get_description())
        result.set_name(actor.get_name())
        result.set_type(actor.get_type().value)

        from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
        aa = ActorRegistrySingleton.get().get_actor(actor.get_name())
        result.set_online(aa is not None)

        return result

    @staticmethod
    def fill_actors_from_db(act_list: list) -> list:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor_from_db(a)
            result.append(act_mng)

        return result

    @staticmethod
    def fill_actors_from_db_status(act_list: list, status: int) -> list:
        result = []
        for a in act_list:
            act_mng = Converter.fill_actor_from_db(a)

            if status == 0:
                result.append(act_mng)

            elif status == 1 and act_mng.get_online():
                result.append(act_mng)

            elif status == 2 and not act_mng.get_online():
                result.append(act_mng)

        return result
