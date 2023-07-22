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

from typing import TYPE_CHECKING, List, Union

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.poa_avro import PoaAvro
from fabric_mb.message_bus.messages.poa_info_avro import PoaInfoAvro
from fabric_mb.message_bus.messages.resource_ticket_avro import ResourceTicketAvro
from fabric_mb.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_poa_avro import ResultPoaAvro
from fabric_mb.message_bus.messages.site_avro import SiteAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fabric_mb.message_bus.messages.term_avro import TermAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro
from fabric_mb.message_bus.messages.update_data_avro import UpdateDataAvro
from fabric_mb.message_bus.messages.ticket import Ticket as AvroTicket

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ProxyException
from fabric_cf.actor.core.core.ticket import Ticket
from fabric_cf.actor.core.core.unit import Unit, UnitState
from fabric_cf.actor.core.core.unit_set import UnitSet
from fabric_cf.actor.core.delegation.resource_ticket import ResourceTicket
from fabric_cf.actor.core.kernel.poa import Poa, PoaFactory
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceFactory
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.container.maintenance import Site
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.db import Units
from fabric_cf.actor.security.auth_token import AuthToken


if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice


class Translate:
    @staticmethod
    def translate_udd(*, udd: UpdateData) -> UpdateDataAvro:
        result = UpdateDataAvro()
        result.message = udd.get_message()
        result.failed = udd.is_failed()
        return result

    @staticmethod
    def translate_udd_from_avro(*, udd: UpdateDataAvro) -> UpdateData:
        result = UpdateData()
        result.message = udd.message
        result.failed = udd.failed
        return result

    @staticmethod
    def translate_slice(*, slice_avro: SliceAvro) -> ABCSlice:
        if slice_avro is None:
            return None
        if slice_avro.guid is None and slice_avro.slice_name is None:
            return None

        if slice_avro.guid is None:
            raise ProxyException(Constants.NOT_SPECIFIED_PREFIX.format("Slice id"))

        slice_obj = SliceFactory.create(slice_id=ID(uid=slice_avro.guid),
                                        name=slice_avro.slice_name,
                                        project_id=slice_avro.project_id,
                                        project_name=slice_avro.project_name)
        slice_obj.set_description(description=slice_avro.description)
        slice_obj.set_config_properties(value=slice_avro.config_properties)
        slice_obj.set_lease_start(lease_start=slice_avro.get_lease_start())
        slice_obj.set_lease_end(lease_end=slice_avro.get_lease_end())
        slice_obj.set_graph_id(graph_id=slice_avro.graph_id)
        return slice_obj

    @staticmethod
    def translate_slice_to_avro(*, slice_obj: ABCSlice) -> SliceAvro:
        avro_slice = SliceAvro()
        avro_slice.slice_name = slice_obj.get_name()
        avro_slice.guid = str(slice_obj.get_slice_id())
        avro_slice.description = slice_obj.get_description()
        avro_slice.owner = Translate.translate_auth_to_avro(auth=slice_obj.get_owner())
        avro_slice.state = slice_obj.get_state().value
        avro_slice.config_properties = slice_obj.get_config_properties()
        avro_slice.set_lease_end(lease_end=slice_obj.get_lease_end())
        avro_slice.set_lease_start(lease_start=slice_obj.get_lease_start())
        if hasattr(slice_obj, 'project_id'):
            avro_slice.set_project_id(project_id=slice_obj.get_project_id())

        if hasattr(slice_obj, 'project_name'):
            avro_slice.set_project_name(project_name=slice_obj.get_project_name())

        if slice_obj.get_resource_type() is not None:
            avro_slice.set_resource_type(str(slice_obj.get_resource_type()))

        avro_slice.set_client_slice(slice_obj.is_client())

        if slice_obj.get_graph_id() is not None:
            avro_slice.graph_id = slice_obj.get_graph_id()

        return avro_slice

    @staticmethod
    def translate_site_to_avro(*, site: Site) -> SiteAvro or None:
        if site is None:
            return None
        result = SiteAvro()
        result.name = site.get_name()
        result.maint_info = site.get_maintenance_info()
        return result

    @staticmethod
    def translate_site_from_avro(*, site_avro: SiteAvro) -> Site or None:
        if site_avro is None:
            return None
        result = Site(name=site_avro.get_name(), maint_info=site_avro.get_maint_info())
        return result

    @staticmethod
    def translate_auth_to_avro(*, auth: AuthToken) -> AuthAvro:
        result = AuthAvro()
        result.name = auth.get_name()
        result.guid = str(auth.get_guid())
        result.oidc_sub_claim = auth.get_oidc_sub_claim()
        result.email = auth.get_email()
        return result

    @staticmethod
    def translate_auth_from_avro(*, auth_avro: AuthAvro) -> AuthToken:
        if auth_avro is None:
            return None
        result = AuthToken()
        result.name = auth_avro.name
        result.guid = auth_avro.guid
        result.oidc_sub_claim = auth_avro.oidc_sub_claim
        result.email = auth_avro.email
        return result

    @staticmethod
    def translate_term(*, term: Term) -> TermAvro:
        avro_term = TermAvro()
        if term.get_start_time() is not None:
            avro_term.start_time = ActorClock.to_milliseconds(when=term.get_start_time())
        else:
            term.start_time = 0
        if term.get_end_time() is not None:
            avro_term.end_time = ActorClock.to_milliseconds(when=term.get_end_time())
        else:
            term.end_time = 0
        if term.get_new_start_time() is not None:
            avro_term.new_start_time = ActorClock.to_milliseconds(when=term.get_new_start_time())
        else:
            avro_term.new_start_time = 0
        return avro_term

    @staticmethod
    def translate_resource_ticket(*, resource_ticket: ResourceTicket) -> ResourceTicketAvro:
        rd = ResourceTicketAvro()
        rd.units = resource_ticket.units
        if resource_ticket.term is not None:
            rd.term = Translate.translate_term(term=resource_ticket.term)

        if resource_ticket.type is not None:
            rd.type = str(resource_ticket.type)

        if resource_ticket.guid is not None:
            rd.guid = str(resource_ticket.guid)

        if resource_ticket.properties is not None:
            rd.properties = resource_ticket.properties

        if resource_ticket.issuer is not None:
            rd.issuer = str(resource_ticket.issuer)

        if resource_ticket.holder is not None:
            rd.holder = str(resource_ticket.holder)

        return rd

    @staticmethod
    def translate_resource_ticket_from_avro(*, resource_ticket: ResourceTicketAvro) -> ResourceTicket:
        rd = ResourceTicket()
        rd.units = resource_ticket.units
        if resource_ticket.term is not None:
            rd.term = Translate.translate_term_from_avro(term=resource_ticket.term)

        if resource_ticket.type is not None:
            rd.type = ResourceType(resource_type=resource_ticket.type)

        if resource_ticket.guid is not None:
            rd.guid = ID(uid=resource_ticket.guid)

        if resource_ticket.properties is not None:
            rd.properties = resource_ticket.properties

        if resource_ticket.issuer is not None:
            rd.issuer = ID(uid=resource_ticket.issuer)

        if resource_ticket.holder is not None:
            rd.holder = ID(uid=resource_ticket.holder)

        return rd

    @staticmethod
    def translate_ticket(*, ticket: Ticket) -> AvroTicket:
        avro_ticket = AvroTicket()
        avro_ticket.authority = Translate.translate_auth_to_avro(auth=ticket.authority.get_identity())
        avro_ticket.old_units = ticket.old_units
        avro_ticket.delegation_id = ticket.delegation_id
        avro_ticket.resource_ticket = Translate.translate_resource_ticket(
            resource_ticket=ticket.resource_ticket)
        return avro_ticket

    @staticmethod
    def translate_ticket_from_avro(*, avro_ticket: AvroTicket) -> Ticket:
        ticket = Ticket()
        ticket.delegation_id = avro_ticket.delegation_id
        auth_identity = Translate.translate_auth_from_avro(auth_avro=avro_ticket.authority)
        ticket.authority = ActorRegistrySingleton.get().get_proxy(protocol=Constants.PROTOCOL_KAFKA,
                                                                  actor_name=auth_identity.get_name())
        ticket.old_units = avro_ticket.old_units
        if avro_ticket.resource_ticket is not None:
            ticket.resource_ticket = Translate.translate_resource_ticket_from_avro(
                resource_ticket=avro_ticket.resource_ticket)

        return ticket

    @staticmethod
    def translate_resource_set(*, resource_set: ResourceSet) -> ResourceSetAvro:
        avro_rset = ResourceSetAvro()
        avro_rset.type = str(resource_set.get_type())
        avro_rset.units = resource_set.get_units()
        avro_rset.set_sliver(sliver=resource_set.get_sliver())
        return avro_rset

    @staticmethod
    def translate_term_from_avro(*, term: TermAvro) -> Term:
        start_time = None
        end_time = None
        new_start_time = None
        if term.start_time > 0:
            start_time = ActorClock.from_milliseconds(milli_seconds=term.start_time)

        if term.end_time > 0:
            end_time = ActorClock.from_milliseconds(milli_seconds=term.end_time)

        if term.new_start_time > 0:
            new_start_time = ActorClock.from_milliseconds(milli_seconds=term.new_start_time)

        return Term(start=start_time, end=end_time, new_start=new_start_time)

    @staticmethod
    def translate_resource_set_from_avro(*, rset: ResourceSetAvro) -> ResourceSet:
        result = ResourceSet(units=rset.units, rtype=ResourceType(resource_type=rset.type), sliver=rset.get_sliver())
        return result

    @staticmethod
    def attach_properties(*, slice_mng: SliceAvro, slice_obj: ABCSlice) -> SliceAvro:
        if slice_obj.get_config_properties() is not None:
            slice_mng.set_config_properties(slice_obj.get_config_properties())

        return slice_mng

    @staticmethod
    def fill_slices(*, slice_list: List[ABCSlice], full: bool) -> List[SliceAvro]:
        result = []
        for slice_obj in slice_list:
            if slice_obj is not None:
                ss = Translate.translate_slice_to_avro(slice_obj=slice_obj)
                if full:
                    ss = Translate.attach_properties(slice_mng=ss, slice_obj=slice_obj)
                result.append(ss)
        return result

    @staticmethod
    def translate_delegation_to_avro(*, delegation: ABCDelegation) -> DelegationAvro:
        avro_delegation = DelegationAvro()
        avro_delegation.delegation_id = delegation.get_delegation_id()
        avro_delegation.state = delegation.get_state().value
        avro_delegation.slice = Translate.translate_slice_to_avro(slice_obj=delegation.get_slice_object())
        avro_delegation.site = delegation.get_site()
        if delegation.get_graph() is not None and not (delegation.is_reclaimed() or delegation.is_closed()):
            avro_delegation.graph = delegation.get_graph().serialize_graph()
        return avro_delegation

    @staticmethod
    def translate_to_broker_query_model(*, query_response: dict, level: int) -> BrokerQueryModelAvro:
        bqm = BrokerQueryModelAvro()
        bqm.level = level
        bqm.model = query_response.get(Constants.BROKER_QUERY_MODEL, None)
        return bqm

    @staticmethod
    def translate_unit_set(*, unit_set: UnitSet) -> List[Units]:
        result = None
        if unit_set.units is not None:
            result = []
            for u in unit_set.units.values():
                result.append(Translate.translate_unit(unit=u))
        return result

    @staticmethod
    def translate_unit_from_avro(*, unit_avro: UnitAvro) -> Unit:
        unit = Unit(rid=ID(uid=unit_avro.reservation_id))

        if unit_avro.properties is not None:
            unit.properties = unit_avro.properties

        if unit_avro.rtype is not None:
            unit.rtype = ResourceType(resource_type=unit.rtype)

        if unit_avro.parent_id is not None:
            unit.parent_id = ID(uid=unit_avro.parent_id)

        if unit_avro.slice_id is not None:
            unit.slice_id = ID(uid=unit_avro.slice_id)

        if unit_avro.actor_id is not None:
            unit.actor_id = ID(uid=unit_avro.actor_id)

        if unit_avro.state is not None:
            unit.state = UnitState(unit_avro.state)
        return unit

    @staticmethod
    def translate_unit(*, unit: Unit) -> UnitAvro:
        result = UnitAvro()
        result.properties = unit.properties
        if unit.rtype is not None:
            result.rtype = str(unit.rtype)

        if unit.parent_id is not None:
            result.parent_id = str(unit.parent_id)

        if unit.reservation_id is not None:
            result.reservation_id = str(unit.reservation_id)

        if unit.slice_id is not None:
            result.slice_id = str(unit.slice_id)

        if unit.actor_id is not None:
            result.actor_id = str(unit.actor_id)

        if unit.state is not None:
            result.state = unit.state.value
        return result

    @staticmethod
    def translate_unit_set_from_avro(*, unit_list: List[UnitAvro]) -> UnitSet:
        unit_set = UnitSet(plugin=None)
        unit_set.units = {}
        for u in unit_list:
           obj = Translate.translate_unit_from_avro(unit_avro=u)
           unit_set.units[obj.get_id()] = obj
        return unit_set

    @staticmethod
    def translate_poa_avro_to_poa(*, poa_avro: PoaAvro) -> Union[Poa, None]:
        if poa_avro is None:
            return poa_avro
        if poa_avro.poa_id is None and poa_avro.rid is None:
            return None

        poa_obj = PoaFactory.create(poa_id=poa_avro.poa_id, operation=poa_avro.get_operation(),
                                    sliver_id=ID(uid=poa_avro.rid), vcpu_cpu_map=poa_avro.get_vcpu_cpu_map(),
                                    node_set=poa_avro.get_node_set(), keys=poa_avro.get_keys())
        poa_obj.sequence_poa_in = poa_avro.sequence
        poa_obj.slice_id = poa_avro.slice_id
        # NOTE: Sliver, Slice and Project Info to be set by the caller
        return poa_obj

    @staticmethod
    def translate_poa_to_poa_avro(*, poa: Poa) -> Union[PoaAvro, None]:
        if poa is None:
            return poa

        auth_avro = Translate.translate_auth_to_avro(auth=poa.get_slice().get_owner())
        poa_avro = PoaAvro(operation=poa.operation, rid=str(poa.sliver_id), poa_id=poa.poa_id,
                           vcpu_cpu_map=poa.vcpu_cpu_map, node_set=poa.node_set, auth=auth_avro,
                           project_id=poa.get_slice().get_project_id(), slice_id=str(poa.get_slice_id()),
                           keys=poa.keys)
        poa_avro.sequence = poa.sequence_poa_out
        return poa_avro

    @staticmethod
    def translate_poa_to_poa_info_avro(*, poa: Poa) -> Union[PoaInfoAvro, None]:
        if poa is None:
            return poa

        auth_avro = Translate.translate_auth_to_avro(auth=poa.get_slice().get_owner())
        poa_avro = PoaInfoAvro(operation=poa.operation, rid=str(poa.sliver_id), poa_id=str(poa.poa_id),
                               auth=auth_avro, project_id=poa.get_slice().get_project_id(),
                               slice_id=str(poa.get_slice().get_slice_id()), state=str(poa.get_state()),
                               error=poa.get_message())

        if poa.get_info() is not None:
            poa_avro.info = poa.get_info().copy()
        return poa_avro

    @staticmethod
    def translate_poa_to_result_poa_avro(*, poa: Poa) -> Union[ResultPoaAvro, None]:
        if poa is None:
            return poa

        poa_info_avro = Translate.translate_poa_to_poa_info_avro(poa=poa)

        poa_result_avro = ResultPoaAvro()
        poa_result_avro.status = ResultAvro()
        poa_result_avro.status.code = poa.get_error_code()
        poa_result_avro.status.message = poa.get_message()
        poa_result_avro.poas = [poa_info_avro]

        return poa_result_avro

    @staticmethod
    def translate_result_poa_avro_to_poa(*, poa_result: ResultPoaAvro) -> Union[Poa, None]:
        if poa_result is None:
            return poa_result
        if poa_result.poas is None or len(poa_result.poas) <= 0:
            return None

        poa_info = poa_result.poas[0]

        poa_obj = PoaFactory.create(poa_id=poa_info.poa_id, operation=poa_info.get_operation(),
                                    sliver_id=ID(uid=poa_info.rid))
        poa_obj.slice_id = poa_info.get_slice_id()
        poa_obj.error_code = poa_result.get_status().get_code()
        poa_obj.message = poa_result.get_status().get_message()
        poa_obj.info = poa_info.get_info()

        return poa_obj
