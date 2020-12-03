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

from fabric.actor.core.apis.i_delegation import IDelegation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.common.exceptions import ProxyException
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory
from fabric.actor.core.time.actor_clock import ActorClock

from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.auth_token import AuthToken
from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.delegation_avro import DelegationAvro
from fabric.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric.message_bus.messages.resource_data_avro import ResourceDataAvro
from fabric.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric.message_bus.messages.slice_avro import SliceAvro
from fabric.message_bus.messages.term_avro import TermAvro
from fabric.message_bus.messages.update_data_avro import UpdateDataAvro


if TYPE_CHECKING:
    from fabric.actor.core.apis.i_slice import ISlice


class Translate:
    # The direction constants specify the direction of a request. They are used
    # to determine what properties should pass from one actor to another.
    direction_agent = 1
    direction_authority = 2
    direction_return = 3

    @staticmethod
    def translate_udd(*, udd: UpdateData) -> UpdateDataAvro:
        result = UpdateDataAvro()
        result.message = udd.get_message()
        result.failed = udd.is_failed()
        return result

    @staticmethod
    def translate_udd_from_avro(*, udd: UpdateDataAvro) -> UpdateData:
        if udd.failed:
            return UpdateData(message=udd.message)
        else:
            return UpdateData()

    @staticmethod
    def translate_slice(*, slice_id: ID, slice_name: str) -> ISlice:
        if slice_id is None and slice_name is None:
            return None

        if slice_id is None:
            raise ProxyException(Constants.not_specified_prefix.format("Slice id"))

        slice_obj = SliceFactory.create(slice_id=slice_id, name=slice_name)
        return slice_obj

    @staticmethod
    def translate_slice_to_avro(*, slice_obj: ISlice) -> SliceAvro:
        avro_slice = SliceAvro()
        avro_slice.slice_name = slice_obj.get_name()
        avro_slice.guid = str(slice_obj.get_slice_id())
        avro_slice.description = slice_obj.get_description()
        avro_slice.owner = Translate.translate_auth_to_avro(auth=slice_obj.get_owner())

        if slice_obj.get_resource_type() is not None:
            avro_slice.set_resource_type(str(slice_obj.get_resource_type()))

        avro_slice.set_client_slice(slice_obj.is_client())

        if slice_obj.get_graph_id() is not None:
            avro_slice.graph_id = str(slice_obj.get_graph_id())

        return avro_slice

    @staticmethod
    def translate_auth_to_avro(*, auth: AuthToken) -> AuthAvro:
        result = AuthAvro()
        result.name = auth.name
        result.guid = str(auth.guid)
        result.id_token = auth.id_token
        return result

    @staticmethod
    def translate_auth_from_avro(*, auth_avro: AuthAvro) -> AuthToken:
        if auth_avro is None:
            return None
        result = AuthToken()
        result.name = auth_avro.name
        result.guid = auth_avro.guid
        result.id_token = auth_avro.id_token
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
    def translate_resource_data(*, resource_data: ResourceData, direction: int) -> ResourceDataAvro:
        if resource_data is None:
            return None

        avro_rdata = ResourceDataAvro()
        if direction == Translate.direction_agent:
            properties = resource_data.get_request_properties()
            avro_rdata.request_properties = properties

        elif direction == Translate.direction_authority:
            properties = resource_data.get_configuration_properties()
            avro_rdata.config_properties = properties

        elif direction == Translate.direction_return:
            properties = resource_data.get_resource_properties()
            avro_rdata.resource_properties = properties

        else:
            return None
        return avro_rdata

    @staticmethod
    def translate_resource_set(*, resource_set: ResourceSet, direction: int) -> ResourceSetAvro:
        avro_rset = ResourceSetAvro()
        avro_rset.type = str(resource_set.get_type())
        avro_rset.units = resource_set.get_units()
        avro_rset.resource_data = Translate.translate_resource_data(resource_data=resource_set.get_resource_data(),
                                                                    direction=direction)
        return avro_rset

    @staticmethod
    def translate_resource_data_from_avro(*, rdata: ResourceDataAvro) -> ResourceData:
        result = ResourceData()
        properties = rdata.config_properties
        if properties is not None:
            result.configuration_properties = properties

        properties = rdata.request_properties
        if properties is not None:
            result.request_properties = properties

        properties = rdata.resource_properties
        if properties is not None:
            result.resource_properties = properties

        return result

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
        rdata = Translate.translate_resource_data_from_avro(rdata=rset.resource_data)
        result = ResourceSet(units=rset.units, rtype=ResourceType(resource_type=rset.type), rdata=rdata)
        return result

    @staticmethod
    def attach_properties(*, slice_mng: SliceAvro, slice_obj: ISlice) -> SliceAvro:
        if slice_obj.get_request_properties() is not None:
            slice_mng.set_request_properties(slice_obj.get_request_properties())

        if slice_obj.get_resource_properties() is not None:
            slice_mng.set_resource_properties(slice_obj.get_resource_properties())

        if slice_obj.get_config_properties() is not None:
            slice_mng.set_config_properties(slice_obj.get_config_properties())

        if slice_obj.get_local_properties() is not None:
            slice_mng.set_config_properties(slice_obj.get_local_properties())

        return slice_mng

    @staticmethod
    def fill_slices(*, slice_list: list, full: bool) -> list:
        result = []
        for s in slice_list:
            slice_obj = SliceFactory.create_instance(properties=s)
            if slice_obj is not None:
                ss = Translate.translate_slice_to_avro(slice_obj=slice_obj)
                if full:
                    ss = Translate.attach_properties(slice_mng=ss, slice_obj=slice_obj)
                result.append(ss)
        return result

    @staticmethod
    def absorb_properties(*, slice_mng: SliceAvro, slice_obj: ISlice) -> ISlice:
        slice_obj.set_local_properties(value=PropList.merge_properties(
            incoming=slice_mng.get_local_properties(), outgoing=slice_obj.get_local_properties()))

        slice_obj.set_config_properties(value=PropList.merge_properties(
            incoming=slice_mng.get_config_properties(), outgoing=slice_obj.get_config_properties()))

        slice_obj.set_request_properties(value=PropList.merge_properties(
            incoming=slice_mng.get_request_properties(), outgoing=slice_obj.get_request_properties()))

        slice_obj.set_resource_properties(value=PropList.merge_properties(
            incoming=slice_mng.get_resource_properties(), outgoing=slice_obj.get_resource_properties()))

        return slice_obj

    @staticmethod
    def translate_delegation_to_avro(*, delegation: IDelegation) -> DelegationAvro:
        avro_delegation = DelegationAvro()
        avro_delegation.delegation_id = delegation.get_delegation_id()
        avro_delegation.slice = Translate.translate_slice_to_avro(slice_obj=delegation.get_slice_object())
        if delegation.get_graph() is not None:
            avro_delegation.graph = delegation.get_graph().serialize_graph()
        return avro_delegation

    @staticmethod
    def translate_to_pool_info(*, query_response: dict) -> PoolInfoAvro:
        bqm = query_response.get(Constants.broker_query_model, None)
        pool_info = PoolInfoAvro()
        pool_info.type = Constants.pool_type
        pool_info.name = Constants.broker_query_model
        if bqm is not None:
            pool_info.properties = {Constants.broker_query_model: bqm}
        return pool_info
