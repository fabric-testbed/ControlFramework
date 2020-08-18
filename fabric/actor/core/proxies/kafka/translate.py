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

import pickle
from datetime import datetime
from typing import TYPE_CHECKING

from fabric.actor.core.kernel.sesource_set import ResourceSet
from fabric.actor.core.kernel.slice_factory import SliceFactory

from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.resource_data import ResourceData
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.auth_token import AuthToken
from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.resource_data_avro import ResourceDataAvro
from fabric.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric.message_bus.messages.slice_avro import SliceAvro
from fabric.message_bus.messages.term_avro import TermAvro
from fabric.message_bus.messages.update_data_avro import UpdateDataAvro

from fabric.actor.core.apis.i_reservation import IReservation

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_slice import ISlice


class Translate:
    # The direction constants specify the direction of a request. They are used
    # to determine what properties should pass from one actor to another.
    DirectionAgent = 1
    DirectionAuthority = 2
    DirectionReturn = 3
    @staticmethod
    def translate_reservation_to_avro(reservation: IReservation):
        result = None
        try:
            result = pickle.dumps(reservation)
        except Exception as e:
            print("Exception occurred {}".format(e))

        return result

    @staticmethod
    def translate_reservation_from_avro(reservation) -> IReservation:
        result = None
        try:
            result = pickle.loads(reservation)
        except Exception as e:
            print("Exception occurred {}".format(e))

        return result

    @staticmethod
    def translate_udd(udd: UpdateData) -> UpdateDataAvro:
        result = UpdateDataAvro()
        result.message = udd.get_message()
        result.failed = udd.is_failed()
        return result

    @staticmethod
    def translate_udd_from_avro(udd: UpdateDataAvro) -> UpdateData:
        if udd.failed:
            return UpdateData(udd.message)
        else:
            return UpdateData()

    @staticmethod
    def translate_slice(slice_id: ID, slice_name: str) -> ISlice:
        if slice_id is None and slice_name is None:
            return None

        if slice_id is None:
            raise Exception("Missing guid")

        slice_obj = SliceFactory.create(slice_id, slice_name)
        return slice_obj

    @staticmethod
    def translate_slice_to_avro(slice_obj: ISlice) -> SliceAvro:
        avro_slice = SliceAvro()
        avro_slice.slice_name = slice_obj.get_name()
        avro_slice.guid = str(slice_obj.get_slice_id())
        avro_slice.description = slice_obj.get_description()
        avro_slice.owner = Translate.translate_auth_to_avro(slice_obj.get_owner())
        return avro_slice

    @staticmethod
    def translate_auth_to_avro(auth: AuthToken) -> AuthAvro:
        result = AuthAvro()
        result.name = auth.name
        result.guid = str(auth.guid)
        result.id_token = auth.id_token
        return result

    @staticmethod
    def translate_auth_from_avro(auth_avro: AuthAvro) -> AuthToken:
        if auth_avro is None:
            return None
        result = AuthToken()
        result.name = auth_avro.name
        result.guid = auth_avro.guid
        result.id_token = auth_avro.id_token
        return result

    @staticmethod
    def translate_term(term: Term) -> TermAvro:
        avro_term = TermAvro()
        if term.get_start_time() is not None:
            avro_term.start_time = int(term.get_start_time().timestamp() * 1000)
        else:
            term.start_time = 0
        if term.get_end_time() is not None:
            avro_term.end_time = int(term.get_end_time().timestamp() * 1000)
        else:
            term.end_time = 0
        if term.get_new_start_time() is not None:
            avro_term.new_start_time = int(term.get_new_start_time().timestamp() * 1000)
        else:
            avro_term.new_start_time = 0
        return avro_term

    @staticmethod
    def translate_resource_data(resource_data: ResourceData, direction: int) -> ResourceDataAvro:
        if resource_data is None:
            return None

        avro_rdata = ResourceDataAvro()
        if direction == Translate.DirectionAgent:
            properties = resource_data.get_request_properties()
            avro_rdata.request_properties = properties

        elif direction == Translate.DirectionAuthority:
            properties = resource_data.get_config_properties()
            avro_rdata.config_properties = properties

        elif direction == Translate.DirectionReturn:
            properties = resource_data.get_resource_properties()
            avro_rdata.resource_properties = properties

        else:
            return None
        return avro_rdata

    @staticmethod
    def translate_resource_set(resource_set: ResourceSet, direction: int) -> ResourceSetAvro:
        avro_rset = ResourceSetAvro()
        avro_rset.type = str(resource_set.get_type())
        avro_rset.units = resource_set.get_units()
        avro_rset.resource_data = Translate.translate_resource_data(resource_set.get_resource_data(), direction)
        return avro_rset

    @staticmethod
    def translate_resource_data_from_avro(rdata: ResourceDataAvro) -> ResourceData:
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
    def translate_term_from_avro(term: TermAvro) -> Term:
        start_time = None
        end_time = None
        new_start_time = None
        if term.start_time > 0:
            start_time = datetime.fromtimestamp(term.start_time / 1000)

        if term.end_time > 0:
            end_time = datetime.fromtimestamp(term.end_time / 1000)

        if term.new_start_time > 0:
            new_start_time = datetime.fromtimestamp(term.new_start_time / 1000)

        return Term(start=start_time, end=end_time, new_start=new_start_time)

    @staticmethod
    def translate_resource_set_from_avro(rset: ResourceSetAvro) -> ResourceSet:
        rdata = Translate.translate_resource_data_from_avro(rset.resource_data)
        result = ResourceSet(units=rset.units, rtype=ResourceType(rset.type), rdata=rdata)
        return result

    @staticmethod
    def fill_slice(slice_obj: ISlice) -> SliceAvro:
        result = SliceAvro()
        result.set_slice_name(slice_obj.get_name())
        result.set_description(slice_obj.get_description())
        result.set_slice_id(str(slice_obj.get_slice_id()))
        result.set_owner(Translate.translate_auth_to_avro(slice_obj.get_owner()))

        if slice_obj.get_resource_type() is not None:
            result.set_resource_type(str(slice_obj.get_resource_type()))

        result.set_client_slice(slice_obj.is_client())

        return result

    @staticmethod
    def attach_properties(slice_mng: SliceAvro, slice_obj: ISlice) -> SliceAvro:
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
    def fill_slices(slice_list: list, full: bool) -> list:
        result = []
        for s in slice_list:
            slice_obj = SliceFactory.create_instance(s)
            if slice_obj is not None:
                ss = Translate.fill_slice(slice_obj)
                if full:
                    ss = Translate.attach_properties(ss, slice_obj)
                result.append(ss)
        return result

    @staticmethod
    def absorb_properties(slice_mng: SliceAvro, slice_obj: ISlice) -> ISlice:
        slice_obj.set_local_properties(
            PropList.merge_properties(slice_mng.get_local_properties(), slice_obj.get_local_properties()))

        slice_obj.set_config_properties(
            PropList.merge_properties(slice_mng.get_config_properties(), slice_obj.get_config_properties()))

        slice_obj.set_request_properties(
            PropList.merge_properties(slice_mng.get_request_properties(), slice_obj.get_request_properties()))

        slice_obj.set_resource_properties(
            PropList.merge_properties(slice_mng.get_resource_properties(), slice_obj.get_resource_properties()))

        return slice_obj
