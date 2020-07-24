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

from fabric.actor.core.apis.IAuthority import IAuthority
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.common.Exceptions import ReservationNotFoundException
from fabric.actor.core.kernel.ReservationFactory import ReservationFactory
from fabric.actor.core.manage.Converter import Converter
from fabric.actor.core.manage.ManagementObject import ManagementObject
from fabric.actor.core.manage.ProxyProtocolDescriptor import ProxyProtocolDescriptor
from fabric.actor.core.manage.ServerActorManagementObject import ServerActorManagementObject
from fabric.actor.core.manage.messages.ResultReservationMng import ResultReservationMng
from fabric.actor.core.manage.messages.ResultUnitMng import ResultUnitMng
from fabric.message_bus.messages.ResultAvro import ResultAvro

if TYPE_CHECKING:
    from fabric.actor.security.AuthToken import AuthToken
    from fabric.actor.core.apis.ISubstrateDatabase import ISubstrateDatabase
    from fabric.actor.core.util.ID import ID


class AuthorityManagementObject(ServerActorManagementObject):
    def __init__(self, authority: IAuthority = None):
        super().__init__(authority)

    def register_protocols(self):
        from fabric.actor.core.manage.local.LocalAuthority import LocalAuthority
        local = ProxyProtocolDescriptor(Constants.ProtocolLocal, LocalAuthority.__name__, LocalAuthority.__module__)

        from fabric.actor.core.manage.kafka.KafkaAuthority import KafkaAuthority
        kakfa = ProxyProtocolDescriptor(Constants.ProtocolKafka, KafkaAuthority.__name__, KafkaAuthority.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kakfa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PropertyClassName] = AuthorityManagementObject.__name__,
        properties[Constants.PropertyModuleName] = AuthorityManagementObject.__name__

        return properties

    def get_authority_reservations(self, caller: AuthToken) -> ResultReservationMng:
        result = ResultReservationMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_authority_reservations()
            except Exception as e:
                self.logger.error("get_authority_reservations:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if res_list is not None:
                result.result = []
                for r in res_list:
                    slice_obj = self.get_slice_by_id(r['slc_id'])
                    rsv_obj = ReservationFactory.create_instance(r, self.actor, slice_obj,
                                                                 self.actor.get_logger())
                    if rsv_obj is not None:
                        rr = Converter.fill_reservation(rsv_obj, False)
                        result.result.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("getReservations: {}".format(e))
            result.status.set_code(Constants.ErrorNoSuchReservation)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_authority_reservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_substrate_database(self) -> ISubstrateDatabase:
        return self.actor.get_plugin().get_database()

    def get_reservation_units(self, caller: AuthToken, rid: ID) -> ResultUnitMng:
        result = ResultUnitMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result
        try:
            units_list = None
            try:
                units_list = self.db.get_units(rid)
            except Exception as e:
                self.logger.error("get_reservation_units:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if units_list is not None:
                result.result = Converter.fill_units(units_list)
        except Exception as e:
            self.logger.error("get_reservation_units: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result

    def get_reservation_unit(self, caller: AuthToken, uid: ID) -> ResultUnitMng:
        result = ResultUnitMng()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(Constants.ErrorInvalidArguments)
            return result
        try:
            units_list = None
            try:
                unit = self.db.get_unit(uid)
                units_list = [unit]
            except Exception as e:
                self.logger.error("get_reservation_units:db access {}".format(e))
                result.status.set_code(Constants.ErrorDatabaseError)
                result.status = ManagementObject.set_exception_details(result.status, e)
                return result

            if units_list is not None:
                result.result = Converter.fill_units(units_list)
        except Exception as e:
            self.logger.error("get_authority_reservations: {}".format(e))
            result.status.set_code(Constants.ErrorInternalError)
            result.status = ManagementObject.set_exception_details(result.status, e)

        return result