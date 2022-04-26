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

from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_units_avro import ResultUnitsAvro

from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.common.constants import Constants, ErrorCodes
from fabric_cf.actor.core.common.exceptions import ReservationNotFoundException
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.manage.management_object import ManagementObject
from fabric_cf.actor.core.manage.proxy_protocol_descriptor import ProxyProtocolDescriptor
from fabric_cf.actor.core.manage.server_actor_management_object import ServerActorManagementObject
from fabric_cf.actor.security.access_checker import AccessChecker
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType

if TYPE_CHECKING:
    from fabric_cf.actor.security.auth_token import AuthToken
    from fabric_cf.actor.core.apis.abc_substrate_database import ABCSubstrateDatabase
    from fabric_cf.actor.core.util.id import ID


class AuthorityManagementObject(ServerActorManagementObject):
    def __init__(self, *, authority: ABCAuthority = None):
        super().__init__(sa=authority)

    def register_protocols(self):
        from fabric_cf.actor.core.manage.local.local_authority import LocalAuthority
        local = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_LOCAL,
                                        proxy_class=LocalAuthority.__name__,
                                        proxy_module=LocalAuthority.__module__)

        from fabric_cf.actor.core.manage.kafka.kafka_authority import KafkaAuthority
        kakfa = ProxyProtocolDescriptor(protocol=Constants.PROTOCOL_KAFKA, proxy_class=KafkaAuthority.__name__,
                                        proxy_module=KafkaAuthority.__module__)

        self.proxies = []
        self.proxies.append(local)
        self.proxies.append(kakfa)

    def save(self) -> dict:
        properties = super().save()
        properties[Constants.PROPERTY_CLASS_NAME] = AuthorityManagementObject.__name__
        properties[Constants.PROPERTY_MODULE_NAME] = AuthorityManagementObject.__module__

        return properties

    def get_authority_reservations(self, *, caller: AuthToken) -> ResultReservationAvro:
        result = ResultReservationAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result
        try:
            res_list = None
            try:
                res_list = self.db.get_authority_reservations()
            except Exception as e:
                self.logger.error("get_authority_reservations:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if res_list is not None:
                result.reservations = []
                for r in res_list:
                    slice_obj = self.get_slice_by_guid(guid=r.get_slice_id())
                    r.restore(actor=self.actor, slice_obj=slice_obj, logger=self.logger)
                    rr = Converter.fill_reservation(reservation=r, full=True)
                    result.reservations.append(rr)
        except ReservationNotFoundException as e:
            self.logger.error("get_authority_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorNoSuchReservation.value)
            result.status.set_message(e.text)
        except Exception as e:
            self.logger.error("get_authority_reservations: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_substrate_database(self) -> ABCSubstrateDatabase:
        return self.actor.get_plugin().get_database()

    def get_reservation_units(self, *, caller: AuthToken, rid: ID) -> ResultUnitsAvro:
        result = ResultUnitsAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result
        try:

            units_list = None
            try:
                units_list = self.db.get_units(rid=rid)
            except Exception as e:
                self.logger.error("get_reservation_units:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if units_list is not None:
                result.units = Converter.fill_units(unit_list=units_list)
        except Exception as e:
            self.logger.error("get_reservation_units: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result

    def get_reservation_unit(self, *, caller: AuthToken, uid: ID) -> ResultUnitsAvro:
        result = ResultUnitsAvro()
        result.status = ResultAvro()

        if caller is None:
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            return result
        try:
            units_list = None
            try:
                unit = self.db.get_unit(uid=uid)
                units_list = [unit]
            except Exception as e:
                self.logger.error("get_reservation_unit:db access {}".format(e))
                result.status.set_code(ErrorCodes.ErrorDatabaseError.value)
                result.status.set_message(ErrorCodes.ErrorDatabaseError.interpret(exception=e))
                result.status = ManagementObject.set_exception_details(result=result.status, e=e)
                return result

            if units_list is not None:
                result.units = Converter.fill_units(unit_list=units_list)
        except Exception as e:
            self.logger.error("get_reservation_unit: {}".format(e))
            result.status.set_code(ErrorCodes.ErrorInvalidArguments.value)
            result.status.set_message(ErrorCodes.ErrorInvalidArguments.interpret())
            result.status = ManagementObject.set_exception_details(result=result.status, e=e)

        return result
