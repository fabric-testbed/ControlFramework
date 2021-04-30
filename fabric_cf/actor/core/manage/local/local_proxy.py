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

from fabric_mb.message_bus.messages.result_avro import ResultAvro

from fabric_cf.actor.core.common.constants import ErrorCodes
from fabric_cf.actor.core.manage.error import Error
from fabric_cf.actor.core.apis.abc_component import ABCComponent

if TYPE_CHECKING:
    from fabric_cf.actor.core.manage.management_object import ManagementObject
    from fabric_cf.actor.security.auth_token import AuthToken


class LocalProxy(ABCComponent):
    def __init__(self, *, manager: ManagementObject, auth: AuthToken):
        self.manager = manager
        self.auth = auth
        self.last_status = None
        self.last_exception = None

    def clear_last(self):
        self.last_exception = None
        self.last_status = ResultAvro()

    def get_last_error(self) -> Error:
        return Error(status=self.last_status, e=self.last_exception)

    def get_manager_object(self) -> ManagementObject:
        return self.manager

    def get_protocols(self) -> list:
        return self.manager.get_proxies()

    def get_type_id(self) -> str:
        return str(self.manager.get_type_id())

    def get_first(self, *, result_list: list):
        if result_list is not None and len(result_list) > 0:
            return result_list.__iter__().__next__()
        return None

    def on_exception(self, e: Exception, traceback_str: str):
        self.last_exception = e
        status = ResultAvro()
        status.code = ErrorCodes.ErrorInternalError.value
        status.message = ErrorCodes.ErrorInternalError.interpret(exception=e)
        status.details = traceback_str
        self.last_status = status
