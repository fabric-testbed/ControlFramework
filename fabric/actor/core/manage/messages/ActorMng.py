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
if TYPE_CHECKING:
    from fabric.message_bus.messages.AuthTokenAvro import AuthTokenAvro


class ActorMng:
    def __init__(self):
        self.name = None
        self.type = None
        self.owner = None
        self.descripton = None
        self.policy_class = None
        self.policy_module = None
        self.event_handler = None
        self.load_source = None
        self.actor_class = None
        self.actor_module = None
        self.online = None
        self.management_class = None
        self.management_module = None
        self.id = None
        self.policy_guid = None

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str):
        self.name = value

    def set_type(self, value: int):
        self.type = value

    def get_type(self) -> int:
        return self.type

    def set_owner(self, value: AuthTokenAvro):
        self.owner = value

    def get_owner(self) -> AuthTokenAvro:
        return self.owner

    def set_description(self, value: str):
        self.description = value

    def get_description(self) -> str:
        return self.description

    def get_policy_class(self) -> str:
        return self.policy_class

    def set_policy_class(self, value: str):
        self.policy_class = value

    def get_policy_module(self) -> str:
        return self.policy_module

    def set_policy_module(self, value: str):
        self.policy_module = value

    def get_event_handler(self) -> str:
        return self.event_handler

    def set_event_handler(self, value: str):
        self.event_handler = value

    def get_load_source(self) -> str:
        return self.load_source

    def set_load_source(self, value: str):
        self.load_source = value

    def get_actor_class(self) -> str:
        return self.actor_class

    def set_actor_class(self, value: str):
        self.actor_class = value

    def get_actor_module(self) -> str:
        return self.actor_module

    def set_actor_module(self, value: str):
        self.actor_module = value

    def set_online(self, value: bool):
        self.online = value

    def get_online(self) -> bool:
        return self.online

    def get_management_class(self) -> str:
        return self.management_class

    def set_management_class(self, value: str):
        self.management_class = value

    def get_management_module(self) -> str:
        return self.management_module

    def set_management_module(self, value: str):
        self.management_module = value

    def set_id(self, value: str):
        self.id = value

    def get_id(self) -> str:
        return self.id

    def set_policy_guid(self, value: str):
        self.policy_guid = value

    def get_policy_guid(self) -> str:
        return self.policy_guid
