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
from fabric_cf.actor.core.core.actor import ActorMixin
from fabric_cf.actor.core.manage.kafka.services.kafka_authority_service import KafkaAuthorityService
from fabric_cf.actor.core.proxies.kafka.services.authority_service import AuthorityService


class TestActor(ActorMixin):
    @staticmethod
    def get_kafka_service_class() -> str:
        return AuthorityService.__name__

    @staticmethod
    def get_kafka_service_module() -> str:
        return AuthorityService.__module__

    @staticmethod
    def get_mgmt_kafka_service_class() -> str:
        return KafkaAuthorityService.__name__

    @staticmethod
    def get_mgmt_kafka_service_module() -> str:
        return KafkaAuthorityService.__module__
