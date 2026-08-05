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
"""
Unit tests for Broker peer selection on the Orchestrator.

A client actor's broker registry holds both the Broker proxy and the Authority
proxies, so the Broker must be identified by type rather than by list position.
These tests need no Kafka/Postgres/Neo4j.
"""
import logging
import unittest

from fabric_cf.actor.boot.configuration import Peer
from fabric_cf.actor.core.apis.abc_actor_mixin import ActorType
from fabric_cf.actor.core.manage.converter import Converter
from fabric_cf.actor.core.proxies.kafka.kafka_authority_proxy import KafkaAuthorityProxy
from fabric_cf.actor.core.proxies.kafka.kafka_broker_proxy import KafkaBrokerProxy
from fabric_cf.actor.core.registry.peer_registry import PeerRegistry
from fabric_cf.actor.security.auth_token import AuthToken
from fabric_cf.orchestrator.core.orchestrator_handler import OrchestratorHandler


class FillProxyTypeTest(unittest.TestCase):
    """
    Converter.fill_proxy must carry the peer's actor type, otherwise every
    consumer of ProxyAvro.get_type() sees None.
    """
    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def test_broker_proxy_type(self):
        proxy = KafkaBrokerProxy(kafka_topic="broker-1", logger=self.logger,
                                 identity=AuthToken(name="broker", guid="broker-guid"))
        result = Converter.fill_proxy(proxy=proxy)
        self.assertEqual(ActorType.Broker,
                         ActorType.get_actor_type_from_string(actor_type=result.get_type()))
        self.assertEqual("broker", result.get_name())
        self.assertEqual("broker-1", result.get_kafka_topic())

    def test_authority_proxy_type(self):
        # KafkaAuthorityProxy derives from KafkaBrokerProxy; it must not be
        # reported as a Broker
        proxy = KafkaAuthorityProxy(kafka_topic="uky-am-1", logger=self.logger,
                                    identity=AuthToken(name="uky-am", guid="uky-am-guid"))
        result = Converter.fill_proxy(proxy=proxy)
        self.assertEqual(ActorType.Authority,
                         ActorType.get_actor_type_from_string(actor_type=result.get_type()))


class SelectBrokerProxyTest(unittest.TestCase):
    """
    Exercise OrchestratorHandler broker selection without booting a container.
    """
    def setUp(self):
        self.handler = OrchestratorHandler.__new__(OrchestratorHandler)
        self.handler.logger = logging.getLogger(self.__class__.__name__)
        self.handler.config = self
        self.peers = [Peer(config={"name": "broker", "type": "broker", "guid": "broker-guid"}),
                      Peer(config={"name": "uky-am", "type": "authority", "guid": "uky-am-guid"})]

    def get_peers(self):
        """
        Stand in for Configuration.get_peers()
        """
        return self.peers

    def select(self, brokers: list):
        """
        Invoke the name-mangled selection helper
        """
        return self.handler._OrchestratorHandler__select_broker_proxy(brokers=brokers)

    @staticmethod
    def make_proxy(*, name: str, guid: str, actor_type: str = None):
        """
        Build a ProxyAvro peer entry
        """
        from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
        proxy = ProxyAvro()
        proxy.set_name(name)
        proxy.set_guid(guid)
        proxy.set_type(actor_type)
        return proxy

    def test_selects_broker_not_first_entry(self):
        # Registry ordering is arbitrary after recovery: Authority entries first
        brokers = [self.make_proxy(name="uky-am", guid="uky-am-guid", actor_type="authority"),
                   self.make_proxy(name="lbnl-am", guid="lbnl-am-guid", actor_type="authority"),
                   self.make_proxy(name="broker", guid="broker-guid", actor_type="broker")]
        selected = self.select(brokers)
        self.assertIsNotNone(selected)
        self.assertEqual("broker-guid", selected.get_guid())

    def test_falls_back_to_configured_peer_name(self):
        # Proxies with no actor type must still resolve via the configured peers
        brokers = [self.make_proxy(name="uky-am", guid="uky-am-guid"),
                   self.make_proxy(name="broker", guid="broker-guid")]
        selected = self.select(brokers)
        self.assertIsNotNone(selected)
        self.assertEqual("broker-guid", selected.get_guid())

    def test_no_broker_returns_none(self):
        brokers = [self.make_proxy(name="uky-am", guid="uky-am-guid", actor_type="authority"),
                   self.make_proxy(name="lbnl-am", guid="lbnl-am-guid", actor_type="authority")]
        self.assertIsNone(self.select(brokers))


class PeerRegistryDefaultBrokerTest(unittest.TestCase):
    """
    The default broker must be a Broker peer, whatever order peers arrive in.
    """
    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.registry = PeerRegistry()

    def add(self, proxy):
        """
        Invoke the name-mangled default-broker setter
        """
        self.registry._PeerRegistry__set_default_broker_if_needed(broker=proxy)

    def make_broker(self):
        return KafkaBrokerProxy(kafka_topic="broker-1", logger=self.logger,
                                identity=AuthToken(name="broker", guid="broker-guid"))

    def make_authority(self, *, name: str = "uky-am"):
        return KafkaAuthorityProxy(kafka_topic=f"{name}-1", logger=self.logger,
                                   identity=AuthToken(name=name, guid=f"{name}-guid"))

    def test_authority_never_becomes_default(self):
        self.add(self.make_authority())
        self.add(self.make_authority(name="lbnl-am"))
        self.assertIsNone(self.registry.get_default_broker())

    def test_broker_wins_regardless_of_order(self):
        self.add(self.make_authority())
        self.add(self.make_broker())
        self.add(self.make_authority(name="lbnl-am"))
        default_broker = self.registry.get_default_broker()
        self.assertIsNotNone(default_broker)
        self.assertEqual("broker", default_broker.get_name())

    def test_first_broker_is_kept(self):
        first = self.make_broker()
        self.add(first)
        second = KafkaBrokerProxy(kafka_topic="broker-2", logger=self.logger,
                                  identity=AuthToken(name="broker-2", guid="broker-2-guid"))
        self.add(second)
        self.assertEqual("broker", self.registry.get_default_broker().get_name())


if __name__ == '__main__':
    unittest.main()
