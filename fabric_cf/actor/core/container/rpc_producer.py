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

import logging
import time

import threading
import traceback

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.producer import AvroProducerApi

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import KafkaServiceException
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.kernel.failed_rpc_event import FailedRPCEvent
from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.rpc_exception import RPCException, RPCError


class RPCProducer(AvroProducerApi):
    """
    Extends the AVRO Producer to invoke poll in a thread to handle delivery failure for messages
    """
    AVRO_RPC_TYPE_MAP = {AbcMessageAvro.close: RPCRequestType.Close,
                         AbcMessageAvro.update_lease: RPCRequestType.UpdateLease,
                         AbcMessageAvro.update_ticket: RPCRequestType.UpdateTicket}

    def __init__(self, *, producer_conf: dict, key_schema_location, value_schema_location: str, actor: ABCActorMixin,
                 logger: logging.Logger = None):
        super(RPCProducer, self).__init__(producer_conf=producer_conf, key_schema_location=key_schema_location,
                                          value_schema_location=value_schema_location, logger=logger)
        self.actor = actor
        self.thread_lock = threading.Lock()
        self.thread = None
        self.running = False

    def start(self):
        """
        Start Poller Thread which checks for the Delivery Failures
        """
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise KafkaServiceException(f"{self.__class__.__name__} has already been started")

            self.running = True
            self.thread = threading.Thread(target=self.delivery_check)
            self.thread.setName(self.__class__.__name__)
            self.thread.setDaemon(True)
            self.thread.start()
            self.logger.debug(f"{self.__class__.__name__} has been started")
        finally:
            self.thread_lock.release()

    def stop(self):
        """
        Stop the Poller Thread
        """
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            self.running = False
            if temp is not None:
                self.logger.warning(f"It seems that the {self.__class__.__name__} thread is running. Interrupting it")
                try:
                    temp.join()
                except Exception as e:
                    self.logger.error(f"Could not join {self.__class__.__name__} thread {e}")
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def delivery_check(self):
        """
        Thread Main function which periodically polls to check for message deliver
        """
        while self.running:
            try:
                num_msgs = self.poll(timeout=0.0)
                self.logger.debug(f"KAFKA: Processed messages {num_msgs}")
                time.sleep(10)
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"KAFKA: Error {e}")
                self.logger.error(traceback.format_exc())
                continue

        self.logger.debug(f"KAFKA: Shutting down {self.__class__.__name__}..")
        self.flush()

    @staticmethod
    def __is_update_lease_to_broker(*, topic: str, obj: AbcMessageAvro):
        """
        Check if the UpdateLease was sent to broker
        :param topic
        :param obj
        :return True if message was sent to Broker; False otherwise
        """
        ret_val = False
        if obj.name != AbcMessageAvro.update_lease:
            return ret_val
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        topic_to_peer = GlobalsSingleton.get().get_config().get_topic_peer_map()
        if topic_to_peer is None or topic_to_peer[topic]:
            return ret_val
        peer = topic_to_peer[topic]
        peer_type = ActorType.get_actor_type_from_string(actor_type=peer.get_type())
        return peer_type == ActorType.Broker

    def delivery_report(self, err, msg, obj: AbcMessageAvro):
        """
            Handle delivery reports served from producer.poll.
            This callback takes an extra argument, obj.
            This allows the original contents to be included for debugging purposes.
            :param err
            :param msg
            :param obj
        """
        if err is not None:
            self.logger.error(f"KAFKA: Message Delivery Failure! Error [{err}] MsgId: [{obj.id}] "
                              f"Msg Name: [{obj.name}] Topic: [{msg.topic()}]")
            obj.set_kafka_error(kafka_error=err)

            exception = RPCException(message=err, error=RPCError.NetworkError)

            if obj.name is not None and obj.name in self.AVRO_RPC_TYPE_MAP and obj.reservation is not None:
                # Temporary hack
                if RPCProducer.__is_update_lease_to_broker(topic=msg.topic, obj=obj):
                    self.logger.debug("Ignoring failure of UpdateLease to broker")
                    return
                # Send FailedRPC to the Actor
                failed = FailedRPC(e=exception, request_type=self.AVRO_RPC_TYPE_MAP[obj.name],
                                   rid=ID(uid=obj.reservation.reservation_id))
                self.actor.queue_event(incoming=FailedRPCEvent(actor=self.actor, failed=failed))
        else:
            self.logger.debug(f"KAFKA: Message Delivery Successful! MsgId: [{obj.id}] Msg Name: [{obj.name}] "
                              f"Topic: [{msg.topic()}] Partition [{msg.partition()}] Offset [{msg.offset()}]")
