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

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro

from fabric_cf.actor.core.apis.abc_reservation_mixin import ReservationCategory
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.manage.kafka.services.kafka_client_actor_service import KafkaClientActorService
from fabric_cf.actor.core.manage.kafka.services.kafka_server_actor_service import KafkaServerActorService


class KafkaBrokerService(KafkaClientActorService, KafkaServerActorService):
    def process(self, *, message: AbcMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))
        result = self.authorize_request(id_token=message.get_id_token(), message_name=message.get_message_name())

        # If authorization failed, return the result
        if result is None:
            if message.get_message_name() == AbcMessageAvro.claim_resources:
                result = self.claim(request=message)

            elif message.get_message_name() == AbcMessageAvro.reclaim_resources:
                result = self.reclaim(request=message)

            elif message.get_message_name() == AbcMessageAvro.add_reservation:
                result = self.add_reservation(request=message)

            elif message.get_message_name() == AbcMessageAvro.add_reservations:
                result = self.add_reservations(request=message)

            elif message.get_message_name() == AbcMessageAvro.demand_reservation:
                result = self.demand_reservation(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_actors_request:
                result = self.get_brokers(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_broker_query_model_request:
                result = self.get_broker_query_model(request=message)

            elif message.get_message_name() == AbcMessageAvro.extend_reservation:
                result = self.extend_reservation(request=message)

            elif message.get_message_name() == AbcMessageAvro.get_reservations_request and \
                    message.get_type() is not None and \
                    message.get_type() == ReservationCategory.Broker.name:
                result = self.get_reservations_by_category(request=message, category=ReservationCategory.Broker)

            elif message.get_message_name() == AbcMessageAvro.get_slices_request and \
                    message.get_type() is not None and \
                    message.get_type() == SliceTypes.InventorySlice.name:
                result = self.get_slices_by_slice_type(request=message, slice_type=SliceTypes.InventorySlice)

            elif message.get_message_name() == AbcMessageAvro.get_reservations_request and \
                    message.get_type() is not None and \
                    message.get_type() == ReservationCategory.Inventory.name:
                result = self.get_reservations_by_category(request=message, category=ReservationCategory.Inventory)

            elif message.get_message_name() == AbcMessageAvro.get_slices_request and \
                    message.get_type() is not None and \
                    message.get_type() == SliceTypes.ClientSlice.name:
                result = self.get_slices_by_slice_type(request=message, slice_type=SliceTypes.ClientSlice)

            elif message.get_message_name() == AbcMessageAvro.add_slice and message.slice_obj is not None and \
                    (message.slice_obj.is_client_slice() or message.slice_obj.is_broker_client_slice()):
                result = self.add_client_slice(request=message)
            else:
                super().process(message=message)
                return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if result is None:
            self.logger.error("No response generated {}".format(result))
            return

        if self.producer.produce(topic=callback_topic, record=result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))
