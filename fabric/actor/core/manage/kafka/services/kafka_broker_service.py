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

from fabric.actor.core.apis.i_reservation import ReservationCategory
from fabric.actor.core.kernel.slice import SliceTypes
from fabric.actor.core.manage.kafka.services.kafka_client_actor_service import KafkaClientActorService
from fabric.actor.core.manage.kafka.services.kafka_server_actor_service import KafkaServerActorService
from fabric.message_bus.messages.message import IMessageAvro


class KafkaBrokerService(KafkaClientActorService, KafkaServerActorService):
    def __init__(self):
        super().__init__()

    def process(self, *, message: IMessageAvro):
        callback_topic = message.get_callback_topic()
        result = None

        self.logger.debug("Processing message: {}".format(message.get_message_name()))

        if message.get_message_name() == IMessageAvro.ClaimResources:
            result = self.claim_resources(request=message)
        elif message.get_message_name() == IMessageAvro.ReclaimResources:
            result = self.reclaim_resources(request=message)
        elif message.get_message_name() == IMessageAvro.AddReservation:
            result = self.add_reservation(request=message)
        elif message.get_message_name() == IMessageAvro.AddReservations:
            result = self.add_reservations(request=message)
        elif message.get_message_name() == IMessageAvro.DemandReservation:
            result = self.demand_reservation(request=message)
        elif message.get_message_name() == IMessageAvro.GetActorsRequest:
            result = self.get_brokers(request=message)
        elif message.get_message_name() == IMessageAvro.GetPoolInfoRequest:
            result = self.get_pool_info(request=message)
        elif message.get_message_name() == IMessageAvro.ExtendReservation:
            result = self.extend_reservation(request=message)
        elif message.get_message_name() == IMessageAvro.GetReservationsRequest and \
                message.get_reservation_type() is not None and \
                message.get_reservation_type() == ReservationCategory.Broker.name:
            self.get_broker_reservations(request=message)
        elif message.get_message_name() == IMessageAvro.GetSlicesRequest and \
                message.get_slice_type() is not None and \
                message.get_slice_type() == SliceTypes.InventorySlice.name:
            self.get_inventory_slices(request=message)
        elif message.get_message_name() == IMessageAvro.GetReservationsRequest and \
                message.get_reservation_type() is not None and \
                message.get_reservation_type() == ReservationCategory.Client.name:
            self.get_inventory_reservations(request=message)
        elif message.get_message_name() == IMessageAvro.GetSlicesRequest and \
                message.get_slice_type() is not None and \
                message.get_slice_type() == SliceTypes.ClientSlice.name:
            self.get_client_slices(request=message)
        elif message.get_message_name() == IMessageAvro.AddSlice and message.slice_obj is not None and \
                (message.slice_obj.is_client_slice() or message.slice_obj.is_broker_client_slice()):
            self.add_client_slice(request=message)
        else:
            super().process(message=message)
            return

        if callback_topic is None:
            self.logger.debug("No callback specified, ignoring the message")

        if self.producer.produce_sync(topic=callback_topic, record=result):
            self.logger.debug("Successfully send back response: {}".format(result.to_dict()))
        else:
            self.logger.debug("Failed to send back response: {}".format(result.to_dict()))

