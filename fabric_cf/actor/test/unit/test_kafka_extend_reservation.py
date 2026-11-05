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
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from fabric_mb.message_bus.messages.extend_reservation_avro import ExtendReservationAvro

from fabric_cf.actor.core.manage.controller_management_object import ControllerManagementObject
from fabric_cf.actor.core.manage.kafka.services.kafka_client_actor_service import KafkaClientActorService
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.util.id import ID


class KafkaExtendReservationTest(unittest.TestCase):
    """
    An extend (renew) from the management CLI arrives over Kafka; the service must call the
    management object with the arguments it accepts, or every renew fails with an internal error
    """
    def test_extend_reaches_management_object(self):
        service = KafkaClientActorService.__new__(KafkaClientActorService)
        service.logger = MagicMock()
        mo = MagicMock(spec=ControllerManagementObject)
        mo.extend_reservation.return_value = "ok"
        service.get_actor_mo = MagicMock(return_value=mo)

        end = datetime(2026, 11, 14, 23, 36, 4, tzinfo=timezone.utc)
        request = ExtendReservationAvro()
        request.guid = str(ID())
        request.message_id = "msg"
        request.reservation_id = str(ID())
        request.end_time = ActorClock.to_milliseconds(when=end)

        result = service.extend_reservation(request=request)

        self.assertEqual("ok", result.status)
        kwargs = mo.extend_reservation.call_args.kwargs
        self.assertEqual(request.reservation_id, str(kwargs["reservation"]))
        self.assertEqual(end, kwargs["new_end_time"])
        self.assertIsNone(kwargs["sliver"])


if __name__ == '__main__':
    unittest.main()
