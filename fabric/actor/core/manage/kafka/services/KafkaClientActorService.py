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

import traceback

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.kafka.services.KafkaActorService import KafkaActorService
from fabric.actor.core.proxies.kafka.Translate import Translate
from fabric.message_bus.messages.ClaimResourcesAvro import ClaimResourcesAvro
from fabric.actor.core.util.ID import ID
from fabric.message_bus.messages.ClaimResourcesResponseAvro import ClaimResourcesResponseAvro
from fabric.message_bus.messages.ResultAvro import ResultAvro


class KafkaClientActorService(KafkaActorService):
    def __init__(self):
        super().__init__()

    def claim_resources(self, request: ClaimResourcesAvro) -> ClaimResourcesResponseAvro:
        result = ClaimResourcesResponseAvro()
        result.status = ResultAvro()
        try:
            if request.guid is None or request.broker_id is None or request.reservation_id is None:
                result.status.set_code(Constants.ErrorInvalidArguments)
                return result

            auth = Translate.translate_auth_from_avro(request.auth)
            mo = self.get_actor_mo(ID(request.guid))

            if mo is None:
                print("Management object could not be found: guid: {} auth: {}".format(request.guid, auth))
                result.status.set_code(Constants.ErrorNoSuchBroker)
                return result

            if request.slice_id is not None:
                temp_result = mo.claim_resources_slice(ID(request.broker_id), ID(request.slice_id), ID(request.reservation_id), auth)
            else:
                temp_result = mo.claim_resources(ID(request.broker_id), ID(request.reservation_id), auth)

            result.message_id = request.message_id

            if temp_result.status.get_code() == 0 and temp_result.result is not None and len(temp_result.result) > 0:
                result.reservation = temp_result.result.__iter__().__next__()

            result.status = temp_result.status
        except Exception as e:
            result.status.set_code(Constants.ErrorInternalError)
            result.status.set_message(str(e))
            result.status.set_details(traceback.format_exc())

        return result


