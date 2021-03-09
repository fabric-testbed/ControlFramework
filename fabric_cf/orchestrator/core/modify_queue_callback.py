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
import threading
from typing import List

from fabric_cf.actor.core.util.id import ID
from fabric_cf.orchestrator.core.exceptions import OrchestratorException
from fabric_cf.orchestrator.core.orchestrator_kernel import OrchestratorKernelSingleton
from fabric_cf.orchestrator.core.i_status_update_callback import IStatusUpdateCallback
from fabric_cf.orchestrator.core.modify_operation import ModifyOperation
from fabric_cf.orchestrator.core.reservation_id_with_modify_index import ReservationIDWithModifyIndex


class ModifyQueueCallback(IStatusUpdateCallback):
    def __init__(self):
        self.lock = threading.Lock()
        self.modify_queue = {}

    def success(self, *, ok: List[ReservationIDWithModifyIndex], act_on: List[ID]):
        """
        Success callback
        :param ok:
        :param act_on:
        :return:
        """
        self.check_modify_queue(ok_or_failed=next(iter(ok)))

    def failure(self, *, failed: List[ReservationIDWithModifyIndex], ok: List[ReservationIDWithModifyIndex],
                act_on: List[ID]):
        """
        Failure callback
        :param failed:
        :param ok:
        :param act_on:
        :return:
        """
        self.check_modify_queue(ok_or_failed=next(iter(failed)))

    def check_modify_queue(self, *, ok_or_failed: ReservationIDWithModifyIndex):
        """
        Check Modify Queue
        :param ok_or_failed:
        :return:
        """
        try:
            self.lock.acquire()
            res_queue = self.modify_queue.get(ok_or_failed.get_reservation_id(), None)
            if res_queue is None:
                raise OrchestratorException("no queue found for {}, skipping processing".format(ok_or_failed))

            mop = res_queue.pop(0)

            if mop is None:
                raise OrchestratorException("no modify operation found at top of the queue, proceeding")

            if mop.get() != ok_or_failed:
                raise OrchestratorException("dequeued reservation {} which doesn't match expected {}".
                                            format(mop.get(),ok_or_failed))

            if len(res_queue) > 0:
                mop = res_queue.pop(0)
            else:
                mop = None

            if mop is not None:
                mop_index = self.modify_sliver(rid=ok_or_failed.get_reservation_id(),
                                               modify_sub_command=mop.get_sub_command(),
                                               properties=mop.get_properties())
                mop.override_index(mop_index)
                watch_list = [mop.get()]
                OrchestratorKernelSingleton.get().get_sut().add_modify_status_watch(watch=watch_list, act=None,
                                                                                    callback=self)
            else:
                self.modify_queue.pop(ok_or_failed.get_reservation_id())
        finally:
            self.lock.release()

    def enqueue_modify(self, *, res: str, modify_sub_command: str, properties: dict):
        """
        Enqueue Modify
        :param res:
        :param modify_sub_command:
        :param properties:
        :return:
        """
        rid = ID(uid=res)
        try:
            self.lock.acquire()
            reservation_queue = None
            if rid not in self.modify_queue:
                reservation_queue = []
            else:
                reservation_queue = self.modify_queue.get(rid)

            mop = ModifyOperation(rid=rid, index=0, modify_sub_command=modify_sub_command, properties=properties)
            reservation_queue.append(mop)

            self.modify_queue[rid] = reservation_queue

            if len(reservation_queue) == 1:
                mod_index = self.modify_sliver(rid=rid, modify_sub_command=modify_sub_command, properties=properties)
                mop.override_index(index=mod_index)
                watch_list = [mop.get()]
                OrchestratorKernelSingleton.get().get_sut().add_modify_status_watch(watch=watch_list, act=None,
                                                                                    callback=self)
        finally:
            self.lock.release()

    def modify_sliver(self, *, rid: ID, modify_sub_command: str, properties: dict) -> int:
        """
        Modify sliver
        :param rid:
        :param modify_sub_command:
        :param properties:
        :return:
        """
        try:
            controller = OrchestratorKernelSingleton.get().get_management_actor()
            reservation = controller.get_reservation(rid=rid)
            if reservation is None:
                raise OrchestratorException("Unable to find reservation {}".format(rid))

            config_props = reservation.get_config_properties()
            if config_props is None:
                raise OrchestratorException("Unable to get configuration properties for reservation {}".format(rid))

            # Update the properties
            # TODO
            index = 0

            controller.modify_reservation(rid=rid, modify_properties=properties)
            return index
        except Exception as e:
            raise OrchestratorException("Unable to modify sliver reservation: {}".format(e))


class ModifyQueueCallbackSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise OrchestratorException("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = ModifyQueueCallback()
        return self.__instance

    get = classmethod(get)