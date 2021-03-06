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
import concurrent.futures
import threading
import traceback

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils


class AnsibleHandlerProcessor(HandlerProcessor):
    MAX_WORKERS = 10

    def __init__(self):
        super().__init__()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS,
                                                              thread_name_prefix=self.__class__.__name__)
        self.futures = set()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['plugin']
        del state['initialized']
        del state['lock']
        del state['executor']
        del state['futures']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.plugin = None
        self.initialized = False
        self.lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS,
                                                              thread_name_prefix=self.__class__.__name__)
        self.futures = set()

    def shutdown(self):
        try:
            self.lock.acquire()
            self.executor.shutdown(wait=True)
            for f in self.futures:
                f.cancel()
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.lock.release()

    def set_logger(self, *, logger):
        self.logger = logger

    def invoke_handler(self, unit: ConfigToken, operation: str):
        try:
            handler = self.config_mappings.get(str(unit.get_resource_type()), None)
            if handler is None:
                raise AuthorityException(f"No handler found for resource type {unit.get_resource_type()}")

            handler_class = ReflectionUtils.create_instance_with_params(module_name=handler.get_module_name(),
                                                                        class_name=handler.get_class_name())
            handler_obj = handler_class(self.logger, handler.get_properties())

            self.lock.acquire()
            future = None
            if operation == Constants.TARGET_CREATE:
                future = self.executor.submit(handler_obj.create, unit)
            elif operation == Constants.TARGET_DELETE:
                future = self.executor.submit(handler_obj.delete, unit)
            elif operation == Constants.TARGET_MODIFY:
                future = self.executor.submit(handler_obj.modify, unit)
            else:
                raise AuthorityException("Invalid operation")

            future.add_done_callback(self.handler_complete)
            self.futures.add(future)
            self.logger.debug(f"Handler operation {operation} scheduled for Resource Type: {unit.get_resource_type()} "
                              f"Unit: {unit.get_id()} Reservation: {unit.get_reservation_id()}")

        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
            result = {Constants.PROPERTY_TARGET_NAME: operation,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}

            self.plugin.configuration_complete(token=unit, properties=result)
        finally:
            if self.lock.locked():
                self.lock.release()
            self.logger.info(f"Executing {operation} completed")

    def create(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_CREATE)

    def modify(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_MODIFY)

    def delete(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_DELETE)

    def handler_complete(self, future):
        self.logger.debug(f"Handler Execution completed Result: {future.result()}")
        if future.exception() is not None:
            self.logger.error(f"Exception occurred while executing the handler: {future.exception()}")
        properties, unit = future.result()
        try:
            self.lock.acquire()
            self.futures.remove(future)
            self.plugin.configuration_complete(token=unit, properties=properties)
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.lock.release()
