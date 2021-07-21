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
import time
import traceback

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils


class AnsibleHandlerProcessor(HandlerProcessor):
    """
    Ansible Handler Processor
    """
    MAX_WORKERS = 10

    def __init__(self):
        super().__init__()
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.MAX_WORKERS)
        self.futures = []
        self.thread = None
        self.future_lock = threading.Condition()
        self.stopped = False
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.log_config = GlobalsSingleton.get().get_log_config()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['plugin']
        del state['initialized']
        del state['lock']
        del state['executor']
        del state['futures']
        del state['thread']
        del state['future_lock']
        del state['stopped']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.plugin = None
        self.initialized = False
        self.lock = threading.Lock()
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.MAX_WORKERS)
        self.futures = []
        self.thread = None
        self.future_lock = threading.Condition()
        self.stopped = False

    def start(self):
        """
        Start the Future Processor Thread
        """
        self.thread = threading.Thread(target=self.process_futures)
        self.thread.setName("FutureProcessor")
        self.thread.setDaemon(True)
        self.thread.start()

    def shutdown(self):
        """
        Shutdown Process Pool, Future Processor
        """
        try:
            self.lock.acquire()
            self.executor.shutdown(wait=True)
            self.stopped = True
            for f, u in self.futures:
                f.cancel()

            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the future thread is running. Interrupting it")
                try:
                    with self.future_lock:
                        self.future_lock.notify_all()
                    temp.join()
                except Exception as e:
                    self.logger.error("Could not join future thread {}".format(e))
                    self.logger.error(traceback.format_exc())
                finally:
                    self.lock.release()
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.lock.release()

    def set_logger(self, *, logger):
        self.logger = logger

    def queue_future(self, future: concurrent.futures.Future, unit: ConfigToken):
        """
        Queue an future on Future Processor timer queue
        @param future
        @param unit Unit being processed; This is needed
        """
        with self.future_lock:
            self.futures.append((future, unit))
            self.logger.debug("Added future to Future queue")
            self.future_lock.notify_all()

    def invoke_handler(self, unit: ConfigToken, operation: str):
        try:
            handler = self.config_mappings.get(str(unit.get_resource_type()), None)
            if handler is None:
                raise AuthorityException(f"No handler found for resource type {unit.get_resource_type()}")

            handler_class = ReflectionUtils.create_instance_with_params(module_name=handler.get_module_name(),
                                                                        class_name=handler.get_class_name())
            handler_obj = handler_class(self.log_config, handler.get_properties())

            future = None
            if operation == Constants.TARGET_CREATE:
                future = self.executor.submit(handler_obj.create, unit)
            elif operation == Constants.TARGET_DELETE:
                future = self.executor.submit(handler_obj.delete, unit)
            elif operation == Constants.TARGET_MODIFY:
                future = self.executor.submit(handler_obj.modify, unit)
            else:
                raise AuthorityException("Invalid operation")

            self.queue_future(future=future, unit=unit)
            self.logger.debug(f"Handler operation {operation} scheduled for Resource Type: {unit.get_resource_type()} "
                              f"Unit: {unit.get_id()} Reservation: {unit.get_reservation_id()}")

        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
            result = {Constants.PROPERTY_TARGET_NAME: operation,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}

            self.handler_complete(unit=unit, properties=result, old_unit=unit)
        finally:
            self.logger.info(f"Executing {operation} completed")

    def create(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_CREATE)

    def modify(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_MODIFY)

    def delete(self, unit: ConfigToken):
        self.invoke_handler(unit=unit, operation=Constants.TARGET_DELETE)

    def handler_complete(self, properties: dict, unit: ConfigToken, old_unit: ConfigToken):
        try:
            self.lock.acquire()
            self.logger.debug(f"Properties: {properties} Unit: {unit}")
            # Copy the sliver from the Unit to
            old_unit.update_sliver(sliver=unit.get_sliver())
            self.plugin.configuration_complete(token=old_unit, properties=properties)
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.lock.release()

    def process(self, future: concurrent.futures.Future, old_unit: ConfigToken):
        try:
            self.logger.debug(f"Handler Execution completed Result: {future.result()}")
            if future.exception() is not None:
                self.logger.error(f"Exception occurred while executing the handler: {future.exception()}")
            properties, unit = future.result()
            self.handler_complete(properties=properties, unit=unit, old_unit=old_unit)
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())

    def process_futures(self):
        while True:
            done = []

            with self.future_lock:

                while len(self.futures) == 0 and not self.stopped:
                    try:
                        self.future_lock.wait()
                    except InterruptedError as e:
                        self.logger.info("Future Processor thread interrupted. Exiting")
                        return

                if self.stopped:
                    self.logger.info("Future Processor exiting")
                    return

                if len(self.futures) > 0:
                    try:
                        for f, u in self.futures:
                            if f.done():
                                done.append((f, u))

                        for x in done:
                            self.futures.remove(x)
                    except Exception as e:
                        self.logger.error(f"Error while adding future to future queue! e: {e}")
                        self.logger.error(traceback.format_exc())

                self.future_lock.notify_all()

            if len(done) > 0:
                self.logger.debug(f"Processing {len(done)} futures")
                for f, u in done:
                    try:
                        self.process(future=f, old_unit=u)
                    except Exception as e:
                        self.logger.error(f"Error while processing event {type(f)}, {e}")
                        self.logger.error(traceback.format_exc())

                done.clear()

            time.sleep(5)