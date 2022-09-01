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
import logging
import multiprocessing
import os
import threading
import time
import traceback

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.plugins.handlers.handler_processor import HandlerProcessor
from fabric_cf.actor.core.util.log_helper import LogHelper
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils

process_pool_logger = None


class AnsibleHandlerProcessor(HandlerProcessor):
    """
    Ansible Handler Processor
    """
    MAX_WORKERS = 10

    def __init__(self):
        super().__init__()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        self.log_config = GlobalsSingleton.get().get_log_config()
        self.executor = None
        self.process_pool_manager = None
        self.process_pool_lock = None
        self.__setup_process_pool()
        self.futures = []
        self.thread = None
        self.future_lock = threading.Condition()
        self.stopped = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['plugin']
        del state['initialized']
        del state['lock']
        del state['executor']
        del state['process_pool_manager']
        del state['process_pool_lock']
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
        self.__setup_process_pool()
        self.futures = []
        self.thread = None
        self.future_lock = threading.Condition()
        self.stopped = False

    def __setup_process_pool(self):
        log_dir = self.log_config.get(Constants.PROPERTY_CONF_LOG_DIRECTORY, ".")
        log_file = self.log_config.get(Constants.PROPERTY_CONF_HANDLER_LOG_FILE, "handler.log")
        log_level = self.log_config.get(Constants.PROPERTY_CONF_LOG_LEVEL, logging.DEBUG)
        log_retain = int(self.log_config.get(Constants.PROPERTY_CONF_LOG_RETAIN, 50))
        log_size = int(self.log_config.get(Constants.PROPERTY_CONF_LOG_SIZE, 5000000))
        logger = self.log_config.get(Constants.PROPERTY_CONF_LOGGER, "handler")
        logger = f"{logger}-handler"
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.MAX_WORKERS,
                                                               initializer=AnsibleHandlerProcessor.process_pool_initializer,
                                                               initargs=(log_dir, log_file, log_level, log_retain,
                                                                         log_size, logger))
        self.process_pool_manager = multiprocessing.Manager()
        self.process_pool_lock = self.process_pool_manager.Lock()

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
            if self.lock.locked():
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
            # clean restart
            if unit is None:
                if len(self.config_mappings) == 0:
                    return
                handler = list(self.config_mappings.values())[0]
            else:
                handler = self.config_mappings.get(str(unit.get_resource_type()), None)
            if handler is None:
                raise AuthorityException(f"No handler found for resource type {unit.get_resource_type()}")

            future = self.executor.submit(self.process_pool_main, operation, handler.get_class_name(),
                                          handler.get_module_name(), handler.get_properties(), unit,
                                          self.process_pool_lock)

            self.queue_future(future=future, unit=unit)
            if unit:
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

    def clean_restart(self):
        self.invoke_handler(unit=None, operation=Constants.TARGET_CLEAN_RESTART)

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
        if old_unit is None:
            return
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

    @staticmethod
    def process_pool_main(operation: str, handler_class: str, handler_module: str, properties: dict,
                          unit: ConfigToken, process_lock: multiprocessing.Lock):
        global process_pool_logger
        handler_class = ReflectionUtils.create_instance_with_params(module_name=handler_module,
                                                                    class_name=handler_class)
        handler_obj = handler_class(process_pool_logger, properties, process_lock)

        if operation == Constants.TARGET_CREATE:
            return handler_obj.create(unit)
        elif operation == Constants.TARGET_DELETE:
            return handler_obj.delete(unit)
        elif operation == Constants.TARGET_MODIFY:
            return handler_obj.modify(unit)
        elif operation == Constants.TARGET_CLEAN_RESTART:
            return handler_obj.clean_restart()
        else:
            process_pool_logger.error("Invalid operation")
            result = {Constants.PROPERTY_TARGET_NAME: operation,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            return result, unit

    @staticmethod
    def process_pool_initializer(log_dir: str, log_file: str, log_level, log_retain: int, log_size: int,
                                 logger: str):
        global process_pool_logger
        if process_pool_logger is None:
            log_file = f"{os.getpid()}-{log_file}"
            process_pool_logger = LogHelper.make_logger(log_dir=log_dir, log_file=log_file, log_level=log_level,
                                                        log_retain=log_retain, log_size=log_size, logger=logger)
