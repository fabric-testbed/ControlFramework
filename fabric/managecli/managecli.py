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
import logging
import threading
import traceback
import click

from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.manage.kafka.KafkaActor import KafkaActor
from fabric.actor.core.manage.kafka.KafkaBroker import KafkaBroker
from fabric.actor.core.manage.kafka.KafkaMgmtMessageProcessor import KafkaMgmtMessageProcessor
from fabric.actor.core.util.ID import ID
from fabric.managecli.ConfigProcessor import ConfigProcessor
from fabric.managecli.ManageCommand import ManageCommand


class MainShell:
    PATH = "/Users/komalthareja/renci/code/fabric/ActorBase/fabric/managecli/config/manage-cli.yaml"

    def __init__(self):
        self.config_processor = ConfigProcessor(self.PATH)
        self.message_processor = None
        self.actor_cache = {}
        self.lock = threading.Lock()
        self.auth = None
        self.logger = None
        self.conf = None
        self.key_schema = None
        self.val_schema = None

    def set_up_kafka_conf_and_schema(self):
        try:
            self.conf = {'bootstrap.servers': self.config_processor.get_kafka_server(),
                         'schema.registry.url': self.config_processor.get_kafka_schema_registry()}

            from confluent_kafka import avro

            file = open(self.config_processor.get_kafka_key_schema(), "r")
            key_bytes = file.read()
            file.close()
            self.key_schema = avro.loads(key_bytes)
            file = open(self.config_processor.get_kafka_value_schema(), "r")
            val_bytes = file.read()
            file.close()
            self.val_schema = avro.loads(val_bytes)
        except Exception as e:
            err_str = traceback.format_exc()
            self.logger.error(err_str)
            self.logger.error("Exception occurred while loading schemas {}".format(e))
            raise e

    def initialize(self):
        self.config_processor.process()

        self.logger = self.make_logger()

        self.set_up_kafka_conf_and_schema()

        self.setup_kafka_processor()

        self.load_actor_cache()

    def setup_kafka_processor(self):
        consumer_conf = self.conf
        consumer_conf['group.id'] = "fabric_cf"
        consumer_conf['auto.offset.reset'] = "earliest"
        topics = [self.config_processor.get_kafka_topic()]

        self.message_processor = KafkaMgmtMessageProcessor(consumer_conf, self.key_schema, self.val_schema, topics,
                                                           logger=self.logger)

    def load_actor_cache(self):
        peers = self.config_processor.get_peers()
        if peers is not None:
            for p in peers:
                # TODO Actor Live Check
                mgmt_actor = None
                if p.get_type() == Constants.BROKER:
                    mgmt_actor = KafkaBroker(ID(p.get_guid()), p.get_kafka_topic(), self.config_processor.get_auth(),
                                             self.config_processor.get_kafka_config(), self.logger,
                                             self.message_processor)
                else:
                    mgmt_actor = KafkaActor(ID(p.get_guid()), p.get_kafka_topic(), self.config_processor.get_auth(),
                                            self.config_processor.get_kafka_config(), self.logger,
                                            self.message_processor)
                try:
                    self.lock.acquire()
                    self.logger.debug("Added actor {} to cache".format(p.get_name()))
                    self.actor_cache[p.get_name()] = mgmt_actor
                finally:
                    self.lock.release()
        else:
            self.logger.debug("No peers available")

    def get_mgmt_actor(self, name: str) -> KafkaActor:

        try:
            self.lock.acquire()
            return self.actor_cache.get(name, None)
        finally:
            self.lock.release()

    def make_logger(self):
        """
        Detects the path and level for the log file from the actor config and sets
        up a logger. Instead of detecting the path and/or level from the
        config, a custom path and/or level for the log file can be passed as
        optional arguments.

        :param log_path: Path to custom log file
        :param log_level: Custom log level
        :return: logging.Logger object
        """

        # Get the log path
        if self.config_processor is None:
            raise RuntimeError('No config information available')

        log_path = self.config_processor.get_log_dir() + '/' + self.config_processor.get_log_file()

        if log_path is None:
            raise RuntimeError('The log file path must be specified in config or passed as an argument')

        # Get the log level
        log_level = self.config_processor.get_log_level()
        if log_level is None:
            log_level = logging.INFO

        # Set up the root logger
        log = logging.getLogger(self.config_processor.get_log_name())
        log.setLevel(log_level)
        log_format = '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'

        logging.basicConfig(format=log_format, filename=log_path)

        return log

    def get_callback_topic(self) -> str:
        return self.config_processor.get_kafka_topic()

    def start(self):
        try:
            self.initialize()
            self.message_processor.start()
        except Exception as e:
            err_str = traceback.format_exc()
            self.logger.error(err_str)
            self.logger.debug("Failed to start Management Shell: {}".format(str(e)))

    def stop(self):
        try:
            self.message_processor.stop()
        except Exception as e:
            err_str = traceback.format_exc()
            self.logger.error(err_str)
            self.logger.debug("Failed to stop Management Shell: {}".format(str(e)))


class MainShellSingleton:
    __instance = None

    def __init__(self):
        if self.__instance is not None:
            raise Exception("Singleton can't be created twice !")

    def get(self):
        """
        Actually create an instance
        """
        if self.__instance is None:
            self.__instance = MainShell()
        return self.__instance

    get = classmethod(get)


@click.group()
@click.option('-v', '--verbose', is_flag=True)
@click.pass_context
def managecli(ctx, verbose):
    ctx.ensure_object(dict)
    ctx.obj['VERBOSE'] = verbose


@click.group()
@click.pass_context
def manage(ctx):
    """ issue management commands
    """
    return


@manage.command()
@click.option('--broker', default=None, help='Broker Name', required=True)
@click.option('--am', default=None, help='AM Name', required=True)
@click.pass_context
def claim(ctx, broker, am):
    """ Claim reservations for am to broker
    """
    MainShellSingleton.get().start()
    mgmt_command = ManageCommand(MainShellSingleton.get().logger)
    mgmt_command.claim_resources(broker, am, MainShellSingleton.get().get_callback_topic())
    MainShellSingleton.get().stop()


@click.group()
@click.pass_context
def show(ctx):
    """ issue management commands
    """
    return


@show.command()
@click.option('--actor', default=None, help='Actor Name', required=True)
@click.pass_context
def slices(ctx, actor):
    """ Get Slices from an actor
    """
    MainShellSingleton.get().start()
    mgmt_command = ManageCommand(MainShellSingleton.get().logger)
    mgmt_command.get_slices(actor, MainShellSingleton.get().get_callback_topic())
    MainShellSingleton.get().stop()


@show.command()
@click.option('--actor', default=None, help='Actor Name', required=True)
@click.option('--sliceid', default=None, help='Slice ID', required=True)
@click.pass_context
def slice(ctx, actor, sliceid):
    """ Get Slices from an actor
    """
    MainShellSingleton.get().start()
    mgmt_command = ManageCommand(MainShellSingleton.get().logger)
    mgmt_command.get_slice(actor, sliceid, MainShellSingleton.get().get_callback_topic())
    MainShellSingleton.get().stop()


@show.command()
@click.option('--actor', default=None, help='Actor Name', required=True)
@click.pass_context
def reservations(ctx, actor):
    """ Get Slices from an actor
    """
    MainShellSingleton.get().start()
    mgmt_command = ManageCommand(MainShellSingleton.get().logger)
    mgmt_command.get_reservations(actor, MainShellSingleton.get().get_callback_topic())
    MainShellSingleton.get().stop()

@show.command()
@click.option('--actor', default=None, help='Actor Name', required=True)
@click.option('--rid', default=None, help='Reservation Id', required=True)
@click.pass_context
def reservation(ctx, actor, rid):
    """ Get Slices from an actor
    """
    MainShellSingleton.get().start()
    mgmt_command = ManageCommand(MainShellSingleton.get().logger)
    mgmt_command.get_reservation(actor, rid, MainShellSingleton.get().get_callback_topic())
    MainShellSingleton.get().stop()


managecli.add_command(manage)
managecli.add_command(show)