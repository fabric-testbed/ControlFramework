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
import pickle
import queue
import threading
import traceback

from fabric.actor.core.apis.IPolicy import IPolicy
from fabric.actor.core.apis.ITimerTask import ITimerTask
from fabric.actor.core.apis.IActor import IActor
from fabric.actor.core.apis.IActorEvent import IActorEvent
from fabric.actor.core.apis.IActorProxy import IActorProxy
from fabric.actor.core.apis.IActorRunnable import IActorRunnable
from fabric.actor.core.apis.IQueryResponseHandler import IQueryResponseHandler
from fabric.actor.core.apis.IReservation import IReservation
from fabric.actor.core.apis.IReservationTracker import IReservationTracker
from fabric.actor.core.apis.ISlice import ISlice
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.container.MessageService import MessageService
from fabric.actor.core.core.ReservationTracker import ReservationTracker
from fabric.actor.core.kernel.FailedRPC import FailedRPC
from fabric.actor.core.kernel.KernelWrapper import KernelWrapper
from fabric.actor.core.kernel.RPCManagerSingleton import RPCManagerSingleton
from fabric.actor.core.kernel.ReservationFactory import ReservationFactory
from fabric.actor.core.kernel.ResourceSet import ResourceSet
from fabric.actor.core.kernel.SliceFactory import SliceFactory
from fabric.actor.core.proxies.Proxy import Proxy
from fabric.actor.core.time.ActorClock import ActorClock
from fabric.actor.core.time.Term import Term
from fabric.actor.core.util.AllActorEventsFilter import AllActorEventsFilter
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.IterableQueue import IterableQueue
from fabric.actor.core.util.ReflectionUtils import ReflectionUtils
from fabric.actor.core.util.ReservationSet import ReservationSet
from fabric.actor.security.AccessMonitor import AccessMonitor
from fabric.actor.security.AuthToken import AuthToken
from fabric.actor.security.Guard import Guard


class ExecutionStatus:
    def __init__(self):
        self.done = False
        self.exception = None
        self.result = None
        self.lock = threading.Condition()

    def mark_done(self):
        self.done = True


class ActorEvent(IActorEvent):
    def __init__(self, status: ExecutionStatus, runnable: IActorRunnable):
        self.status = status
        self.runnable = runnable

    def process(self):
        try:
            self.status.result = self.runnable.run()
        except Exception as e:
            self.status.exception = e
        finally:
            with self.status.lock:
                self.status.done = True
                self.status.lock.notify_all()


class Actor(IActor):
    """
    Actor is the base class for all actor implementations
    """
    PropertyAuthToken = "ActorAuthToken"
    PropertyDescription = "ActorDescription"
    PropertyMapper = "ActorMapper"
    PropertyMapperClass = "ActorMapperClass"
    PropertyPlugin = "ActorPlugin"
    PropertyPluginClass = "ActorPluginClass"
    DefaultDescription = "no description"

    actor_count = 0

    def __init__(self, auth: AuthToken = None, clock: ActorClock = None):
        # Globally unique identifier for this actor.
        self.guid = ID()
        # Actor name.
        self.name = None
        # Actor type code.
        self.type = None
        # Actor description.
        self.description = self.DefaultDescription
        # Identity object representing this actor.
        self.identity = auth
        # Actor policy object.
        self.policy = None
        # Actor plugin
        self.plugin = None
        # True if this actor has completed the recovery phase.
        self.recovered = False
        # The kernel wrapper.
        self.wrapper = None
        # logger
        self.logger = None
        # Factory for term.
        self.clock = clock
        # Access control monitor
        self.monitor = None
        # current cycle
        self.current_cycle = -1
        # True if the current tick is the first tick this actor has received.
        self.first_tick = True
        # Set to true when the actor is stopped.
        self.stopped = False
        # Initialization status.
        self.initialized = False
        # Contains a reference to the thread currently executing the timer handler.
        # This field is set at the entry to and clear at the exit.
        # The primary use of the field is to handle correctly stopping the actor.
        self.thread = None
        # A queue of timers that have fired and need to be processed.
        self.timer_queue = queue.Queue()
        self.event_queue = queue.Queue()
        self.reservation_tracker = None
        self.subscription_id = None
        # Reservations to close once recovery is complete.
        self.closing = ReservationSet()

        self.thread_lock = threading.Lock()
        self.actor_main_lock = threading.Condition()
        self.message_service = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['recovered']
        del state['wrapper']
        del state['logger']
        del state['clock']
        del state['monitor']
        del state['current_cycle']
        del state['first_tick']
        del state['stopped']
        del state['initialized']
        del state['thread_lock']
        del state['thread']
        del state['timer_queue']
        del state['event_queue']
        del state['reservation_tracker']
        del state['subscription_id']
        del state['actor_main_lock']
        del state['closing']
        del state['message_service']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.recovered = False
        self.wrapper = None
        self.logger = None
        self.clock = None
        self.monitor = None
        self.current_cycle = -1
        self.first_tick = True
        self.stopped = False
        self.initialized = False
        self.thread = None
        self.thread_lock = threading.Lock()
        self.timer_queue = queue.Queue()
        self.event_queue = queue.Queue()
        self.reservation_tracker = None
        self.subscription_id = None
        self.actor_main_lock = threading.Condition()
        self.closing = ReservationSet()
        self.message_service = None

    def actor_added(self):
        self.plugin.actor_added()
        self.reservation_tracker = ReservationTracker()
        filters = [AllActorEventsFilter(self.get_guid())]
        from fabric.actor.core.container.Globals import GlobalsSingleton
        self.subscription_id = GlobalsSingleton.get().event_manager.create_subscription(self.identity,
                                                                                        filters,
                                                                                        self.reservation_tracker)

    def actor_removed(self):
        if self.subscription_id is not None:
            try:
                from fabric.actor.core.container.Globals import GlobalsSingleton
                GlobalsSingleton.get().event_manager.delete_subscription(self.subscription_id, self.identity)
            except Exception as e:
                self.logger.error(e)

    def fail(self, rid: ID, message: str):
        self.wrapper.fail(rid, message)

    def close_by_rid(self, rid: ID):
        self.wrapper.close(rid)

    def close(self, reservation: IReservation):
        if reservation is not None:
            if not self.recovered:
                self.closing.add(reservation)
            else:
                self.wrapper.close(reservation.get_reservation_id())

    def close_slice_reservations(self, slice_id: ID):
        self.wrapper.close_slice_reservations(slice_id)

    def close_reservations(self, reservations: ReservationSet):
        for reservation in reservations.values():
            try:
                self.close(reservation=reservation)
            except Exception as e:
                self.logger.error("Could not close for #{} {}".format(reservation.get_reservation_id(), e))

    def error(self, err: str):
        """
        Logs and propagates a general error.

        @param err
                   log/exception message.
        @throws Exception
                    always
        """
        self.logger.error(err)
        raise Exception(err)

    def extend(self, rid: ID, resources: ResourceSet, term: Term):
        self.wrapper.extend_reservation(rid, resources, term)

    def external_tick(self, cycle: int):
        self.logger.debug("External Tick start cycle: {}".format(cycle))

        class TickEvent(IActorEvent):
            def __init__(self, base, cycle: int):
                self.base = base
                self.cycle = cycle

            def __str__(self):
                return "{} {}".format(self.base, self.cycle)

            def process(self):
                self.base.actor_tick(self.cycle)

        self.queue_event(TickEvent(self, cycle))
        self.logger.debug("External Tick end cycle: {}".format(cycle))

    def actor_tick(self, cycle: int):
        if not self.recovered:
            self.logger.warning("Tick for an actor that has not completed recovery")
            return
        current_cycle = 0
        if self.first_tick:
            current_cycle = cycle
        else:
            current_cycle = self.current_cycle + 1

        while current_cycle <= cycle:
            self.logger.debug("actor_tick: {} start".format(current_cycle))
            self.current_cycle = current_cycle
            self.policy.prepare(self.current_cycle)

            if self.first_tick:
                self.reset()

            self.tick_handler()
            self.policy.finish(self.current_cycle)

            self.wrapper.tick()

            self.first_tick = False
            self.logger.debug("actor_tick: {} end".format(current_cycle))
            current_cycle += 1

    def get_actor_clock(self) -> ActorClock:
        return self.clock

    def get_client_slices(self) -> list:
        return self.wrapper.get_client_slices()

    def get_current_cycle(self) -> int:
        return self.current_cycle

    def get_description(self) -> str:
        return self.get_description()

    def get_guid(self) -> ID:
        if self.identity is not None:
            return self.identity.get_guid()
        return None

    def get_identity(self) -> AuthToken:
        return self.identity

    def get_inventory_slices(self) -> list:
        return self.wrapper.get_inventory_slices()

    def get_logger(self):
        return self.logger

    def get_name(self) -> str:
        return self.name

    def get_policy(self) -> IPolicy:
        return self.policy

    def get_reservation(self, rid: ID) -> IReservation:
        return self.wrapper.get_reservation(rid)

    def get_reservations(self, slice_id: ID) -> list:
        return self.wrapper.get_reservations(slice_id)

    def get_plugin(self):
        return self.plugin

    def get_slice(self, slice_id: ID) -> ISlice:
        return self.wrapper.get_slice(slice_id)

    def get_slices(self):
        return self.wrapper.get_slices()

    def get_type(self):
        return self.type

    def initialize(self):
        from fabric.actor.core.container.Globals import GlobalsSingleton

        if not self.initialized:
            if self.identity is None:
                raise Exception("The actor is not properly created: no identity")

            if self.plugin is None:
                raise Exception("The actor is not properly created: no plugin")

            if self.policy is None:
                raise Exception("The actor is not properly created: no policy")

            if self.monitor is None:
                self.monitor = AccessMonitor()

            if self.name is None:
                self.name = self.identity.get_name()

            if self.name is None:
                raise Exception("The actor is not properly created: no name")

            if self.clock is None:
                self.clock = GlobalsSingleton.get().get_container().get_actor_clock()

            if self.clock is None:
                raise Exception("The actor is not properly created: no clock")

            if self.logger is None:
                self.logger = GlobalsSingleton.get().get_logger()

            self.plugin.set_actor(self)
            self.plugin.set_logger(self.logger)
            self.plugin.initialize()

            self.policy.set_actor(self)
            self.policy.initialize()

            self.wrapper = KernelWrapper(self, self.plugin, self.policy, self.monitor, Guard())

            self.current_cycle = -1

            self.setup_message_service()

            self.initialized = True

    def is_recovered(self) -> bool:
        return self.recovered

    def is_stopped(self) -> bool:
        return self.stopped

    def query(self, query: dict= None, caller: AuthToken= None, actor_proxy: IActorProxy = None,
              handler: IQueryResponseHandler = None) -> dict:
        if actor_proxy is None and handler is None:
            return self.wrapper.query(query, caller)
        else:
            callback = Proxy.get_callback(self, actor_proxy.get_type())
            RPCManagerSingleton.get().query(self, actor_proxy, callback, query, handler)
            return None

    def recover(self):
        self.logger.info("Starting recovery")
        self.recovery_starting()
        self.logger.debug("Recovering inventory slices")

        inventory_slices = self.plugin.get_database().get_inventory_slices()
        self.logger.debug("Found {} inventory slices".format(len(inventory_slices)))
        self.recover_slices(inventory_slices)
        self.logger.debug("Recovery of inventory slices complete")

        self.logger.debug("Recovering client slices")
        client_slices = self.plugin.get_database().get_client_slices()
        self.logger.debug("Found {} client slices".format(len(client_slices)))
        self.recover_slices(client_slices)
        self.logger.debug("Recovery of client slices complete")

        self.recovered = True

        self.recovery_ended()
        self.logger.info("Recovery complete")

    def recovery_starting(self):
        self.plugin.recovery_starting()
        self.policy.recovery_starting()

    def recovery_ended(self):
        self.plugin.recovery_ended()
        self.policy.recovery_ended()

    def recover_slices(self, properties: list):
        for p in properties:
            try:
                self.recover_slice(p)
            except Exception as e:
                self.logger.error("Error in recoverSlice for property list {}".format(e))
                # TODO
                raise e

    def recover_slice(self, properties: dict):
        slice_id = ID(properties['slc_guid'])

        if slice_id is None:
            raise Exception("Missing slice guid")
        slice_obj = self.get_slice(slice_id)
        self.logger.debug("Found slice_id: {} slice:{}".format(slice_id, slice_obj))

        if slice_obj is None:
            self.logger.info("Recovering slice: {}".format(slice_id))

            self.logger.debug("Instantiating slice object and recovering it")
            slice_obj = SliceFactory.create_instance(properties)

            self.logger.debug("Informing the plugin about the slice")
            self.plugin.revisit(slice_obj=slice_obj)

            self.logger.debug("Registering slice: {}".format(slice_id))
            self.re_register_slice(slice_obj)

            self.logger.debug("Recovering reservations in slice: {}".format(slice_id))
            self.recover_reservations(slice_obj)

            self.logger.info("Recovery of slice {} complete".format(slice_id))

    def recover_reservations(self, slice_obj: ISlice):
        self.logger.info(
            "Starting to recover reservations in slice {}({})".format(slice_obj.get_name(), slice_obj.get_slice_id()))
        reservations = None
        try:
            reservations = self.plugin.get_database().get_reservations_by_slice_id(slice_obj.get_slice_id())
        except Exception as e:
            self.logger.error(e)
            raise Exception(
                "Could not fetch reservation records for slice {}({}) from database".format(slice_obj.get_name(),
                                                                                            slice_obj.get_slice_id()))

        self.logger.debug("There are {} reservations(s) in slice".format(len(reservations)))

        for properties in reservations:
            try:
                self.logger.info("Reservation has properties: {}".format(properties))
                self.recover_reservation(properties, slice_obj)
            except Exception as e:
                self.logger.error("Unexpected error while recovering reservation {}".format(e))

        self.logger.info("Recovery for reservations in slice {} completed".format(slice_obj))

    def recover_reservation(self, properties: dict, slice_obj: ISlice):
        try:
            r = ReservationFactory.create_instance(properties, self, slice_obj, self.logger)

            self.logger.info(
                "Found reservation # {} in state {}".format(r.get_reservation_id(), r.get_reservation_state()))
            if r.is_closed():
                self.logger.info("Reservation #{} is closed. Nothing to recover.".format(r.get_reservation_id()))
                return

            self.logger.info("Recovering reservation #{}".format(r.get_reservation_id()))
            self.logger.debug("Recovering reservation object r={}".format(r))

            self.logger.debug("Registering the reservation with the actor")
            self.re_register(r)

            self.logger.info(r)

            self.logger.debug("Revisiting with the Plugin")

            self.plugin.revisit(reservation=r)

            self.logger.info(r)

            self.logger.debug("Revisiting with the actor policy")
            self.policy.revisit(r)

            self.logger.info("Recovered reservation #{}".format(r.get_reservation_id()))
        except Exception as e:
            self.logger.error("Exception occurred in recovering reservation e={}".format(e))
            raise Exception("Could not recover Reservation #{}".format(properties))

    def register(self, reservation: IReservation):
        self.wrapper.register_reservation(reservation)

    def register_slice(self, slice_object: ISlice):
        self.wrapper.register_slice(slice_object)

    def remove_reservation(self, reservation: IReservation = None, rid: ID = None):
        if reservation is not None:
            self.wrapper.remove_reservation(reservation.get_reservation_id())

        if rid is not None:
            self.wrapper.remove_reservation(rid)

    def remove_slice(self, slice_obj: ISlice):
        self.wrapper.remove_slice(slice_obj.get_slice_id())

    def remove_slice_by_slice_id(self, slice_id: ID):
        self.wrapper.remove_slice(slice_id)

    def re_register(self, reservation: IReservation):
        self.wrapper.re_register_reservation(reservation)

    def re_register_slice(self, slice_obj: ISlice):
        self.wrapper.re_register_slice(slice_obj)

    def issue_delayed(self):
        self.close_reservations(self.closing)
        self.closing.clear()

    def reset(self):
        self.issue_delayed()
        self.policy.reset()

    def set_actor_clock(self, clock):
        self.clock = clock

    def set_description(self, description: str):
        self.description = description

    def set_identity(self, token: AuthToken):
        self.identity = token
        self.name = self.identity.get_name()
        self.guid = token.get_guid()

    def set_policy(self, policy):
        self.policy = policy

    def set_recovered(self, value: bool):
        self.recovered = value

    def set_plugin(self, plugin):
        self.plugin = plugin

    def set_stopped(self, value: bool):
        self.stopped = value

    def is_on_actor_thread(self) -> bool:
        result = False
        try:
            self.thread_lock.acquire()
            result = self.thread == threading.current_thread()
        finally:
            self.thread_lock.release()
        return result

    def execute_on_actor_thread_and_wait(self, runnable: IActorRunnable):
        if self.is_on_actor_thread():
            return runnable.run()
        else:
            status = ExecutionStatus()
            event = ActorEvent(status, runnable)

            self.queue_event(event)

            with status.lock:
                while not status.done:
                    status.lock.wait()

            if status is not None and status.exception is not None:
                raise status.exception

            return status.result

    def run(self):
        try:
            self.actor_count -= 1
            self.actor_main()
        except Exception as e:
            self.logger.error("Unexpected error {}".format(e))

    def start(self):
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise Exception("This actor has already been started")

            self.thread = threading.Thread(target=self.run)
            self.thread.setName(self.get_name())
            self.thread.setDaemon(True)
            self.thread.start()
        finally:
            self.thread_lock.release()

        self.message_service.start()

    def stop(self):
        self.stopped = True
        self.message_service.stop()
        try:
            self.thread_lock.acquire()
            temp = self.thread
            self.thread = None
            if temp is not None:
                self.logger.warning("It seems that the actor thread is running. Interrupting it")
                try:
                    # TODO find equivalent of interrupt
                    with self.actor_main_lock:
                        self.actor_main_lock.notify_all()
                    temp.join()
                except Exception as e:
                    self.logger.error("Could not join actor thread {}".format(e))
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

    def tick_handler(self):
        return

    def handle_failed_rpc(self, rid: ID, rpc: FailedRPC):
        self.wrapper.process_failed_rpc(rid, rpc)

    def __str__(self):
        return "actor"

    def unregister(self, reservation: IReservation, rid: ID):
        if reservation is not None:
            self.wrapper.unregister_reservation(reservation.get_reservation_id())

        if rid is not None:
            self.wrapper.unregister_reservation(rid)

    def unregister_slice(self, slice_obj: ISlice):
        self.wrapper.unregister_slice(slice_obj.get_slice_id())

    def unregister_slice_by_slice_id(self, slice_id: ID):
        self.wrapper.unregister_slice(slice_id)

    def queue_timer(self, timer: ITimerTask):
        with self.actor_main_lock:
            self.timer_queue.put_nowait(timer)
            self.logger.debug("Added timer to timer queue")
            self.actor_main_lock.notify_all()

    def queue_event(self, incoming: IActorEvent):
        with self.actor_main_lock:
            self.event_queue.put_nowait(incoming)
            self.logger.debug("Added event to event queue {}".format(incoming.__class__.__name__))
            self.actor_main_lock.notify_all()

    def await_no_pending_reservations(self):
        self.wrapper.await_nothing_pending()

    def get_reservation_tracker(self) -> IReservationTracker:
        return self.reservation_tracker

    def get_recovery_root(self):
        return self

    def get_reference(self) -> ID:
        return self.guid

    def actor_main(self):
        while True:
            events = []
            timers = []

            with self.actor_main_lock:

                while self.event_queue.empty() and self.timer_queue.empty() and not self.stopped:
                    try:
                        self.actor_main_lock.wait()
                    except InterruptedError as e:
                        self.logger.info("Actor thread interrupted. Exiting")
                        return

                if self.stopped:
                    self.logger.info("Actor exiting")
                    return

                if not self.event_queue.empty():
                    for event in IterableQueue(self.event_queue):
                        events.append(event)

                if not self.timer_queue.empty():
                    for timer in IterableQueue(self.timer_queue):
                        timers.append(timers)

                self.actor_main_lock.notify_all()

            if len(events) > 0:
                self.logger.debug("Processing {} events".format(len(events)))
                for e in events:
                    try:
                        e.process()
                    except Exception as e:
                        traceback.print_exc()
                        self.logger.error("Error while processing event {} {}".format(type(e), e))

            if len(timers) > 0:
                for t in timers:
                    try:
                        t.execute()
                    except Exception as e:
                        self.logger.error("Error while processing a timer {}".format(e))

    def setup_message_service(self):
        try:
            module_name = self.get_kafka_service_module()
            class_name = self.get_kafka_service_class()
            kafka_service = ReflectionUtils.create_instance_with_params(module_name, class_name)(self)

            from fabric.actor.core.container.Globals import GlobalsSingleton
            config = GlobalsSingleton.get().get_config()

            bootstrap_server = config.get_global_config().get_runtime()[Constants.PropertyConfKafkaServer]
            schema_registry = config.get_global_config().get_runtime()[Constants.PropertyConfKafkaSchemaRegistry]
            key_schema_file = config.get_global_config().get_runtime()[Constants.PropertyConfKafkaKeySchema]
            value_schema_file = config.get_global_config().get_runtime()[Constants.PropertyConfKafkaValueSchema]

            conf = {'bootstrap.servers': bootstrap_server,
                    'schema.registry.url': schema_registry,
                    'group.id': "fabric_cf",
                    'auto.offset.reset': "earliest"}

            topic = config.get_actor().get_kafka_topic()
            topics = [topic]

            from confluent_kafka import avro

            file = open(key_schema_file, "r")
            key_bytes = file.read()
            file.close()
            key_schema = avro.loads(key_bytes)
            file = open(value_schema_file, "r")
            val_bytes = file.read()
            file.close()
            val_schema = avro.loads(val_bytes)

            module_name = self.get_mgmt_kafka_service_module()
            class_name = self.get_mgmt_kafka_service_class()
            producer_conf = {'bootstrap.servers': bootstrap_server, 'schema.registry.url': schema_registry}
            kafka_mgmt_service = ReflectionUtils.create_instance_with_params(module_name, class_name)(producer_conf, key_schema, val_schema)
            kafka_mgmt_service.set_logger(self.logger)

            self.message_service = MessageService(kafka_service, kafka_mgmt_service, conf, key_schema, val_schema, topics, logger=self.logger)
        except Exception as e:
            traceback.print_exc()
            self.logger.error("Failed to setup message service e={}".format(e))
            raise e

    @staticmethod
    def create_instance(properties: dict) -> IActor:
        if Constants.PropertyPickleProperties not in properties:
            raise Exception("Invalid arguments")
        deserialized_actor = None
        try:
            serialized_actor = properties[Constants.PropertyPickleProperties]
            deserialized_actor = pickle.loads(serialized_actor)
        except Exception as e:
            raise e
        return deserialized_actor

    @staticmethod
    def get_actor_type_from_string(type_str: str) -> int:
        if type_str.lower() == Constants.AUTHORITY or type_str.lower() == Constants.SITE:
            return Constants.ActorTypeSiteAuthority

        elif type_str.lower() == Constants.CONTROLLER:
            return Constants.ActorTypeController

        elif type_str.lower() == Constants.BROKER:
            return Constants.ActorTypeBroker

        return Constants.ActorTypeAll

