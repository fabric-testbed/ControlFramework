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
import queue
import threading
import traceback
from typing import List

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation, DelegationState
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.apis.abc_timer_task import ABCTimerTask
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
from fabric_cf.actor.core.apis.abc_actor_proxy import ABCActorProxy
from fabric_cf.actor.core.apis.abc_actor_runnable import ABCActorRunnable
from fabric_cf.actor.core.apis.abc_query_response_handler import ABCQueryResponseHandler
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.exceptions import ActorException
from fabric_cf.actor.core.container.message_service import MessageService
from fabric_cf.actor.core.delegation.delegation_factory import DelegationFactory
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.kernel.kernel_wrapper import KernelWrapper
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.proxies.proxy import Proxy
from fabric_cf.actor.core.time.actor_clock import ActorClock
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.iterable_queue import IterableQueue
from fabric_cf.actor.core.util.reflection_utils import ReflectionUtils
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.security.auth_token import AuthToken


class ExecutionStatus:
    """
    Execution status of an action on Actor Thread
    """
    def __init__(self):
        self.done = False
        self.exception = None
        self.result = None
        self.lock = threading.Condition()

    def mark_done(self):
        """
        Mark as done
        """
        self.done = True


class ActorEvent(ABCActorEvent):
    """
    Actor Event
    """
    def __init__(self, *, status: ExecutionStatus, runnable: ABCActorRunnable):
        self.status = status
        self.runnable = runnable

    def process(self):
        """
        Process an event
        """
        try:
            self.status.result = self.runnable.run()
        except Exception as e:
            traceback.print_exc()
            self.status.exception = e
        finally:
            with self.status.lock:
                self.status.done = True
                self.status.lock.notify_all()


class ActorMixin(ABCActorMixin):
    """
    Actor is the base class for all actor implementations
    """
    DefaultDescription = "no description"

    actor_count = 0

    def __init__(self, *, auth: AuthToken = None, clock: ActorClock = None):
        # Globally unique identifier for this actor.
        self.guid = ID()
        # Actor name.
        self.name = None
        # Actor type code.
        self.type = ActorType.All
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
        del state['current_cycle']
        del state['first_tick']
        del state['stopped']
        del state['initialized']
        del state['thread_lock']
        del state['thread']
        del state['timer_queue']
        del state['event_queue']
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
        self.current_cycle = -1
        self.first_tick = True
        self.stopped = False
        self.initialized = False
        self.thread = None
        self.thread_lock = threading.Lock()
        self.timer_queue = queue.Queue()
        self.event_queue = queue.Queue()
        self.actor_main_lock = threading.Condition()
        self.closing = ReservationSet()
        self.message_service = None
        self.policy.set_actor(actor=self)

    def actor_added(self):
        self.plugin.actor_added()

    def actor_removed(self):
        return

    def fail(self, *, rid: ID, message: str):
        self.wrapper.fail(rid=rid, message=message)

    def fail_delegation(self, *, did: str, message: str):
        self.wrapper.fail_delegation(did=did, message=message)

    def close_by_rid(self, *, rid: ID):
        self.wrapper.close(rid=rid)

    def close(self, *, reservation: ABCReservationMixin):
        if reservation is not None:
            if not self.recovered:
                self.logger.debug("Adding reservation: {} to closing list".format(reservation.get_reservation_id()))
                self.closing.add(reservation=reservation)
            else:
                self.logger.debug("Closing reservation: {}".format(reservation.get_reservation_id()))
                self.wrapper.close(rid=reservation.get_reservation_id())

    def close_slice_reservations(self, *, slice_id: ID):
        self.wrapper.close_slice_reservations(slice_id=slice_id)

    def close_reservations(self, *, reservations: ReservationSet):
        for reservation in reservations.values():
            try:
                self.logger.debug("Closing reservation: {}".format(reservation.get_reservation_id()))
                self.close(reservation=reservation)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("Could not close for #{} {}".format(reservation.get_reservation_id(), e))

    def error(self, *, err: str):
        """
        Logs and propagates a general error.

        @param err
                   log/exception message.
        @throws Exception
                    always
        """
        self.logger.error(err)
        raise ActorException(err)

    def extend(self, *, rid: ID, resources: ResourceSet, term: Term):
        self.wrapper.extend_reservation(rid=rid, resources=resources, term=term)

    def external_tick(self, *, cycle: int):
        self.logger.info("External Tick start cycle: {}".format(cycle))

        class TickEvent(ABCActorEvent):
            def __init__(self, *, base, cycle: int):
                self.base = base
                self.cycle = cycle

            def __str__(self):
                return "{} {}".format(self.base, self.cycle)

            def process(self):
                self.base.actor_tick(cycle=self.cycle)

        self.queue_event(incoming=TickEvent(base=self, cycle=cycle))
        self.logger.info("External Tick end cycle: {}".format(cycle))

    def actor_tick(self, *, cycle: int):
        """
        Actor Tick
        :param cycle: cycle
        :return:
        """
        try:
            if not self.recovered:
                self.logger.warning("Tick for an actor that has not completed recovery")
                return
            current_cycle = 0
            if self.first_tick:
                current_cycle = cycle
            else:
                current_cycle = self.current_cycle + 1

            while current_cycle <= cycle:
                self.logger.info("actor_tick: {} start".format(current_cycle))
                self.current_cycle = current_cycle
                self.policy.prepare(cycle=self.current_cycle)

                if self.first_tick:
                    self.reset()

                self.tick_handler()
                self.policy.finish(cycle=self.current_cycle)

                self.wrapper.tick()

                self.first_tick = False
                self.logger.info("actor_tick: {} end".format(current_cycle))
                current_cycle += 1
        except Exception as e:
            self.logger.debug(traceback.format_exc())
            raise e

    def get_actor_clock(self) -> ActorClock:
        return self.clock

    def get_client_slices(self) -> List[ABCSlice]:
        return self.wrapper.get_client_slices()

    def get_current_cycle(self) -> int:
        return self.current_cycle

    def get_description(self) -> str:
        return self.description

    def get_guid(self) -> ID:
        if self.identity is not None:
            return self.identity.get_guid()
        return None

    def get_identity(self) -> AuthToken:
        return self.identity

    def get_inventory_slices(self) -> List[ABCSlice]:
        """
        Get inventory slices
        @return inventory slices
        """
        return self.wrapper.get_inventory_slices()

    def get_logger(self):
        return self.logger

    def get_name(self) -> str:
        return self.name

    def get_policy(self) -> ABCPolicy:
        return self.policy

    def get_delegation(self, *, did: str) -> ABCDelegation:
        return self.wrapper.get_delegation(did=did)

    def get_reservation(self, *, rid: ID) -> ABCReservationMixin:
        return self.wrapper.get_reservation(rid=rid)

    def get_reservations(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        return self.wrapper.get_reservations(slice_id=slice_id)

    def get_plugin(self):
        return self.plugin

    def get_slice(self, *, slice_id: ID) -> ABCSlice:
        return self.wrapper.get_slice(slice_id=slice_id)

    def get_slices(self):
        return self.wrapper.get_slices()

    def get_type(self) -> ActorType:
        return self.type

    def initialize(self):
        from fabric_cf.actor.core.container.globals import GlobalsSingleton

        if not self.initialized:
            if self.identity is None or self.plugin is None or self.policy is None:
                raise ActorException(f"The actor is not properly created: identity: {self.identity} "
                                     f"plugin: {self.plugin} policy: {self.policy}")

            if self.name is None:
                self.name = self.identity.get_name()

            if self.name is None:
                raise ActorException("The actor is not properly created: no name")

            if self.clock is None:
                self.clock = GlobalsSingleton.get().get_container().get_actor_clock()

            if self.clock is None:
                raise ActorException("The actor is not properly created: no clock")

            if self.logger is None:
                self.logger = GlobalsSingleton.get().get_logger()

            self.plugin.set_actor(actor=self)
            self.plugin.set_logger(logger=self.logger)
            self.plugin.initialize()

            self.policy.set_actor(actor=self)
            self.policy.initialize()
            self.policy.set_logger(logger=self.logger)

            self.wrapper = KernelWrapper(actor=self, plugin=self.plugin, policy=self.policy)

            self.current_cycle = -1

            self.setup_message_service()

            self.initialized = True

    def is_recovered(self) -> bool:
        return self.recovered

    def is_stopped(self) -> bool:
        return self.stopped

    def query(self, *, query: dict = None, caller: AuthToken = None, actor_proxy: ABCActorProxy = None,
              handler: ABCQueryResponseHandler = None) -> dict:
        """
        Query an actor
        @param query query
        @param caller caller
        @param actor_proxy actor proxy
        @param handler response handler
        """
        if actor_proxy is None and handler is None:
            return self.wrapper.query(properties=query, caller=caller)
        else:
            callback = Proxy.get_callback(actor=self, protocol=actor_proxy.get_type())
            RPCManagerSingleton.get().query(actor=self, remote_actor=actor_proxy, callback=callback, query=query,
                                            handler=handler)
            return None

    def recover(self):
        """
        Recover
        """
        self.logger.info("Starting recovery")
        self.recovery_starting()
        self.logger.debug("Recovering inventory slices")

        inventory_slices = self.plugin.get_database().get_slices(slc_type=[SliceTypes.InventorySlice])
        self.logger.debug("Found {} inventory slices".format(len(inventory_slices)))
        self.recover_slices(slices=inventory_slices)
        self.logger.debug("Recovery of inventory slices complete")

        self.logger.debug("Recovering client slices")
        client_slices = self.plugin.get_database().get_slices(slc_type=[SliceTypes.ClientSlice,
                                                                        SliceTypes.BrokerClientSlice])
        self.logger.debug("Found {} client slices".format(len(client_slices)))
        self.recover_slices(slices=client_slices)
        self.logger.debug("Recovery of client slices complete")

        self.recovered = True

        self.recovery_ended()
        self.logger.info("Recovery complete")

    def recovery_starting(self):
        """
        Recovery starting
        """
        self.plugin.recovery_starting()
        self.policy.recovery_starting()

    def recovery_ended(self):
        """
        Recovery ended
        """
        self.plugin.recovery_ended()
        self.policy.recovery_ended()
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        if GlobalsSingleton.get().can_reload_model():
            GlobalsSingleton.get().delete_reload_model_state_file()

    def recover_slices(self, *, slices: List[ABCSlice]):
        """
        Recover slices
        @param slices slices
        """
        for s in slices:
            try:
                self.recover_slice(slice_obj=s)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error("Error in recoverSlice for property list {}".format(e))
                if s.is_inventory():
                    raise e

    def recover_broker_slice(self, *, slice_obj: ABCSlice):
        """
        Recover broker slice at the AM, do the following if the model.reload file is detected
        - Close the existing delegations
        - Create the new delegations from the reloaded ARM
        - Add the delegations to the Broker Slice

        @param slice_obj Slice object
        """
        if self.get_type() != ActorType.Authority:
            return False

        if not slice_obj.is_broker_client():
            return False

        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        if not GlobalsSingleton.get().can_reload_model():
            return False

        self.logger.info(f"Closing old delegations and adding new delegations to the slice: {slice_obj}!")
        delegation_names = []

        try:
            delegations = self.plugin.get_database().get_delegations(slice_id=str(slice_obj.get_slice_id()))
        except Exception as e:
            self.logger.error(e)
            raise ActorException(f"Could not fetch delegations records for slice {slice_obj} from database")

        for d in delegations:
            self.logger.info(f"Closing delegation: {d}!")
            d.set_graph(graph=None)
            d.transition(prefix="closed as part of recovers", state=DelegationState.Closed)
            delegation_names.append(d.get_delegation_name())
            self.plugin.get_database().update_delegation(delegation=d)

        adms = self.policy.aggregate_resource_model.generate_adms()

        # Create new delegations and add to the broker slice; they will be re-registered with the policy in the recovery
        for name in delegation_names:
            new_delegation_graph = adms.get(name)
            dlg_obj = DelegationFactory.create(did=new_delegation_graph.get_graph_id(),
                                               slice_id=slice_obj.get_slice_id(),
                                               delegation_name=name)
            dlg_obj.set_slice_object(slice_object=slice_obj)
            dlg_obj.set_graph(graph=new_delegation_graph)
            dlg_obj.transition(prefix="Reload Model", state=DelegationState.Delegated)
            self.plugin.get_database().add_delegation(delegation=dlg_obj)

    def recover_inventory_slice(self, *, slice_obj: ABCSlice) -> bool:
        """
        Check and Reload ARM for an inventory slice for an AM
        @param slice_obj slice object
        @return True if ARM was reloaded; otherwise False
        """
        if self.get_type() != ActorType.Authority:
            return False

        if not slice_obj.is_inventory():
            return False

        # Check and Reload ARM if needed
        from fabric_cf.actor.core.container.globals import GlobalsSingleton
        arm_graph = GlobalsSingleton.get().check_and_reload_model(graph_id=slice_obj.get_graph_id())
        if arm_graph is not None:
            slice_obj.set_graph(graph=arm_graph)

        return arm_graph is not None

    def recover_slice(self, *, slice_obj: ABCSlice):
        """
        Recover slice
        @param slice_obj slice_obj
        """
        slice_id = slice_obj.get_slice_id()

        if self.get_slice(slice_id=slice_id) is not None:
            self.logger.debug("Found slice_id: {} slice:{}".format(slice_id, slice_obj))
        else:
            self.logger.info("Recovering slice: {}".format(slice_id))
            self.recover_inventory_slice(slice_obj=slice_obj)

            self.recover_broker_slice(slice_obj=slice_obj)

            self.logger.debug("Informing the plugin about the slice")
            self.plugin.revisit(slice_obj=slice_obj)

            self.logger.debug("Registering slice: {}".format(slice_id))
            self.re_register_slice(slice_object=slice_obj)

            self.logger.debug("Recovering reservations in slice: {}".format(slice_id))
            self.recover_reservations(slice_obj=slice_obj)

            self.logger.debug("Recovering delegations in slice: {}".format(slice_id))
            self.recover_delegations(slice_obj=slice_obj)

            self.logger.info("Recovery of slice {} complete".format(slice_id))

    def recover_reservations(self, *, slice_obj: ABCSlice):
        """
        Recover reservations
        @param slice_obj slice object
        """
        self.logger.info(
            "Starting to recover reservations in slice {}({})".format(slice_obj.get_name(), slice_obj.get_slice_id()))
        reservations = None
        try:
            reservations = self.plugin.get_database().get_reservations(slice_id=slice_obj.get_slice_id())
        except Exception as e:
            self.logger.error(e)
            raise ActorException(
                "Could not fetch reservation records for slice {}({}) from database".format(slice_obj.get_name(),
                                                                                            slice_obj.get_slice_id()))

        self.logger.debug("There are {} reservations(s) in slice".format(len(reservations)))

        for r in reservations:
            try:
                self.recover_reservation(r=r, slice_obj=slice_obj)
            except Exception as e:
                self.logger.error("Unexpected error while recovering reservation {}".format(e))

        self.logger.info("Recovery for reservations in slice {} completed".format(slice_obj))

    def recover_reservation(self, *, r: ABCReservationMixin, slice_obj: ABCSlice):
        """
        Recover reservation
        @param r reservation
        @param slice_obj slice object
        """
        try:
            r.restore(actor=self, slice_obj=slice_obj)

            self.logger.info(
                "Found reservation # {} in state {}".format(r.get_reservation_id(), r.get_reservation_state()))
            if r.is_closed():
                self.logger.info("Reservation #{} is closed. Nothing to recover.".format(r.get_reservation_id()))
                return

            self.logger.info("Recovering reservation #{}".format(r.get_reservation_id()))
            self.logger.debug("Recovering reservation object r={}".format(r))

            self.logger.debug("Registering the reservation with the actor")
            self.re_register(reservation=r)

            self.logger.info(r)

            self.logger.debug("Revisiting with the Plugin")

            self.plugin.revisit(reservation=r)

            self.logger.info(r)

            self.logger.debug("Revisiting with the actor policy")
            self.policy.revisit(reservation=r)

            self.logger.info("Recovered reservation #{}".format(r.get_reservation_id()))
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred in recovering reservation e={}".format(e))
            raise ActorException("Could not recover Reservation #{}".format(r))

    def recover_delegations(self, *, slice_obj: ABCSlice):
        """
        Recover delegations for a slice
        @param slice_obj slice object
        """
        self.logger.info(
            "Starting to recover delegations in slice {}({})".format(slice_obj.get_name(), slice_obj.get_slice_id()))
        try:
            delegations = self.plugin.get_database().get_delegations(slice_id=str(slice_obj.get_slice_id()))
        except Exception as e:
            self.logger.error(e)
            raise ActorException(
                "Could not fetch delegations records for slice {}({}) from database".format(slice_obj.get_name(),
                                                                                            slice_obj.get_slice_id()))

        self.logger.debug("There are {} delegations(s) in slice".format(len(delegations)))

        for d in delegations:
            try:
                self.logger.info("Delegation has properties: {}".format(d))
                self.recover_delegation(d=d, slice_obj=slice_obj)
            except Exception as e:
                self.logger.error("Unexpected error while recovering delegation {}".format(e))

        self.logger.info("Recovery for delegations in slice {} completed".format(slice_obj))

    def recover_delegation(self, *, d: ABCDelegation, slice_obj: ABCSlice):
        """
        Recover delegation
        @param d delegation
        @param slice_obj slice object
        """
        try:

            d.restore(actor=self, slice_obj=slice_obj)

            self.logger.info(
                "Found delegation # {} in state {}".format(d.get_delegation_id(), d.get_state_name()))
            if d.is_closed():
                self.logger.info("Delegation #{} is closed. Nothing to recover.".format(d.get_delegation_id()))
                return

            self.logger.info("Recovering delegation #{}".format(d.get_delegation_id()))
            self.logger.debug("Recovering delegation object d={}".format(d))

            self.logger.debug("Registering the delegation with the actor")
            self.re_register_delegation(delegation=d)

            self.logger.info(d)

            self.logger.debug("Revisiting with the Plugin")

            self.plugin.revisit(delegation=d)

            self.logger.info(d)

            self.logger.debug("Revisiting with the actor policy")
            self.policy.revisit_delegation(delegation=d)

            self.logger.info("Recovered delegation #{}".format(d.get_delegation_id()))
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred in recovering delegation e={}".format(e))
            raise ActorException("Could not recover delegation #{}".format(d))

    def register(self, *, reservation: ABCReservationMixin):
        self.wrapper.register_reservation(reservation=reservation)

    def register_slice(self, *, slice_object: ABCSlice):
        self.wrapper.register_slice(slice_object=slice_object)

    def register_delegation(self, *, delegation: ABCDelegation):
        self.wrapper.register_delegation(delegation=delegation)

    def remove_reservation(self, *, reservation: ABCReservationMixin = None, rid: ID = None):
        if reservation is not None:
            self.wrapper.remove_reservation(rid=reservation.get_reservation_id())

        if rid is not None:
            self.wrapper.remove_reservation(rid=rid)

    def remove_slice(self, *, slice_object: ABCSlice):
        self.wrapper.remove_slice(slice_id=slice_object.get_slice_id())

    def remove_slice_by_slice_id(self, *, slice_id: ID):
        self.wrapper.remove_slice(slice_id=slice_id)

    def re_register_delegation(self, *, delegation: ABCDelegation):
        self.wrapper.re_register_delegation(delegation=delegation)

    def re_register(self, *, reservation: ABCReservationMixin):
        self.wrapper.re_register_reservation(reservation=reservation)

    def re_register_slice(self, *, slice_object: ABCSlice):
        self.wrapper.re_register_slice(slice_object=slice_object)

    def issue_delayed(self):
        """
        Issues delayed operations
        """
        assert self.recovered
        self.close_reservations(reservations=self.closing)
        self.closing.clear()

    def reset(self):
        """
        Reset an actor
        """
        self.issue_delayed()
        self.policy.reset()

    def set_actor_clock(self, *, clock):
        """
        Set actor clock
        @param clock clock
        """
        self.clock = clock

    def set_description(self, *, description: str):
        """
        Set description
        @param description description
        """
        self.description = description

    def set_identity(self, *, token: AuthToken):
        """
        Set identity
        @param token token
        """
        self.identity = token
        self.name = self.identity.get_name()
        self.guid = token.get_guid()

    def set_policy(self, *, policy):
        """
        Set policy
        @param policy policy
        """
        self.policy = policy

    def set_recovered(self, *, value: bool):
        """
        Set recovered flag
        @param value value
        """
        self.recovered = value

    def set_plugin(self, *, plugin):
        """
        Set plugin
        @param plugin
        """
        self.plugin = plugin

    def set_stopped(self, *, value: bool):
        """
        Set stopped flag
        @param value value
        """
        self.stopped = value

    def is_on_actor_thread(self) -> bool:
        """
        Check if running on actor thread
        @return true if running on actor thread, false otherwise
        """
        result = False
        try:
            self.thread_lock.acquire()
            result = self.thread == threading.current_thread()
        finally:
            self.thread_lock.release()
        return result

    def execute_on_actor_thread_and_wait(self, *, runnable: ABCActorRunnable):
        """
        Execute an incoming action on actor thread
        @param runnable incoming action/operation
        """
        if self.is_on_actor_thread():
            return runnable.run()
        else:
            status = ExecutionStatus()
            event = ActorEvent(status=status, runnable=runnable)

            self.queue_event(incoming=event)

            with status.lock:
                while not status.done:
                    status.lock.wait()

            if status.exception is not None:
                raise status.exception

            return status.result

    def run(self):
        """
        Actor run function for actor thread
        """
        try:
            self.logger.info("Actor Main Thread started")
            self.actor_count -= 1
            self.actor_main()
        except Exception as e:
            self.logger.error(f"Unexpected error {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.info("Actor Main Thread exited")

    def start(self):
        """
        Start an Actor
        """
        try:
            self.thread_lock.acquire()
            if self.thread is not None:
                raise ActorException("This actor has already been started")

            self.thread = threading.Thread(target=self.run)
            self.thread.setName(self.get_name())
            self.thread.setDaemon(True)
            self.thread.start()
        finally:
            self.thread_lock.release()

        self.message_service.start()

        if self.plugin.get_handler_processor() is not None:
            self.plugin.get_handler_processor().start()

    def stop(self):
        """
        Stop an actor
        """
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
                    self.logger.error(traceback.format_exc())
                finally:
                    self.thread_lock.release()
        finally:
            if self.thread_lock is not None and self.thread_lock.locked():
                self.thread_lock.release()

        if self.plugin.get_handler_processor() is not None:
            self.plugin.get_handler_processor().shutdown()

    def tick_handler(self):
        """
        Tick handler
        """

    def handle_failed_rpc(self, *, rid: ID, rpc: FailedRPC):
        """
        Handler failed rpc
        """
        self.wrapper.process_failed_rpc(rid=rid, rpc=rpc)

    def __str__(self):
        return "actor: [{}/{}]".format(self.name, self.guid)

    def unregister(self, *, reservation: ABCReservationMixin, rid: ID):
        """
        Unregister reservation
        @param reservation reservation
        @param rid reservation id
        """
        if reservation is not None:
            self.wrapper.unregister_reservation(rid=reservation.get_reservation_id())

        if rid is not None:
            self.wrapper.unregister_reservation(rid=rid)

    def unregister_slice(self, *, slice_object: ABCSlice):
        """
        Unregister slice
        @param slice_obj slice object
        """
        self.wrapper.unregister_slice(slice_id=slice_object.get_slice_id())

    def unregister_slice_by_slice_id(self, *, slice_id: ID):
        """
        Unregister slice by slice id
        @param slice_id slice id
        """
        self.wrapper.unregister_slice(slice_id=slice_id)

    def queue_timer(self, timer: ABCTimerTask):
        """
        Queue an event on Actor timer queue
        """
        with self.actor_main_lock:
            self.timer_queue.put_nowait(timer)
            self.logger.debug("Added timer to timer queue {}".format(timer.__class__.__name__))
            self.actor_main_lock.notify_all()

    def queue_event(self, *, incoming: ABCActorEvent):
        """
        Queue an even on Actor Event Queue
        """
        with self.actor_main_lock:
            self.event_queue.put_nowait(incoming)
            self.logger.debug("Added event to event queue {}".format(incoming.__class__.__name__))
            self.actor_main_lock.notify_all()

    def await_no_pending_reservations(self):
        """
        Await until no pending reservations
        """
        self.wrapper.await_nothing_pending()

    def actor_main(self):
        """
        Actor Main loop
        """
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
                    try:
                        for event in IterableQueue(source_queue=self.event_queue):
                            events.append(event)
                    except Exception as e:
                        self.logger.error(f"Error while adding event to event queue! e: {e}")
                        self.logger.error(traceback.format_exc())

                if not self.timer_queue.empty():
                    try:
                        for timer in IterableQueue(source_queue=self.timer_queue):
                            timers.append(timer)
                    except Exception as e:
                        self.logger.error(f"Error while adding event to event queue! e: {e}")
                        self.logger.error(traceback.format_exc())

                self.actor_main_lock.notify_all()

            if len(events) > 0:
                self.logger.debug(f"Processing {len(events)} events")
                for event in events:
                    #self.logger.debug("Processing event of type {}".format(type(event)))
                    #self.logger.debug("Processing event {}".format(event))
                    try:
                        event.process()
                        #self.logger.debug("Processing event {} done".format(event))
                    except Exception as e:
                        self.logger.error(f"Error while processing event {type(event)}, {e}")
                        self.logger.error(traceback.format_exc())

            if len(timers) > 0:
                self.logger.debug(f"Processing {len(timers)} timers")
                for t in timers:
                    try:
                        t.execute()
                    except Exception as e:
                        self.logger.error(f"Error while processing a timer {type(t)}, {e}")
                        self.logger.error(traceback.format_exc())

    def setup_message_service(self):
        """
        Set up Message Service for incoming Kafka Messages
        """
        try:
            # Kafka Proxy Service object
            module_name = self.get_kafka_service_module()
            class_name = self.get_kafka_service_class()
            kafka_service = ReflectionUtils.create_instance_with_params(module_name=module_name,
                                                                        class_name=class_name)(actor=self)

            # Kafka Management Service object
            module_name = self.get_mgmt_kafka_service_module()
            class_name = self.get_mgmt_kafka_service_class()
            kafka_mgmt_service = ReflectionUtils.create_instance_with_params(module_name=module_name,
                                                                             class_name=class_name)()
            kafka_mgmt_service.set_logger(logger=self.logger)

            # Incoming Message Service
            from fabric_cf.actor.core.container.globals import GlobalsSingleton
            config = GlobalsSingleton.get().get_config()
            topic = config.get_actor().get_kafka_topic()
            topics = [topic]
            consumer_conf = GlobalsSingleton.get().get_kafka_config_consumer()
            self.message_service = MessageService(kafka_service=kafka_service, kafka_mgmt_service=kafka_mgmt_service,
                                                  consumer_conf=consumer_conf,
                                                  key_schema_location=GlobalsSingleton.get().get_config().get_kafka_key_schema_location(),
                                                  value_schema_location=GlobalsSingleton.get().get_config().get_kafka_value_schema_location(),
                                                  topics=topics, logger=self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Failed to setup message service e={}".format(e))
            raise e

    def set_logger(self, logger):
        self.logger = logger
        if self.policy is not None:
            self.policy.set_logger(logger=logger)
        if self.plugin is not None:
            self.plugin.set_logger(logger=logger)

    def load_model(self, *, graph_id: str):
        return
