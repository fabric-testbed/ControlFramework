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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_actor_proxy import IActorProxy
from fabric.actor.core.apis.i_authority_proxy import IAuthorityProxy
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_callback_proxy import ICallbackProxy
from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
from fabric.actor.core.apis.i_controller_reservation import IControllerReservation
from fabric.actor.core.apis.i_query_response_handler import IQueryResponseHandler
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.kernel.claim_timeout import ClaimTimeout
from fabric.actor.core.kernel.failed_rpc import FailedRPC
from fabric.actor.core.kernel.failed_rpc_event import FailedRPCEvent
from fabric.actor.core.kernel.inbound_rpc_event import InboundRPCEvent
from fabric.actor.core.kernel.incoming_failed_rpc import IncomingFailedRPC
from fabric.actor.core.kernel.incoming_rpc import IncomingRPC
from fabric.actor.core.kernel.incoming_rpc_event import IncomingRPCEvent
from fabric.actor.core.kernel.incoming_reservation_rpc import IncomingReservationRPC
from fabric.actor.core.kernel.outbound_rpc_event import OutboundRPCEvent
from fabric.actor.core.kernel.query_timeout import QueryTimeout
from fabric.actor.core.kernel.rpc_executor import RPCExecutor
from fabric.actor.core.kernel.rpc_request import RPCRequest
from fabric.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric.actor.core.proxies.proxy import Proxy
from fabric.actor.core.util.kernel_timer import KernelTimer
from fabric.actor.core.util.rpc_exception import RPCException, RPCError
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.auth_token import AuthToken


class RPCManager:
    CLAIM_TIMEOUT_MS = 120000
    QUERY_TIMEOUT_MS = 120000

    def __init__(self):
        # Table of pending RPC requests.
        self.pending = {}
        self.started = False
        self.numQueued = 0
        self.pending_lock = threading.Lock()
        self.stats_lock = threading.Condition()

    @staticmethod
    def validate(reservation: IReservation, check_requested: bool = False):
        if reservation is None:
            raise Exception("Missing reservation")

        if reservation.get_slice() is None:
            raise Exception("Missing slice")

        if check_requested :
            if reservation.get_requested_resources() is None:
                raise Exception("Missing requested resources")

            if reservation.get_requested_term() is None:
                raise Exception("Missing requested term")

        if isinstance(reservation, IClientReservation):
            if reservation.get_broker() is None:
                raise Exception("Missing broker proxy")

            if reservation.get_client_callback_proxy() is None:
                raise Exception("Missing client callback proxy")

        elif isinstance(reservation, IControllerReservation):
            if reservation.get_authority() is None:
                raise Exception("Missing authority proxy")

            if reservation.get_client_callback_proxy() is None:
                raise Exception("Missing client callback proxy")

    def start(self):
        self.do_start()

    def stop(self):
        self.do_stop()

    def retry(self, request: RPCRequest):
        if request is None:
            raise Exception("Missing request")
        self.do_retry(request)

    def failed_rpc(self, actor: IActor, rpc: IncomingRPC, e: Exception):
        if actor is None:
            raise Exception("Missing actor")

        if rpc is None:
            raise Exception("Missing rpc")

        if rpc.get_callback() is None:
            raise Exception("Missing callback in rpc")

        if isinstance(rpc, IncomingFailedRPC):
            raise Exception("Cannot reply to a FailedRPC with a FailedRPC")

        self.do_failed_rpc(actor, rpc.get_callback(), rpc, e, actor.get_identity())

    def claim(self, reservation: IClientReservation):
        self.validate(reservation)
        self.do_claim(reservation.get_actor(), reservation.get_broker(),
                      reservation, reservation.get_client_callback_proxy(),
                      reservation.get_slice().get_owner())

    def ticket(self, reservation: IClientReservation):
        self.validate(reservation, True)
        self.do_ticket(reservation.get_actor(), reservation.get_broker(),
                       reservation, reservation.get_client_callback_proxy(),
                       reservation.get_slice().get_owner())

    def extend_ticket(self, reservation: IClientReservation):
        self.validate(reservation, True)
        self.do_extend_ticket(reservation.get_actor(), reservation.get_broker(),
                              reservation, reservation.get_client_callback_proxy(),
                              reservation.get_slice().get_owner())

    def relinquish(self, reservation: IClientReservation):
        self.validate(reservation)
        self.do_relinquish(reservation.get_actor(), reservation.get_broker(),
                           reservation, reservation.get_client_callback_proxy(),
                           reservation.get_slice().get_owner())

    def redeem(self, reservation: IControllerReservation):
        self.validate(reservation, True)
        self.do_redeem(reservation.get_actor(), reservation.get_authority(),
                       reservation, reservation.get_client_callback_proxy(),
                       reservation.get_slice().get_owner())

    def extend_lease(self, proxy: IAuthorityProxy, reservation: IControllerReservation, caller: AuthToken):
        self.validate(reservation, True)
        self.do_extend_lease(reservation.get_actor(), reservation.get_authority(),
                             reservation, reservation.get_client_callback_proxy(),
                             reservation.get_slice().get_owner())

    def modify_lease(self, proxy: IAuthorityProxy, reservation: IControllerReservation, caller: AuthToken):
        self.validate(reservation, True)
        self.do_modify_lease(reservation.get_actor(), reservation.get_authority(),
                             reservation, reservation.get_client_callback_proxy(),
                             reservation.get_slice().get_owner())

    def close(self, reservation: IControllerReservation):
        self.validate(reservation)
        self.do_close(reservation.get_actor(), reservation.get_authority(),
                      reservation, reservation.get_client_callback_proxy(),
                      reservation.get_slice().get_owner())

    def update_ticket(self, reservation: IBrokerReservation):
        self.validate(reservation)
        # get a callback to the actor calling updateTicket, so that any
        # failures in the remote actor can be delivered back
        callback = Proxy.get_callback(reservation.get_actor(), reservation.get_callback().get_type())
        if callback is None:
            raise Exception("Missing callback")
        self.do_update_ticket(reservation.get_actor(), reservation.get_callback(),
                              reservation, reservation.get_update_data(),
                              callback, reservation.get_actor().get_identity())

    def update_lease(self, reservation: IAuthorityReservation):
        self.validate(reservation)
        # get a callback to the actor calling updateTicket, so that any
        # failures in the remote actor can be delivered back
        callback = Proxy.get_callback(reservation.get_actor(), reservation.get_callback().get_type())
        if callback is None:
            raise Exception("Missing callback")
        self.do_update_lease(reservation.get_actor(), reservation.get_callback(),
                             reservation, reservation.get_update_data(),
                             callback, reservation.get_actor().get_identity())

    def query(self, actor: IActor, remote_actor: IActorProxy, callback: ICallbackProxy,
              query: dict, handler: IQueryResponseHandler):
        if actor is None:
            raise Exception("Missing actor")
        if remote_actor is None:
            raise Exception("Missing remote actor")
        if callback is None:
            raise Exception("Missing callback")
        if query is None:
            raise Exception("Missing query")
        if handler is None:
            raise Exception("Missing handler")
        self.do_query(actor, remote_actor, callback, query, handler, callback.get_identity())

    def query_result(self, actor: IActor, remote_actor: ICallbackProxy, request_id: str, response: dict,
                     caller: AuthToken):
        if actor is None:
            raise Exception("Missing actor")
        if remote_actor is None:
            raise Exception("Missing remote actor")
        if request_id is None:
            raise Exception("Missing request_id")
        if response is None:
            raise Exception("Missing response")
        if caller is None:
            raise Exception("Missing caller")
        self.do_query_result(actor, remote_actor, request_id, response, caller)

    def dispatch_incoming(self, actor: IActor, rpc: IncomingRPC):
        if actor is None:
            raise Exception("Missing actor")
        if rpc is None:
            raise Exception("Missing rpc")
        self.do_dispatch_incoming_rpc(actor, rpc)

    def await_nothing_pending(self):
        self.do_await_nothing_pending()

    def do_await_nothing_pending(self):
        with self.stats_lock:
            while self.numQueued > 0:
                self.stats_lock.wait()

    def do_start(self):
        try:
            self.pending_lock.acquire()
            self.pending.clear()
        finally:
            self.pending_lock.release()
        self.started = True

    def do_stop(self):
        self.started = False
        try:
            self.pending_lock.acquire()
            self.pending.clear()
        finally:
            self.pending_lock.release()

    def do_failed_rpc(self, actor: IActor, proxy: ICallbackProxy, rpc: IncomingRPC, e: Exception, caller: AuthToken):
        proxy.get_logger().info("Outbound failedRPC request from <{}>: requestID={}".format(caller.get_name(),
                                                                                            rpc.getMessageID()))
        message = "RPC failed at remote actor."
        if e is not None:
            message += " message:{}".format(e)

        rid = None
        if isinstance(rpc, IncomingReservationRPC):
            rid = rpc.get_reservation().get_reservation_id()

        state = proxy.prepare_failed_request(rpc.get_message_id(),
                                             rpc.get_request_type(),
                                             rid, message, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.FailedRPC)
        outgoing = RPCRequest(state, actor, proxy, None, 0, None)
        self.enqueue(outgoing)

    def do_claim(self, actor: IActor, proxy: IBrokerProxy, reservation: IClientReservation,
                 callback: IClientCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound claim request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_claim(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Claim)

        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_ticket_sequence_out(), None)
        # Schedule a timeout
        rpc.timer = KernelTimer.schedule(actor, ClaimTimeout(rpc), self.CLAIM_TIMEOUT_MS)
        self.enqueue(rpc)

    def do_ticket(self, actor: IActor, proxy: IBrokerProxy, reservation: IClientReservation,
                 callback: IClientCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound ticket request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_ticket(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Ticket)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_ticket_sequence_out(), None)
        self.enqueue(rpc)

    def do_extend_ticket(self, actor: IActor, proxy: IBrokerProxy, reservation: IClientReservation,
                         callback: IClientCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound extend ticket request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_extend_ticket(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.ExtendTicket)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_ticket_sequence_out(), None)
        self.enqueue(rpc)

    def do_relinquish(self, actor: IActor, proxy: IBrokerProxy, reservation: IClientReservation,
                         callback: IClientCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound relinquish request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_relinquish(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Relinquish)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_ticket_sequence_out(), None)
        self.enqueue(rpc)

    def do_redeem(self, actor: IActor, proxy: IAuthorityProxy, reservation: IControllerReservation,
                         callback: IControllerCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound relinquish redeem from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_redeem(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Redeem)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_ticket_sequence_out(), None)
        self.enqueue(rpc)

    def do_extend_lease(self, actor: IActor, proxy: IAuthorityProxy, reservation: IControllerReservation,
                        callback: IControllerCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound extend lease request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_extend_lease(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.ExtendLease)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_lease_sequence_out(), None)
        self.enqueue(rpc)

    def do_modify_lease(self, actor: IActor, proxy: IAuthorityProxy, reservation: IControllerReservation,
                         callback: IControllerCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound modify lease request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_modify_lease(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.ModifyLease)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_lease_sequence_out(), None)
        self.enqueue(rpc)

    def do_close(self, actor: IActor, proxy: IAuthorityProxy, reservation: IControllerReservation,
                 callback: IControllerCallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound close request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_close(reservation, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Close)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_lease_sequence_out(), None)
        self.enqueue(rpc)

    def do_update_ticket(self, actor: IActor, proxy: IClientCallbackProxy, reservation: IBrokerReservation,
                         update_data: UpdateData, callback: ICallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound update ticket request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_update_ticket(reservation, update_data, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.UpdateTicket)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_sequence_out(), None)
        self.enqueue(rpc)

    def do_update_lease(self, actor: IActor, proxy: IControllerCallbackProxy, reservation: IAuthorityReservation,
                        update_data: UpdateData, callback: ICallbackProxy, caller: AuthToken):
        proxy.get_logger().info("Outbound update lease request from <{}>: {}".format(caller.get_name(), reservation))

        state = proxy.prepare_update_lease(reservation, update_data, callback, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.UpdateLease)
        rpc = RPCRequest(state, actor, proxy, reservation, reservation.get_sequence_out(), None)
        self.enqueue(rpc)

    def do_query(self, actor: IActor, remote_actor: IActorProxy, local_actor: ICallbackProxy,
                 query: dict, handler: IQueryResponseHandler, caller: AuthToken):
        remote_actor.get_logger().info("Outbound query request from <{}>".format(caller.get_name()))

        state = remote_actor.prepare_query(local_actor, query, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.Query)
        rpc = RPCRequest(state, actor, remote_actor, None, None, handler)
        # Timer
        rpc.timer = KernelTimer.schedule(actor, QueryTimeout(rpc), self.QUERY_TIMEOUT_MS)
        self.enqueue(rpc)

    def do_query_result(self, actor: IActor, remote_actor: ICallbackProxy, request_id: str,
                        response: dict, caller: AuthToken):
        remote_actor.get_logger().info("Outbound query_result request from <{}>".format(caller.get_name()))

        state = remote_actor.prepare_query_result(request_id, response, caller)
        state.set_caller(caller)
        state.set_type(RPCRequestType.QueryResult)
        rpc = RPCRequest(state, actor, remote_actor, None, None, None)
        self.enqueue(rpc)

    def do_dispatch_incoming_rpc(self, actor: IActor, rpc: IncomingRPC):
        # see if this is a response for an earlier request that has an
        # associated handler function. If a handler exists, attach the handler
        # to the incoming rpc object.
        request = None
        if rpc.get_request_id() is not None:
            request = self.remove_pending_request(rpc.get_request_id())
            if request is not None:
                if request.handler is not None:
                    rpc.set_response_handler(request.handler)

        if rpc.get_request_type() == RPCRequestType.Query:
            actor.get_logger().info("Inbound query from <{}>".format(rpc.get_caller().get_name()))

        elif rpc.get_request_type() == RPCRequestType.QueryResult:
            actor.get_logger().info("Inbound query response from <{}>".format(rpc.get_caller().get_name()))

            if request is None:
                actor.get_logger().warning("No queryRequest to match to inbound queryResponse. Ignoring response")

        elif rpc.get_request_type() == RPCRequestType.Claim:
            actor.get_logger().info("Inbound claim request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.Ticket:
            actor.get_logger().info("Inbound ticket request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                 rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.ExtendTicket:
            actor.get_logger().info("Inbound extend ticket request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                        rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.Relinquish:
            actor.get_logger().info("Inbound relinquish request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                     rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.UpdateTicket:
            actor.get_logger().info("Inbound update ticket request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                        rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.Redeem:
            actor.get_logger().info("Inbound redeem request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                 rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.ExtendLease:
            actor.get_logger().info("Inbound extend lease request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                       rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.Close:
            actor.get_logger().info("Inbound close request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.UpdateLease:
            actor.get_logger().info("Inbound update lease request from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                                       rpc.get_reservation()))

        elif rpc.get_request_type() == RPCRequestType.FailedRPC:
            actor.get_logger().info("Inbound FailedRPC from <{}>:{}".format(rpc.get_caller().get_name(),
                                                                            rpc.get_reservation()))

        if rpc.get_request_type() == RPCRequestType.FailedRPC:
            actor.get_logger().debug("Failed RPC")
            failed = None
            exception = RPCException(rpc.get_error_details(), RPCError.RemoteError)
            if request is not None:
                if request.proxy.get_identity() == rpc.get_caller():
                    failed = FailedRPC(e=exception, request=request)
                else:
                    actor.get_logger().warning("Failed RPC from an unauthorized caller: expected={} but was={}".format(
                        request.proxy.get_identity(), rpc.get_caller()))

            elif rpc.get_failed_reservation_id() is not None:
                failed = FailedRPC(e=exception, request_type=rpc.get_failed_request_type(),
                                   rid=rpc.get_failed_reservation_id(), auth=rpc.caller)
            else:
                failed = FailedRPC(e=exception, request_type=rpc.get_failed_request_type(), auth=rpc.caller)

            if failed is not None:
                actor.queue_event(FailedRPCEvent(actor, failed))

        else:
            actor.get_logger().debug("Added to actor queue to be processed")
            from fabric.actor.core.container.globals import GlobalsSingleton
            GlobalsSingleton.get().event_manager.dispatch_event(InboundRPCEvent(rpc, actor))
            actor.queue_event(IncomingRPCEvent(actor, rpc))

    def do_retry(self, rpc: RPCRequest):
        rpc.retry_count += 1
        from fabric.actor.core.container.globals import GlobalsSingleton
        logger = GlobalsSingleton.get().get_logger()
        logger.debug("Retrying RPC({}) count={} actor={}".format(rpc.get_request_type(), rpc.retry_count,
                                                          rpc.getActor().getName()))
        self.enqueue(rpc)

    def add_pending_request(self, guid: str, request: RPCRequest):
        try:
            self.pending_lock.acquire()
            self.pending[guid] = request
        finally:
            self.pending_lock.release()

    def remove_pending_request(self, guid: str) -> RPCRequest:
        result = None
        try:
            self.pending_lock.acquire()
            result = self.pending.pop(guid)
        finally:
            self.pending_lock.release()
        return result

    def queued(self):
        with self.stats_lock:
            self.numQueued += 1
            print("Queued: {}".format(self.numQueued))

    def de_queued(self):
        with self.stats_lock:
            if self.numQueued == 0:
                raise Exception("De-queued invoked, but nothing is queued!!!")

            self.numQueued -= 1
            print("DeQueued: {}".format(self.numQueued))
            if self.numQueued == 0:
                self.stats_lock.notify_all()

    def enqueue(self, rpc: RPCRequest):
        if not self.started:
            print("Ignoring RPC request: container is shutting down")
            return
        if rpc.handler is not None:
            self.add_pending_request(rpc.request.get_message_id(), rpc)

        from fabric.actor.core.container.globals import GlobalsSingleton
        GlobalsSingleton.get().event_manager.dispatch_event(OutboundRPCEvent(rpc))

        try:
            self.queued()
            rpc_executor = RPCExecutor(rpc)
            # TODO Thread Pool
            thread = threading.Thread(target=rpc_executor.run())
            thread.setName("RPCExecutor {}".format(rpc.request.get_message_id()))
            thread.start()
        except Exception as e:
            print("Exception occurred while starting RPC Executor {}".format(e))
            self.de_queued()
            if rpc.handler is not None:
                self.remove_pending_request(rpc.request.get_message_id())
            raise e
