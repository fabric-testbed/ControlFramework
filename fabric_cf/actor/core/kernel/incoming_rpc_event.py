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
from typing import TYPE_CHECKING

from fabric_cf.actor.core.kernel.rpc_request_type import RPCRequestType
from fabric_cf.actor.core.apis.abc_actor_event import ABCActorEvent
from fabric_cf.actor.core.apis.abc_authority import ABCAuthority
from fabric_cf.actor.core.apis.abc_broker_mixin import ABCBrokerMixin
from fabric_cf.actor.core.apis.abc_controller import ABCController
from fabric_cf.actor.core.util.rpc_exception import RPCException

if TYPE_CHECKING:

    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.core.kernel.incoming_rpc import IncomingRPC
    from fabric_cf.actor.core.apis.abc_client_actor import ABCClientActor
    from fabric_cf.actor.core.apis.abc_server_actor import ABCServerActor


class IncomingRPCEvent(ABCActorEvent):
    """
    Represents incoming RPC event
    """
    def __init__(self, *, actor: ABCActorMixin, rpc: IncomingRPC):
        self.actor = actor
        self.rpc = rpc

    def do_process_actor(self, *, actor: ABCActorMixin):
        """
        Process Incoming RPC events common for all actors
        """
        processed = True
        if self.rpc.get_request_type() == RPCRequestType.Query:
            actor.get_logger().info("processing query from <{}>".format(self.rpc.get_caller().get_name()))
            result = actor.query(query=self.rpc.get(), caller=self.rpc.get_caller(), id_token=self.rpc.get_id_token())

            from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
            RPCManagerSingleton.get().query_result(actor=actor, remote_actor=self.rpc.get_callback(),
                                                   request_id=self.rpc.get_message_id(),
                                                   response=result, caller=actor.get_identity())
        elif self.rpc.get_request_type() == RPCRequestType.QueryResult:
            actor.get_logger().info("processing query response from <{}>".format(self.rpc.get_caller().get_name()))
            result = self.rpc.get()
            if self.rpc.get_response_handler() is not None:
                handler = self.rpc.get_response_handler()
                handler.handle(status=self.rpc.get_error(), result=result)
            else:
                actor.get_logger().warning("No response handler is associated with the queryResponse. "
                                           "Ignoring queryResponse")
        else:
            processed = False
        return processed

    def do_process_client(self, *, client: ABCClientActor):
        """
        Process Incoming RPC events common for client actors
        """
        processed = True
        if self.rpc.get_request_type() == RPCRequestType.UpdateLease:
            client.get_logger().info("processing update lease from <{}>".format(self.rpc.get_caller().get_name()))
            if self.rpc.get().get_resources().get_resources() is not None:
                client.get_logger().info("inbound lease is {}".format(
                    self.rpc.get().get_resources().get_resources()))

            client.update_lease(reservation=self.rpc.get(), update_data=self.rpc.get_update_data(),
                                    caller=self.rpc.get_caller())
        elif self.rpc.get_request_type() == RPCRequestType.UpdateTicket:
            client.get_logger().info("processing update ticket from <{}>".format(self.rpc.get_caller().get_name()))
            client.update_ticket(reservation=self.rpc.get(), update_data=self.rpc.get_update_data(),
                                 caller=self.rpc.get_caller())
            client.get_logger().info("update ticket processed from <{}>".format(self.rpc.get_caller().get_name()))
        elif self.rpc.get_request_type() == RPCRequestType.UpdateDelegation:
            client.get_logger().info("processing update delegation from <{}>".format(self.rpc.get_caller().get_name()))
            client.update_delegation(delegation=self.rpc.get(), update_data=self.rpc.get_update_data(),
                                     caller=self.rpc.get_caller())
            client.get_logger().info("update delegation processed from <{}>".format(self.rpc.get_caller().get_name()))
        else:
            processed = self.do_process_actor(actor=client)
        return processed

    def do_process_server(self, *, server: ABCServerActor):
        """
        Process Incoming RPC events common for server actors
        """
        processed = True
        if self.rpc.get_request_type() == RPCRequestType.ClaimDelegation:
            server.get_logger().info("processing claim delegation from <{}>".format(self.rpc.get_caller().get_name()))
            server.claim_delegation(delegation=self.rpc.get(), callback=self.rpc.get_callback(),
                                    caller=self.rpc.get_caller(), id_token=self.rpc.get_id_token())
            server.get_logger().info("claim processed from <{}>".format(self.rpc.get_caller().get_name()))

        elif self.rpc.get_request_type() == RPCRequestType.ReclaimDelegation:
            server.get_logger().info("processing reclaim delegation from <{}>".format(self.rpc.get_caller().get_name()))
            server.reclaim_delegation(delegation=self.rpc.get(), callback=self.rpc.get_callback(),
                                      caller=self.rpc.get_caller(), id_token=self.rpc.get_id_token())
            server.get_logger().info("reclaim processed from <{}>".format(self.rpc.get_caller().get_name()))

        elif self.rpc.get_request_type() == RPCRequestType.Ticket:
            server.get_logger().info("processing ticket from <{}>".format(self.rpc.get_caller().get_name()))
            server.ticket(reservation=self.rpc.get(), callback=self.rpc.get_callback(),
                          caller=self.rpc.get_caller())
            server.get_logger().info("ticket processed from <{}>".format(self.rpc.get_caller().get_name()))

        elif self.rpc.get_request_type() == RPCRequestType.ExtendTicket:
            server.get_logger().info("processing extend ticket from <{}>".format(self.rpc.get_caller().get_name()))
            server.extend_ticket(reservation=self.rpc.get(), caller=self.rpc.get_caller())
            server.get_logger().info("extend ticket processed from <{}>".format(self.rpc.get_caller().get_name()))

        elif self.rpc.get_request_type() == RPCRequestType.Relinquish:
            server.get_logger().info("processing relinquish from <{}>".format(self.rpc.get_caller().get_name()))
            server.relinquish(reservation=self.rpc.get(), caller=self.rpc.get_caller())
            server.get_logger().info("relinquish processed from <{}>".format(self.rpc.get_caller().get_name()))

        else:
            processed = self.do_process_actor(actor=server)
        return processed

    def do_process_broker(self, *, broker: ABCBrokerMixin):
        """
        Process Incoming RPC events common for brokers
        """
        processed = self.do_process_server(server=broker)
        if not processed:
            processed = self.do_process_client(client=broker)
        return processed

    def do_process_authority(self, *, authority: ABCAuthority):
        """
        Process Incoming RPC events common for AMs
        """
        processed = True
        if self.rpc.get_request_type() == RPCRequestType.Redeem:
            authority.get_logger().info("processing redeem from <{}>".format(self.rpc.get_caller().get_name()))
            authority.redeem(reservation=self.rpc.get(), callback=self.rpc.get_callback(),
                             caller=self.rpc.get_caller())

        elif self.rpc.get_request_type() == RPCRequestType.ExtendLease:
            authority.get_logger().info("processing extend lease from <{}>".format(self.rpc.get_caller().get_name()))
            authority.extend_lease(reservation=self.rpc.get(), caller=self.rpc.get_caller())

        elif self.rpc.get_request_type() == RPCRequestType.ModifyLease:
            authority.get_logger().info("processing modify lease from <{}>".format(self.rpc.get_caller().get_name()))
            authority.modify_lease(reservation=self.rpc.get(), caller=self.rpc.get_caller())

        elif self.rpc.get_request_type() == RPCRequestType.Close:
            authority.get_logger().info("processing close from <{}>".format(self.rpc.get_caller().get_name()))
            authority.relinquish(reservation=self.rpc.get(), caller=self.rpc.get_caller())

        else:
            processed = self.do_process_server(server=authority)
        return processed

    def do_process_controller(self, *, controller: ABCController):
        """
        Process Incoming RPC events common for all controller/orchestrator
        """
        processed = self.do_process_client(client=controller)
        return processed

    def process(self):
        """
        Process Incoming RPC events
        """
        done = False
        if isinstance(self.actor, ABCAuthority):
            done = self.do_process_authority(authority=self.actor)
            if done:
                return

        if isinstance(self.actor, ABCBrokerMixin):
            done = self.do_process_broker(broker=self.actor)
            if done:
                return

        if isinstance(self.actor, ABCController):
            done = self.do_process_controller(controller=self.actor)
            if done:
                return

        raise RPCException(message="Unsupported RPC request type: {}".format(self.rpc.get_request_type()))
