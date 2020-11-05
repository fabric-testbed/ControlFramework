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
import traceback

from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
from fabric.actor.core.apis.i_delegation import DelegationState, IDelegation
from fabric.actor.core.apis.i_policy import IPolicy
from fabric.actor.core.delegation.delegation import Delegation
from fabric.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.update_data import UpdateData


class BrokerDelegation(Delegation):
    def __init__(self, dlg_graph_id: ID, slice_id: ID, broker: IBrokerProxy = None):
        super().__init__(dlg_graph_id=dlg_graph_id, slice_id=slice_id)
        self.exported = False
        self.broker = broker
        self.authority = None
        # Relinquish status.
        self.relinquished = False
        # The status of the last ticket update.
        self.last_delegation_update = UpdateData()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['graph']
        del state['actor']
        del state['slice_object']
        del state['logger']
        del state['policy']
        del state['callback']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.graph = None
        self.actor = None
        self.slice_object = None
        self.logger = None
        self.policy = None
        self.callback = None

    def get_broker(self) -> IBrokerProxy:
        return self.broker

    def set_exported(self, value: bool):
        self.exported = value

    def is_exported(self) -> bool:
        return self.exported

    def claim(self):
        raise Exception("Not supported on Broker Delegation")

    def delegate(self, policy: IPolicy, id_token:str = None):
        self.policy = policy

        if self.state == DelegationState.Nascent:

            if self.exported:
                self.sequence_out += 1
                RPCManagerSingleton.get().claim_delegation(delegation=self, id_token=id_token)
                self.transition(prefix="delegate", state=DelegationState.Delegated)
            else:
                self.error(err="Invalid state for claim. Did you already claim this Delegation?")

        elif self.state == DelegationState.Delegated:
            if self.exported:
                self.error(err="Invalid state for claim. Did you already claim this Delegation?")
        elif self.state == DelegationState.Closed:
            self.error(err="initiating reserve on defunct Delegation")

    def reclaim(self, id_token: str = None):
        if self.state == DelegationState.Delegated:
            # We are an agent asked to return a pre-reserved "will call" ticket
            # to a client. Set mustSendUpdate so that the update will be sent
            # on the next probe.
            self.transition(prefix="reclaimed", state=DelegationState.Delegated)
            self.service_reclaim(id_token=id_token)
        else:
            self.error(err="Wrong delegation state for delegation reclaim")

    def service_reclaim(self, id_token: str = None):
        self.logger.debug("Servicing reclaim")
        if self.callback is None:
            self.logger.warning("Cannot generate reclaim: no callback.")
            return

        self.logger.debug("Generating reclaim")
        try:
            if self.exported:
                self.sequence_out += 1
                RPCManagerSingleton.get().reclaim_delegation(delegation=self, id_token=id_token)
        except Exception as e:
            # Note that this may result in a "stuck" delegation... not much we
            # can do if the receiver has failed or rejects our update. We will
            # regenerate on any user-initiated probe.
            self.logger.error("callback failed: {}".format(e))

    def validate_outgoing(self):
        if self.slice_object is None:
            self.error(err="No slice specified")

        if self.dlg_graph_id is None:
            self.error(err="No Graph specified")

    def do_relinquish(self):
        if not self.relinquished:
            self.relinquished = True
            try:
                if self.policy is not None:
                    self.policy.closed_delegation(delegation=self)
                else:
                    self.logger.warning("policy not set in reservation {}, unable to call policy.closed(), "
                                        "continuing".format(self.dlg_graph_id))
            except Exception as e:
                self.logger.error("close with policy {}".format(e))

            try:
                self.sequence_out += 1
                RPCManagerSingleton.get().relinquish_delegation(delegation=self)
            except Exception as e:
                self.logger.error("broker reports relinquish error: {}".format(e))

    def close(self):
        if self.state == DelegationState.Nascent:
            self.transition(prefix="close", state=DelegationState.Closed)
            self.do_relinquish()
            # TODO

    def get_client_callback_proxy(self) -> IClientCallbackProxy:
        return self.callback

    def delegation_update_satisfies(self, *, incoming: IDelegation, update_data: UpdateData):
        incoming.get_graph().validate_graph()
        return True

    def absorb_delegation_update(self, *, incoming: IDelegation, update_data: UpdateData):
        """
        Absorbs an incoming delegation update.

        @param incoming
                   incoming delegation update
        @param update_data
                   update data
        @throws Exception
        """
        self.logger.debug("absorb_update: {}".format(incoming))
        self.graph = incoming.get_graph()
        self.policy.update_delegation_complete(delegation=self)
        self.graph.delete_graph()
        self.graph = None

    def accept_delegation_update(self, *, incoming: IDelegation, update_data: UpdateData):
        """
        Determines whether the incoming delegation update is acceptable and if so
        accepts it.

        @param incoming
                   incoming delegation update
        @param update_data
                   update data
        @return true if the update was successful
        """
        self.last_delegation_update.absorb(other=update_data)
        success = True
        if update_data.is_failed():
            success = False
        else:
            try:
                self.delegation_update_satisfies(incoming=incoming, update_data=update_data)
                self.absorb_delegation_update(incoming=incoming, update_data=update_data)
            except Exception as e:
                success = False
                update_data.error(message=str(e))
                self.logger.error(traceback.format_exc())
                self.logger.error("accept_delegation_update: {}".format(e))

        if not success:
            if self.state == DelegationState.Nascent:
                self.transition(prefix="failed delegation reserve",
                                state=DelegationState.Failed)
            else:
                self.transition(prefix="failed delegation update", state=self.state)

        return success

    def update_delegation(self, *, incoming: IDelegation, update_data: UpdateData):
        if self.state == DelegationState.Nascent or self.state == DelegationState.Delegated:
            if self.accept_delegation_update(incoming=incoming, update_data=update_data):
                self.transition(prefix="Delegation update", state=DelegationState.Delegated)
                self.set_dirty()

        elif self.state == DelegationState.Closed:
            self.logger.warning("Delegation update after close")

        elif self.state == DelegationState.Failed:
            self.logger.error(message="Delegation update on failed delegation: {}".format(e))

        elif self.state == DelegationState.Reclaimed:
            self.transition(prefix="ticket update", state=DelegationState.Delegated)

    def service_update_delegation(self):
        # TODO
        # update the graph
        return

    def service_delegate(self):
        return
