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

from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import DelegationState, ABCDelegation
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.apis.abc_proxy import ABCProxy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DelegationException
from fabric_cf.actor.core.delegation.delegation import Delegation
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.update_data import UpdateData


class BrokerDelegation(Delegation):
    def __init__(self, dlg_graph_id: str, slice_id: ID, broker: ABCBrokerProxy = None):
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
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.graph = None
        self.actor = None
        self.slice_object = None
        self.logger = None
        self.policy = None

    def get_broker(self) -> ABCBrokerProxy:
        """
        Get the broker
        @return broker
        """
        return self.broker

    def set_exported(self, value: bool):
        """
        Set exported
        @param value value
        """
        self.exported = value

    def is_exported(self) -> bool:
        """
        Return is exported
        @return true if exported; false otherwise
        """
        return self.exported

    def claim(self):
        raise DelegationException("Not supported on Broker Delegation")

    def delegate(self, policy: ABCPolicy, id_token: str = None):
        self.policy = policy

        if self.state == DelegationState.Nascent or self.state == DelegationState.Reclaimed:
            if self.exported:
                self.sequence_out += 1
                RPCManagerSingleton.get().claim_delegation(delegation=self, id_token=id_token)
                self.transition(prefix="delegate", state=DelegationState.Delegated)
            else:
                self.logger.error(
                    self.error_string_prefix.format(self, self.invalid_state_prefix.format('claim', 'claim')))
                raise DelegationException(self.invalid_state_prefix.format('claim', 'claim'))

        elif self.state == DelegationState.Delegated:
            if self.exported:
                self.logger.error(
                    self.error_string_prefix.format(self, self.invalid_state_prefix.format('claim', 'claim')))
                raise DelegationException(self.invalid_state_prefix.format('claim', 'claim'))

        elif self.state == DelegationState.Closed:
            self.logger.error(
                self.error_string_prefix.format(self, "initiating delegate on defunct Delegation"))
            raise DelegationException("initiating delegate on defunct Delegation")

    def reclaim(self, id_token: str = None):
        if self.state == DelegationState.Delegated:
            # We are an agent asked to return a pre-reserved "will call" ticket
            # to a client. Set mustSendUpdate so that the update will be sent
            # on the next probe.
            self.transition(prefix="reclaimed", state=DelegationState.Delegated)
            self.policy.update_delegation_complete(delegation=self)
            self.service_reclaim(id_token=id_token)
        else:
            self.logger.error(
                self.error_string_prefix.format(self, self.invalid_state_prefix.format('reclaim', 'reclaim')))
            raise DelegationException(self.invalid_state_prefix.format('reclaim', 'reclaim'))

    def service_reclaim(self, id_token: str = None):
        """
        Service reclaim
        @param id_token identity token
        """
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
        """
        Validate outgoing delegation
        """
        if self.slice_object is None:
            self.logger.error(self.error_string_prefix.format(self, Constants.NOT_SPECIFIED_PREFIX.format("slice")))
            raise DelegationException(Constants.NOT_SPECIFIED_PREFIX.format("slice"))

        if self.dlg_graph_id is None:
            self.logger.error(self.error_string_prefix.format(self, Constants.NOT_SPECIFIED_PREFIX.format("graph id")))
            raise DelegationException(Constants.NOT_SPECIFIED_PREFIX.format("graph id"))

    def do_relinquish(self):
        """
        Perform required actions for a relinquish
        """
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
        """
        Close a delegation, remove the corresponding graph from CBM
        """
        if self.state == DelegationState.Nascent:
            self.transition(prefix="close", state=DelegationState.Closed)
            self.do_relinquish()

    def get_client_callback_proxy(self) -> ABCClientCallbackProxy:
        """
        Get Client callback proxy
        @return client callback proxy
        """
        return self.callback

    def delegation_update_satisfies(self, *, incoming: ABCDelegation, update_data: UpdateData):
        """
        Check if the incoming delegation satisfies the update
        @param incoming incoming delegation
        @param update_data update data
        """
        if incoming.get_graph() is not None:
            incoming.get_graph().validate_graph()
        return True

    def absorb_delegation_update(self, *, incoming: ABCDelegation, update_data: UpdateData):
        """
        Absorbs an incoming delegation update.

        @param incoming
                   incoming delegation update
        @param update_data
                   update data
        @throws Exception
        """
        self.logger.debug("absorb_update: {}".format(incoming))
        if self.authority is None and incoming.get_site_proxy() is not None:
            self.authority = incoming.get_site_proxy()

        self.graph = incoming.get_graph()
        self.policy.update_delegation_complete(delegation=self)
        if self.graph is not None:
            self.graph.delete_graph()
            self.graph = None

    def accept_delegation_update(self, *, incoming: ABCDelegation, update_data: UpdateData):
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

    def update_delegation(self, *, incoming: ABCDelegation, update_data: UpdateData):
        """
        Update delegation
        @param incoming incoming delegation
        @param update_data update data
        """
        if self.state == DelegationState.Nascent or self.state == DelegationState.Delegated:
            if self.accept_delegation_update(incoming=incoming, update_data=update_data):
                if incoming.get_graph() is not None:
                    self.transition(prefix="Delegation update", state=DelegationState.Delegated)
                else:
                    self.transition(prefix="Delegation reclaimed", state=DelegationState.Reclaimed)
                self.set_dirty()

        elif self.state == DelegationState.Closed:
            self.logger.warning("Delegation update after close")

        elif self.state == DelegationState.Failed:
            self.logger.error(message="Delegation update on failed delegation: {}".format(update_data))

        elif self.state == DelegationState.Reclaimed:
            self.transition(prefix="ticket update", state=DelegationState.Delegated)

    def service_update_delegation(self):
        """
        Service an update delegation
        """
        # TODO
        # update the graph

    def service_delegate(self):
        """
        Service delegate
        """

    def set_site_proxy(self, *, site_proxy: ABCProxy):
        """
        Set Authority Proxy
        """
        self.authority = site_proxy

    def get_site_proxy(self) -> ABCProxy:
        """
        get Authority Proxy
        """
        return self.authority
