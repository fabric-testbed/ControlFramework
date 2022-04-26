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
from fim.graph.abc_property_graph import ABCPropertyGraph

from fabric_cf.actor.fim.fim_helper import FimHelper
from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_callback_proxy import ABCCallbackProxy
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation, DelegationState
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DelegationException
from fabric_cf.actor.core.kernel.rpc_manager_singleton import RPCManagerSingleton
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class Delegation(ABCDelegation):
    error_string_prefix = 'error for delegation: {} : {}'
    invalid_state_prefix = "Invalid state for {}. Did you already {} this Delegation?"

    def __init__(self, dlg_graph_id: str, slice_id: ID, delegation_name: str = None):
        self.dlg_graph_id = dlg_graph_id
        self.slice_id = slice_id
        self.state = DelegationState.Nascent
        self.dirty = False
        self.state_transition = False
        self.sequence_in = 0
        self.update_count = 0
        self.sequence_out = 0
        self.update_data = UpdateData()
        self.must_send_update = False
        self.graph = None
        self.actor = None
        self.slice_object = None
        self.logger = None
        self.policy = None
        self.callback = None
        self.error_message = None
        self.owner = None
        self.delegation_name = delegation_name

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

    def restore(self, actor: ABCActorMixin, slice_obj: ABCSlice):
        self.actor = actor
        self.slice_object = slice_obj
        if actor is not None:
            self.logger = actor.get_logger()
            if self.callback is not None:
                self.callback.set_logger(logger=actor.get_logger())
        if actor.get_type() == ActorType.Authority:
            self.graph = FimHelper.get_arm_graph(graph_id=str(self.dlg_graph_id))

        if actor.get_policy() is not None:
            self.policy = actor.get_policy()

    def set_graph(self, graph: ABCPropertyGraph):
        self.graph = graph

    def get_graph(self) -> ABCPropertyGraph:
        return self.graph

    def get_actor(self) -> ABCActorMixin:
        return self.actor

    def get_delegation_id(self) -> str:
        return self.dlg_graph_id

    def get_slice_id(self) -> ID:
        if self.slice_id is not None:
            return self.slice_id
        elif self.slice_object is not None:
            return self.slice_object.get_slice_id()
        return None

    def get_slice_object(self) -> ABCSlice:
        return self.slice_object

    def get_state(self) -> DelegationState:
        return self.state

    def get_state_name(self) -> str:
        return self.state.name

    def set_logger(self, *, logger):
        self.logger = logger

    def set_slice_object(self, *, slice_object: ABCSlice):
        self.slice_object = slice_object

    def transition(self, *, prefix: str, state: DelegationState):
        if self.logger is not None:
            self.logger.debug("Delegation #{} {} transition: {} -> {}".format(self.dlg_graph_id, prefix,
                                                                              self.get_state_name(),
                                                                              state.name))

        self.state = state
        self.set_dirty()
        self.state_transition = True

    def clear_notice(self):
        """
        Clear the notices
        """
        self.update_data.clear()

    def get_notices(self) -> str:
        s = "Delegation {} (Slice {} ) is in state [{}]".format(self.get_delegation_id(), self.get_slice_id(),
                                                                DelegationState(self.state).name)
        if self.error_message is not None and self.error_message != "":
            s += ", err={}".format(self.error_message)

        notices = self.update_data.get_events()
        if notices is not None:
            s += "\n{}".format(notices)

        notices = self.update_data.get_message()
        if notices is not None:
            s += "\n{}".format(notices)

        return s

    def has_uncommitted_transition(self) -> bool:
        return self.state_transition

    def is_dirty(self) -> bool:
        return self.dirty

    def set_dirty(self):
        self.dirty = True

    def clear_dirty(self):
        self.dirty = False

    def set_actor(self, actor: ABCActorMixin):
        self.actor = actor

    def prepare(self, *, callback: ABCCallbackProxy, logger):
        """
        Prepare a delegation
        """
        self.set_logger(logger=logger)
        self.callback = callback

        # Null callback indicates a locally initiated request to create an
        # exported delegation. Else the request is from a client and must have
        # a client-specified delegation id.

        if self.callback is not None and self.dlg_graph_id is None:
            self.logger.error(self.error_string_prefix.format(self, Constants.NOT_SPECIFIED_PREFIX.format("graph id")))
            raise DelegationException(Constants.NOT_SPECIFIED_PREFIX.format("graph id"))

        self.set_dirty()

    def is_closed(self) -> bool:
        return self.state == DelegationState.Closed

    def is_delegated(self) -> bool:
        return self.state == DelegationState.Delegated

    def is_reclaimed(self) -> bool:
        return self.state == DelegationState.Reclaimed

    def is_failed(self) -> bool:
        return self.state == DelegationState.Failed

    def delegate(self, policy: ABCPolicy):
        # These handlers may need to be slightly more sophisticated, since a
        # client may bid multiple times on a ticket as part of an auction
        # protocol: so we may receive a reserve or extend when there is already
        # a request pending.
        self.incoming_request()

        self.policy = policy
        self.map_and_update(delegated=False)

    def claim(self):
        if self.state == DelegationState.Delegated:
            # We are an agent asked to return a pre-reserved "will call" ticket
            # to a client. Set mustSendUpdate so that the update will be sent
            # on the next probe.
            self.must_send_update = True
        elif self.state == DelegationState.Reclaimed:
            self.transition(prefix="claim", state=DelegationState.Delegated)
            self.must_send_update = True
        else:
            self.logger.error(self.error_string_prefix.format(self, self.invalid_state_prefix.format('claim', 'claim')))
            raise DelegationException(self.invalid_state_prefix.format('claim', 'claim'))

    def reclaim(self):
        if self.state == DelegationState.Delegated:
            self.policy.reclaim(delegation=self)
            self.transition(prefix="reclaimed", state=DelegationState.Reclaimed)
        else:
            self.logger.error(self.error_string_prefix.format(self, self.invalid_state_prefix.format('reclaim',
                                                                                                     'reclaim')))
            raise DelegationException(self.invalid_state_prefix.format('reclaim', 'reclaim'))

    def close(self):
        send_notification = False
        if self.state == DelegationState.Nascent:
            self.logger.warning("Closing a reservation in progress")
            send_notification = True

        if self.state != DelegationState.Closed:
            self.transition(prefix="closed", state=DelegationState.Closed)
            self.policy.close_delegation(delegation=self)

        if send_notification:
            self.update_data.error(message="Closed while advertising delegation")
            self.generate_update()

    def incoming_request(self):
        """
        Checks reservation state prior to handling an incoming request. These
        checks are not applied to probes or closes.

        @throws Exception
        """
        assert self.slice_object is not None

        # Disallow any further requests on a closed delegation. Generate and update to reset the client.
        if self.is_closed():
            self.generate_update()
            self.logger.error(self.error_string_prefix.format(self, "server cannot satisfy request closing"))
            raise DelegationException("server cannot satisfy request closing")

    def generate_update(self):
        """
        Generate an update
        """
        self.logger.debug("Generating update")
        if self.callback is None:
            self.logger.warning("Cannot generate update: no callback.")
            return

        self.logger.debug("Generating update: update count={}".format(self.update_count))
        try:
            self.update_count += 1
            self.sequence_out += 1
            RPCManagerSingleton.get().update_delegation(delegation=self)
            self.must_send_update = False
        except Exception as e:
            # Note that this may result in a "stuck" reservation... not much we
            # can do if the receiver has failed or rejects our update. We will
            # regenerate on any user-initiated probe.
            self.logger.error("callback failed, exception={}".format(e))

    def map_and_update(self, *, delegated: bool):
        """
        Call the policy to fill a request, with associated state transitions.
        Catch exceptions and report all errors using callback mechanism.

        @param delegated
                   true if this is delegated (i.e., request is reclaim)
        @return boolean success
        """
        success = False
        granted = False

        if self.state == DelegationState.Nascent:
            if delegated:
                self.fail_notify(message="delegation is not yet delegated")
            else:
                self.logger.debug("Using policy {} to bind delegation".format(self.policy.__class__.__name__))
                try:
                    granted = False
                    # If the policy has processed this reservation, granted should
                    # be set true so that we can send the result back to the
                    # client. If the policy has not yet processed this reservation
                    # (binPending is true) then call the policy. The policy may
                    # choose to process the request immediately (true) or to defer
                    # it (false). In case of a deferred request, we will eventually
                    # come back to this method after the policy has done its job.
                    granted = self.policy.bind_delegation(delegation=self)

                except Exception as e:
                    self.logger.error("map_and_update bind_delegation failed for advertise: {e}")
                    self.fail_notify(message=str(e))
                    return success

                if granted:
                    self.logger.debug("Delegation {} has been granted".format(self.get_delegation_id()))
                    success = True
                    self.transition(prefix="delegated", state=DelegationState.Delegated)
        elif self.state == DelegationState.Delegated:
            if not delegated:
                self.fail_notify(message="delegation is already ticketed")
        else:
            self.logger.error("map_and_update: unexpected state")
            self.fail_notify(message="invalid operation for the current reservation state")

        return success

    def fail_notify(self, *, message: str):
        """
        Notify a failed delegation by generating an update
        @param message message
        """
        self.error_message = message
        self.generate_update()
        self.logger.error(message)

    def service_delegate(self):
        if self.dlg_graph_id is not None:
            # Update the graph
            self.transition(prefix="update absorbed", state=DelegationState.Delegated)
            self.generate_update()

    def prepare_probe(self):
        return

    def probe_pending(self):
        return

    def service_probe(self):
        if self.must_send_update:
            self.generate_update()

    def __str__(self):
        msg = "del: "
        if self.dlg_graph_id is not None:
            msg += "#{} ".format(self.dlg_graph_id)

        if self.slice_object is not None:
            msg += "slice: [{}] ".format(self.slice_object)
        elif self.slice_id is not None:
            msg += "slice_id: [{}] ".format(self.get_slice_id())

        msg += "state:[{}] ".format(self.get_state_name())

        if self.graph is not None:
            msg += "graph:[{}] ".format(self.graph)

        return msg

    def get_callback(self) -> ABCCallbackProxy:
        return self.callback

    def get_update_data(self) -> UpdateData:
        return self.update_data

    def set_sequence_in(self, value: int):
        """
        Set incoming sequence number
        @param value value
        """
        self.sequence_in = value

    def get_sequence_in(self) -> int:
        return self.sequence_in

    def get_sequence_out(self) -> int:
        return self.sequence_out

    def update_delegation(self, *, incoming: ABCDelegation, update_data: UpdateData):
        self.logger.error(self.error_string_prefix.format(self, "Cannot update a authority delegation"))
        raise DelegationException("Cannot update a authority delegation")

    def service_update_delegation(self):
        return

    def validate(self):
        if self.slice_object is None:
            self.logger.error(self.error_string_prefix.format(self, Constants.NOT_SPECIFIED_PREFIX.format("slice")))
            raise DelegationException(Constants.NOT_SPECIFIED_PREFIX.format("slice"))

        if self.dlg_graph_id is None:
            self.logger.error(self.error_string_prefix.format(self, Constants.NOT_SPECIFIED_PREFIX.format("graph id")))
            raise DelegationException(Constants.NOT_SPECIFIED_PREFIX.format("graph id"))

    def validate_incoming(self):
        self.validate()

    def validate_outgoing(self):
        self.validate()

    def set_owner(self, *, owner: AuthToken):
        """
        Set owner
        @param owner owner
        """
        self.owner = owner

    def load_graph(self, *, graph_str: str):
        self.graph = FimHelper.get_graph_from_string_direct(graph_str=graph_str)

    def get_delegation_name(self) -> str:
        return self.delegation_name

    def fail(self, *, message: str, exception: Exception = None):
        """
        Fail a delegation
        """
        self.error_message = message
        self.update_data.error(message=message)
        self.transition(prefix=message, state=DelegationState.Failed)
        self.logger.error(f"{message}  e: {exception}")