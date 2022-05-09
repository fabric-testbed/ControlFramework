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
import traceback
from typing import List

from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import ReservationNotFoundException, DelegationNotFoundException, \
    KernelException
from fabric_cf.actor.core.kernel.authority_reservation import AuthorityReservation
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_server_reservation import ABCServerReservation
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.kernel.request_types import RequestTypes
from fabric_cf.actor.core.kernel.reservation import Reservation
from fabric_cf.actor.core.kernel.reservation_states import ReservationPendingStates, ReservationStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.sequence_comparison_codes import SequenceComparisonCodes
from fabric_cf.actor.core.kernel.slice_state_machine import SliceStateMachine
from fabric_cf.actor.core.kernel.slice_table import SliceTable
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.auth_token import AuthToken


class Kernel:
    def __init__(self, *, plugin: ABCBasePlugin, policy: ABCPolicy, logger):
        # The plugin.
        self.plugin = plugin
        # Policy
        self.policy = policy
        # Logger
        self.logger = logger
        # All slices managed by the kernel
        self.slices = SliceTable()
        # All reservations managed by the kernel.
        self.reservations = ReservationSet()
        self.delegations = {}
        self.lock = threading.Lock()
        self.nothing_pending = threading.Condition()

    def amend_reserve(self, *, reservation: ABCReservationMixin):
        """
        Amends a previous reserve operation (both client and server side) for the
        reservation.
        @param reservation reservation
        @throws Exception
        """
        try:
            reservation.reserve(policy=self.policy)
            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            err = f"An error occurred during amend reserve for reservation #{reservation.get_reservation_id()}"
            self.error(err=err, e=e)

    def amend_delegate(self, *, delegation: ABCDelegation):
        """
        Amends a previous delegate operation for the delegation.
        @param delegation delegation
        @throws Exception
        """
        try:
            delegation.delegate(policy=self.policy)
            self.plugin.get_database().update_delegation(delegation=delegation)
            if not delegation.is_closed():
                delegation.service_delegate()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            err = f"An error occurred during amend delegate for delegation #{delegation.get_delegation_id()}"
            self.error(err=err, e=e)

    def claim_delegation(self, *, delegation: ABCDelegation):
        """
        Processes a requests to claim new ticket for previously exported
        resources (broker role). On the client side this request is issued by
        @param delegation the delegation being claimed
        @throws Exception
        """
        try:
            delegation.claim()
            self.plugin.get_database().update_delegation(delegation=delegation)
        except Exception as e:
            err = f"An error occurred during claim for delegation #{delegation.get_delegation_id()}"
            self.logger.error(traceback.format_exc())
            self.error(err=err, e=e)

    def reclaim_delegation(self, *, delegation: ABCDelegation):
        """
        Processes a requests to claim new ticket for previously exported
        resources (broker role). On the client side this request is issued by
        @param delegation the delegation being claimed
        @throws Exception
        """
        try:
            delegation.reclaim()
            self.plugin.get_database().update_delegation(delegation=delegation)
        except Exception as e:
            err = f"An error occurred during reclaim for delegation #{delegation.get_delegation_id()}"
            self.logger.error(traceback.format_exc())
            self.error(err=err, e=e)

    def fail(self, *, reservation: ABCReservationMixin, message: str):
        """
        Handle a failed reservation
        @param reservation reservation
        @param message message
        """
        if not reservation.is_failed() and not reservation.is_closed():
            reservation.fail(message=message, exception=None)
        self.plugin.get_database().update_reservation(reservation=reservation)

    def fail_delegation(self, *, delegation: ABCDelegation, message: str):
        """
        Handle a failed delegation
        @param delegation delegation
        @param message message
        """
        if not delegation.is_failed() and not delegation.is_closed():
            delegation.fail(message=message, exception=None)
        self.plugin.get_database().update_delegation(delegation=delegation)

    def close(self, *, reservation: ABCReservationMixin):
        """
        Handles a close operation for the reservation.
        Client: perform local close operations and issue close request to
        authority.
        Broker: perform local close operations
        Authority: process a close request
        @param reservation reservation for which to perform close
        @throws Exception
        """
        try:
            if not reservation.is_closed() and not reservation.is_closing():
                self.policy.close(reservation=reservation)
                reservation.close()
                self.plugin.get_database().update_reservation(reservation=reservation)
                reservation.service_close()
        except Exception as e:
            err = f"An error occurred during close for reservation #{reservation.get_reservation_id()}"
            self.logger.error(traceback.format_exc())
            self.error(err=err, e=e)

    @staticmethod
    def compare_and_update(*, incoming: ABCServerReservation, current: ABCServerReservation):
        """
        Compares the incoming request to the corresponding reservation stored at
        this actor. First compares sequence numbers. If the incoming request has
        a larger sequence number and there is no pending operation for this
        reservation, we update the sequence number of the current reservation and
        set the requestedTerm and requestedResources fields.

        @param incoming the incoming request
        @param current the corresponding reservation stored at the server
        @return a comparison status flag (see Sequence*)
        """
        code = SequenceComparisonCodes.SequenceEqual
        if current.get_sequence_in() < incoming.get_sequence_in():
            if current.is_no_pending():
                code = SequenceComparisonCodes.SequenceGreater
                current.set_sequence_in(sequence=incoming.get_sequence_in())
                current.set_requested_resources(resources=incoming.get_requested_resources())
                current.set_requested_term(term=incoming.get_requested_term())
        else:
            if current.get_sequence_in() > incoming.get_sequence_in():
                code = SequenceComparisonCodes.SequenceSmaller
        return code

    @staticmethod
    def compare_and_update_ignore_pending(*, incoming: ABCServerReservation, current: ABCServerReservation):
        """
        Compares the incoming request to the corresponding reservation stored at
        this actor. First compares sequence numbers. If the incoming request has
        a larger sequence number and there is no pending operation for this
        reservation, we update the sequence number of the current reservation and
        set the requestedTerm and requestedResources fields.

        @param incoming the incoming request
        @param current the corresponding reservation stored at the server
        @return a comparison status flag (see Sequence*)
        """
        code = SequenceComparisonCodes.SequenceEqual
        if current.get_sequence_in() < incoming.get_sequence_in():
            code = SequenceComparisonCodes.SequenceGreater
            current.set_sequence_in(sequence=incoming.get_sequence_in())
            current.set_requested_resources(resources=incoming.get_requested_resources())
            current.set_requested_term(term=incoming.get_requested_term())
        else:
            if current.get_sequence_in() > incoming.get_sequence_in():
                code = SequenceComparisonCodes.SequenceSmaller
        return code

    def error(self, *, err: str, e: Exception):
        """
        Logs the specified exception and re-throws it.
        @param err error message
        @param e exception
        """
        self.logger.error(f"Error: {err} Exception: {e}")
        raise e

    def extend_lease(self, *, reservation: ABCReservationMixin):
        """
        Handles an extend lease operation for the reservation.
        Client: issue an extend lease request.
        Authority: process a request for a lease extension.
        @param reservation reservation for which to perform extend lease
        @throws Exception
        """
        try:
            reservation.extend_lease()
            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_extend_lease()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during extend lease for reservation #{reservation.get_reservation_id()}",
                       e=e)

    def modify_lease(self, *, reservation: ABCReservationMixin):
        """
        Handles a modify lease operation for the reservation.
        Client: issue a modify lease request.

        Authority: process a request for a modifying a lease.
        @param reservation reservation for which to perform extend lease
        @throws Exception
        """
        try:
            reservation.modify_lease()
            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_modify_lease()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during modify lease for reservation #{reservation.get_reservation_id()}",
                       e=e)

    def extend_reservation(self, *, rid: ID, resources: ResourceSet, term: Term) -> int:
        """
        Extends the reservation with the given resources and term.
        @param rid reservation identifier of reservation to extend
        @param resources resources to use for the extension
        @param term term to use for the extension
        @return 0 if the reservation extension operation can be initiated,
                if the reservation has a pending operation, which prevents the extend
                operation from being initiated.
        @throws Exception
        """
        real = self.reservations.get(rid=rid)
        ticket = True

        if real is None:
            raise KernelException(f"Unknown reservation rid: {rid}")

        # check for a pending operation: we cannot service the extend if there is another operation in progress.
        if real.get_pending_state() != ReservationPendingStates.None_:
            return Constants.RESERVATION_HAS_PENDING_OPERATION

        # attach the desired extension term and resource set
        real.set_approved(term=term, approved_resources=resources)
        # notify the policy that a reservation is about to be extended
        self.policy.extend(reservation=real, resources=resources, term=term)

        if isinstance(real, AuthorityReservation):
            ticket = False

        if ticket:
            real.extend_ticket(actor=self.plugin.get_actor())
        else:
            real.extend_lease()

        self.plugin.get_database().update_reservation(reservation=real)

        if not real.is_failed():
            if ticket:
                real.service_extend_ticket()
            else:
                real.service_extend_lease()

        return 0

    def extend_ticket(self, *, reservation: ABCReservationMixin):
        """
        Handles an extend ticket operation for the reservation.
        Client: issue an extend ticket request.
        Broker: process a request for a ticket extension.
        @param reservation reservation for which to perform extend ticket
        @throws Exception
        """
        try:
            self.logger.debug(f"Processing extend ticket for reservation={type(reservation)}")
            if reservation.can_renew():
                reservation.extend_ticket(actor=self.plugin.get_actor())
            else:
                raise KernelException("The reservation state prevents it from extending its ticket.")

            self.plugin.get_database().update_reservation(reservation=reservation)

            if not reservation.is_failed():
                reservation.service_extend_ticket()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during extend ticket for reservation #{reservation.get_reservation_id()}",
                       e=e)

    def get_client_slices(self) -> List[ABCSlice]:
        """
        Returns all client slices.
        @return an array of client slices
        """
        return self.slices.get_client_slices()

    def get_inventory_slices(self) -> List[ABCSlice]:
        """
        Returns all inventory slices.
        @return an array of inventory slices
        """
        return self.slices.get_inventory_slices()

    def get_local_slice(self, *, slice_object: ABCSlice) -> ABCSlice:
        """
        Returns the slice object registered with the kernel that corresponds to
        the argument.
        @param slice_object incoming slice object
        @return the locally registered slice object
        @throws IllegalArgumentException if the arguments are invalid
        @throws Exception if no locally registered slice object exists
        """
        if slice_object is None or slice_object.get_slice_id() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        return self.slices.get(slice_id=slice_object.get_slice_id(), raise_exception=True)

    def get_or_create_local_slice(self, *, identity: AuthToken, reservation: ABCReservationMixin,
                                  create_new_slice: bool) -> ABCSlice:
        """
        Returns the slice specified in the reservation or creates a new slice
        with the given parameters. Newly created slices are registered with the
        kernel.
        @param identity actor identity
        @param reservation reservation
        @param create_new_slice true if slice to be created if not found
        @return the slice object
        @throws Exception
        """
        slice_name = reservation.get_slice().get_name()
        slice_id = reservation.get_slice().get_slice_id()
        project_id = reservation.get_slice().get_project_id()
        config_properties = reservation.get_slice().get_config_properties()

        result = self.get_slice(slice_id=slice_id)
        if result is None:
            if create_new_slice:
                result = self.plugin.create_slice(slice_id=slice_id, name=slice_name, project_id=project_id)
                result.set_config_properties(value=config_properties)
                if reservation.get_slice().is_broker_client():
                    result.set_broker_client()
                else:
                    if reservation.get_slice().is_client():
                        result.set_client()
            else:
                result = reservation.get_kernel_slice()

            result.set_owner(owner=identity)
            self.register_slice(slice_object=result)
        return result

    def get_reservation(self, *, rid: ID) -> ABCReservationMixin:
        """
        Returns the specified reservation.
        @param rid reservation id
        @return reservation
        """
        if rid is not None:
            return self.reservations.get(rid=rid)
        return None

    def get_reservations(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        """
        Returns all reservations in the specified slice.
        @param slice_id slice id
        @return an array of reservations
        """
        result = None
        if slice_id is not None:
            sl = self.slices.get(slice_id=slice_id)
            result = sl.get_reservations_list()
        return result

    def get_delegation(self, *, did: str) -> ABCDelegation:
        """
        Returns the specified delegation.
        @param did delegation id
        @return delegation
        """
        try:
            if did is not None:
                self.lock.acquire()
                return self.delegations.get(did, None)
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def get_plugin(self) -> ABCBasePlugin:
        """
        Returns the plugin.
        @return plugin object
        """
        return self.plugin

    def get_slice(self, *, slice_id: ID) -> ABCSlice:
        """
        Returns a slice previously registered with the kernel.
        @param slice_id slice identifier
        @return slice object
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        return self.slices.get(slice_id=slice_id)

    def is_known_slice(self, *, slice_id: ID) -> bool:
        """
        Is slice known
        @param slice_id slice id
        @return true if slice exists; false otherwise
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        return self.slices.contains(slice_id=slice_id)

    def get_slices(self) -> list:
        """
        Returns all registered slices.
        @return an array of slices
        """
        return self.slices.get_slices()

    @staticmethod
    def handle_duplicate_request(*, current: ABCReservationMixin, operation: RequestTypes):
        """
        Handles a duplicate request.
        @param current reservation
        @param operation operation code
        """
        current.handle_duplicate_request(operation=operation)

    def probe_pending_slices(self, *, slice_obj: ABCSlice):
        """
        Probes to check for completion of pending operation.
        @param slice_obj the slice_obj being probed
        @throws Exception rare
        """
        try:
            if slice_obj.is_broker_client():
                return
            state_changed, slice_state = slice_obj.transition_slice(operation=SliceStateMachine.REEVALUATE)
            if state_changed:
                slice_obj.set_dirty()
            self.plugin.get_database().update_slice(slice_object=slice_obj)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during probe pending for slice_obj #{slice_obj.get_slice_id()}", e=e)

    def probe_pending(self, *, reservation: ABCReservationMixin):
        """
        Probes to check for completion of pending operation.
        @param reservation the reservation being probed
        @throws Exception rare
        """
        try:
            reservation.prepare_probe()
            reservation.probe_pending()
            self.plugin.get_database().update_reservation(reservation=reservation)
            reservation.service_probe()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during probe pending for reservation #{reservation.get_reservation_id()}",
                       e=e)

    def probe_pending_delegation(self, *, delegation: ABCDelegation):
        """
        Probes to check for completion of pending operation.
        @param delegation the delegation being probed
        @throws Exception rare
        """
        try:
            delegation.prepare_probe()
            delegation.probe_pending()
            self.plugin.get_database().update_delegation(delegation=delegation)
            delegation.service_probe()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during probe pending for delegation #{delegation.get_delegation_id()}",
                       e=e)

    def purge(self):
        """
        Purges all closed reservations.
        @throws Exception
        """
        for reservation in self.reservations.values():
            if reservation.is_closed():
                try:
                    reservation.get_kernel_slice().unregister(reservation=reservation)
                except Exception as e:
                    self.logger.error(f"An error occurred during purge for "
                                      f"reservation #{reservation.get_reservation_id()} e: {e}")
                finally:
                    self.reservations.remove(reservation=reservation)

        try:
            self.lock.acquire()
            delegations_to_be_removed = []
            for delegation in self.delegations.values():
                if delegation.is_closed():
                    try:
                        delegation.get_slice_object().unregister_delegation(delegation=delegation)
                    except Exception as e:
                        self.logger.error(f"An error occurred during purge for "
                                          f"delegation #{delegation.get_delegation_id()} e:{e}")
                    finally:
                        delegations_to_be_removed.append(delegation.get_delegation_id())

            for d in delegations_to_be_removed:
                if d in self.delegations:
                    self.delegations.pop(d)
        finally:
            self.lock.release()

    def query(self, *, properties: dict):
        """
        Processes a query request.
        @param properties query
        @return query response
        """
        return self.policy.query(p=properties)

    def redeem(self, *, reservation: ABCClientReservation):
        """
        Redeem a reservation
        @param reservation reservation
        """
        try:
            if reservation.can_redeem():
                reservation.reserve(policy=self.policy)
            else:
                raise KernelException("The current reservation state prevent it from being redeemed")

            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            self.logger.error(f"An error occurred during redeem for reservation #{reservation.get_reservation_id()} "
                              f"e: {e}")

    def register(self, *, reservation: ABCReservationMixin, slice_object: ABCSlice) -> bool:
        """
        Registers a new reservation with its slice and the kernel reservation
        table. Must be called with the kernel lock on.
        @param reservation local reservation object
        @param slice_object local slice object. The slice must have previously been
                   registered with the kernel.
        @return true if the reservation was registered, false otherwise
        @throws Exception
        """
        add = False
        reservation.set_logger(logger=self.logger)

        if not reservation.is_closed():
            # Note: as of now slice.register must be the first operation in
            # this method. slice.register will throw an exception if the
            # reservation is already present in the slice table.

            # register with the local slice
            slice_object.register(reservation=reservation)

            # register with the reservations table
            if self.reservations.contains(reservation=reservation):
                slice_object.unregister(reservation=reservation)
                raise KernelException("There is already a reservation with the given identifier")

            self.reservations.add(reservation=reservation)

            # attach actor to the reservation
            reservation.set_actor(actor=self.plugin.get_actor())
            # attach the local slice object
            reservation.set_slice(slice_object=slice_object)
            add = True
        else:
            self.logger.warning(f"Attempting to register a closed reservation #{reservation.get_reservation_id()}")

        return add

    def register_delegation_with_slice(self, *, delegation: ABCDelegation, slice_object: ABCSlice) -> bool:
        """
        Registers a new delegation with its slice and the kernel delegation
        table. Must be called with the kernel lock on.
        @param delegation local delegation object
        @param slice_object local slice object. The slice must have previously been
                   registered with the kernel.
        @return true if the delegation was registered, false otherwise
        @throws Exception
        """
        add = False

        if not delegation.is_closed():
            # Note: as of now slice.register must be the first operation in
            # this method. slice.register will throw an exception if the
            # delegation is already present in the slice table.

            # register with the local slice
            slice_object.register_delegation(delegation=delegation)

            try:
                self.lock.acquire()
                # register with the delegation table
                if delegation.get_delegation_id() in self.delegations:
                    slice_object.unregister_delegation(delegation=delegation)
                    raise KernelException("There is already a delegation with the given identifier")

                self.delegations[delegation.get_delegation_id()] = delegation
            finally:
                self.lock.release()

            # attach actor to the reservation
            delegation.set_actor(actor=self.plugin.get_actor())
            # attach the local slice object
            delegation.set_slice_object(slice_object=slice_object)
            add = True
        else:
            self.logger.warning(f"Attempting to register a closed delegation #{delegation.get_delegation_id()}")

        return add

    def register_delegation(self, *, delegation: ABCDelegation):
        """
        Re-registers the delegation.
        @param delegation delegation
        @throws Exception
        """
        if delegation is None or delegation.get_delegation_id() is None or \
                delegation.get_slice_object() is None or delegation.get_slice_object().get_name() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        local_slice = None
        add = False

        local_slice = self.slices.get(slice_id=delegation.get_slice_id(), raise_exception=True)
        add = self.register_delegation_with_slice(delegation=delegation, slice_object=local_slice)

        if add:
            try:
                self.plugin.get_database().add_delegation(delegation=delegation)
            except Exception as e:
                self.unregister_no_check_d(delegation=delegation, slice_object=local_slice)
                raise e

    def register_reservation(self, *, reservation: ABCReservationMixin):
        """
        Re-registers the reservation.
        @param reservation reservation
        @throws Exception
        """
        if reservation is None or reservation.get_reservation_id() is None or \
                reservation.get_slice() is None or reservation.get_slice().get_name() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        local_slice = None
        add = False

        local_slice = self.slices.get(slice_id=reservation.get_slice().get_slice_id(), raise_exception=True)
        add = self.register(reservation=reservation, slice_object=local_slice)

        if add:
            try:
                self.plugin.get_database().add_reservation(reservation=reservation)
            except Exception as e:
                self.unregister_no_check(reservation=reservation, slice_object=local_slice)
                raise e

    def register_slice(self, *, slice_object: ABCSlice):
        """
        Registers the specified slice with the kernel.
        @param slice_object slice to register
        @throws Exception if the slice cannot be registered
        """
        slice_object.prepare()
        self.slices.add(slice_object=slice_object)

        try:
            self.plugin.get_database().add_slice(slice_object=slice_object)
        except Exception as e:
            self.slices.remove(slice_id=slice_object.get_slice_id())
            self.logger.error(traceback.format_exc())
            self.error(err="could not register slice", e=e)

    def remove_reservation(self, *, rid: ID):
        """
        Removes the reservation.
        @param rid reservation id.
        @throws Exception
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        real = self.reservations.get(rid=rid)

        if real is not None:
            if real.is_closed() or real.is_failed() or real.get_state() == ReservationStates.CloseWait:
                self.unregister_reservation(rid=rid)
            else:
                raise KernelException("Only reservations in failed, closed, or closewait state can be removed.")
        else:
            self.logger.debug(f"Reservation # {rid} not found")

        self.plugin.get_database().remove_reservation(rid=rid)
        self.logger.debug(f"Reservation # {rid} removed from DB")

    def remove_slice(self, *, slice_id: ID):
        """
        Removes the specified slice.
        @param slice_id slice identifier
        @throws Exception if the slice contains active reservations or removal fails
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        possible = False
        slice_object = self.get_slice(slice_id=slice_id)

        if slice_object is None:
            self.logger.debug("Slice object not found in local data structure, removing from database")
            self.plugin.get_database().remove_slice(slice_id=slice_id)
        else:
            possible = (slice_object.get_reservations().size() == 0)

            if possible:
                # remove the slice from the slices table
                self.slices.remove(slice_id=slice_id)
                # release any resources assigned to the slice: unlocked,
                # because it may be blocking. The plugin is responsible for
                # synchronization.
                self.plugin.release_slice(slice_obj=slice_object)
                # remove from the database
                self.plugin.get_database().remove_slice(slice_id=slice_id)

    def re_register_delegation(self, *, delegation: ABCDelegation):
        """
        Re-registers the delegation.
        @param delegation delegation
        @throws Exception
        """
        if delegation is None or delegation.get_delegation_id() is None or \
                delegation.get_slice_object() is None or delegation.get_slice_object().get_slice_id() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        local_slice = None
        local_slice = self.slices.get(slice_id=delegation.get_slice_object().get_slice_id(), raise_exception=True)

        if local_slice is None:
            raise KernelException("slice not registered with the kernel")
        else:
            self.register_delegation_with_slice(delegation=delegation, slice_object=local_slice)

        # Check if the delegation has a database record.
        temp = self.plugin.get_database().get_delegation(dlg_graph_id=delegation.get_delegation_id())

        if temp is None:
            self.unregister_no_check_d(delegation=delegation, slice_object=local_slice)
            raise KernelException("The delegation has no database record")

    def re_register_reservation(self, *, reservation: ABCReservationMixin):
        """
        Re-registers the reservation.
        @param reservation reservation
        @throws Exception
        """
        if reservation is None or reservation.get_reservation_id() is None or \
                reservation.get_slice() is None or reservation.get_slice().get_slice_id() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        local_slice = None
        local_slice = self.slices.get(slice_id=reservation.get_slice().get_slice_id(), raise_exception=True)

        if local_slice is None:
            raise KernelException("slice not registered with the kernel")
        else:
            self.register(reservation=reservation, slice_object=local_slice)

        # Check if the reservation has a database record.
        temp = None
        temp = self.plugin.get_database().get_reservations(rid=reservation.get_reservation_id())

        if temp is None or len(temp) == 0:
            self.unregister_no_check(reservation=reservation, slice_object=local_slice)
            raise KernelException("The reservation has no database record")

    def re_register_slice(self, *, slice_object: ABCSlice):
        """
        Re-registers the specified slice with the kernel.
        @param slice_object slice to re-register
        @throws Exception if the slice cannot be registered
        """
        slice_object.prepare()
        self.slices.add(slice_object=slice_object)

        try:
            slices = self.plugin.get_database().get_slices(slice_id=slice_object.get_slice_id())
            if slices is None or len(slices) == 0:
                raise KernelException("The slice does not have a database record")
        except Exception as e:
            self.slices.remove(slice_id=slice_object.get_slice_id())
            self.logger.error(traceback.format_exc())
            self.error(err="could not re-register slice", e=e)

    def reserve(self, *, reservation: ABCReservationMixin):
        """
        Handles a reserve operation for the reservation.
        Client: issue a ticket request or a claim request.
        Broker: process a request for a new ticket. Claims for previously
        exported tickets are handled by #claim(BrokerReservation).
        Authority: process a request for a new lease.
        @param reservation reservation for which to perform redeem
        @throws Exception
        """
        try:
            reservation.reserve(policy=self.policy)
            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during reserve for reservation #{reservation.get_reservation_id()}", e=e)

    def delegate(self, *, delegation: ABCDelegation):
        """
        Handles a delegate operation for the delegation.
        Broker: process a request for a new delegate.
        Authority: process a request for a new delegate.
        @param delegation delegation
        @throws Exception
        """
        try:
            delegation.delegate(policy=self.policy)
            self.plugin.get_database().update_delegation(delegation=delegation)
            if not delegation.is_closed():
                delegation.service_delegate()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during delegate for delegation #{delegation.get_delegation_id()}", e=e)

    def soft_validate_delegation(self, *, delegation: ABCDelegation = None, did: str = None) -> ABCDelegation:
        """
        Retrieves the locally registered delegation that corresponds to the
        passed delegation. Obtains the reservation from the containing slice
        object.
        @param delegation delegation being validated
        @param did delegation identifier or reservation being validated
        @return the locally registered did that corresponds to the passed
                delegation or null if no local did exists
        @throws Exception if the slice referenced by the incoming delegation is
                    not locally registered
        """
        if delegation is None and did is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        result = None

        if delegation is not None:
            # Each local delegation is indexed in two places: (1) The delegation
            # set in the kernel, and (2) inside the local slice object. Here we
            # will check to see if the local slice exists and will retrieve the
            # delegation from the local slice.
            s = self.get_local_slice(slice_object=delegation.get_slice_object())

            result = s.soft_lookup_delegation(did=delegation.get_delegation_id())

        if did is not None:
            try:
                self.lock.acquire()
                result = self.delegations.get(did, None)
            finally:
                self.lock.release()

        return result

    def soft_validate(self, *, reservation: ABCReservationMixin = None, rid: ID = None) -> ABCReservationMixin:
        """
        Retrieves the locally registered reservation that corresponds to the
        passed reservation. Obtains the reservation from the containing slice
        object.
        @param reservation reservation being validated
        @param rid reservation identifier or reservation being validated
        @return the locally registered reservation that corresponds to the passed
                reservation or null if no local reservation exists
        @throws IllegalArgumentException if the arguments are invalid
        @throws Exception if the slice referenced by the incoming reservation is
                    not locally registered
        """
        if reservation is None and rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        result = None

        if reservation is not None:
            # Each local reservation is indexed in two places: (1) The reservation
            # set in the kernel, and (2) inside the local slice object. Here we
            # will check to see if the local slice exists and will retrieve the
            # reservation from the local slice.
            s = self.get_local_slice(slice_object=reservation.get_slice())

            result = s.soft_lookup(rid=reservation.get_reservation_id())

        if rid is not None:
            result = self.reservations.get(rid=rid)

        return result

    def tick(self):
        """
        Timer interrupt.
        @throws Exception
        """
        try:
            try:
                self.lock.acquire()
                for delegation in self.delegations.values():
                    self.probe_pending_delegation(delegation=delegation)
            finally:
                self.lock.release()

            for reservation in self.reservations.values():
                self.probe_pending(reservation=reservation)

            try:
                self.lock.acquire()
                for slice_obj in self.slices.get_client_slices():
                    self.probe_pending_slices(slice_obj=slice_obj)
            finally:
                self.lock.release()

            self.purge()
            self.check_nothing_pending()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err="exception in Kernel.tick", e=e)

    def has_something_pending(self) -> bool:
        """
        Check is kernel has any pending reservations
        @return true if no terminal/nascent/pending reservations exist; false otherwise
        """
        for reservation in self.reservations.values():
            if not reservation.is_terminal() and (reservation.is_nascent() or not reservation.is_no_pending()):
                return True

        return False

    def check_nothing_pending(self):
        """
        Check for pending reservations
        """
        if not self.has_something_pending():
            with self.nothing_pending:
                self.nothing_pending.notify_all()

    def await_nothing_pending(self):
        """
        Await until nothing is pending
        """
        with self.nothing_pending:
            self.nothing_pending.wait()

    def unregister(self, *, reservation: ABCReservationMixin, slice_object: ABCSlice):
        """
        Unregisters a reservation from the kernel data structures. Must be called
        with the kernel lock on. Performs state checks.
        @param reservation reservation to unregister
        @param slice_object local slice object
        @throws Exception
        """
        if reservation.is_closed() or reservation.is_failed() or reservation.get_state() == ReservationStates.CloseWait:
            slice_object.unregister(reservation=reservation)
            self.reservations.remove(reservation=reservation)
        else:
            raise KernelException("Only reservations in failed, closed, or closewait state can be unregistered.")

    def unregister_no_check(self, *, reservation: ABCReservationMixin, slice_object: ABCSlice):
        """
        Unregisters a reservation from the kernel data structures. Must be called
        with the kernel lock on. Does not perform state checks.
        @param reservation reservation to unregister
        @param slice_object local slice object
        @throws Exception
        """
        slice_object.unregister(reservation=reservation)
        self.reservations.remove(reservation=reservation)

    def unregister_no_check_d(self, *, delegation: ABCDelegation, slice_object: ABCSlice):
        """
        Unregisters a delegation from the kernel data structures. Must be called
        with the kernel lock on. Does not perform state checks.
        @param delegation delegation to unregister
        @param slice_object local slice object
        @throws Exception
        """
        slice_object.unregister_delegation(delegation=delegation)
        try:
            self.lock.acquire()
            if delegation.get_delegation_id() in self.delegations:
                self.delegations.pop(delegation.get_delegation_id())
        finally:
            if self.lock.locked():
                self.lock.release()

    def unregister_reservation(self, *, rid: ID):
        """
        Unregisters the reservation
        @param rid reservation id
        @throws Exception
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        local_reservation = self.reservations.get_exception(rid=rid)
        self.unregister(reservation=local_reservation, slice_object=local_reservation.get_kernel_slice())

        self.policy.remove(reservation=local_reservation)

    def unregister_slice(self, *, slice_id: ID):
        """
        Unregisters the specified slice.
        @param slice_id slice id
        @throws Exception if the slice cannot be unregistered (it has active
                    reservations) or has not been previously registered with the
                    kernel
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        s = self.get_slice(slice_id=slice_id)

        if s is None:
            raise KernelException("Trying to unregister a slice, which is not registered with the kernel")

        if s.get_reservations().size() == 0:
            self.slices.remove(slice_id=slice_id)
            self.plugin.release_slice(slice_obj=s)
        else:
            raise KernelException("Slice cannot be unregistered: not empty")

    def update_lease(self, *, reservation: ABCReservationMixin, update: Reservation, update_data: UpdateData):
        """
        Handles an incoming update lease operation (client side only).
        @param reservation local reservation
        @param update update sent from site authority
        @param update_data status of the operation the authority is informing us about
        @throws Exception
        """
        try:
            self.logger.debug(f"update_lease: Incoming term {update.get_term()}")
            reservation.update_lease(incoming=update, update_data=update_data)

            # NOTE: the database update has to happen BEFORE the service
            # update: we need to record the fact that we received a concrete
            # set, so that on recovery we can go and recover the concrete set.
            # If the database update is after the service call, we may end up
            # in Ticketed, Redeeming with state in the database that will
            # prevent us from incorporating a leaseUpdate.

            self.plugin.get_database().update_reservation(reservation=reservation)

            if not reservation.is_failed():
                reservation.service_update_lease()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during update lease for reservation "
                           f"# {reservation.get_reservation_id()}", e=e)

    def update_ticket(self, *, reservation: ABCReservationMixin, update: Reservation, update_data: UpdateData):
        """
        Handles an incoming update ticket operation (client side only).
        @param reservation local reservation
        @param update update sent from upstream broker
        @param update_data status of the operation the broker is informing us about
        @throws Exception
        """
        try:
            reservation.update_ticket(incoming=update, update_data=update_data)
            self.plugin.get_database().update_reservation(reservation=reservation)
            if not reservation.is_failed():
                reservation.service_update_ticket()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during update ticket for "
                           f"reservation # {reservation.get_reservation_id()}", e=e)

    def update_delegation(self, *, delegation: ABCDelegation, update: ABCDelegation, update_data: UpdateData):
        """
        Handles an incoming update delegation operation (client side only).
        @param delegation local delegation
        @param update update sent from upstream broker
        @param update_data status of the operation the broker is informing us about
        @throws Exception
        """
        try:
            delegation.update_delegation(incoming=update, update_data=update_data)
            self.plugin.get_database().update_delegation(delegation=delegation)
            delegation.service_update_delegation()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.error(err=f"An error occurred during update delegation for "
                           f"delegation # {delegation.get_delegation_id()}", e=e)

    def validate(self, *, reservation: ABCReservationMixin = None, rid: ID = None):
        """
        Retrieves the locally registered reservation that corresponds to the
        passed reservation. Obtains the reservation from the containing slice
        object.
        @param reservation reservation being validated
        @param rid reservation id
        @return the locally registered reservation that corresponds to the passed
                reservation
        @throws Exception if there is no local reservation that corresponds to
                    the passed reservation
        """
        if (reservation is not None and rid is not None) or (reservation is None and rid is None):
            raise KernelException(Constants.INVALID_ARGUMENT)

        if reservation is not None:
            local = self.soft_validate(reservation=reservation)
            if local is None:
                self.error(err="reservation not found", e=ReservationNotFoundException(rid=rid))
            return local

        if rid is not None:
            local = self.soft_validate(rid=rid)
            if local is None:
                self.error(err="reservation not found", e=ReservationNotFoundException(rid=rid))
            return local

        return None

    @staticmethod
    def handle_failed_rpc(*, reservation: ABCReservationMixin, rpc: FailedRPC):
        """
        Handle failed rpc
        @param reservation reservation
        @param rpc rpc
        """
        reservation.handle_failed_rpc(failed=rpc)

    def validate_delegation(self, *, delegation: ABCDelegation = None, did: str = None):
        """
        Retrieves the locally registered delegation that corresponds to the
        passed delegation. Obtains the delegation from the containing slice
        object.
        @param delegation delegation being validated
        @param did delegation id
        @return the locally registered delegation that corresponds to the passed
                delegation
        @throws Exception if there is no local delegation that corresponds to
                    the passed delegation
        """
        if (delegation is not None and did is not None) or (delegation is None and did is None):
            raise KernelException(Constants.INVALID_ARGUMENT)

        if delegation is not None:
            local = self.soft_validate_delegation(delegation=delegation)
            if local is None:
                self.error(err="delegation not found", e=DelegationNotFoundException(did=did))
            return local

        if did is not None:
            local = self.soft_validate_delegation(did=did)
            if local is None:
                self.error(err="delegation not found", e=DelegationNotFoundException(did=did))
            return local

        return None
