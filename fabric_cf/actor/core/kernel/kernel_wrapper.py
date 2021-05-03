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
from typing import List

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
from fabric_cf.actor.core.apis.abc_actor_identity import ABCActorIdentity
from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
from fabric_cf.actor.core.apis.abc_authority_reservation import ABCAuthorityReservation
from fabric_cf.actor.core.apis.abc_broker_reservation import ABCBrokerReservation
from fabric_cf.actor.core.apis.abc_client_callback_proxy import ABCClientCallbackProxy
from fabric_cf.actor.core.apis.abc_client_reservation import ABCClientReservation
from fabric_cf.actor.core.apis.abc_controller_callback_proxy import ABCControllerCallbackProxy
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_policy import ABCPolicy
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import SliceNotFoundException
from fabric_cf.actor.core.kernel.failed_rpc import FailedRPC
from fabric_cf.actor.core.apis.abc_kernel_client_reservation_mixin import ABCKernelClientReservationMixin
from fabric_cf.actor.core.apis.abc_kernel_reservation import ABCKernelReservation
from fabric_cf.actor.core.apis.abc_kernel_slice import ABCKernelSlice
from fabric_cf.actor.core.kernel.kernel import Kernel
from fabric_cf.actor.core.common.exceptions import KernelException
from fabric_cf.actor.core.kernel.request_types import RequestTypes
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.kernel.sequence_comparison_codes import SequenceComparisonCodes
from fabric_cf.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.update_data import UpdateData
from fabric_cf.actor.security.access_checker import AccessChecker
from fabric_cf.actor.security.auth_token import AuthToken
from fabric_cf.actor.security.pdp_auth import ActionId, ResourceType


class KernelWrapper:
    """
    KernelWrapper is responsible for validating the arguments to internal kernel methods before
    invoking these calls. The internal kernel methods can only be invoked through
    an instance of the kernel wrapper.
    """
    def __init__(self, *, actor: ABCActorMixin, plugin: ABCBasePlugin, policy: ABCPolicy):
        # The actor linked by the wrapper to the kernel.
        self.actor = actor
        # The kernel instance.
        self.kernel = Kernel(plugin=plugin, policy=policy, logger=actor.get_logger())
        # Logger.
        self.logger = actor.get_logger()

    def await_nothing_pending(self):
        """
        Blocks until there are no more reservations in a pending state.
        @throws InterruptedException in case of error
        """
        self.kernel.await_nothing_pending()

    def claim_delegation_request(self, *, delegation: ABCDelegation, caller: AuthToken,
                                 callback: ABCClientCallbackProxy, id_token: str = None):
        """
        Processes a request to claim a pre-advertised delegation

        Role: Broker

        @param delegation delegation describing the claim request
        @param caller caller identity
        @param callback callback proxy
        @param id_token identity token
        @raises Exception in case of error
        """
        if delegation is None or caller is None or callback is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if id_token is not None:
            AccessChecker.check_access(action_id=ActionId.claim, resource_type=ResourceType.delegation,
                                       token=id_token, logger=self.logger, actor_type=self.actor.get_type())

        # Note: for claim we do not need the slice object, so we use
        # validate_delegation(delegation_id) instead of validate_delegation(delegation).
        exported = self.kernel.validate_delegation(did=delegation.get_delegation_id())
        exported.prepare(callback=callback, logger=self.logger)
        self.kernel.claim_delegation(delegation=exported)

    def reclaim_delegation_request(self, *, delegation: ABCDelegation, caller: AuthToken,
                                   callback: ABCClientCallbackProxy, id_token: str = None):
        """
        Processes a request to re-claim a pre-claimed delegation

        Role: Broker

        @param delegation delegation describing the reclaim request
        @param caller caller identity
        @param callback callback proxy
        @param id_token identity token
        @raises Exception in case of error
        """
        if delegation is None or caller is None or callback is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if id_token is not None:
            AccessChecker.check_access(action_id=ActionId.claim, resource_type=ResourceType.delegation,
                                       token=id_token, logger=self.logger, actor_type=self.actor.get_type())

        # Note: for claim we do not need the slice object, so we use
        # validate(ReservationID) instead of validate(Reservation).
        exported = self.kernel.validate_delegation(did=delegation.get_delegation_id())
        exported.prepare(callback=callback, logger=self.logger)
        self.kernel.reclaim_delegation(delegation=exported, id_token=id_token)

    def fail(self, *, rid: ID, message: str):
        """
        Fails the specified reservation.
        @param rid reservation id
        @param message message
        @throws Exception in case of error
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=rid)
        self.kernel.fail(reservation=target, message=message)

    def fail_delegation(self, *, did: str, message: str):
        """
        Fails the specified fail_delegation.
        @param did fail_delegation id
        @param message message
        @throws Exception in case of error
        """
        if did is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate_delegation(did=did)
        self.kernel.fail_delegation(delegation=target, message=message)

    def close(self, *, rid: ID):
        """
        Closes the reservation, potentially initiating a close request to another
        actor. If the reservation has concrete resources bound to it, this method
        may return before all close operations have completed. Check the
        reservation state to determine when close completes.
        @param rid identifier of reservation to close
        @throws Exception in case of error
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        target = self.kernel.validate(rid=rid)
        # NOTE: this call does not require access control check, since
        # it is executed in the context of the actor represented by KernelWrapper.
        self.kernel.close(reservation=target)

    def close_slice_reservations(self, *, slice_id: ID):
        """
        Close Slice reservations
        @param slice_id slice id
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if not self.kernel.is_known_slice(slice_id=slice_id):
            raise SliceNotFoundException(slice_id=str(slice_id))

        reservations = self.get_reservations(slice_id=slice_id)
        for r in reservations:
            try:
                self.kernel.close(reservation=r)
            except Exception as e:
                self.logger.error("Error during close: {}".format(e))

    def close_request(self, *, reservation: ABCReservationMixin, caller: AuthToken, compare_sequence_numbers: bool):
        """
        Processes an incoming request to close a reservation.
        Role: Authority
        @param reservation reservation to close.
        @param caller caller identity
        @param compare_sequence_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        if reservation is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if compare_sequence_numbers:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            sequence_number_compare = self.kernel.compare_and_update_ignore_pending(incoming=reservation,
                                                                                    current=target)
            if sequence_number_compare == SequenceComparisonCodes.SequenceGreater:
                self.kernel.close(reservation=target)
            elif sequence_number_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("closeRequest with a smaller sequence number")
            elif sequence_number_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate closeRequest")
                self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestClose)
        else:
            target = self.kernel.validate(reservation=reservation)
            self.kernel.close(reservation=target)

    def advertise(self, *, delegation: ABCDelegation, client: AuthToken):
        """
        Initiates a ticket export.
        Role: Broker or Authority
        Prepare/hold a ticket for "will call" claim by a client.
        @param delegation reservation to be exported
        @param client client identity
        @throws Exception in case of error
        """
        if delegation is None or delegation.get_slice_object() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        delegation.prepare(callback=None, logger=self.logger)
        self.handle_delegate(delegation=delegation, identity=client)

    def extend_lease(self, *, reservation: ABCControllerReservation):
        """
        Initiates a request to extend a lease.
        Role: Controller/Orchestrator
        @param reservation reservation describing the extend request
        @raises Exception in case of error
        """
        if reservation is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("extendLease for a reservation not registered with the kernel")
            return

        target.validate_redeem()
        self.kernel.extend_lease(reservation=reservation)

    def extend_lease_request(self, *, reservation: ABCAuthorityReservation, caller: AuthToken,
                             compare_sequence_numbers: bool):
        """
        Processes an incoming request for a lease extension.
        Role: Authority
        @param reservation reservation representing the lease extension request.
        @param caller caller identity
        @param compare_sequence_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        if reservation is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if compare_sequence_numbers:
            reservation.validate_incoming()
            target = self.kernel.validate(rid=reservation.get_reservation_id())

            sequence_compare = self.kernel.compare_and_update(incoming=reservation, current=target)
            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                target.prepare_extend_lease()
                self.kernel.extend_lease(reservation=target)

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("extendLeaseRequest with a smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate extendLease request")
                self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestExtendLease)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            self.kernel.extend_lease(reservation=target)

    def modify_lease_request(self, *, reservation: ABCAuthorityReservation, caller: AuthToken,
                             compare_sequence_numbers: bool):
        """
        Processes an incoming request for a lease modification.
        Role: Authority
        @param reservation reservation representing the lease modification request.
        @param caller caller identity
        @param compare_sequence_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        if reservation is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        if compare_sequence_numbers:
            reservation.validate_incoming()
            target = self.kernel.validate(rid=reservation.get_reservation_id())

            sequence_compare = self.kernel.compare_and_update(incoming=reservation, current=target)

            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                target.prepare_modify_lease()
                self.kernel.modify_lease(reservation=reservation)

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("modifyLeaseRequest with a smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate extendLease request")
                self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestModifyLease)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            self.kernel.modify_lease(reservation=target)

    def extend_reservation(self, *, rid: ID, resources: ResourceSet, term: Term) -> int:
        """
        Extends the reservation with the given resources and term.
        @param rid identifier of reservation to extend
        @param resources resources for extension
        @param term term for extension
        @return 0 on success, a negative exit code on error
        @throws Exception in case of error
        """
        if rid is None or resources is None or term is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        return self.kernel.extend_reservation(rid=rid, resources=resources, term=term)

    def extend_ticket(self, *, reservation: ABCClientReservation):
        """
        Initiates a request to extend a ticket.
        Role: Broker or Controller
        @param reservation reservation describing the ticket extension request
        @throws Exception in case of error
        """
        if reservation is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            raise KernelException("extendTicket on a reservation not registered with the kernel")

        target.validate_outgoing()
        self.kernel.extend_ticket(reservation=target)

    def extend_ticket_request(self, *, reservation: ABCBrokerReservation, caller: AuthToken,
                              compare_sequence_numbers: bool):
        """
        Processes an incoming request for a ticket extension.
        Role: Broker
        @param reservation reservation representing the ticket extension request.
        @param caller caller identity
        @param compare_sequence_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        self.logger.debug("extend_ticket_request")
        if reservation is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        if compare_sequence_numbers:
            target = self.kernel.validate(rid=reservation.get_reservation_id())

            sequence_compare = self.kernel.compare_and_update(incoming=reservation, current=target)

            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                self.logger.debug("extend_ticket SequenceGreater")
                self.kernel.extend_ticket(reservation=target)

            elif sequence_compare == SequenceComparisonCodes.SequenceInProgress:
                self.logger.warning("New request for a reservation with a pending action")

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("Incoming extendTicket request has smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("Duplicate extendTicket request")
                self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestExtendTicket)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            self.logger.debug("extend_ticket No sequence number comparison")
            self.extend_ticket(reservation=target)

    def modify_lease(self, *, reservation: ABCControllerReservation):
        """
        Initiates a request to modify a lease.
        Role: Controller
        @param reservation reservation describing the modify request
        @throws Exception in case of error
        """
        if reservation is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("modifyLease for a reservation not registered with the kernel")

        target.validate_redeem()
        self.kernel.modify_lease(reservation=target)

    def relinquish_request(self, *, reservation: ABCBrokerReservation, caller: AuthToken):
        if reservation is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        reservation.validate_incoming()

        target = self.kernel.soft_validate(rid=reservation.get_reservation_id())
        if target is None:
            self.logger.info("Relinquish for non-existent reservation. Reservation has already been closed. "
                             "Nothing to relinquish")
            return

        sequence_compare = self.kernel.compare_and_update(incoming=reservation, current=target)

        if sequence_compare == SequenceComparisonCodes.SequenceGreater or \
                sequence_compare == SequenceComparisonCodes.SequenceInProgress:
            self.kernel.close(reservation=target)

        elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
            self.logger.warning("Incoming relinquish request has smaller sequence number")

        elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
            self.logger.warning("Duplicate relinquish request")
            self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestRelinquish)

    def get_client_slices(self) -> List[ABCSlice]:
        """
        Returns all client slices registered with the kernel.
        @return an array of client slices registered with the kernel
        """
        return self.kernel.get_client_slices()

    def get_inventory_slices(self) -> List[ABCSlice]:
        """
        Returns all inventory slices registered with the kernel.
        @return an array of inventory slices registered with the kernel
        """
        return self.kernel.get_inventory_slices()

    def get_delegation(self, *, did: str) -> ABCDelegation:
        """
        Returns the delegation with the given delegation identifier.
        @param did delegation identifier
        @return delegation with the given delegation identifier
        """
        if did is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        return self.kernel.get_delegation(did=did)

    def get_reservation(self, *, rid: ID) -> ABCReservationMixin:
        """
        Returns the reservation with the given reservation identifier.
        @param rid reservation identifier
        @return reservation with the given reservation identifier
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        return self.kernel.get_reservation(rid=rid)

    def get_reservations(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        """
        Returns all reservations in the given slice
        @param slice_id identifier of slice
        @return an array of all reservations in the slice
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        return self.kernel.get_reservations(slice_id=slice_id)

    def get_slice(self, *, slice_id: ID) -> ABCSlice:
        """
        Returns the slice with the given name.
        @param slice_id identifier of slice to return
        @return the requested slice or null if no slice with the requested name
                is registered with the kernel.
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)
        return self.kernel.get_slice(slice_id=slice_id)

    def get_slices(self) -> list:
        """
        Returns all slice registered with the kernel.
        @return an array of slices registered with the kernel
        """
        return self.kernel.get_slices()

    def handle_delegate(self, *, delegation: ABCDelegation, identity: AuthToken, id_token: str = None):
        """
        Handles a delegation. Called from both AM and Broker.

        @param delegation the delegation
        @param identity caller identity
        @param id_token id token

        @throws Exception in case of error
        """
        if delegation.get_slice_object() is None or delegation.get_slice_object().get_name() is None or \
                delegation.get_slice_object().get_slice_id() is None or delegation.get_delegation_id() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        # Obtain the previously created slice or create a new slice. When this
        # function returns we will have a slice object that is registered with the kernel
        s = self.kernel.get_slice(slice_id=delegation.get_slice_id())
        if s is None:
            self.kernel.register_slice(slice_object=delegation.get_slice_object())

        # Determine if this is a new or an already existing reservation. We
        # will register new reservations and call reserve for them. For
        # existing reservations we will perform amendReserve. XXX: Note, if the
        # caller makes two concurrent requests for a new reservation, it is
        # possible that kernel.softValidate can return null for both, yet only
        # one of them will succeed to register itself. For the second the
        # kernel will throw an exception. This is a limitation that does not
        # seem to be too much of a problem and can be resolved using the same
        # technique we use for creating/registering slices.
        temp = self.kernel.soft_validate_delegation(delegation=delegation)

        if temp is None:
            self.kernel.register_delegation(delegation=delegation)
            self.kernel.delegate(delegation=delegation, id_token=id_token)
        else:
            self.kernel.amend_delegate(delegation=temp)

    def handle_reserve(self, *, reservation: ABCKernelReservation, identity: AuthToken, create_new_slice: bool):
        """
        Handles a reserve, i.e., obtain a new ticket or lease. Called from both
        client and server side code. If the slice does not exist it will create
        and register a slice (if create_new_slice is true (server
        side)). If the slice does not exist and create_new_slice is
        false (client side), it will register the slice contained in the
        reservation object.
        @param reservation the reservation
        @param identity caller identity
        @param create_new_slice true -> creates a new slice object, false -> reuses
                   the slice object in the reservation. This flag is considered
                   only if the slice referenced by the reservation is not
                   registered with the kernel.
        @throws Exception in case of error
        """
        if reservation.get_slice() is None or reservation.get_slice().get_name() is None or \
                reservation.get_slice().get_slice_id() is None or reservation.get_reservation_id() is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        # Obtain the previously created slice or create a new slice. When this
        # function returns we will have a slice object that is registered with the kernel
        self.kernel.get_or_create_local_slice(identity=identity, reservation=reservation,
                                              create_new_slice=create_new_slice)

        # Determine if this is a new or an already existing reservation. We
        # will register new reservations and call reserve for them. For
        # existing reservations we will perform amendReserve. XXX: Note, if the
        # caller makes two concurrent requests for a new reservation, it is
        # possible that kernel.softValidate can return null for both, yet only
        # one of them will succeed to register itself. For the second the
        # kernel will throw an exception. This is a limitation that does not
        # seem to be too much of a problem and can be resolved using the same
        # technique we use for creating/registering slices.
        temp = self.kernel.soft_validate(reservation=reservation)

        if temp is None:
            self.kernel.register_reservation(reservation=reservation)
            self.kernel.reserve(reservation=reservation)
        else:
            self.kernel.amend_reserve(reservation=temp)

    def handle_update_reservation(self, *, reservation: ABCKernelReservation, auth: AuthToken):
        """
        Amend a reservation request or initiation, i.e., to issue a new bid on a
        previously filed request.
        @param reservation the reservation
        @param auth the slice owner
        @throws Exception in case of error
        """
        self.kernel.amend_reserve(reservation=reservation)

    def query(self, *, properties: dict, caller: AuthToken, id_token: str):
        """
        Processes an incoming query request.
        @param properties query
        @param caller caller identity
        @param id_token id_token
        @return query response
        """
        if id_token is not None:
            AccessChecker.check_access(action_id=ActionId.query, resource_type=ResourceType.resources,
                                       token=id_token, logger=self.logger, actor_type=self.actor.get_type())

        return self.kernel.query(properties=properties)

    def redeem(self, *, reservation: ABCControllerReservation):
        """
        Initiates a request to redeem a ticketed reservation.
        Role: Controller
        @param reservation the reservation being redeemed
        @throws Exception in case of error
        """
        if reservation is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        slice_object = reservation.get_slice()

        for r in slice_object.get_reservations().values():
            self.logger.debug("redeem() Reservation {} is in state: {}".format(r.get_reservation_id(),
                                                                               ReservationStates(r.get_state()).name))

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("Redeem on a reservation not registered with the kernel")

        target.validate_redeem()
        self.kernel.redeem(reservation=target)

    def redeem_request(self, *, reservation: ABCAuthorityReservation, caller: AuthToken,
                       callback: ABCControllerCallbackProxy, compare_sequence_numbers: bool):
        """
        Processes an incoming request for a new lease.
        Role: Authority
        @param reservation reservation representing the lease request. Must
                   contain a valid ticket.
        @param caller caller identity
        @param callback callback object
        @param compare_sequence_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        if reservation is None or caller is None or callback is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        try:
            if compare_sequence_numbers:
                reservation.validate_incoming()
                reservation.get_slice().set_client()

                s = self.kernel.get_slice(slice_id=reservation.get_slice().get_slice_id())

                if s is not None:
                    target = self.kernel.soft_validate(rid=reservation.get_reservation_id())

                    if target is not None:
                        self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestRedeem)

                reservation.prepare(callback=callback, logger=self.logger)
                self.handle_reserve(reservation=reservation, identity=caller, create_new_slice=True)
            else:
                reservation.set_logger(self.logger)
                self.handle_reserve(reservation=reservation, identity=reservation.get_client_auth_token(),
                                    create_new_slice=True)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error("Exception occurred in processing redeem request {}".format(e))
            reservation.fail_notify(message=str(e))

    def register_reservation(self, *, reservation: ABCReservationMixin):
        """
        Registers the given reservation with the kernel. Adds a database record
        for the reservation. The containing slice should have been previously
        registered with the kernel and no database record should exist. When
        registering a reservation with an existing database record use
        #reregisterReservation(IReservation)}. Only reservations
        that are not closed or failed can be registered. Closed or failed
        reservations will be ignored.
        @param reservation the reservation to register
        @throws IllegalArgumentException when the passed in argument is illegal
        @throws Exception if the reservation has already been registered with the
                    kernel.
        @throws RuntimeException when a database error occurs. In this case the
                    reservation will be unregistered from the kernel data
                    structures.
        """
        if reservation is None or not isinstance(reservation, ABCKernelReservation):
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.register_reservation(reservation=reservation)

    def register_delegation(self, *, delegation: ABCDelegation):
        """
        Registers the given delegation with the kernel. Adds a database record
        for the delegation. The containing slice should have been previously
        registered with the kernel and no database record should exist. When
        registering a delegation with an existing database record use
        #re_register_delegation(ABCDelegation)}. Only delegations
        that are not closed or failed can be registered. Closed or failed
        delegations will be ignored.
        @param delegation the delegation to register
        @throws IllegalArgumentException when the passed in argument is illegal
        @throws Exception if the delegation has already been registered with the
                    kernel.
        @throws RuntimeException when a database error occurs. In this case the
                    delegation will be unregistered from the kernel data
                    structures.
        """
        if delegation is None or not isinstance(delegation, ABCDelegation):
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.register_delegation(delegation=delegation)

    def register_slice(self, *, slice_object: ABCSlice):
        """
        Registers the slice with the kernel: adds the slice object to the kernel
        data structures and adds a database record for the slice.
        @param slice_object slice to register
        @throws Exception if the slice is already registered or a database error
                    occurs. If a database error occurs, the slice will be
                    unregistered.
        """
        if slice_object is None or slice_object.get_slice_id() is None or not isinstance(slice_object, ABCKernelSlice):
            raise KernelException("Invalid argument {}".format(slice_object))

        slice_object.set_owner(owner=self.actor.get_identity())
        self.kernel.register_slice(slice_object=slice_object)

    def remove_reservation(self, *, rid: ID):
        """
        Unregisters the reservation from the kernel data structures and removes
        its record from the database.
        Note:Only failed, closed, or close waiting reservations can be
        removed.
        @param rid identifier of reservation to remove
        @throws Exception in case of error
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.remove_reservation(rid=rid)

    def remove_slice(self, *, slice_id: ID):
        """
        Unregisters the slice (if it is registered with the kernel) and removes
        it from the database.

        Note: A slice can be removed only if it contains only closed or
        failed reservations.

        @param slice_id identifier of slice to remove
        @throws Exception in case of error
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.remove_slice(slice_id=slice_id)

    def re_register_reservation(self, *, reservation: ABCReservationMixin):
        """
         Registers a previously unregistered reservation with the kernel. The
        containing slice should have been previously registered with the kernel
        and a database record for the reservation should exist. When registering
        a reservation without and existing database record use
        register_reservation(IReservation). Only reservations
        that are not closed or failed can be registered. Closed or failed
        reservations will be ignored.
        @param reservation the reservation to reregister
        @throws IllegalArgumentException when the passed in argument is illegal
        @throws Exception if the reservation has already been registered with the
                    kernel or the reservation does not have a database record. In
                    the latter case the reservation will be unregistered from the
                    kernel data structures.
        @throws RuntimeException if a database error occurs
        """
        if reservation is None or not isinstance(reservation, ABCKernelReservation):
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.re_register_reservation(reservation=reservation)

    def re_register_delegation(self, *, delegation: ABCDelegation):
        """
         Registers a previously unregistered delegation with the kernel.
        @param delegation the delegation to reregister
        @throws IllegalArgumentException when the passed in argument is illegal
        @throws Exception if the delegation has already been registered with the
                    kernel or the delegation does not have a database record. In
                    the latter case the delegation will be unregistered from the
                    kernel data structures.
        @throws RuntimeException if a database error occurs
        """
        if delegation is None or not isinstance(delegation, ABCDelegation):
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.re_register_delegation(delegation=delegation)

    def re_register_slice(self, *, slice_object: ABCSlice):
        """
        Registers the slice with the kernel: adds the slice object to the kernel
        data structures. The slice object must have an existing database record.
        @param slice_object slice to register
        @throws Exception if the slice is already registered or a database error
                    occurs. If a database error occurs, the slice will be
                    unregistered.
        """
        if slice_object is None or slice_object.get_slice_id() is None or not isinstance(slice_object, ABCKernelSlice):
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.re_register_slice(slice_object=slice_object)

    def tick(self):
        """
        Checks all reservations for completions or problems. We might want to do
        these a few at a time.
        @throws Exception in case of error
        """
        try:
            self.kernel.tick()
        except Exception as e:
            self.logger.error("Tick error: {}".format(e))
            self.logger.error(traceback.format_exc())

    def delegate(self, *, delegation: ABCDelegation, destination: ABCActorIdentity, id_token: str = None):
        """
        Initiates a delegate request. If the exported flag is set, this is a claim
        on a pre-reserved "will call" ticket.
        Role: Broker or Controller.
        @param delegation delegation parameters for ticket request
        @param destination identity of the actor the request must be sent to
        @param id_token id token
        @throws Exception in case of error
        """
        if delegation is None or destination is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        callback = ActorRegistrySingleton.get().get_callback(protocol=Constants.PROTOCOL_KAFKA,
                                                             actor_name=destination.get_name())
        if callback is None:
            raise KernelException("Unsupported")

        delegation.prepare(callback=callback, logger=self.logger)
        delegation.validate_outgoing()
        self.handle_delegate(delegation=delegation, identity=destination.get_identity(), id_token=id_token)

    def ticket(self, *, reservation: ABCClientReservation, destination: ABCActorIdentity):
        """
        Initiates a ticket request. If the exported flag is set, this is a claim
        on a pre-reserved "will call" ticket.
        Role: Broker or Controller.
        @param reservation reservation parameters for ticket request
        @param destination identity of the actor the request must be sent to
        @throws Exception in case of error
        """
        if reservation is None or destination is None or not isinstance(reservation, ABCKernelClientReservationMixin):
            raise KernelException(Constants.INVALID_ARGUMENT)

        protocol = reservation.get_broker().get_type()
        callback = ActorRegistrySingleton.get().get_callback(protocol=protocol, actor_name=destination.get_name())
        if callback is None:
            raise KernelException("Unsupported")

        reservation.prepare(callback=callback, logger=self.logger)
        reservation.validate_outgoing()
        self.handle_reserve(reservation=reservation, identity=destination.get_identity(), create_new_slice=False)

    def ticket_request(self, *, reservation: ABCBrokerReservation, caller: AuthToken, callback: ABCClientCallbackProxy,
                       compare_seq_numbers: bool):
        """
        Processes an incoming request for a new ticket.
        Role: Broker
        @param reservation reservation representing the ticket request
        @param caller caller identity
        @param callback callback object
        @param compare_seq_numbers if true, the incoming sequence number will
                   be compared to the local sequence number to detect fresh
                   requests, if false, no comparison will be performed.
        @throws Exception in case of error
        """
        if reservation is None or caller is None or callback is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        try:
            if compare_seq_numbers:
                reservation.validate_incoming()
                # Mark the slice as client slice.
                reservation.get_slice().set_client()

                # This reservation has just arrived at this actor from another
                # actor. Attach the current actor object to the reservation so
                # that operations on this reservation can get access to it.
                reservation.set_actor(actor=self.kernel.get_plugin().get_actor())

                # If the slice referenced in the reservation exists, then we
                # must check the incoming sequence number to determine if this
                # request is fresh.
                s = self.kernel.get_slice(slice_id=reservation.get_slice().get_slice_id())

                if s is not None:
                    target = self.kernel.soft_validate(rid=reservation.get_reservation_id())
                    if target is not None:
                        seq_num = self.kernel.compare_and_update(incoming=reservation, current=target)
                        if seq_num == SequenceComparisonCodes.SequenceGreater:
                            self.handle_update_reservation(reservation=target, auth=caller)

                        elif seq_num == SequenceComparisonCodes.SequenceSmaller:
                            self.logger.warning("Incoming request has a smaller sequence number")

                        elif seq_num == SequenceComparisonCodes.SequenceInProgress:
                            self.logger.warning("New request for a reservation with a pending action")

                        elif seq_num == SequenceComparisonCodes.SequenceEqual:
                            self.kernel.handle_duplicate_request(current=target, operation=RequestTypes.RequestTicket)
                    else:
                        # This is a new reservation. No need to check sequence numbers
                        reservation.prepare(callback=callback, logger=self.logger)
                        self.handle_reserve(reservation=reservation, identity=caller, create_new_slice=True)
                else:
                    # New reservation for a new slice.
                    reservation.prepare(callback=callback, logger=self.logger)
                    self.handle_reserve(reservation=reservation, identity=caller, create_new_slice=True)
            else:
                # This is most likely a reservation being recovered. Do not compare sequence numbers,
                # just trigger reserve.
                reservation.set_logger(logger=self.logger)
                self.handle_reserve(reservation=reservation, identity=reservation.get_client_auth_token(),
                                    create_new_slice=True)
        except Exception as e:
            self.logger.error(f"Exception occurred {e}")
            self.logger.error(traceback.format_exc())
            reservation.fail_notify(message=str(e))

    def unregister_reservation(self, *, rid: ID):
        """
        Unregisters the reservation from the kernel data structures.
         Note:does not remove the reservation database record.
         Note:Only failed, closed, or close waiting reservations can be
        unregistered.
        @param rid identifier for reservation to unregister
        @throws Exception in case of error
        """
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.unregister_reservation(rid=rid)

    def unregister_slice(self, *, slice_id: ID):
        """
        Unregisters the slice and releases any resources that it may hold.
        Note: A slice can be unregistered only if it contains only closed
        or failed reservations.
        @param slice_id identifier of slice to unregister
        @throws Exception in case of error
        """
        if slice_id is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        self.kernel.unregister_slice(slice_id=slice_id)

    def update_lease(self, *, reservation: ABCReservationMixin, update_data: UpdateData, caller: AuthToken):
        """
        Handles a lease update from an authority.
        Role: Controller
        @param reservation reservation describing the update
        @param update_data status of the update
        @param caller identity of the caller
        @throws Exception in case of error
        """
        if reservation is None or update_data is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=reservation.get_reservation_id())
        reservation.validate_incoming()
        self.kernel.update_lease(reservation=target, update=reservation, update_data=update_data)

    def update_ticket(self, *, reservation: ABCReservationMixin, update_data: UpdateData, caller: AuthToken):
        """
        Handles a ticket update from upstream broker.
        Role: Agent or Controller.
        @param reservation reservation describing the update
        @param update_data status of the update
        @param caller identity of the caller
        @throws Exception in case of error
        """
        if reservation is None or update_data is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate(rid=reservation.get_reservation_id())
        reservation.validate_incoming_ticket()
        self.kernel.update_ticket(reservation=target, update=reservation, update_data=update_data)

    def update_delegation(self, *, delegation: ABCDelegation, update_data: UpdateData, caller: AuthToken):
        """
        Handles a delegation update from upstream broker.
        Role: Agent or Controller.
        @param delegation delegation describing the update
        @param update_data status of the update
        @param caller identity of the caller
        @throws Exception in case of error
        """
        if delegation is None or update_data is None or caller is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.validate_delegation(did=delegation.get_delegation_id())
        delegation.validate_incoming()
        self.kernel.update_delegation(delegation=target, update=delegation, update_data=update_data)

    def process_failed_rpc(self, *, rid: ID, rpc: FailedRPC):
        if rid is None:
            raise KernelException(Constants.INVALID_ARGUMENT)

        target = self.kernel.soft_validate(rid=rid)
        if target is None:
            self.logger.warning("Could not find reservation #{} while processing a failed RPC.".format(rid))
            return
        self.kernel.handle_failed_rpc(reservation=target, rpc=rpc)
