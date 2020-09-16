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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_actor_identity import IActorIdentity
from fabric.actor.core.apis.i_base_plugin import IBasePlugin
from fabric.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.apis.i_client_callback_proxy import IClientCallbackProxy
from fabric.actor.core.apis.i_client_reservation import IClientReservation
from fabric.actor.core.apis.i_controller_callback_proxy import IControllerCallbackProxy
from fabric.actor.core.apis.i_controller_reservation import IControllerReservation
from fabric.actor.core.apis.i_policy import IPolicy
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.common.exceptions import SliceNotFoundException
from fabric.actor.core.kernel.failed_rpc import FailedRPC
from fabric.actor.core.apis.i_kernel_client_reservation import IKernelClientReservation
from fabric.actor.core.apis.i_kernel_reservation import IKernelReservation
from fabric.actor.core.apis.i_kernel_slice import IKernelSlice
from fabric.actor.core.kernel.kernel import Kernel
from fabric.actor.core.kernel.request_types import RequestTypes
from fabric.actor.core.kernel.reservation_states import ReservationStates
from fabric.actor.core.kernel.resource_set import ResourceSet
from fabric.actor.core.kernel.sequence_comparison_codes import SequenceComparisonCodes
from fabric.actor.core.registry.actor_registry import ActorRegistrySingleton
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.update_data import UpdateData
from fabric.actor.security.access_monitor import AccessMonitor
from fabric.actor.security.auth_token import AuthToken
from fabric.actor.security.guard import Guard


class KernelWrapper:
    """
    KernelWrapper is responsible for validating the arguments to internal kernel methods before
    invoking these calls. The internal kernel methods can only be invoked through
    an instance of the kernel wrapper.
    """
    def __init__(self, actor: IActor, plugin: IBasePlugin, policy: IPolicy, monitor: AccessMonitor, guard: Guard):
        # The actor linked by the wrapper to the kernel.
        self.actor = actor
        # The kernel instance.
        self.kernel = Kernel(plugin, policy, actor.get_logger())
        # Access control monitor.
        self.monitor = monitor
        # Access control lists.
        self.guard = guard
        # Logger.
        self.logger = actor.get_logger()

    def await_nothing_pending(self):
        """
        Blocks until there are no more reservations in a pending state.
        @throws InterruptedException in case of error
        """
        self.kernel.await_nothing_pending()

    def claim_request(self, reservation: IBrokerReservation, caller: AuthToken, callback: IClientCallbackProxy):
        if reservation is None or caller is None or callback is None:
            raise Exception("Invalid argument")

        # Note: for claim we do not need the slice object, so we use
        # validate(ReservationID) instead of validate(Reservation).
        exported = self.kernel.validate(rid=reservation.get_reservation_id())
        # check access
        self.monitor.check_reserve(exported.get_slice().get_guard(), caller)
        exported.prepare(callback, self.logger)
        self.kernel.claim(exported)

    def reclaim_request(self, reservation: IBrokerReservation, caller: AuthToken, callback: IClientCallbackProxy):
        if reservation is None or caller is None or callback is None:
            raise Exception("Invalid argument")

        # Note: for claim we do not need the slice object, so we use
        # validate(ReservationID) instead of validate(Reservation).
        exported = self.kernel.validate(rid=reservation.get_reservation_id())
        # check access
        self.monitor.check_reserve(exported.get_slice().get_guard(), caller)
        exported.prepare(callback, self.logger)
        self.kernel.reclaim(exported)

    def fail(self, rid: ID, message: str):
        """
        Fails the specified reservation.
        @param rid reservation id
        @param message message
        @throws Exception in case of error
        """
        if rid is None:
            raise Exception("Invalid argument")

        target = self.kernel.validate(rid=rid)
        self.monitor.check_reserve(target.get_slice().get_guard(), self.actor.get_identity())
        self.kernel.fail(target, message)

    def close(self, rid: ID):
        """
        Closes the reservation, potentially initiating a close request to another
        actor. If the reservation has concrete resources bound to it, this method
        may return before all close operations have completed. Check the
        reservation state to determine when close completes.
        @param rid identifier of reservation to close
        @throws Exception in case of error
        """
        if rid is None:
            raise Exception("Invalid argument")
        target = self.kernel.validate(rid=rid)
        # NOTE: this call does not require access control check, since
        # it is executed in the context of the actor represented by KernelWrapper.
        self.kernel.close(target)

    def close_slice_reservations(self, slice_id: ID):
        if slice_id is None:
            raise Exception("Invalid argument")

        if not self.kernel.is_known_slice(slice_id):
            raise SliceNotFoundException(str(slice_id))

        reservations = self.get_reservations(slice_id)
        for r in reservations:
            try:
                self.kernel.close(r)
            except Exception as e:
                self.logger.error("Error during close: {}".format(e))

    def close_request(self, reservation: IReservation, caller: AuthToken, compare_sequence_numbers: bool):
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
            raise Exception("Invalid argument")

        if compare_sequence_numbers:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_config_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)

            sequence_number_compare = self.kernel.compare_and_update_ignore_pending(reservation, target)
            if sequence_number_compare == SequenceComparisonCodes.SequenceGreater:
                self.kernel.close(target)
            elif sequence_number_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("closeRequest with a smaller sequence number")
            elif sequence_number_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate closeRequest")
                self.kernel.handle_duplicate_request(target, RequestTypes.RequestClose)
        else:
            target = self.kernel.validate(reservation=reservation)
            # TODO
            auth_properties = reservation.get_requested_resources().get_config_properties()
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)
            self.kernel.close(target)

    def export(self, reservation: IBrokerReservation, client: AuthToken):
        """
        Initiates a ticket export.
        Role: Broker or Authority
        Prepare/hold a ticket for "will call" claim by a client.
        @param reservation reservation to be exported
        @param client client identity
        @throws Exception in case of error
        """
        if reservation is None or reservation.get_slice() is None:
            raise Exception("Invalid argument")

        reservation.prepare(None, self.logger)
        reservation.client = client
        reservation.get_slice().set_broker_client()
        self.handle_reserve(reservation, client, False, False)

    def extend_lease(self, reservation: IControllerReservation):
        if reservation is None:
            raise Exception("Invalid argument")

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("extendLease for a reservation not registered with the kernel")
            return

        auth_properties = reservation.get_requested_resources().get_request_properties()
        requester = self.monitor.check_proxy(self.actor.get_identity(), None)
        self.monitor.check_reserve(target.get_slice().get_guard(), requester)
        target.validate_redeem()
        self.kernel.extend_lease(reservation)

    def extend_lease_request(self, reservation: IAuthorityReservation, caller: AuthToken, compare_sequence_numbers: bool):
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
            raise Exception("Invalid argument")

        if compare_sequence_numbers:
            reservation.validate_incoming()
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_config_properties()
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)

            sequence_compare = self.kernel.compare_and_update(reservation, target)
            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                target.prepare_extend_lease()
                self.kernel.extend_lease(target)

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("extendLeaseRequest with a smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate extendLease request")
                self.kernel.handle_duplicate_request(target, RequestTypes.RequestExtendLease)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_config_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)
            self.kernel.extend_lease(target)

    def modify_lease_request(self, reservation: IAuthorityReservation, caller: AuthToken, compare_sequence_numbers: bool):
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
            raise Exception("Invalid argument")

        if compare_sequence_numbers:
            reservation.validate_incoming()
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_config_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)

            sequence_compare = self.kernel.compare_and_update(reservation, target)

            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                target.prepare_modify_lease()
                self.kernel.modify_lease(reservation)

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("modifyLeaseRequest with a smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("duplicate extendLease request")
                self.kernel.handle_duplicate_request(target, RequestTypes.RequestModifyLease)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_config_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)
            self.kernel.modify_lease(target)

    def extend_reservation(self, rid: ID, resources: ResourceSet, term: Term) -> int:
        """
        Extends the reservation with the given resources and term.
        @param rid identifier of reservation to extend
        @param resources resources for extension
        @param term term for extension
        @return 0 on success, a negative exit code on error
        @throws Exception in case of error
        """
        if rid is None or resources is None or term is None:
            raise Exception("Invalid argument")

        return self.kernel.extend_reservation(rid, resources, term)

    def extend_ticket(self, reservation: IClientReservation):
        """
        Initiates a request to extend a ticket.
        Role: Broker or Controller
        @param reservation reservation describing the ticket extension request
        @throws Exception in case of error
        """
        if reservation is None:
            raise Exception("Invalid argument")

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            raise Exception("extendTicket on a reservation not registered with the kernel")

        auth_properties = reservation.get_resources().get_request_properties()
        # TODO
        requester = self.monitor.check_proxy(self.actor.get_identity(), None)

        self.monitor.check_reserve(target.get_slice().get_guard(), requester)

        target.validate_outgoing()
        self.kernel.extend_ticket(target)

    def extend_ticket_request(self, reservation: IBrokerReservation, caller: AuthToken, compare_sequence_numbers: bool):
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
            raise Exception("Invalid argument")
        if compare_sequence_numbers:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_requested_resources().get_request_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)

            sequence_compare = self.kernel.compare_and_update(reservation, target)

            if sequence_compare == SequenceComparisonCodes.SequenceGreater:
                self.logger.debug("extend_ticket SequenceGreater")
                self.kernel.extend_ticket(target)

            elif sequence_compare == SequenceComparisonCodes.SequenceInProgress:
                self.logger.warning("New request for a reservation with a pending action")

            elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
                self.logger.warning("Incoming extendTicket request has smaller sequence number")

            elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
                self.logger.warning("Duplicate extendTicket request")
                self.kernel.handle_duplicate_request(target, RequestTypes.RequestExtendTicket)
        else:
            target = self.kernel.validate(rid=reservation.get_reservation_id())
            auth_properties = reservation.get_resources().get_request_properties()
            # TODO
            requester = self.monitor.check_proxy(caller, None)
            self.monitor.check_reserve(target.get_slice().get_guard(), requester)
            self.logger.debug("extend_ticket No sequence number comparison")
            self.extend_ticket(target)

    def modify_lease(self, reservation: IControllerReservation):
        """
        Initiates a request to modify a lease.
        Role: Controller
        @param reservation reservation describing the modify request
        @throws Exception in case of error
        """
        if reservation is None:
            raise Exception("Invalid argument")

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("modifyLease for a reservation not registered with the kernel")

        auth_properties = reservation.get_resources().get_request_properties()
        # TODO
        requester = self.monitor.check_proxy(self.actor.get_identity(), None)

        self.monitor.check_reserve(target.get_slice().get_guard(), requester)

        target.validate_redeem()
        self.kernel.modify_lease(target)

    def relinquish_request(self, reservation: IBrokerReservation, caller: AuthToken):
        if reservation is None or caller is None:
            raise Exception("Invalid argument")

        reservation.validate_incoming()

        target = self.kernel.soft_validate(rid=reservation.get_reservation_id())
        if target is None:
            self.logger.info("Relinquish for non-existent reservation. Reservation has already been closed. Nothing to relinquish")
            return

        # TODO
        requester = self.monitor.check_proxy(caller, None)

        self.monitor.check_reserve(target.get_slice().get_guard(), requester)

        sequence_compare = self.kernel.compare_and_update(reservation, target)

        if sequence_compare == SequenceComparisonCodes.SequenceGreater or sequence_compare == SequenceComparisonCodes.SequenceInProgress:
            self.kernel.close(target)

        elif sequence_compare == SequenceComparisonCodes.SequenceSmaller:
            self.logger.warning("Incoming relinquish request has smaller sequence number")

        elif sequence_compare == SequenceComparisonCodes.SequenceEqual:
            self.logger.warning("Duplicate relinquish request")
            self.kernel.handle_duplicate_request(target, RequestTypes.RequestRelinquish)

    def get_client_slices(self) -> list:
        """
        Returns all client slices registered with the kernel.
        @return an array of client slices registered with the kernel
        """
        return self.kernel.get_client_slices()

    def get_inventory_slices(self) -> list:
        """
        Returns all inventory slices registered with the kernel.
        @return an array of inventory slices registered with the kernel
        """
        return self.kernel.get_inventory_slices()

    def get_reservation(self, rid: ID) -> IReservation:
        """
        Returns the reservation with the given reservation identifier.
        @param rid reservation identifier
        @return reservation with the given reservation identifier
        """
        if rid is None:
            raise Exception("Invalid argument")
        return self.kernel.get_reservation(rid)

    def get_reservations(self, slice_id: ID) -> list:
        """
        Returns all reservations in the given slice
        @param slice_id identifier of slice
        @return an array of all reservations in the slice
        """
        if slice_id is None:
            raise Exception("Invalid argument")

        return self.kernel.get_reservations(slice_id)

    def get_slice(self, slice_id: ID) -> ISlice:
        """
        Returns the slice with the given name.
        @param slice_id identifier of slice to return
        @return the requested slice or null if no slice with the requested name
                is registered with the kernel.
        """
        if slice_id is None:
            raise Exception("Invalid argument")
        return self.kernel.get_slice(slice_id)

    def get_slices(self) -> list:
        """
        Returns all slice registered with the kernel.
        @return an array of slices registered with the kernel
        """
        return self.kernel.get_slices()

    def handle_reserve(self, reservation: IKernelReservation, identity: AuthToken, create_new_slice: bool,
                       verify_credentials: bool):
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
        @param verify_credentials true if credentials to be verified
        @throws Exception in case of error
        """
        if reservation.get_slice() is None or reservation.get_slice().get_name() is None or \
                reservation.get_slice().get_slice_id() is None or reservation.get_reservation_id() is None:
            raise Exception("Invalid argument")
        auth_properties = None

        if isinstance(reservation, IAuthorityReservation):
            auth_properties = reservation.get_requested_resources().get_config_properties()

        elif isinstance(reservation, IBrokerReservation):
            auth_properties = reservation.get_requested_resources().get_request_properties()

        else:
            auth_properties = reservation.get_resources().get_request_properties()

        if verify_credentials:
            identity = self.monitor.check_proxy(identity, None)

        # Obtain the previously created slice or create a new slice. When this
        # function returns we will have a slice object that is registered with the kernel
        s = self.kernel.get_or_create_local_slice(identity, reservation, create_new_slice)

        if verify_credentials:
            if auth_properties is not None:
                self.monitor.check_reserve(s.get_guard(), identity)

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
            self.kernel.register_reservation(reservation)
            self.kernel.reserve(reservation)
        else:
            self.kernel.amend_reserve(temp)

    def handle_update_reservation(self, reservation: IKernelReservation, auth: AuthToken):
        """
        Amend a reservation request or initiation, i.e., to issue a new bid on a
        previously filed request.
        @param reservation the reservation
        @param auth the slice owner
        @throws Exception in case of error
        """
        auth_properties = reservation.get_requested_resources().get_request_properties()
        requester = self.monitor.check_proxy(auth, None)

        self.monitor.check_reserve(reservation.get_slice().get_guard(), requester)

        self.kernel.amend_reserve(reservation)

    def query(self, properties: dict, caller: AuthToken):
        """
        Processes an incoming query request.
        @param properties query
        @param caller caller identity
        @return query response
        """
        return self.kernel.query(properties)

    def redeem(self, reservation: IControllerReservation):
        """
        Initiates a request to redeem a ticketed reservation.
        Role: Controller
        @param reservation the reservation being redeemed
        @throws Exception in case of error
        """
        if reservation is None:
            raise Exception("Invalid argument")

        slice_object = reservation.get_slice()

        for r in slice_object.get_reservations().values():
            self.logger.trace("redeem() Reservation {} is in state: {}".format(r.get_reservation_id(), ReservationStates(r.get_state()).name))

        target = self.kernel.validate(rid=reservation.get_reservation_id())

        if target is None:
            self.logger.error("Redeem on a reservation not registered with the kernel")

        auth_properties = reservation.get_resources().get_request_properties()
        requester = self.monitor.check_proxy(self.actor.get_identity(), None)
        self.monitor.check_reserve(target.get_slice().get_guard(), requester)

        target.validate_redeem()
        self.kernel.redeem(target)

    def redeem_request(self, reservation: IAuthorityReservation, caller: AuthToken,
                       callback: IControllerCallbackProxy, compare_sequence_numbers: bool):
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
            raise Exception("Invalid arguments")

        try:
            if compare_sequence_numbers:
                reservation.validate_incoming()
                reservation.get_slice().set_client()

                s = self.kernel.get_slice(reservation.get_slice().get_slice_id())

                if s is not None:
                    target = self.kernel.soft_validate(rid=reservation.get_reservation_id())

                    if target is not None:
                        self.kernel.handle_duplicate_request(target, RequestTypes.RequestRedeem)

                reservation.prepare(callback, self.logger)
                self.handle_reserve(reservation, callback, True, True)
            else:
                reservation.set_logger(self.logger)
                self.handle_reserve(reservation, reservation.get_client_auth_token(), True, True)
        except Exception as e:
            traceback.print_exc()
            self.logger.error("Exception occurred in processing redeem request {}".format(e))
            reservation.fail_notify(str(e))

    def register_reservation(self, reservation: IReservation):
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
        if reservation is None or not isinstance(reservation, IKernelReservation):
            raise Exception("Invalid argument")

        self.kernel.register_reservation(reservation)

    def register_slice(self, slice_object: ISlice):
        """
        Registers the slice with the kernel: adds the slice object to the kernel
        data structures and adds a database record for the slice.
        @param slice_object slice to register
        @throws Exception if the slice is already registered or a database error
                    occurs. If a database error occurs, the slice will be
                    unregistered.
        """
        if slice_object is None or slice_object.get_slice_id() is None or not isinstance(slice_object, IKernelSlice):
            raise Exception("Invalid argument {}".format(slice_object))

        slice_object.set_owner(self.actor.get_identity())
        self.kernel.register_slice(slice_object)

    def remove_reservation(self, rid: ID):
        """
        Unregisters the reservation from the kernel data structures and removes
        its record from the database.
        Note:Only failed, closed, or close waiting reservations can be
        removed.
        @param rid identifier of reservation to remove
        @throws Exception in case of error
        """
        if rid is None:
            raise Exception("Invalid argument")

        self.kernel.remove_reservation(rid)

    def remove_slice(self, slice_id: ID):
        """
        Unregisters the slice (if it is registered with the kernel) and removes
        it from the database.

        Note: A slice can be removed only if it contains only closed or
        failed reservations.

        @param slice_id identifier of slice to remove
        @throws Exception in case of error
        """
        if slice_id is None:
            raise Exception("Invalid argument")

        self.kernel.remove_slice(slice_id)

    def re_register_reservation(self, reservation: IReservation):
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
        if reservation is None or not isinstance(reservation, IKernelReservation):
            raise Exception("Invalid argument")

        self.kernel.re_register_reservation(reservation)

    def re_register_slice(self, slice_object: ISlice):
        """
        Registers the slice with the kernel: adds the slice object to the kernel
        data structures. The slice object must have an existing database record.
        @param slice_object slice to register
        @throws Exception if the slice is already registered or a database error
                    occurs. If a database error occurs, the slice will be
                    unregistered.
        """
        if slice_object is None or slice_object.get_slice_id() is None or not isinstance(slice_object, IKernelSlice):
            return Exception("Invalid argument")

        self.kernel.re_register_slice(slice_object)

    def tick(self):
        """
        Checks all reservations for completions or problems. We might want to do
        these a few at a time.
        @throws Exception in case of error
        """
        try:
            self.kernel.tick()
        except Exception as e:
            traceback.print_exc()

    def ticket(self, reservation: IClientReservation, destination: IActorIdentity):
        """
        Initiates a ticket request. If the exported flag is set, this is a claim
        on a pre-reserved "will call" ticket.
        Role: Broker or Controller.
        @param reservation reservation parameters for ticket request
        @param destination identity of the actor the request must be sent to
        @throws Exception in case of error
        """
        if reservation is None or destination is None or not isinstance(reservation, IKernelClientReservation):
            raise Exception("Invalid arguments")

        protocol = reservation.get_broker().get_type()
        callback = ActorRegistrySingleton.get().get_callback(protocol, destination.get_name())
        if callback is None:
            raise Exception("Unsupported")

        reservation.prepare(callback, self.logger)
        reservation.validate_outgoing()
        self.handle_reserve(reservation, destination.get_identity(), False, False)

    def ticket_request(self, reservation: IBrokerReservation, caller: AuthToken, callback: IClientCallbackProxy, compare_seq_numbers: bool):
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
            raise Exception("Invalid argument")

        try:
            if compare_seq_numbers:
                reservation.validate_incoming()
                # Mark the slice as client slice.
                reservation.get_slice().set_client()

                # This reservation has just arrived at this actor from another
                # actor. Attach the current actor object to the reservation so
                # that operations on this reservation can get access to it.
                reservation.set_actor(self.kernel.get_plugin().get_actor())

                # If the slice referenced in the reservation exists, then we
                # must check the incoming sequence number to determine if this
                # request is fresh.
                s = self.kernel.get_slice(reservation.get_slice().get_slice_id())

                if s is not None:
                    target = self.kernel.soft_validate(rid=reservation.get_reservation_id())
                    if target is not None:
                        seq_num = self.kernel.compare_and_update(reservation, target)
                        if seq_num == SequenceComparisonCodes.SequenceGreater:
                            self.handle_update_reservation(target, caller)

                        elif seq_num == SequenceComparisonCodes.SequenceSmaller:
                            self.logger.warning("Incoming request has a smaller sequence number")

                        elif seq_num == SequenceComparisonCodes.SequenceInProgress:
                            self.logger.warning("New request for a reservation with a pending action")

                        elif seq_num == SequenceComparisonCodes.SequenceEqual:
                            self.kernel.handle_duplicate_request(target, RequestTypes.RequestTicket)
                    else:
                        # This is a new reservation. No need to check sequence numbers
                        reservation.prepare(callback, self.logger)
                        self.handle_reserve(reservation, callback, True, True)
                else:
                    # New reservation for a new slice.
                    reservation.prepare(callback, self.logger)
                    self.handle_reserve(reservation, callback, True, True)
            else:
                # This is most likely a reservation being recovered. Do not compare sequence numbers, just trigger reserve.
                reservation.set_logger(self.logger)
                self.handle_reserve(reservation, reservation.get_client_auth_token(), True, True)
        except Exception as e:
            self.logger.error("ticketRequest{}".format(e))
            reservation.fail_notify(str(e))

    def unregister_reservation(self, rid: ID):
        """
        Unregisters the reservation from the kernel data structures.
         Note:does not remove the reservation database record.
         Note:Only failed, closed, or close waiting reservations can be
        unregistered.
        @param rid identifier for reservation to unregister
        @throws Exception in case of error
        """
        if rid is None:
            raise Exception("Invalid arguments")

        self.kernel.unregister_reservation(rid)

    def unregister_slice(self, slice_id: ID):
        """
        Unregisters the slice and releases any resources that it may hold.
        Note: A slice can be unregistered only if it contains only closed
        or failed reservations.
        @param slice_id identifier of slice to unregister
        @throws Exception in case of error
        """
        if slice_id is None:
            raise Exception("Invalid arguments")

        self.kernel.unregister_slice(slice_id)

    def update_lease(self, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
        """
        Handles a lease update from an authority.
        Role: Controller
        @param reservation reservation describing the update
        @param update_data status of the update
        @param caller identity of the caller
        @throws Exception in case of error
        """
        if reservation is None or update_data is None or caller is None:
            raise Exception("Invalid arguments")

        target = self.kernel.validate(rid=reservation.get_reservation_id())
        auth_properties = reservation.get_resources().get_request_properties()
        requester = self.monitor.check_proxy(caller, None)

        self.monitor.check_update(target.get_slice().get_guard(), requester)
        reservation.validate_incoming()
        self.kernel.update_lease(target, reservation, update_data)

    def update_ticket(self, reservation: IReservation, update_data: UpdateData, caller: AuthToken):
        """
        Handles a ticket update from upstream broker.
        Role: Agent or Controller.
        @param reservation reservation describing the update
        @param update_data status of the update
        @param caller identity of the caller
        @throws Exception in case of error
        """
        if reservation is None or update_data is None or caller is None:
            raise Exception("Invalid arguments")

        target = self.kernel.validate(rid=reservation.get_reservation_id())
        auth_properties = reservation.get_resources().get_request_properties()
        requester = self.monitor.check_proxy(caller, None)
        self.monitor.check_update(target.get_slice().get_guard(), requester)
        reservation.validate_incoming_ticket()
        self.kernel.update_ticket(target, reservation, update_data)

    def process_failed_rpc(self, rid: ID, rpc: FailedRPC):
        if rid is None:
            raise Exception("Invalid arguments")

        target = self.kernel.soft_validate(rid=rid)
        if target is None:
            self.logger.warning("Could not find reservation #{} while processing a failed RPC.".format(rid))
            return
        self.kernel.handle_failed_rpc(target, rpc)



