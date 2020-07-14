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

from fabric.actor.core.apis.IBasePlugin import IBasePlugin
from fabric.actor.core.apis.IPolicy import IPolicy
from fabric.actor.core.apis.ISlice import ISlice
from fabric.actor.core.common.Constants import Constants
from fabric.actor.core.container.Globals import GlobalsSingleton
from fabric.actor.core.kernel.AuthorityReservation import AuthorityReservation
from fabric.actor.core.kernel.FailedRPC import FailedRPC
from fabric.actor.core.apis.IKernelBrokerReservation import IKernelBrokerReservation
from fabric.actor.core.apis.IKernelControllerReservation import IKernelControllerReservation
from fabric.actor.core.apis.IKernelReservation import IKernelReservation
from fabric.actor.core.apis.IKernelServerReservation import IKernelServerReservation
from fabric.actor.core.apis.IKernelSlice import IKernelSlice
from fabric.actor.core.kernel.RequestTypes import RequestTypes
from fabric.actor.core.kernel.Reservation import Reservation
from fabric.actor.core.kernel.ReservationPurgedEvent import ReservationPurgedEvent
from fabric.actor.core.kernel.ReservationStates import ReservationPendingStates, ReservationStates
from fabric.actor.core.kernel.ResourceSet import ResourceSet
from fabric.actor.core.kernel.SequenceComparisonCodes import SequenceComparisonCodes
from fabric.actor.core.kernel.SliceTable2 import SliceTable2
from fabric.actor.core.time.Term import Term
from fabric.actor.core.util.ID import ID
from fabric.actor.core.util.ReservationSet import ReservationSet
from fabric.actor.core.util.ResourceData import ResourceData
from fabric.actor.core.util.UpdateData import UpdateData
from fabric.actor.security.AuthToken import AuthToken


class Kernel:
    def __init__(self, plugin: IBasePlugin, mapper: IPolicy, logger):
        # The plugin.
        self.plugin = plugin
        # Policy
        self.policy = mapper
        # Logger
        self.logger = logger
        # All slices managed by the kernel
        self.slices = SliceTable2()
        # All reservations managed by the kernel.
        self.reservations = ReservationSet()
        self.lock = threading.Lock()
        self.nothing_pending = threading.Condition()

    def amend_reserve(self, reservation: IKernelReservation):
        """
        Amends a previous reserve operation (both client and server side) for the
        reservation.
        @param reservation reservation
        @throws Exception
        """
        try:
            reservation.reserve(policy=self.policy)
            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            self.error("An error occurred during amend reserve for reservation #{}".format(reservation.get_reservation_id()), e)

    def claim(self, reservation: IKernelBrokerReservation):
        """
        Processes a requests to claim new ticket for previously exported
        resources (broker role). On the client side this request is issued by
        @param reservation the reservation being claimed
        @throws Exception
        """
        try:
            reservation.claim()
            self.plugin.get_database().update_reservation(reservation)
        except Exception as e:
            self.error("An error occurred during claim for reservation #{}".format(reservation.get_reservation_id()), e)

    def fail(self, reservation: IKernelReservation, message: str):
        if not reservation.is_failed() and not reservation.is_closed():
            reservation.fail(message, None)
        self.plugin.get_database().update_reservation(reservation)

    def close(self, reservation: IKernelReservation):
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
                self.policy.close(reservation)
                reservation.close()
                self.plugin.get_database().update_reservation(reservation)
                reservation.service_close()
        except Exception as e:
            traceback.print_exc()
            self.error("An error occurred during close for reservation #{}".format(reservation.get_reservation_id()), e)

    def compare_and_update(self, incoming: IKernelServerReservation, current: IKernelServerReservation):
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
                current.set_sequence_in(incoming.get_sequence_in())
                current.set_requested_resources(incoming.get_requested_resources())
                current.set_requested_term(incoming.get_requested_term())
        else:
            if current.get_sequence_in() > incoming.get_sequence_in():
                code = SequenceComparisonCodes.SequenceSmaller
        return code

    def compare_and_update_ignore_pending(self, incoming: IKernelServerReservation, current: IKernelServerReservation):
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
            current.set_sequence_in(incoming.get_sequence_in())
            current.set_requested_resources(incoming.get_requested_resources())
            current.set_requested_term(incoming.get_requested_term())
        else:
            if current.get_sequence_in() > incoming.get_sequence_in():
                code = SequenceComparisonCodes.SequenceSmaller
        return code

    def error(self, err: str, e: Exception):
        """
        Logs the specified exception and re-throws it.
        @param err error message
        @param e exception
        """
        self.logger.error("Error: {} Exception: {}".format(err, e))
        raise Exception(e)

    def extend_lease(self, reservation: IKernelReservation):
        """
        Handles an extend lease operation for the reservation.
        Client: issue an extend lease request.
        Authority: process a request for a lease extension.
        @param reservation reservation for which to perform extend lease
        @throws Exception
        """
        try:
            reservation.extend_lease()
            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_extend_lease()
        except Exception as e:
            self.error("An error occurred during extend lease for reservation #{}".format(reservation.get_reservation_id()), e)

    def modify_lease(self, reservation: IKernelReservation):
        """
        Handles a modify lease operation for the reservation.
        Client: issue a modify lease request.

        Authority: process a request for a modifying a lease.
        @param reservation reservation for which to perform extend lease
        @throws Exception
        """
        try:
            reservation.modify_lease()
            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_modify_lease()
        except Exception as e:
            self.error("An error occurred during modify lease for reservation #{}".format(reservation.get_reservation_id()), e)

    def extend_reservation(self, rid: ID, resources: ResourceSet, term: Term) -> int:
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
        real = self.reservations.get(rid)
        ticket = True

        if real is None:
            raise Exception("Unknown reservation rid: {}".format(rid))

        # check for a pending operation: we cannot service the extend if there is another operation in progress.
        if real.get_pending_state() != ReservationPendingStates.None_:
            return Constants.ReservationHasPendingOperation

        # attach the desired extension term and resource set
        real.set_approved(term, resources)
        # notify the policy that a reservation is about to be extended
        self.policy.extend(real, resources, term)

        if isinstance(real, AuthorityReservation):
            ticket = False

        if ticket:
            real.extend_ticket(self.plugin.get_actor())
        else:
            real.extend_lease()

        self.plugin.get_database().update_reservation(real)

        if not real.is_failed():
            if ticket:
                real.service_extend_ticket()
            else:
                real.service_extend_lease()

        return 0

    def extend_ticket(self, reservation: IKernelReservation):
        """
        Handles an extend ticket operation for the reservation.
        Client: issue an extend ticket request.
        Broker: process a request for a ticket extension.
        @param reservation reservation for which to perform extend ticket
        @throws Exception
        """
        try:
            if reservation.can_renew():
                reservation.extend_ticket(self.plugin.get_actor())
            else:
                raise Exception("The reservation state prevents it from extending its ticket.")

            self.plugin.get_database().update_reservation(reservation)

            if not reservation.is_failed():
                reservation.service_extend_ticket()
        except Exception as e:
            self.error("An error occurred during extend ticket for reservation #{}".format(reservation.get_reservation_id()), e)

    def get_client_slices(self) -> list:
        """
        Returns all client slices.
        @return an array of client slices
        """
        return self.slices.get_client_slices()

    def get_inventory_slices(self) -> list:
        """
        Returns all inventory slices.
        @return an array of inventory slices
        """
        return self.slices.get_inventory_slices()

    def get_local_slice(self, slice_object: ISlice) -> IKernelSlice:
        """
        Returns the slice object registered with the kernel that corresponds to
        the argument.
        @param slice_object incoming slice object
        @return the locally registered slice object
        @throws IllegalArgumentException if the arguments are invalid
        @throws Exception if no locally registered slice object exists
        """
        if slice_object is None or slice_object.get_slice_id() is None:
            raise Exception("Invalid Argument")

        return self.slices.get(slice_object.get_slice_id(), True)

    def get_or_create_local_slice(self, identity: AuthToken, reservation: IKernelReservation, create_new_slice: bool) -> IKernelSlice:
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

        result = self.get_slice(slice_id)
        if result is None:
            if create_new_slice:
                result = self.plugin.create_slice(slice_id, slice_name, ResourceData())
                if reservation.get_slice().is_broker_client():
                    result.set_broker_client()
                else:
                    if reservation.get_slice().is_client():
                        result.set_client()
            else:
                result = reservation.get_kernel_slice()

            result.set_owner(identity)
            self.register_slice(result)
        return result

    def get_reservation(self, rid: ID) -> IKernelReservation:
        """
        Returns the specified reservation.
        @param rid reservation id
        @return reservation
        """
        if rid is not None:
            self.reservations.get(rid)
        return None

    def get_reservations(self, slice_id: ID) -> list:
        """
        Returns all reservations in the specified slice.
        @param slice_id slice id
        @return an array of reservations
        """
        result = None
        if slice_id is not None:
            sl = self.slices.get(slice_id)
            result = sl.get_reservations_list()
        return result

    def get_plugin(self) -> IBasePlugin:
        """
        Returns the plugin.
        @return plugin object
        """
        return self.plugin

    def get_slice(self, slice_id: ID) -> IKernelSlice:
        """
        Returns a slice previously registered with the kernel.
        @param slice_id slice identifier
        @return slice object
        """
        if slice_id is None:
            raise Exception("Invalid argument")

        return self.slices.get(slice_id)

    def is_known_slice(self, slice_id: ID) -> bool:
        if slice_id is None:
            raise Exception("Invalid argument")
        return self.slices.contains(slice_id)

    def get_slices(self) -> list:
        """
        Returns all registered slices.
        @return an array of slices
        """
        return self.slices.get_slices()

    def handle_duplicate_request(self, current: IKernelReservation, operation: RequestTypes):
        """
        Handles a duplicate request.
        @param current reservation
        @param operation operation code
        """
        current.handle_duplicate_request(operation)

    def probe_pending(self, reservation: IKernelReservation):
        """
        Probes to check for completion of pending operation.
        @param reservation the reservation being probed
        @throws Exception rare
        """
        try:
            reservation.prepare_probe()
            reservation.probe_pending()
            self.plugin.get_database().update_reservation(reservation)
            reservation.service_probe()
        except Exception as e:
            traceback.print_exc()
            self.error("An error occurred during probe pending for reservation #{}".format(reservation.get_reservation_id()), e)

    def purge(self):
        """
        Purges all closed reservations.
        @throws Exception
        """
        for reservation in self.reservations.values():
            if reservation.is_closed():
                try:
                    reservation.get_kernel_slice().unregister(reservation)
                except Exception as e:
                    self.logger.error("An error occurred during purge for reservation #{}".format(reservation.get_reservation_id()), e)
                finally:
                    GlobalsSingleton.get().event_manager.dispatch_event(ReservationPurgedEvent(reservation))
                    self.reservations.remove(reservation)

    def query(self, properties: dict):
        """
        Processes a query request.
        @param properties query
        @return query response
        """
        return self.policy.query(properties)

    def redeem(self, reservation: IKernelControllerReservation):
        try:
            if reservation.can_redeem():
                reservation.reserve(self.policy)
            else:
                raise Exception("The current reservation state prevent it from being redeemed")

            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            self.logger.error(
                "An error occurred during redeem for reservation #{}".format(reservation.get_reservation_id()), e)

    def register(self, reservation: IKernelReservation, slice_object: IKernelSlice) -> bool:
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
        reservation.set_logger(self.logger)

        if not reservation.is_closed():
            # Note: as of now slice.register must be the first operation in
            # this method. slice.register will throw an exception if the
            # reservation is already present in the slice table.

            # register with the local slice
            slice_object.register(reservation)

            # register with the reservations table
            if self.reservations.contains(reservation):
                slice_object.unregister(reservation)
                raise Exception("There is already a reservation with the given identifier")

            self.reservations.add(reservation)

            # attach actor to the reservation
            reservation.set_actor(self.plugin.get_actor())
            # attach the local slice object
            reservation.set_slice(slice_object)
            add = True
        else:
            self.logger.warning("Attempting to register a closed reservation #{}".format(reservation.get_reservation_id()))

        return add

    def register_reservation(self, reservation: IKernelReservation):
        """
        Re-registers the reservation.
        @param reservation reservation
        @throws Exception
        """
        if reservation is None or reservation.get_reservation_id() is None or \
                reservation.get_slice() is None or reservation.get_slice().get_name() is None:
            raise Exception("Invalid argument")

        local_slice = None
        add = False

        local_slice = self.slices.get(reservation.get_slice().get_slice_id(), True)
        add = self.register(reservation, local_slice)

        if add :
            try:
                self.plugin.get_database().add_reservation(reservation)
            except Exception as e:
                self.unregister_no_check(reservation, local_slice)
                raise e

    def register_slice(self, slice_object: IKernelSlice):
        """
        Registers the specified slice with the kernel.
        @param slice_object slice to register
        @throws Exception if the slice cannot be registered
        """
        slice_object.prepare()
        self.slices.add(slice_object)

        try:
            self.plugin.get_database().add_slice(slice_object)
        except Exception as e:
            self.slices.remove(slice_object.get_slice_id())
            self.error("could not register slice", e)

    def remove_reservation(self, rid: ID):
        """
        Removes the reservation.
        @param rid reservation id.
        @throws Exception
        """
        if rid is None:
            raise Exception("Invalid Argument")

        real = self.reservations.get(rid)

        if real is not None:
            if real.is_closed() or real.is_failed() or real.get_state() == ReservationStates.CloseWait:
                self.unregister_reservation(rid)
            else:
                raise Exception("Only reservations in failed, closed, or closewait state can be removed.")

        self.plugin.get_database().remove_reservation(rid)

    def remove_slice(self, slice_id: ID):
        """
        Removes the specified slice.
        @param slice_id slice identifier
        @throws Exception if the slice contains active reservations or removal fails
        """
        if slice_id is None:
            raise Exception("Invalid argument")

        possible = False
        slice_object = self.get_slice(slice_id)

        if slice_object is None:
            self.plugin.get_database().remove_slice(slice_id)
        else:
            possible = (slice_object.get_reservations().size() == 0)

            if possible:
                # remove the slice from the slices table
                self.slices.remove(slice_id)
                # release any resources assigned to the slice: unlocked,
                # because it may be blocking. The plugin is responsible for
                # synchronization.
                self.plugin.release_slice(slice_object)
                # remove from the database
                self.plugin.get_database().remove_slice(slice_id)

    def re_register_reservation(self, reservation: IKernelReservation):
        """
        Re-registers the reservation.
        @param reservation reservation
        @throws Exception
        """
        if reservation is None or reservation.get_reservation_id() is None or \
                reservation.get_slice() is None or reservation.get_slice().get_slice_id() is None:
            raise Exception("Invalid argument")

        local_slice = None
        local_slice = self.slices.get(reservation.get_slice().get_slice_id(), True)

        if local_slice is None:
            raise Exception("slice not registered with the kernel")
        else:
            self.register(reservation, local_slice)

        # Check if the reservation has a database record.
        temp = None
        try:
            temp = self.plugin.get_database().get_reservation(reservation.get_reservation_id())
        except Exception as e:
            raise e

        if temp is None or len(temp) == 0:
            self.unregister_no_check(reservation, local_slice)
            raise Exception("The reservation has no database record")

    def re_register_slice(self, slice_object: IKernelSlice):
        """
        Re-registers the specified slice with the kernel.
        @param slice_object slice to re-register
        @throws Exception if the slice cannot be registered
        """
        slice_object.prepare()
        self.slices.add(slice_object)

        try:
            temp = self.plugin.get_database().get_slice(slice_object.get_slice_id())
            if temp is not None and len(temp) == 0:
                raise Exception("The slice does not have a database record")
        except Exception as e:
            try:
                self.lock.acquire()
                self.slices.remove(slice_object.get_slice_id())
            finally:
                self.lock.release()
            self.error("could not reregister slice", e)

    def reserve(self, reservation: IKernelReservation):
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
            reservation.reserve(self.policy)
            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_reserve()
        except Exception as e:
            traceback.print_exc()
            self.error("An error occurred during reserve for reservation #{}".format(reservation.get_reservation_id()), e)

    def soft_validate(self, reservation: IKernelReservation = None, rid: ID = None) -> IKernelReservation:
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
            raise Exception("Invalid arguments")

        result = None

        if reservation is not None:
            # Each local reservation is indexed in two places: (1) The reservation
            # set in the kernel, and (2) inside the local slice object. Here we
            # will check to see if the local slice exists and will retrieve the
            # reservation from the local slice.
            s = self.get_local_slice(reservation.get_slice())

            result = s.soft_lookup(reservation.get_reservation_id())

        if rid is not None:
            result = self.reservations.get(rid)

        return result

    def tick(self):
        """
        Timer interrupt.
        @throws Exception
        """
        try:
            for reservation in self.reservations.values():
                self.probe_pending(reservation)

            self.purge()
            self.check_nothing_pending()
        except Exception as e:
            traceback.print_exc()
            self. error("exception in Kernel.tick", e)

    def has_something_pending(self) -> bool:
        for reservation in self.reservations.values():
            if not reservation.is_terminal() and (reservation.is_nascent() or not reservation.is_no_pending()):
                return True

        return False

    def check_nothing_pending(self):
        if not self.has_something_pending():
            with self.nothing_pending:
                self.nothing_pending.notify_all()

    def await_nothing_pending(self):
        with self.nothing_pending:
            self.nothing_pending.wait()

    def unregister(self, reservation: IKernelReservation, slice_object: IKernelSlice):
        """
        Unregisters a reservation from the kernel data structures. Must be called
        with the kernel lock on. Performs state checks.
        @param reservation reservation to unregister
        @param slice_object local slice object
        @throws Exception
        """
        if reservation.is_closed() or reservation.is_failed() or reservation.get_state() == ReservationStates.CloseWait:
            slice_object.unregister(reservation)
            self.reservations.remove(reservation)
        else:
            raise Exception("Only reservations in failed, closed, or closewait state can be unregistered.")

    def unregister_no_check(self, reservation: IKernelReservation, slice_object: IKernelSlice):
        """
        Unregisters a reservation from the kernel data structures. Must be called
        with the kernel lock on. Does not perform state checks.
        @param reservation reservation to unregister
        @param slice_object local slice object
        @throws Exception
        """
        slice_object.unregister(reservation)
        self.reservations.remove(reservation)

    def unregister_reservation(self, rid: ID):
        """
        Unregisters the reservation
        @param rid reservation id
        @throws Exception
        """
        if rid is None:
            raise Exception("Invalid argument")
        local_reservation = self.reservations.get_exception(rid)
        self.unregister(local_reservation, local_reservation.get_kernel_slice())

        self.policy.remove(local_reservation)

    def unregister_slice(self, slice_id: ID):
        """
        Unregisters the specified slice.
        @param slice_id slice id
        @throws Exception if the slice cannot be unregistered (it has active
                    reservations) or has not been previously registered with the
                    kernel
        """
        if slice_id is None:
            raise Exception("Invalid argument")

        s = self.get_slice(slice_id)

        if s is None:
            raise Exception("Trying to unregister a slice, which is not registered with the kernel")

        if s.get_reservations().size() == 0:
            self.slices.remove(slice_id)
            self.plugin.release_slice(s)
        else:
            raise Exception("Slice cannot be unregistered: not empty")

    def update_lease(self, reservation: IKernelReservation, update: Reservation, update_data: UpdateData):
        """
        Handles an incoming update lease operation (client side only).
        @param reservation local reservation
        @param update update sent from site authority
        @param update_data status of the operation the authority is informing us about
        @throws Exception
        """
        try:
            self.logger.debug("updateLease: Incoming term {}".format(update.get_term()))
            reservation.update_lease(update, update_data)

            # NOTE: the database update has to happen BEFORE the service
            # update: we need to record the fact that we received a concrete
            # set, so that on recovery we can go and recover the concrete set.
            # If the database update is after the service call, we may end up
            # in Ticketed, Redeeming with state in the database that will
            # prevent us from incorporating a leaseUpdate.

            self.plugin.get_database().update_reservation(reservation)

            if not reservation.is_failed():
                reservation.service_update_lease()
        except Exception as e:
            self.error("An error occurred during update lease for reservation # {}".format(reservation.get_reservation_id()), e)

    def update_ticket(self, reservation: IKernelReservation, update: Reservation, update_data: UpdateData):
        """
        Handles an incoming update ticket operation (client side only).
        @param reservation local reservation
        @param update update sent from upstream broker
        @param update_data status of the operation the broker is informing us about
        @throws Exception
        """
        try:
            reservation.update_ticket(update, update_data)
            self.plugin.get_database().update_reservation(reservation)
            if not reservation.is_failed():
                reservation.service_update_ticket()
        except Exception as e:
            self.error("An error occurred during update ticket for reservation # {}".format(reservation.get_reservation_id()), e)

    def validate(self, reservation: IKernelReservation = None, rid: ID = None):
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
            raise Exception("Invalid argument")

        if reservation is not None:
            local = self.soft_validate(reservation=reservation)
            if local is None:
                self.error("reservation not found", None)
            return local

        if rid is not None:
            local = self.soft_validate(rid=rid)
            if local is None:
                self.error("reservation not found", None)
            return local

    def handle_failed_rpc(self, reservation: IKernelReservation, rpc: FailedRPC):
        reservation.handle_failed_rpc(rpc)
