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
from datetime import datetime

from fabric_cf.actor.core.apis.i_authority_reservation import IAuthorityReservation
from fabric_cf.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric_cf.actor.core.apis.i_client_reservation import IClientReservation
from fabric_cf.actor.core.apis.i_reservation import IReservation
from fabric_cf.actor.core.core.authority_policy import AuthorityPolicy
from fabric_cf.actor.core.common.exceptions import AuthorityException
from fabric_cf.actor.core.kernel.resource_set import ResourceSet
from fabric_cf.actor.core.plugins.config.config_token import ConfigToken
from fabric_cf.actor.core.apis.i_resource_control import IResourceControl
from fabric_cf.actor.core.time.term import Term
from fabric_cf.actor.core.time.calendar.authority_calendar import AuthorityCalendar
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.reservation_set import ReservationSet
from fabric_cf.actor.core.util.resource_type import ResourceType


class AuthorityCalendarPolicy(AuthorityPolicy):
    """
    The base for authority policy implementations
    """
    UNSUPPORTED_RESOURCE_TYPE = "Unsupported resource type: {}"

    def __init__(self):
        """
        Creates a new instance.
        """
        super().__init__()
        # If true, we will use lazy revocation.
        self.lazy_close = False
        # Resource control objects indexed by guid. <guid, IResourceControl>
        self.controls_by_guid = {}
        # ResourceControl objects indexed by resource type. <type, IResourceControl>
        self.controls_by_resource_type = {}
        # The authority's calendar. A calendar of all requests
        self.calendar = None
        # Says if the actor has been initialized
        self.initialized = False

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['tickets']

        del state['controls_by_resource_type']
        del state['calendar']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.tickets = None

        self.controls_by_resource_type = {}
        self.restore()

    def restore(self):
        """
        Custom restore function. Invoked during recovering the policy object.
        """
        for c in self.controls_by_guid.values():
            try:
                self.register_control_types(control=c)
            except Exception as e:
                raise AuthorityException("Cannot restore resource control e:{}".format(e))

    def initialize(self):
        """
        initialize the policy
        """
        if not self.initialized:
            super().initialize()
            self.calendar = AuthorityCalendar(clock=self.clock)
            self.initialize_controls()
            self.initialized = True

    def initialize_controls(self):
        """
        Initializes all registered controls.
        @raises Exception in case of error
        """
        for control in self.controls_by_guid.values():
            control.set_actor(actor=self.actor)
            control.initialize()

    def donate(self, *, resources: ResourceSet):
        super().donate(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.donate(resource_set=resources)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def eject(self, *, resources: ResourceSet):
        code = super().unavailable(resources=resources)
        if code == 0:
            rc = self.get_control_by_type(rtype=resources.get_type())
            if rc is not None:
                code = rc.unavailable(resource_set=resources)
            else:
                raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))
        return code

    def available(self, *, resources: ResourceSet):
        super().available(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.available(resource_set=resources)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def freed(self, *, resources: ResourceSet):
        super().freed(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.freed(resource_set=resources)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def release(self, *, resources: ResourceSet):
        super().release(resources=resources)
        rc = self.get_control_by_type(rtype=resources.get_type())
        if rc is not None:
            rc.release(resource_set=resources)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(resources.get_type()))

    def recovery_starting(self):
        super().recovery_starting()
        for c in self.controls_by_guid.values():
            c.recovery_starting()

    def revisit(self, *, reservation: IReservation):
        super().revisit(reservation=reservation)
        if isinstance(reservation, IAuthorityReservation):
            self.calendar.add_closing(reservation=reservation, cycle=self.get_close(term=reservation.get_term()))
            approved = reservation.get_approved_resources()
            if approved is None:
                self.logger.debug("Reservation has no approved resources. Nothing is allocated to it.")

            rtype = approved.get_type()
            self.logger.debug("Resource type for recovered reservation: " + rtype)
            control = self.get_control_by_type(rtype=rtype)
            if control is None:
                raise AuthorityException("Missing resource control")
            control.revisit(reservation=reservation)

    def recovery_ended(self):
        super().recovery_ended()
        for c in self.controls_by_guid.values():
            c.recovery_ended()

    def donate_reservation(self, *, reservation: IClientReservation):
        super().donate_reservation(reservation=reservation)
        rc = self.get_control_by_type(rtype=reservation.get_type())
        if rc is not None:
            rc.donate_reservation(reservation=reservation)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def bind(self, *, reservation: IAuthorityReservation) -> bool:
        if isinstance(reservation, IBrokerReservation):
            return super().bind(reservation=reservation)
        # Simple for now: make sure that this is a valid term and do not modify
        # its start/end time and add it to the calendar. If the request came
        # after its start time, but before its end cycle, add it for allocation
        # to lastAllocatedCycle + 1. If it came after its end cycle, throw.
        current_cycle = self.actor.get_current_cycle()
        approved = reservation.get_requested_term()
        start = self.clock.cycle(when=approved.get_new_start_time())

        if start <= current_cycle:
            end = self.clock.cycle(when=approved.get_end_time())
            if end <= current_cycle:
                self.error(message="The request cannot be redeemed: its term has expired")
            start = current_cycle + 1

        self.calendar.add_request(reservation=reservation, cycle=start)
        close = self.get_close(term=reservation.get_requested_term())
        self.calendar.add_closing(reservation=reservation, cycle=close)
        return False

    def extend(self, *, reservation: IReservation, resources: ResourceSet, term: Term):
        # Simple for now: make sure that this is a valid term and do not modify
        # its start/end time and add it to the calendar. If the request came
        # after its start time, but before its end cycle, add it for allocation
        # to lastAllocatedCycle + 1. If it came after its end cycle, throw an
        # exception.

        if resources is not None and term is not None:
            raise AuthorityException("Not implemented")
        current_cycle = self.actor.get_current_cycle()
        approved = reservation.get_requested_term()
        start = self.clock.cycle(when=approved.get_new_start_time())

        if start <= current_cycle:
            end = self.clock.cycle(when=approved.get_end_time())
            if end <= current_cycle:
                self.error(message="The request cannot be redeemed: its term has expired")
            start = current_cycle + 1

        self.calendar.remove_closing(reservation=reservation)
        self.calendar.add_request(reservation=reservation, cycle=start)
        close = self.get_close(term=reservation.get_requested_term())
        self.calendar.add_closing(reservation=reservation, cycle=close)
        return True

    def correct_deficit(self, *, reservation: IAuthorityReservation):
        if reservation.get_resources() is not None:
            rc = self.get_control_by_type(rtype=reservation.get_resources().get_type())
            if rc is not None:
                self.finish_correct_deficit(rset=rc.correct_deficit(reservation=reservation), reservation=reservation)
            else:
                raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def close(self, *, reservation: IReservation):
        self.calendar.remove_schedule_or_in_progress(reservation=reservation)
        if reservation.get_type() is not None:
            rc = self.get_control_by_type(rtype=reservation.get_type())
            if rc is not None:
                rc.close(reservation=reservation)
            else:
                raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def closed(self, *, reservation: IReservation):
        if isinstance(reservation, IAuthorityReservation):
            self.calendar.remove_outlay(reservation=reservation)

    def remove(self, *, reservation: IReservation):
        raise AuthorityException("Not implemented")

    def finish(self, *, cycle: int):
        super().finish(cycle=cycle)
        self.calendar.tick(cycle=cycle)

    def assign(self, *, cycle: int):
        try:
            requests = self.get_requests(cycle=cycle)
            self.map_for_cycle(requests=requests, cycle=cycle)
        except Exception as e:
            self.logger.error("error in assign: {}".format(e))

    def map_for_cycle(self, *, requests: ReservationSet, cycle: int):
        """
        Orders mapper request processing for this cycle.
        
        @params requests The requests for this cycle
        @params cycle The cycle
        @raises Exception in case of error
        """
        if requests is None or cycle == 0:
            # self.logger.debug("Authority requests for cycle {} = [none]".format(cycle))
            return

        # self.logger.debug("Authority requests for cycle {} = {}".format(cycle, requests))

        self.map_shrinking(bids=requests)
        self.map_growing(bids=requests)

    def map_shrinking(self, *, bids: ReservationSet):
        """
        Maps reservations that are shrinking or staying the same (extending with
        no flex) in this cycle, and removes them from the bid set.
        
        @param bids set of deferred operations for this cycle (non-null)
        @raises Exception in case of error
        """
        # self.logger.debug("Processing shrinking requests")
        rids_to_remove = []
        for reservation in bids.values():
            adjust = reservation.get_deficit()
            if adjust > 0:
                continue
            if not reservation.is_terminal() and reservation.is_extending_lease():
                if adjust < 0:
                    self.logger.debug("**Shrinking reservation by {}:{}".format(adjust, reservation))
                else:
                    self.logger.debug("**Extending reservation (no flex): {}".format(reservation))
                self.map(reservation=reservation)
                rids_to_remove.append(reservation.get_reservation_id())

        for rid in rids_to_remove:
            bids.remove_by_rid(rid=rid)

    def map_growing(self, *, bids: ReservationSet):
        """
        Maps reservations that are growing in this cycle (redeems or expanding
        extends), and removes them from the bid set.
        
        @param bids set of deferred operations for this cycle (non-null)
        @throws Exception in case of error
        """
        # self.logger.debug("Processing growing requests")
        rids_to_remove = []
        for reservation in bids.values():
            if reservation.is_terminal():
                continue
            adjust = reservation.get_deficit()

            if adjust > 0:
                if reservation.is_extending_lease():
                    self.logger.debug("**Growing reservation by {}:{}".format(adjust, reservation))
                else:
                    self.logger.debug("**Redeeming reservation by {}:{}".format(adjust, reservation))
            self.map(reservation=reservation)
            rids_to_remove.append(reservation.get_reservation_id())

        for rid in rids_to_remove:
            bids.remove_by_rid(rid=rid)

    def map(self, *, reservation: IAuthorityReservation):
        """
        Maps a reservation. Indicates we will approve the request: update its
        expire time in the calendar, and issue a map probe. The map probe will
        result in a retry of the mapper request through bind or
        <code>extend</code> above, which will release the request to the
        associated mapper.
        
        @param reservation: the reservation
        @throws Exception in case of error
        """
        assigned = self.assign_reservation(reservation=reservation)
        if assigned is not None:
            approved = reservation.get_requested_term()
            reservation.set_approved(term=approved, approved_resources=assigned)
            reservation.set_bid_pending(value=False)
        else:
            if not reservation.is_terminal():
                self.logger.debug("Deferring reservation {} for the next cycle: {}".format(
                    reservation, self.actor.get_current_cycle() + 1))
                self.reschedule(reservation=reservation)

    def assign_reservation(self, *, reservation: IAuthorityReservation):
        """
        Assign resources for the given reservation
        
        @params reservation
                   the request
        @returns a set of resources for the request
        @raises Exception in case of error
        """
        rc = self.get_control_by_type(rtype=reservation.get_requested_resources().get_type())
        if rc is not None:
            try:
                return rc.assign(reservation=reservation)
            except Exception as e:
                traceback.print_exc()
                self.logger.error("Could not assign {}".format(e))
                return None
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(reservation.get_type()))

    def configuration_complete(self, *, action: str, token: ConfigToken, out_properties: dict):
        super().configuration_complete(action=action, token=token, out_properties=out_properties)
        rc = self.get_control_by_type(rtype=token.get_resource_type())
        if rc is not None:
            rc.configuration_complete(action=action, token=token, out_properties=out_properties)
        else:
            raise AuthorityException(self.UNSUPPORTED_RESOURCE_TYPE.format(token.get_resource_type()))

    def is_expired(self, *, reservation: IReservation) -> bool:
        """
        See if a reservation has expired
        
        @params reservation: reservation
        @return true if the reservation expired; otherwise, return false
        @raises Exception in case of error
        """
        now = datetime.utcnow()
        end = reservation.get_term().get_end_time()

        return now > end

    def reschedule(self, *, reservation: IAuthorityReservation):
        """
        Reschedule a reservation into the calendar
        
        @param reservation the reservation
        """
        self.calendar.remove(reservation=reservation)
        self.calendar.add_request(reservation=reservation, cycle=self.actor.get_current_cycle() + 1)

    def get_close(self, *, term: Term) -> int:
        """
        Return the cycle when a term closes
        
        @params term: the term
        @returns the cycle of the end of a term
        """
        if self.lazy_close:
            return -1
        else:
            return self.clock.cycle(when=term.get_end_time()) + 1

    def get_closing(self, *, cycle: int) -> ReservationSet:
        return self.calendar.get_closing(cycle=cycle)

    def get_requests(self, *, cycle: int) -> ReservationSet:
        return self.calendar.get_requests(cycle=cycle)

    def get_control_by_id(self, *, guid: ID) -> IResourceControl:
        return self.controls_by_guid.get(guid, None)

    def get_control_by_type(self, *, rtype: ResourceType) -> IResourceControl:
        return self.controls_by_resource_type.get(rtype, None)

    def get_control_types(self):
        """
        Returns a reverse map of resource control to resource types. The table is
        indexed by the resource control object and each entry is a linked list of
        resource types.
        
        @returns a table of all of the different control types
        """
        result = {}
        for key, value in self.controls_by_resource_type.items():
            if value not in result:
                result[value] = []
            result[value].append(key)
        return result

    def register_control(self, *, control: IResourceControl):
        """
        Registers the given control for the specified resource type. If the
        policy plugin has already been initialized, the control should be
        initialized.
        
        @param control: the control
        @raises ConfigurationException in case of error
        """
        self.register_control_types(control=control)
        self.controls_by_guid[control.get_guid()] = control

    def register_control_types(self, *, control: IResourceControl):
        types = control.get_types()
        if types is None or len(types) == 0:
            raise AuthorityException("Resource control does not specify any types")
        for t in types:
            if t is None:
                raise AuthorityException("Invalid resource type specified")

        index = 0
        try:
            for rtype in types:
                if rtype in self.controls_by_resource_type:
                    raise AuthorityException("There is already a control associated with resource type {}".format(rtype))
                self.controls_by_resource_type[rtype] = control
                index += 1
        except Exception as e:
            j = 0
            for t in types:
                if t in self.controls_by_resource_type:
                    self.controls_by_resource_type.pop(t)
                    j += 1
                if j == index:
                    break
            raise e
