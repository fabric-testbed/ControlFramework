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

import threading
from typing import TYPE_CHECKING

from fabric.actor.core.apis.i_broker_reservation import IBrokerReservation
from fabric.actor.core.common.constants import Constants
from fabric.actor.core.kernel.reservation_states import ReservationStates
from fabric.actor.core.time.actor_clock import ActorClock
from fabric.actor.core.time.term import Term
from fabric.actor.core.util.prop_list import PropList
from fabric.actor.core.util.reservation_set import ReservationSet

if TYPE_CHECKING:
    from fabric.actor.core.apis.i_broker import IBroker
    from fabric.actor.core.policy.inventory_for_type import InventoryForType
    from fabric.actor.core.util.resource_type import ResourceType
    from fabric.actor.core.kernel.resource_set import ResourceSet

from fabric.actor.core.policy.broker_priority_policy import BrokerPriorityPolicy
from fabric.actor.core.policy.inventory import Inventory
from fabric.actor.core.apis.i_client_reservation import IClientReservation


class BrokerSimplerUnitsPolicy(BrokerPriorityPolicy):
    def __init__(self, *, actor: IBroker = None):
        super().__init__(actor=actor)
        self.inventory = Inventory()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['actor']
        del state['clock']
        del state['initialized']

        del state['delegations']
        del state['combined_broker_model']
        del state['for_approval']
        del state['lock']

        del state['calendar']

        del state['last_allocation']
        del state['allocation_horizon']
        del state['ready']
        del state['queue']
        del state['inventory']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.actor = None
        self.clock = None
        self.initialized = False

        self.delegations = {}
        self.combined_broker_model = None
        self.load_combined_broker_model()

        self.lock = threading.Lock()
        self.calendar = None

        if self.required_approval:
            self.for_approval = ReservationSet()

        self.last_allocation = -1
        self.allocation_horizon = 0
        self.ready = False

        self.queue = None
        self.inventory = Inventory()

        # TODO Fetch Actor object and setup logger, actor and clock member variables

    def donate_reservation(self, *, reservation: IClientReservation):
        super().donate_reservation(reservation=reservation)
        self.inventory.get_new(reservation=reservation)

    def allocate(self, *, cycle: int):
        if self.get_next_allocation(cycle=cycle) != cycle:
            return

        self.last_allocation = cycle

        start_cycle = self.get_start_for_allocation(allocation_cycle=cycle)
        advance_cycle = self.get_end_for_allocation(allocation_cycle=cycle)
        requests = self.calendar.get_all_requests(cycle=advance_cycle)

        if requests is None or requests.size() == 0:
            if self.queue is None or self.queue.size() == 0:
                self.logger.debug("no requests for auction start cycle {}".format(start_cycle))
                return

        self.logger.debug("allocating resources for cycle {}".format(start_cycle))

        self.allocate_extending_reservation_set(requests=requests, start_cycle=start_cycle)
        self.allocate_queue(start_cycle=start_cycle)
        self.allocate_ticketing(requests=requests, start_cycle=start_cycle)

    '''
    def query(self, *, p: dict) -> dict:
        self.logger.debug("Processing Query with properties: {}".format(p))
        action = self.get_query_action(properties=p)
        if action.lower() != Constants.QueryActionDiscoverPools:
            return super().query(p=p)

        response = self.inventory.get_resource_pools()
        response[Constants.QueryResponse] = Constants.QueryActionDiscoverPools

        self.logger.debug("Returning Query Result: {}".format(response))
        return response
    '''

    def get_default_pool_id(self) -> ResourceType:
        result = None

        if len(self.inventory.get_inventory().keys()) > 0:
            result = self.inventory.get_inventory().keys().__iter__().__next__()

        return result

    def allocate_extending_reservation_set(self, *, requests: ReservationSet, start_cycle: int):
        if requests is not None:
            for reservation in requests.values():
                if reservation.is_extending_ticket() and not reservation.is_closed():
                    start = reservation.get_requested_term().get_new_start_time()
                    end = self.align_end(when=reservation.get_requested_term().get_end_time())

                    pool_id = self.get_current_pool_id(reservation=reservation)

                    inv = self.inventory.get(resource_type=pool_id)

                    if inv is not None:
                        ext_term = Term(start=reservation.get_term().get_start_time(), end=end, new_start=start)
                        self.extend(reservation=reservation, inv=inv, term=ext_term)
                    else:
                        reservation.fail(message="there is no pool to satisfy this request")

    def allocate_ticketing(self, *, requests: ReservationSet, start_cycle: int):
        if requests is not None:
            for reservation in requests.values():
                if not reservation.is_ticketing():
                    continue

                if self.ticket(reservation=reservation, start_cycle=start_cycle):
                    continue

                if self.queue is None:
                    if not reservation.is_failed():
                        reservation.fail(message="Insufficient resources")
                        continue

                if not reservation.is_failed():
                    if PropList.is_elastic_time(rset=reservation.get_requested_resources()):
                        self.logger.debug("Adding reservation + {} to the queue".format(reservation.get_reservation_id()))
                        self.queue.add(reservation=reservation)
                    else:
                        reservation.fail("Insufficient resources for specified start time, Failing reservation: {}".format(reservation.get_reservation_id()))

    def allocate_queue(self, *, start_cycle: int):
        if self.queue is None:
            return

        for reservation in self.queue.values():
            if not self.ticket(reservation=reservation,start_cycle= start_cycle):
                request_properties = reservation.get_requested_resources().get_request_properties()
                threshold = request_properties[BrokerPriorityPolicy.QueueThreshold]
                start = self.clock.cycle(when=reservation.get_requested_term().get_new_start_time())

                if threshold != 0 and ((start_cycle - start) > threshold):
                    reservation.fail_warn(message="Request has exceeded its threshold on the queue {}".format(reservation))
                    self.queue.remove(reservation=reservation)
            else:
                self.queue.remove(reservation=reservation)

    def ticket(self, *, reservation: IBrokerReservation, start_cycle: int) -> bool:
        start_time = self.clock.date(cycle=start_cycle)

        self.logger.debug("cycle: {} new ticket request: {}".format(self.actor.get_current_cycle(), reservation))

        start = self.align_start(when=reservation.get_requested_term().get_new_start_time())
        end = self.align_end(when=reservation.get_requested_term().get_end_time())

        term = None
        if start < start_time and PropList.is_elastic_time(rset=reservation.get_requested_resources()):
            length = ActorClock.to_milliseconds(when=end) - ActorClock.to_milliseconds(when=start)

            start = self.clock.cycle_start_in_millis(cycle=start_cycle)
            term = Term(start=start, length=length)

        pool_id = reservation.get_requested_resources().get_type()
        if pool_id is None or not self.inventory.contains_type(resource_type=pool_id):
            pool_id = self.get_default_pool_id()

        if pool_id is not None:
            inv = self.inventory.get(resource_type=pool_id)

            if inv is not None:
                if term is None:
                    term = Term(start=start, end=end)

                return self.ticket_inventory(reservation=reservation, inv=inv, term=term)
            else:
                reservation.fail(message="there is no pool to satisfy this request")
        else:
            reservation.fail(message="there is no pool to satisfy this request")

        return False

    def ticket_inventory(self, *, reservation: IBrokerReservation, inv: InventoryForType, term: Term) -> bool:
        try:
            rset = reservation.get_requested_resources()
            needed = rset.get_units()
            available = inv.get_free()
            to_allocate = min(needed, available)

            if to_allocate == 0:
                return False

            if to_allocate < needed:
                if not PropList.is_elastic_size(rset=reservation.get_requested_resources()):
                    return False

            properties = inv.allocate(count=to_allocate, request=rset.get_request_properties())
            properties = PropList.merge_properties(incoming=inv.get_properties(), outgoing=properties)

            if to_allocate < needed:
                self.logger.error("partially satisfied request: allocated= {} needed={}".format(to_allocate, needed))

            self.issue_ticket(reservation=reservation, units=to_allocate, rtype=inv.get_type(), term=term,
                              properties=properties, source=inv.get_source())
            return True
        except Exception as e:
            self.logger.error(e)
            reservation.fail(message=str(e))
            return False

    def extend(self, *, reservation: IBrokerReservation, inv: InventoryForType, term: Term):
        try:
            rset = reservation.get_requested_resources()
            needed = rset.get_units()
            current = reservation.get_resources().get_units()
            difference = needed - current

            units = current

            properties = None

            if difference > 0:
                available = inv.get_free()
                to_allocate = min(difference, available)

                if to_allocate > 0:
                    properties = inv.allocate(count=to_allocate, request=rset.get_request_properties(),
                                              resource=rset.get_resource_properties())

                if to_allocate < difference:
                    self.logger.error("partially satisfied request: allocated= {} needed={}".format(to_allocate, difference))

                units += to_allocate
            elif difference < 0:
                properties = inv.free(count=-difference, request=rset.get_request_properties(),
                                      resource=rset.get_resource_properties())
                units += difference

            properties = PropList.merge_properties(incoming=inv.get_properties(), outgoing=properties)

            self.issue_ticket(reservation=reservation, units=units, rtype=inv.get_type(), term=term,
                              properties=properties, source=inv.get_source())
        except Exception as e:
            self.logger.error(e)
            reservation.fail(message=str(e), exception=e)

    def issue_ticket(self, *, reservation: IBrokerReservation, units: int, rtype: ResourceType,
                     term: Term, properties: dict, source: IClientReservation):

        delegation = self.actor.get_plugin().get_ticket_factory().make_delegation(units=units, term=term, rtype=rtype,
                                                                                  properties=properties,
                                                                                  holder=self.get_client_id(reservation=reservation))

        mine = self.extract(source=source.get_resources(), delegation=delegation)

        p = reservation.get_requested_resources().get_request_properties()
        mine.set_request_properties(p=p)

        if mine is not None and not reservation.is_failed():
            reservation.set_approved(term=term, approved_resources=mine)
            reservation.set_source(source=source)
            self.logger.debug("allocated: {} for term: {}".format(mine.get_units(), term))
            self.logger.debug("resourceshare= {} mine= {}".format(units, mine.get_units()))

            if self.required_approval:
                self.add_for_approval(reservation=reservation)
            else:
                self.add_to_calendar(reservation=reservation)
                reservation.set_bid_pending(value=False)
        else:
            if mine is None:
                raise Exception("There was an error extracting a ticket from the source ticket")

    def release(self, *, reservation):
        if isinstance(reservation, IBrokerReservation):
            self.logger.debug("Broker reservation")
            super().release(reservation=reservation)
            if reservation.is_closed_in_priming():
                self.logger.debug("Releasing resources (closed in priming)")
                self.release_resources(rset=reservation.get_approved_resources(),
                                       term=reservation.get_approved_term())
            else:
                self.logger.debug("Releasing resources")
                self.release_resources(rset=reservation.get_resources(),
                                       term=reservation.get_term())
        elif isinstance(reservation, IClientReservation):
            self.logger.debug("Client reservation")
            super().release(reservation=reservation)
            status = self.inventory.remove(source=reservation)
            self.logger.debug("Removing reservation: {} from inventory status: {}".format(reservation.get_reservation_id(), status))

    def release_not_approved(self, *, reservation: IBrokerReservation):
        super().release_not_approved(reservation=reservation)
        self.release_resources(rset=reservation.get_approved_resources(), term=reservation.get_approved_term())

    def release_resources(self, *, rset: ResourceSet, term: Term):
        try:
            if rset is None or term is None or rset.get_resources() is None:
                self.logger.warning("Reservation does not have resources to release")
                return
            inv = self.inventory.get(resource_type=rset.get_type())
            if inv is None:
                raise Exception("Cannot release resources: missing inventory")
            inv.free(count=rset.get_units(), resource=rset.get_resource_properties())
        except Exception as e:
            self.logger.error("release resources {}".format(e))

    def revisit_server(self, *, reservation: IBrokerReservation):
        super().revisit_server(reservation=reservation)

        if reservation.get_state() == ReservationStates.Ticketed:
            self.revisit_ticketed(reservation=reservation)

    def revisit_ticketed(self, *, reservation: IBrokerReservation):
        rset = reservation.get_resources()
        rtype = rset.get_type()
        inv = self.inventory.get(resource_type=rtype)
        if inv is None:
            raise Exception("cannot free resources: no inventory")

        inv.allocate_revisit(count=rset.get_units(), resource=rset.get_resource_properties())


