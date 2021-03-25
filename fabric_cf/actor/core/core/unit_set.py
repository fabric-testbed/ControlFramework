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

import traceback
from datetime import datetime
from typing import TYPE_CHECKING

from fabric_cf.actor.core.common.exceptions import UnitException
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.notice import Notice

from fabric_cf.actor.core.apis.abc_concrete_set import ABCConcreteSet
from fabric_cf.actor.core.core.unit import UnitState, Unit

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_authority_proxy import ABCAuthorityProxy
    from fabric_cf.actor.core.apis.abc_base_plugin import ABCBasePlugin
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.time.term import Term
    from fabric_cf.actor.core.util.resource_type import ResourceType


class UnitSet(ABCConcreteSet):
    """
    Represents the unit in a reservation
    """
    def __init__(self, *, plugin: ABCBasePlugin, units: dict = None):
        self.units = units
        if self.units is None:
            self.units = {}
        self.reservation = None
        self.plugin = plugin
        self.logger = None
        if self.plugin is not None:
            self.logger = self.plugin.logger
        self.is_closed = False
        self.is_fresh = False
        self.released = None

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['logger']
        del state['plugin']
        del state['reservation']
        del state['released']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.logger = None
        self.plugin = None
        self.reservation = None
        self.released = None

    def __str__(self):
        result = "[UnitSet = "
        for unit in self.units:
            result += f"[{unit}],"
        result += "]"
        return result

    def restore(self, *, plugin: ABCBasePlugin, reservation: ABCReservationMixin):
        """
        Restore post read from database
        @param plugin plugin
        @param reservation reservation
        """
        self.plugin = plugin
        if plugin is not None:
            self.logger = plugin.get_logger()
        self.reservation = reservation

    def ensure_type(self, *, cset: ABCConcreteSet):
        """
        Validate the type of incoming concrete set
        @param cset cset
        """
        if not isinstance(cset, UnitSet):
            raise UnitException("Must be UnitSet")

    def add_unit(self, *, u: Unit):
        """
        Add a unit
        @parm u unit
        """
        if u.get_id() not in self.units:
            self.units[u.get_id()] = u

    def add(self, *, concrete_set: ABCConcreteSet, configure: bool):
        self.ensure_type(cset=concrete_set)

        self.add_from_dict(units=concrete_set.units)

        if configure:
            self.transfer_in_units(units=concrete_set.units)

    def add_from_dict(self, *, units: dict):
        """
        Add units from a dictionary
        @param units units
        """
        self.is_fresh = False
        for u in units.values():
            self.add_unit(u=u)

    def missing(self, *, units: dict) -> dict:
        """
        Find units not present in incoming units
        @param units incoming units
        """
        result = {}
        for u in self.units.values():
            if u.get_id() not in units:
                result[u.get_id()] = u
        return result

    def change(self, *, concrete_set: ABCConcreteSet, configure: bool):
        if not isinstance(concrete_set, UnitSet):
            raise UnitException("Must be UnitSet")

        lost = self.missing(units=concrete_set.units)
        gained = concrete_set.missing(units=self.units)

        for u in gained.values():
            u.set_state(state=UnitState.DEFAULT)

        if len(gained) == 0 and len(lost) == 0:
            self.logger.debug("Updating properties on Controller side for modify or extend")
            self.update(units=concrete_set.units)

        self.remove_from_dict(units=lost, configure=configure)
        self.add_from_dict(units=gained)
        if concrete_set:
            self.transfer_in_units(units=gained)

    def update(self, *, units: dict):
        """
        Update the units
        @param units units to be updated
        """
        for u in units.values():
            u.set_reservation(reservation=self.reservation)
            u.set_slice_id(slice_id=self.reservation.get_slice_id())
            u.set_actor_id(actor_id=self.plugin.get_actor().get_guid())
            self.plugin.update_props(reservation=self.reservation, unit=u)

    def clone(self):
        result = UnitSet(plugin=self.plugin, units=self.units.copy())
        result.is_closed = self.is_closed
        result.is_fresh = self.is_fresh
        result.reservation = self.reservation
        return result

    def clone_empty(self) -> UnitSet:
        result = UnitSet(plugin=self.plugin)
        result.is_fresh = True
        return result

    def close(self):
        lost = self.units.copy()
        self.transfer_out_units(units=lost)
        self.is_closed = True

    def collect_released(self) -> ABCConcreteSet:
        result = None
        if self.released is not None and len(self.released) > 0:
            result = UnitSet(plugin=self.plugin, units=self.released)
            self.released = None

        return result

    def select_extract(self, *, count: int, victims: str) -> dict:
        """
        Extract specified victims
        @param count count
        @param victims token string identifying the victims
        """
        num_taken = 0
        taken = {}

        if victims is not None:
            for v in victims.split(" "):
                uid = ID(uid=v)
                if uid in self.units:
                    taken[uid] = self.units[uid]
                    num_taken += 1

        for u in self.units.values():
            if num_taken == count:
                break

            if u.get_id() not in taken:
                taken[u.get_id()] = u
                num_taken += 1

        return taken

    def get_notices(self) -> Notice:
        result = Notice()
        for u in self.units.values():
            n = u.get_notices()
            if not n.is_empty():
                result.add(msg=n.get_notice())
        return result

    def get_site_proxy(self) -> ABCAuthorityProxy:
        return None

    def get_units(self) -> int:
        return len(self.units)

    def holding(self, *, when: datetime) -> int:
        return self.get_units()

    def get_pending_count(self) -> int:
        """
        Get Pending Action Count
        """
        count = 0
        for u in self.units.values():
            if u.has_pending_action():
                count += 1

        return count

    def is_active(self) -> bool:
        return not self.is_fresh and self.reservation is not None and self.get_pending_count() == 0

    def modify(self, *, concrete_set: ABCConcreteSet, configure: bool):
        self.ensure_type(cset=concrete_set)

        for u in concrete_set.units.values():
            if u.get_id() in self.units:
                self.units[u.get_id()].set_modified(u)
                if configure:
                    self.modify_unit(u=self.units[u.get_id()])
            else:
                self.logger.warning("Modify for unit not present in seet: {}".format(u.get_id()))

    def probe(self):
        rel = None
        for u in self.units.values():
            if u.is_closed() or u.is_failed():
                if rel is None:
                    rel = {}
                rel[u.get_id()] = u

        if rel is not None:
            if self.released is None:
                self.released = rel
            else:
                for u in rel.values():
                    self.units.pop(u.get_id())

            for u in rel.values():
                self.units.pop(u.get_id())

    def remove(self, *, concrete_set: ABCConcreteSet, configure: bool):
        self.ensure_type(cset=concrete_set)
        self.is_fresh = False

        self.remove_from_dict(units=configure.units, configure=configure)

    def remove_from_dict(self, *, units: dict, configure: bool):
        """
        Remove units from units dictionary
        @param units units to be removed
        @param configure flag to indicate if transfer_out to be triggerd or not
        """
        self.is_fresh = False
        for u in units.values():
            if u in self.units:
                self.units.pop(u)
            if configure:
                self.transfer_out(u=u)

    def setup(self, *, reservation: ABCReservationMixin):
        self.reservation = reservation

    def validate_concrete(self, *, rtype: ResourceType, units: int, term: Term):
        if self.get_units() < units:
            raise UnitException("Insufficient units")

    def validate_incoming(self):
        """
        Validate incoming unit
        """

    def validate_outgoing(self):
        """
        Validate an outgoing unit
        """

    def modify_unit(self, *, u: Unit):
        """
        Modify a unit
        @param u unit
        """
        try:
            u.start_modify()
            self.plugin.modify(reservation=self.reservation, u=u)
        except Exception as e:
            self.fail(u=u, message="Modify for node failed", e=e)

    def restart_actions(self):
        """
        Restart actions
        """
        for u in self.units.values():
            if u.get_state() == UnitState.ACTIVE:
                self.logger.debug("Do nothing")
            elif u.get_state() == UnitState.CLOSING:
                u.decrement_sequence()
                self.transfer_out(u=u)
            elif u.get_state() == UnitState.PRIMING or u.get_state() == UnitState.DEFAULT:
                u.decrement_sequence()
                self.transfer_in(unit=u)
            elif u.get_state() == UnitState.MODIFYING:
                u.decrement_sequence()
                self.modify_unit(u=u)
            elif u.get_state() == UnitState.FAILED or u.get_state() == UnitState.CLOSED:
                self.logger.debug("Do nothing")

    def transfer_in(self, *, unit: Unit):
        """
        Transfer in a unit
        @param unit unit
        """
        try:
            if unit.start_prime():
                unit.set_reservation(reservation=self.reservation)
                unit.set_slice_id(slice_id=self.reservation.get_slice_id())
                unit.set_actor_id(actor_id=self.plugin.get_actor().get_guid())
                self.plugin.transfer_in(reservation=self.reservation, unit=unit)
            else:
                self.post(u=unit, message="Unit cannot be transferred., State={}".format(unit.get_state()))
        except Exception as e:
            self.fail(u=unit, message="Transfer in for node failed", e=e)

    def post(self, *, u: Unit, message: str):
        """
        Post the notice
        @param u unit
        @param message message
        """
        self.logger.error(message)
        u.add_notice(notice=message)

    def fail(self, *, u: Unit, message: str, e: Exception = None):
        """
        Fail a unit and log error message
        @param u unit
        @param message message
        @param e exception
        """
        self.logger.error(message)
        if e is not None:
            self.logger.error(e)

        u.fail(message=message, exception=e)

    def transfer_in_units(self, *, units: dict):
        """
        Transfer in the units
        @param units units to be transferred
        """
        for u in units.values():
            self.transfer_in(unit=u)

    def transfer_out(self, *, u: Unit):
        """
        Transfer a unit
        @param u unit to be transferred
        """
        if u.transfer_out_started:
            return

        try:
            u.start_close()
            self.plugin.transfer_out(reservation=self.reservation, unit=u)
        except Exception as e:
            self.fail(u=u, message="tranferOut error", e=e)

    def transfer_out_units(self, *, units: dict):
        """
        Transfer out units
        @param units units to be transferred
        """
        for u in units.values():
            self.transfer_out(u=u)

    def get_set(self) -> dict:
        """
        Get a copy of the units
        """
        return self.units.copy()
