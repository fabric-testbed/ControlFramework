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

from abc import abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, List, Tuple, Dict

from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.poa_info_avro import PoaInfoAvro
from fabric_mb.message_bus.messages.site_avro import SiteAvro

from fabric_cf.actor.core.apis.abc_component import ABCComponent
from fabric_cf.actor.core.container.maintenance import Site

if TYPE_CHECKING:
    from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
    from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
    from fabric_mb.message_bus.messages.slice_avro import SliceAvro
    from fabric_cf.actor.core.util.id import ID


class ABCMgmtActor(ABCComponent):
    @abstractmethod
    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, email: str = None, project: str = None,
                   states: List[int] = None, limit: int = None, offset: int = None,
                   user_id: str = None, search: str = None, exact_match: bool = False) -> List[SliceAvro] or None:
        """
        Obtains all slices.
        @param slice_id slice id
        @param slice_name slice name
        @param email email
        @param project project id
        @param states slice states
        @param limit limit
        @param offset offset
        @param user_id user_id
        @param search: search term applied
        @param exact_match: Exact Match for Search term
        @return returns list of slices
        """

    def increment_metrics(self, *, project_id: str, oidc_sub: str, slice_count: int = 1) -> bool:
        """
        Add or update metrics

        @param project_id project id
        @param oidc_sub oidc sub
        @param slice_count slice_count

        @return true or false

        @throws Exception in case of error
        """
        raise NotImplementedError

    def get_metrics(self, *, project_id: str, oidc_sub: str, excluded_projects: List[str] = None) -> list:
        """
        Get metrics

        @param project_id project id
        @param oidc_sub oidc sub
        @param excluded_projects excluded_projects

        @return list of metric information

        @throws Exception in case of error
        """
        raise NotImplementedError

    def get_slice_count(self, *, email: str = None, project: str = None, states: List[int] = None,
                        user_id: str = None, excluded_projects: List[str] = None) -> int:
        """
        Obtains slice count.
        @param email email
        @param project project id
        @param states slice states
        @param user_id user_id
        @param excluded_projects excluded_projects
        @return returns list of slices
        """
        raise NotImplementedError

    @abstractmethod
    def add_slice(self, *, slice_obj: SliceAvro) -> ID:
        """
        Adds a new slice
        @param slice_obj slice
        @return returns slice id
        """

    @abstractmethod
    def remove_slice(self, *, slice_id: ID) -> bool:
        """
        Removes the specified slice
        @param slice_id slice id
        @return true for success; false otherwise
        """

    @abstractmethod
    def delete_slice(self, *, slice_id: ID) -> bool:
        """
        Deletes the specified slice - moves the slice into Closing state.
        @param slice_id slice ID
        @return true for success; false otherwise
        """

    @abstractmethod
    def update_slice(self, *, slice_obj: SliceAvro, modify_state: bool = False) -> bool:
        """
        Updates the specified slice.
        The only updatable slice attributes are:
        - description
        - all properties lists
        @param slice_obj slice
        @param modify_state - trigger state change
        @return true for success; false otherwise
        """

    @abstractmethod
    def accept_update_slice(self, *, slice_id: ID) -> bool:
        """
        Accept the last slice update
        @param slice_id slice_id
        @return true for success; false otherwise
        """

    @abstractmethod
    def get_reservations(self, *, states: List[int] = None, slice_id: ID = None,
                         rid: ID = None, oidc_claim_sub: str = None, email: str = None, rid_list: List[str] = None,
                         type: str = None, site: str = None, node_id: str = None,
                         host: str = None, ip_subnet: str = None, full: bool = False,
                         start: datetime = None, end: datetime = None) -> List[ReservationMng]:
        """
        Get Reservations
        @param states states
        @param slice_id slice ID
        @param rid reservation id
        @param oidc_claim_sub: oidc claim sub
        @param email: user email
        @param rid_list: list of Reservation Id
        @param type type of reservations like NodeSliver/NetworkServiceSliver
        @param site site
        @param node_id node id
        @param ip_subnet ip subnet
        @param host host
        @param full
        @param start: start time
        @param end: end time
        Obtains all reservations
        @return returns list of the reservations
        """

    def get_components(self, *, node_id: str, rsv_type: list[str], states: list[int],
                       component: str = None, bdf: str = None, start: datetime = None,
                       end: datetime = None, excludes: List[str] = None) -> Dict[str, List[str]]:
        """
        Returns components matching the search criteria
        @param node_id: Worker Node ID to which components belong
        @param states: list of states used to find reservations
        @param rsv_type: type of reservations
        @param component: component name
        @param bdf: Component's PCI address
        @param start: start time
        @param end: end time
        @param excludes: Excludes the list of reservations
        NOTE# For P4 switches; node_id=node+renc-p4-sw  component=ip+192.168.11.8 bdf=p1

        @return Dictionary with component name as the key and value as list of associated PCI addresses in use.
        """
        raise NotImplementedError

    @abstractmethod
    def get_sites(self, *, site: str) -> List[SiteAvro] or None:
        """
        @param site site name
        Obtains Maintenance Info for a site
        @return returns list of the Sites
        """

    @abstractmethod
    def update_reservation(self, *, reservation: ReservationMng) -> bool:
        """
        Updates the specified reservation
        The fields that can be updated are:

        - all properties lists

        @param reservation reservation to be updated
        @return true for success; false otherwise
        """

    @abstractmethod
    def close_reservation(self, *, rid: ID) -> bool:
        """
        Closes the specified reservation
        @param rid reservation id
        @return true for success; false otherwise
        """

    @abstractmethod
    def close_delegation(self, *, did: str) -> bool:
        """
        Closes the specified delegation
        @param did delegation id
        @return true for success; false otherwise
        """

    @abstractmethod
    def close_reservations(self, *, slice_id: ID) -> bool:
        """
        Closes all reservations in the specified slice.
        @param slice_id slice ID
        @return true for success; false otherwise
        """

    @abstractmethod
    def remove_reservation(self, *, rid: ID) -> bool:
        """
        Removes the specified reservation.
        Note only closed reservations can be removed.
        @param rid reservation id of the reservation to be removed
        @return true for success; false otherwise
        """

    @abstractmethod
    def remove_delegation(self, *, did: str) -> bool:
        """
        Removes the specified delegation.
        Note only closed delegation can be removed.
        @param did delegation id of the delegation to be removed
        @return true for success; false otherwise
        """

    @abstractmethod
    def get_reservation_state_for_reservations(self, *, reservation_list: List[str]) -> List[ReservationStateAvro]:
        """
        Returns the state of each of the specified reservations.
        The order in the return list matches the order in the @reservations li
        @param reservation_list list of reservations
        @return list of state of the specified reservations
        """

    @abstractmethod
    def get_name(self) -> str:
        """
        Returns the name of the actor.
        @return returns name of the actor
        """

    @abstractmethod
    def get_guid(self) -> ID:
        """
        Returns the guid of the actor.
        @return returns guid of the actor
        """

    @abstractmethod
    def clone(self):
        """
        Creates clone of this proxy to make it possible to be used by a separate thread.
        @return returns the cloned actor
        """

    @abstractmethod
    def get_delegations(self, *, slice_id: ID = None, states: List[int] = None,
                        delegation_id: str = None) -> List[DelegationAvro]:
        """
        Get Delegations
        @param slice_id slice id
        @param states states
        @param delegation_id delegation id
        @return returns list of the delegations
        """

    def is_testbed_in_maintenance(self) -> Tuple[bool, Dict[str, str] or None]:
        """
        Determine if testbed is in maintenance
        @return True if testbed is in maintenance; False otherwise
        """
        return False, None

    def is_site_in_maintenance(self, *, site_name: str) -> Tuple[bool, Site or None]:
        """
        Determine if site is in maintenance
        @return True if site is in maintenance; False otherwise
        """
        return False, None

    def is_sliver_provisioning_allowed(self, *, project: str, email: str, site: str,
                                       worker: str) -> Tuple[bool, str or None]:
        """
        Determine if sliver can be provisioned
        Sliver provisioning can be prohibited if Testbed or Site or Worker is in maintenance mode
        Sliver provisioning in maintenance mode may be allowed for specific projects/users
        @param project project
        @param email user's email
        @param site site name
        @param worker worker name
        @return True if allowed; False otherwise
        """
        return True, None

    def is_slice_provisioning_allowed(self, *, project: str, email: str) -> bool:
        """
        Determine if slice can be provisioned
        Slice provisioning can be prohibited if Testbed is in maintenance mode
        Slice provisioning in maintenance mode may be allowed for specific projects/users
        @param project project
        @param email user's email
        @return True if allowed; False otherwise
        """
        return True

    @abstractmethod
    def get_poas(self, *, states: List[int] = None, slice_id: ID = None, rid: ID = None,
                 email: str = None, poa_id: str = None, project_id: str = None,
                 limit: int = 200, offset: int = 0) -> List[PoaInfoAvro]:
        """
        Get POA
        @param states states
        @param slice_id slice ID
        @param rid reservation id
        @param email: user email
        @param project_id: project_id
        @param poa_id: poa_id
        @param limit: limit of records to be returned
        @param offset: offset
        @return returns list of the poas
        """