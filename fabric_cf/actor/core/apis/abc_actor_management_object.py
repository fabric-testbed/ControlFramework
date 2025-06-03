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
from typing import TYPE_CHECKING, Tuple, Dict, List

from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric_mb.message_bus.messages.result_poa_avro import ResultPoaAvro
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_sites_avro import ResultSitesAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fim.user import GraphFormat

from fabric_cf.actor.core.apis.abc_management_object import ABCManagementObject
from fabric_cf.actor.core.container.maintenance import Site
from fabric_cf.actor.core.util.id import ID

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin
    from fabric_cf.actor.security.auth_token import AuthToken


class ABCActorManagementObject(ABCManagementObject):
    """
    Interface for Management Object for an Actor
    """
    @abstractmethod
    def set_actor(self, *, actor: ABCActorMixin):
        """
        Set an actor
        @params actor: actor
        """

    @abstractmethod
    def update_reservation(self, *, reservation, caller: AuthToken):
        """
        Update Reservation
        @params reservation: reservation
        @params caller: caller
        """

    def get_poas(self, *, caller: AuthToken, states: List[int] = None,
                 slice_id: ID = None, rid: ID = None, email: str = None,
                 poa_id: str = None, project_id: str = None,
                 limit: int = 200, offset: int = 0) -> ResultPoaAvro:
        """
        Get Reservations
        @param states states
        @param slice_id slice ID
        @param rid reservation id
        @param email: user email
        @param poa_id: POA Id
        @param project_id project id
        Obtains all poa with error information in case of failure
        @param caller caller
        @param limit: limit of records to be returned
        @param offset: offset
        @return returns list of the poa
        """

    def is_sliver_provisioning_allowed(self, *, project: str, email: str, site: str,
                                       worker: str) -> Tuple[bool, str or None]:
        """
        Check if sliver provisioning is allowed for a given project on a site/worker
        @param project project id
        @param email user's email
        @param site site
        @param worker worker
        @return True if provisioning is allowed; False otherwise
        """

    def is_slice_provisioning_allowed(self, *, project: str, email: str) -> bool:
        """
        Determine if slice can be provisioned
        Slice provisioning can be prohibited if Testbed is in maintenance mode
        Slice provisioning in maintenance mode may be allowed for specific projects/users
        @param project project
        @param email user's email
        @return True if allowed; False otherwise
        """

    def is_site_in_maintenance(self, *, site_name: str) -> Tuple[bool, Site or None]:
        """
        Check if site is in Maintenance
        @param site_name site name
        @return True, Site Information if site is in Maintenance, False otherwise
        """

    def is_testbed_in_maintenance(self) -> Tuple[bool, Dict[str, str] or None]:
        """
        Check if testbed is in maintenance
        @return True - if testbed is in maintenance, False otherwise
        """

    def remove_delegation(self, *, caller: AuthToken, did: str) -> ResultAvro:
        """
        Removes the specified delegation.
        Note only closed delegation can be removed.
        @param did delegation id of the delegation to be removed
        @param caller caller
        @return result status with error information in case of failure
        """

    @abstractmethod
    def close_delegation(self, *, caller: AuthToken, did: str) -> ResultAvro:
        """
        Closes the specified delegation
        @param did delegation id
        @param caller caller
        @return result status with error information in case of failure
        """

    def get_delegations(self, *, caller: AuthToken, slice_id: ID = None, did: str = None,
                        states: List[int] = None) -> ResultDelegationAvro:
        """
        Get Delegations
        @param slice_id slice id
        @param states states
        @param did delegation id
        @param caller caller
        @return result delegation with error information in case of failure
        """

    def get_reservation_state_for_reservations(self, *, caller: AuthToken,
                                               rids: List[str]) -> ResultReservationStateAvro:
        """
        Returns the state of each of the specified reservations.
        The order in the return list matches the order in the @reservations li
        @param rids list of reservation ids
        @param caller caller
        @return list of state of the specified reservations along with error status in case of failure
        """

    def close_slice_reservations(self, *, caller: AuthToken, slice_id: ID) -> ResultAvro:
        """
        Closes all reservations in the specified slice.
        @param slice_id slice ID
        @param caller caller
        @return result status with error information in case of failure
        """

    def accept_update_slice(self, *, slice_id: ID, caller: AuthToken) -> ResultAvro:
        """
        Accept the last slice update
        @param slice_id slice_id
        @param caller caller
        @return result status with error information in case of failure
        """

    def update_slice(self, *, slice_mng: SliceAvro, caller: AuthToken, modify_state: bool = False) -> ResultAvro:
        """
        Updates the specified slice.
        The only updatable slice attributes are:
        - description
        - all properties lists
        @param slice_mng slice
        @param modify_state - trigger state change
        @param caller caller
        @return result status with error information in case of failure
        """

    def delete_slice(self, *, slice_id: ID, caller: AuthToken) -> ResultAvro:
        """
        Deletes the specified slice - moves the slice into Closing state.
        @param slice_id slice ID
        @param caller caller
        @return result status with error information in case of failure
        """

    def add_slice(self, *, slice_obj: SliceAvro, caller: AuthToken) -> ResultStringAvro:
        """
        Adds a new slice
        @param slice_obj slice
        @param caller caller
        @return returns slice id with error info
        """

    def get_actor(self) -> ABCActorMixin:
        """
        Return Actor
        @return actor
        """

    def close_reservation(self, *, caller: AuthToken, rid: ID) -> ResultAvro:
        """
        Closes the specified reservation
        @param rid reservation id
        @param caller caller
        @return result status with error information in case of failure
        """

    def remove_reservation(self, *, caller: AuthToken, rid: ID) -> ResultAvro:
        """
        Removes the specified reservation.
        Note only closed reservations can be removed.
        @param rid reservation id of the reservation to be removed
        @param caller caller
        @return result status with error information in case of failure
        """

    def get_sites(self, *, caller: AuthToken, site: str) -> ResultSitesAvro:
        """
        @param site site name
        Obtains Maintenance Info for a site
        @param caller caller
        @return returns list of the Sites with error information in case of failure
        """

    def get_reservations(self, *, caller: AuthToken, states: List[int] = None,
                         slice_id: ID = None, rid: ID = None, oidc_claim_sub: str = None,
                         email: str = None, rid_list: List[str] = None, type: str = None,
                         site: str = None, node_id: str = None, host: str = None, ip_subnet: str = None,
                         full: bool = False, start: datetime = None, end: datetime = None) -> ResultReservationAvro:
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
        Obtains all reservations with error information in case of failure
        @param caller caller
        @param host host
        @param ip_subnet ip subnet
        @param full
        @param start: start time
        @param end: end time

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

    def get_links(self, *, node_id: str, rsv_type: list[str], states: list[int], start: datetime = None,
                  end: datetime = None, excludes: List[str] = None) -> Dict[str, int]:
        """
        Returns links matching the search criteria
        @param node_id: Link Node ID
        @param states: list of states used to find reservations
        @param rsv_type: type of reservations
        @param start: start time
        @param end: end time
        @param excludes: Excludes the list of reservations

        @return Dictionary with link node id as key and cumulative bandwidth.
        """

    def get_slices(self, *, slice_id: ID, caller: AuthToken, slice_name: str = None, email: str = None,
                   states: List[int] = None, project: str = None, limit: int = None,
                   offset: int = None, user_id: str = None, search: str = None,
                   exact_match: bool = False) -> ResultSliceAvro:
        """
        Obtains all slices.
        @param slice_id slice id
        @param slice_name slice name
        @param email email
        @param project project id
        @param states slice states
        @param limit limit
        @param offset offset
        @param caller caller
        @param user_id user_id
        @param search: search term applied
        @param exact_match: Exact Match for Search term
        @return returns list of slices
        """

    @abstractmethod
    def increment_metrics(self, *, project_id: str, oidc_sub: str, slice_count: int = 1) -> bool:
        """
        Add or update metrics

        @param project_id project id
        @param oidc_sub oidc sub
        @param slice_count slice_count

        @return true or false

        @throws Exception in case of error
        """

    @abstractmethod
    def get_metrics(self, *, project_id: str, oidc_sub: str, excluded_projects: List[str] = None) -> list:
        """
        Get metrics

        @param project_id project id
        @param oidc_sub oidc sub
        @param excluded_projects excluded_projects

        @return list of metric information

        @throws Exception in case of error
        """

    def get_slice_count(self, *, caller: AuthToken, email: str = None, states: List[int] = None,
                        project: str = None, user_id: str = None, excluded_projects: List[str] = None) -> int:
        """
        Obtains Slice count matching the filter criteria.

        @param email email
        @param project project id
        @param states slice states
        @param caller caller
        @param user_id user_id
        @param excluded_projects excluded_projects
        @return returns number of slices
        """

    def remove_slice(self, *, slice_id: ID, caller: AuthToken) -> ResultAvro:
        """
        Removes the specified slice
        @param slice_id slice id
        @param caller caller
        @return true for success; false otherwise
        """

    def build_broker_query_model(self, level_0_broker_query_model: str, level: int,
                                 graph_format: GraphFormat = GraphFormat.GRAPHML,
                                 start: datetime = None, end: datetime = None, includes: str = None,
                                 excludes: str = None) -> str:
        """
        Build the BQM Model using current usage
        @param level_0_broker_query_model Capacity Model
        @param level: level of details
        @param graph_format: Graph Format
        @param start: start time
        @param end: end time
        @param includes: comma separated lists of sites to include
        @param excludes: comma separated lists of sites to exclude
        @return BQM
        """