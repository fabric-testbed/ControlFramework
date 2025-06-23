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

from abc import abstractmethod, ABC
from datetime import datetime
from typing import TYPE_CHECKING, List, Union, Tuple, Dict

from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.kernel.slice import SliceTypes

if TYPE_CHECKING:
    from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
    from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin
    from fabric_cf.actor.core.apis.abc_slice import ABCSlice
    from fabric_cf.actor.core.util.id import ID
    from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
    from fabric_cf.actor.core.container.maintenance import Site
    from fabric_cf.actor.core.kernel.poa import Poa


class ABCDatabase(ABC):
    """
    IDatabase is the base database layer interface. It specifies methods for managing slices, reservations,
    broker proxies, and configuration mapping files.
    """

    @abstractmethod
    def actor_added(self, *, actor):
        """
        Performs initialization actions as a result of the actor being
        added to the container.

        @throws Exception in case of error
        """

    @abstractmethod
    def add_broker(self, *, broker: ABCBrokerProxy):
        """
        Adds a new broker proxy record.

        @param broker broker proxy

        @throws Exception in case of error
        """

    @abstractmethod
    def add_reservation(self, *, reservation: ABCReservationMixin):
        """
        Adds a new record to the database representing this reservation
        object.

        @param reservation reservation

        @throws Exception in case of error
        """

    @abstractmethod
    def add_slice(self, *, slice_object: ABCSlice):
        """
        Adds a new record to the database representing this slice
        object.

        @param slice_object Slice object

        @throws Exception in case of error
        """

    @abstractmethod
    def remove_broker(self, *, broker: ABCBrokerProxy):
        """
        Removes the specified broker proxy record.

        @param broker broker proxy

        @throws Exception in case of error
        """

    @abstractmethod
    def remove_reservation(self, *, rid: ID):
        """
        Removes the corresponding reservation object.

        @param rid reservation id

        @throws Exception in case of error
        """

    @abstractmethod
    def remove_slice(self, *, slice_id: ID):
        """
        Removes the corresponding database slice record.

        @param slice_id slice name

        @throws Exception in case of error
        """

    @abstractmethod
    def set_actor_name(self, *, name: str):
        """
        Sets the name of the actor this database belongs to.

        @param name actor name
        """

    @abstractmethod
    def update_broker(self, *, broker: ABCBrokerProxy):
        """
        Updates the specified broker proxy record.

        @param broker broker proxy

        @throws Exception in case of error
        """

    @abstractmethod
    def update_reservation(self, *, reservation: ABCReservationMixin):
        """
        Updates the corresponding reservation object.

        @param reservation reservation

        @throws Exception in case of error
        """

    @abstractmethod
    def update_slice(self, *, slice_object: ABCSlice):
        """
        Updates the corresponding database slice record.

        @param slice_object slice object

        @throws Exception in case of error
        """

    @abstractmethod
    def get_reservations(self, *, slice_id: ID = None, graph_node_id: str = None, project_id: str = None,
                         email: str = None, oidc_sub: str = None, rid: ID = None, states: list[int] = None,
                         site: str = None, rsv_type: list[str] = None, start: datetime = None,
                         end: datetime = None, ip_subnet: str = None, host: str = None) -> List[ABCReservationMixin]:
        """
        Retrieves the reservations.

        @return list of reservations

        @throws Exception in case of error
        """

    @abstractmethod
    def get_components(self, *, node_id: str, states: list[int], rsv_type: list[str], component: str = None,
                       bdf: str = None, start: datetime = None, end: datetime = None,
                       excludes: List[str] = None) -> Dict[str, List[str]]:
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

    @abstractmethod
    def get_links(self, *, node_id: str, states: list[int], rsv_type: list[str], start: datetime = None,
                  end: datetime = None, excludes: List[str] = None) -> Dict[str, int]:
        """
        Returns links matching the search criteria
        @param node_id: Link Node ID
        @param states: list of states used to find reservations
        @param rsv_type: type of reservations
        @param start: start time
        @param end: end time
        @param excludes: Excludes the list of reservations

        @return Dictionary with link node id as the key and value cumulative bw.
        """

    @abstractmethod
    def get_client_reservations(self, *, slice_id: ID = None) -> List[ABCReservationMixin]:
        """
        Retrieves the client reservations

        @return list of reservations

        @throws Exception in case of error
        """

    @abstractmethod
    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, project_id: str = None, email: str = None,
                   states: list[int] = None, oidc_sub: str = None, slc_type: List[SliceTypes] = None,
                   limit: int = None, offset: int = None, lease_end: datetime = None,
                   search: str = None, exact_match: bool = False,
                   updated_after: datetime = None) -> List[ABCSlice] or None:
        """
        Retrieves the specified slices.

        @param slice_id slice id
        @param slice_name slice name
        @param project_id project id
        @param email email
        @param states states
        @param oidc_sub oidc sub
        @param slc_type slice type
        @param limit limit
        @param offset offset
        @param lease_end lease_end
        @param search: search term applied
        @param exact_match: Exact Match for Search term
        @param updated_after: Filter slices updated after this timestamp

        @return list of slices

        @throws Exception in case of error
        """

    @abstractmethod
    def get_slice_count(self, *, project_id: str = None, email: str = None, states: list[int] = None,
                        oidc_sub: str = None, slc_type: List[SliceTypes] = None,
                        excluded_projects: List[str] = None) -> int:
        """
        Retrieves the slices count.

        @param project_id project id
        @param email email
        @param states states
        @param oidc_sub oidc sub
        @param slc_type slice type
        @param excluded_projects excluded_projects

        @return number of slices matching the filter criteria

        @throws Exception in case of error
        """

    @abstractmethod
    def increment_metrics(self, *, project_id: str, oidc_sub: str, slice_count: int = 1) -> bool:
        """
        Add or Update Metrics

        @param project_id project id
        @param oidc_sub oidc sub
        @param slice_count slice_count

        @return true or false

        @throws Exception in case of error
        """

    @abstractmethod
    def get_metrics(self, *, project_id: str, oidc_sub: str, excluded_projects: List[str] = None) -> list:
        """
        Get Metrics
        @param project_id: project id
        @param oidc_sub: user id
        @param excluded_projects: list of project ids to exclude

        @return list of metrics
        """

    @abstractmethod
    def initialize(self):
        """
        Initializes the object.

        @throws Exception if initialization fails
        """

    @abstractmethod
    def get_holdings(self, *, slice_id: ID = None) -> List[ABCReservationMixin]:
        """
        Retrieves all reservations representing resources held by this
        actor Broker/Controller.

        @return list of properties

        @throws Exception in case of error
        """

    @abstractmethod
    def get_broker_reservations(self) -> List[ABCReservationMixin]:
        """
        Retrieves all reservations for which this actor acts as a
        broker.

        @return list of properties

        @throws Exception in case of error
        """

    @abstractmethod
    def get_authority_reservations(self) -> List[ABCReservationMixin]:
        """
        Retrieves all reservations for which this actor acts as a site.

        @return list of properties

        @throws Exception in case of error
        """

    @abstractmethod
    def get_reservations_by_rids(self, *, rid: List[str]) -> List[ABCReservationMixin]:
        """
        Retrieves the specified reservation records.
        The order in the return vector is the same order as @rids
        @param rid rids
        @return list of reservations
        @throws Exception in case of error
        """

    @abstractmethod
    def add_delegation(self, *, delegation: ABCDelegation):
        """
        Add delegation
        @params delegation: delegation
        """

    @abstractmethod
    def update_delegation(self, *, delegation: ABCDelegation):
        """
        Update delegation
        @params delegation: delegation
        """

    @abstractmethod
    def remove_delegation(self, *, dlg_graph_id: str):
        """
        Remove delegation
        @params dlg_graph_id: dlg_graph_id
        """

    @abstractmethod
    def get_delegation(self, *, dlg_graph_id: str) -> ABCDelegation or None:
        """
        Get Delegation
        @params dlg_graph_id: dlg_graph_id
        @return Delegation
        """

    @abstractmethod
    def get_delegations(self, *, slice_id: ID = None, states: List[int] = None) -> List[ABCDelegation]:
        """
        Get delegations
        @params slice_id: slice_id
        @params state: state
        @return Delegations
        """

    @abstractmethod
    def add_config_mapping(self, key: str, config_mapping: ConfigurationMapping):
        """
        Add configuration Mapping
        @param key key
        @param config_mapping config mapping
        """

    @abstractmethod
    def get_config_mappings(self) -> List[ConfigurationMapping]:
        """
        Return all config mappings
        """

    @abstractmethod
    def get_brokers(self) -> List[ABCBrokerProxy] or None:
        """
        Return all brokers
        """

    @abstractmethod
    def add_site(self, *, site: Site):
        """
        Add site
        @param site site
        """

    def update_site(self, *, site: Site):
        """
        Update Site
        @param site site
        """
    def remove_site(self, *, site_name: str):
        """
        Remove Site
        @param site_name site name
        """
    def get_site(self, *, site_name: str) -> Site or None:
        """
        Get site
        @param site_name site name
        @return Site
        """

    def get_sites(self) -> List[Site]:
        """
        Return all sites
        @return list of sites
        """

    def add_maintenance_properties(self, *, properties: dict):
        """
        Add maintenance properties
        @param properties properties
        """

    def update_maintenance_properties(self, *, properties: dict):
        """
        Update maintenance properties
        @param properties properties
        """

    def remove_maintenance_properties(self):
        """
        Remove maintenance properties
        """

    def get_maintenance_properties(self) -> dict:
        """
        Get maintenance Properties
        @return properties
        """

    @abstractmethod
    def add_poa(self, *, poa: Poa):
        """
        Adds a new record to the database representing this slice
        object.

        @param poa POA object

        @throws Exception in case of error
        """

    @abstractmethod
    def update_poa(self, *, poa: Poa):
        """
        Updates the corresponding database poa record.

        @param poa poa info object

        @throws Exception in case of error
        """

    @abstractmethod
    def remove_poa(self, *, poa_id: str):
        """
        Removes the corresponding database slice record.

        @param poa_id poa id

        @throws Exception in case of error
        """

    @abstractmethod
    def get_poas(self, *, poa_id: str = None, email: str = None, sliver_id: ID = None, slice_id: ID = None,
                 project_id: str = None, limit: int = None, offset: int = None, last_update_time: datetime = None,
                 states: list[int] = None) -> Union[List[Poa] or None]:
        """
        Get POAs
        @param poa_id poa id
        @param email email
        @param sliver_id sliver id
        @param slice_id slice id
        @param project_id project id
        @param limit limit
        @param offset offset
        @param states states
        @param last_update_time last update time
        """
