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
import pickle
import threading
import time
import traceback
from datetime import datetime
from typing import List, Union, Dict

from fim.slivers.network_link import NetworkLinkSliver

from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin, ReservationCategory
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.core.kernel.poa import Poa, PoaStates
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.plugins.handlers.configuration_mapping import ConfigurationMapping
from fabric_cf.actor.core.container.maintenance import Site
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.db.psql_database import PsqlDatabase


class ActorDatabase(ABCDatabase):
    MAINTENANCE = 'maintenance'

    def __init__(self, *, user: str, password: str, database: str, db_host: str, logger):
        self.user = user
        self.password = password
        self.database = database
        self.db_host = db_host
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database, db_host=self.db_host,
                               logger=logger)
        self.actor_type = None
        self.actor_name = None
        self.actor_id = None
        self.initialized = False
        self.logger = logger
        self.reset_state = False
        self.actor = None
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['db']
        del state['actor_type']
        del state['actor_name']
        del state['actor_id']
        del state['initialized']
        del state['logger']
        del state['reset_state']
        del state['actor']
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database, db_host=self.db_host,
                               logger=None)
        self.actor_id = None
        self.actor_name = None
        self.actor_type = None
        self.initialized = False
        self.logger = None
        self.reset_state = False
        self.lock = threading.Lock()

    def set_logger(self, *, logger):
        self.logger = logger
        if self.db is not None:
            self.db.set_logger(logger=logger)

    def set_reset_state(self, *, state: bool):
        self.reset_state = state

    def initialize(self):
        if not self.initialized:
            if self.actor_name is None:
                raise DatabaseException(Constants.NOT_SPECIFIED_PREFIX.format("actor name"))
            self.initialized = True

    def actor_added(self, *, actor):
        self.actor = actor
        self.actor_id = self.get_actor_id_from_name(actor_name=self.actor_name)
        if self.actor_id is None:
            raise DatabaseException(Constants.OBJECT_NOT_FOUND.format("actor", self.actor_name))

    def revisit(self, *, actor: ABCActorMixin, properties: dict):
        return

    def get_actor_id_from_name(self, *, actor_name: str) -> int or None:
        try:
            #self.lock.acquire()
            actor = self.db.get_actor(name=actor_name)
            self.actor_id = actor['act_id']
            self.actor_type = ActorType(actor['act_type'])
            return self.actor_id
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def get_slice_by_id(self, *, slc_id: int) -> ABCSlice or None:
        try:
            #self.lock.acquire()
            slice_dict = self.db.get_slice_by_id(slc_id=slc_id)
            if slice_dict is not None:
                pickled_slice = slice_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                return pickle.loads(pickled_slice)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def add_slice(self, *, slice_object: ABCSlice):
        try:
            slices = self.get_slices(slice_id=slice_object.get_slice_id())
            if len(slices) > 0:
                raise DatabaseException("Slice # {} already exists".format(slice_object.get_slice_id()))

            properties = pickle.dumps(slice_object)
            oidc_claim_sub = None
            email = None
            if slice_object.get_owner() is not None:
                oidc_claim_sub = slice_object.get_owner().get_oidc_sub_claim()
                email = slice_object.get_owner().get_email()

            #self.lock.acquire()
            self.db.add_slice(slc_guid=str(slice_object.get_slice_id()),
                              slc_state=slice_object.get_state().value,
                              slc_name=slice_object.get_name(),
                              slc_type=slice_object.get_slice_type().value,
                              slc_resource_type=str(slice_object.get_resource_type()),
                              properties=properties,
                              slc_graph_id=slice_object.get_graph_id(),
                              oidc_claim_sub=oidc_claim_sub, email=email,
                              lease_end=slice_object.get_lease_end(),
                              lease_start=slice_object.get_lease_start(),
                              project_id=slice_object.get_project_id())
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_slice(self, *, slice_object: ABCSlice):
        # Update the slice only when there are changes to be reflected in database
        if not slice_object.is_dirty():
            return
        slice_object.clear_dirty()
        try:
            #self.lock.acquire()
            properties = pickle.dumps(slice_object)
            self.db.update_slice(slc_guid=str(slice_object.get_slice_id()),
                                 slc_name=slice_object.get_name(),
                                 slc_type=slice_object.get_slice_type().value,
                                 slc_state=slice_object.get_state().value,
                                 slc_resource_type=str(slice_object.get_resource_type()),
                                 properties=properties,
                                 slc_graph_id=slice_object.get_graph_id(),
                                 lease_end=slice_object.get_lease_end(), lease_start=slice_object.get_lease_start())
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_slice(self, *, slice_id: ID):
        try:
            #self.lock.acquire()
            self.db.remove_slice(slc_guid=str(slice_id))
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, project_id: str = None, email: str = None,
                   states: list[int] = None, oidc_sub: str = None, slc_type: List[SliceTypes] = None,
                   limit: int = None, offset: int = None, lease_end: datetime = None,
                   search: str = None, exact_match: bool = False,
                   updated_after: datetime = None) -> List[ABCSlice] or None:
        result = []
        try:
            try:
                #self.lock.acquire()
                slice_type = None
                if slc_type is not None:
                    slice_type = [x.value for x in slc_type]
                sid = str(slice_id) if slice_id is not None else None
                slices = self.db.get_slices(slice_id=sid, slice_name=slice_name, project_id=project_id, email=email,
                                            states=states, oidc_sub=oidc_sub, slc_type=slice_type, limit=limit,
                                            offset=offset, lease_end=lease_end, search=search, exact_match=exact_match,
                                            updated_after=updated_after)
            finally:
                if self.lock.locked():
                    self.lock.release()
            if slices is not None:
                for s in slices:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    slice_obj.set_last_updated_time(s.get('last_updated_time'))
                    result.append(slice_obj)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def increment_metrics(self, *, project_id: str, oidc_sub: str, slice_count: int = 1) -> bool:
        try:
            self.lock.acquire()
            self.db.increment_metrics(project_id=project_id, user_id=oidc_sub, slice_count=slice_count)
            return True
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return False

    def get_metrics(self, *, project_id: str, oidc_sub: str, excluded_projects: List[str] = None) -> list:
        try:
            return self.db.get_metrics(project_id=project_id, user_id=oidc_sub, excluded_projects=excluded_projects)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_slice_count(self, *, project_id: str = None, email: str = None, states: list[int] = None,
                        oidc_sub: str = None, slc_type: List[SliceTypes] = None,
                        excluded_projects: List[str] = None) -> int:
        try:
            slice_type = [SliceTypes.ClientSlice.value]
            if slc_type is not None:
                slice_type = [x.value for x in slc_type]
            return self.db.get_slice_count(project_id=project_id, email=email, states=states, oidc_sub=oidc_sub,
                                           slc_type=slice_type, excluded_projects=excluded_projects)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return -1

    def add_reservation(self, *, reservation: ABCReservationMixin):
        try:
            #self.lock.acquire()
            self.logger.debug("Adding reservation {} to slice {}".format(reservation.get_reservation_id(),
                                                                         reservation.get_slice()))
            properties = pickle.dumps(reservation)
            oidc_claim_sub = None
            email = None
            if reservation.get_slice() is not None and reservation.get_slice().get_owner() is not None:
                oidc_claim_sub = reservation.get_slice().get_owner().get_oidc_sub_claim()
                email = reservation.get_slice().get_owner().get_email()

            site = None
            rsv_type = None
            components = None
            host = None
            ip_subnet = None
            sliver = None
            links = []
            from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
            if isinstance(reservation, ReservationClient) and reservation.get_leased_resources() and \
                    reservation.get_leased_resources().get_sliver():
                sliver = reservation.get_leased_resources().get_sliver()
            if not sliver and reservation.get_resources() and reservation.get_resources().get_sliver():
                sliver = reservation.get_resources().get_sliver()

            if sliver:
                rsv_type = sliver.get_type().name
                from fim.slivers.network_service import NetworkServiceSliver
                from fim.slivers.network_node import NodeSliver

                if isinstance(sliver, NetworkServiceSliver) and sliver.interface_info:
                    site = sliver.get_site()
                    if sliver.get_gateway():
                        ip_subnet = sliver.get_gateway().subnet

                    components = []
                    for interface in sliver.interface_info.interfaces.values():
                        graph_id_node_id_component_id, bqm_if_name = interface.get_node_map()
                        if ":" in graph_id_node_id_component_id or "#" in graph_id_node_id_component_id:
                            if "#" in graph_id_node_id_component_id:
                                split_string = graph_id_node_id_component_id.split("#")
                            else:
                                split_string = graph_id_node_id_component_id.split(":")
                            node_id = split_string[1] if len(split_string) > 1 else None
                            comp_id = split_string[2] if len(split_string) > 2 else None
                            bdf = ":".join(split_string[3:]) if len(split_string) > 3 else None
                            if node_id and comp_id and bdf:
                                components.append((node_id, comp_id, bdf))
                    if sliver.ero and sliver.capacities:
                        type, path = sliver.ero.get()
                        if path and len(path.get()):
                            for hop in path.get()[0]:
                                if hop.startswith('link:'):
                                    links.append({"node_id": hop,
                                                  "bw": sliver.capacities.bw})

                elif isinstance(sliver, NodeSliver):
                    site = sliver.get_site()
                    if sliver.get_labels() and sliver.get_labels().instance_parent:
                        host = sliver.get_labels().instance_parent
                    if sliver.get_label_allocations() and sliver.get_label_allocations().instance_parent:
                        host = sliver.get_label_allocations().instance_parent
                    if sliver.get_management_ip():
                        ip_subnet = str(sliver.get_management_ip())

                    node_id = reservation.get_graph_node_id()
                    if node_id and sliver.attached_components_info:
                        components = []
                        for c in sliver.attached_components_info.devices.values():
                            if c.get_node_map():
                                bqm_id, comp_id = c.get_node_map()
                                if c.labels and c.labels.bdf:
                                    bdf = c.labels.bdf
                                    if isinstance(c.labels.bdf, str):
                                        bdf = [c.labels.bdf]
                                    for x in bdf:
                                        components.append((node_id, comp_id, x))

            term = reservation.get_term()

            self.db.add_reservation(slc_guid=str(reservation.get_slice_id()),
                                    rsv_resid=str(reservation.get_reservation_id()),
                                    rsv_category=reservation.get_category().value,
                                    rsv_state=reservation.get_state().value,
                                    rsv_pending=reservation.get_pending_state().value,
                                    rsv_joining=reservation.get_join_state().value,
                                    properties=properties,
                                    rsv_graph_node_id=reservation.get_graph_node_id(),
                                    oidc_claim_sub=oidc_claim_sub, email=email, site=site, rsv_type=rsv_type,
                                    components=components,
                                    lease_start=term.get_start_time() if term else None,
                                    lease_end=term.get_end_time() if term else None,
                                    host=host, ip_subnet=ip_subnet, links=links)
            self.logger.debug(
                "Reservation {} added to slice {}".format(reservation.get_reservation_id(), reservation.get_slice()))
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_reservation(self, *, reservation: ABCReservationMixin):
        # Update the reservation only when there are changes to be reflected in database
        if not reservation.is_dirty():
            return
        reservation.clear_dirty()
        try:
            #self.lock.acquire()
            self.logger.debug("Updating reservation {} in slice {}".format(reservation.get_reservation_id(),
                                                                           reservation.get_slice()))

            site = None
            rsv_type = None
            components = None
            ip_subnet = None
            host = None
            sliver = None
            links = []
            from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
            if isinstance(reservation, ReservationClient) and reservation.get_leased_resources() and \
                    reservation.get_leased_resources().get_sliver():
                sliver = reservation.get_leased_resources().get_sliver()
            if not sliver and reservation.get_resources() and reservation.get_resources().get_sliver():
                sliver = reservation.get_resources().get_sliver()

            if sliver:
                rsv_type = sliver.get_type().name
                from fim.slivers.network_service import NetworkServiceSliver
                from fim.slivers.network_node import NodeSliver
                if isinstance(sliver, NetworkServiceSliver) and sliver.interface_info:
                    site = sliver.get_site()

                    if sliver.get_gateway():
                        ip_subnet = sliver.get_gateway().subnet

                    components = []
                    for interface in sliver.interface_info.interfaces.values():
                        graph_id_node_id_component_id, bqm_if_name = interface.get_node_map()
                        if ":" in graph_id_node_id_component_id or "#" in graph_id_node_id_component_id:
                            if "#" in graph_id_node_id_component_id:
                                split_string = graph_id_node_id_component_id.split("#")
                            else:
                                split_string = graph_id_node_id_component_id.split(":")
                            node_id = split_string[1] if len(split_string) > 1 else None
                            comp_id = split_string[2] if len(split_string) > 2 else None
                            bdf = ":".join(split_string[3:]) if len(split_string) > 3 else None
                            if node_id and comp_id and bdf:
                                components.append((node_id, comp_id, bdf))

                    if sliver.ero and sliver.capacities:
                        type, path = sliver.ero.get()
                        if path and len(path.get()):
                            for hop in path.get()[0]:
                                if hop.startswith('link:'):
                                    links.append({"node_id": hop,
                                                  "bw": sliver.capacities.bw})
                elif isinstance(sliver, NodeSliver):
                    site = sliver.get_site()

                    if sliver.get_labels() and sliver.get_labels().instance_parent:
                        host = sliver.get_labels().instance_parent
                    if sliver.get_label_allocations() and sliver.get_label_allocations().instance_parent:
                        host = sliver.get_label_allocations().instance_parent
                    if sliver.get_management_ip():
                        ip_subnet = str(sliver.get_management_ip())
                    node_id = reservation.get_graph_node_id()
                    if node_id and sliver.attached_components_info:
                        components = []
                        for c in sliver.attached_components_info.devices.values():
                            if c.get_node_map():
                                bqm_id, comp_id = c.get_node_map()
                                if c.labels and c.labels.bdf:
                                    bdf = c.labels.bdf
                                    if isinstance(c.labels.bdf, str):
                                        bdf = [c.labels.bdf]
                                    for x in bdf:
                                        components.append((node_id, comp_id, x))

            term = reservation.get_term()
            begin = time.time()
            properties = pickle.dumps(reservation)
            diff = int(time.time() - begin)
            if diff > 0:
                self.logger.info(f"PICKLE TIME: {diff}")
            begin = time.time()
            self.db.update_reservation(slc_guid=str(reservation.get_slice_id()),
                                       rsv_resid=str(reservation.get_reservation_id()),
                                       rsv_category=reservation.get_category().value,
                                       rsv_state=reservation.get_state().value,
                                       rsv_pending=reservation.get_pending_state().value,
                                       rsv_joining=reservation.get_join_state().value,
                                       properties=properties,
                                       rsv_graph_node_id=reservation.get_graph_node_id(),
                                       site=site, rsv_type=rsv_type, components=components,
                                       lease_start=term.get_start_time() if term else None,
                                       lease_end=term.get_end_time() if term else None,
                                       ip_subnet=ip_subnet, host=host, links=links)
            diff = int(time.time() - begin)
            if diff > 0:
                self.logger.info(f"DB TIME: {diff}")
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_reservation(self, *, rid: ID):
        try:
            #self.lock.acquire()
            self.logger.debug("Removing reservation {}".format(rid))
            try:
                self.db.remove_unit(unt_uid=str(rid))
            except Exception as e:
                self.logger.debug("No associated Unit associated with the reservation")
            self.db.remove_reservation(rsv_resid=str(rid))
        finally:
            if self.lock.locked():
                self.lock.release()

    def _load_reservation_from_pickled_object(self, pickled_res: bytes, slc_id: int) -> ABCReservationMixin or None:
        try:
            slice_obj = self.get_slice_by_id(slc_id=slc_id)
            result = pickle.loads(pickled_res)
            result.restore(actor=self.actor, slice_obj=slice_obj)

            if isinstance(result, ABCControllerReservation):
                if result.get_redeem_predecessors() is not None:
                    for p in result.get_redeem_predecessors():
                        if p.reservation_id is not None:
                            parent = self.get_reservations(rid=p.reservation_id)
                            if parent is not None and len(parent) > 0:
                                p.set_reservation(reservation=parent[0])

                    for p in result.get_join_predecessors():
                        if p.reservation_id is not None:
                            parent = self.get_reservations(rid=p.reservation_id)
                            if parent is not None and len(parent) > 0:
                                p.set_reservation(reservation=parent[0])

            # Load in progress POAs
            poa_list = self.get_poas(sliver_id=result.get_reservation_id(), include_res_info=False,
                                     states=[PoaStates.Nascent.value, PoaStates.Performing.value,
                                             PoaStates.AwaitingCompletion.value, PoaStates.SentToAuthority.value])

            from fabric_cf.actor.core.kernel.reservation_client import ReservationClient
            from fabric_cf.actor.core.kernel.authority_reservation import AuthorityReservation
            if isinstance(result, ReservationClient) or isinstance(result, AuthorityReservation):
                for poa in poa_list:
                    poa.restore(actor=self.actor, reservation=result)
                    result.poas[poa.get_poa_id()] = poa

            return result
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        return None

    def _load_reservations_from_db(self, *, res_dict_list: List[dict]) -> List[ABCReservationMixin]:
        result = []
        if res_dict_list is None:
            return result

        for r in res_dict_list:
            pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            slc_id = r.get(Constants.RSV_SLC_ID)
            res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slc_id=slc_id)
            result.append(res_obj)
        return result

    def get_client_reservations(self, *, slice_id: ID = None) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            res_dict_list = self.db.get_reservations(slice_id=sid,
                                                     category=[ReservationCategory.Broker.value,
                                                               ReservationCategory.Authority.value])
            if self.lock.locked():
                self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def get_holdings(self, *, slice_id: ID = None) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            res_dict_list = self.db.get_reservations(slice_id=sid, category=[ReservationCategory.Client.value])
            if self.lock.locked():
                self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def get_broker_reservations(self) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            res_dict_list = self.db.get_reservations(category=[ReservationCategory.Broker.value])
            if self.lock.locked():
                self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def get_authority_reservations(self) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            result = []
            res_dict_list = self.db.get_reservations(category=[ReservationCategory.Authority.value])
            if self.lock.locked():
                self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def get_components(self, *, node_id: str, states: list[int], rsv_type: list[str], component: str = None,
                       bdf: str = None, start: datetime = None, end: datetime = None,
                       excludes: List[str] = None) -> Dict[str, List[str]]:
        try:
            return self.db.get_components(node_id=node_id, states=states, component=component, bdf=bdf,
                                          rsv_type=rsv_type, start=start, end=end, excludes=excludes)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_links(self, *, node_id: str, states: list[int], rsv_type: list[str], start: datetime = None,
                  end: datetime = None, excludes: List[str] = None) -> Dict[str, int]:
        try:
            return self.db.get_links(node_id=node_id, states=states, rsv_type=rsv_type, start=start,
                                     end=end, excludes=excludes)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_reservations(self, *, slice_id: ID = None, graph_node_id: str = None, project_id: str = None,
                         email: str = None, oidc_sub: str = None, rid: ID = None, states: list[int] = None,
                         site: str = None, rsv_type: list[str] = None, start: datetime = None,
                         end: datetime = None, ip_subnet: str = None, host: str = None) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            res_id = str(rid) if rid is not None else None
            res_dict_list = self.db.get_reservations(slice_id=sid, graph_node_id=graph_node_id, host=host, ip_subnet=ip_subnet,
                                                     project_id=project_id, email=email, oidc_sub=oidc_sub, rid=res_id,
                                                     states=states, site=site, rsv_type=rsv_type, start=start, end=end)
            if self.lock.locked():
               self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def get_reservations_by_rids(self, *, rid: List[str]) -> List[ABCReservationMixin]:
        result = []
        try:
            #self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_rids(rsv_resid_list=rid)
            if self.lock.locked():
                self.lock.release()
            result = self._load_reservations_from_db(res_dict_list=res_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def add_broker(self, *, broker: ABCBrokerProxy):
        try:
            #self.lock.acquire()
            self.logger.debug("Adding broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.add_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_broker(self, *, broker: ABCBrokerProxy):
        try:
            #self.lock.acquire()
            self.logger.debug("Updating broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.update_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_broker(self, *, broker: ABCBrokerProxy):
        try:
            #self.lock.acquire()
            self.logger.debug("Removing broker {}({})".format(broker.get_name(), broker.get_guid()))
            self.db.remove_proxy(act_id=self.actor_id, prx_name=broker.get_name())
        finally:
            if self.lock.locked():
                self.lock.release()

    def set_actor_name(self, *, name: str):
        self.actor_name = name

    def get_brokers(self) -> List[ABCBrokerProxy] or None:
        try:
            #self.lock.acquire()
            result = []
            broker_dict_list = self.db.get_proxies(act_id=self.actor_id)
            if broker_dict_list is not None:
                for b in broker_dict_list:
                    pickled_broker = b.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    broker_obj = pickle.loads(pickled_broker)
                    result.append(broker_obj)
            return result
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def add_delegation(self, *, delegation: ABCDelegation):
        self.logger.debug("Adding delegation {} to slice {}".format(delegation.get_delegation_id(),
                                                                    delegation.get_slice_id()))
        try:
            #self.lock.acquire()
            properties = pickle.dumps(delegation)
            self.db.add_delegation(slice_id=str(delegation.get_slice_id()),
                                   dlg_graph_id=str(delegation.get_delegation_id()),
                                   dlg_state=delegation.get_state().value,
                                   properties=properties,
                                   site=delegation.get_site())
            self.logger.debug(
                "Delegation {} added to slice {}".format(delegation.get_delegation_id(),
                                                         delegation.get_slice_id()))
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_delegation(self, *, delegation: ABCDelegation):
        # Update the delegation only when there are changes to be reflected in database
        if not delegation.is_dirty():
            return
        delegation.clear_dirty()
        try:
            #self.lock.acquire()
            self.logger.debug("Updating delegation {} in slice {}".format(delegation.get_delegation_id(),
                                                                          delegation.get_slice_id()))
            properties = pickle.dumps(delegation)
            self.db.update_delegation(dlg_graph_id=str(delegation.get_delegation_id()),
                                      dlg_state=delegation.get_state().value,
                                      properties=properties,
                                      site=delegation.get_site())
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_delegation(self, *, dlg_graph_id: str):
        try:
            #self.lock.acquire()
            self.logger.debug("Removing delegation {}".format(dlg_graph_id))
            self.db.remove_delegation(dlg_graph_id=str(dlg_graph_id))
        finally:
            if self.lock.locked():
                self.lock.release()

    def _load_delegation_from_pickled_instance(self, pickled_del: bytes, slc_id: int) -> ABCDelegation:
        slice_obj = self.get_slice_by_id(slc_id=slc_id)
        delegation = pickle.loads(pickled_del)
        delegation.restore(actor=self.actor, slice_obj=slice_obj)
        return delegation

    def _load_delegation_from_db(self, dlg_dict_list: List[dict]) -> List[ABCDelegation]:
        result = []
        if dlg_dict_list is None:
            return result

        for d in dlg_dict_list:
            pickled_del = d.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            slc_id = d.get(Constants.DLG_SLC_ID)
            dlg_obj = self._load_delegation_from_pickled_instance(pickled_del=pickled_del, slc_id=slc_id)
            result.append(dlg_obj)

        return result

    def get_delegation(self, *, dlg_graph_id: str) -> ABCDelegation or None:
        try:
            #self.lock.acquire()
            dlg_dict = self.db.get_delegation(dlg_graph_id=str(dlg_graph_id))
            if self.lock.locked():
                self.lock.release()
            return self._load_delegation_from_db(dlg_dict_list=[dlg_dict])[0]
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def get_delegations(self, *, slice_id: ID = None, states: List[int] = None) -> List[ABCDelegation]:
        result = []
        try:
            #self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            dlg_dict_list = self.db.get_delegations(slc_guid=sid, states=states)
            if self.lock.locked():
                self.lock.release()
            result = self._load_delegation_from_db(dlg_dict_list=dlg_dict_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def add_maintenance_properties(self, *, properties: dict):
        """
        Add maintenance properties
        @param properties properties
        """
        self.db.add_miscellaneous(name=self.MAINTENANCE, properties=properties)

    def update_maintenance_properties(self, *, properties: dict):
        """
        Update maintenance properties
        @param properties properties
        """
        self.db.update_miscellaneous(name=self.MAINTENANCE, properties=properties)

    def remove_maintenance_properties(self):
        """
        Remove maintenance properties
        """
        self.db.remove_miscellaneous(name=self.MAINTENANCE)

    def get_maintenance_properties(self) -> dict:
        """
        Get maintenance Properties
        @return properties
        """
        result = None
        try:
            result = self.db.get_miscellaneous(name=self.MAINTENANCE)
        except Exception as e:
            self.logger.error(e)
        return result

    def add_site(self, *, site: Site):
        self.logger.debug(f"Adding site {site.get_name()}")
        try:
            #self.lock.acquire()
            properties = pickle.dumps(site)
            self.db.add_site(site_name=site.get_name(), state=site.get_state().value, properties=properties)
            self.logger.debug(f"Site {site.get_name()} added")
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_site(self, *, site: Site):
        try:
            #self.lock.acquire()
            self.logger.debug(f"Updating site {site.get_name()}")
            properties = pickle.dumps(site)
            self.db.update_site(site_name=site.get_name(), state=site.get_state().value, properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_site(self, *, site_name: str):
        try:
            #self.lock.acquire()
            self.logger.debug(f"Removing site {site_name}")
            self.db.remove_site(site_name=site_name)
        finally:
            if self.lock.locked():
                self.lock.release()

    def _load_site_from_db(self, site_list: List[dict]) -> List[Site]:
        result = []
        if site_list is None:
            return result

        for s in site_list:
            pickled_site = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            site = pickle.loads(pickled_site)
            result.append(site)

        return result

    def get_site(self, *, site_name: str) -> Site or None:
        try:
            #self.lock.acquire()
            site_list = self.db.get_site(site_name=site_name)
            if self.lock.locked():
                self.lock.release()
            if site_list is not None and len(site_list) > 0:
                return self._load_site_from_db(site_list=site_list)[0]
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return None

    def get_sites(self) -> List[Site]:
        result = []
        try:
            #self.lock.acquire()
            site_list = self.db.get_sites()
            if self.lock.locked():
                self.lock.release()
            result = self._load_site_from_db(site_list=site_list)
        except Exception as e:
            self.logger.error(e)
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def add_config_mapping(self, key: str, config_mapping: ConfigurationMapping):
        try:
            #self.lock.acquire()
            properties = pickle.dumps(config_mapping)
            self.db.add_config_mapping(cfgm_type=key, act_id=self.actor_id, properties=properties)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            raise e
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_config_mappings(self) -> List[ConfigurationMapping]:
        cfg_map_list = None
        result = []
        try:
            #self.lock.acquire()
            cfg_map_list = self.db.get_config_mappings(act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        if cfg_map_list is not None:
            for c in cfg_map_list:
                pickled_cfg_map = c.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                cfg_obj = pickle.loads(pickled_cfg_map)
                result.append(cfg_obj)
        return result

    def _load_poa_from_db(self, *, poa_dict_list: List[dict], include_res_info: bool) -> List[Poa]:
        result = []
        if poa_dict_list is None:
            return result

        for p in poa_dict_list:
            pickled_poa = p.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            poa_obj = pickle.loads(pickled_poa)
            sliver_id = poa_obj.get_sliver_id()
            if include_res_info:
                reservations = self.get_reservations(rid=sliver_id)
                if reservations is not None:
                    poa_obj.restore(actor=self.actor, reservation=reservations[0])
            result.append(poa_obj)
        return result

    def get_poas(self, *, poa_id: str = None, email: str = None, sliver_id: ID = None, slice_id: ID = None,
                 project_id: str = None, limit: int = None, offset: int = None, last_update_time: datetime = None,
                 states: list[int] = None, include_res_info: bool = True) -> Union[List[Poa] or None]:
        result = []
        try:
            try:
                sliver_id_str = str(sliver_id) if sliver_id is not None else None
                slice_id_str = str(slice_id) if slice_id is not None else None

                poa_dict_list = self.db.get_poas(poa_guid=poa_id, email=email, sliver_id=sliver_id_str, limit=limit,
                                                 offset=offset, last_update_time=last_update_time,
                                                 project_id=project_id, slice_id=slice_id_str, states=states)
            finally:
                if self.lock.locked():
                    self.lock.release()
            if poa_dict_list is not None:
                result = self._load_poa_from_db(poa_dict_list=poa_dict_list, include_res_info=include_res_info)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def add_poa(self, *, poa: Poa):
        try:
            poa_list = self.get_poas(poa_id=poa.poa_id)
            if len(poa_list) > 0:
                raise DatabaseException(f"POA # {poa.get_poa_id()} already exists")

            properties = pickle.dumps(poa)
            email = None
            project_id = None
            slice_id = None
            if poa.get_slice().get_owner() is not None:
                email = poa.get_slice().get_owner().get_email()
                project_id = poa.get_slice().get_project_id()
                slice_id = str(poa.get_slice().get_slice_id())

            self.db.add_poa(poa_guid=poa.get_poa_id(), sliver_id=str(poa.get_sliver_id()), email=email,
                            project_id=project_id, slice_id=slice_id, properties=properties,
                            state=poa.get_state().value)
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_poa(self, *, poa: Poa):
        try:
            #self.lock.acquire()
            properties = pickle.dumps(poa)
            self.db.update_poa(poa_guid=poa.poa_id, state=poa.get_state().value, properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_poa(self, *, poa_id: str):
        try:
            #self.lock.acquire()
            self.db.remove_poa(poa_guid=poa_id)
        finally:
            if self.lock.locked():
                self.lock.release()
