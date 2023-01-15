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
import traceback
from datetime import datetime
from typing import List


from fabric_cf.actor.core.apis.abc_actor_mixin import ABCActorMixin, ActorType
from fabric_cf.actor.core.apis.abc_broker_proxy import ABCBrokerProxy
from fabric_cf.actor.core.apis.abc_controller_reservation import ABCControllerReservation
from fabric_cf.actor.core.apis.abc_database import ABCDatabase
from fabric_cf.actor.core.apis.abc_delegation import ABCDelegation
from fabric_cf.actor.core.apis.abc_reservation_mixin import ABCReservationMixin, ReservationCategory
from fabric_cf.actor.core.apis.abc_slice import ABCSlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
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
            self.lock.acquire()
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
            self.lock.acquire()
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

            self.lock.acquire()
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
            self.lock.acquire()
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
            self.lock.acquire()
            self.db.remove_slice(slc_guid=str(slice_id))
        finally:
            if self.lock.locked():
                self.lock.release()

    def get_slices(self, *, slice_id: ID = None, slice_name: str = None, project_id: str = None, email: str = None,
                   states: list[int] = None, oidc_sub: str = None, slc_type: List[SliceTypes] = None,
                   limit: int = None, offset: int = None, lease_end: datetime = None) -> List[ABCSlice] or None:
        result = []
        try:
            try:
                self.lock.acquire()
                slice_type = None
                if slc_type is not None:
                    slice_type = [x.value for x in slc_type]
                sid = str(slice_id) if slice_id is not None else None
                slices = self.db.get_slices(slice_id=sid, slice_name=slice_name, project_id=project_id, email=email,
                                            states=states, oidc_sub=oidc_sub, slc_type=slice_type, limit=limit,
                                            offset=offset, lease_end=lease_end)
            finally:
                if self.lock.locked():
                    self.lock.release()
            if slices is not None:
                for s in slices:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return result

    def add_reservation(self, *, reservation: ABCReservationMixin):
        try:
            self.lock.acquire()
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
            if reservation.get_resources() is not None and reservation.get_resources().get_sliver() is not None:
                site = reservation.get_resources().get_sliver().get_site()
                rsv_type = reservation.get_resources().get_sliver().get_type().name

            self.db.add_reservation(slc_guid=str(reservation.get_slice_id()),
                                    rsv_resid=str(reservation.get_reservation_id()),
                                    rsv_category=reservation.get_category().value,
                                    rsv_state=reservation.get_state().value,
                                    rsv_pending=reservation.get_pending_state().value,
                                    rsv_joining=reservation.get_join_state().value,
                                    properties=properties,
                                    rsv_graph_node_id=reservation.get_graph_node_id(),
                                    oidc_claim_sub=oidc_claim_sub, email=email, site=site, rsv_type=rsv_type)
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
            self.lock.acquire()
            self.logger.debug("Updating reservation {} in slice {}".format(reservation.get_reservation_id(),
                                                                           reservation.get_slice()))

            site = None
            rsv_type = None
            if reservation.get_resources() is not None and reservation.get_resources().get_sliver() is not None:
                site = reservation.get_resources().get_sliver().get_site()
                rsv_type = reservation.get_resources().get_sliver().get_type().name

            properties = pickle.dumps(reservation)
            self.db.update_reservation(slc_guid=str(reservation.get_slice_id()),
                                       rsv_resid=str(reservation.get_reservation_id()),
                                       rsv_category=reservation.get_category().value,
                                       rsv_state=reservation.get_state().value,
                                       rsv_pending=reservation.get_pending_state().value,
                                       rsv_joining=reservation.get_join_state().value,
                                       properties=properties,
                                       rsv_graph_node_id=reservation.get_graph_node_id(),
                                       site=site, rsv_type = rsv_type)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_reservation(self, *, rid: ID):
        try:
            self.lock.acquire()
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

            if isinstance(result, ABCControllerReservation) and result.get_redeem_predecessors() is not None:
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
            self.lock.acquire()
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
            self.lock.acquire()
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
            self.lock.acquire()
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
            self.lock.acquire()
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

    def get_reservations(self, *, slice_id: ID = None, graph_node_id: str = None, project_id: str = None,
                         email: str = None, oidc_sub: str = None, rid: ID = None,
                         states: list[int] = None, site: str = None, rsv_type: int = None) -> List[ABCReservationMixin]:
        result = []
        try:
            self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            res_id = str(rid) if rid is not None else None
            res_dict_list = self.db.get_reservations(slice_id=sid, graph_node_id=graph_node_id,
                                                     project_id=project_id, email=email, oidc_sub=oidc_sub, rid=res_id,
                                                     states=states, site=site, rsv_type=rsv_type)
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
            self.lock.acquire()
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
            self.lock.acquire()
            self.logger.debug("Adding broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.add_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Updating broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.update_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Removing broker {}({})".format(broker.get_name(), broker.get_guid()))
            self.db.remove_proxy(act_id=self.actor_id, prx_name=broker.get_name())
        finally:
            if self.lock.locked():
                self.lock.release()

    def set_actor_name(self, *, name: str):
        self.actor_name = name

    def get_brokers(self) -> List[ABCBrokerProxy] or None:
        try:
            self.lock.acquire()
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
            self.lock.acquire()
            properties = pickle.dumps(delegation)
            self.db.add_delegation(slice_id=str(delegation.get_slice_id()),
                                   dlg_graph_id=str(delegation.get_delegation_id()),
                                   dlg_state=delegation.get_state().value,
                                   properties=properties)
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
            self.lock.acquire()
            self.logger.debug("Updating delegation {} in slice {}".format(delegation.get_delegation_id(),
                                                                          delegation.get_slice_id()))
            properties = pickle.dumps(delegation)
            self.db.update_delegation(dlg_graph_id=str(delegation.get_delegation_id()),
                                      dlg_state=delegation.get_state().value,
                                      properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_delegation(self, *, dlg_graph_id: str):
        try:
            self.lock.acquire()
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
            self.lock.acquire()
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
            self.lock.acquire()
            sid = str(slice_id) if slice_id is not None else None
            dlg_dict_list = self.db.get_delegations(slc_guid=sid, states=states)
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
            self.lock.acquire()
            properties = pickle.dumps(site)
            self.db.add_site(site_name=site.get_name(), state=site.get_state().value, properties=properties)
            self.logger.debug(f"Site {site.get_name()} added")
        finally:
            if self.lock.locked():
                self.lock.release()

    def update_site(self, *, site: Site):
        try:
            self.lock.acquire()
            self.logger.debug(f"Updating site {site.get_name()}")
            properties = pickle.dumps(site)
            self.db.update_site(site_name=site.get_name(), state=site.get_state().value, properties=properties)
        finally:
            if self.lock.locked():
                self.lock.release()

    def remove_site(self, *, site_name: str):
        try:
            self.lock.acquire()
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
            self.lock.acquire()
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
            self.lock.acquire()
            site_list = self.db.get_sites()
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
            self.lock.acquire()
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
            self.lock.acquire()
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