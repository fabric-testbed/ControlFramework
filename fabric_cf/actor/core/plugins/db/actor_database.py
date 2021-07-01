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
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.db.psql_database import PsqlDatabase
from fabric_cf.actor.fim.fim_helper import FimHelper


class ActorDatabase(ABCDatabase):
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
        self.logger = None
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
            self.lock.release()
        return None

    def get_slice_id_from_guid(self, *, slice_id: ID) -> int or None:
        try:
            self.lock.acquire()
            slice_obj = self.db.get_slice(slice_guid=str(slice_id))
            return slice_obj['slc_id']
        except Exception as e:
            self.logger.error(e)
        finally:
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
            self.lock.release()
        return None

    def add_slice(self, *, slice_object: ABCSlice):
        try:
            if self.get_slice(slice_id=slice_object.get_slice_id()) is not None:
                raise DatabaseException("Slice # {} already exists".format(slice_object.get_slice_id()))
            self.lock.acquire()
            properties = pickle.dumps(slice_object)
            oidc_claim_sub = None
            email = None
            if slice_object.get_owner() is not None:
                oidc_claim_sub = slice_object.get_owner().get_oidc_sub_claim()
                email = slice_object.get_owner().get_email()
            self.db.add_slice(slc_guid=str(slice_object.get_slice_id()),
                              slc_name=slice_object.get_name(),
                              slc_type=slice_object.get_slice_type().value,
                              slc_resource_type=str(slice_object.get_resource_type()),
                              properties=properties,
                              slc_graph_id=slice_object.get_graph_id(),
                              oidc_claim_sub=oidc_claim_sub, email=email,
                              lease_end=slice_object.get_lease_end(), lease_start=slice_object.get_lease_start())
        finally:
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
                                 slc_resource_type=str(slice_object.get_resource_type()),
                                 properties=properties,
                                 slc_graph_id=slice_object.get_graph_id(),
                                 lease_end=slice_object.get_lease_end(), lease_start=slice_object.get_lease_start())
        finally:
            self.lock.release()

    def remove_slice(self, *, slice_id: ID):
        try:
            self.lock.acquire()
            self.db.remove_slice(slc_guid=str(slice_id))
        finally:
            self.lock.release()

    def get_slice(self, *, slice_id: ID) -> ABCSlice or None:
        try:
            slice_dict = self.db.get_slice(slice_guid=str(slice_id))
            if slice_dict is not None:
                pickled_slice = slice_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                return pickle.loads(pickled_slice)
        except Exception as e:
            self.logger.error(e)
        return None

    def get_slice_by_name(self, *, slice_name: str, oidc_claim_sub: str, email: str) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slice_by_name(slice_name=slice_name,
                                                        oidc_claim_sub=oidc_claim_sub)
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_oidc_claim_sub(self, *, oidc_claim_sub: str) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slice_by_oidc_claim_sub(oidc_claim_sub=oidc_claim_sub)
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_email(self, *, email: str) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slice_by_email(email=email)
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slices(self) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slices()
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_inventory_slices(self) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slices_by_type(slc_type=SliceTypes.InventorySlice.value)
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_client_slices(self) -> List[ABCSlice] or None:
        try:
            self.lock.acquire()
            result = []
            slice_dict_list = self.db.get_slices_by_types(slc_type1=SliceTypes.ClientSlice.value,
                                                          slc_type2=SliceTypes.BrokerClientSlice.value)
            if slice_dict_list is not None:
                for s in slice_dict_list:
                    pickled_slice = s.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                    slice_obj = pickle.loads(pickled_slice)
                    result.append(slice_obj)
            return result
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_resource_type(self, *, rtype: ResourceType) -> ABCSlice or None:
        try:
            self.lock.acquire()
            result_list = self.db.get_slices_by_resource_type(slc_resource_type=str(rtype))
            if result_list is not None and len(result_list) > 0:
                slice_dict = next(iter(result_list))
                pickled_slice = slice_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                return pickle.loads(pickled_slice)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

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

            self.db.add_reservation(slc_guid=str(reservation.get_slice_id()),
                                    rsv_resid=str(reservation.get_reservation_id()),
                                    rsv_category=reservation.get_category().value,
                                    rsv_state=reservation.get_state().value,
                                    rsv_pending=reservation.get_pending_state().value,
                                    rsv_joining=reservation.get_join_state().value,
                                    properties=properties,
                                    rsv_graph_node_id=reservation.get_graph_node_id(),
                                    oidc_claim_sub=oidc_claim_sub, email=email)
            self.logger.debug(
                "Reservation {} added to slice {}".format(reservation.get_reservation_id(), reservation.get_slice()))
        finally:
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
            properties = pickle.dumps(reservation)
            self.db.update_reservation(slc_guid=str(reservation.get_slice_id()),
                                       rsv_resid=str(reservation.get_reservation_id()),
                                       rsv_category=reservation.get_category().value,
                                       rsv_state=reservation.get_state().value,
                                       rsv_pending=reservation.get_pending_state().value,
                                       rsv_joining=reservation.get_join_state().value,
                                       properties=properties,
                                       rsv_graph_node_id=reservation.get_graph_node_id())
        finally:
            self.lock.release()

    def remove_reservation(self, *, rid: ID):
        try:
            self.lock.acquire()
            self.logger.debug("Removing reservation {}".format(rid))
            self.db.remove_reservation(rsv_resid=str(rid))
        finally:
            self.lock.release()

    def _load_reservation_from_pickled_object(self, pickled_res: bytes, slice_id: int) -> ABCReservationMixin or None:
        try:
            slice_obj = self.get_slice_by_id(slc_id=slice_id)
            result = pickle.loads(pickled_res)
            result.restore(actor=self.actor, slice_obj=slice_obj)

            if isinstance(result, ABCControllerReservation) and result.get_redeem_predecessors() is not None:
                for p in result.get_redeem_predecessors():
                    if p.reservation_id is not None:
                        parent = self.get_reservation(rid=p.reservation_id)
                        p.set_reservation(reservation=parent)

                for p in result.get_join_predecessors():
                    if p.reservation_id is not None:
                        parent = self.get_reservation(rid=p.reservation_id)
                        p.set_reservation(reservation=parent)

            return result
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
        return None

    def get_reservation(self, *, rid: ID, oidc_claim_sub: str = None) -> ABCReservationMixin or None:
        res_dict = None
        try:
            self.lock.acquire()
            res_dict = self.db.get_reservation(rsv_resid=str(rid), oidc_claim_sub=oidc_claim_sub)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict is not None:
            pickled_res = res_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            slice_id = res_dict.get(Constants.RSV_SLC_ID)
            return self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
        return None

    def get_reservations_by_slice_id(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_slice_id(slc_guid=str(slice_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_graph_node_id(self, *, graph_node_id: str) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_graph_node_id(graph_node_id=graph_node_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_oidc_claim_sub(self, *, oidc_claim_sub: str) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_oidc_claim_sub(oidc_claim_sub=oidc_claim_sub)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_email(self, *, email: str) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_email(email=email)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_slice_id_state(self, *, slice_id: ID, state: int) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            res_dict_list = self.db.get_reservations_by_slice_id_state(slc_guid=str(slice_id), rsv_state=state)
        except Exception as e:
            self.logger.error(e)
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_email_state(self, *, email: str, state: int) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            res_dict_list = self.db.get_reservations_by_email_state(email=email, rsv_state=state)
        except Exception as e:
            self.logger.error(e)
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_client_reservations(self) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_2_category(rsv_cat1=ReservationCategory.Broker.value,
                                                                   rsv_cat2=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_client_reservations_by_slice_id(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_slice_id_by_2_category(slc_guid=str(slice_id),
                                                                               rsv_cat1=ReservationCategory.Broker.value,
                                                                               rsv_cat2=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_holdings(self) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_category(rsv_cat=ReservationCategory.Client.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_holdings_by_slice_id(self, *, slice_id: ID) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_slice_id_by_category(slc_guid=str(slice_id),
                                                                             rsv_cat=ReservationCategory.Client.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_broker_reservations(self) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_category(rsv_cat=ReservationCategory.Broker.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_authority_reservations(self) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            result = []
            res_dict_list = self.db.get_reservations_by_category(rsv_cat=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations(self) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            self.logger.debug("Actor ID: {}".format(self.actor_id))
            res_dict_list = self.db.get_reservations()
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_state(self, *, state: int) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_state(rsv_state=state)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def get_reservations_by_rids(self, *, rid: List[str]) -> List[ABCReservationMixin]:
        result = []
        res_dict_list = None
        try:
            self.lock.acquire()
            res_dict_list = self.db.get_reservations_by_rids(rsv_resid_list=rid)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if res_dict_list is not None:
            for r in res_dict_list:
                pickled_res = r.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = r.get(Constants.RSV_SLC_ID)
                res_obj = self._load_reservation_from_pickled_object(pickled_res=pickled_res, slice_id=slice_id)
                result.append(res_obj)
        return result

    def add_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Adding broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.add_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            self.lock.release()

    def update_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Updating broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.update_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            self.lock.release()

    def remove_broker(self, *, broker: ABCBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Removing broker {}({})".format(broker.get_name(), broker.get_guid()))
            self.db.remove_proxy(act_id=self.actor_id, prx_name=broker.get_name())
        finally:
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
            self.lock.release()
        return None

    def add_delegation(self, *, delegation: ABCDelegation):
        self.logger.debug("Adding delegation {} to slice {}".format(delegation.get_delegation_id(),
                                                                    delegation.get_slice_id()))

        slc_id = self.get_slice_id_from_guid(slice_id=delegation.get_slice_id())
        if slc_id is None:
            raise DatabaseException("Slice with id: {} not found".format(delegation.get_slice_id()))

        try:
            self.lock.acquire()
            properties = pickle.dumps(delegation)
            self.db.add_delegation(dlg_slc_id=slc_id,
                                   dlg_graph_id=str(delegation.get_delegation_id()),
                                   dlg_state=delegation.get_state().value,
                                   properties=properties)
            self.logger.debug(
                "Delegation {} added to slice {}".format(delegation.get_delegation_id(),
                                                         delegation.get_slice_id()))
        finally:
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
            self.lock.release()

    def remove_delegation(self, *, dlg_graph_id: str):
        try:
            self.lock.acquire()
            self.logger.debug("Removing delegation {}".format(dlg_graph_id))
            self.db.remove_delegation(dlg_graph_id=str(dlg_graph_id))
        finally:
            self.lock.release()

    def _load_delegation_from_pickled_instance(self, pickled_del: bytes, slice_id: int) -> ABCDelegation:
        slice_obj = self.get_slice_by_id(slc_id=slice_id)
        delegation = pickle.loads(pickled_del)
        delegation.restore(actor=self.actor, slice_obj=slice_obj)
        return delegation

    def get_delegation(self, *, dlg_graph_id: str) -> ABCDelegation or None:
        dlg_dict = None
        try:
            self.lock.acquire()
            dlg_dict = self.db.get_delegation(dlg_graph_id=str(dlg_graph_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if dlg_dict is not None:
            pickled_del = dlg_dict.get(Constants.PROPERTY_PICKLE_PROPERTIES)
            slice_id = dlg_dict.get(Constants.DLG_SLC_ID)
            return self._load_delegation_from_pickled_instance(pickled_del=pickled_del, slice_id=slice_id)
        return None

    def get_delegations(self, state: int = None) -> List[ABCDelegation]:
        result = []
        dlg_dict_list = None
        try:
            self.lock.acquire()
            self.logger.debug("Actor ID: {}".format(self.actor_id))
            dlg_dict_list = self.db.get_delegations(state=state)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if dlg_dict_list is not None:
            for d in dlg_dict_list:
                pickled_del = d.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = d.get(Constants.DLG_SLC_ID)
                dlg_obj = self._load_delegation_from_pickled_instance(pickled_del=pickled_del, slice_id=slice_id)
                result.append(dlg_obj)
        return result

    def get_delegations_by_slice_id(self, *, slice_id: ID, state: int = None) -> List[ABCDelegation]:
        result = []
        dlg_dict_list = None
        try:
            self.lock.acquire()
            result = []
            dlg_dict_list = self.db.get_delegations_by_slice_id(slc_guid=str(slice_id), state=state)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        if dlg_dict_list is not None:
            for d in dlg_dict_list:
                pickled_del = d.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                slice_id = d.get(Constants.DLG_SLC_ID)
                dlg_obj = self._load_delegation_from_pickled_instance(pickled_del=pickled_del, slice_id=slice_id)
                result.append(dlg_obj)
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
            self.lock.release()
        if cfg_map_list is not None:
            for c in cfg_map_list:
                pickled_cfg_map = c.get(Constants.PROPERTY_PICKLE_PROPERTIES)
                cfg_obj = pickle.loads(pickled_cfg_map)
                result.append(cfg_obj)
        return result
