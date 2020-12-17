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
from typing import List

from fabric_cf.actor.core.apis.i_actor import IActor
from fabric_cf.actor.core.apis.i_broker_proxy import IBrokerProxy
from fabric_cf.actor.core.apis.i_database import IDatabase
from fabric_cf.actor.core.apis.i_delegation import IDelegation
from fabric_cf.actor.core.apis.i_reservation import IReservation, ReservationCategory
from fabric_cf.actor.core.apis.i_slice import ISlice
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.core.kernel.slice import SliceTypes
from fabric_cf.actor.core.util.id import ID
from fabric_cf.actor.core.util.resource_type import ResourceType
from fabric_cf.actor.db.psql_database import PsqlDatabase


class ActorDatabase(IDatabase):
    def __init__(self, *, user: str, password: str, database: str, db_host: str, logger):
        self.user = user
        self.password = password
        self.database = database
        self.db_host = db_host
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database, db_host=self.db_host,
                               logger=logger)
        self.actor_name = None
        self.actor_id = None
        self.initialized = False
        self.logger = None
        self.reset_state = False
        self.lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['db']
        del state['actor_name']
        del state['actor_id']
        del state['initialized']
        del state['logger']
        del state['reset_state']
        del state['lock']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.db = PsqlDatabase(user=self.user, password=self.password, database=self.database, db_host=self.db_host,
                               logger=None)
        self.actor_id = None
        self.actor_name = None
        self.initialized = False
        self.logger = None
        self.reset_state = False
        self.lock = threading.Lock()

    def set_logger(self, *, logger):
        self.logger = logger

    def set_reset_state(self, *, state: bool):
        self.reset_state = state

    def initialize(self):
        if not self.initialized:
            if self.actor_name is None:
                raise DatabaseException(Constants.not_specified_prefix.format("actor name"))
            self.initialized = True

    def actor_added(self):
        self.actor_id = self.get_actor_id_from_name(actor_name=self.actor_name)
        if self.actor_id is None:
            raise DatabaseException(Constants.object_not_found.format("actor", self.actor_name))

    def revisit(self, *, actor: IActor, properties: dict):
        return

    def get_actor_id_from_name(self, *, actor_name: str) -> int:
        try:
            self.lock.acquire()
            actor = self.db.get_actor(name=actor_name)
            self.actor_id = actor['act_id']
            return self.actor_id
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_id_from_guid(self, *, slice_id: ID) -> int:
        try:
            self.lock.acquire()
            slice_obj = self.db.get_slice(act_id=self.actor_id, slice_guid=str(slice_id))
            return slice_obj['slc_id']
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_id(self, *, id: int) -> dict:
        try:
            self.lock.acquire()
            slice_obj = self.db.get_slice_by_id(act_id=self.actor_id, id=id)
            return slice_obj
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_slice(self, *, slice_object: ISlice):
        try:
            if self.get_slice(slice_id=slice_object.get_slice_id()) is not None:
                raise DatabaseException("Slice # {} already exists".format(slice_object.get_slice_id()))
            self.lock.acquire()
            properties = pickle.dumps(slice_object)
            self.db.add_slice(act_id=self.actor_id, slc_guid=str(slice_object.get_slice_id()),
                              slc_name=slice_object.get_name(),
                              slc_type=slice_object.get_slice_type().value,
                              slc_resource_type=str(slice_object.get_resource_type()),
                              properties=properties,
                              slc_graph_id=str(slice_object.get_graph_id()))
        finally:
            self.lock.release()

    def update_slice(self, *, slice_object: ISlice):
        try:
            self.lock.acquire()
            properties = pickle.dumps(slice_object)
            self.db.update_slice(act_id=self.actor_id,
                                 slc_guid=str(slice_object.get_slice_id()),
                                 slc_name=slice_object.get_name(),
                                 slc_type=slice_object.get_slice_type().value,
                                 slc_resource_type=str(slice_object.get_resource_type()),
                                 properties=properties,
                                 slc_graph_id=str(slice_object.get_graph_id()))
        finally:
            self.lock.release()

    def remove_slice(self, *, slice_id: ID):
        try:
            self.lock.acquire()
            self.db.remove_slice(slc_guid=str(slice_id))
        finally:
            self.lock.release()

    def get_slice(self, *, slice_id: ID) -> dict:
        try:
            return self.db.get_slice(act_id=self.actor_id, slice_guid=str(slice_id))
        except Exception as e:
            self.logger.error(e)
        return None

    def get_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices(act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_inventory_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices_by_type(act_id=self.actor_id, slc_type=SliceTypes.InventorySlice.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_client_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices_by_types(act_id=self.actor_id,
                                               slc_type1=SliceTypes.ClientSlice.value,
                                               slc_type2=SliceTypes.BrokerClientSlice.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_resource_type(self, *, rtype: ResourceType) -> dict:
        try:
            self.lock.acquire()
            result_list = self.db.get_slices_by_resource_type(act_id=self.actor_id, slc_resource_type=str(rtype))
            if result_list is not None and len(result_list) > 0:
                return next(iter(result_list))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_reservation(self, *, reservation: IReservation):
        try:
            self.lock.acquire()
            self.logger.debug("Adding reservation {} to slice {}".format(reservation.get_reservation_id(),
                                                                         reservation.get_slice()))
            properties = pickle.dumps(reservation)
            self.db.add_reservation(act_id=self.actor_id,
                                    slc_guid=str(reservation.get_slice_id()),
                                    rsv_resid=str(reservation.get_reservation_id()),
                                    rsv_category=reservation.get_category().value,
                                    rsv_state=reservation.get_state().value,
                                    rsv_pending=reservation.get_pending_state().value,
                                    rsv_joining=reservation.get_join_state().value,
                                    properties=properties)
            self.logger.debug(
                "Reservation {} added to slice {}".format(reservation.get_reservation_id(), reservation.get_slice()))
        finally:
            self.lock.release()

    def update_reservation(self, *, reservation: IReservation):
        # Update the reservation only when there are changes to be reflected in database
        if not reservation.is_dirty():
            return
        reservation.clear_dirty()
        try:
            self.lock.acquire()
            self.logger.debug("Updating reservation {} in slice {}".format(reservation.get_reservation_id(),
                                                                           reservation.get_slice()))
            properties = pickle.dumps(reservation)
            self.db.update_reservation(act_id=self.actor_id,
                                       slc_guid=str(reservation.get_slice_id()),
                                       rsv_resid=str(reservation.get_reservation_id()),
                                       rsv_category=reservation.get_category().value,
                                       rsv_state=reservation.get_state().value,
                                       rsv_pending=reservation.get_pending_state().value,
                                       rsv_joining=reservation.get_join_state().value,
                                       properties=properties)

        finally:
            self.lock.release()

    def remove_reservation(self, *, rid: ID):
        try:
            self.lock.acquire()
            self.logger.debug("Removing reservation {}".format(rid))
            self.db.remove_reservation(rsv_resid=str(rid))
        finally:
            self.lock.release()

    def get_reservation(self, *, rid: ID) -> dict:
        try:
            self.lock.acquire()
            return self.db.get_reservation(act_id=self.actor_id, rsv_resid=str(rid))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_slice_id(self, *, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id(act_id=self.actor_id, slc_guid=str(slice_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_slice_id_state(self, *, slice_id: ID, state: int) -> list:
        try:
            return self.db.get_reservations_by_slice_id_state(act_id=self.actor_id, slc_guid=str(slice_id),
                                                              rsv_state=state)
        except Exception as e:
            self.logger.error(e)
        return None

    def get_client_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_2_category(act_id=self.actor_id,
                                                          rsv_cat1=ReservationCategory.Broker.value,
                                                          rsv_cat2=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_client_reservations_by_slice_id(self, *, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id_by_2_category(act_id=self.actor_id,
                                                                      slc_guid=str(slice_id),
                                                                      rsv_cat1=ReservationCategory.Broker.value,
                                                                      rsv_cat2=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_holdings(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(act_id=self.actor_id, rsv_cat=ReservationCategory.Client.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_holdings_by_slice_id(self, *, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id_by_category(act_id=self.actor_id, slc_guid=str(slice_id),
                                                                    rsv_cat=ReservationCategory.Client.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_broker_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(act_id=self.actor_id, rsv_cat=ReservationCategory.Broker.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_authority_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(act_id=self.actor_id,
                                                        rsv_cat=ReservationCategory.Authority.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations(self) -> list:
        try:
            self.lock.acquire()
            self.logger.debug("Actor ID: {}".format(self.actor_id))
            return self.db.get_reservations(act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_state(self, *, state: int) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_state(act_id=self.actor_id, rsv_state=state)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_rids(self, *, rid: List[str]):
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_rids(act_id=self.actor_id, rsv_resid_list=rid)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_broker(self, *, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Adding broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.add_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            self.lock.release()

    def update_broker(self, *, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Updating broker {}({})".format(broker.get_name(), broker.get_guid()))
            properties = pickle.dumps(broker)
            self.db.update_proxy(act_id=self.actor_id, prx_name=broker.get_name(), properties=properties)
        finally:
            self.lock.release()

    def remove_broker(self, *, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            self.logger.debug("Removing broker {}({})".format(broker.get_name(), broker.get_guid()))
            self.db.remove_proxy(act_id=self.actor_id, prx_name=broker.get_name())
        finally:
            self.lock.release()

    def set_actor_name(self, *, name: str):
        self.actor_name = name

    def get_brokers(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_proxies(act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_delegation(self, *, delegation: IDelegation):
        try:
            self.lock.acquire()
            self.logger.debug("Adding delegation {} to slice {}".format(delegation.get_delegation_id(),
                                                                        delegation.get_slice_id()))

            slice_object = self.get_slice(slice_id=delegation.get_slice_id())
            if slice_object is None:
                raise DatabaseException("Slice with id: {} not found".format(delegation.get_slice_id()))

            properties = pickle.dumps(delegation)
            self.db.add_delegation(dlg_act_id=self.actor_id,
                                   dlg_slc_id=slice_object['slc_id'],
                                   dlg_graph_id=str(delegation.get_delegation_id()),
                                   dlg_state=delegation.get_state().value,
                                   properties=properties)
            self.logger.debug(
                "Delegation {} added to slice {}".format(delegation.get_delegation_id(),
                                                         delegation.get_slice_id()))
        finally:
            self.lock.release()

    def update_delegation(self, *, delegation: IDelegation):
        # Update the delegation only when there are changes to be reflected in database
        if not delegation.is_dirty():
            return
        delegation.clear_dirty()
        try:
            self.lock.acquire()
            self.logger.debug("Updating delegation {} in slice {}".format(delegation.get_delegation_id(),
                                                                          delegation.get_slice_id()))
            properties = pickle.dumps(delegation)
            self.db.update_delegation(dlg_act_id=self.actor_id,
                                      dlg_graph_id=str(delegation.get_delegation_id()),
                                      dlg_state=delegation.get_state().value,
                                      properties=properties)
        finally:
            self.lock.release()

    def remove_delegation(self, *, dlg_graph_id: ID):
        try:
            self.lock.acquire()
            self.logger.debug("Removing delegation {}".format(dlg_graph_id))
            self.db.remove_delegation(dlg_graph_id=str(dlg_graph_id))
        finally:
            self.lock.release()

    def get_delegation(self, *, dlg_graph_id: ID) -> dict:
        try:
            self.lock.acquire()
            return self.db.get_delegation(dlg_act_id=self.actor_id, dlg_graph_id=str(dlg_graph_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_delegations(self) -> list:
        try:
            self.lock.acquire()
            self.logger.debug("Actor ID: {}".format(self.actor_id))
            return self.db.get_delegations(dlg_act_id=self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_delegations_by_slice_id(self, *, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_delegations_by_slice_id(dlg_act_id=self.actor_id, slc_guid=str(slice_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None
