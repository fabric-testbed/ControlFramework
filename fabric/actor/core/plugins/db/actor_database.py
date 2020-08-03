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

from fabric.actor.core.apis.i_actor import IActor
from fabric.actor.core.apis.i_broker_proxy import IBrokerProxy
from fabric.actor.core.apis.i_database import IDatabase
from fabric.actor.core.apis.i_reservation import IReservation
from fabric.actor.core.apis.i_slice import ISlice
from fabric.actor.core.kernel.slice import SliceTypes
from fabric.actor.core.util.id import ID
from fabric.actor.core.util.resource_type import ResourceType
from fabric.actor.db.psql_database import PsqlDatabase


class ActorDatabase(IDatabase):
    def __init__(self, user: str, password: str, database: str, db_host: str, logger):
        self.user = user
        self.password = password
        self.database = database
        self.db_host = db_host
        self.db = PsqlDatabase(self.user, self.password, self.database, self.db_host, logger)
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
        from fabric.actor.db.psql_database import PsqlDatabase
        self.db = PsqlDatabase(self.user, self.password, self.database, self.db_host, None)
        self.actor_id = None
        self.actor_name = None
        self.initialized = False
        self.logger = None
        self.reset_state = False
        self.lock = threading.Lock()

    def set_logger(self, logger):
        self.logger = logger

    def set_reset_state(self, state: bool):
        self.reset_state = state

    def initialize(self):
        if not self.initialized:
            if self.actor_name is None:
                raise Exception("Missing actor_name")
            self.initialized = True

    def actor_added(self):
        self.actor_id = self.get_actor_id_from_name(self.actor_name)
        if self.actor_id is None:
            raise Exception("Actor record is not present in the database: {}".format(self.actor_name))

    def revisit(self, actor: IActor, properties: dict):
        return

    def get_actor_id_from_name(self, actor_name: str) -> int:
        try:
            self.lock.acquire()
            actor = self.db.get_actor(actor_name)
            self.actor_id = actor['act_id']
            return self.actor_id
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_id_from_guid(self, slice_id: ID) -> int:
        try:
            self.lock.acquire()
            slice_obj = self.db.get_slice(self.actor_id, str(slice_id))
            return slice_obj['slc_id']
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_id(self, id: int) -> dict:
        try:
            self.lock.acquire()
            slice_obj = self.db.get_slice_by_id(self.actor_id, id)
            return slice_obj
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_slice(self, slice_object: ISlice):
        try:
            self.lock.acquire()
            properties = pickle.dumps(slice_object)
            self.db.add_slice(self.actor_id, str(slice_object.get_slice_id()), slice_object.get_name(),
                              slice_object.get_slice_type().value, str(slice_object.get_resource_type()), properties)
        finally:
            self.lock.release()

    def update_slice(self, slice_object: ISlice):
        try:
            self.lock.acquire()
            properties = pickle.dumps(slice_object)
            self.db.update_slice(self.actor_id, str(slice_object.get_slice_id()), slice_object.get_name(),
                                 slice_object.get_slice_type().value, str(slice_object.get_resource_type()), properties)
        finally:
            self.lock.release()

    def remove_slice(self, slice_id: ID):
        try:
            self.lock.acquire()
            self.db.remove_slice(self.actor_id, str(slice_id))
        finally:
            self.lock.release()

    def get_slice(self, slice_id: ID) -> dict:
        try:
            return self.db.get_slice(self.actor_id, str(slice_id))
        except Exception as e:
            self.logger.error(e)
        return None

    def get_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices(self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_inventory_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices_by_type(self.actor_id, SliceTypes.InventorySlice.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_client_slices(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_slices_by_types(self.actor_id, SliceTypes.ClientSlice.value, SliceTypes.BrokerClientSlice.value)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_slice_by_resource_type(self, rtype: ResourceType) -> dict:
        try:
            self.lock.acquire()
            return self.db.get_slices_by_resource_type(self.actor_id, str(rtype))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_reservation(self, reservation: IReservation):
        try:
            self.lock.acquire()
            self.logger.debug("Adding reservation {} to slice {}".format(reservation.get_reservation_id(), reservation.get_slice()))
            properties = pickle.dumps(reservation)
            self.db.add_reservation(self.actor_id, str(reservation.get_slice_id()), str(reservation.get_reservation_id()),
                                    reservation.get_category(), reservation.get_state().value, reservation.get_pending_state().value,
                                    reservation.get_join_state().value, properties)
        finally:
            self.lock.release()

    def update_reservation(self, reservation: IReservation):
        try:
            self.lock.acquire()
            properties = pickle.dumps(reservation)
            self.db.update_reservation(self.actor_id, str(reservation.get_slice_id()), str(reservation.get_reservation_id()),
                                       reservation.get_category(), reservation.get_state().value, reservation.get_pending_state().value,
                                       reservation.get_join_state().value, properties)
        finally:
            self.lock.release()

    def remove_reservation(self, rid: ID):
        try:
            self.lock.acquire()
            self.db.remove_reservation(self.actor_id, str(rid))
        finally:
            self.lock.release()

    def get_reservation(self, rid: ID) -> dict:
        try:
            self.lock.acquire()
            return self.db.get_reservation(self.actor_id, str(rid))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_slice_id(self, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id(self.actor_id, str(slice_id))
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_slice_id_state(self, slice_id: ID, state: int) -> list:
        try:
            return self.db.get_reservations_by_slice_id_state(self.actor_id, str(slice_id), state)
        except Exception as e:
            self.logger.error(e)
        return None

    def get_client_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_2_category(self.actor_id, IReservation.CategoryBroker,
                                                      IReservation.CategoryAuthority)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_client_reservations_by_slice_id(self, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id_by_2_category(self.actor_id, str(slice_id),
                                                                      IReservation.CategoryBroker,
                                                                      IReservation.CategoryAuthority)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_holdings(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(self.actor_id, IReservation.CategoryClient)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_holdings_by_slice_id(self, slice_id: ID) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_slice_id_by_category(self.actor_id, str(slice_id), IReservation.CategoryClient)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_broker_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(self.actor_id, IReservation.CategoryBroker)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_authority_reservations(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_category(self.actor_id, IReservation.CategoryAuthority)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations(self) -> list:
        try:
            self.lock.acquire()
            self.logger.debug("Actor ID: {}".format(self.actor_id))
            return self.db.get_reservations(self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_state(self, state: int) -> list:
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_state(self.actor_id, state)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def get_reservations_by_rids(self, rid: list):
        try:
            self.lock.acquire()
            return self.db.get_reservations_by_rids(self.actor_id, rid)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None

    def add_broker(self, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            properties = pickle.dumps(broker)
            self.db.add_proxy(self.actor_id, broker.get_name(), properties)
        finally:
            self.lock.release()

    def update_broker(self, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            properties = pickle.dumps(broker)
            self.db.update_proxy(self.actor_id, broker.get_name(), properties)
        finally:
            self.lock.release()

    def remove_broker(self, broker: IBrokerProxy):
        try:
            self.lock.acquire()
            self.db.remove_proxy(self.actor_id, broker.get_name())
        finally:
            self.lock.release()

    def set_actor_name(self, name: str):
        self.actor_name = name

    def get_brokers(self) -> list:
        try:
            self.lock.acquire()
            return self.db.get_proxies(self.actor_id)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.lock.release()
        return None




