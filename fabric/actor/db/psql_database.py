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
import logging
import pickle
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from fabric.actor.db import Base, Clients, ConfigMappings, Proxies, Units, Reservations, Slices, ManagerObjects, \
    Miscellaneous, Plugins, Actors


@contextmanager
def session_scope(psql_db_engine):
    """Provide a transactional scope around a series of operations."""
    session = scoped_session(sessionmaker(psql_db_engine))
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class PsqlDatabase:
    def __init__(self, *, user: str, password: str, database: str, db_host: str, logger):
        # Connecting to PostgreSQL server at localhost using psycopg2 DBAPI
        self.db_engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(user, password, db_host, database))
        self.logger = logger

    def create_db(self):
        Base.metadata.create_all(self.db_engine)

    def reset_db(self):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).delete()
                session.query(ConfigMappings).delete()
                session.query(Proxies).delete()
                session.query(Units).delete()
                session.query(Reservations).delete()
                session.query(Slices).delete()
                session.query(ManagerObjects).delete()
                session.query(Miscellaneous).delete()
                session.query(Plugins).delete()
                session.query(Actors).delete()

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def add_actor(self, *, name: str, guid: str, act_type: int, properties):
        try:
            # Save the actor in the database
            actor_obj = Actors(act_name=name, act_guid=guid, act_type=act_type, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(actor_obj)

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_actor(self, *, name: str, properties):
        try:
            with session_scope(self.db_engine) as session:
                actor = session.query(Actors).filter_by(act_name=name).first()
                if actor is not None:
                    actor.properties = properties

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_actor(self, *, name: str):
        try:
            # Delete the actor in the database
            with session_scope(self.db_engine) as session:
                session.query(Actors).filter(Actors.act_name == name).delete()

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_actors(self) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_actors_by_name_and_type(self, *, actor_name: str, act_type: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).filter(Actors.act_type == act_type).filter(Actors.act_name.like(actor_name)).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_actors_by_name(self, *, act_name: str) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).filter(Actors.act_name.like(act_name)).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_actor(self, *, name: str) -> dict:
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                actor = session.query(Actors).filter_by(act_name=name).first()
                if actor is not None:
                    result['act_guid'] = actor.act_guid
                    result['act_name'] = actor.act_name
                    result['act_type'] = actor.act_type
                    result['properties'] = actor.properties
                    result['act_id'] = actor.act_id
                else:
                    result = None
                    self.logger.error("Actor: {} not found!".format(name))
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_miscellaneous(self, *, name: str, properties: dict):
        try:
            msc_obj = Miscellaneous(msc_path=name, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(msc_obj)

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_miscellaneous(self, *, name: str) -> dict:
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                msc_obj = session.query(Miscellaneous).filter_by(msc_path=name).first()
                if msc_obj is not None:
                    result = msc_obj.properties
                else:
                    return None
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_manager_object(self, *, manager_key: str, properties: dict, act_id: int = None):
        try:
            if act_id is not None:
                mng_obj = ManagerObjects(mo_key=manager_key, mo_act_id=act_id, properties=properties)
            else:
                mng_obj = ManagerObjects(mo_key=manager_key, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(mng_obj)

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_manager_object(self, *, manager_key: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(ManagerObjects).filter(ManagerObjects.mo_key == manager_key).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_manager_object_by_actor(self, *, act_id: int):
        try:
            with session_scope(self.db_engine) as session:
                session.query(ManagerObjects).filter(ManagerObjects.mo_act_id == act_id).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_manager_objects(self, *, act_id: int = None) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                if act_id is None:
                    for row in session.query(ManagerObjects).all():
                        mo = {'mo_act_id': row.mo_act_id, 'mo_key': row.mo_key, 'properties': row.properties,
                              'mo_id': row.mo_id}
                        result.append(mo.copy())
                        mo.clear()
                else:
                    for row in session.query(ManagerObjects).filter(ManagerObjects.mo_act_id == act_id):
                        mo = {'mo_act_id': row.mo_act_id, 'mo_key': row.mo_key, 'properties': row.properties,
                              'mo_id': row.mo_id}
                        result.append(mo.copy())
                        mo.clear()

        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_manager_objects_by_actor_name(self, *, act_name: str = None) -> list:
        result = []
        try:
            act_obj = self.get_actor(name=act_name)
            act_id = act_obj['act_id']
            result = self.get_manager_objects(act_id=act_id)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_manager_object(self, *, mo_key: str) -> dict:
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                mo_obj = session.query(ManagerObjects).filter_by(mo_key=mo_key).first()
                if mo_obj is not None:
                    result['mo_id'] = mo_obj.mo_id
                    result['mo_key'] = mo_obj.mo_key
                    if mo_obj.mo_act_id is not None:
                        result['mo_act_id'] = mo_obj.mo_act_id
                    result['properties'] = mo_obj.properties
                else:
                    raise Exception("Manager Object not found")
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_manager_containers(self) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for mo_obj in session.query(ManagerObjects).filter(ManagerObjects.mo_act_id.is_(None)):
                    mo = {'mo_id': mo_obj.mo_id, 'mo_key': mo_obj.mo_key, 'properties': mo_obj.properties}
                    result.append(mo.copy())
                    mo.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_slice(self, *, act_id: int, slc_guid: str, slc_name: str, slc_type: int,
                  slc_resource_type: str, properties):
        try:
            slc_obj = Slices(slc_guid=slc_guid, slc_name=slc_name, slc_type=slc_type,
                             slc_resource_type=slc_resource_type, properties=properties, slc_act_id=act_id)
            with session_scope(self.db_engine) as session:
                session.add(slc_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_slice(self, *, act_id: int, slc_guid: str, slc_name: str, slc_type: int,
                  slc_resource_type: str, properties):
        try:
            with session_scope(self.db_engine) as session:
                slc_obj = session.query(Slices).filter(Slices.slc_guid == slc_guid).filter(Slices.slc_act_id == act_id).first()
                if slc_obj is not None:
                    slc_obj.properties = properties
                    slc_obj.slc_name = slc_name
                    slc_obj.slc_type = slc_type
                    slc_obj.slc_resource_type = slc_resource_type
                else:
                    raise Exception("Slice not found")
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_slice(self, *, act_id: int, slc_guid: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Slices).filter(Slices.slc_guid == slc_guid).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_slices(self, *, act_id: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).filter(Slices.slc_act_id == act_id):
                    slice_obj = {'slc_id': row.slc_id, 'slc_guid': row.slc_guid, 'slc_name': row.slc_name,
                                 'slc_type': row.slc_type, 'slc_resource_type': row.slc_resource_type,
                                 'properties': row.properties, 'slc_act_id': row.slc_act_id}
                    result.append(slice_obj.copy())
                    slice_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slice_ids(self, *, act_id: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).filter(Slices.slc_act_id == act_id):
                    result.append(row.slc_id)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slice(self, *, act_id: int, slice_guid: str) -> dict:
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                slc_obj = session.query(Slices).filter(Slices.slc_act_id == act_id).filter(Slices.slc_guid == slice_guid).first()
                if slc_obj is not None:
                    result = {'slc_id': slc_obj.slc_id, 'slc_guid': slc_obj.slc_guid, 'slc_name': slc_obj.slc_name,
                              'slc_type': slc_obj.slc_type, 'slc_resource_type': slc_obj.slc_resource_type,
                              'properties': slc_obj.properties, 'slc_act_id': slc_obj.slc_act_id}
                else:
                    raise Exception("Slice not found for actor {} slice {}".format(act_id, slice_guid))
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slice_by_id(self, *, act_id: int, id: int) -> dict:
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                slc_obj = session.query(Slices).filter(Slices.slc_act_id == act_id).filter(Slices.slc_id == id).first()
                if slc_obj is not None:
                    result = {'slc_id': slc_obj.slc_id, 'slc_guid': slc_obj.slc_guid, 'slc_name': slc_obj.slc_name,
                              'slc_type': slc_obj.slc_type, 'slc_resource_type': slc_obj.slc_resource_type,
                              'properties': slc_obj.properties, 'slc_act_id': slc_obj.slc_act_id}
                else:
                    raise Exception("Slice not found for actor {} slice {}".format(act_id, id))
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slices_by_type(self, *, act_id: int, slc_type: int):
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).filter(Slices.slc_act_id == act_id).filter(Slices.slc_type == slc_type):
                    slice_obj = {'slc_id': row.slc_id, 'slc_guid': row.slc_guid, 'slc_name': row.slc_name,
                                 'slc_type': row.slc_type, 'slc_resource_type': row.slc_resource_type,
                                 'properties': row.properties, 'slc_act_id': row.slc_act_id}
                    result.append(slice_obj.copy())
                    slice_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slices_by_types(self, *, act_id: int, slc_type1: int, slc_type2: int):
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).filter(Slices.slc_act_id == act_id):
                    if row.slc_type == slc_type1 or row.slc_type == slc_type2:
                        slice_obj = {'slc_id': row.slc_id, 'slc_guid': row.slc_guid, 'slc_name': row.slc_name,
                                     'slc_type': row.slc_type, 'slc_resource_type': row.slc_resource_type,
                                     'properties': row.properties, 'slc_act_id': row.slc_act_id}
                        result.append(slice_obj.copy())
                        slice_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_slices_by_resource_type(self, *, act_id: int, slc_resource_type: str):
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).filter(Slices.slc_act_id == act_id).filter(Slices.slc_resource_type == slc_resource_type):
                    slice_obj = {'slc_id': row.slc_id, 'slc_guid': row.slc_guid, 'slc_name': row.slc_name,
                                 'slc_type': row.slc_type, 'slc_resource_type': row.slc_resource_type,
                                 'properties': row.properties, 'slc_act_id': row.slc_act_id}
                    result.append(slice_obj.copy())
                    slice_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_reservation(self, *, act_id: int, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                        rsv_pending: int, rsv_joining: int, properties):
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            rsv_obj = Reservations(rsv_slc_id=slc_obj['slc_id'], rsv_resid=rsv_resid, rsv_category=rsv_category,
                                   rsv_state=rsv_state, rsv_pending=rsv_pending, rsv_joining=rsv_joining,
                                   properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(rsv_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_reservation(self, *, act_id: int, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                           rsv_pending: int, rsv_joining: int, properties):
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            with session_scope(self.db_engine) as session:
                rsv_obj = session.query(Reservations).filter(Reservations.rsv_slc_id == slc_obj['slc_id']).filter(Reservations.rsv_resid == rsv_resid).first()
                if rsv_obj is not None:
                    rsv_obj.rsv_category = rsv_category
                    rsv_obj.rsv_state = rsv_state
                    rsv_obj.rsv_pending = rsv_pending
                    rsv_obj.rsv_joining = rsv_joining
                    rsv_obj.properties = properties
                else:
                    raise Exception("Reservation not found")
        except Exception as e:
            print("Exception occurred while updating reservation {}".format(self.get_slices(act_id=act_id)))
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_reservation(self, *, act_id: int, rsv_resid: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Reservations).filter(Reservations.rsv_resid == rsv_resid).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_reservations(self, *, act_id: int) -> list:
        result = []
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(Reservations.rsv_slc_id.in_(slc_id_list)):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_slice_id(self, *, act_id: int, slc_guid: str) -> list:
        result = []
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            if slc_obj is None :
                raise Exception("Slice for {} not found".format(slc_guid))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(Reservations.rsv_slc_id == slc_obj['slc_id']):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_state(self, *, act_id: int, rsv_state: int) -> list:
        result = []
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(
                        Reservations.rsv_state == rsv_state).filter(Reservations.rsv_slc_id.in_(slc_id_list)):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_slice_id_state(self, *, act_id: int, slc_guid: str, rsv_state: int) -> list:
        result = []
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            if slc_obj is None :
                raise Exception("Slice for {} not found".format(slc_guid))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(Reservations.rsv_state == rsv_state).filter(
                        Reservations.rsv_slc_id == slc_obj['slc_id']):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_2_category(self, *, act_id: int, rsv_cat1: int, rsv_cat2: int) -> list:
        result = []
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(
                        Reservations.rsv_category == rsv_cat1 or Reservations.rsv_category == rsv_cat2).filter(
                        Reservations.rsv_slc_id.in_(slc_id_list)):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_category(self, *, act_id: int, rsv_cat: int) -> list:
        result = []
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(
                        Reservations.rsv_category == rsv_cat).filter(
                        Reservations.rsv_slc_id.in_(slc_id_list)):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_slice_id_by_2_category(self, *, act_id: int, slc_guid: str, rsv_cat1: int, rsv_cat2: int) -> list:
        result = []
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            if slc_obj is None :
                raise Exception("Slice for {} not found".format(slc_guid))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(
                        Reservations.rsv_slc_id == slc_obj['slc_id']).filter(
                        Reservations.rsv_category == rsv_cat1 or Reservations.rsv_category == rsv_cat2):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_slice_id_by_category(self, *, act_id: int, slc_guid: str, rsv_cat: int) -> list:
        result = []
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            if slc_obj is None :
                raise Exception("Slice for {} not found".format(slc_guid))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(Reservations.rsv_slc_id == slc_obj['slc_id']).filter(
                        Reservations.rsv_category == rsv_cat):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservation(self, *, act_id: int, rsv_resid: str) -> dict:
        result = None
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                rsv_obj = session.query(Reservations).filter(
                    Reservations.rsv_resid == rsv_resid).filter(Reservations.rsv_slc_id.in_(slc_id_list)).first()
                if rsv_obj is None:
                    raise Exception("Reservation not found")
                result = {'rsv_id': rsv_obj.rsv_id, 'rsv_slc_id': rsv_obj.rsv_slc_id, 'rsv_resid': rsv_obj.rsv_resid,
                          'rsv_category': rsv_obj.rsv_category, 'rsv_state': rsv_obj.rsv_state,
                          'rsv_pending': rsv_obj.rsv_pending, 'properties': rsv_obj.properties}
                if rsv_obj.rsv_joining is not None:
                    result['rsv_joining'] = rsv_obj.rsv_joining
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_reservations_by_rids(self, *, act_id: int, rsv_resid_list: list) -> list:
        result = []
        try:
            slc_id_list = self.get_slice_ids(act_id=act_id)
            if slc_id_list is None or len(slc_id_list) == 0:
                raise Exception("Slice for {} not found".format(act_id))
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(
                        Reservations.rsv_resid.in_(rsv_resid_list)).filter(Reservations.rsv_slc_id.in_(slc_id_list)):
                    rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                               'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                               'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                               'properties': row.properties}
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_proxy(self, *, act_id: int, prx_name: str, properties):
        try:
            prx_obj = Proxies(prx_act_id=act_id, prx_name=prx_name, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(prx_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_proxy(self, *, act_id: int, prx_name: str, properties):
        try:
            with session_scope(self.db_engine) as session:
                prx_obj = session.query(Proxies).filter(Proxies.prx_act_id == act_id).filter(Proxies.prx_name == prx_name).first()
                if prx_obj is not None:
                    prx_obj.properties = properties
                else:
                    raise Exception("Proxy not found")
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_proxy(self, *, act_id: int, prx_name: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Proxies).filter(Proxies.prx_act_id == act_id).filter(Proxies.prx_name == prx_name).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_proxies(self, *, act_id: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Proxies).filter(Proxies.prx_act_id == act_id):
                    prx_obj = {'prx_id': row.prx_id, 'prx_act_id': row.prx_act_id, 'prx_name': row.prx_name,
                               'properties': row.properties}
                    result.append(prx_obj.copy())
                    prx_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_config_mapping(self, *, act_id: int, cfgm_type: str, cfgm_path: str, properties: dict):
        try:
            cfg_obj = ConfigMappings(cfgm_act_id=act_id, cfgm_type=cfgm_type, cfgm_path=cfgm_path,
                                     properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(cfg_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_config_mapping(self, *, act_id: int, cfgm_type: str, cfgm_path: str, properties: dict):
        try:
            with session_scope(self.db_engine) as session:
                cfg_obj = session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id).filter(ConfigMappings.cfgm_type == cfgm_type).first()
                if cfg_obj is not None:
                    cfg_obj.cfgm_path = cfgm_path
                    cfg_obj.properties = properties
                else:
                    raise Exception("Reservation not found")
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_config_mapping(self, *, act_id: int, cfgm_type: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(ConfigMappings).filter(ConfigMappings.cfgm_type == cfgm_type).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_config_mappings(self, *, act_id: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id):
                    cfg_obj = {'cfgm_id': row.cfgm_id, 'cfgm_act_id': row.cfgm_act_id, 'cfgm_type': row.cfgm_type,
                               'cfgm_path':row.cfgm_path, 'properties': row.properties}
                    result.append(cfg_obj.copy())
                    cfg_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_config_mappings_by_type(self, *, act_id: int, cfgm_type: str) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id).filter(ConfigMappings.cfgm_type == cfgm_type):
                    cfg_obj = {'cfgm_id': row.cfgm_id, 'cfgm_act_id': row.cfgm_act_id, 'cfgm_type': row.cfgm_type,
                               'cfgm_path':row.cfgm_path, 'properties': row.properties}
                    result.append(cfg_obj.copy())
                    cfg_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_client(self, *, act_id: int, clt_name: str, clt_guid: str, properties):
        try:
            clt_obj = Clients(clt_act_id=act_id, clt_name=clt_name, clt_guid=clt_guid,
                                     properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(clt_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_client(self, *, act_id: int, clt_name: str, properties):
        try:
            with session_scope(self.db_engine) as session:
                clt_obj = session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_name == clt_name).first()
                if clt_obj is not None:
                    clt_obj.properties = properties
                else:
                    raise Exception("Client not found")
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_client_by_name(self, *, act_id: int, clt_name: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_name == clt_name).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_client_by_guid(self, *, act_id: int, clt_guid: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_guid == clt_guid).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_client_by_name(self, *, act_id: int, clt_name: str) -> dict:
        result = None
        try:
            with session_scope(self.db_engine) as session:
                clt_obj = session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_name == clt_name).first()
                if clt_obj is None:
                    raise Exception("Client with clt_name {} not found".format(clt_name))
                result = {'clt_id': clt_obj.clt_id, 'clt_act_id': clt_obj.clt_act_id, 'clt_name': clt_obj.clt_name,
                          'clt_guid':clt_obj.clt_guid, 'properties': clt_obj.properties}
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_client_by_guid(self, *, act_id: int, clt_guid: str) -> dict:
        result = None
        try:
            with session_scope(self.db_engine) as session:
                clt_obj = session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_guid == clt_guid).first()
                if clt_obj is None:
                    raise Exception("Client with guid {} not found".format(clt_guid))
                result = {'clt_id': clt_obj.clt_id, 'clt_act_id': clt_obj.clt_act_id, 'clt_name': clt_obj.clt_name,
                          'clt_guid':clt_obj.clt_guid, 'properties': clt_obj.properties}
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_clients(self, *, act_id: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Clients).filter(Clients.clt_act_id == act_id):
                    clt_obj = {'clt_id': row.clt_id, 'clt_act_id': row.clt_act_id, 'clt_name': row.clt_name,
                               'clt_guid':row.clt_guid, 'properties': row.properties}
                    result.append(clt_obj.copy())
                    clt_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_unit(self, *, act_id: int, slc_guid: str, rsv_resid: str, unt_uid: str, unt_unt_id: int, unt_type: int,
                 unt_state: int, properties):
        try:
            slc_obj = self.get_slice(act_id=act_id, slice_guid=slc_guid)
            if slc_obj is None:
                raise Exception("Slice {} not found".format(slc_guid))
            rsv_obj = self.get_reservation(act_id=act_id, rsv_resid=rsv_resid)
            if rsv_obj is None:
                raise Exception("Reservation {} not found".format(rsv_resid))
            if slc_obj['slc_id'] != rsv_obj['rsv_slc_id']:
                raise Exception("Inconsistent Database, slice and reservation do not match Slice:{} Reservation:{}".format(slc_obj, rsv_obj))

            unt_obj = Units(unt_uid=unt_uid, unt_act_id=slc_obj['slc_act_id'],
                            unt_slc_id=rsv_obj['rsv_slc_id'],
                            unt_rsv_id=rsv_obj['rsv_id'], unt_type=unt_type,
                            unt_state=unt_state, properties=properties)
            if unt_unt_id is not None:
                unt_obj.unt_unt_id = unt_unt_id
            with session_scope(self.db_engine) as session:
                session.add(unt_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_unit(self, *, act_id: int, unt_uid: str):
        result = None
        try:
            with session_scope(self.db_engine) as session:
                unt_obj = session.query(Units).filter(Units.unt_uid == unt_uid).filter(Units.unt_act_id == act_id).first()
                if unt_obj is None:
                    self.logger.error("Unit with guid {} not found".format(unt_uid))
                    return result
                result = {'unt_id': unt_obj.unt_id, 'unt_uid': unt_obj.unt_uid, 'unt_unt_id': unt_obj.unt_unt_id,
                          'unt_act_id': unt_obj.unt_act_id, 'unt_slc_id': unt_obj.unt_slc_id,
                          'unt_rsv_id':unt_obj.unt_rsv_id, 'unt_type':unt_obj.unt_type,
                          'unt_state':unt_obj.unt_state, 'properties': unt_obj.properties}
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_units(self, *, act_id: int, rsv_resid: str):
        result = []
        try:
            rsv_obj = self.get_reservation(act_id=act_id, rsv_resid=rsv_resid)
            if rsv_obj is None:
                raise Exception("Reservation {} not found".format(rsv_resid))

            with session_scope(self.db_engine) as session:
                for row in session.query(Units).filter(Units.unt_rsv_id == rsv_obj['rsv_id']):
                    unt_obj = {'unt_id': row.unt_id, 'unt_uid': row.unt_uid, 'unt_unt_id': row.unt_unt_id,
                               'unt_act_id': row.unt_act_id, 'unt_slc_id': row.unt_slc_id,
                               'unt_rsv_id': row.unt_rsv_id, 'unt_type': row.unt_type,
                               'unt_state': row.unt_state, 'properties': row.properties}
                    result.append(unt_obj.copy())
                    unt_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def remove_unit(self, *, act_id: int, unt_uid: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Units).filter(Units.unt_uid == unt_uid).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def update_unit(self, *, act_id: int, unt_uid: str, properties):
        result = None
        try:
            with session_scope(self.db_engine) as session:
                unt_obj = session.query(Units).filter(Units.unt_uid == unt_uid).filter(Units.unt_act_id == act_id).first()
                if unt_obj is None:
                    raise Exception("Unit with guid {} not found".format(unt_uid))
                unt_obj.properties = properties
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def add_plugin(self, *, plugin_id: str, plg_type: int, plg_actor_type: int, properties):
        try:
            plg_obj = Plugins(plg_local_id=plugin_id, plg_type=plg_type, plg_actor_type=plg_actor_type,
                              properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(plg_obj)
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def remove_plugin(self, *, plugin_id: str):
        try:
            with session_scope(self.db_engine) as session:
                session.query(Plugins).filter(Plugins.plg_local_id == plugin_id).delete()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e

    def get_plugins(self, *, plg_type: int, plg_actor_type: int) -> list:
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Plugins).filter(Plugins.plg_type == plg_type).filter(Plugins.plg_actor_type == plg_actor_type):
                    plg_obj = {'plg_id': row.plg_id, 'plg_local_id': row.plg_local_id, 'plg_type': row.plg_type,
                               'plg_actor_type': row.plg_actor_type, 'properties': row.properties}
                    result.append(plg_obj.copy())
                    plg_obj.clear()
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result

    def get_plugin(self, *, plugin_id: str) -> dict:
        result = None
        try:
            with session_scope(self.db_engine) as session:
                plg_obj = session.query(Plugins).filter(Plugins.plg_local_id == plugin_id).first()
                if plg_obj is None:
                    raise Exception("Plugin with guid {} not found".format(plugin_id))
                result = {'plg_id': plg_obj.plg_id, 'plg_local_id': plg_obj.plg_local_id, 'plg_type': plg_obj.plg_type,
                          'plg_actor_type': plg_obj.plg_actor_type, 'properties': plg_obj.properties}
        except Exception as e:
            self.logger.error("Exception occurred " + str(e))
            raise e
        return result


def test():
    Logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='am', db_host='127.0.0.1:8432', logger=Logger)
    db.create_db()
    db.reset_db()

    # Actor Operations
    prop = {'abc':'def'}

    db.add_actor(name="test-actor1", guid="12345", act_type=1, properties=pickle.dumps(prop))
    print("All actors: {}".format(db.get_actors()))
    print("one actor {}".format(db.get_actor(name="test-actor1")))
    prop['pqr'] = 'stu'
    db.update_actor(name="test-actor1", properties=pickle.dumps(prop))
    print("actor after update {}".format(db.get_actor(name="test-actor1")))
    db.remove_actor(name="test-actor1")
    print("actors after delete: {}".format(db.get_actors()))

    # Miscellaneous operations
    db.add_miscellaneous(name='time', properties=prop)
    print(db.get_miscellaneous(name='time'))

    # Manager Object operations
    db.add_actor(name="test-actor1", guid="12345", act_type=1, properties=pickle.dumps(prop))
    print("All actors: {}".format(db.get_actors()))
    actor = db.get_actor(name="test-actor1")
    actor_id = actor['act_id']
    db.add_manager_object(manager_key="mgr-1234", properties=prop, act_id=actor_id)
    db.add_manager_object(manager_key="mgr-1235", properties=prop)
    print(db.get_manager_objects())
    print("Get specific MO {}".format(db.get_manager_object(mo_key="mgr-1234")))
    print("Get MO by actor {}".format(db.get_manager_objects(act_id=actor_id)))
    print("Get MO with no actor {}".format(db.get_manager_containers()))
    db.remove_manager_object(manager_key="mgr-1235")
    print("After removing MO by key {}".format(db.get_manager_objects()))
    db.remove_manager_object_by_actor(act_id=actor_id)
    print("After removing MO by actor {}".format(db.get_manager_objects()))

    # Slice operations
    db.add_slice(act_id=actor_id, slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop))
    print("Slices after add slice {}".format(db.get_slices(act_id=actor_id)))
    prop['fabric'] = 'testbed'
    db.update_slice(act_id=actor_id, slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                    properties=pickle.dumps(prop))
    print("Get slice after update {}".format(db.get_slice(act_id=actor_id, slice_guid="1234")))

    print("Get slice by type after update {}".format(db.get_slices_by_type(act_id=actor_id, slc_type=1)))
    print("Get slice by type after update {}".format(db.get_slices_by_resource_type(act_id=actor_id,
                                                                                    slc_resource_type="def")))
    db.remove_slice(act_id=actor_id, slc_guid="1234")
    print("Get all slices {} after delete".format(db.get_slices(act_id=actor_id)))

    # Reservation operations
    db.add_slice(act_id=actor_id, slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop))
    print("Get all slices {} after add".format(db.get_slices(act_id=actor_id)))
    prop['rsv'] = 'test'
    db.add_reservation(act_id=actor_id, slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                       rsv_pending=3, rsv_joining=4, properties=pickle.dumps(prop))
    print("Reservations after add {}".format(db.get_reservations(act_id=actor_id)))
    prop['update'] = 'test'
    db.update_reservation(act_id=actor_id, slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                          rsv_pending=3, rsv_joining=5, properties=pickle.dumps(prop))
    print("Reservations after update {}".format(db.get_reservations(act_id=actor_id)))
    print("Reservations after by state {}".format(db.get_reservations_by_slice_id_state(act_id=actor_id, slc_guid="1234", rsv_state=2)))

    print("Reservations after by rid {}".format(db.get_reservation(act_id=actor_id, rsv_resid="rsv_567")))
    print("Reservations after by rid list {}".format(db.get_reservations_by_rids(act_id=actor_id, rsv_resid_list=["rsv_567"])))

    db.remove_reservation(act_id=actor_id, rsv_resid="rsv_567")
    print("Reservations after delete {}".format(db.get_reservations(act_id=actor_id)))

    # Proxy operations
    db.add_proxy(act_id=actor_id, prx_name="prx-123", properties=pickle.dumps(prop))
    print("Proxies after add {}".format(db.get_proxies(act_id=actor_id)))
    prop['prx-update'] = 'fly'
    db.update_proxy(act_id=actor_id, prx_name="prx-123", properties=pickle.dumps(prop))
    print("Proxies after update {}".format(db.get_proxies(act_id=actor_id)))
    db.remove_proxy(act_id=actor_id, prx_name="prx-123")
    print("Proxies after remove {}".format(db.get_proxies(act_id=actor_id)))

    # Config Mapping operations
    db.add_config_mapping(act_id=actor_id, cfgm_type="cfg-1", cfgm_path="/abc/def", properties=prop)
    print("Config Mappings after add {}".format(db.get_config_mappings(act_id=actor_id)))
    prop['cfg-update'] = 'done'
    db.update_config_mapping(act_id=actor_id, cfgm_type="cfg-1", cfgm_path="/abc/def", properties=prop)
    print("Config Mappings after update by type {}".format(db.get_config_mappings_by_type(act_id=actor_id, cfgm_type="cfg-1")))
    db.remove_config_mapping(act_id=actor_id, cfgm_type="cfg-1")
    print("Config Mappings after remove {}".format(db.get_config_mappings(act_id=actor_id)))

    # Client operations
    db.add_client(act_id=actor_id, clt_name="clt-test", clt_guid="clt-1234", properties=pickle.dumps(prop))
    print("Clients after add {}".format(db.get_clients(act_id=actor_id)))
    print("Clients after by guid {}".format(db.get_client_by_guid(act_id=actor_id, clt_guid="clt-1234")))
    prop['clt-up']='done'
    db.update_client(act_id=actor_id, clt_name="clt-test", properties=pickle.dumps(prop))
    print("Clients after by name {}".format(db.get_client_by_name(act_id=actor_id, clt_name="clt-test")))
    db.remove_client_by_guid(act_id=actor_id, clt_guid="clt-1234")
    print("Clients after remove {}".format(db.get_clients(act_id=actor_id)))

    # Unit operations
    db.add_reservation(act_id=actor_id, slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                       rsv_pending=3, rsv_joining=4, properties=pickle.dumps(prop))

    db.add_unit(act_id=actor_id, slc_guid="1234", rsv_resid="rsv_567", unt_uid="unt_123", unt_unt_id=None, unt_type=1,
                unt_state=2, properties=pickle.dumps(prop))
    print("Unit after add {}".format(db.get_units(act_id=actor_id,  rsv_resid="rsv_567")))
    prop['update_unit'] = 'done'
    db.update_unit(act_id=actor_id, unt_uid="unt_123", properties=pickle.dumps(prop))
    print("Unit after update {}".format(db.get_unit(act_id=actor_id, unt_uid="unt_123")))
    db.remove_unit(act_id=actor_id, unt_uid="unt_123")
    print("Unit after remove {}".format(db.get_units(act_id=actor_id, rsv_resid="rsv_567")))

    db.remove_reservation(act_id=actor_id, rsv_resid="rsv_567")
    db.reset_db()
    print("Get all actors after reset {}".format(db.get_actors()))


def test2():
    Logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='am', db_host='127.0.0.1:8432', logger=Logger)
    print("Get all actors after reset {}".format(db.get_actors()))

if __name__ == '__main__':
    test2()
    test()
