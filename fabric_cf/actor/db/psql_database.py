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
from datetime import datetime, timezone
from typing import List

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import scoped_session, sessionmaker

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.db import Base, Clients, ConfigMappings, Proxies, Units, Reservations, Slices, ManagerObjects, \
    Miscellaneous, Actors, Delegations, Sites


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
    """
    Implements interface to Postgres database
    """
    OBJECT_NOT_FOUND = "{} Not Found {}"

    def __init__(self, *, user: str, password: str, database: str, db_host: str, logger):
        # Connecting to PostgreSQL server at localhost using psycopg2 DBAPI
        self.db_engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(user, password, db_host, database))
        self.logger = logger

    def create_db(self):
        """
        Create the database
        """
        Base.metadata.create_all(self.db_engine)

    def set_logger(self, logger):
        """
        Set the logger
        """
        self.logger = logger

    def reset_db(self):
        """
        Reset the database
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).delete()
                session.query(ConfigMappings).delete()
                session.query(Proxies).delete()
                session.query(Units).delete()
                session.query(Delegations).delete()
                session.query(Reservations).delete()
                session.query(Slices).delete()
                session.query(ManagerObjects).delete()
                session.query(Miscellaneous).delete()
                session.query(Actors).delete()

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def add_actor(self, *, name: str, guid: str, act_type: int, properties):
        """
        Add an actor
        @param name name
        @param guid guid
        @param act_type actor type
        @param properties pickle dump for actor instance
        """
        try:
            # Save the actor in the database
            actor_obj = Actors(act_name=name, act_guid=guid, act_type=act_type, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(actor_obj)

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_actor(self, *, name: str, properties):
        """
        Update an actor
        @param name name
        @param properties pickle dump for actor instance
        """
        try:
            with session_scope(self.db_engine) as session:
                actor = session.query(Actors).filter_by(act_name=name).first()
                if actor is not None:
                    actor.properties = properties

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_actor(self, *, name: str):
        """
        Remove an actor
        @param name name
        """
        try:
            # Delete the actor in the database
            with session_scope(self.db_engine) as session:
                session.query(Actors).filter(Actors.act_name == name).delete()

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_actors(self) -> list:
        """
        Get all actors
        @return list of actors
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_actors_by_name_and_type(self, *, actor_name: str, act_type: int) -> list:
        """
        Get actors by name and  actor type
        @param actor_name actor name
        @param act_type actor type
        @return list of actors
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).filter(Actors.act_type == act_type).filter(
                        Actors.act_name.like(actor_name)).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_actors_by_name(self, *, act_name: str) -> list:
        """
        Get actors by name
        @param act_name actor name
        @return list of actors
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Actors).filter(Actors.act_name.like(act_name)).all():
                    actor = {'act_guid': row.act_guid, 'act_name': row.act_name, 'act_type': row.act_type,
                             'properties': row.properties, 'act_id': row.act_id}
                    result.append(actor.copy())
                    actor.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_actor(self, *, name: str) -> dict:
        """
        Get actor by name
        @return actor
        """
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
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_miscellaneous(self, *, name: str, properties: dict):
        """
        Add Miscellaneous entries
        @param name name
        @param properties properties

        """
        try:
            msc_obj = Miscellaneous(msc_path=name, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(msc_obj)

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_miscellaneous(self, *, name: str, properties: dict):
        """
        Add Miscellaneous entries
        @param name name
        @param properties properties

        """
        try:
            with session_scope(self.db_engine) as session:
                msc_obj = session.query(Miscellaneous).filter(Miscellaneous.msc_path == name).first()
                if msc_obj is not None:
                    msc_obj.properties = properties
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_miscellaneous(self, *, name: str):
        """
        Remove Miscellaneous entries
        @param name name
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Miscellaneous).filter(Miscellaneous.msc_path == name).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_miscellaneous(self, *, name: str) -> dict or None:
        """
        Get Miscellaneous entry
        @param name name
        @return entry identified by name
        """
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                msc_obj = session.query(Miscellaneous).filter_by(msc_path=name).first()
                if msc_obj is not None:
                    result = msc_obj.properties
                else:
                    return None
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_manager_object(self, *, manager_key: str, properties: dict, act_id: int = None):
        """
        Add mananger object
        @param manager_key management object key
        @param properties properties
        @param act_id actor id
        """
        try:
            if act_id is not None:
                mng_obj = ManagerObjects(mo_key=manager_key, mo_act_id=act_id, properties=properties)
            else:
                mng_obj = ManagerObjects(mo_key=manager_key, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(mng_obj)

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_manager_object(self, *, manager_key: str):
        """
        Remove management object
        @param manager_key management object key
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(ManagerObjects).filter(ManagerObjects.mo_key == manager_key).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_manager_object_by_actor(self, *, act_id: int):
        """
        Remove management object by actor id
        @param act_id actor id
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(ManagerObjects).filter(ManagerObjects.mo_act_id == act_id).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_manager_objects(self, *, act_id: int = None) -> list:
        """
        Get Management objects
        @param act_id actor id
        @return list of objects
        """
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
                    for row in session.query(ManagerObjects).filter(ManagerObjects.mo_act_id == act_id).all():
                        mo = {'mo_act_id': row.mo_act_id, 'mo_key': row.mo_key, 'properties': row.properties,
                              'mo_id': row.mo_id}
                        result.append(mo.copy())
                        mo.clear()

        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_manager_objects_by_actor_name(self, *, act_name: str = None) -> list:
        """
        Get Management objects
        @param act_name actor name
        @return list of objects
        """
        result = []
        try:
            act_obj = self.get_actor(name=act_name)
            act_id = act_obj['act_id']
            result = self.get_manager_objects(act_id=act_id)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_manager_object(self, *, mo_key: str) -> dict:
        """
        Get Management object by key
        @param mo_key management object key
        @return objects
        """
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
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Manager Object", mo_key))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_manager_containers(self) -> list:
        """
        Get Management object for the container i.e entry with no actor id
        @return object
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for mo_obj in session.query(ManagerObjects).filter(ManagerObjects.mo_act_id.is_(None)).all():
                    mo = {'mo_id': mo_obj.mo_id, 'mo_key': mo_obj.mo_key, 'properties': mo_obj.properties}
                    result.append(mo.copy())
                    mo.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_slice(self, *, slc_guid: str, slc_name: str, slc_type: int, slc_state: int, lease_start: datetime = None,
                  lease_end: datetime = None, slc_resource_type: str, properties, slc_graph_id: str = None,
                  oidc_claim_sub: str = None, email: str = None, project_id: str = None):
        """
        Add a slice
        @param slc_guid slice id
        @param slc_name slice name
        @param slc_type slice type
        @param slc_state slice state
        @param slc_resource_type slice resource type
        @param lease_start Lease Start time
        @param lease_end Lease End time
        @param properties pickled instance
        @param slc_graph_id slice graph id
        @param oidc_claim_sub User OIDC Sub
        @param email User Email
        @param project_id Project Id
        """
        try:
            slc_obj = Slices(slc_guid=slc_guid, slc_name=slc_name, slc_type=slc_type, slc_state=slc_state,
                             oidc_claim_sub=oidc_claim_sub, email=email, slc_resource_type=slc_resource_type,
                             lease_start=lease_start, lease_end=lease_end, properties=properties,
                             project_id=project_id)
            if slc_graph_id is not None:
                slc_obj.slc_graph_id = slc_graph_id
            with session_scope(self.db_engine) as session:
                session.add(slc_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_slice(self, *, slc_guid: str, slc_name: str, slc_type: int, slc_state: int, slc_resource_type: str,
                     properties, slc_graph_id: str = None, lease_start: datetime = None, lease_end: datetime = None):
        """
        Update a slice
        @param slc_guid slice id
        @param slc_name slice name
        @param slc_type slice type
        @param slc_state slice state
        @param slc_resource_type slice resource type
        @param lease_start Lease Start time
        @param lease_end Lease End time
        @param properties pickled instance
        @param slc_graph_id slice graph id
        """
        try:
            with session_scope(self.db_engine) as session:
                slc_obj = session.query(Slices).filter(Slices.slc_guid == slc_guid).first()
                if slc_obj is not None:
                    slc_obj.properties = properties
                    slc_obj.slc_name = slc_name
                    slc_obj.slc_type = slc_type
                    slc_obj.slc_resource_type = slc_resource_type
                    slc_obj.slc_state = slc_state
                    slc_obj.lease_start = lease_start
                    slc_obj.lease_end = lease_end
                    if slc_graph_id is not None:
                        slc_obj.slc_graph_id = slc_graph_id
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Slice", slc_guid))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_slice(self, *, slc_guid: str):
        """
        Remove Slice
        @param slc_guid slice id
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Slices).filter(Slices.slc_guid == slc_guid).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_slice_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a slice row read from database
        """
        if row is None:
            return None

        slice_obj = {'slc_id': row.slc_id, 'slc_guid': row.slc_guid, 'slc_name': row.slc_name,
                     'slc_type': row.slc_type, 'slc_resource_type': row.slc_resource_type, 'slc_state': row.slc_state,
                     'project_id': row.project_id, 'lease_start': row.lease_start, 'lease_end': row.lease_end,
                     'properties': row.properties}
        if row.slc_graph_id is not None:
            slice_obj['slc_graph_id'] = row.slc_graph_id

        return slice_obj

    def get_slice_ids(self) -> list:
        """
        Get slice ids for an actor
        @param act_id actor id
        @return list of slice ids
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Slices).all():
                    result.append(row.slc_id)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    @staticmethod
    def create_slices_filter(*, slice_id: str = None, slice_name: str = None, project_id: str = None, email: str = None,
                             oidc_sub: str = None) -> dict:

        filter_dict = {}
        if slice_id is not None:
            filter_dict['slc_guid'] = slice_id
        if slice_name is not None:
            filter_dict['slc_name'] = slice_name
        if project_id is not None:
            filter_dict['project_id'] = project_id
        if email is not None:
            filter_dict['email'] = email
        if oidc_sub is not None:
            filter_dict['oidc_claim_sub'] = oidc_sub
        return filter_dict

    def get_slices(self, *, slice_id: str = None, slice_name: str = None, project_id: str = None, email: str = None,
                   states: list[int] = None, oidc_sub: str = None, slc_type: list[int] = None, limit: int = None,
                   offset: int = None, lease_end: datetime = None) -> list:
        """
        Get slices for an actor
        @param slice_id actor id
        @param slice_name slice name
        @param project_id project id
        @param email email
        @param states states
        @param oidc_sub oidc claim sub
        @param slc_type slice type
        @param limit limit
        @param offset offset
        @param lease_end lease_end
        @return list of slices
        """
        result = []
        try:
            filter_dict = self.create_slices_filter(slice_id=slice_id, slice_name=slice_name, project_id=project_id,
                                                    email=email, oidc_sub=oidc_sub)
            with session_scope(self.db_engine) as session:
                rows = session.query(Slices).filter_by(**filter_dict)

                if lease_end is not None:
                    rows = rows.filter(Slices.lease_end < lease_end)

                rows = rows.order_by(desc(Slices.lease_end))

                if states is not None:
                    rows = rows.filter(Slices.slc_state.in_(states))

                if slc_type is not None:
                    rows = rows.filter(Slices.slc_type.in_(slc_type))

                if offset is not None and limit is not None:
                    rows = rows.offset(offset).limit(limit)

                for row in rows.all():
                    slice_obj = self.generate_slice_dict_from_row(row)
                    result.append(slice_obj.copy())
                    slice_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_slice_by_id(self, *, slc_id: int) -> dict:
        """
        Get slice by id for an actor
        @param slc_id slice id
        @return slice dictionary
        """
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                slc_obj = session.query(Slices).filter(Slices.slc_id == slc_id).first()
                if slc_obj is not None:
                    result = self.generate_slice_dict_from_row(slc_obj)
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Slice", slc_id))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_reservation(self, *, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                        rsv_pending: int, rsv_joining: int, properties, lease_start: datetime = None,
                        lease_end: datetime = None, rsv_graph_node_id: str = None, oidc_claim_sub: str = None,
                        email: str = None, project_id: str = None, site: str = None, rsv_type: str = None):
        """
        Add a reservation
        @param slc_guid slice guid
        @param rsv_resid reservation guid
        @param rsv_category category
        @param rsv_state state
        @param rsv_pending pending state
        @param rsv_joining join state
        @param properties pickled instance
        @param lease_start Lease start time
        @param lease_end Lease end time
        @param rsv_graph_node_id graph id
        @param oidc_claim_sub OIDC Sub claim
        @param email User email
        @param project_id Project id
        @param site site
        @param rsv_type reservation type
        """
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            rsv_obj = Reservations(rsv_slc_id=slc_id, rsv_resid=rsv_resid, rsv_category=rsv_category,
                                   rsv_state=rsv_state, rsv_pending=rsv_pending, rsv_joining=rsv_joining,
                                   lease_start=lease_start, lease_end=lease_end,
                                   properties=properties, oidc_claim_sub=oidc_claim_sub, email=email,
                                   project_id=project_id, site=site, rsv_type=rsv_type)
            if rsv_graph_node_id is not None:
                rsv_obj.rsv_graph_node_id = rsv_graph_node_id
            with session_scope(self.db_engine) as session:
                session.add(rsv_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_reservation(self, *, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                           rsv_pending: int, rsv_joining: int, properties, lease_start: datetime = None,
                           lease_end: datetime = None, rsv_graph_node_id: str = None, site: str = None,
                           rsv_type: str = None):
        """
        Update a reservation
        @param slc_guid slice guid
        @param rsv_resid reservation guid
        @param rsv_category category
        @param rsv_state state
        @param rsv_pending pending state
        @param rsv_joining join state
        @param properties pickled instance
        @param lease_start Lease start time
        @param lease_end Lease end time
        @param rsv_graph_node_id graph id
        @param site site
        @param rsv_type reservation type
        """
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            with session_scope(self.db_engine) as session:
                rsv_obj = session.query(Reservations).filter(Reservations.rsv_slc_id == slc_id).filter(
                    Reservations.rsv_resid == rsv_resid).first()
                if rsv_obj is not None:
                    rsv_obj.rsv_category = rsv_category
                    rsv_obj.rsv_state = rsv_state
                    rsv_obj.rsv_pending = rsv_pending
                    rsv_obj.rsv_joining = rsv_joining
                    rsv_obj.properties = properties
                    rsv_obj.lease_end = lease_end
                    rsv_obj.lease_start = lease_start
                    if site is not None:
                        rsv_obj.site = site
                    if rsv_graph_node_id is not None:
                        rsv_obj.rsv_graph_node_id = rsv_graph_node_id
                    if rsv_type is not None:
                        rsv_obj.rsv_type = rsv_type
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Reservation", rsv_resid))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_reservation(self, *, rsv_resid: str):
        """
        Remove a reservation
        @param rsv_resid reservation guid
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Reservations).filter(Reservations.rsv_resid == rsv_resid).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_reservation_dict_from_row(row) -> dict:
        """
        Generate a dictionary representing a reservation row read from database
        @param row row
        """
        if row is None:
            return None
        rsv_obj = {'rsv_id': row.rsv_id, 'rsv_slc_id': row.rsv_slc_id, 'rsv_resid': row.rsv_resid,
                   'rsv_category': row.rsv_category, 'rsv_state': row.rsv_state,
                   'rsv_pending': row.rsv_pending, 'rsv_joining': row.rsv_joining,
                   'properties': row.properties, 'rsv_graph_node_id': row.rsv_graph_node_id}

        return rsv_obj

    def create_reservation_filter(self, *, slice_id: str = None, graph_node_id: str = None, project_id: str = None,
                                  email: str = None, oidc_sub: str = None, rid: str = None, site: str = None,
                                  rsv_type: str = None) -> dict:

        filter_dict = {}
        if slice_id is not None:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slice_id)
            filter_dict['rsv_slc_id'] = slc_id
        if graph_node_id is not None:
            filter_dict['rsv_graph_node_id'] = graph_node_id
        if project_id is not None:
            filter_dict['project_id'] = project_id
        if email is not None:
            filter_dict['email'] = email
        if oidc_sub is not None:
            filter_dict['oidc_claim_sub'] = oidc_sub
        if rid is not None:
            filter_dict['rsv_resid'] = rid
        if site is not None:
            filter_dict['site'] = site
        if rsv_type is not None:
            filter_dict['rsv_type'] = rsv_type
        return filter_dict

    def get_reservations(self, *, slice_id: str = None, graph_node_id: str = None, project_id: str = None,
                         email: str = None, oidc_sub: str = None, rid: str = None, states: list[int] = None,
                         category: list[int] = None, site: str = None, rsv_type: str = None) -> list:
        """
        Get Reservations for an actor
        @param slice_id slice id
        @param graph_node_id graph node id
        @param project_id project id
        @param email email
        @param oidc_sub oidc sub
        @param rid reservation id
        @param states reservation state
        @param category reservation category
        @param site site name
        @param rsv_type rsv_type

        @return list of reservations
        """
        result = []
        try:
            filter_dict = self.create_reservation_filter(slice_id=slice_id, graph_node_id=graph_node_id,
                                                         project_id=project_id, email=email, oidc_sub=oidc_sub,
                                                         rid=rid, site=site, rsv_type=rsv_type)
            with session_scope(self.db_engine) as session:
                rows = session.query(Reservations).filter_by(**filter_dict)

                if states is not None:
                    rows = rows.filter(Reservations.rsv_state.in_(states))

                if category is not None:
                    rows = rows.filter(Reservations.rsv_category.in_(category))

                for row in rows.all():
                    rsv_obj = self.generate_reservation_dict_from_row(row)
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_reservations_by_rids(self, *, rsv_resid_list: list) -> list:
        """
        Get Reservations for an actor by reservation ids
        @param act_id actor id
        @param rsv_resid_list reservation guid list
        @return list of reservations
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Reservations).filter(Reservations.rsv_resid.in_(rsv_resid_list)).all():
                    rsv_obj = self.generate_reservation_dict_from_row(row)
                    result.append(rsv_obj.copy())
                    rsv_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_proxy(self, *, act_id: int, prx_name: str, properties):
        """
        Add a proxy
        @param act_id actor id
        @param prx_name proxy name
        @param properties pickled instance
        """
        try:
            prx_obj = Proxies(prx_act_id=act_id, prx_name=prx_name, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(prx_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_proxy(self, *, act_id: int, prx_name: str, properties):
        """
        Update a proxy
        @param act_id actor id
        @param prx_name proxy name
        @param properties pickled instance
        """
        try:
            with session_scope(self.db_engine) as session:
                prx_obj = session.query(Proxies).filter(Proxies.prx_act_id == act_id).filter(
                    Proxies.prx_name == prx_name).first()
                if prx_obj is not None:
                    prx_obj.properties = properties
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Proxy", prx_name))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_proxy(self, *, act_id: int, prx_name: str):
        """
        Remove a proxy
        @param act_id actor id
        @param prx_name proxy name
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Proxies).filter(Proxies.prx_act_id == act_id).filter(
                    Proxies.prx_name == prx_name).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_proxy_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a proxy row read from database
        """
        if row is None:
            return None

        prx_obj = {'prx_id': row.prx_id, 'prx_act_id': row.prx_act_id, 'prx_name': row.prx_name,
                   'properties': row.properties}

        return prx_obj

    def get_proxies(self, *, act_id: int) -> list:
        """
        Get Proxies
        @param act_id actor id
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Proxies).filter(Proxies.prx_act_id == act_id).all():
                    prx_obj = self.generate_proxy_dict_from_row(row)
                    result.append(prx_obj.copy())
                    prx_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_config_mapping(self, *, act_id: int, cfgm_type: str, properties):
        """
        Add handlers mapping
        @param act_id actor id
        @param cfgm_type type
        @param properties properties
        """
        try:
            cfg_obj = ConfigMappings(cfgm_act_id=act_id, cfgm_type=cfgm_type, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(cfg_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_config_mapping(self, *, act_id: int, cfgm_type: str, properties):
        """
        Update handlers mapping
        @param act_id actor id
        @param cfgm_type type
        @param properties properties
        """
        try:
            with session_scope(self.db_engine) as session:
                cfg_obj = session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id).filter(
                    ConfigMappings.cfgm_type == cfgm_type).first()
                if cfg_obj is not None:
                    cfg_obj.properties = properties
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Config Mapping", cfgm_type))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_config_mapping(self, *, cfgm_type: str):
        """
        Remove handlers mapping
        @param cfgm_type handlers mapping type
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(ConfigMappings).filter(ConfigMappings.cfgm_type == cfgm_type).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_config_mapping_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a handlers mapping row read from database
        """
        if row is None:
            return None

        cfg_obj = {'cfgm_id': row.cfgm_id, 'cfgm_act_id': row.cfgm_act_id, 'cfgm_type': row.cfgm_type,
                   'properties': row.properties}

        return cfg_obj

    def get_config_mappings(self, *, act_id: int) -> list:
        """
        Get Config Mappings
        @param act_id actor id
        @retur list of mappings
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id).all():
                    cfg_obj = self.generate_config_mapping_dict_from_row(row)
                    result.append(cfg_obj.copy())
                    cfg_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_client(self, *, act_id: int, clt_name: str, clt_guid: str, properties):
        """
        Add a client
        @param act_id actor id
        @param clt_name client name
        @param clt_guid client guid
        @param properties pickled instance
        """
        try:
            clt_obj = Clients(clt_act_id=act_id, clt_name=clt_name, clt_guid=clt_guid, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(clt_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_client(self, *, act_id: int, clt_name: str, properties):
        """
        Update a client
        @param act_id actor id
        @param clt_name client name
        @param properties pickled instance
        """
        try:
            with session_scope(self.db_engine) as session:
                clt_obj = session.query(Clients).filter(Clients.clt_act_id == act_id).filter(
                    Clients.clt_name == clt_name).first()
                if clt_obj is not None:
                    clt_obj.properties = properties
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Client", clt_name))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_client_by_name(self, *, act_id: int, clt_name: str):
        """
        Remove a client by name
        @param act_id actor id
        @param clt_name client name
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).filter(Clients.clt_act_id == act_id).filter(
                    Clients.clt_name == clt_name).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_client_by_guid(self, *, act_id: int, clt_guid: str):
        """
        Remove client by guid
        @param act_id actor id
        @param clt_guid client guid
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Clients).filter(Clients.clt_act_id == act_id).filter(
                    Clients.clt_guid == clt_guid).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_client_dict_from_row(clt_obj) -> dict or None:
        """
        Generate dictionary representing a client row read from database
        """
        if clt_obj is None:
            return None

        result = {'clt_id': clt_obj.clt_id, 'clt_act_id': clt_obj.clt_act_id, 'clt_name': clt_obj.clt_name,
                  'clt_guid': clt_obj.clt_guid, 'properties': clt_obj.properties}

        return result

    def get_client_by_guid(self, *, clt_guid: str) -> dict:
        """
        Get Client by name
        @param act_id actor id
        @param clt_guid client guid
        @return client
        """
        result = None
        try:
            with session_scope(self.db_engine) as session:
                clt_obj = session.query(Clients).filter(Clients.clt_guid == clt_guid).first()
                if clt_obj is None:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Client", clt_guid))
                result = self.generate_client_dict_from_row(clt_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_clients(self) -> list:
        """
        Get Clients
        @param act_id actor id
        @return client list
        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Clients).all():
                    clt_obj = self.generate_client_dict_from_row(row)
                    result.append(clt_obj.copy())
                    clt_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_unit(self, *, slc_guid: str, rsv_resid: str, unt_uid: str, unt_unt_id: int,
                 unt_state: int, properties):
        """
        Add Unit
        @param act_id actor id
        @param slc_guid slice guid
        @param rsv_resid reservation id
        @param unt_uid unit id
        @param unt_unt_id parent unit id
        @param unt_state unit state
        @param properties properties
        """
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            rsv_id = self.get_rsv_id_by_reservation_id(reservation_id=rsv_resid)

            unt_obj = Units(unt_uid=unt_uid,
                            unt_slc_id=slc_id,
                            unt_rsv_id=rsv_id,
                            unt_state=unt_state, properties=properties)
            if unt_unt_id is not None:
                unt_obj.unt_unt_id = unt_unt_id
            with session_scope(self.db_engine) as session:
                session.add(unt_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_unit_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a unit row read from database
        """
        if row is None:
            return None

        result = {'unt_id': row.unt_id, 'unt_uid': row.unt_uid, 'unt_unt_id': row.unt_unt_id,
                  'unt_slc_id': row.unt_slc_id, 'unt_rsv_id': row.unt_rsv_id, 'unt_state': row.unt_state,
                  'properties': row.properties}

        return result

    def get_unit(self, *, unt_uid: str) -> dict or None:
        """
        Get Unit
        @param act_id actor id
        @param unt_uid unit guid
        @return Unit dict
        """
        result = None
        try:
            with session_scope(self.db_engine) as session:
                unt_obj = session.query(Units).filter(Units.unt_uid == unt_uid).first()
                if unt_obj is None:
                    self.logger.error("Unit with guid {} not found".format(unt_uid))
                    return result
                result = self.generate_unit_dict_from_row(unt_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_units(self, *, rsv_resid: str):
        """
        Get Units
        @param act_id actor id
        @param rsv_resid reservation guid
        @return Unit list
        """
        result = []
        try:
            rsv_obj = self.get_reservations(rid=rsv_resid)[0]
            if rsv_obj is None:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Reservation", rsv_resid))

            with session_scope(self.db_engine) as session:
                for row in session.query(Units).filter(Units.unt_rsv_id == rsv_obj['rsv_id']).all():
                    unt_obj = self.generate_unit_dict_from_row(row)
                    result.append(unt_obj.copy())
                    unt_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def remove_unit(self, *, unt_uid: str):
        """
        Remove a unit
        @param unt_uid unit id
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Units).filter(Units.unt_uid == unt_uid).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_unit(self, *, unt_uid: str, properties):
        """
        Add Unit
        @param unt_uid unit id
        @param properties properties
        """
        result = None
        try:
            with session_scope(self.db_engine) as session:
                unt_obj = session.query(Units).filter(Units.unt_uid == unt_uid).first()
                if unt_obj is None:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Unit", unt_uid))
                unt_obj.properties = properties
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_delegation(self, *, slice_id: str, dlg_graph_id: str, dlg_state: int, properties, site: str = None):
        """
        Add delegation
        @param slice_id slice id
        @param dlg_graph_id graph id
        @param dlg_state state
        @param properties properties
        @param site site
        """
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slice_id)
            dlg_obj = Delegations(dlg_slc_id=slc_id, dlg_graph_id=dlg_graph_id,
                                  dlg_state=dlg_state, properties=properties, site=site)
            with session_scope(self.db_engine) as session:
                session.add(dlg_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_delegation(self, *, dlg_graph_id: str, dlg_state: int, properties, site: str = None):
        """
        Update delegation
        @param dlg_graph_id graph id
        @param dlg_state state
        @param properties properties
        """
        try:
            with session_scope(self.db_engine) as session:
                dlg_obj = session.query(Delegations).filter(Delegations.dlg_graph_id == dlg_graph_id).first()
                if dlg_obj is not None:
                    dlg_obj.dlg_state = dlg_state
                    dlg_obj.properties = properties
                    if site is not None:
                        dlg_obj.site = site
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Delegation", dlg_graph_id))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_delegation(self, *, dlg_graph_id: str):
        """
        Remove delegation
        @param dlg_graph_id graph id
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Delegations).filter(Delegations.dlg_graph_id == dlg_graph_id).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_delegation_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a delegation row read from database
        """
        if row is None:
            return None

        dlg_obj = {'dlg_id': row.dlg_id, 'dlg_slc_id': row.dlg_slc_id,
                   'dlg_graph_id':row.dlg_graph_id, 'dlg_state': row.dlg_state, 'properties': row.properties}

        return dlg_obj

    def get_slc_id_by_slice_id(self, *, slice_id: str) -> int:
        slices = self.get_slices(slice_id=slice_id)
        if slices is None or len(slices) == 0:
            raise DatabaseException(self.OBJECT_NOT_FOUND.format("Slice", slice_id))
        return slices[0]['slc_id']

    def get_rsv_id_by_reservation_id(self, *, reservation_id: str) -> int:
        reservations = self.get_reservations(rid=reservation_id)
        if reservations is None or len(reservations) == 0:
            raise DatabaseException(self.OBJECT_NOT_FOUND.format("Reservation", reservation_id))
        return reservations[0]['rsv_id']

    def get_delegations(self, *, slc_guid: str = None, states: List[int] = None) -> list:
        """
        Get delegations
        @param slc_guid slice guid
        @param states delegation state
        @param list of delegations
        """
        result = []
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            with session_scope(self.db_engine) as session:
                rows = session.query(Delegations)
                if slc_guid is not None:
                    rows = rows.filter(Delegations.dlg_slc_id == slc_id)
                if states is not None:
                    rows = rows.filter(Delegations.dlg_state.in_(states))
                for row in rows.all():
                    dlg_obj = self.generate_delegation_dict_from_row(row)
                    result.append(dlg_obj.copy())
                    dlg_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_delegation(self, *, dlg_graph_id: str) -> dict:
        """
        Get delegation
        @param dlg_graph_id delegation graph id
        @return delegation
        """
        result = {}
        try:
            with session_scope(self.db_engine) as session:
                dlg_obj = session.query(Delegations).filter(Delegations.dlg_graph_id == dlg_graph_id).first()
                if dlg_obj is not None:
                    result = self.generate_delegation_dict_from_row(dlg_obj)
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Delegation", dlg_graph_id))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_site(self, *, site_name: str, state: int, properties):
        """
        Add a site
        @param site_name Site Name
        @param state state
        @param properties pickled instance
        """
        try:
            site_obj = Sites(name=site_name, state=state, properties=properties)
            with session_scope(self.db_engine) as session:
                session.add(site_obj)
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_site(self, *, site_name: str, state: int, properties):
        """
        Update a site
        @param site_name Site Name
        @param state state
        @param properties pickled instance
        """
        try:
            with session_scope(self.db_engine) as session:
                site_obj = session.query(Sites).filter(Sites.name == site_name).first()
                if site_obj is not None:
                    site_obj.properties = properties
                    site_obj.state = state
                else:
                    raise DatabaseException(self.OBJECT_NOT_FOUND.format("Sites", site_name))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_site(self, *, site_name: str):
        """
        Remove a proxy
        @param site_name site_name
        """
        try:
            with session_scope(self.db_engine) as session:
                session.query(Sites).filter(Sites.name == site_name).delete()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def generate_site_dict_from_row(row) -> dict:
        """
        Generate dictionary representing a site row read from database
        """
        if row is None:
            return None

        site_obj = {'name': row.name, 'properties': row.properties}

        return site_obj

    def get_sites(self) -> list:
        """
        Get Sites
=        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Sites).all():
                    site_obj = self.generate_site_dict_from_row(row)
                    result.append(site_obj.copy())
                    site_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_site(self, *, site_name: str) -> list:
        """
        Get Sites
=        """
        result = []
        try:
            with session_scope(self.db_engine) as session:
                for row in session.query(Sites).filter(Sites.name == site_name).all():
                    site_obj = self.generate_site_dict_from_row(row)
                    result.append(site_obj.copy())
                    site_obj.clear()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result


def test():
    logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='am', db_host='127.0.0.1:8432', logger=logger)
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
    db.add_slice(slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc),
                 lease_end=datetime.now(timezone.utc), project_id="12345", slc_state=1)
    print("Slices after add slice {}".format(db.get_slices()))
    prop['fabric'] = 'testbed'
    db.update_slice(slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                    properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc),
                    lease_end=datetime.now(timezone.utc), slc_state=1)
    print("Get slice after update {}".format(db.get_slices(slice_id="1234")))

    print("Get slice by type after update {}".format(db.get_slices(slc_type=[1])))

    db.remove_slice(slc_guid="1234")
    print("Get all slices {} after delete".format(db.get_slices()))

    # Reservation operations
    db.add_slice(slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc),
                 lease_end=datetime.now(timezone.utc), slc_state=1)
    print("Get all slices {} after add".format(db.get_slices()))
    prop['rsv'] = 'test'
    db.add_reservation(slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                       rsv_pending=3, rsv_joining=4, properties=pickle.dumps(prop))
    print("Reservations after add {}".format(db.get_reservations()))
    prop['update'] = 'test'
    db.update_reservation(slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                          rsv_pending=3, rsv_joining=5, properties=pickle.dumps(prop))
    print("Reservations after update {}".format(db.get_reservations()))
    print("Reservations after by state {}".format(db.get_reservations(slice_id="1234", state=[2])))

    print("Reservations after by rid {}".format(db.get_reservations(rid="rsv_567")))
    print("Reservations after by rid list {}".format(db.get_reservations_by_rids(rsv_resid_list=["rsv_567"])))

    db.remove_reservation(rsv_resid="rsv_567")
    print("Reservations after delete {}".format(db.get_reservations()))

    # Proxy operations
    db.add_proxy(act_id=actor_id, prx_name="prx-123", properties=pickle.dumps(prop))
    print("Proxies after add {}".format(db.get_proxies(act_id=actor_id)))
    prop['prx-update'] = 'fly'
    db.update_proxy(act_id=actor_id, prx_name="prx-123", properties=pickle.dumps(prop))
    print("Proxies after update {}".format(db.get_proxies(act_id=actor_id)))
    db.remove_proxy(act_id=actor_id, prx_name="prx-123")
    print("Proxies after remove {}".format(db.get_proxies(act_id=actor_id)))

    # Config Mapping operations
    db.add_config_mapping(act_id=actor_id, cfgm_type="cfg-1", properties=prop)
    print("Config Mappings after add {}".format(db.get_config_mappings(act_id=actor_id)))
    prop['cfg-update'] = 'done'
    db.update_config_mapping(act_id=actor_id, cfgm_type="cfg-1", properties=prop)
    db.remove_config_mapping(cfgm_type="cfg-1")
    print("Config Mappings after remove {}".format(db.get_config_mappings(act_id=actor_id)))

    # Client operations
    db.add_client(act_id=actor_id, clt_name="clt-test", clt_guid="clt-1234", properties=pickle.dumps(prop))
    print("Clients after add {}".format(db.get_clients()))
    print("Clients after by guid {}".format(db.get_client_by_guid(clt_guid="clt-1234")))
    prop['clt-up'] = 'done'
    db.update_client(act_id=actor_id, clt_name="clt-test", properties=pickle.dumps(prop))
    db.remove_client_by_guid(act_id=actor_id, clt_guid="clt-1234")
    print("Clients after remove {}".format(db.get_clients()))

    # Unit operations
    db.add_reservation(slc_guid="1234", rsv_resid="rsv_567", rsv_category=1, rsv_state=2,
                       rsv_pending=3, rsv_joining=4, properties=pickle.dumps(prop))

    db.add_unit(slc_guid="1234", rsv_resid="rsv_567", unt_uid="unt_123", unt_unt_id=None,
                unt_state=2, properties=pickle.dumps(prop))
    print("Unit after add {}".format(db.get_units(rsv_resid="rsv_567")))
    prop['update_unit'] = 'done'
    db.update_unit(unt_uid="unt_123", properties=pickle.dumps(prop))
    print("Unit after update {}".format(db.get_unit(unt_uid="unt_123")))
    db.remove_unit(unt_uid="unt_123")
    print("Unit after remove {}".format(db.get_units(rsv_resid="rsv_567")))

    db.remove_reservation(rsv_resid="rsv_567")
    db.reset_db()
    print("Get all actors after reset {}".format(db.get_actors()))


def test2():
    logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='broker', db_host='152.54.15.56:5432', logger=logger)
    from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
    states = [ReservationStates.Active.value,
              ReservationStates.ActiveTicketed.value,
              ReservationStates.Ticketed.value,
              ReservationStates.Nascent.value]
    res = db.get_reservations(graph_node_id='node+renc-data-sw:ip+192.168.11.3-ns', state=states)
    print(f"All {len(res)}")
    for r in res:
        print(r['rsv_state'])
    print("Get all actors after reset {}".format(db.get_actors()))

def test3():
    logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='am', db_host='127.0.0.1:5432', logger=logger)
    db.create_db()
    db.reset_db()

    # Actor Operations
    prop = {'abc': 'def'}

    # Slice operations
    from fabric_cf.actor.core.kernel.slice_state_machine import SliceState
    db.add_slice(slc_guid="1234", slc_name="test-slice", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc), lease_end=datetime.now(timezone.utc),
                 slc_state=SliceState.Closing.value, email="kthare10@email.unc.edu")

    db.add_slice(slc_guid="1234", slc_name="test-slice2", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc), lease_end=datetime.now(timezone.utc),
                 slc_state=SliceState.Dead.value, email="kthare10@email.unc.edu")

    db.add_slice(slc_guid="1234", slc_name="test-slice3", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc), lease_end=datetime.now(timezone.utc),
                 slc_state=SliceState.StableOK.value, email="kthare10@email.unc.edu")

    db.add_slice(slc_guid="1234", slc_name="test-slice4", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc), lease_end=datetime.now(timezone.utc),
                 slc_state=SliceState.StableError.value, email="kthare10@email.unc.edu")

    db.add_slice(slc_guid="1234", slc_name="test-slice3", slc_type=1, slc_resource_type="def",
                 properties=pickle.dumps(prop), lease_start=datetime.now(timezone.utc), lease_end=datetime.now(timezone.utc),
                 slc_state=SliceState.Configuring.value, email="kthare10@email.unc.edu")

    ss = db.get_slices(state=[SliceState.Dead.value, SliceState.Closing.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 2

    ss = db.get_slices(state=[SliceState.StableOK.value, SliceState.StableError.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 2

    ss = db.get_slices(state=[SliceState.Configuring.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 1


if __name__ == '__main__':
    test2()
    test()
    test3()
