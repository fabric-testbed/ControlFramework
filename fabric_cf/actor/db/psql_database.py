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
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Tuple, Dict

from sqlalchemy import create_engine, desc, func, and_, or_
from sqlalchemy.orm import scoped_session, sessionmaker, joinedload

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.common.exceptions import DatabaseException
from fabric_cf.actor.db import Base, Clients, ConfigMappings, Proxies, Units, Reservations, Slices, ManagerObjects, \
    Miscellaneous, Actors, Delegations, Sites, Poas, Components, Metrics


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
        self.session_factory = sessionmaker(bind=self.db_engine)
        self.sessions = {}

    def get_session(self):
        thread_id = threading.get_ident()
        session = None
        if thread_id in self.sessions:
            session = self.sessions.get(thread_id)
        else:
            session = scoped_session(self.session_factory)
            self.sessions[thread_id] = session
        return session

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
        session = self.get_session()
        try:
            session.query(Clients).delete()
            session.query(ConfigMappings).delete()
            session.query(Proxies).delete()
            session.query(Units).delete()
            session.query(Delegations).delete()
            session.query(Components).delete()
            session.query(Reservations).delete()
            session.query(Slices).delete()
            session.query(ManagerObjects).delete()
            session.query(Miscellaneous).delete()
            session.query(Actors).delete()
            session.query(Sites).delete()
            session.query(Poas).delete()
            session.query(Metrics).delete()
            session.commit()
        except Exception as e:
            session.rollback()
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
        session = self.get_session()
        try:
            # Save the actor in the database
            actor_obj = Actors(act_name=name, act_guid=guid, act_type=act_type, properties=properties)
            session.add(actor_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_actor(self, *, name: str, properties):
        """
        Update an actor
        @param name name
        @param properties pickle dump for actor instance
        """
        session = self.get_session()
        try:
            actor = session.query(Actors).filter_by(act_name=name).one_or_none()
            if actor is not None:
                actor.properties = properties
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_actor(self, *, name: str):
        """
        Remove an actor
        @param name name
        """
        session = self.get_session()
        try:
            # Delete the actor in the database
            session.query(Actors).filter_by(act_name=name).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_actors(self) -> list:
        """
        Get all actors
        @return list of actors
        """
        result = []
        session = self.get_session()
        try:
            for row in session.query(Actors).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            for row in session.query(Actors).filter(Actors.act_type == act_type).filter(
                    Actors.act_name.like(actor_name)).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            for row in session.query(Actors).filter(Actors.act_name.like(act_name)).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            actor = session.query(Actors).filter_by(act_name=name).one_or_none()
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
        session = self.get_session()
        try:
            msc_obj = Miscellaneous(msc_path=name, properties=properties)
            session.add(msc_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_miscellaneous(self, *, name: str, properties: dict):
        """
        Add Miscellaneous entries
        @param name name
        @param properties properties

        """
        session = self.get_session()
        try:
            msc_obj = session.query(Miscellaneous).filter_by(msc_path=name).one_or_none()
            if msc_obj is not None:
                msc_obj.properties = properties
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_miscellaneous(self, *, name: str):
        """
        Remove Miscellaneous entries
        @param name name
        """
        session = self.get_session()
        try:
            session.query(Miscellaneous).filter_by(msc_path=name).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_miscellaneous(self, *, name: str) -> dict or None:
        """
        Get Miscellaneous entry
        @param name name
        @return entry identified by name
        """
        result = {}
        session = self.get_session()
        try:
            msc_obj = session.query(Miscellaneous).filter_by(msc_path=name).one_or_none()
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
        session = self.get_session()
        try:
            if act_id is not None:
                mng_obj = ManagerObjects(mo_key=manager_key, mo_act_id=act_id, properties=properties)
            else:
                mng_obj = ManagerObjects(mo_key=manager_key, properties=properties)
            session.add(mng_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_manager_object(self, *, manager_key: str):
        """
        Remove management object
        @param manager_key management object key
        """
        session = self.get_session()
        try:
            session.query(ManagerObjects).filter_by(mo_key=manager_key).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_manager_object_by_actor(self, *, act_id: int):
        """
        Remove management object by actor id
        @param act_id actor id
        """
        session = self.get_session()
        try:
            session.query(ManagerObjects).filter_by(mo_act_id=act_id).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_manager_objects(self, *, act_id: int = None) -> list:
        """
        Get Management objects
        @param act_id actor id
        @return list of objects
        """
        result = []
        session = self.get_session()
        try:
            if act_id is None:
                rows = session.query(ManagerObjects).all()
            else:
                rows = session.query(ManagerObjects).filter_by(mo_act_id=act_id).all()

            for row in rows:
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            mo_obj = session.query(ManagerObjects).filter_by(mo_key=mo_key).one_or_none()
            if mo_obj is not None:
                result = self.generate_dict_from_row(mo_obj)
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
        session = self.get_session()
        try:
            for mo_obj in session.query(ManagerObjects).filter(ManagerObjects.mo_act_id.is_(None)).all():
                result.append(self.generate_dict_from_row(mo_obj))
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
        session = self.get_session()
        try:
            slc_obj = Slices(slc_guid=slc_guid, slc_name=slc_name, slc_type=slc_type, slc_state=slc_state,
                             oidc_claim_sub=oidc_claim_sub, email=email, slc_resource_type=slc_resource_type,
                             lease_start=lease_start, lease_end=lease_end, properties=properties,
                             project_id=project_id)
            if slc_graph_id is not None:
                slc_obj.slc_graph_id = slc_graph_id
            
            session.add(slc_obj)
            session.commit()
        except Exception as e:
            session.rollback()
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
        session = self.get_session()
        try:
            slc_obj = session.query(Slices).filter_by(slc_guid=slc_guid).one_or_none()
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
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_slice(self, *, slc_guid: str):
        """
        Remove Slice
        @param slc_guid slice id
        """
        session = self.get_session()
        try:
            session.query(Slices).filter_by(slc_guid=slc_guid).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_slice_ids(self) -> list:
        """
        Get slice ids for an actor
        @param act_id actor id
        @return list of slice ids
        """
        result = []
        session = self.get_session()
        try:
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

    def get_slice_count(self, *, project_id: str = None, email: str = None, states: List[int] = None,
                        oidc_sub: str = None, slc_type: List[int] = None, excluded_projects: List[str]) -> int:
        """
        Get slices count for an actor
        @param project_id project id
        @param email email
        @param states states
        @param oidc_sub oidc claim sub
        @param slc_type slice type
        @param excluded_projects excluded_projects
        @return list of slices
        """
        session = self.get_session()
        try:
            filter_dict = self.create_slices_filter(project_id=project_id, email=email, oidc_sub=oidc_sub)

            rows = session.query(Slices).filter_by(**filter_dict)

            rows = rows.order_by(desc(Slices.lease_end))

            if states is not None:
                rows = rows.filter(Slices.slc_state.in_(states))

            if slc_type is not None:
                rows = rows.filter(Slices.slc_type.in_(slc_type))

            if excluded_projects is not None:
                rows = rows.filter(Slices.project_id.notin_(excluded_projects))

            return rows.count()
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_slices(self, *, slice_id: str = None, slice_name: str = None, project_id: str = None, email: str = None,
                   states: list[int] = None, oidc_sub: str = None, slc_type: list[int] = None, limit: int = None,
                   offset: int = None, lease_end: datetime = None, search: str = None,
                   exact_match: bool = False) -> List[dict]:
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
        @param search: search term applied
        @param exact_match: Exact Match for Search term
        @return list of slices
        """
        result = []
        session = self.get_session()
        try:
            filter_dict = self.create_slices_filter(slice_id=slice_id, slice_name=slice_name, project_id=project_id,
                                                    email=email, oidc_sub=oidc_sub)

            rows = session.query(Slices).filter_by(**filter_dict)

            if search:
                if exact_match:
                    search_term = func.lower(search)
                    rows = rows.filter(((func.lower(Slices.email) == search_term) |
                                        (func.lower(Slices.oidc_claim_sub) == search_term)))
                else:
                    rows = rows.filter(
                        ((Slices.email.ilike("%" + search + "%")) |
                         (Slices.oidc_claim_sub.ilike("%" + search + "%"))))

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
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            slc_obj = session.query(Slices).filter_by(slc_id=slc_id).one_or_none()
            if slc_obj is not None:
                result = self.generate_dict_from_row(slc_obj)
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Slice", slc_id))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def add_reservation(self, *, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                        rsv_pending: int, rsv_joining: int, properties, lease_start: datetime = None,
                        lease_end: datetime = None, rsv_graph_node_id: str = None, oidc_claim_sub: str = None,
                        email: str = None, project_id: str = None, site: str = None, rsv_type: str = None,
                        components: List[Tuple[str, str, str]] = None, host: str = None, ip_subnet: str = None):
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
        @param components list of components
        @param host host
        @param ip_subnet ip_subnet
        """
        session = self.get_session()
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            rsv_obj = Reservations(rsv_slc_id=slc_id, rsv_resid=rsv_resid, rsv_category=rsv_category,
                                   rsv_state=rsv_state, rsv_pending=rsv_pending, rsv_joining=rsv_joining,
                                   lease_start=lease_start, lease_end=lease_end,
                                   properties=properties, oidc_claim_sub=oidc_claim_sub, email=email,
                                   project_id=project_id, site=site, rsv_type=rsv_type, host=host, ip_subnet=ip_subnet)
            if rsv_graph_node_id is not None:
                rsv_obj.rsv_graph_node_id = rsv_graph_node_id

            # Create Component Mapping for the Network Service reservation
            if components:
                for node_id, cid, bdf in components:
                    mapping = Components(node_id=node_id, reservation=rsv_obj,
                                         component=cid, bdf=bdf)
                    session.add(mapping)

            session.add(rsv_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_reservation(self, *, slc_guid: str, rsv_resid: str, rsv_category: int, rsv_state: int,
                           rsv_pending: int, rsv_joining: int, properties, lease_start: datetime = None,
                           lease_end: datetime = None, rsv_graph_node_id: str = None, site: str = None,
                           rsv_type: str = None, components: List[Tuple[str, str, str]] = None,
                           host: str = None, ip_subnet: str = None):
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
        @param components list of components
        @param ip_subnet ip subnet
        @param host host
        """
        session = self.get_session()
        try:
            rsv_obj = session.query(Reservations).filter_by(rsv_resid=rsv_resid).one()
            if rsv_obj is not None:
                rsv_obj.rsv_category = rsv_category
                rsv_obj.rsv_state = rsv_state
                rsv_obj.rsv_pending = rsv_pending
                rsv_obj.rsv_joining = rsv_joining
                rsv_obj.properties = properties
                rsv_obj.lease_end = lease_end
                rsv_obj.lease_start = lease_start
                if host:
                    rsv_obj.host = host
                if ip_subnet:
                    rsv_obj.ip_subnet = ip_subnet
                if site is not None:
                    rsv_obj.site = site
                if rsv_graph_node_id is not None:
                    rsv_obj.rsv_graph_node_id = rsv_graph_node_id
                if rsv_type is not None:
                    rsv_obj.rsv_type = rsv_type

                # Update components records for the reservation
                if components and len(components):
                    existing = session.query(Components).filter(Components.reservation_id == rsv_obj.rsv_id).all()
                    existing_components = {(c.node_id, c.component, c.bdf) for c in existing}

                    # Identify new string values
                    added_comps = set(components) - existing_components

                    # Identify outdated string values
                    removed_comps = existing_components - set(components)

                    # Remove outdated comps
                    for node_id, cid, bdf in removed_comps:
                        comp_to_remove = next(
                            (comp for comp in existing if comp.component == cid and
                             comp.node_id == node_id and comp.bdf == bdf),
                            None)
                        if comp_to_remove:
                            session.delete(comp_to_remove)

                    # Add new comps
                    for node_id, cid, bdf in added_comps:
                        new_mapping = Components(node_id=node_id, reservation=rsv_obj,
                                                 component=cid, bdf=bdf)
                        session.add(new_mapping)

            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Reservation", rsv_resid))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_reservation(self, *, rsv_resid: str):
        """
        Remove a reservation
        @param rsv_resid reservation guid
        """
        session = self.get_session()
        try:
            #session.query(Reservations).filter_by(rsv_resid=rsv_resid).delete()
            reservation = session.query(Reservations).filter_by(rsv_resid=rsv_resid).one_or_none()

            if reservation:
                # Delete associated Components records
                mappings = session.query(Components).filter(Components.reservation_id == reservation.rsv_id).all()
                for mapping in mappings:
                    session.delete(mapping)

            session.delete(reservation)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def create_reservation_filter(self, *, slice_id: str = None, graph_node_id: str = None, project_id: str = None,
                                  email: str = None, oidc_sub: str = None, rid: str = None, site: str = None,
                                  ip_subnet: str = None, host: str = None) -> dict:

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
        if ip_subnet:
            filter_dict['ip_subnet'] = ip_subnet
        if host:
            filter_dict['host'] = host

        return filter_dict

    def get_reservations(self, *, slice_id: str = None, graph_node_id: str = None, project_id: str = None,
                         email: str = None, oidc_sub: str = None, rid: str = None, states: list[int] = None,
                         category: list[int] = None, site: str = None, rsv_type: list[str] = None,
                         start: datetime = None, end: datetime = None, ip_subnet: str = None,
                         host: str = None) -> List[dict]:
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
        @param start search for slivers with lease_end_time after start
        @param end search for slivers with lease_end_time before end
        @param ip_subnet ip subnet
        @param host host

        @return list of reservations
        """
        result = []
        session = self.get_session()
        try:
            filter_dict = self.create_reservation_filter(slice_id=slice_id, graph_node_id=graph_node_id,
                                                         project_id=project_id, email=email, oidc_sub=oidc_sub,
                                                         rid=rid, site=site, ip_subnet=ip_subnet, host=host)
            rows = session.query(Reservations).filter_by(**filter_dict)

            if rsv_type is not None:
                rows = rows.filter(Reservations.rsv_type.in_(rsv_type))

            if states is not None:
                rows = rows.filter(Reservations.rsv_state.in_(states))

            if category is not None:
                rows = rows.filter(Reservations.rsv_category.in_(category))

            # Ensure start and end are datetime objects
            if start and isinstance(start, str):
                start = datetime.fromisoformat(start)
            if end and isinstance(end, str):
                end = datetime.fromisoformat(end)

            # Construct filter condition for lease_end within the given time range
            if start is not None or end is not None:
                lease_end_filter = True  # Initialize with True to avoid NoneType comparison
                if start is not None and end is not None:
                    lease_end_filter = or_(
                        and_(start <= Reservations.lease_end, Reservations.lease_end <= end),
                        and_(start <= Reservations.lease_start, Reservations.lease_start <= end),
                        and_(Reservations.lease_start <= start, Reservations.lease_end >= end)
                    )
                elif start is not None:
                    lease_end_filter = start <= Reservations.lease_end
                elif end is not None:
                    lease_end_filter = Reservations.lease_end <= end

                rows = rows.filter(lease_end_filter)

            for row in rows.all():
                result.append(self.generate_dict_from_row(row=row))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

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
        @param excludes: list of the reservations ids to exclude

        NOTE# For P4 switches; node_id=node+renc-p4-sw  component=ip+192.168.11.8 bdf=p1

        @return Dictionary with component name as the key and value as list of associated PCI addresses in use.
        """
        result = {}
        session = self.get_session()
        try:
            lease_end_filter = True  # Initialize with True to avoid NoneType comparison
            # Construct filter condition for lease_end within the given time range
            if start is not None or end is not None:
                if start is not None and end is not None:
                    lease_end_filter = or_(
                        and_(start <= Reservations.lease_end, Reservations.lease_end <= end),
                        and_(start <= Reservations.lease_start, Reservations.lease_start <= end),
                        and_(Reservations.lease_start <= start, Reservations.lease_end >= end)
                    )
                elif start is not None:
                    lease_end_filter = start <= Reservations.lease_end
                elif end is not None:
                    lease_end_filter = Reservations.lease_end <= end

            # Query to retrieve Components based on specific Reservation types and states
            rows = (
                session.query(Components)
                    .join(Reservations, Components.reservation_id == Reservations.rsv_id)
                    .filter(Reservations.rsv_type.in_(rsv_type))
                    .filter(Reservations.rsv_state.in_(states))
                    .filter(lease_end_filter)
                    .filter(Components.node_id == node_id)
                    .options(joinedload(Components.reservation))
            )

            # Add excludes filter if excludes list is not None and not empty
            if excludes:
                rows = rows.filter(Reservations.rsv_resid.notin_(excludes))

            # Query Component records for reservations in the specified state and owner with the target string
            if component is not None and bdf is not None:
                rows = rows.filter(Components.component == component, Components.bdf == bdf)
            elif component is not None:
                rows = rows.filter(Components.component == component)
            elif bdf is not None:
                rows = rows.filter(Components.bdf == bdf)

            for row in rows.all():
                if row.component not in result:
                    result[row.component] = []
                if row.bdf not in result[row.component]:
                    result[row.component].append(row.bdf)
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
        session = self.get_session()
        try:
            for row in session.query(Reservations).filter(Reservations.rsv_resid.in_(rsv_resid_list)).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            prx_obj = Proxies(prx_act_id=act_id, prx_name=prx_name, properties=properties)
            session.add(prx_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_proxy(self, *, act_id: int, prx_name: str, properties):
        """
        Update a proxy
        @param act_id actor id
        @param prx_name proxy name
        @param properties pickled instance
        """
        session = self.get_session()
        try:
            prx_obj = session.query(Proxies).filter_by(prx_act_id=act_id).filter(
                Proxies.prx_name == prx_name).one_or_none()
            if prx_obj is not None:
                prx_obj.properties = properties
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Proxy", prx_name))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_proxy(self, *, act_id: int, prx_name: str):
        """
        Remove a proxy
        @param act_id actor id
        @param prx_name proxy name
        """
        session = self.get_session()
        try:
            session.query(Proxies).filter(Proxies.prx_act_id == act_id).filter(Proxies.prx_name == prx_name).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_proxies(self, *, act_id: int) -> list:
        """
        Get Proxies
        @param act_id actor id
        """
        result = []
        session = self.get_session()
        try:
            for row in session.query(Proxies).filter_by(prx_act_id=act_id).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            cfg_obj = ConfigMappings(cfgm_act_id=act_id, cfgm_type=cfgm_type, properties=properties)
            session.add(cfg_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_config_mapping(self, *, act_id: int, cfgm_type: str, properties):
        """
        Update handlers mapping
        @param act_id actor id
        @param cfgm_type type
        @param properties properties
        """
        session = self.get_session()
        try:
            cfg_obj = session.query(ConfigMappings).filter(ConfigMappings.cfgm_act_id == act_id).filter(
                ConfigMappings.cfgm_type == cfgm_type).one_or_none()
            if cfg_obj is not None:
                cfg_obj.properties = properties
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Config Mapping", cfgm_type))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_config_mapping(self, *, cfgm_type: str):
        """
        Remove handlers mapping
        @param cfgm_type handlers mapping type
        """
        session = self.get_session()
        try:
            session.query(ConfigMappings).filter_by(cfgm_type=cfgm_type).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_config_mappings(self, *, act_id: int) -> list:
        """
        Get Config Mappings
        @param act_id actor id
        @retur list of mappings
        """
        result = []
        session = self.get_session()
        try:
            for row in session.query(ConfigMappings).filter_by(cfgm_act_id=act_id).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            clt_obj = Clients(clt_act_id=act_id, clt_name=clt_name, clt_guid=clt_guid, properties=properties)
            session.add(clt_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_client(self, *, act_id: int, clt_name: str, properties):
        """
        Update a client
        @param act_id actor id
        @param clt_name client name
        @param properties pickled instance
        """
        session = self.get_session()
        try:
            clt_obj = session.query(Clients).filter(Clients.clt_act_id == act_id).filter(
                Clients.clt_name == clt_name).one_or_none()
            if clt_obj is not None:
                clt_obj.properties = properties
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Client", clt_name))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_client_by_name(self, *, act_id: int, clt_name: str):
        """
        Remove a client by name
        @param act_id actor id
        @param clt_name client name
        """
        session = self.get_session()
        try:
            session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_name == clt_name).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_client_by_guid(self, *, act_id: int, clt_guid: str):
        """
        Remove client by guid
        @param act_id actor id
        @param clt_guid client guid
        """
        session = self.get_session()
        try:
            session.query(Clients).filter(Clients.clt_act_id == act_id).filter(Clients.clt_guid == clt_guid).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_client_by_guid(self, *, clt_guid: str) -> dict:
        """
        Get Client by name
        @param act_id actor id
        @param clt_guid client guid
        @return client
        """
        result = None
        session = self.get_session()
        try:
            clt_obj = session.query(Clients).filter_by(clt_guid=clt_guid).one_or_none()
            if clt_obj is None:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Client", clt_guid))
            result = self.generate_dict_from_row(clt_obj)
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
        session = self.get_session()
        try:
            for row in session.query(Clients).all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            rsv_id = self.get_rsv_id_by_reservation_id(reservation_id=rsv_resid)

            unt_obj = Units(unt_uid=unt_uid,
                            unt_slc_id=slc_id,
                            unt_rsv_id=rsv_id,
                            unt_state=unt_state, properties=properties)
            if unt_unt_id is not None:
                unt_obj.unt_unt_id = unt_unt_id
            session.add(unt_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_unit(self, *, unt_uid: str) -> dict or None:
        """
        Get Unit
        @param act_id actor id
        @param unt_uid unit guid
        @return Unit dict
        """
        result = None
        session = self.get_session()
        try:
            unt_obj = session.query(Units).filter_by(unt_uid=unt_uid).one_or_none()
            if unt_obj is None:
                self.logger.debug("Unit with guid {} not found".format(unt_uid))
                return result
            result = self.generate_dict_from_row(unt_obj)
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
        session = self.get_session()
        try:
            rsv_obj = self.get_reservations(rid=rsv_resid)[0]
            if rsv_obj is None:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Reservation", rsv_resid))

            for row in session.query(Units).filter_by(unt_rsv_id=rsv_obj['rsv_id']).all():
                result.append(self.generate_dict_from_row(row=row))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def remove_unit(self, *, unt_uid: str):
        """
        Remove a unit
        @param unt_uid unit id
        """
        session = self.get_session()
        try:
            session.query(Units).filter_by(unt_uid=unt_uid).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_unit(self, *, unt_uid: str, properties):
        """
        Add Unit
        @param unt_uid unit id
        @param properties properties
        """
        result = None
        session = self.get_session()
        try:
            unt_obj = session.query(Units).filter_by(unt_uid=unt_uid).one_or_none()
            if unt_obj is None:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Unit", unt_uid))
            unt_obj.properties = properties
            session.commit()
        except Exception as e:
            session.rollback()
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
        session = self.get_session()
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slice_id)
            dlg_obj = Delegations(dlg_slc_id=slc_id, dlg_graph_id=dlg_graph_id,
                                  dlg_state=dlg_state, properties=properties, site=site)
            session.add(dlg_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_delegation(self, *, dlg_graph_id: str, dlg_state: int, properties, site: str = None):
        """
        Update delegation
        @param dlg_graph_id graph id
        @param dlg_state state
        @param properties properties
        @param site site
        """
        session = self.get_session()
        try:
            dlg_obj = session.query(Delegations).filter_by(dlg_graph_id=dlg_graph_id).one_or_none()
            if dlg_obj is not None:
                dlg_obj.dlg_state = dlg_state
                dlg_obj.properties = properties
                if site is not None:
                    dlg_obj.site = site
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Delegation", dlg_graph_id))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_delegation(self, *, dlg_graph_id: str):
        """
        Remove delegation
        @param dlg_graph_id graph id
        """
        session = self.get_session()
        try:
            session.query(Delegations).filter_by(dlg_graph_id=dlg_graph_id).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

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

    def get_delegations(self, *, slc_guid: str = None, states: List[int] = None) -> List[dict]:
        """
        Get delegations
        @param slc_guid slice guid
        @param states delegation state
        @param list of delegations
        """
        result = []
        session = self.get_session()
        try:
            slc_id = self.get_slc_id_by_slice_id(slice_id=slc_guid)
            rows = session.query(Delegations)
            if slc_guid is not None:
                rows = rows.filter(Delegations.dlg_slc_id == slc_id)
            if states is not None:
                rows = rows.filter(Delegations.dlg_state.in_(states))
            for row in rows.all():
                result.append(self.generate_dict_from_row(row=row))
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
        session = self.get_session()
        try:
            dlg_obj = session.query(Delegations).filter_by(dlg_graph_id=dlg_graph_id).one_or_none()
            if dlg_obj is not None:
                result = self.generate_dict_from_row(dlg_obj)
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
        session = self.get_session()
        try:
            site_obj = Sites(name=site_name, state=state, properties=properties)
            session.add(site_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_site(self, *, site_name: str, state: int, properties):
        """
        Update a site
        @param site_name Site Name
        @param state state
        @param properties pickled instance
        """
        session = self.get_session()
        try:
            site_obj = session.query(Sites).filter_by(name=site_name).one_or_none()
            if site_obj is not None:
                site_obj.properties = properties
                site_obj.state = state
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Sites", site_name))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_site(self, *, site_name: str):
        """
        Remove a proxy
        @param site_name site_name
        """
        session = self.get_session()
        try:
            session.query(Sites).filter_by(name=site_name).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_sites(self) -> list:
        """
        Get Sites
=        """
        result = []
        session = self.get_session()
        try:
            for row in session.query(Sites).all():
                result.append(self.generate_dict_from_row(row=row))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def get_site(self, *, site_name: str) -> list:
        """
        Get Sites
=        """
        result = []
        session = self.get_session()
        try:
            for row in session.query(Sites).filter_by(name=site_name).all():
                result.append(self.generate_dict_from_row(row=row))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    @staticmethod
    def generate_dict_from_row(row):
        d = row.__dict__.copy()
        for k in row.__dict__:
            if d[k] is None:
                d.pop(k)
        return d

    def add_poa(self, *, poa_guid: str, properties, email: str, project_id: str, sliver_id: str, slice_id: str,
                state: int):
        """
        Add a POA
        @param poa_guid POA id
        @param properties pickled instance
        @param project_id User OIDC Sub
        @param email User Email
        @param sliver_id Sliver Id
        @param slice_id slice id
        @param state state
        """
        session = self.get_session()
        try:
            poa_obj = Poas(poa_guid=poa_guid, project_id=project_id, email=email, sliver_id=sliver_id,
                           slice_id=slice_id, state=state,last_update_time=datetime.now(timezone.utc),
                           properties=properties)
            session.add(poa_obj)
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def update_poa(self, *, poa_guid: str, properties, state: int, email: str = None, project_id: str = None,
                   sliver_id: str = None):
        """
        Update a POA
        @param poa_guid POA id
        @param properties pickled instance
        @param state state
        @param project_id User OIDC Sub
        @param email User Email
        @param sliver_id Sliver Id
        """
        session = self.get_session()
        try:
            poa_obj = session.query(Poas).filter_by(poa_guid=poa_guid).one_or_none()
            if poa_obj is not None:
                poa_obj.properties = properties
                if email is not None:
                    poa_obj.email = email
                if project_id is not None:
                    poa_obj.project_id = project_id
                if sliver_id is not None:
                    poa_obj.sliver_id = sliver_id
                poa_obj.last_update_time = datetime.now(timezone.utc)
                poa_obj.state = state
            else:
                raise DatabaseException(self.OBJECT_NOT_FOUND.format("Poa", poa_guid))
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def remove_poa(self, *, poa_guid: str):
        """
        Remove POA
        @param poa_guid POA Guid
        """
        session = self.get_session()
        try:
            session.query(Poas).filter_by(poa_guid=poa_guid).delete()
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    @staticmethod
    def create_poa_filter(*, poa_guid: str = None, sliver_id: str = None, project_id: str = None,
                          email: str = None, slice_id: str = None) -> dict:

        filter_dict = {}
        if poa_guid is not None:
            filter_dict['poa_guid'] = poa_guid
        if sliver_id is not None:
            filter_dict['sliver_id'] = str(sliver_id)
        if slice_id is not None:
            filter_dict['slice_id'] = str(slice_id)
        if project_id is not None:
            filter_dict['project_id'] = project_id
        if email is not None:
            filter_dict['email'] = email
        return filter_dict

    def get_poas(self, *, poa_guid: str = None, project_id: str = None, email: str = None, sliver_id: str = None,
                 slice_id: str, limit: int = None, offset: int = None, last_update_time: datetime = None,
                 states: list[int] = None) -> List[dict]:
        """
        Get slices for an actor
        @param poa_guid POA Guid
        @param project_id project id
        @param email email
        @param limit limit
        @param offset offset
        @param sliver_id Sliver Id
        @param slice_id Slice Id
        @param last_update_time Last Update Time
        @param states
        @return list of POAs
        """
        result = []
        session = self.get_session()
        try:
            filter_dict = self.create_poa_filter(poa_guid=poa_guid, project_id=project_id, email=email,
                                                 sliver_id=sliver_id, slice_id=slice_id)

            rows = session.query(Poas).filter_by(**filter_dict)

            if last_update_time is not None:
                rows = rows.filter(Poas.last_update_time < last_update_time)

            if states is not None:
                rows = rows.filter(Poas.state.in_(states))

            rows = rows.order_by(desc(Poas.last_update_time))

            if offset is not None and limit is not None:
                rows = rows.offset(offset).limit(limit)

            for row in rows.all():
                result.append(self.generate_dict_from_row(row=row))
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e
        return result

    def increment_metrics(self, *, project_id: str, user_id: str, slice_count: int = 1):
        """
        Add or Update Metrics
        @param project_id: project_id
        @param user_id: user_id
        @param slice_count: slice_count
        """
        session = self.get_session()
        try:
            metric_obj = session.query(Metrics).filter_by(project_id=project_id, user_id=user_id).one_or_none()
            if not metric_obj:
                metric_obj = Metrics(project_id=project_id, user_id=user_id, slice_count=slice_count)
                session.add(metric_obj)
            else:
                metric_obj.slice_count += slice_count
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e

    def get_metrics(self, *, project_id: str = None, user_id: str = None, excluded_projects: List[str] = None) -> list:
        """
        Get Metric count
        @param project_id: project_id
        @param user_id: user_id
        @param excluded_projects: excluded_projects
        @return list of metrics
        """
        result = []
        session = self.get_session()
        try:
            filter_criteria = True
            # Construct filter condition
            if project_id and user_id:
                filter_criteria = and_(Metrics.project_id == project_id, Metrics.user_id == user_id)
            elif project_id is not None:
                filter_criteria = and_(Metrics.project_id == project_id)
            elif user_id is not None:
                filter_criteria = and_(Metrics.user_id == user_id)

            if excluded_projects:
                filter_criteria = and_(Metrics.project_id.notin_(excluded_projects))

            rows = session.query(Metrics).filter(filter_criteria).all()

            for r in rows:
                result.append(self.generate_dict_from_row(row=r))
            return result
        except Exception as e:
            self.logger.error(Constants.EXCEPTION_OCCURRED.format(e))
            raise e


def test():
    logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='am', db_host='127.0.0.1:5432', logger=logger)
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
    print("Reservations after by state {}".format(db.get_reservations(slice_id="1234", states=[2])))

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
    db.add_config_mapping(act_id=actor_id, cfgm_type="cfg-1", properties=pickle.dumps(prop))
    print("Config Mappings after add {}".format(db.get_config_mappings(act_id=actor_id)))
    prop['cfg-update'] = 'done'
    db.update_config_mapping(act_id=actor_id, cfgm_type="cfg-1", properties=pickle.dumps(prop))
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
    db = PsqlDatabase(user='fabric', password='fabric', database='broker', db_host='127.0.0.1:5432', logger=logger)
    from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
    states = [ReservationStates.Active.value,
              ReservationStates.ActiveTicketed.value,
              ReservationStates.Ticketed.value,
              ReservationStates.Nascent.value]
    res = db.get_reservations(graph_node_id='node+renc-data-sw:ip+192.168.11.3-ns', states=states)
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

    ss = db.get_slices(states=[SliceState.Dead.value, SliceState.Closing.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 2

    ss = db.get_slices(states=[SliceState.StableOK.value, SliceState.StableError.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 2

    ss = db.get_slices(states=[SliceState.Configuring.value],
                       email="kthare10@email.unc.edu")

    assert len(ss) == 1


if __name__ == '__main__':
    test2()
    #test()
    #test3()

    logger = logging.getLogger('PsqlDatabase')
    db = PsqlDatabase(user='fabric', password='fabric', database='orchestrator', db_host='127.0.0.1:5432',
                      logger=logger)
    comps = db.get_components(node_id="HX7LQ53")