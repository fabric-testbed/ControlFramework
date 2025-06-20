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

from sqlalchemy import JSON, ForeignKey, LargeBinary, Index, TIMESTAMP, func
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Sequence
from sqlalchemy.orm import relationship


Base = declarative_base()

FOREIGN_KEY_ACTOR_ID = 'Actors.act_id'
FOREIGN_KEY_SLICE_ID = 'Slices.slc_id'
FOREIGN_KEY_RESERVATION_ID = 'Reservations.rsv_id'


class Actors(Base):
    """
    Represents Actors Database Table
    """
    __tablename__ = 'Actors'
    act_id = Column(Integer, Sequence('act_id', start=1, increment=1), autoincrement=True, primary_key=True)
    act_name = Column(String, unique=True, nullable=False)
    act_guid = Column(String, nullable=False)
    act_type = Column(Integer, nullable=False)
    properties = Column(LargeBinary)


class Clients(Base):
    """
    Represents Clients Database Table
    """
    __tablename__ = 'Clients'
    clt_id = Column(Integer, Sequence('clt_id', start=1, increment=1), autoincrement=True, primary_key=True)
    clt_act_id = Column(Integer, ForeignKey(FOREIGN_KEY_ACTOR_ID))
    clt_name = Column(String, nullable=False)
    clt_guid = Column(String, nullable=False)
    properties = Column(LargeBinary)


class ConfigMappings(Base):
    """
    Represents ConfigMappings Database Table
    """
    __tablename__ = 'ConfigMappings'
    cfgm_id = Column(Integer, Sequence('cfgm_id', start=1, increment=1), autoincrement=True, primary_key=True)
    cfgm_act_id = Column(Integer, ForeignKey(FOREIGN_KEY_ACTOR_ID))
    cfgm_type = Column(String, nullable=False)
    properties = Column(LargeBinary)


class ManagerObjects(Base):
    """
    Represents ManagerObjects Database Table
    """
    __tablename__ = 'ManagerObjects'
    mo_id = Column(Integer, Sequence('mo_id', start=1, increment=1), autoincrement=True, primary_key=True)
    mo_key = Column(String, nullable=False, unique=True)
    mo_act_id = Column(Integer, ForeignKey(FOREIGN_KEY_ACTOR_ID))
    properties = Column(JSON)


class Miscellaneous(Base):
    """
    Represents Miscellaneous Database Table
    """
    __tablename__ = 'Miscellaneous'
    msc_id = Column(Integer, Sequence('msc_id', start=1, increment=1), autoincrement=True, primary_key=True)
    msc_path = Column(String, nullable=False, unique=True)
    properties = Column(JSON)


class Metrics(Base):
    """
    Represents Metrics Database Table
    """
    __tablename__ = 'Metrics'
    m_id = Column(Integer, Sequence('m_id', start=1, increment=1), autoincrement=True, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    project_id = Column(String, nullable=False, index=True)
    slice_count = Column(Integer, nullable=False)


class Proxies(Base):
    """
    Represents Proxies Database Table
    """
    __tablename__ = 'Proxies'
    prx_id = Column(Integer, Sequence('prx_id', start=1, increment=1), autoincrement=True, primary_key=True)
    prx_act_id = Column(Integer, ForeignKey(FOREIGN_KEY_ACTOR_ID))
    prx_name = Column(String)
    properties = Column(LargeBinary)


class Reservations(Base):
    """
    Represents Reservations Database Table
    """
    __tablename__ = 'Reservations'
    rsv_id = Column(Integer, Sequence('rsv_id', start=1, increment=1), autoincrement=True, primary_key=True)
    rsv_graph_node_id = Column(String, nullable=True, index=True)
    rsv_slc_id = Column(Integer, ForeignKey(FOREIGN_KEY_SLICE_ID), index=True)
    rsv_resid = Column(String, nullable=False, index=True)
    oidc_claim_sub = Column(String, nullable=True, index=True)
    host = Column(String, nullable=True, index=True)
    ip_subnet = Column(String, nullable=True, index=True)
    email = Column(String, nullable=True, index=True)
    project_id = Column(String, nullable=True, index=True)
    site = Column(String, nullable=True, index=True)
    rsv_type = Column(String, nullable=True, index=True)
    rsv_state = Column(Integer, nullable=False, index=True)
    rsv_category = Column(Integer, nullable=False)
    rsv_pending = Column(Integer, nullable=False)
    rsv_joining = Column(Integer, nullable=False)
    lease_start = Column(TIMESTAMP(timezone=True), nullable=True)
    lease_end = Column(TIMESTAMP(timezone=True), nullable=True)
    properties = Column(LargeBinary)
    components = relationship('Components', back_populates='reservation')
    links = relationship('Links', back_populates='reservation')

    Index('idx_slc_guid_resid', rsv_slc_id, rsv_resid)
    Index('idx_slc_guid_resid_email', rsv_slc_id, rsv_resid, email)
    Index('idx_resid_state', rsv_resid, rsv_state)
    Index('idx_slcid_state', rsv_slc_id, rsv_state)
    Index('idx_graph_id_res_id', rsv_graph_node_id, rsv_resid)
    Index('idx_host', host)
    Index('idx_ip_subnet', ip_subnet)


class Slices(Base):
    """
    Represents Slices Database Table
    """
    __tablename__ = 'Slices'
    slc_id = Column(Integer, Sequence('slc_id', start=1, increment=1), autoincrement=True, primary_key=True)
    slc_graph_id = Column(String, index=True)
    oidc_claim_sub = Column(String, index=True)
    email = Column(String, nullable=True, index=True)
    project_id = Column(String, nullable=True, index=True)
    slc_guid = Column(String, nullable=False, index=True)
    slc_name = Column(String, nullable=False, index=True)
    slc_state = Column(Integer, nullable=False, index=True)
    slc_type = Column(Integer, nullable=False, index=True)
    slc_resource_type = Column(String)
    lease_start = Column(TIMESTAMP(timezone=True), nullable=True)
    lease_end = Column(TIMESTAMP(timezone=True), nullable=True)
    properties = Column(LargeBinary)

    # New column for tracking last update time
    last_update_time = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    Index('idx_slc_guid_name', slc_guid, slc_name)
    Index('idx_slc_guid_name_email', slc_guid, slc_name, email)
    Index('idx_slc_guid_state', slc_guid, slc_state)


class Units(Base):
    """
    Represents Units Database Table
    """
    __tablename__ = 'Units'
    unt_id = Column(Integer, Sequence('unt_id', start=1, increment=1), autoincrement=True, primary_key=True)
    unt_uid = Column(String)
    unt_unt_id = Column(Integer, nullable=True)
    unt_slc_id = Column(Integer, ForeignKey(FOREIGN_KEY_SLICE_ID))
    unt_rsv_id = Column(Integer, ForeignKey(FOREIGN_KEY_RESERVATION_ID))
    unt_state = Column(Integer, nullable=False)
    properties = Column(LargeBinary)


class Delegations(Base):
    """
    Represents Delegations Database Table
    """
    __tablename__ = 'Delegations'
    dlg_id = Column(Integer, Sequence('dlg_id', start=1, increment=1), autoincrement=True, primary_key=True)
    dlg_slc_id = Column(Integer, ForeignKey(FOREIGN_KEY_SLICE_ID))
    site = Column(String, nullable=True, index=True)
    dlg_graph_id = Column(String, nullable=False)
    dlg_state = Column(Integer, nullable=False)
    properties = Column(LargeBinary)


class Sites(Base):
    """
    Represents Sites Database Table
    """
    __tablename__ = 'Sites'
    site_id = Column(Integer, Sequence('site_id', start=1, increment=1), autoincrement=True, primary_key=True)
    name = Column(String, index=True)
    state = Column(Integer, nullable=False)
    properties = Column(LargeBinary)


class Poas(Base):
    """
    Represents Poas Database Table
    """
    __tablename__ = 'Poas'
    poa_id = Column(Integer, Sequence('poa_id', start=1, increment=1), autoincrement=True, primary_key=True)
    poa_guid = Column(String, nullable=False, index=True)
    email = Column(String, nullable=True, index=True)
    project_id = Column(String, nullable=True, index=True)
    sliver_id = Column(String, nullable=True, index=True)
    state = Column(Integer, nullable=False, index=True)
    slice_id = Column(String, nullable=True, index=True)
    last_update_time = Column(TIMESTAMP(timezone=True), nullable=True)
    properties = Column(LargeBinary)

    Index('idx_poa_guid_email', poa_guid, email)
    Index('idx_poa_guid_project_id', poa_guid, project_id)
    Index('idx_poa_guid_sliver_id', poa_guid, sliver_id)
    Index('idx_poa_guid_email_sliver_id', poa_guid, email, sliver_id)
    Index('idx_poa_guid_email_project_id', poa_guid, email, project_id)


class Components(Base):
    __tablename__ = 'Components'
    reservation_id = Column(Integer, ForeignKey('Reservations.rsv_id'), primary_key=True)
    node_id = Column(String, primary_key=True, index=True)
    component = Column(String, primary_key=True, index=True)
    bdf = Column(String, primary_key=True, index=True)
    reservation = relationship('Reservations', back_populates='components')


class Links(Base):
    __tablename__ = 'Links'
    reservation_id = Column(Integer, ForeignKey('Reservations.rsv_id'), primary_key=True)
    node_id = Column(String, primary_key=True, index=True)
    layer = Column(String)
    type = Column(String)
    bw = Column(Integer)
    properties = Column(LargeBinary)
    reservation = relationship('Reservations', back_populates='links')