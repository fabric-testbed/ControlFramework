from sqlalchemy import ForeignKey, TIMESTAMP, Index, JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Integer, Sequence

Base = declarative_base()


class Sites(Base):
    __tablename__ = 'sites'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)


class Hosts(Base):
    __tablename__ = 'hosts'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    site_id = Column(Integer, ForeignKey('sites.id'), index=True)
    name = Column(String, nullable=False, index=True)


class Projects(Base):
    __tablename__ = 'projects'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    project_uuid = Column(String, nullable=False, index=True)
    project_name = Column(String, nullable=True, index=True)


class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    user_uuid = Column(String, nullable=False, index=True)
    user_email = Column(String, nullable=True, index=True)


class Slices(Base):
    __tablename__ = 'slices'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey('projects.id'), index=True)
    user_id = Column(Integer, ForeignKey('users.id'), index=True)
    slice_guid = Column(String, nullable=False, index=True)
    slice_name = Column(String, nullable=False, index=True)
    state = Column(Integer, nullable=False, index=True)
    lease_start = Column(TIMESTAMP(timezone=True), nullable=True, index=True)
    lease_end = Column(TIMESTAMP(timezone=True), nullable=True, index=True)

    __table_args__ = (
        Index('idx_slice_lease_range', 'lease_start', 'lease_end'),
    )


class Slivers(Base):
    __tablename__ = 'slivers'
    id = Column(Integer, Sequence('id', start=1, increment=1), autoincrement=True, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey('projects.id'), index=True)
    slice_id = Column(Integer, ForeignKey('slices.id'), index=True)
    user_id = Column(Integer, ForeignKey('users.id'), index=True)
    host_id = Column(Integer, ForeignKey('hosts.id'), index=True)
    site_id = Column(Integer, ForeignKey('sites.id'), index=True)
    sliver_guid = Column(String, nullable=False, index=True)
    state = Column(Integer, nullable=False, index=True)
    sliver_type = Column(String, nullable=False, index=True)
    ip_subnet = Column(String, nullable=True, index=True)
    image = Column(String, nullable=True)
    core = Column(Integer, nullable=True)
    ram = Column(Integer, nullable=True)
    disk = Column(Integer, nullable=True)
    bandwidth = Column(Integer, nullable=True)
    lease_start = Column(TIMESTAMP(timezone=True), nullable=True, index=True)
    lease_end = Column(TIMESTAMP(timezone=True), nullable=True, index=True)

    __table_args__ = (
        Index('idx_sliver_lease_range', 'lease_start', 'lease_end'),
    )


class Components(Base):
    __tablename__ = 'components'
    sliver_id = Column(Integer, ForeignKey('slivers.id'), primary_key=True)
    component_guid = Column(String, primary_key=True, index=True)
    type = Column(String, nullable=False, index=True)
    model = Column(String, nullable=False, index=True)
    bdfs = Column(JSON, nullable=True)  # Store BDFs as a JSON list


class Interfaces(Base):
    __tablename__ = 'interfaces'
    sliver_id = Column(Integer, ForeignKey('slivers.id'), primary_key=True, index=True)
    interface_guid = Column(String, primary_key=True, index=True)
    port = Column(String, nullable=False, index=True)
    vlan = Column(String, nullable=False, index=True)
    bdf = Column(String, nullable=False, index=True)
