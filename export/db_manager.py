from typing import List, Optional

from sqlalchemy import create_engine, and_, cast, func, String
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from export import Base, Projects, Users, Slivers, Slices, Components, Interfaces, Hosts, Sites


class DatabaseManager:
    def __init__(self, user: str, password: str, database: str, db_host: str):
        """
        Initializes the connection to the PostgreSQL database.
        """
        self.db_engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{db_host}/{database}", echo=True)
        SessionLocal = sessionmaker(bind=self.db_engine)
        self.session = SessionLocal()
        Base.metadata.create_all(self.db_engine)

    # -------------------- DELETE DATA --------------------
    def delete_slice(self, slice_id):
        slice_object = self.session.query(Slices).filter(Slices.id == slice_id).first()
        if slice:
            self.session.delete(slice_object)
            self.session.commit()
            return True
        return False

    def delete_project(self, project_id):
        project = self.session.query(Projects).filter(Projects.id == project_id).first()
        if project:
            self.session.delete(project)
            self.session.commit()
            return True
        return False

    def delete_user(self, user_id):
        user = self.session.query(Users).filter(Users.id == user_id).first()
        if user:
            self.session.delete(user)
            self.session.commit()
            return True
        return False

    # -------------------- QUERY DATA --------------------
    def get_all_projects(self):
        return self.session.query(Projects).all()

    def get_users_by_email(self, email):
        return self.session.query(Users).filter(Users.user_email.ilike(f"%{email}%")).all()

    def get_slivers_by_project(self, project_id):
        return self.session.query(Slivers).filter(Slivers.project_id == project_id).all()

    def get_slivers_in_time_range(self, start_time: datetime, end_time: datetime):
        return self.session.query(Slivers).filter(
            and_(Slivers.lease_start >= start_time, Slivers.lease_end <= end_time)
        ).all()

    # -------------------- FILTER ACTIVE USERS IN TIME RANGE --------------------
    def get_users_with_active_slices(self, start_time: datetime, end_time: datetime):
        results = self.session.query(Users).join(Slices, Slices.user_id == Users.id).filter(
            and_(
                Slices.lease_start <= end_time,
                Slices.lease_end >= start_time,
            )
        ).distinct().all()
        return results

    # -------------------- FILTER ACTIVE PROJECTS IN TIME RANGE --------------------
    def get_active_projects(self, start_time: datetime, end_time: datetime):
        results = self.session.query(Projects).join(Slices, Slices.project_id == Projects.id).filter(
            and_(
                Slices.lease_start <= end_time,
                Slices.lease_end >= start_time,
            )
        ).distinct().all()
        return results

    def get_components_by_sliver(self, sliver_id: int) -> List[dict]:
        """
        Retrieves all components for a given sliver, converting BDFs from JSON.
        """
        components = self.session.query(Components).filter(Components.sliver_id == sliver_id).all()
        return [
            {
                "component_guid": c.component_guid,
                "type": c.type,
                "model": c.model,
                "bdfs": c.bdfs  # JSON automatically converts to a list
            }
            for c in components
        ]

    def get_components_by_bdf(self, bdf_value: str) -> List[dict]:
        """
        Retrieves all components that contain a specific BDF value.
        """
        components = self.session.query(Components).filter(
            func.jsonb_contains(cast(Components.bdfs, String), f'"{bdf_value}"')
        ).all()

        return [
            {
                "component_guid": c.component_guid,
                "type": c.type,
                "model": c.model,
                "bdfs": c.bdfs
            }
            for c in components
        ]

    # -------------------- ADD OR UPDATE DATA --------------------
    def add_or_update_project(self, project_uuid: str, project_name: Optional[str] = None) -> int:
        """
        Adds a project if it doesn't exist, otherwise updates the name.
        """
        project = self.session.query(Projects).filter(Projects.project_uuid == project_uuid).first()
        if project:
            if project_name:
                project.project_name = project_name
        else:
            project = Projects(project_uuid=project_uuid, project_name=project_name)
            self.session.add(project)

        self.session.commit()
        return project.id

    def add_or_update_user(self, user_uuid: str, user_email: Optional[str] = None) -> int:
        """
        Adds a user if it doesn't exist, otherwise updates the email.
        """
        user = self.session.query(Users).filter(Users.user_uuid == user_uuid).first()
        if user:
            if user_email:
                user.user_email = user_email
        else:
            user = Users(user_uuid=user_uuid, user_email=user_email)
            self.session.add(user)

        self.session.commit()
        return user.id

    # -------------------- ADD OR UPDATE SLICE --------------------
    def add_or_update_slice(
                self, project_id: int, user_id: int, slice_guid: str, slice_name: str, state: int,
                lease_start: Optional[datetime], lease_end: Optional[datetime]
        ) -> int:
        """
        Adds a slice if it doesn’t exist, otherwise updates its fields.
        """
        slice_obj = self.session.query(Slices).filter(Slices.slice_guid == slice_guid).first()
        if slice_obj:
            slice_obj.project_id = project_id
            slice_obj.user_id = user_id
            slice_obj.slice_name = slice_name
            slice_obj.state = state
            if lease_start:
                slice_obj.lease_start = lease_start
            if lease_end:
                slice_obj.lease_end = lease_end
        else:
            slice_obj = Slices(
                project_id=project_id,
                user_id=user_id,
                slice_guid=slice_guid,
                slice_name=slice_name,
                state=state,
                lease_start=lease_start,
                lease_end=lease_end
            )
            self.session.add(slice_obj)

        self.session.commit()
        return slice_obj.id

    # -------------------- ADD OR UPDATE SLIVER --------------------
    def add_or_update_sliver(
        self,
        project_id: int,
        slice_id: int,
        user_id: int,
        host_id: int,
        site_id: int,
        sliver_guid: str,
        state: int,
        sliver_type: str,
        ip_subnet: Optional[str] = None,
        image: Optional[str] = None,
        core: Optional[int] = None,
        ram: Optional[int] = None,
        disk: Optional[int] = None,
        bandwidth: Optional[int] = None,
        lease_start: Optional[datetime] = None,
        lease_end: Optional[datetime] = None,
        error: Optional[str] = None
    ) -> int:
        """
        Adds a sliver if it doesn’t exist, otherwise updates its fields.
        """
        sliver = self.session.query(Slivers).filter(Slivers.sliver_guid == sliver_guid).first()

        if sliver:
            sliver.project_id = project_id
            sliver.slice_id = slice_id
            sliver.user_id = user_id
            sliver.host_id = host_id
            sliver.site_id = site_id
            sliver.state = state
            sliver.sliver_type = sliver_type
            if ip_subnet:
                sliver.ip_subnet = ip_subnet
            if image:
                sliver.image = image
            if core:
                sliver.core = core
            if ram:
                sliver.ram = ram
            if disk:
                sliver.disk = disk
            if bandwidth:
                sliver.bandwidth = bandwidth
            if lease_start:
                sliver.lease_start = lease_start
            if lease_end:
                sliver.lease_end = lease_end
            if error:
                sliver.error = error
        else:
            sliver = Slivers(
                project_id=project_id,
                slice_id=slice_id,
                user_id=user_id,
                host_id=host_id,
                site_id=site_id,
                sliver_guid=sliver_guid,
                state=state,
                sliver_type=sliver_type,
                ip_subnet=ip_subnet,
                image=image,
                core=core,
                ram=ram,
                disk=disk,
                bandwidth=bandwidth,
                lease_start=lease_start,
                lease_end=lease_end,
                error=error
            )
            self.session.add(sliver)

        self.session.commit()
        return sliver.id

    def add_or_update_component(
        self, sliver_id: int, component_guid: str, component_type: str, model: str, bdfs: List[str]
    ) -> str:
        """
        Adds a Component if it doesn't exist, otherwise updates its fields.
        """
        component = self.session.query(Components).filter(
            Components.component_guid == component_guid, Components.sliver_id == sliver_id
        ).first()

        if component:
            if component_type:
                component.type = component_type
            if model:
                component.model = model
            if bdfs:
                component.bdfs = bdfs  # Store as JSON
        else:
            component = Components(
                sliver_id=sliver_id,
                component_guid=component_guid,
                type=component_type,
                model=model,
                bdfs=bdfs
            )
            self.session.add(component)

        self.session.commit()
        return component.component_guid

    def add_or_update_interface(self, sliver_id: int, interface_guid: str, vlan: str,
                                bdf: str, local_name: str, device_name: str, name: str) -> str:
        """
        Adds an Interface if it doesn't exist, otherwise updates its fields.
        """
        interface = self.session.query(Interfaces).filter(
            Interfaces.interface_guid == interface_guid, Interfaces.sliver_id == sliver_id
        ).first()

        if interface:
            if local_name:
                interface.local_name = local_name
            if vlan:
                interface.vlan = vlan
            if bdf:
                interface.bdf = bdf
            if device_name:
                interface.facility = device_name
            if name:
                interface.name = name
        else:
            interface = Interfaces(
                sliver_id=sliver_id,
                interface_guid=interface_guid,
                local_name=local_name,
                device_name=device_name,
                vlan=vlan,
                bdf=bdf,
                name=name
            )
            self.session.add(interface)

        self.session.commit()
        return interface.interface_guid

    # -------------------- ADD OR UPDATE HOST --------------------
    def add_or_update_host(self, host_name: str, site_id: int) -> int:
        """
        Adds a host if it doesn’t exist, otherwise updates the name.
        """
        host = self.session.query(Hosts).filter(Hosts.name == host_name).first()
        if not host:
            host = Hosts(name=host_name, site_id=site_id)
            self.session.add(host)

        self.session.commit()
        return host.id

    # -------------------- ADD OR UPDATE SITE --------------------
    def add_or_update_site(self, site_name: str) -> int:
        """
        Adds a site if it doesn’t exist, otherwise updates the name.
        """
        site = self.session.query(Sites).filter(Sites.name == site_name).first()
        if not site:
            site = Sites(name=site_name)
            self.session.add(site)

        self.session.commit()
        return site.id

    # -------------------- CLOSE SESSION --------------------
    def close(self):
        self.session.close()


# -------------------- EXAMPLE USAGE --------------------
if __name__ == "__main__":
    db = DatabaseManager(
        user="your_username",
        password="your_password",
        database="your_database",
        db_host="your_db_host:5432"  # Replace with actual Postgres host
    )

    # Add a project and a user
    project_id = db.add_or_update_project("1234-uuid", "Test Project")
    user_id = db.add_or_update_user("5678-uuid", "user@example.com")

    # Query all projects
    print("Projects:", db.get_all_projects())

    # Search users by email
    print("Users with email containing 'example':", db.get_users_by_email("example"))

    # Update a project name
    db.add_or_update_project(project_uuid="1234-uuid", project_name="Updated Project Name")

    # Delete a user
    db.delete_user(user_id)

    # Query slivers by timestamp range
    start_time = datetime(2025, 1, 1, 0, 0, 0)
    end_time = datetime(2025, 12, 31, 23, 59, 59)
    print("Slivers in time range:", db.get_slivers_in_time_range(start_time, end_time))

    # Close session
    db.close()
