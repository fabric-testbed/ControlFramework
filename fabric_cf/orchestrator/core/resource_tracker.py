from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging

from fabric_cf.actor.fim.fim_helper import FimHelper

from fim.slivers.base_sliver import BaseSliver

from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.kernel.reservation_states import ReservationStates
from fim.slivers.attached_components import ComponentSliver, ComponentType
from fim.slivers.capacities_labels import Capacities
from fim.slivers.network_node import NodeSliver

class TimeSlot:
    """Represents a time slot for resource availability, tracking capacities and components."""

    def __init__(self, end: datetime):
        """
        Initialize a TimeSlot instance with end time and empty resource capacities and components.

        :param end: The end datetime for this time slot.
        :type end: datetime
        """
        self.end = end
        self.available_capacities = Capacities()
        self.available_components = {}

    def __update_capacities(self, capacities: Capacities):
        """
        Update the available capacities in this time slot.

        :param capacities: The capacities to add to this time slot.
        :type capacities: Capacities
        """
        self.available_capacities += capacities

    def __update_components(self, by_type: dict[ComponentType, list[ComponentSliver]]):
        """
        Update the available components by type in this time slot.

        :param by_type: Dictionary with component types as keys and lists of ComponentSliver as values.
        :type by_type: dict[ComponentType, list[ComponentSliver]]
        """
        for comp_type, comps in by_type.items():
            if comp_type not in self.available_components:
                self.available_components[comp_type] = 0
            self.available_components[comp_type] += len(comps)

    def add_sliver(self, sliver: BaseSliver):
        """
        Add sliver capacities and components to the current time slot.

        :param sliver: The sliver containing resource capacities and components to add.
        :type sliver: BaseSliver
        """
        if isinstance(sliver, NodeSliver):
            if sliver.capacity_allocations:
                self.__update_capacities(capacities=sliver.capacity_allocations)
            else:
                self.__update_capacities(capacities=sliver.capacities)

            if sliver.attached_components_info:
                self.__update_components(by_type=sliver.attached_components_info.by_type)

    def __str__(self):
        """
        Return a string representation of the available capacities and components in this time slot.

        :return: String representation of available capacities and components.
        :rtype: str
        """
        return f"Capacities: {self.available_capacities}, Components: {self.available_components}"


class ResourceTracker:
    """Tracks resource availability over time slots and checks availability of resources."""

    def __init__(self, cbm_node: NodeSliver):
        """
        Initialize ResourceTracker with total capacities and components from a CBM node.

        :param cbm_node: The CBM node from which to initialize capacities and components.
        :type cbm_node: NodeSliver
        """
        _, self.total_capacities = FimHelper.get_delegations(delegations=cbm_node.get_capacity_delegations())
        self.total_components = {}

        if cbm_node.attached_components_info:
            for comp_type, comps in cbm_node.attached_components_info.by_type.items():
                if comp_type not in self.total_components:
                    self.total_components[comp_type] = 0
                self.total_components[comp_type] += len(comps)

        self.time_slots = defaultdict(TimeSlot)

    def add_sliver(self, end: datetime, sliver: BaseSliver):
        """
        Add sliver to the nearest hour time slot and update total available resources.

        :param end: The end datetime of the reservation for the sliver.
        :type end: datetime
        :param sliver: The sliver containing resources to add to the time slot.
        :type sliver: BaseSliver
        """
        nearest_hour = end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        if nearest_hour not in self.time_slots:
            self.time_slots[nearest_hour] = TimeSlot(nearest_hour)
        slot = self.time_slots[nearest_hour]
        slot.add_sliver(sliver=sliver)
        if sliver.capacity_allocations:
            self.total_capacities -= sliver.capacity_allocations
        else:
            self.total_capacities -= sliver.capacities

        if not sliver.attached_components_info:
            return

        for comp_type, comps in sliver.attached_components_info.by_type.items():
            self.total_components[comp_type] -= len(comps)

    @staticmethod
    def __check_components(requested_sliver: NodeSliver,
                           available_components: dict[ComponentType, int]) -> bool:
        """
        Check if requested components can be fulfilled by available components.

        :param requested_sliver: The sliver with requested components.
        :type requested_sliver: NodeSliver
        :param available_components: Dictionary of available components by type.
        :type available_components: dict[ComponentType, int]
        :return: True if components can be fulfilled, False otherwise.
        :rtype: bool
        """
        if not requested_sliver.attached_components_info:
            return True
        for comp_type, comps in requested_sliver.attached_components_info.by_type.items():
            if comp_type not in available_components:
                return False
            elif available_components[comp_type] < len(comps):
                return False
            else:
                available_components[comp_type] -= len(comps)
        return True

    def find_next_available(self, requested_sliver: NodeSliver,
                            from_time: datetime = datetime.now(timezone.utc)) -> datetime:
        """
        Find the next available time slot that can fulfill the requested sliver capacities and components.

        :param requested_sliver: The sliver with requested capacities and components.
        :type requested_sliver: NodeSliver
        :param from_time: The datetime from which to search for availability.
        :type from_time: datetime
        :return: The datetime of the next available time slot, or None if not found.
        :rtype: datetime
        """
        nearest_hour = from_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        if not (self.total_capacities - requested_sliver.capacities).negative_fields() and \
                self.__check_components(requested_sliver=requested_sliver,
                                        available_components=self.total_components):
            return nearest_hour

        sorted_times = sorted(self.time_slots.keys(), key=lambda x: abs(x - nearest_hour))

        accumulated_capacities = Capacities()
        accumulated_components = {}

        for closest_time in sorted_times:
            slot = self.time_slots[closest_time]
            accumulated_capacities += slot.available_capacities
            for comp_type, comp_count in slot.available_components.items():
                if comp_type not in accumulated_components:
                    accumulated_components[comp_type] = 0
                accumulated_components[comp_type] += comp_count

            if self.__check_components(requested_sliver, accumulated_components) and \
                    not (accumulated_capacities - requested_sliver.capacities).negative_fields():
                return closest_time


if __name__ == '__main__':
    from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton

    Globals.config_file = '/etc/fabric/actor/config/config.yaml'
    GlobalsSingleton.get().load_config()
    GlobalsSingleton.get().initialized = True

    # Configure logging and database connections
    logger = logging.getLogger("db-cli")
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])

    # Assuming connection to database and fetching reservations for testing purposes
    db = ActorDatabase(user="fabric", password="fabric", database="orchestrator", db_host="orchestrator-db:5432", logger=logger)
    states = [ReservationStates.Active.value, ReservationStates.ActiveTicketed.value, ReservationStates.Ticketed.value]
    existing = db.get_reservations(graph_node_id="GDXYNF3", site="WASH", states=states)

    tracker = ResourceTracker()

    # Add slivers from reservations to the tracker
    for e in existing:
        resource_sliver = e.get_approved_resources().get_sliver() if e.is_ticketing() else e.get_resources().get_sliver()
        if resource_sliver:
            tracker.add_sliver(sliver=resource_sliver, end=e.get_term().get_end_time())

    # Output time slots and search for availability
    for k in sorted(tracker.time_slots.keys()):
        print(f"Time: {k}, Resources: {tracker.time_slots[k]}")

    sliver = NodeSliver()
    sliver.set_capacities(cap=Capacities(core=15, ram=16, disk=10))
    curr = datetime.now(timezone.utc) + timedelta(hours=24)
    next_available_time = tracker.find_next_available(requested_sliver=sliver, from_time=curr)

    print(f"Next available time for requested resources starting from {curr}: {next_available_time}")
