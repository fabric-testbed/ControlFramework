#!/usr/bin/env python3
import argparse
import logging
import traceback
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from fabric_cf.actor.core.kernel.slice import SliceTypes, Slice
from fabric_cf.actor.core.plugins.db.actor_database import ActorDatabase
from fabric_cf.actor.core.container.globals import Globals, GlobalsSingleton
from reports.db_manager import DatabaseManager


LAST_EXPORT_FILE = "./last_export_time.txt"


class ExportScript:
    """
    CLI interface to fetch data from Postgres and insert it into the new SQLAlchemy database via DatabaseManager.
    """

    def __init__(self, src_user, src_password, src_db, src_host,
                 dest_user, dest_password, dest_db, dest_host, batch_size=1000):
        """
        Initializes connections to both source (Postgres) and destination (DatabaseManager).
        """
        self.logger = logging.getLogger("export")
        file_handler = RotatingFileHandler('./export.log', backupCount=5, maxBytes=50000)
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s [%(filename)s:%(lineno)d] [%(levelname)s] %(message)s",
                            handlers=[logging.StreamHandler(), file_handler])

        # Connect to the source Postgres database
        Globals.config_file = '/etc/fabric/actor/config/config.yaml'
        GlobalsSingleton.get().load_config()
        GlobalsSingleton.get().initialized = True

        self.src_db = ActorDatabase(user=src_user, password=src_password, database=src_db,
                                    db_host=src_host, logger=self.logger)

        # Initialize the destination database manager
        self.dest_db = DatabaseManager(user=dest_user, password=dest_password, database=dest_db,
                                       db_host=dest_host)

        self.batch_size = batch_size
        self.last_export_time = self.get_last_export_time()

    def get_last_export_time(self):
        """
        Reads the last export timestamp from a file.
        """
        if os.path.exists(LAST_EXPORT_FILE):
            with open(LAST_EXPORT_FILE, "r") as f:
                timestamp_str = f.read().strip()
                try:
                    return datetime.fromisoformat(timestamp_str)
                except ValueError:
                    self.logger.warning("Invalid timestamp in last export file, defaulting to epoch.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)  # Default to epoch

    def update_last_export_time(self, new_timestamp):
        """
        Updates the last export timestamp file with the current run time.
        """
        with open(LAST_EXPORT_FILE, "w") as f:
            f.write(new_timestamp.isoformat())

    def export(self):
        """
        Exports only the slices updated after the last execution timestamp.
        """
        try:
            self.logger.info(f"Starting export process... Last export was at {self.last_export_time}")

            offset = 0
            max_timestamp = self.last_export_time

            while True:
                self.logger.info(f"Fetching slices from offset {offset} (batch size: {self.batch_size})")
                slices = self.src_db.get_slices(offset=offset, limit=self.batch_size, slc_type=[SliceTypes.ClientSlice],
                                                updated_after=self.last_export_time)  # Fetch only updated slices

                if not slices:
                    self.logger.info("No more slices to process. Export complete.")
                    break  # Stop when no more slices are found

                for slice_object in slices:

                    slice_object = Slice()
                    try:
                        slice_updated_at = slice_object.get_last_updated_time()  # Get last update time
                        if slice_updated_at and slice_updated_at > max_timestamp:
                            max_timestamp = slice_updated_at  # Track latest timestamp

                        project_id = self.dest_db.add_or_update_project(project_uuid=slice_object.get_project_id(),
                                                                        project_name=slice_object.get_project_name())
                        user_id = self.dest_db.add_or_update_user(user_uuid=slice_object.get_owner().get_oidc_sub_claim(),
                                                                  user_email=slice_object.get_owner().get_email())

                        slice_id = self.dest_db.add_or_update_slice(project_id=project_id, user_id=user_id,
                                                                    slice_guid=str(slice_object.get_slice_id()),
                                                                    slice_name=slice_object.get_name(),
                                                                    state=slice_object.get_state().value,
                                                                    lease_start=slice_object.get_lease_start(),
                                                                    lease_end=slice_object.get_lease_end())

                    except Exception as slice_error:
                        self.logger.error(f"Error processing slice {slice_object.get_slice_id()}: {slice_error}")
                        traceback.print_exc()

                offset += self.batch_size  # Move to the next batch

            if max_timestamp > self.last_export_time:
                self.logger.info(f"Updating last export time to {max_timestamp}")
                self.update_last_export_time(max_timestamp)

            self.logger.info("Export process completed successfully!")

        except Exception as e:
            self.logger.error(f"Exception occurred during export: {e}")
            traceback.print_exc()

        finally:
            self.dest_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export data from Postgres to SQLAlchemy DB via DatabaseManager")
    parser.add_argument("--src_user", default="fabric", help="Source database username")
    parser.add_argument("--src_password", default="fabric", help="Source database password")
    parser.add_argument("--src_db", default="orchestrator", help="Source database name")
    parser.add_argument("--src_host", default="orchestrator-db:5432", help="Source database host")
    parser.add_argument("--dest_user", default="fabric", help="Destination database username")
    parser.add_argument("--dest_password", default="fabric", help="Destination database password")
    parser.add_argument("--dest_db", default="report", help="Destination database name")
    parser.add_argument("--dest_host", default="report-db:6432", help="Destination database host")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of slices to process per batch")

    args = parser.parse_args()

    exporter = ExportScript(
        src_user=args.src_user, src_password=args.src_password, src_db=args.src_db, src_host=args.src_host,
        dest_user=args.dest_user, dest_password=args.dest_password, dest_db=args.dest_db, dest_host=args.dest_host,
        batch_size=args.batch_size
    )

    exporter.export()
