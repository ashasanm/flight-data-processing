import logging
from datetime import datetime
from django.utils import timezone
from flight_processing.models import FlightTracking
from pyspark.sql import DataFrame, Row

from flight_processing.utils.data_processor.spark_processor import SparkProcessor
from flight_processing.utils.data_processor._base import BaseProcessor

logger = logging.getLogger(__name__)


class ADSBProcessor(BaseProcessor):
    def __init__(
        self,
        file_path: str = "flight_data/data/adsb_multi_aircraft.json",
        batch_size: int = 1000,
    ):
        self.file_path = file_path
        self.batch_size = batch_size

    def save_to_django(self, df: DataFrame) -> None:
        try:
            total_rows = df.count()  # Get total number of rows in the DataFrame
            logger.info(f"Total rows in DataFrame: {total_rows}")

            # Iterate over the DataFrame in batches
            for start in range(0, total_rows, self.batch_size):
                end = min(start + self.batch_size, total_rows)
                logger.info(f"Processing rows {start + 1} to {end}")

                # Take a batch of rows (take only the subset in each iteration)
                batch = df.limit(end).subtract(df.limit(start))

                # Collect the data from the batch
                rows = batch.collect()

                flight_tracking_objs = self._create_flight_tracking_in_batch(rows=rows)
                self._save_flight_tracking_in_batch(
                    flight_tracking_objs=flight_tracking_objs
                )

        except Exception as e:
            logger.error(f"Error processing DataFrame and saving to Django: {e}")
            raise e

    def process(self) -> None:
        try:
            logger.info(
                f"Processing ADS-B data from {self.file_path} at {datetime.now()}"
            )

            spark_processor = SparkProcessor()

            # TODO: Need to detect if there is changes in field name
            # TODO: Need add custom schema for spark data load
            # Load data into Spark
            df = spark_processor.load_json_data(self.file_path)

            # Save Raw data for future analytic
            self.save_to_django(df=df)
            logger.info(f"Processed ADS-B data and transformed at {datetime.now()}")

        except Exception as e:
            logger.error(f"Error processing ADS-B data: {e}")

    def _process_timestamp_last_update(self, last_update: int) -> datetime:
        """Convert Timestamp to Django UNIX timestmap compatible"""
        naive_datetime = datetime.fromtimestamp(last_update)  # This creates a naive datetime
        return timezone.make_aware(naive_datetime, timezone.get_current_timezone())

    def _save_flight_tracking_in_batch(
        self, flight_tracking_objs: list[FlightTracking]
    ) -> None:
        if flight_tracking_objs:
            try:
                FlightTracking.objects.bulk_create(flight_tracking_objs)
                logger.info(f"Successfully saved {len(flight_tracking_objs)} flights.")
            except Exception as e:
                logger.error(f"Error saving flight records: {e}")

    def _create_flight_tracking_in_batch(self, rows: list[Row]) -> list[FlightTracking]:
        """Create flight tracking object in a list"""
        try:
            flight_tracking_objs = []
            # Save the batch of data to Django
            for row in rows:
                data = {
                    "aircraft_id": row.AircraftId,
                    "registration": row.Registration,
                    "flight": row.Flight,
                    "callsign": row.Callsign,
                    "aircraft_type": row.Type,
                    "latitude": row.Latitude,
                    "longitude": row.Longitude,
                    "track": row.Track,
                    "altitude": row.Altitude,
                    "speed": row.Speed,
                    "vspeed": row.Vspeed,
                    "onground": row.Onground,
                    "squawk": row.Squawk,
                    "radar_id": row.RadarId,
                    "source_type": row.SourceType,
                    "origin": row.Origin,
                    "destination": row.Destination,
                    "last_update": self._process_timestamp_last_update(
                        last_update=row.LastUpdate
                    ),
                    "eta": row.ETA,
                }
                flight_tracking_objs.append(FlightTracking(**data))
            return flight_tracking_objs
        except Exception as e:
            logger.error(f"Error processing _create_flight_tracking_in_batch: {e}")
            raise e
