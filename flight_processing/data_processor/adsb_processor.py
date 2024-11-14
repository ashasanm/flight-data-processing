from pyspark.sql import SparkSession
from datetime import datetime
from django.db import IntegrityError

import logging
from flight_processing.spark_utils.spark_processor import SparkProcessor

logger = logging.getLogger(__name__)


class ADSBProcessor:
    def __init__(
        self,
        file_path: str = "flight_data/data/adsb_multi_aircraft.json",
        batch_size: int = 1000,
    ):
        self.file_path = file_path
        self.batch_size = batch_size

    def save_raw_data(self, df):
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

                # Save the batch of data to Django
                for row in rows:
                    data = {
                        "AircraftId": row.AircraftId,
                        "Latitude": row.Latitude,
                        "Longitude": row.Longitude,
                        "Track": row.Track,
                        "Altitude": row.Altitude,
                        "Speed": row.Speed,
                        "Squawk": row.Squawk,
                        "Type": row.Type,
                        "Registration": row.Registration,
                        "LastUpdate": row.LastUpdate,
                        "Origin": row.Origin,
                        "Destination": row.Destination,
                        "Flight": row.Flight,
                        "Onground": row.Onground,
                        "Vspeed": row.Vspeed,
                        "Callsign": row.Callsign,
                        "SourceType": row.SourceType,
                        "ETA": row.ETA,
                        "raw_data": row.asDict(),  # Store the entire row as JSON
                    }
                    logger.info(f"\nRAW DATA: {data}\n")
                    # # Create and save the RawADSBData instance
                    # try:
                    #     # Save to Django model
                    #     RawADSBData.objects.create(**data)
                    #     logger.info(
                    #         f"Saved ADS-B data for flight {row.Flight} (AircraftId: {row.AircraftId})"
                    #     )
                    # except IntegrityError as e:
                    #     logger.error(f"Error saving data for flight {row.Flight}: {e}")

        except Exception as e:
            logger.error(f"Error processing DataFrame and saving to Django: {e}")

    def process(self):
        try:
            logger.info(
                f"Processing ADS-B data from {self.file_path} at {datetime.now()}"
            )

            spark_processor = SparkProcessor()

            # Load data into Spark
            df = spark_processor.load_json_data(self.file_path)

            # Save Raw data for future analytic
            # self.save_raw_data(df=df)
            logger.info(f"Processed ADS-B data and transformed at {datetime.now()}")

        except Exception as e:
            logger.error(f"Error processing ADS-B data: {e}")
