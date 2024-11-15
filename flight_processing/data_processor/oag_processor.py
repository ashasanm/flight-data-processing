import django
from pyspark.sql import SparkSession
from datetime import datetime
from django.db import IntegrityError, transaction
from pyspark.sql.functions import col, explode, unix_timestamp, when, to_timestamp
from pyspark.sql.types import FloatType
from flight_processing.models import Airline, Airport, Flight, FlightStatus
import logging
from flight_processing.spark_utils.spark_processor import SparkProcessor
from flight_processing.data_processor._base import BaseProcessor

# Initialize Django environment
django.setup()

# Set up logging
logger = logging.getLogger("flight_processing")


class OAGProcessor(BaseProcessor):
    def __init__(
        self,
        file_path: str = "flight_data/data/oag_multiple.json",
        batch_size: int = 1000,
    ):
        self.file_path = file_path
        self.batch_size = batch_size
        self.spark_processor = SparkProcessor()  # Initialize SparkProcessor once

    def process(self):
        logger.info(f"Processing ADS-B data from {self.file_path} at {datetime.now()}")

        # Load data into Spark
        df = self.spark_processor.load_json_data(self.file_path)

        # Flatten the data as per the previous logic
        df = self.flatten_data(df)
        df = self.filter_data(df)

        # Perform analysis to compute delays
        df_with_delays = self.calculate_delays(df)
        df_with_delays = self.get_is_delayed_from_status(df_with_delays)

        # Optionally, save processed data to database
        self.save_to_django(df_with_delays)

    def save_to_django(self, df_with_delays):
        """
        Save the calculated flight and status data into Django models (Flight and FlightStatus).
        """
        logger.info("Saving Processed Data..")

        # Define the lists to hold flight and flight status objects
        flight_objs = []
        flight_status_objs = []

        # Loop through the rows in the DataFrame (df_with_delays)
        for row in df_with_delays.collect():
            # Get or create related records (Airline, Airport)
            airline, departure_airport, arrival_airport = (
                self.get_or_create_airlines_and_airports(row)
            )

            # Parse datetime fields
            departure_time_utc = self.parse_datetime(
                row["departure_date_utc"], row["departure_time_utc"]
            )
            arrival_time_utc = self.parse_datetime(
                row["arrival_date_utc"], row["arrival_time_utc"]
            )

            # Skip if datetime parsing fails
            if not departure_time_utc or not arrival_time_utc:
                continue

            # Parse additional times with fallback checks
            departure_estimated_outGate = (
                self.parse_datetime(
                    row["departure_estimated_outGate"], "", "%Y-%m-%dT%H:%M:%S%z"
                )
                if row["departure_estimated_outGate"]
                else None
            )
            departure_actual_outGate = (
                self.parse_datetime(
                    row["departure_actual_outGate"], "", "%Y-%m-%dT%H:%M:%S%z"
                )
                if row["departure_actual_outGate"]
                else None
            )
            arrival_estimated_inGate = (
                self.parse_datetime(
                    row["arrival_estimated_inGate"], "", "%Y-%m-%dT%H:%M:%S%z"
                )
                if row["arrival_estimated_inGate"]
                else None
            )
            arrival_actual_inGate = (
                self.parse_datetime(
                    row["arrival_actual_inGate"], "", "%Y-%m-%dT%H:%M:%S%z"
                )
                if row["arrival_actual_inGate"]
                else None
            )

            # Create the Flight record
            flight = self.create_flight(
                row,
                airline,
                departure_airport,
                arrival_airport,
                departure_time_utc,
                arrival_time_utc,
                departure_actual_outGate,
                arrival_actual_inGate,
            )
            flight_objs.append(flight)

            # Create the FlightStatus record
            flight_status = self.create_flight_status(
                row,
                flight,
                departure_estimated_outGate,
                departure_actual_outGate,
                arrival_estimated_inGate,
                arrival_actual_inGate,
            )
            if flight_status:
                flight_status_objs.append(flight_status)

        # Bulk create the records in Django
        self.bulk_create_records(flight_objs, flight_status_objs)

        logger.info("Saving process completed.")

    def filter_data(self, df):
        df = df.filter(col("flightType") == "Scheduled")
        return df

    def flatten_data(self, df):
        """
        Flatten the JSON data into a Spark DataFrame.
        """
        logger.info(f"Flattening schema: {df.printSchema()}")

        df = df.withColumn("data", explode("data"))
        df = df.withColumn("statusDetail", explode("data.statusDetails"))

        return df.select(
            col("data.carrier.iata").alias("carrier_iata"),
            col("data.flightNumber").alias("flightNumber"),
            col("data.sequenceNumber").alias("sequenceNumber"),
            col("data.flightType"),
            col("data.departure.airport.iata").alias("departure_airport_iata"),
            col("data.departure.airport.icao").alias("departure_airport_icao"),
            col("data.departure.time.utc").alias("departure_time_utc"),
            col("data.departure.date.utc").alias("departure_date_utc"),
            col("data.arrival.airport.iata").alias("arrival_airport_iata"),
            col("data.arrival.airport.icao").alias("arrival_airport_icao"),
            col("data.arrival.time.utc").alias("arrival_time_utc"),
            col("data.arrival.date.utc").alias("arrival_date_utc"),
            col("data.elapsedTime"),
            col("data.serviceType").getField("iata").alias("service_type_iata"),
            col("statusDetail.equipment.actualAircraftType")
            .getField("iata")
            .alias("aircraft_type_iata"),
            col("statusDetail.equipment.actualAircraftType")
            .getField("icao")
            .alias("aircraft_type_icao"),
            col("statusDetail.state").alias("status_state"),
            col("statusDetail.updatedAt").alias("status_updated_at"),
            col("statusDetail.departure.estimatedTime.outGate.utc").alias(
                "departure_estimated_outGate"
            ),
            col("statusDetail.departure.actualTime.outGate.utc").alias(
                "departure_actual_outGate"
            ),
            col("statusDetail.departure.estimatedTime.outGateTimeliness").alias(
                "departure_estimated_outGate_status"
            ),
            col("statusDetail.departure.actualTime.outGateTimeliness").alias(
                "departure_actual_outGate_status"
            ),
            col("statusDetail.arrival.estimatedTime.inGate.utc").alias(
                "arrival_estimated_inGate"
            ),
            col("statusDetail.arrival.actualTime.inGate.utc").alias(
                "arrival_actual_inGate"
            ),
            col("statusDetail.arrival.actualTime.inGateTimeliness").alias(
                "arrival_actual_inGate_status"
            ),
            col("statusDetail.arrival.estimatedTime.inGateTimeliness").alias(
                "arrival_estimated_inGate_status"
            ),
        )

    def get_is_delayed_from_status(self, df_with_data):
        # Flags for Departure Delay
        df_with_delays = df_with_data.withColumn(
            "departure_is_delayed",
            when(
                col("departure_actual_outGate_status") == "Delayed",
                True,  # If delayed, set to True
            ).otherwise(
                False
            ),  # Otherwise, set to False (OnTime)
        )

        # Flags for Arrival Delay
        df_with_delays = df_with_delays.withColumn(
            "arrival_is_delayed",
            when(
                col("arrival_actual_inGate_status") == "Delayed",
                True,  # If delayed, set to True
            ).otherwise(
                False
            ),  # Otherwise, set to False (OnTime)
        )
        return df_with_delays

    def calculate_delays(self, df_with_data):
        """
        Calculates delays (departure and arrival) and returns the updated DataFrame with delay information.
        """
        # Parse the datetime strings to timestamp data type
        df_with_delays = df_with_data.withColumn(
            "departure_actual_outGate_ts",
            to_timestamp("departure_actual_outGate", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )

        df_with_delays = df_with_delays.withColumn(
            "departure_estimated_outGate_ts",
            to_timestamp("departure_estimated_outGate", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )

        df_with_delays = df_with_delays.withColumn(
            "arrival_actual_inGate_ts",
            to_timestamp("arrival_actual_inGate", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )

        df_with_delays = df_with_delays.withColumn(
            "arrival_estimated_inGate_ts",
            to_timestamp("arrival_estimated_inGate", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        )

        # Calculate the departure delay (in minutes)
        df_with_delays = df_with_delays.withColumn(
            "departure_delay",
            (
                unix_timestamp("departure_actual_outGate_ts")
                - unix_timestamp("departure_estimated_outGate_ts")
            )
            / 60,
        )

        # Calculate the arrival delay (in minutes)
        df_with_delays = df_with_delays.withColumn(
            "arrival_delay",
            (
                unix_timestamp("arrival_actual_inGate_ts")
                - unix_timestamp("arrival_estimated_inGate_ts")
            )
            / 60,
        )

        # Flags for Delay (Departure)
        df_with_delays = df_with_delays.withColumn(
            "departure_is_delayed",
            when(col("departure_delay") > 0, True).otherwise(False),
        )

        # Flags for Delay (Arrival)
        df_with_delays = df_with_delays.withColumn(
            "arrival_is_delayed",
            when(col("arrival_delay") > 0, True).otherwise(False),
        )

        # Flags for Early (Negative Delay)
        df_with_delays = df_with_delays.withColumn(
            "departure_is_early",
            when(col("departure_delay") < 0, True).otherwise(False),
        )

        df_with_delays = df_with_delays.withColumn(
            "arrival_is_early",
            when(col("arrival_delay") < 0, True).otherwise(False),
        )

        return df_with_delays

    def get_or_create_airlines_and_airports(self, row):
        """
        Fetch or create Airline and Airport objects based on the row data.
        """
        airline, _ = Airline.objects.get_or_create(iata_code=row["carrier_iata"])
        departure_airport, _ = Airport.objects.get_or_create(
            iata_code=row["departure_airport_iata"]
        )
        arrival_airport, _ = Airport.objects.get_or_create(
            iata_code=row["arrival_airport_iata"]
        )
        return airline, departure_airport, arrival_airport

    def parse_datetime(self, date_str, time_str, format="%Y-%m-%dT%H:%M"):
        """
        Parse a combined date and time string into a datetime object.
        """
        try:
            if date_str and time_str:
                fixed_datetime = f"{date_str}T{time_str}"
                return datetime.strptime(fixed_datetime, format)
            else:
                return datetime.strptime(date_str, format)
        except ValueError as e:
            logger.error(f"Invalid datetime format: {e} | {fixed_datetime}")
            return None

    def create_flight(
        self,
        row,
        airline,
        departure_airport,
        arrival_airport,
        departure_time_utc,
        arrival_time_utc,
        departure_actual_outGate,
        arrival_actual_inGate,
    ):
        """
        Create and return a Flight instance based on the row and related entities.
        """
        return Flight(
            flight_number=row["flightNumber"],
            airline=airline,
            departure_airport=departure_airport,
            arrival_airport=arrival_airport,
            estimated_departure_time=departure_time_utc,
            actual_departure_time=departure_actual_outGate,
            estimated_arrival_time=arrival_time_utc,
            actual_arrival_time=arrival_actual_inGate,
            departure_delay=row["departure_delay"],
            arrival_delay=row["arrival_delay"],
            flight_type=row["flightType"],
            departure_is_delayed=row["departure_is_delayed"],
            arrival_is_delayed=row["arrival_is_delayed"],
        )

    def create_flight_status(
        self,
        row,
        flight,
        departure_estimated_outGate,
        departure_actual_outGate,
        arrival_estimated_inGate,
        arrival_actual_inGate,
    ):
        """
        Create and return a FlightStatus instance based on the row and related Flight.
        """
        try:
            return FlightStatus(
                flight=flight,
                state=row["status_state"],
                updated_at=datetime.strptime(
                    row["status_updated_at"], "%Y-%m-%dT%H:%M:%S.%f"
                ),
                departure_estimated_out_gate=departure_estimated_outGate,
                departure_actual_out_gate=departure_actual_outGate,
                arrival_estimated_in_gate=arrival_estimated_inGate,
                arrival_actual_in_gate=arrival_actual_inGate,
            )
        except ValueError as e:
            logger.error(
                f"Invalid datetime format for status update for flight {row['flightNumber']} - {e}"
            )
            return None

    def bulk_create_records(self, flight_objs, flight_status_objs):
        """
        Perform bulk creation of Flight and FlightStatus records in Django.
        """
        if flight_objs:
            try:
                Flight.objects.bulk_create(flight_objs)
                logger.info(f"Successfully saved {len(flight_objs)} flights.")
            except Exception as e:
                logger.error(f"Error saving flight records: {e}")

        if flight_status_objs:
            try:
                FlightStatus.objects.bulk_create(flight_status_objs)
                logger.info(
                    f"Successfully saved {len(flight_status_objs)} flight statuses."
                )
            except Exception as e:
                logger.error(f"Error saving flight status records: {e}")
