import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

# Set up logging
logger = logging.getLogger(__name__)


class SparkProcessor:
    """
    A class to manage Spark session initialization, data loading, and error handling.
    """

    def __init__(self):
        """
        Initializes the SparkProcessor class, sets up the Spark session.
        """
        self.spark = None

    def get_spark_session(self) -> None:
        """
        Creates or retrieves the Spark session for the Django app.
        Configures Spark settings through Django settings.py if needed.
        """
        if not self.spark:
            try:
                logger.info("Initializing Spark session...")
                self.spark = (
                    SparkSession.builder.appName("FlightDataRealTimeETL")
                    .config("spark.some.config.option", settings.SPARK_CONFIG_OPTION)
                    .getOrCreate()
                )
                logger.info("Spark session created or retrieved successfully.")
            except Exception as e:
                logger.error(f"Error initializing Spark session: {e}")
                raise ImproperlyConfigured(f"Error initializing Spark session: {e}")
        return self.spark

    def load_json_data(self, file_path: str) -> DataFrame:
        """
        Loads a JSON file into a Spark DataFrame with error handling and corrupt record management.
        Handles bad records using permissive mode and logs all steps.
        """
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist.")
            raise FileNotFoundError(f"File {file_path} does not exist.")

        # Get the Spark session
        spark = self.get_spark_session()

        try:
            logger.info(f"Loading JSON data from {file_path} into DataFrame...")

            # Read JSON data into DataFrame with permissive mode and bad record handling
            df = (
                spark.read.option("mode", "PERMISSIVE")
                .option("badRecordsPath", settings.SPARK_BAD_RECORDS_PATH)
                .json(file_path, multiLine=True)
            )

            # Check if DataFrame is empty
            if df.isEmpty():
                logger.warning(f"The loaded DataFrame from {file_path} is empty.")
                raise ValueError(f"The loaded DataFrame from {file_path} is empty.")

            self.log_corrupt_records(df=df)

            logger.info(f"Successfully loaded JSON data from {file_path}.")
            return df

        except Exception as e:
            logger.error(f"Error loading JSON data from {file_path}: {e}")
            raise RuntimeError(f"Error loading JSON data from {file_path}: {e}")

    def log_corrupt_records(self, df: DataFrame):
        """
        Logs any corrupt records from the DataFrame by filtering for the '_corrupt_record' column.
        """
        # Check if there are corrupt records
        if "_corrupt_record" not in df.columns:
            return
        logger.error(f"Corrupt record column found")
        # Cache the DataFrame to ensure operations work with raw JSON
        df.cache()

        corrupt_df = df.filter(
            col("_corrupt_record").isNotNull()
        )  # Filter rows with corrupt records

        # Log the corrupt records
        if corrupt_df.count() > 0:
            logger.error(f"Corrupt records found: {corrupt_df.count()} rows.")
            corrupt_records = corrupt_df.select("_corrupt_record")
            corrupt_records.show(
                truncate=False
            )  # Show raw corrupt records (without truncating)
