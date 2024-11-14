# myapp/tasks.py
from celery import shared_task
from flight_processing.spark_utils.spark_processor import SparkProcessor
from flight_processing.data_processor.adsb_processor import ADSBProcessor
from flight_processing.data_processor.oag_processor import OAGProcessor
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@shared_task
def process_adsb_json():
    try:
        data_processor = ADSBProcessor()
        data_processor.process()

    except Exception as e:
        logger.error(f"Error running task process_adsb_json | err: {e}")


@shared_task
def process_oag_json():
    try:
        data_processor = OAGProcessor()
        data_processor.process()

    except Exception as e:
        logger.error(f"Error running task process_oag_json | err: {e}")
