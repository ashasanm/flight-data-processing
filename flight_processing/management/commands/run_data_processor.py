# flight_processing/management/commands/run_data_processor.py
from django.core.management.base import BaseCommand
from flight_processing.data_processor.oag_processor import OAGProcessor
from flight_processing.data_processor.adsb_processor import ADSBProcessor
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run the OAGProcessor to process flight data"

    def handle(self, *args, **kwargs):
        try:
            data_processors = [OAGProcessor, ADSBProcessor]
            for data_processor in data_processors:
                data_processor().process()

            self.stdout.write(self.style.SUCCESS("Successfully processed flight data"))
        except Exception as e:
            logger.error(f"Error during data processing: {e}")
            self.stdout.write(self.style.ERROR(f"Error: {e}"))
