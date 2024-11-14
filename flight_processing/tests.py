import unittest
from pyspark.sql import SparkSession
from django.test import TestCase
from django.db import connections
from flight_processing.models import RawADSBData, RawOAGData, Airline, Airport, Flight
from flight_processing.processor import FlightDataProcessor
import pyspark.sql.functions as F


class FlightDataProcessorTestCase(TestCase):
    
    @classmethod
    def setUpTestData(cls):
        """Set up test data for the test case."""
        
        # Create a sample airline and airport data (use actual model creation)
        cls.airline = Airline.objects.create(iata_code='AA', icao_code='AAL')
        cls.departure_airport = Airport.objects.create(iata_code='JFK', icao_code='KJFK', name='John F. Kennedy International')
        cls.arrival_airport = Airport.objects.create(iata_code='LAX', icao_code='KLAX', name='Los Angeles International')
        
        # Create sample flight data
        cls.flight_data = {
            'flight_number': 'AA100',
            'airline': cls.airline,
            'departure_airport': cls.departure_airport,
            'arrival_airport': cls.arrival_airport,
            'departure_time': '2024-01-01 10:00:00',
            'arrival_time': '2024-01-01 13:00:00',
            'departure_delay': 0,
            'arrival_delay': 10,
            'flight_type': 'Domestic',
        }
        cls.flight = Flight.objects.create(**cls.flight_data)
        
        # Create sample RawADSBData and RawOAGData records in the Django database
        cls.raw_adsb = RawADSBData.objects.create(raw_data={'Flight': 'AA100', 'DepartureDelay': 0, 'ArrivalDelay': 10})
        cls.raw_oag = RawOAGData.objects.create(raw_data={'flightNumber': 'AA100', 'airlineIATA': 'AA', 'departureAirport': 'JFK', 'arrivalAirport': 'LAX'})

    @classmethod
    def setUpClass(cls):
        """Create a Spark session for the test case."""
        cls.spark = SparkSession.builder \
            .appName("FlightDataProcessorTest") \
            .master("local") \
            .getOrCreate()

    def setUp(self):
        """Setup before each test method."""
        # Clear any data from the test database if needed
        RawADSBData.objects.all().delete()
        RawOAGData.objects.all().delete()
        Airline.objects.all().delete()
        Airport.objects.all().delete()
        Flight.objects.all().delete()

    def test_process(self):
        """Test the process method of FlightDataProcessor."""
        
        # Create FlightDataProcessor with a real Spark session and paths (not used in this case)
        processor = FlightDataProcessor(self.spark, "path/to/adsb", "path/to/oag")

        # Mock the methods to simulate real behavior
        adsb_rdd = self.spark.sparkContext.parallelize([{'Flight': 'AA100', 'DepartureDelay': 0, 'ArrivalDelay': 10}])
        oag_rdd = self.spark.sparkContext.parallelize([{'flightNumber': 'AA100', 'airlineIATA': 'AA', 'departureAirport': 'JFK', 'arrivalAirport': 'LAX'}])

        # Create DataFrames from the RDDs
        adsb_df = self.spark.read.json(adsb_rdd)
        oag_df = self.spark.read.json(oag_rdd)

        # Ensure that the data frames have been created correctly
        self.assertEqual(adsb_df.count(), 1)
        self.assertEqual(oag_df.count(), 1)

        # Save the raw data to the database
        processor.save_raw_adsb_data(adsb_df.rdd)
        processor.save_raw_oag_data(oag_df.rdd)

        # Check that the data has been saved to the database
        self.assertEqual(RawADSBData.objects.count(), 1)
        self.assertEqual(RawOAGData.objects.count(), 1)

        # Join the DataFrames
        joined_df = processor.join_adsb_oag(adsb_df, oag_df)
        
        # Analyze delays
        delayed_flights = processor.analyze_delays(joined_df)
        
        # Assert that the number of delayed flights is correct
        self.assertEqual(delayed_flights, 1)  # Since we have one flight with a delay

        # Insert a flight (the one we created earlier)
        flight = processor.insert_flight(self.flight_data)
        
        # Assert that the flight was inserted correctly
        self.assertIsNotNone(flight)
        self.assertEqual(flight.flight_number, 'AA100')
        self.assertEqual(flight.departure_delay, 0)
        self.assertEqual(flight.arrival_delay, 10)

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session."""
        cls.spark.stop()
