from rest_framework import serializers

# Nested serializers for individual parts of the OAG data

class AirportSerializer(serializers.Serializer):
    iata = serializers.CharField(max_length=10)
    icao = serializers.CharField(max_length=10)

class DateTimeSerializer(serializers.Serializer):
    local = serializers.CharField()
    utc = serializers.CharField()

class TimeSerializer(serializers.Serializer):
    local = serializers.CharField()
    utc = serializers.CharField()

class FlightDetailSerializer(serializers.Serializer):
    carrier_iata = serializers.CharField(max_length=10)
    carrier_icao = serializers.CharField(max_length=10)
    service_suffix = serializers.CharField(required=False, allow_blank=True)
    flight_number = serializers.IntegerField()
    sequence_number = serializers.IntegerField()
    flight_type = serializers.CharField()
    departure_airport = AirportSerializer()
    departure_terminal = serializers.CharField()
    departure_date = DateTimeSerializer()
    departure_time = TimeSerializer()
    arrival_airport = AirportSerializer()
    arrival_terminal = serializers.CharField()
    arrival_date = DateTimeSerializer()
    arrival_time = TimeSerializer()
    elapsed_time = serializers.IntegerField()
    aircraft_type_iata = serializers.CharField(max_length=3)
    service_type_iata = serializers.CharField(max_length=1)

class OAGDataSerializer(serializers.Serializer):
    carrier = FlightDetailSerializer()
    status_key = serializers.CharField(max_length=100)
    schedule_instance_key = serializers.CharField(max_length=100)
    codeshare = serializers.DictField(required=False)
    elapsed_time = serializers.IntegerField()
    status_details = serializers.ListField(child=serializers.DictField(), required=False)
    
    def validate(self, data):
        # Add any custom validation if needed here (e.g., check if fields are correct)
        return data