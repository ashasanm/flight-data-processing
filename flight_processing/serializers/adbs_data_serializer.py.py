from rest_framework import serializers

class ADSBDataSerializer(serializers.Serializer):
    AircraftId = serializers.CharField(max_length=10)
    Latitude = serializers.FloatField()
    Longitude = serializers.FloatField()
    Track = serializers.FloatField()
    Altitude = serializers.FloatField()
    Speed = serializers.FloatField()
    Squawk = serializers.IntegerField()
    Type = serializers.CharField(max_length=20)
    Registration = serializers.CharField(max_length=20)
    LastUpdate = serializers.IntegerField()
    Origin = serializers.CharField(max_length=3)
    Destination = serializers.CharField(max_length=3)
    Flight = serializers.CharField(max_length=20)
    Onground = serializers.IntegerField()
    Vspeed = serializers.FloatField()
    Callsign = serializers.CharField(max_length=20)
    SourceType = serializers.CharField(max_length=100)
    ETA = serializers.IntegerField()

    def validate(self, data):
        # Add any custom validation if needed here (e.g., check if fields are correct)
        return data