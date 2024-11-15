from django.db import models
from django.utils import timezone


# Airline Model (keeps info about the airline)
class Airline(models.Model):
    objects = models.Manager()

    iata_code = models.CharField(max_length=10, blank=True, null=True, unique=True)
    icao_code = models.CharField(max_length=10, blank=True, null=True, unique=True)
    faa_code = models.CharField(max_length=10, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"{self.iata_code} - {self.icao_code}"

    class Meta:
        db_table = "airlines"  # Custom table name


# Airport Model (keeps info about the airport)
class Airport(models.Model):
    objects = models.Manager()

    iata_code = models.CharField(max_length=10, blank=True, null=True, unique=True)
    icao_code = models.CharField(max_length=10, blank=True, null=True, unique=True)
    faa_code = models.CharField(max_length=10, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return f"{self.name} - {self.iata_code} - {self.icao_code}"

    class Meta:
        db_table = "airports"  # Custom table name


# Flight Model (stores main flight data)
class Flight(models.Model):
    objects = models.Manager()

    flight_number = models.CharField(max_length=10)
    airline = models.ForeignKey(Airline, on_delete=models.CASCADE)
    departure_airport = models.ForeignKey(
        Airport, related_name="departure_airport", on_delete=models.CASCADE
    )
    arrival_airport = models.ForeignKey(
        Airport, related_name="arrival_airport", on_delete=models.CASCADE
    )
    # Estimated and Actual Times for Departure and Arrival
    estimated_departure_time = models.DateTimeField(null=True, blank=True)
    actual_departure_time = models.DateTimeField(null=True, blank=True)
    estimated_arrival_time = models.DateTimeField(null=True, blank=True)
    actual_arrival_time = models.DateTimeField(null=True, blank=True)

    # Delays (in minutes)
    departure_delay = models.IntegerField(default=0, null=True, blank=True)
    arrival_delay = models.IntegerField(default=0, null=True, blank=True)

    # Flags for Delay
    departure_is_delayed = models.BooleanField(default=False)
    arrival_is_delayed = models.BooleanField(default=False)

    # Flight type (Scheduled, etc.)
    flight_type = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.flight_number} - {self.airline}"

    @classmethod
    def get_total_delayed_flights(cls):
        """Returns the total count of flights that are delayed either for departure or arrival."""
        # Count flights that are delayed either in departure or arrival
        total_delayed_departure = cls.objects.filter(departure_is_delayed=True).count()
        total_delayed_arrival = cls.objects.filter(arrival_is_delayed=True).count()

        # Return total count of delayed departures and arrivals
        return total_delayed_departure, total_delayed_arrival

    def calculate_delays(self):
        """Calculate the delays based on actual vs estimated times and set delay flags."""
        if self.actual_departure_time and self.estimated_departure_time:
            departure_delay = (
                self.actual_departure_time - self.estimated_departure_time
            ).total_seconds() / 60
            self.departure_delay = max(0, int(departure_delay))
            self.departure_is_delayed = self.departure_delay > 0
        else:
            self.departure_delay = 0
            self.departure_is_delayed = False

        if self.actual_arrival_time and self.estimated_arrival_time:
            arrival_delay = (
                self.actual_arrival_time - self.estimated_arrival_time
            ).total_seconds() / 60
            self.arrival_delay = max(0, int(arrival_delay))
            self.arrival_is_delayed = self.arrival_delay > 0
        else:
            self.arrival_delay = 0
            self.arrival_is_delayed = False

        self.save()

    class Meta:
        db_table = "flights"  # Custom table name


# FlightStatus Model (keeps status updates and timing details for the flight)
class FlightStatus(models.Model):
    objects = models.Manager()

    flight = models.ForeignKey(Flight, on_delete=models.CASCADE)
    state = models.CharField(max_length=50)  # e.g., "InGate", "Landed"
    updated_at = models.DateTimeField(default=timezone.now)

    # Departure-related info
    departure_estimated_out_gate = models.DateTimeField(null=True, blank=True)
    departure_actual_out_gate = models.DateTimeField(null=True, blank=True)
    departure_gate = models.CharField(max_length=20, blank=True, null=True)

    # Arrival-related info
    arrival_estimated_in_gate = models.DateTimeField(null=True, blank=True)
    arrival_actual_in_gate = models.DateTimeField(null=True, blank=True)
    arrival_gate = models.CharField(max_length=20, blank=True, null=True)

    # Aircraft info
    aircraft_registration_number = models.CharField(
        max_length=50, blank=True, null=True
    )
    actual_aircraft_type_iata = models.CharField(max_length=10, blank=True, null=True)

    def __str__(self):
        return f"Flight {self.flight.flight_number} - Status: {self.state}"

    def update_status(
        self,
        state,
        departure_estimated_time=None,
        departure_actual_time=None,
        arrival_estimated_time=None,
        arrival_actual_time=None,
    ):
        """Updates status information with timing details."""
        self.state = state
        if departure_estimated_time:
            self.departure_estimated_out_gate = departure_estimated_time
        if departure_actual_time:
            self.departure_actual_out_gate = departure_actual_time
        if arrival_estimated_time:
            self.arrival_estimated_in_gate = arrival_estimated_time
        if arrival_actual_time:
            self.arrival_actual_in_gate = arrival_actual_time
        self.updated_at = timezone.now()
        self.save()

    class Meta:
        db_table = "flight_statuses"  # Custom table name


class FlightTracking(models.Model):
    # Aircraft and flight details
    aircraft_id = models.CharField(max_length=10)  # AircraftId
    registration = models.CharField(max_length=20)  # Registration
    flight = models.CharField(max_length=10)
    callsign = models.CharField(max_length=10)
    aircraft_type = models.CharField(max_length=20)  # New field for aircraft type

    # Geographic information
    latitude = models.DecimalField(max_digits=9, decimal_places=6)
    longitude = models.DecimalField(max_digits=9, decimal_places=6)
    track = models.IntegerField()

    # Flight data
    altitude = models.IntegerField()
    speed = models.IntegerField()
    vspeed = models.IntegerField()  # Vertical speed (climb or descent)

    # Aircraft status
    onground = models.BooleanField()
    squawk = models.IntegerField()  # Transponder code

    # Radar and source information
    radar_id = models.CharField(max_length=10, blank=True, null=True)
    source_type = models.CharField(max_length=50)

    # Flight route
    origin = models.CharField(max_length=3)  # IATA code for origin
    destination = models.CharField(max_length=3)  # IATA code for destination

    # Flight ETA and update timestamps
    eta = models.IntegerField()  # ETA (could be a timestamp)
    last_update = models.DateTimeField()  # Last update timestamp

    class Meta:
        db_table = "flight_trackings"  # Custom table name
