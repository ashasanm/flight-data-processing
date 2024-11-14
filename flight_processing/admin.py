from django.contrib import admin
from .models import Airline, Airport, Flight, FlightStatus


# Airline Model Admin
class AirlineAdmin(admin.ModelAdmin):
    list_display = ('iata_code', 'icao_code', 'faa_code', 'name')
    search_fields = ('iata_code', 'icao_code', 'name')
    list_filter = ('name',)


# Airport Model Admin
class AirportAdmin(admin.ModelAdmin):
    list_display = ('iata_code', 'icao_code', 'faa_code', 'name')
    search_fields = ('iata_code', 'icao_code', 'name')
    list_filter = ('name',)


# Flight Model Admin
class FlightAdmin(admin.ModelAdmin):
    list_display = (
        'flight_number',
        'get_airline_name',  # Show the airline's name
        'departure_airport',
        'arrival_airport',
        'get_departure_time',  # Formatted datetime
        'get_arrival_time',    # Formatted datetime
        'departure_delay',
        'arrival_delay',
        'flight_type'
    )
    search_fields = ('flight_number', 'airline__name', 'departure_airport__iata_code', 'arrival_airport__iata_code')
    list_filter = ('airline__name', 'departure_airport', 'arrival_airport', 'flight_type')  # Use 'airline__name'
    list_editable = ('departure_delay', 'arrival_delay')

    def get_airline_name(self, obj):
        return obj.airline.name if obj.airline else "N/A"
    get_airline_name.short_description = 'Airline'

    def get_departure_time(self, obj):
        return obj.estimated_departure_time.strftime("%Y-%m-%d %H:%M:%S") if obj.estimated_departure_time else None
    get_departure_time.short_description = 'Estimated Departure'

    def get_arrival_time(self, obj):
        return obj.estimated_arrival_time.strftime("%Y-%m-%d %H:%M:%S") if obj.estimated_arrival_time else None
    get_arrival_time.short_description = 'Estimated Arrival'


# FlightStatus Model Admin
class FlightStatusAdmin(admin.ModelAdmin):
    list_display = (
        'flight',
        'state',
        'updated_at',
        'get_departure_delay',
        'get_arrival_delay'
    )
    search_fields = ('flight__flight_number', 'state',)
    list_filter = ('state', 'flight__airline__name')  # Use 'flight__airline__name' for filtering by airline's name

    def get_departure_delay(self, obj):
        return obj.flight.departure_delay if obj.flight else None
    get_departure_delay.short_description = 'Departure Delay (minutes)'

    def get_arrival_delay(self, obj):
        return obj.flight.arrival_delay if obj.flight else None
    get_arrival_delay.short_description = 'Arrival Delay (minutes)'


# Register models with the admin site
admin.site.register(Airline, AirlineAdmin)
admin.site.register(Airport, AirportAdmin)
admin.site.register(Flight, FlightAdmin)
admin.site.register(FlightStatus, FlightStatusAdmin)
