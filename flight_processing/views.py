from django.http import JsonResponse
from django.views import View
from .models import Flight

class DelayedFlightsView(View):
    def get(self, request):
        delayed_departures, delayed_arrivals = Flight.get_total_delayed_flights()
        return JsonResponse({
            'total_delayed_departures': delayed_departures,
            'total_delayed_arrivals': delayed_arrivals
        })