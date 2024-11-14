from django.http import JsonResponse
from django.views import View
from datetime import datetime
from .models import Flight


class DelayedFlightsView(View):
    def get(self, request):
        try:
            # Fetch delayed departures and arrivals from the Flight model
            delayed_departures, delayed_arrivals = Flight.get_total_delayed_flights()

            # Standardized response structure
            response_data = {
                "status": "success",
                "message": "Delayed flights data retrieved successfully.",
                "data": {
                    "total_delayed_departures": delayed_departures,
                    "total_delayed_arrivals": delayed_arrivals,
                },
                "timestamp": datetime.now().isoformat(),  # Current timestamp in ISO format
            }

            return JsonResponse(
                response_data, status=200
            )  # 200 OK for successful request

        except Exception as e:
            # Handle potential errors, for example, if the query fails
            error_message = str(e)
            response_data = {
                "status": "error",
                "message": f"Failed to retrieve delayed flights data: {error_message}",
                "data": {},
                "timestamp": datetime.now().isoformat(),
            }

            return JsonResponse(
                response_data, status=500
            )  # 500 Internal Server Error for exceptions
