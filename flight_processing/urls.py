# urls.py
from django.urls import path
from .views import DelayedFlightsView

urlpatterns = [
    path('delayed-flights/', DelayedFlightsView.as_view(), name='delayed_flights'),
]
