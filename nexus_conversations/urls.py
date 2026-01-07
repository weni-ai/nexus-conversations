"""
URL configuration for nexus_conversations project.

This is a minimal configuration for an internal microservice.
No external endpoints are exposed.
"""

from django.http import HttpResponse
from django.urls import path

urlpatterns = [
    path("", lambda _: HttpResponse("Nexus Conversations Microservice")),
]

