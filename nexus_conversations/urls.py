"""
URL configuration for nexus_conversations project.

This is a minimal configuration for an internal microservice.
No external endpoints are exposed.
"""

from django.http import HttpResponse
from django.urls import path
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

from conversation_ms.views import ConversationViewSet

conversation_list = ConversationViewSet.as_view({"get": "list"})
conversation_detail = ConversationViewSet.as_view({"get": "retrieve"})

urlpatterns = [
    path("", lambda _: HttpResponse("Nexus Conversations Microservice")),
    path(
        "api/v1/projects/<uuid:project_uuid>/conversations/",
        conversation_list,
        name="project-conversations-list",
    ),
    path(
        "api/v1/projects/<uuid:project_uuid>/conversations/<uuid:pk>/",
        conversation_detail,
        name="project-conversations-detail",
    ),
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "api/docs/",
        SpectacularSwaggerView.as_view(url_name="schema"),
        name="swagger-ui",
    ),
    path(
        "api/redoc/",
        SpectacularRedocView.as_view(url_name="schema"),
        name="redoc",
    ),
]

