"""
URL configuration for nexus_conversations project.

This is a minimal configuration for an internal microservice.
No external endpoints are exposed.
"""

from django.contrib import admin
from django.http import HttpResponse
from django.urls import include, path
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView

from conversation_ms.views import ConversationViewSet, TopicsViewSet, SubTopicsViewSet

conversation_list = ConversationViewSet.as_view({"get": "list"})
conversation_detail = ConversationViewSet.as_view({"get": "retrieve"})


urlpatterns = [
    path("admin/", admin.site.urls),
    path("", lambda _: HttpResponse("Nexus Conversations Microservice")),
    path("api/v1/", include("conversation_ms.urls")),
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
    path("api/v1/projects/<uuid:project_uuid>/topics/", TopicsViewSet.as_view({"get": "list", "post": "create"}), name="topics"),
    path(
        "api/v1/projects/<uuid:project_uuid>/topics/<uuid:uuid>/",
        TopicsViewSet.as_view({"get": "retrieve", "put": "update", "delete": "destroy"}),
        name="topic-detail",
    ),
    path(
        "api/v1/projects/<uuid:project_uuid>/topics/<uuid:topic_uuid>/subtopics/",
        SubTopicsViewSet.as_view({"get": "list", "post": "create"}),
        name="subtopics",
    ),
    path(
        "api/v1/projects/<uuid:project_uuid>/topics/<uuid:topic_uuid>/subtopics/<uuid:uuid>/",
        SubTopicsViewSet.as_view({"get": "retrieve", "put": "update", "delete": "destroy"}),
        name="subtopic-detail",
    ),
]