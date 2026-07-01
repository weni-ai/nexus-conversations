import pytest
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Project

ALLOWED_ORIGIN = "https://allowed.example.com"
BLOCKED_ORIGIN = "https://evil.example.com"


@pytest.mark.django_db
class TestCorsAllowAllOrigins:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="CORS Project", timezone="UTC")

    def _topics_url(self, project_uuid):
        return reverse("topics", kwargs={"project_uuid": project_uuid})

    @override_settings(CORS_ALLOW_ALL_ORIGINS=True)
    def test_preflight_options_includes_authorization_header(self, api_client, project):
        response = api_client.options(
            self._topics_url(project.uuid),
            HTTP_ORIGIN="http://localhost:8081",
            HTTP_ACCESS_CONTROL_REQUEST_METHOD="GET",
            HTTP_ACCESS_CONTROL_REQUEST_HEADERS="authorization",
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.get("Access-Control-Allow-Origin") in ("*", "http://localhost:8081")
        allowed_headers = response.get("Access-Control-Allow-Headers", "").lower()
        assert "authorization" in allowed_headers

    @override_settings(CORS_ALLOW_ALL_ORIGINS=True)
    def test_get_with_origin_returns_cors_header(self, api_client, project):
        response = api_client.get(
            self._topics_url(project.uuid),
            HTTP_ORIGIN="http://localhost:8081",
            HTTP_AUTHORIZATION="Bearer test-jwt-token",
        )

        assert response.get("Access-Control-Allow-Origin") in ("*", "http://localhost:8081")


@pytest.mark.django_db
class TestCorsWhitelist:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="CORS Whitelist Project", timezone="UTC")

    def _topics_url(self, project_uuid):
        return reverse("topics", kwargs={"project_uuid": project_uuid})

    @override_settings(
        CORS_ALLOW_ALL_ORIGINS=False,
        CORS_ALLOWED_ORIGINS=[ALLOWED_ORIGIN],
    )
    def test_get_allowed_origin_returns_cors_header(self, api_client, project):
        response = api_client.get(
            self._topics_url(project.uuid),
            HTTP_ORIGIN=ALLOWED_ORIGIN,
            HTTP_AUTHORIZATION="Bearer test-jwt-token",
        )

        assert response["Access-Control-Allow-Origin"] == ALLOWED_ORIGIN

    @override_settings(
        CORS_ALLOW_ALL_ORIGINS=False,
        CORS_ALLOWED_ORIGINS=[ALLOWED_ORIGIN],
    )
    def test_get_blocked_origin_omits_cors_header(self, api_client, project):
        response = api_client.get(
            self._topics_url(project.uuid),
            HTTP_ORIGIN=BLOCKED_ORIGIN,
            HTTP_AUTHORIZATION="Bearer test-jwt-token",
        )

        assert response.get("Access-Control-Allow-Origin") != BLOCKED_ORIGIN
        assert "Access-Control-Allow-Origin" not in response

    @override_settings(
        CORS_ALLOW_ALL_ORIGINS=False,
        CORS_ALLOWED_ORIGINS=[ALLOWED_ORIGIN],
    )
    def test_error_response_includes_cors_header_for_allowed_origin(self, api_client, project):
        response = api_client.get(
            reverse("project-conversations-list", kwargs={"project_uuid": project.uuid}),
            HTTP_ORIGIN=ALLOWED_ORIGIN,
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert response["Access-Control-Allow-Origin"] == ALLOWED_ORIGIN
