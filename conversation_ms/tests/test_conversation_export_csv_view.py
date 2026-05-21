from datetime import date
from unittest.mock import patch

import pytest
from django.conf import settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Project


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def project():
    return Project.objects.create(name="Export Test", timezone="UTC")


@pytest.fixture
def auth_headers():
    token = "test-secret-token"
    settings.INTERNAL_API_TOKENS = {"TestTeam": token}
    return {"HTTP_AUTHORIZATION": f"Bearer {token}"}


@pytest.mark.django_db
class TestConversationExportCsvView:
    def test_requires_auth(self, api_client, project):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json")
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_token(self, api_client, project):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {},
            format="json",
            HTTP_AUTHORIZATION="Bearer wrong-token",
        )
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_project_not_found(self, api_client, auth_headers):
        from uuid import uuid4

        missing = uuid4()
        url = reverse("project-conversations-export", kwargs={"project_uuid": missing})
        response = api_client.post(url, {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_invalid_target_date_format(self, api_client, project, auth_headers):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {"target_date": "13-05-2026"},
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_invalid_calendar_date_returns_400(self, api_client, project, auth_headers):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {"target_date": "2026-99-99"},
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    @patch("conversation_ms.views.export_conversations_csv_bytes")
    def test_success_returns_csv_attachment(self, mock_export, api_client, project, auth_headers):
        header = "conversation_uuid,contact_urn\n"
        mock_export.return_value = (header.encode("utf-8"), 0, "2026-05-13")
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {"target_date": "2026-05-13"},
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response["Content-Type"] == "text/csv; charset=utf-8"
        assert "attachment" in response["Content-Disposition"]
        assert "conversations_2026-05-13.csv" in response["Content-Disposition"]
        assert response["X-Export-Row-Count"] == "0"
        assert response["X-Export-Target-Date"] == "2026-05-13"
        assert response.content == header.encode("utf-8")
        mock_export.assert_called_once_with(str(project.uuid), target_date=date(2026, 5, 13))

    @patch("conversation_ms.views.export_conversations_csv_bytes", side_effect=RuntimeError("boom"))
    def test_export_failure_returns_json_error(self, mock_export, api_client, project, auth_headers):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.data["error"] == "export_failed"
