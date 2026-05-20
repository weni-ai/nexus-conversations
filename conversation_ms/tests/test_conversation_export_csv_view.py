from unittest.mock import patch
from uuid import uuid4

import pytest
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
def project_uuid(project):
    return str(project.uuid)


@pytest.fixture
def _bypass_jwt(project_uuid):
    fake_payload = {"project_uuid": project_uuid}

    with patch(
        "conversation_ms.api.internal.jwt_authenticators.JWTModuleAuthentication.authenticate",
        return_value=(None, fake_payload),
    ):
        yield


@pytest.fixture
def auth_headers():
    return {"HTTP_AUTHORIZATION": "Bearer fake-jwt-token"}


@pytest.mark.django_db
class TestConversationExportCsvView:
    @pytest.fixture(autouse=True)
    def _celery_eager(self, settings):
        settings.CELERY_TASK_ALWAYS_EAGER = True
        settings.CELERY_TASK_EAGER_PROPAGATES = True
        settings.AWS_S3_BUCKET_NAME = "test-bucket"

    def test_requires_auth(self, api_client, project):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_project_not_found(self, api_client, _bypass_jwt, auth_headers):
        missing = uuid4()
        with patch(
            "conversation_ms.api.internal.jwt_authenticators.JWTModuleAuthentication.authenticate",
            return_value=(None, {"project_uuid": str(missing)}),
        ):
            url = reverse("project-conversations-export", kwargs={"project_uuid": missing})
            response = api_client.post(url, {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_forbidden_when_jwt_project_mismatch(self, api_client, project, auth_headers):
        other = uuid4()
        with patch(
            "conversation_ms.api.internal.jwt_authenticators.JWTModuleAuthentication.authenticate",
            return_value=(None, {"project_uuid": str(other)}),
        ):
            url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
            response = api_client.post(url, {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_target_date_format(self, api_client, project, _bypass_jwt, auth_headers):
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {"target_date": "13-05-2026"},
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    @patch("conversation_ms.services.conversation_csv_export_runner.run_conversation_csv_export")
    def test_success_returns_presigned_url(self, mock_run, api_client, project, _bypass_jwt, auth_headers):
        mock_run.return_value = {
            "download_url": "https://example.com/presigned.csv",
            "row_count": 3,
            "target_date": "2026-05-13",
            "expires_in": 3600,
        }
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(
            url,
            {"target_date": "2026-05-13"},
            format="json",
            **auth_headers,
        )
        assert response.status_code == status.HTTP_200_OK
        assert response.data["download_url"] == "https://example.com/presigned.csv"
        assert response.data["row_count"] == 3
        assert response.data["target_date"] == "2026-05-13"
        mock_run.assert_called_once_with(str(project.uuid), target_date="2026-05-13")

    @patch("conversation_ms.services.conversation_csv_export_runner.run_conversation_csv_export")
    def test_s3_not_configured_returns_503(self, mock_run, api_client, project, _bypass_jwt, auth_headers):
        from conversation_ms.adapters.s3_export import ConversationExportS3Error

        mock_run.side_effect = ConversationExportS3Error("AWS_S3_BUCKET_NAME is not configured")
        url = reverse("project-conversations-export", kwargs={"project_uuid": project.uuid})
        response = api_client.post(url, {}, format="json", **auth_headers)
        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert response.data["error"] == "export_storage_not_configured"
