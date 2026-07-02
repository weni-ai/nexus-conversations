from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
import requests
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from conversation_ms.models import Project
from conversation_ms.permissions import PROJECT_AUTH_ROLES


@pytest.mark.django_db
class TestImprovementsProjectAuthorization:
    @pytest.fixture
    def api_client(self):
        return APIClient()

    @pytest.fixture
    def project(self):
        return Project.objects.create(name="Auth Project", timezone="UTC")

    def _list_url(self, project_uuid):
        return reverse("project-improvements-list", kwargs={"project_uuid": project_uuid})

    def _run_url(self, project_uuid):
        return reverse("project-improvements-run", kwargs={"project_uuid": project_uuid})

    def test_requires_project_authorization(self, api_client, project):
        response = api_client.get(self._list_url(project.uuid))
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("conversation_ms.permissions.requests.get")
    def test_moderator_can_post_run(self, mock_get, api_client, auth_headers, project, settings):
        settings.GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN = (
            "arn:aws:lambda:us-east-1:123456789012:function:conversations-count"
        )
        settings.CONVERSATIONS_IMPROVEMENTS_TRHESHOLD = 0

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["moderator"]}
        mock_get.return_value = mock_response

        with patch("improvements.views.start_conversations_improvements") as mock_task:
            response = api_client.post(self._run_url(project.uuid), {}, **auth_headers)

        assert response.status_code == status.HTTP_200_OK
        mock_task.delay.assert_called_once()

    @patch("conversation_ms.permissions.requests.get")
    def test_viewer_cannot_post_run(self, mock_get, api_client, auth_headers, project):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["viewer"]}
        mock_get.return_value = mock_response

        response = api_client.post(self._run_url(project.uuid), {}, **auth_headers)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("conversation_ms.permissions.requests.get")
    def test_contributor_can_get_list(self, mock_get, api_client, auth_headers, project):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["contributor"]}
        mock_get.return_value = mock_response

        response = api_client.get(self._list_url(project.uuid), **auth_headers)
        assert response.status_code == status.HTTP_200_OK

    @patch("conversation_ms.permissions.requests.get")
    def test_api_404_returns_forbidden(self, mock_get, api_client, auth_headers, project):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.ok = False
        mock_get.return_value = mock_response

        response = api_client.get(self._list_url(project.uuid), **auth_headers)
        assert response.status_code == status.HTTP_403_FORBIDDEN

    @patch("conversation_ms.permissions.requests.get")
    def test_api_timeout_returns_service_unavailable(self, mock_get, api_client, auth_headers, project):
        mock_get.side_effect = requests.Timeout("timeout")

        response = api_client.get(self._list_url(project.uuid), **auth_headers)
        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    @patch("conversation_ms.permissions.requests.get")
    def test_cross_project_authorization_is_checked_for_requested_uuid(
        self, mock_get, api_client, auth_headers, project
    ):
        other_uuid = uuid4()
        Project.objects.create(uuid=other_uuid, name="Other", timezone="UTC")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["viewer"]}
        mock_get.return_value = mock_response

        response = api_client.get(self._list_url(other_uuid), **auth_headers)
        assert response.status_code == status.HTTP_200_OK
        called_url = mock_get.call_args.args[0]
        assert str(other_uuid) in called_url
