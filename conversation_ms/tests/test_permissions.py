from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import requests as http_requests
from rest_framework.exceptions import ValidationError
from rest_framework.test import APIRequestFactory

from conversation_ms.exceptions import ProjectAuthorizationDenied
from conversation_ms.models import Project
from conversation_ms.permissions import (
    ProjectPermission,
    _check_project_authorization,
    _is_authorized_response,
    _user_email_from_authorization_payload,
    has_external_general_project_permission,
)

factory = APIRequestFactory()


class TestIsAuthorizedResponse:
    def test_200_is_authorized(self):
        resp = MagicMock(status_code=200)
        assert _is_authorized_response(resp) is True

    def test_non_200_is_not_authorized(self):
        for code in (401, 403, 404, 500):
            resp = MagicMock(status_code=code)
            assert _is_authorized_response(resp) is False


class TestUserEmailFromPayload:
    def test_top_level_email(self):
        assert _user_email_from_authorization_payload({"user_email": "a@b.com"}) == "a@b.com"

    def test_nested_email(self):
        data = {"user": {"email": "x@y.com"}}
        assert _user_email_from_authorization_payload(data) == "x@y.com"

    def test_top_level_takes_precedence(self):
        data = {"user_email": "top@a.com", "user": {"email": "nested@a.com"}}
        assert _user_email_from_authorization_payload(data) == "top@a.com"

    def test_missing_email_returns_none(self):
        assert _user_email_from_authorization_payload({}) is None


class TestCheckProjectAuthorization:
    @patch("conversation_ms.permissions.requests.get")
    def test_safe_method_any_role(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"project_authorization": 1, "user_email": "u@test.com"},
        )
        authorized, email = _check_project_authorization("Bearer tok", uuid4(), "GET")
        assert authorized is True
        assert email == "u@test.com"

    @patch("conversation_ms.permissions.requests.get")
    def test_moderator_allows_post(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"project_authorization": 3, "user_email": "mod@test.com"},
        )
        authorized, email = _check_project_authorization("Bearer tok", uuid4(), "POST")
        assert authorized is True

    @patch("conversation_ms.permissions.requests.get")
    def test_contributor_allows_post(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"project_authorization": 2, "user_email": "c@test.com"},
        )
        authorized, _ = _check_project_authorization("Bearer tok", uuid4(), "POST")
        assert authorized is True

    @patch("conversation_ms.permissions.requests.get")
    def test_viewer_denied_post(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"project_authorization": 1, "user_email": "v@test.com"},
        )
        with pytest.raises(ProjectAuthorizationDenied):
            _check_project_authorization("Bearer tok", uuid4(), "POST")

    @patch("conversation_ms.permissions.requests.get")
    def test_unauthorized_response_raises(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(status_code=403)
        with pytest.raises(ProjectAuthorizationDenied):
            _check_project_authorization("Bearer tok", uuid4(), "GET")


@pytest.mark.django_db
class TestHasExternalGeneralProjectPermission:
    @patch("conversation_ms.permissions.requests.get")
    def test_authorized_sets_email_on_request(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"project_authorization": 3, "user_email": "auth@test.com"},
        )
        request = factory.get("/", HTTP_AUTHORIZATION="Bearer tok")
        result = has_external_general_project_permission(request, uuid4(), "GET")
        assert result is True
        assert request.project_auth_user_email == "auth@test.com"

    @patch("conversation_ms.permissions.requests.get")
    def test_request_error_falls_back_to_local(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.side_effect = http_requests.ConnectionError("network down")
        project = Project.objects.create(name="Fallback Project")
        request = factory.get("/", HTTP_AUTHORIZATION="Bearer tok")
        request.user = MagicMock()
        result = has_external_general_project_permission(request, project.uuid, "GET")
        assert result is False

    @patch("conversation_ms.permissions.requests.get")
    def test_denied_falls_back_to_local(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(status_code=403)
        request = factory.get("/", HTTP_AUTHORIZATION="Bearer tok")
        request.user = MagicMock()
        result = has_external_general_project_permission(request, uuid4(), "GET")
        assert result is False

    @patch("conversation_ms.permissions.requests.get")
    def test_nonexistent_project_returns_false(self, mock_get, settings):
        settings.PROJECTS_API_BASE_URL = "https://api.test"
        mock_get.return_value = MagicMock(status_code=404)
        request = factory.get("/", HTTP_AUTHORIZATION="Bearer tok")
        request.user = MagicMock()
        result = has_external_general_project_permission(request, uuid4(), "GET")
        assert result is False


@pytest.mark.django_db
class TestProjectPermission:
    @pytest.fixture
    def permission(self):
        return ProjectPermission()

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_allows_when_authorized(self, mock_ext, permission):
        mock_ext.return_value = True
        project_uuid = uuid4()
        request = factory.get("/")
        view = MagicMock(kwargs={"project_uuid": str(project_uuid)})
        assert permission.has_permission(request, view) is True

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_denies_when_unauthorized(self, mock_ext, permission):
        mock_ext.return_value = False
        project_uuid = uuid4()
        request = factory.get("/")
        view = MagicMock(kwargs={"project_uuid": str(project_uuid)})
        assert permission.has_permission(request, view) is False

    def test_no_project_uuid_returns_false(self, permission):
        request = factory.get("/")
        view = MagicMock(kwargs={})
        assert permission.has_permission(request, view) is False

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_reads_project_uuid_from_query_params(self, mock_ext, permission):
        mock_ext.return_value = True
        project_uuid = str(uuid4())
        request = factory.get(f"/?project_uuid={project_uuid}")
        request.query_params = {"project_uuid": project_uuid}
        view = MagicMock(kwargs={})
        assert permission.has_permission(request, view) is True
        mock_ext.assert_called_once()

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_reads_project_from_request_data(self, mock_ext, permission):
        mock_ext.return_value = True
        project_uuid = str(uuid4())
        request = factory.post("/", data={"project": project_uuid}, format="json")
        request.data = {"project": project_uuid}
        view = MagicMock(kwargs={})
        assert permission.has_permission(request, view) is True

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_authorization_denied_returns_false(self, mock_ext, permission):
        mock_ext.side_effect = ProjectAuthorizationDenied("denied")
        project_uuid = uuid4()
        request = factory.get("/")
        view = MagicMock(kwargs={"project_uuid": str(project_uuid)})
        assert permission.has_permission(request, view) is False

    @patch("conversation_ms.permissions.has_external_general_project_permission")
    def test_unexpected_error_raises_validation_error(self, mock_ext, permission):
        mock_ext.side_effect = RuntimeError("unexpected")
        project_uuid = uuid4()
        request = factory.get("/")
        view = MagicMock(kwargs={"project_uuid": str(project_uuid)})
        with pytest.raises(ValidationError):
            permission.has_permission(request, view)
