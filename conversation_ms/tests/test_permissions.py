from unittest.mock import Mock, patch

import pytest
import requests
from django.test import RequestFactory
from rest_framework.exceptions import APIException

from conversation_ms.api.permissions import ProjectPermission
from conversation_ms.permissions import (
    ARCHIVE_READ_ROLES,
    PROJECT_AUTH_ROLES,
    ProjectAuthNotFound,
    ProjectAuthorizationDenied,
    _check_project_authorization,
    _is_archive_read_role,
    _is_role_authorized_for_method,
    _user_email_from_authorization_payload,
    has_archive_read_project_permission,
    has_external_project_permission,
)


class TestUserEmailFromAuthorizationPayload:
    def test_returns_string_user(self):
        assert _user_email_from_authorization_payload({"user": "user@example.com"}) == "user@example.com"

    def test_returns_email_from_dict_user(self):
        payload = {"user": {"email": "user@example.com"}}
        assert _user_email_from_authorization_payload(payload) == "user@example.com"

    def test_returns_none_for_missing_user(self):
        assert _user_email_from_authorization_payload({}) is None


class TestRoleAuthorizationForMethod:
    @pytest.mark.parametrize(
        ("role", "method", "expected"),
        [
            (PROJECT_AUTH_ROLES["viewer"], "GET", True),
            (PROJECT_AUTH_ROLES["viewer"], "POST", False),
            (PROJECT_AUTH_ROLES["contributor"], "GET", True),
            (PROJECT_AUTH_ROLES["contributor"], "POST", True),
            (PROJECT_AUTH_ROLES["contributor"], "DELETE", True),
            (PROJECT_AUTH_ROLES["moderator"], "DELETE", True),
            (PROJECT_AUTH_ROLES["support"], "GET", True),
            (PROJECT_AUTH_ROLES["support"], "PATCH", False),
            (PROJECT_AUTH_ROLES["chat_user"], "HEAD", True),
            (PROJECT_AUTH_ROLES["chat_user"], "PUT", False),
            (PROJECT_AUTH_ROLES["not_set"], "GET", False),
        ],
    )
    def test_role_method_matrix(self, role, method, expected):
        assert _is_role_authorized_for_method(role, method) is expected


class TestArchiveReadRole:
    def test_archive_read_roles_constant(self):
        assert ARCHIVE_READ_ROLES == frozenset({PROJECT_AUTH_ROLES["moderator"], PROJECT_AUTH_ROLES["support"]})

    @pytest.mark.parametrize(
        ("role", "method", "expected"),
        [
            (PROJECT_AUTH_ROLES["moderator"], "GET", True),
            (PROJECT_AUTH_ROLES["support"], "GET", True),
            (PROJECT_AUTH_ROLES["support"], "HEAD", True),
            (PROJECT_AUTH_ROLES["viewer"], "GET", False),
            (PROJECT_AUTH_ROLES["contributor"], "GET", False),
            (PROJECT_AUTH_ROLES["chat_user"], "GET", False),
            (PROJECT_AUTH_ROLES["support"], "POST", False),
            (PROJECT_AUTH_ROLES["moderator"], "DELETE", False),
            (PROJECT_AUTH_ROLES["not_set"], "GET", False),
        ],
    )
    def test_archive_read_matrix(self, role, method, expected):
        assert _is_archive_read_role(role, method) is expected


@pytest.mark.django_db
class TestCheckProjectAuthorization:
    @pytest.fixture(autouse=True)
    def _configure_settings(self, settings):
        settings.PROJECTS_API_BASE_URL = "https://project-auth.example.com"

    def _mock_response(self, *, status_code=200, payload=None):
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.ok = status_code == 200
        if payload is not None:
            mock_response.json.return_value = payload
        return mock_response

    @patch("conversation_ms.permissions.requests.get")
    def test_moderator_allows_post(self, mock_get):
        mock_get.return_value = self._mock_response(
            payload={"project_authorization": PROJECT_AUTH_ROLES["moderator"], "user": "mod@example.com"},
        )

        authorized, email = _check_project_authorization(
            "Bearer token",
            "11111111-1111-1111-1111-111111111111",
            "POST",
        )

        assert authorized is True
        assert email == "mod@example.com"
        mock_get.assert_called_once_with(
            "https://project-auth.example.com/v2/projects/11111111-1111-1111-1111-111111111111/authorization",
            headers={"Authorization": "Bearer token"},
            timeout=5,
        )

    @patch("conversation_ms.permissions.requests.get")
    def test_contributor_allows_post(self, mock_get):
        mock_get.return_value = self._mock_response(
            payload={"project_authorization": PROJECT_AUTH_ROLES["contributor"]},
        )

        authorized, _ = _check_project_authorization("Bearer token", "uuid", "POST")
        assert authorized is True

    @patch("conversation_ms.permissions.requests.get")
    def test_viewer_denies_post(self, mock_get):
        mock_get.return_value = self._mock_response(
            payload={"project_authorization": PROJECT_AUTH_ROLES["viewer"]},
        )

        with pytest.raises(ProjectAuthorizationDenied):
            _check_project_authorization("Bearer token", "uuid", "POST")

    @patch("conversation_ms.permissions.requests.get")
    def test_raises_not_found_on_404(self, mock_get):
        mock_get.return_value = self._mock_response(status_code=404)

        with pytest.raises(ProjectAuthNotFound):
            _check_project_authorization("Bearer token", "uuid", "GET")

    @patch("conversation_ms.permissions.requests.get")
    def test_raises_denied_on_403(self, mock_get):
        mock_get.return_value = self._mock_response(status_code=403)

        with pytest.raises(ProjectAuthorizationDenied):
            _check_project_authorization("Bearer token", "uuid", "GET")

    @patch("conversation_ms.permissions.requests.get")
    def test_raises_unavailable_on_timeout(self, mock_get):
        mock_get.side_effect = requests.Timeout("timeout")

        with pytest.raises(requests.Timeout):
            _check_project_authorization("Bearer token", "uuid", "GET")


@pytest.mark.django_db
class TestHasExternalProjectPermission:
    @pytest.fixture(autouse=True)
    def _configure_settings(self, settings):
        settings.PROJECTS_API_BASE_URL = "https://project-auth.example.com"

    def _request(self, *, method="GET", authorization="Bearer test-jwt-token"):
        request = RequestFactory().get("/")
        request.method = method
        if authorization is not None:
            request.META["HTTP_AUTHORIZATION"] = authorization
        return request

    @patch("conversation_ms.permissions.requests.get")
    def test_sets_user_email_on_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {
            "project_authorization": PROJECT_AUTH_ROLES["viewer"],
            "user": "user@example.com",
        }
        mock_get.return_value = mock_response

        request = self._request()
        assert has_external_project_permission(request, "uuid", "GET") is True
        assert request.project_auth_user_email == "user@example.com"

    def test_returns_false_without_authorization_header(self):
        request = self._request(authorization=None)
        assert has_external_project_permission(request, "uuid", "GET") is False

    @patch("conversation_ms.permissions.requests.get")
    def test_returns_false_on_404(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.ok = False
        mock_get.return_value = mock_response

        request = self._request()
        assert has_external_project_permission(request, "uuid", "GET") is False

    @patch("conversation_ms.permissions.requests.get")
    def test_raises_unavailable_on_network_error(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("down")

        request = self._request()
        with pytest.raises(APIException) as exc_info:
            has_external_project_permission(request, "uuid", "GET")

        assert exc_info.value.status_code == 503


@pytest.mark.django_db
class TestProjectPermission:
    @pytest.fixture(autouse=True)
    def _configure_settings(self, settings):
        settings.PROJECTS_API_BASE_URL = "https://project-auth.example.com"

    def _request(self, project_uuid, *, method="GET", authorization="Bearer test-jwt-token"):
        request = RequestFactory().get(f"/projects/{project_uuid}/improvements/")
        request.method = method
        if authorization is not None:
            request.META["HTTP_AUTHORIZATION"] = authorization
        return request

    @patch("conversation_ms.permissions.requests.get")
    def test_allows_viewer_get(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["viewer"]}
        mock_get.return_value = mock_response

        request = self._request("11111111-1111-1111-1111-111111111111")
        view = Mock(kwargs={"project_uuid": "11111111-1111-1111-1111-111111111111"})
        permission = ProjectPermission()

        assert permission.has_permission(request, view) is True

    @patch("conversation_ms.permissions.requests.get")
    def test_denies_viewer_post(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["viewer"]}
        mock_get.return_value = mock_response

        request = self._request("11111111-1111-1111-1111-111111111111", method="POST")
        view = Mock(kwargs={"project_uuid": "11111111-1111-1111-1111-111111111111"})
        permission = ProjectPermission()

        assert permission.has_permission(request, view) is False

    def test_denies_without_authorization_header(self):
        request = self._request("11111111-1111-1111-1111-111111111111", authorization=None)
        view = Mock(kwargs={"project_uuid": "11111111-1111-1111-1111-111111111111"})
        permission = ProjectPermission()

        assert permission.has_permission(request, view) is False


@pytest.mark.django_db
class TestArchiveReadProjectPermission:
    @pytest.fixture(autouse=True)
    def _configure_settings(self, settings):
        settings.PROJECTS_API_BASE_URL = "https://project-auth.example.com"

    def _request(self, *, authorization="Bearer test-jwt-token"):
        request = RequestFactory().get("/projects/uuid/archived-conversations/uuid/")
        if authorization is not None:
            request.META["HTTP_AUTHORIZATION"] = authorization
        return request

    @patch("conversation_ms.permissions.requests.get")
    def test_support_allowed(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {
            "project_authorization": PROJECT_AUTH_ROLES["support"],
            "user": "support@example.com",
        }
        mock_get.return_value = mock_response

        request = self._request()
        assert has_archive_read_project_permission(request, "uuid") is True
        assert request.project_auth_user_email == "support@example.com"

    @patch("conversation_ms.permissions.requests.get")
    def test_viewer_denied(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.json.return_value = {"project_authorization": PROJECT_AUTH_ROLES["viewer"]}
        mock_get.return_value = mock_response

        request = self._request()
        assert has_archive_read_project_permission(request, "uuid") is False
