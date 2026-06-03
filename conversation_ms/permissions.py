import logging

import requests
from django.conf import settings
from rest_framework import permissions
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import SAFE_METHODS

from conversation_ms.exceptions import ProjectAuthorizationDenied

logger = logging.getLogger(__name__)

ROLE_LEVELS = {
    "not_set": 0,
    "viewer": 1,
    "contributor": 2,
    "moderator": 3,
    "support": 4,
    "chat_user": 5,
}


def _is_authorized_response(response):
    return response.status_code == 200


def _user_email_from_authorization_payload(data):
    return data.get("user_email") or data.get("user", {}).get("email")


def _check_project_authorization(token, project_uuid, method):
    """Call the external authorization API and return (authorized, user_email)."""
    base_url = settings.PROJECTS_API_BASE_URL
    url = f"{base_url}/v2/projects/{project_uuid}/authorization"

    response = requests.get(url, headers={"Authorization": token}, timeout=10)

    if not _is_authorized_response(response):
        raise ProjectAuthorizationDenied("You do not have permission to perform this action.")

    data = response.json()
    user_email = _user_email_from_authorization_payload(data)

    if method.upper() in SAFE_METHODS:
        return True, user_email

    project_authorization = data.get("project_authorization")

    if project_authorization == ROLE_LEVELS["moderator"]:
        return True, user_email

    if project_authorization == ROLE_LEVELS["contributor"]:
        return method.upper() in ("POST", "PUT", "PATCH", "DELETE"), user_email

    raise ProjectAuthorizationDenied("You do not have permission to perform this action.")


def has_external_general_project_permission(request, project_uuid, method):
    token = request.headers.get("Authorization")
    try:
        authorized, user_email = _check_project_authorization(token, project_uuid, method)
        if user_email:
            request.project_auth_user_email = user_email
        return authorized
    except (requests.RequestException, ProjectAuthorizationDenied):
        return False


class ProjectPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        data = getattr(request, "data", {}) or {}
        query = getattr(request, "query_params", {}) or {}
        uuids = {
            view.kwargs.get("project_uuid"),
            data.get("project"),
            data.get("project_uuid"),
            query.get("project"),
            query.get("project_uuid"),
        }
        uuids.discard(None)

        if not uuids:
            return False

        try:
            project_uuid = next(iter(uuids))
            return has_external_general_project_permission(
                request=request,
                project_uuid=project_uuid,
                method=request.method,
            )
        except ProjectAuthorizationDenied:
            return False
        except Exception as e:
            raise ValidationError({"detail": f"An error occurred: {str(e)}"}) from e
