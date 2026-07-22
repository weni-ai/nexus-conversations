import hashlib
import logging

import requests
import sentry_sdk
from django.conf import settings
from django.core.cache import cache
from rest_framework.exceptions import APIException
from rest_framework.permissions import SAFE_METHODS

logger = logging.getLogger(__name__)

PROJECT_AUTH_ROLES = {
    "not_set": 0,
    "viewer": 1,
    "contributor": 2,
    "moderator": 3,
    "support": 4,
    "chat_user": 5,
}

WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
PROJECT_AUTH_CACHE_TTL_SECONDS = 45


class ProjectAuthNotFound(Exception):
    """Raised when the external authorization API returns 404."""


class ProjectAuthorizationDenied(Exception):
    """Raised when the external authorization API denies access."""


class ProjectAuthUnavailable(APIException):
    status_code = 503
    default_detail = "Project authorization service is temporarily unavailable"
    default_code = "project_auth_unavailable"


def _user_email_from_authorization_payload(payload: dict) -> str | None:
    user = payload.get("user")
    if isinstance(user, str):
        return user.strip() or None
    if isinstance(user, dict):
        email = user.get("email")
        if isinstance(email, str):
            return email.strip() or None
    return None


# Roles allowed to read archived conversations via Support UI (Phase D).
ARCHIVE_READ_ROLES = frozenset(
    {
        PROJECT_AUTH_ROLES["moderator"],
        PROJECT_AUTH_ROLES["support"],
    }
)


def _is_role_authorized_for_method(role: int | None, method: str) -> bool:
    if role is None or role == PROJECT_AUTH_ROLES["not_set"]:
        return False

    if method.upper() in SAFE_METHODS:
        return True

    if role == PROJECT_AUTH_ROLES["moderator"]:
        return True

    if role == PROJECT_AUTH_ROLES["contributor"]:
        return method.upper() in WRITE_METHODS

    return False


def _is_archive_read_role(role: int | None, method: str) -> bool:
    """Archive GET: moderator (3) or support (4) only."""
    if method.upper() not in SAFE_METHODS:
        return False
    return role in ARCHIVE_READ_ROLES


def _method_auth_class(method: str) -> str:
    return "safe" if method.upper() in SAFE_METHODS else "write"


def _project_auth_cache_key(token: str, project_uuid: str, method: str) -> str:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return f"project_auth:{token_hash}:{project_uuid}:{_method_auth_class(method)}"


def _check_project_authorization(
    token: str,
    project_uuid: str,
    method: str,
    *,
    role_checker=_is_role_authorized_for_method,
) -> tuple[bool, str | None]:
    cache_key = _project_auth_cache_key(token, project_uuid, method)
    cached = cache.get(cache_key)
    if cached is not None:
        return True, cached.get("user_email")

    base_url = settings.PROJECTS_API_BASE_URL
    if not base_url:
        raise ProjectAuthUnavailable()

    url = f"{base_url.rstrip('/')}/v2/projects/{project_uuid}/authorization"
    response = requests.get(
        url,
        headers={"Authorization": token},
        timeout=settings.PROJECT_AUTH_API_TIMEOUT_SECONDS,
    )

    if response.status_code == 404:
        raise ProjectAuthNotFound()

    if response.status_code in (401, 403):
        raise ProjectAuthorizationDenied()

    if not response.ok:
        logger.error(
            "[ProjectAuth] Unexpected status from authorization API project_uuid=%s status=%s",
            project_uuid,
            response.status_code,
        )
        raise ProjectAuthUnavailable()

    data = response.json()
    user_email = _user_email_from_authorization_payload(data)
    role = data.get("project_authorization")
    authorized = role_checker(role, method)

    if not authorized:
        raise ProjectAuthorizationDenied()

    cache.set(
        cache_key,
        {"user_email": user_email},
        PROJECT_AUTH_CACHE_TTL_SECONDS,
    )
    return True, user_email


def _fallback_local_permission(request, project_uuid: str, method: str) -> bool:
    """Phase 1: deny when external API has no authorization record."""
    return False


def _bearer_token_from_request(request) -> str | None:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    return auth_header[7:].strip()


def _internal_service_team_name(token: str) -> str | None:
    team_tokens = getattr(settings, "INTERNAL_API_TOKENS", {})
    for team_name, team_token in team_tokens.items():
        if token == team_token:
            return team_name
    return None


def has_internal_service_project_permission(request, project_uuid: str) -> bool:
    """Allow trusted service-to-service calls using INTERNAL_API_TOKENS."""
    token = _bearer_token_from_request(request)
    if not token:
        return False

    team_name = _internal_service_team_name(token)
    if team_name is None:
        return False

    request.project_auth_service_name = team_name
    return True


def has_external_project_permission(request, project_uuid: str, method: str) -> bool:
    token = request.headers.get("Authorization")
    if not token:
        return False

    try:
        authorized, user_email = _check_project_authorization(token, project_uuid, method)
        if user_email:
            request.project_auth_user_email = user_email
        return authorized
    except ProjectAuthNotFound:
        return _fallback_local_permission(request, project_uuid, method)
    except ProjectAuthorizationDenied:
        return False
    except requests.RequestException as exc:
        sentry_sdk.capture_exception(exc)
        logger.error(
            "[ProjectAuth] Authorization API request failed project_uuid=%s error=%s",
            project_uuid,
            exc,
            exc_info=True,
        )
        raise ProjectAuthUnavailable() from exc


def has_archive_read_project_permission(request, project_uuid: str) -> bool:
    """
    Connect RBAC for Support archive API: GET allowed for support/moderator only.
    """
    token = request.headers.get("Authorization")
    if not token:
        return False

    try:
        authorized, user_email = _check_project_authorization(
            token,
            project_uuid,
            "GET",
            role_checker=_is_archive_read_role,
        )
        if user_email:
            request.project_auth_user_email = user_email
        return authorized
    except ProjectAuthNotFound:
        return False
    except ProjectAuthorizationDenied:
        return False
    except requests.RequestException as exc:
        sentry_sdk.capture_exception(exc)
        logger.error(
            "[ProjectAuth] Archive read authorization API failed project_uuid=%s error=%s",
            project_uuid,
            exc,
            exc_info=True,
        )
        raise ProjectAuthUnavailable() from exc
