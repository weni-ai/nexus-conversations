from rest_framework import permissions

from conversation_ms.models import Project
from conversation_ms.permissions import (
    has_external_project_permission,
    has_internal_service_project_permission,
)


def _normalize_project_uuid(value):
    if value is None:
        return None
    return str(value)


def _project_exists(project_uuid: str) -> bool:
    return Project.objects.filter(uuid=project_uuid).exists()


class ProjectPermission(permissions.BasePermission):
    message = "You do not have permission to perform this action on this project."

    def has_permission(self, request, view):
        path_project_uuid = _normalize_project_uuid(view.kwargs.get("project_uuid"))
        if path_project_uuid is not None:
            return has_external_project_permission(
                request=request,
                project_uuid=path_project_uuid,
                method=request.method,
            )

        request_data = getattr(request, "data", {}) or {}
        query_params = getattr(request, "query_params", request.GET)

        uuids = {
            _normalize_project_uuid(request_data.get("project")),
            _normalize_project_uuid(request_data.get("project_uuid")),
            _normalize_project_uuid(query_params.get("project")),
            _normalize_project_uuid(query_params.get("project_uuid")),
        }
        uuids.discard(None)
        if len(uuids) != 1:
            return False

        project_uuid = next(iter(uuids))
        return has_external_project_permission(
            request=request,
            project_uuid=project_uuid,
            method=request.method,
        )


class InternalOrProjectPermission(permissions.BasePermission):
    """
    Service-to-service (INTERNAL_API_TOKENS) or user JWT via Projects API.

    MCP and other internal callers use the shared internal token.
    Support UI and similar clients keep using user JWT + Connect RBAC.
    """

    message = "You do not have permission to perform this action on this project."

    def has_permission(self, request, view):
        path_project_uuid = _normalize_project_uuid(view.kwargs.get("project_uuid"))
        if path_project_uuid is not None:
            if has_internal_service_project_permission(request, path_project_uuid):
                return _project_exists(path_project_uuid)
            return has_external_project_permission(
                request=request,
                project_uuid=path_project_uuid,
                method=request.method,
            )

        request_data = getattr(request, "data", {}) or {}
        query_params = getattr(request, "query_params", request.GET)

        uuids = {
            _normalize_project_uuid(request_data.get("project")),
            _normalize_project_uuid(request_data.get("project_uuid")),
            _normalize_project_uuid(query_params.get("project")),
            _normalize_project_uuid(query_params.get("project_uuid")),
        }
        uuids.discard(None)
        if len(uuids) != 1:
            return False

        project_uuid = next(iter(uuids))
        if has_internal_service_project_permission(request, project_uuid):
            return _project_exists(project_uuid)
        return has_external_project_permission(
            request=request,
            project_uuid=project_uuid,
            method=request.method,
        )
