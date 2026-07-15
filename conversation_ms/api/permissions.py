from rest_framework import permissions

from conversation_ms.permissions import has_archive_read_project_permission, has_external_project_permission


def _normalize_project_uuid(value):
    if value is None:
        return None
    return str(value)


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


class ArchiveReadProjectPermission(permissions.BasePermission):
    """
    Support archive API: Connect JWT must map to support (4) or moderator (3).
    """

    message = "You do not have permission to retrieve archived conversations for this project."

    def has_permission(self, request, view):
        path_project_uuid = _normalize_project_uuid(view.kwargs.get("project_uuid"))
        if path_project_uuid is None:
            return False
        return has_archive_read_project_permission(
            request=request,
            project_uuid=path_project_uuid,
        )
