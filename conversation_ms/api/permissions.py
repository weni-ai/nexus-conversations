from rest_framework import permissions

from conversation_ms.permissions import has_external_project_permission


class ProjectPermission(permissions.BasePermission):
    message = "You do not have permission to perform this action on this project."

    def has_permission(self, request, view):
        request_data = getattr(request, "data", {}) or {}
        query_params = getattr(request, "query_params", request.GET)

        uuids = {
            view.kwargs.get("project_uuid"),
            request_data.get("project"),
            request_data.get("project_uuid"),
            query_params.get("project"),
            query_params.get("project_uuid"),
        }
        uuids.discard(None)
        if len(uuids) != 1:
            return False

        project_uuid = next(iter(uuids))
        return has_external_project_permission(
            request=request,
            project_uuid=str(project_uuid),
            method=request.method,
        )
