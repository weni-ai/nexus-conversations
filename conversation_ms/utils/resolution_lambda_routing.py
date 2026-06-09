"""Project-based routing between legacy and V2 resolution classification Lambdas."""

from django.conf import settings


def _legacy_project_uuids() -> set[str]:
    return {str(u).strip().lower() for u in settings.CONVERSATION_RESOLUTION_LEGACY_PROJECTS if u}


def uses_legacy_resolution_lambda(project_uuid: str) -> bool:
    """Return True when the project must use the legacy resolution Lambda."""
    return str(project_uuid).strip().lower() in _legacy_project_uuids()


def get_resolution_lambda_name(project_uuid: str) -> str | None:
    """Resolve the Lambda function name for resolution classification."""
    if uses_legacy_resolution_lambda(project_uuid):
        return settings.CONVERSATION_RESOLUTION_NAME
    return settings.CONVERSATION_RESOLUTION_V2_NAME
