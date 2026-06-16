from typing import Any

from django.conf import settings

from improvements.dependencies import get_improvements_dependencies


def get_project_customization(project_uuid: str) -> dict[str, Any]:
    return get_improvements_dependencies().project_data.get_project_customization(project_uuid)


def get_collaborative_agents(project_uuid: str) -> list[dict[str, Any]]:
    return get_improvements_dependencies().project_data.get_collaborative_agents(project_uuid)


def get_knowledge_base_chunks(project_uuid: str) -> list[dict[str, Any]]:
    if not getattr(settings, "IMPROVEMENTS_KNOWLEDGE_BASE_FETCH_ENABLED", True):
        return []
    return get_improvements_dependencies().project_data.get_knowledge_base_chunks(project_uuid)


def enrich_customization_for_improvements(
    customization: dict[str, Any],
    project_uuid: str,
) -> dict[str, Any]:
    """Ensure customization includes keys expected by the improvements pipeline."""
    enriched = dict(customization)
    enriched.setdefault("agent", {})
    enriched.setdefault("instructions", [])
    enriched["collaborative_agents"] = get_collaborative_agents(project_uuid)
    enriched["knowledge_base"] = get_knowledge_base_chunks(project_uuid)
    return enriched
