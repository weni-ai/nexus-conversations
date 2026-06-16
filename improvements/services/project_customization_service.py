from typing import Any

from improvements.dependencies import get_improvements_dependencies


def get_project_customization(project_uuid: str) -> dict[str, Any]:
    return get_improvements_dependencies().project_data.get_project_customization(project_uuid)


def get_collaborative_agents(project_uuid: str) -> list[dict[str, Any]]:
    return get_improvements_dependencies().project_data.get_collaborative_agents(project_uuid)


def get_knowledge_base_placeholder(customization: dict[str, Any]) -> list[dict[str, Any]]:
    """Placeholder until knowledge base chunks are returned by the customization API."""
    return customization.get("knowledge_base") or []


def enrich_customization_for_improvements(
    customization: dict[str, Any],
    project_uuid: str,
) -> dict[str, Any]:
    """Ensure customization includes keys expected by the improvements pipeline."""
    enriched = dict(customization)
    enriched.setdefault("agent", {})
    enriched.setdefault("instructions", [])
    enriched["collaborative_agents"] = get_collaborative_agents(project_uuid)
    enriched.setdefault("knowledge_base", get_knowledge_base_placeholder(customization))
    return enriched
