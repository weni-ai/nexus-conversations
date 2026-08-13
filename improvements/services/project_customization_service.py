from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from django.conf import settings

from improvements.dependencies import get_improvements_dependencies
from improvements.services.custom_analysis_service import build_classification_classes
from improvements.services.kb_chunk_registry import build_kb_chunks_dict


def get_project_customization(project_uuid: str) -> dict[str, Any]:
    return get_improvements_dependencies().project_data.get_project_customization(project_uuid)


def get_collaborative_agents(project_uuid: str) -> list[dict[str, Any]]:
    return get_improvements_dependencies().project_data.get_collaborative_agents(project_uuid)


def get_knowledge_base_chunks(project_uuid: str) -> list[dict[str, Any]]:
    if not getattr(settings, "IMPROVEMENTS_KNOWLEDGE_BASE_FETCH_ENABLED", True):
        return []
    return get_improvements_dependencies().project_data.get_knowledge_base_chunks(project_uuid)


def build_customization_for_lambda_upload(project_uuid: str) -> dict[str, Any]:
    """Build customization for Lambda build upload without knowledge_base."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        customization_future = executor.submit(get_project_customization, project_uuid)
        agents_future = executor.submit(get_collaborative_agents, project_uuid)
        customization = customization_future.result()
        collaborative_agents = agents_future.result()

    enriched = dict(customization)
    enriched.setdefault("agent", {})
    enriched.setdefault("instructions", [])
    enriched.setdefault("team", {})
    enriched["collaborative_agents"] = collaborative_agents
    return enriched


def build_customization_artifact(
    customization: dict[str, Any],
    normalized_conversations: list[dict[str, Any]] | None = None,
    *,
    project_uuid: str | None = None,
) -> dict[str, Any]:
    artifact: dict[str, Any] = {
        "customization": customization,
        "kb_chunks_dict": build_kb_chunks_dict(normalized_conversations or []),
    }
    if project_uuid is not None:
        artifact["classification_classes"] = build_classification_classes(project_uuid)
    return artifact


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
