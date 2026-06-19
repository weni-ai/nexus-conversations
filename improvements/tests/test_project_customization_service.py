from unittest.mock import patch

import pytest
from django.test import override_settings

from improvements.services.project_customization_service import (
    build_customization_for_lambda_upload,
    enrich_customization_for_improvements,
)


@pytest.mark.django_db
class TestProjectCustomizationService:
    @patch("improvements.services.project_customization_service.get_collaborative_agents")
    @patch("improvements.services.project_customization_service.get_knowledge_base_chunks")
    def test_enrich_customization_for_improvements(self, mock_get_knowledge_base_chunks, mock_get_collaborative_agents):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        mock_get_collaborative_agents.return_value = [
            {"name": "Broadcast Example Agent", "description": "Example", "instructions": [], "tools": []}
        ]
        mock_get_knowledge_base_chunks.return_value = []
        customization = {
            "agent": {"name": "Taina"},
            "instructions": [{"id": 1, "instruction": "Be helpful"}],
        }

        result = enrich_customization_for_improvements(customization, project_uuid)

        assert result["agent"] == {"name": "Taina"}
        assert result["instructions"] == [{"id": 1, "instruction": "Be helpful"}]
        assert result["collaborative_agents"] == mock_get_collaborative_agents.return_value
        assert result["knowledge_base"] == []
        mock_get_collaborative_agents.assert_called_once_with(project_uuid)
        mock_get_knowledge_base_chunks.assert_called_once_with(project_uuid)

    @patch("improvements.services.project_customization_service.get_collaborative_agents", return_value=[])
    @patch("improvements.services.project_customization_service.get_project_customization")
    def test_build_customization_for_lambda_upload_excludes_knowledge_base(
        self,
        mock_get_project_customization,
        mock_get_collaborative_agents,
    ):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        mock_get_project_customization.return_value = {
            "agent": {"name": "Taina"},
            "instructions": [],
        }

        result = build_customization_for_lambda_upload(project_uuid)

        assert result["agent"] == {"name": "Taina"}
        assert result["collaborative_agents"] == []
        assert "knowledge_base" not in result
        mock_get_collaborative_agents.assert_called_once_with(project_uuid)

    @patch("improvements.services.project_customization_service.get_collaborative_agents", return_value=[])
    @patch("improvements.services.project_customization_service.get_knowledge_base_chunks")
    def test_enrich_customization_overwrites_knowledge_base_from_api(
        self,
        mock_get_knowledge_base_chunks,
        _mock_get_collaborative_agents,
    ):
        mock_get_knowledge_base_chunks.return_value = [{"chunk_id": "kb-001", "content": "Policy text"}]
        customization = {
            "agent": {},
            "knowledge_base": [{"chunk_id": "stale", "content": "Old text"}],
        }

        result = enrich_customization_for_improvements(customization, "project-uuid")

        assert result["knowledge_base"] == [{"chunk_id": "kb-001", "content": "Policy text"}]
        mock_get_knowledge_base_chunks.assert_called_once_with("project-uuid")

    @override_settings(IMPROVEMENTS_KNOWLEDGE_BASE_FETCH_ENABLED=False)
    @patch("improvements.services.project_customization_service.get_improvements_dependencies")
    def test_get_knowledge_base_chunks_returns_empty_when_fetch_disabled(self, mock_get_dependencies):
        from improvements.services.project_customization_service import get_knowledge_base_chunks

        result = get_knowledge_base_chunks("project-uuid")

        assert result == []
        mock_get_dependencies.assert_not_called()
