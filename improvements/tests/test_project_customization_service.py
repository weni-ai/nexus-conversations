from unittest.mock import patch

import pytest

from improvements.services.project_customization_service import enrich_customization_for_improvements


@pytest.mark.django_db
class TestProjectCustomizationService:
    @patch("improvements.services.project_customization_service.get_collaborative_agents")
    def test_enrich_customization_for_improvements(self, mock_get_collaborative_agents):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        mock_get_collaborative_agents.return_value = [
            {"name": "Broadcast Example Agent", "description": "Example", "instructions": [], "tools": []}
        ]
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

    @patch("improvements.services.project_customization_service.get_collaborative_agents")
    def test_enrich_customization_preserves_existing_knowledge_base(self, mock_get_collaborative_agents):
        mock_get_collaborative_agents.return_value = []
        customization = {
            "agent": {},
            "knowledge_base": [{"chunk_id": "kb-001", "content": "Policy text"}],
        }

        result = enrich_customization_for_improvements(customization, "project-uuid")

        assert result["knowledge_base"] == [{"chunk_id": "kb-001", "content": "Policy text"}]
