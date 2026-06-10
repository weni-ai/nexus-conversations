from unittest.mock import Mock, patch

import pytest
import requests
from django.conf import settings

from conversation_ms.clients.nexus_client import NexusClient


class TestNexusClient:
    @patch("conversation_ms.clients.nexus_client.requests.get")
    def test_get_project_customization(self, mock_get):
        project_uuid = "017cd5df-cfc8-4d5c-b659-347fe7a4bee9"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        settings.NEXUS_API_TOKEN = "test-token"
        expected = {
            "agent": {"name": "Taina"},
            "instructions": [],
        }
        mock_response = Mock()
        mock_response.json.return_value = expected
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = NexusClient().get_project_customization(project_uuid)

        assert result == expected
        mock_get.assert_called_once_with(
            f"https://nexus.stg.cloud.weni.ai/api/{project_uuid}/customization/",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-token",
            },
            params=None,
            timeout=30,
        )

    @patch("conversation_ms.clients.nexus_client.requests.get")
    def test_get_collaborative_agents(self, mock_get):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        settings.NEXUS_API_TOKEN = "test-token"
        expected = [
            {
                "name": "Broadcast Example Agent",
                "description": "Demonstrates sending two broadcast messages.",
                "instructions": [{"instruction": "Relay the message returned."}],
                "tools": [{"name": "send-messages", "description": "Sends messages.", "parameters": []}],
            }
        ]
        mock_response = Mock()
        mock_response.json.return_value = expected
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = NexusClient().get_collaborative_agents(project_uuid)

        assert result == expected
        mock_get.assert_called_once_with(
            f"https://nexus.stg.cloud.weni.ai/api/project/{project_uuid}/active-agents/config",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-token",
            },
            params=None,
            timeout=30,
        )

    @patch("conversation_ms.clients.nexus_client.requests.get")
    def test_get_agent_traces(self, mock_get):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        log_id = "f9688af3-2001-41d1-b52d-be0c50ad6bd7"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        settings.NEXUS_API_TOKEN = "test-token"
        expected = [{"trace": {"config": {"type": "executing_tool"}, "trace": {}}}]
        mock_response = Mock()
        mock_response.json.return_value = expected
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = NexusClient().get_agent_traces(project_uuid, log_id)

        assert result == expected
        mock_get.assert_called_once_with(
            "https://nexus.stg.cloud.weni.ai/api/agents/traces/",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer test-token",
            },
            params={"project_uuid": project_uuid, "log_id": log_id},
            timeout=30,
        )

    @patch("conversation_ms.clients.nexus_client.requests.get")
    def test_get_agent_traces_returns_empty_list_on_404(self, mock_get):
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)

        result = NexusClient().get_agent_traces(
            "3017e915-7986-4aee-8f09-ddbafd36bcdb",
            "missing-log-id",
        )

        assert result == []

    @patch("conversation_ms.clients.nexus_client.requests.get")
    def test_get_agent_traces_wraps_dict_payload(self, mock_get):
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        mock_response = Mock()
        mock_response.json.return_value = {"config": {"type": "executing_tool"}, "trace": {}}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = NexusClient().get_agent_traces("3017e915-7986-4aee-8f09-ddbafd36bcdb", "log-id")

        assert result == [{"trace": {"config": {"type": "executing_tool"}, "trace": {}}}]

    def test_requires_base_url(self):
        settings.NEXUS_API_BASE_URL = ""
        client = NexusClient()

        with pytest.raises(ValueError, match="NEXUS_API_BASE_URL"):
            client.get_project_customization("017cd5df-cfc8-4d5c-b659-347fe7a4bee9")
