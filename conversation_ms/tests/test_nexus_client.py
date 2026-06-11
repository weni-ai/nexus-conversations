from unittest.mock import Mock

import pytest
import requests
from django.conf import settings

from conversation_ms.clients.nexus_client import NexusClient


def _mock_response(*, status_code=200, json_payload=None):
    mock_response = Mock()
    mock_response.status_code = status_code
    mock_response.json.return_value = json_payload
    mock_response.raise_for_status = Mock()
    if status_code >= 400:
        mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
    return mock_response


def _client_with_mock_auth(mock_response):
    mock_auth = Mock()
    mock_auth.make_request_with_retry.return_value = mock_response
    return NexusClient(auth=mock_auth), mock_auth


class TestNexusClient:
    def test_get_project_customization(self):
        project_uuid = "017cd5df-cfc8-4d5c-b659-347fe7a4bee9"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        expected = {
            "agent": {"name": "Taina"},
            "instructions": [],
        }
        client, mock_auth = _client_with_mock_auth(_mock_response(json_payload=expected))

        result = client.get_project_customization(project_uuid)

        assert result == expected
        mock_auth.make_request_with_retry.assert_called_once_with(
            "GET",
            f"https://nexus.stg.cloud.weni.ai/api/{project_uuid}/customization/",
            params=None,
            timeout=30,
        )

    def test_get_collaborative_agents(self):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        expected = [
            {
                "name": "Broadcast Example Agent",
                "description": "Demonstrates sending two broadcast messages.",
                "instructions": [{"instruction": "Relay the message returned."}],
                "tools": [{"name": "send-messages", "description": "Sends messages.", "parameters": []}],
            }
        ]
        client, mock_auth = _client_with_mock_auth(_mock_response(json_payload=expected))

        result = client.get_collaborative_agents(project_uuid)

        assert result == expected
        mock_auth.make_request_with_retry.assert_called_once_with(
            "GET",
            f"https://nexus.stg.cloud.weni.ai/api/project/{project_uuid}/active-agents/config",
            params=None,
            timeout=30,
        )

    def test_get_agent_traces(self):
        project_uuid = "3017e915-7986-4aee-8f09-ddbafd36bcdb"
        log_id = "f9688af3-2001-41d1-b52d-be0c50ad6bd7"
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        expected = [{"trace": {"config": {"type": "executing_tool"}, "trace": {}}}]
        client, mock_auth = _client_with_mock_auth(_mock_response(json_payload=expected))

        result = client.get_agent_traces(project_uuid, log_id)

        assert result == expected
        mock_auth.make_request_with_retry.assert_called_once_with(
            "GET",
            "https://nexus.stg.cloud.weni.ai/api/agents/traces/",
            params={"project_uuid": project_uuid, "log_id": log_id},
            timeout=30,
        )

    def test_get_agent_traces_returns_empty_list_on_404(self):
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        client, _mock_auth = _client_with_mock_auth(_mock_response(status_code=404))

        result = client.get_agent_traces(
            "3017e915-7986-4aee-8f09-ddbafd36bcdb",
            "missing-log-id",
        )

        assert result == []

    def test_get_agent_traces_wraps_dict_payload(self):
        settings.NEXUS_API_BASE_URL = "https://nexus.stg.cloud.weni.ai"
        client, _mock_auth = _client_with_mock_auth(
            _mock_response(json_payload={"config": {"type": "executing_tool"}, "trace": {}})
        )

        result = client.get_agent_traces("3017e915-7986-4aee-8f09-ddbafd36bcdb", "log-id")

        assert result == [{"trace": {"config": {"type": "executing_tool"}, "trace": {}}}]

    def test_requires_base_url(self):
        settings.NEXUS_API_BASE_URL = ""
        client = NexusClient(auth=Mock())

        with pytest.raises(ValueError, match="NEXUS_API_BASE_URL"):
            client.get_project_customization("017cd5df-cfc8-4d5c-b659-347fe7a4bee9")
