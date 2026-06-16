from __future__ import annotations

from typing import Any

from conversation_ms.clients.nexus_client import NexusClient


class NexusProjectDataClient:
    def __init__(self, client: NexusClient | None = None) -> None:
        self._client = client or NexusClient()

    def get_project_customization(self, project_uuid: str) -> dict[str, Any]:
        return self._client.get_project_customization(project_uuid)

    def get_collaborative_agents(self, project_uuid: str) -> list[dict[str, Any]]:
        return self._client.get_collaborative_agents(project_uuid)

    def get_agent_traces(self, project_uuid: str, log_id: str) -> list[dict[str, Any]]:
        return self._client.get_agent_traces(project_uuid, log_id)
