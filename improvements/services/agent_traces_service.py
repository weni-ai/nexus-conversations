from typing import Any

from conversation_ms.clients.nexus_client import NexusClient

_nexus_client = NexusClient()


def fetch_agent_traces(project_uuid: str, log_id: str) -> list[dict[str, Any]]:
    return _nexus_client.get_agent_traces(project_uuid, log_id)
