from typing import Any

from improvements.dependencies import get_improvements_dependencies


def fetch_agent_traces(project_uuid: str, log_id: str) -> list[dict[str, Any]]:
    return get_improvements_dependencies().project_data.get_agent_traces(project_uuid, log_id)
