from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from improvements.dependencies import ImprovementsDependencies
from improvements.services.improvements_redbeat_service import improvements_run_key


class InMemoryS3Storage:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        bucket: str,
        key: str,
        *,
        content_type: str,
    ) -> None:
        self.objects[(bucket, key)] = file_obj.read()

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        content_type: str,
    ) -> None:
        self.objects[(bucket, key)] = body

    def object_exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.objects

    def generate_presigned_get_url(self, bucket: str, key: str, *, expires_in: int) -> str:
        return f"https://fake-s3/{bucket}/{key}?expires_in={expires_in}"

    def get_object_bytes(self, bucket: str, key: str) -> bytes | None:
        return self.objects.get((bucket, key))


@dataclass
class ScriptedLambdaClient:
    sample_size: int = 2
    build_response: dict[str, Any] | None = None
    check_responses: deque[dict[str, Any]] = field(default_factory=deque)
    invocations: list[dict[str, Any]] = field(default_factory=list)

    def invoke_sample_size(self, payload: dict[str, Any]) -> int:
        self.invocations.append({"type": "sample_size", "payload": payload})
        return self.sample_size

    def invoke_improvements(self, payload: dict[str, Any]) -> Any:
        self.invocations.append({"type": "improvements", "payload": dict(payload)})
        action = payload.get("action")
        if action == "build":
            return self.build_response or {
                "batches": [
                    {
                        "batch_id": "batch_test",
                        "input_file_id": "file_test",
                        "endpoint": "/v1/responses",
                        "n_requests": 2,
                    }
                ],
                "metadata_passthrough": {
                    "project_uuid": payload.get("metadata_passthrough", {}).get("project_uuid", ""),
                    "target_date": payload.get("metadata_passthrough", {}).get("target_date", ""),
                },
            }
        if action == "check":
            if self.check_responses:
                return self.check_responses.popleft()
            return {"status": "completed"}
        raise ValueError(f"Unexpected improvements lambda action: {action!r}")


class InMemoryBatchCheckScheduler:
    def __init__(self) -> None:
        self.schedules: dict[str, dict[str, Any]] = {}

    def register(
        self,
        project_uuid: str,
        target_date: str,
        *,
        task_kwargs: dict[str, Any],
        interval_seconds: int,
    ) -> str:
        run_key = improvements_run_key(project_uuid, target_date)
        self.schedules[run_key] = {
            "project_uuid": str(project_uuid),
            "target_date": str(target_date),
            "task_kwargs": task_kwargs,
            "interval_seconds": interval_seconds,
        }
        return run_key

    def unregister(self, project_uuid: str, target_date: str) -> None:
        run_key = improvements_run_key(project_uuid, target_date)
        self.schedules.pop(run_key, None)

    def exists(self, project_uuid: str, target_date: str) -> bool:
        run_key = improvements_run_key(project_uuid, target_date)
        return run_key in self.schedules


class FakeProjectDataClient:
    def __init__(
        self,
        customization: dict[str, Any] | None = None,
        collaborative_agents: list[dict[str, Any]] | None = None,
    ) -> None:
        self.customization = customization or {
            "agent": {"name": "Agent", "role": "Support", "personality": "Friendly", "goal": "Help"},
            "instructions": [],
            "team": {"human_support": False, "human_support_prompt": ""},
        }
        self.collaborative_agents = collaborative_agents or []

    def get_project_customization(self, project_uuid: str) -> dict[str, Any]:
        return dict(self.customization)

    def get_collaborative_agents(self, project_uuid: str) -> list[dict[str, Any]]:
        return list(self.collaborative_agents)

    def get_agent_traces(self, project_uuid: str, log_id: str) -> list[dict[str, Any]]:
        return []


def build_in_memory_improvements_dependencies(
    *,
    s3: InMemoryS3Storage | None = None,
    lambda_client: ScriptedLambdaClient | None = None,
    scheduler: InMemoryBatchCheckScheduler | None = None,
    project_data: FakeProjectDataClient | None = None,
) -> ImprovementsDependencies:
    return ImprovementsDependencies(
        s3=s3 or InMemoryS3Storage(),
        lambda_client=lambda_client or ScriptedLambdaClient(),
        scheduler=scheduler or InMemoryBatchCheckScheduler(),
        project_data=project_data or FakeProjectDataClient(),
    )


def parse_s3_json(s3: InMemoryS3Storage, bucket: str, key: str) -> Any:
    raw = s3.get_object_bytes(bucket, key)
    if raw is None:
        raise KeyError(f"Object not found: s3://{bucket}/{key}")
    return json.loads(raw.decode("utf-8"))
