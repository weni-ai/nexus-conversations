from __future__ import annotations

from typing import Any, BinaryIO, Protocol


class S3Storage(Protocol):
    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        bucket: str,
        key: str,
        *,
        content_type: str,
    ) -> None: ...

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        *,
        content_type: str,
    ) -> None: ...

    def object_exists(self, bucket: str, key: str) -> bool: ...

    def generate_presigned_get_url(self, bucket: str, key: str, *, expires_in: int) -> str: ...


class ImprovementsLambdaClient(Protocol):
    def invoke_sample_size(self, payload: dict[str, Any]) -> int: ...

    def invoke_improvements(self, payload: dict[str, Any]) -> Any: ...


class BatchCheckScheduler(Protocol):
    def register(
        self,
        project_uuid: str,
        target_date: str,
        *,
        task_kwargs: dict[str, Any],
        interval_seconds: int,
    ) -> str: ...

    def unregister(self, project_uuid: str, target_date: str) -> None: ...

    def exists(self, project_uuid: str, target_date: str) -> bool: ...


class ProjectDataClient(Protocol):
    def get_project_customization(self, project_uuid: str) -> dict[str, Any]: ...

    def get_collaborative_agents(self, project_uuid: str) -> list[dict[str, Any]]: ...

    def get_agent_traces(self, project_uuid: str, log_id: str) -> list[dict[str, Any]]: ...
