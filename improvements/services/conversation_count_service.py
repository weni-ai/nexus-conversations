from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

import pendulum
from django.conf import settings

from conversation_ms.adapters.aws import get_boto3_client
from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum, parse_api_utc
from conversation_ms.utils.date_helpers import ProjectDay, resolve_effective_project_timezone

logger = logging.getLogger(__name__)


def _iso_utc_string(dt: pendulum.DateTime) -> str:
    return dt.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]")


def resolve_date_range(
    project: Project,
    start_date: str | None,
    end_date: str | None,
) -> tuple[pendulum.DateTime, pendulum.DateTime]:
    start_provided = start_date is not None and str(start_date).strip() != ""
    end_provided = end_date is not None and str(end_date).strip() != ""

    if not start_provided and not end_provided:
        tz = resolve_effective_project_timezone(project.timezone)
        return ProjectDay.for_yesterday(tz).get_utc_range()

    start_bound = parse_api_utc(str(start_date).strip())
    end_bound = parse_api_utc(str(end_date).strip())
    if end_bound < start_bound:
        raise ValueError("end_date must be on or after start_date")
    return start_bound, end_bound


def count_conversations_in_range(
    project_uuid: UUID,
    start_utc: pendulum.DateTime,
    end_utc: pendulum.DateTime,
) -> int:
    return Conversation.objects.filter(
        project__uuid=project_uuid,
        start_date__gte=django_utc_from_pendulum(start_utc),
        start_date__lte=django_utc_from_pendulum(end_utc),
    ).count()


DEFAULT_SAMPLING_MODE = "srs"

LAMBDA_PAYLOAD_KEYS = ("sampling_mode", "total_count", "target_date")


def build_lambda_payload(
    total_count: int,
    target_date: str,
    sampling_mode: str = DEFAULT_SAMPLING_MODE,
) -> dict[str, Any]:
    return {
        "sampling_mode": sampling_mode,
        "total_count": total_count,
        "target_date": target_date,
    }


def build_task_payload(
    project: Project,
    total_count: int,
    start_utc: pendulum.DateTime,
    end_utc: pendulum.DateTime,
) -> dict[str, Any]:
    tz = resolve_effective_project_timezone(project.timezone)
    target_date = start_utc.in_timezone(tz).format("YYYY-MM-DD")
    return {
        **build_lambda_payload(total_count, target_date),
        "project_uuid": str(project.uuid),
        "project_name": project.name or "",
        "start": _iso_utc_string(start_utc),
        "end": _iso_utc_string(end_utc),
    }


def _parse_lambda_sample_size(result: Any) -> int:
    if isinstance(result, dict) and "body" in result:
        body = result["body"]
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                body = body.strip()
        result = body.get("sample_size")

    if isinstance(result, int):
        return result
    if isinstance(result, str):
        stripped = result.strip()
        if stripped.lstrip("-").isdigit():
            return int(stripped)
    raise ValueError(f"Lambda must return an integer sample size, got: {result!r}")


def get_conversations_sample_size_lambda(payload: dict[str, Any]) -> int:
    lambda_arn = getattr(settings, "GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN", None)
    if not lambda_arn:
        raise ValueError("GET_CONVERSATIONS_SAMPLE_SIZE_LAMBDA_ARN is not configured")

    lambda_payload = {key: payload[key] for key in LAMBDA_PAYLOAD_KEYS}
    lambda_client = get_boto3_client("lambda", region_name=settings.LAMBDA_AWS_REGION)
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=json.dumps(lambda_payload),
    )

    status_code = response.get("StatusCode")
    if status_code and status_code >= 400:
        raise RuntimeError(f"Lambda invocation failed with status {status_code}")

    function_error = response.get("FunctionError")
    if function_error:
        error_payload = response["Payload"].read().decode("utf-8")
        raise RuntimeError(f"Lambda returned FunctionError={function_error}: {error_payload}")

    response_payload = response["Payload"].read()
    result = json.loads(response_payload)
    return _parse_lambda_sample_size(result)


def select_random_conversations_in_range(
    project_uuid: str | UUID,
    start: str,
    end: str,
    sample_size: int,
) -> list[Conversation]:
    if sample_size <= 0:
        return []

    start_utc = parse_api_utc(start)
    end_utc = parse_api_utc(end)
    queryset = Conversation.objects.filter(
        project__uuid=project_uuid,
        start_date__gte=django_utc_from_pendulum(start_utc),
        start_date__lte=django_utc_from_pendulum(end_utc),
    )

    total = queryset.count()
    if total == 0:
        return []

    limit = min(sample_size, total)
    return list(
        queryset.select_related(
            "project",
            "messages_data",
            "classification",
            "classification__topic",
            "classification__subtopic",
        ).order_by("?")[:limit]
    )
