from __future__ import annotations

import json
import logging
import random
import time
from typing import Any
from uuid import UUID

import pendulum
from django.conf import settings
from django.core.cache import cache

from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import django_utc_from_pendulum, parse_api_utc
from conversation_ms.utils.date_helpers import ProjectDay, resolve_effective_project_timezone
from improvements.dependencies import get_improvements_dependencies

logger = logging.getLogger(__name__)

YESTERDAY_CONVERSATIONS_COUNT_CACHE_TTL_SECONDS = 90


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
        project_id=project_uuid,
        start_date__gte=django_utc_from_pendulum(start_utc),
        start_date__lte=django_utc_from_pendulum(end_utc),
    ).count()


def _yesterday_count_cache_key(project_uuid: UUID | str, target_date: str) -> str:
    return f"improvements:yesterday_count:{project_uuid}:{target_date}"


def count_yesterday_conversations(project: Project) -> int:
    start_utc, end_utc = resolve_date_range(project, None, None)
    tz = resolve_effective_project_timezone(project.timezone)
    target_date = start_utc.in_timezone(tz).format("YYYY-MM-DD")
    cache_key = _yesterday_count_cache_key(project.uuid, target_date)

    cached = cache.get(cache_key)
    if cached is not None:
        return int(cached)

    started = time.perf_counter()
    total_count = count_conversations_in_range(project.uuid, start_utc, end_utc)
    duration_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "[count_yesterday_conversations] cache_miss duration_ms=%.2f project_uuid=%s " "target_date=%s count=%s",
        duration_ms,
        project.uuid,
        target_date,
        total_count,
    )
    cache.set(cache_key, total_count, YESTERDAY_CONVERSATIONS_COUNT_CACHE_TTL_SECONDS)
    return total_count


DEFAULT_SAMPLING_MODE = "srs"

LAMBDA_PAYLOAD_KEYS = ("sampling_mode", "total_count", "target_date")

CONVERSATION_IMPROVEMENTS_SELECT_RELATED = (
    "project",
    "messages_data",
    "classification",
    "classification__topic",
    "classification__subtopic",
)


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

    return get_improvements_dependencies().lambda_client.invoke_sample_size(payload)


def _conversations_in_range_queryset(
    project_uuid: str | UUID,
    start: str,
    end: str,
):
    start_utc = parse_api_utc(start)
    end_utc = parse_api_utc(end)
    return Conversation.objects.filter(
        project__uuid=project_uuid,
        start_date__gte=django_utc_from_pendulum(start_utc),
        start_date__lte=django_utc_from_pendulum(end_utc),
    )


def select_random_conversation_uuids_in_range(
    project_uuid: str | UUID,
    start: str,
    end: str,
    sample_size: int,
) -> list[UUID]:
    if sample_size <= 0:
        return []

    queryset = _conversations_in_range_queryset(project_uuid, start, end)
    uuids = list(queryset.values_list("uuid", flat=True))
    if not uuids:
        return []

    limit = min(sample_size, len(uuids))
    if limit == len(uuids):
        return uuids
    return random.sample(uuids, limit)


def iter_conversation_batches_by_uuids(
    uuids: list[UUID],
    batch_size: int | None = None,
):
    if not uuids:
        return

    if batch_size is None:
        batch_size = getattr(settings, "IMPROVEMENTS_CONVERSATION_BATCH_SIZE", 50)
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    for index in range(0, len(uuids), batch_size):
        batch_uuids = uuids[index : index + batch_size]
        conversations = list(
            Conversation.objects.filter(uuid__in=batch_uuids).select_related(*CONVERSATION_IMPROVEMENTS_SELECT_RELATED)
        )
        by_uuid = {conversation.uuid: conversation for conversation in conversations}
        yield [by_uuid[uuid] for uuid in batch_uuids if uuid in by_uuid]


def select_random_conversations_in_range(
    project_uuid: str | UUID,
    start: str,
    end: str,
    sample_size: int,
) -> list[Conversation]:
    uuids = select_random_conversation_uuids_in_range(project_uuid, start, end, sample_size)
    conversations: list[Conversation] = []
    for batch in iter_conversation_batches_by_uuids(uuids):
        conversations.extend(batch)
    return conversations
