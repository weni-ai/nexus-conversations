"""Retention eligibility helpers shared by API filters and archive pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum
from django.conf import settings
from django.db.models import Q, QuerySet
from django.db.models.functions import Coalesce

from conversation_ms.archive.constants import RESOLUTION_IN_PROGRESS
from conversation_ms.utils.date_helpers import resolve_effective_project_timezone

if TYPE_CHECKING:
    from conversation_ms.models import Conversation


def retention_days() -> int:
    return int(getattr(settings, "CONVERSATION_RETENTION_DAYS", 90))


def retention_cutoff_utc(
    project_timezone: str | None,
    *,
    retention_days_override: int | None = None,
    now: pendulum.DateTime | None = None,
) -> pendulum.DateTime:
    """
    UTC instant for the start of the project-local calendar day ``retention_days`` ago.

    Closed conversations with eligibility timestamp strictly before this cutoff are
    excluded from list/detail APIs (in-progress conversations remain visible).
    """
    days = retention_days_override if retention_days_override is not None else retention_days()
    effective_tz = resolve_effective_project_timezone(project_timezone)
    reference = now or pendulum.now("UTC")
    cutoff_local = reference.in_timezone(effective_tz).start_of("day").subtract(days=days)
    return cutoff_local.in_timezone("UTC")


def conversation_eligibility_timestamp(conversation: Conversation) -> pendulum.DateTime:
    raw = conversation.end_date or conversation.start_date or conversation.created_at
    return pendulum.instance(raw).in_timezone("UTC")


def is_conversation_within_retention(
    conversation: Conversation,
    project_timezone: str | None,
    *,
    retention_days_override: int | None = None,
    now: pendulum.DateTime | None = None,
) -> bool:
    if str(conversation.resolution) == RESOLUTION_IN_PROGRESS:
        return True
    cutoff = retention_cutoff_utc(
        project_timezone,
        retention_days_override=retention_days_override,
        now=now,
    )
    return conversation_eligibility_timestamp(conversation) >= cutoff


def apply_retention_filter(
    queryset: QuerySet[Conversation],
    project_timezone: str | None,
    *,
    retention_days_override: int | None = None,
    now: pendulum.DateTime | None = None,
) -> QuerySet[Conversation]:
    cutoff = retention_cutoff_utc(
        project_timezone,
        retention_days_override=retention_days_override,
        now=now,
    )
    return queryset.annotate(
        _retention_eligible_ts=Coalesce("end_date", "start_date", "created_at"),
    ).filter(Q(resolution=RESOLUTION_IN_PROGRESS) | Q(_retention_eligible_ts__gte=cutoff))


def apply_archive_eligibility_filter(
    queryset: QuerySet[Conversation],
    project_timezone: str | None,
    *,
    retention_days_override: int | None = None,
    now: pendulum.DateTime | None = None,
) -> QuerySet[Conversation]:
    """
    Closed conversations past retention with a Postgres message snapshot.

    In-progress conversations are excluded (same rules as archive dispatcher).
    """
    cutoff = retention_cutoff_utc(
        project_timezone,
        retention_days_override=retention_days_override,
        now=now,
    )
    return queryset.annotate(
        _archive_eligible_ts=Coalesce("end_date", "start_date", "created_at"),
    ).filter(
        ~Q(resolution=RESOLUTION_IN_PROGRESS),
        _archive_eligible_ts__lt=cutoff,
        messages_data__isnull=False,
    )
