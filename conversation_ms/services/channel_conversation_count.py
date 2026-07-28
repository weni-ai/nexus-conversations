"""Count finalized conversations for a channel in a project calendar range."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from conversation_ms.models import Conversation, Project
from conversation_ms.services.reconcile_cohort_export import date_window_q, terminal_classification_q
from conversation_ms.services.reconcile_window import (
    calendar_range_day_count,
    format_reconcile_utc_instant,
    project_day_utc_bounds,
)
from conversation_ms.utils.date_helpers import resolve_effective_project_timezone

MAX_CHANNEL_COUNT_DAYS = 31


class ChannelConversationCountError(Exception):
    """Base error for channel conversation count."""


class ProjectNotFoundError(ChannelConversationCountError):
    pass


class ChannelProjectNotFoundError(ChannelConversationCountError):
    pass


class AmbiguousChannelProjectError(ChannelConversationCountError):
    def __init__(self, channel_uuid: UUID, project_uuids: list[str]):
        self.channel_uuid = channel_uuid
        self.project_uuids = project_uuids
        super().__init__("channel_uuid maps to more than one project; pass project_uuid")


@dataclass(frozen=True)
class ChannelConversationCountResult:
    project_uuid: UUID
    channel_uuid: UUID
    timezone: str
    start: date
    end: date
    start_utc: str
    end_utc: str
    count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_uuid": str(self.project_uuid),
            "channel_uuid": str(self.channel_uuid),
            "timezone": self.timezone,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "count": self.count,
        }


def _resolve_project(channel_uuid: UUID, project_uuid: UUID | None) -> Project:
    if project_uuid is not None:
        project = Project.objects.filter(uuid=project_uuid).first()
        if project is None:
            raise ProjectNotFoundError("Project not found")
        return project

    project_ids = list(
        Conversation.objects.filter(channel_uuid=channel_uuid)
        .values_list("project_id", flat=True)
        .distinct()
        .order_by("project_id")
    )
    if not project_ids:
        raise ChannelProjectNotFoundError("channel_uuid has no projects")
    if len(project_ids) > 1:
        raise AmbiguousChannelProjectError(
            channel_uuid=channel_uuid,
            project_uuids=[str(pid) for pid in project_ids],
        )
    project = Project.objects.filter(pk=project_ids[0]).first()
    if project is None:
        raise ProjectNotFoundError("Project not found")
    return project


def count_channel_conversations(
    *,
    channel_uuid: UUID,
    start: date,
    end: date,
    project_uuid: UUID | None = None,
) -> ChannelConversationCountResult:
    if end < start:
        raise ValueError("end must be on or after start")
    if calendar_range_day_count(start, end) > MAX_CHANNEL_COUNT_DAYS:
        raise ValueError(f"Date range spans more than {MAX_CHANNEL_COUNT_DAYS} days")

    project = _resolve_project(channel_uuid, project_uuid)
    tz_name = resolve_effective_project_timezone(project.timezone)
    start_utc, _ = project_day_utc_bounds(start, tz_name)
    _, end_utc = project_day_utc_bounds(end, tz_name)

    cfg = {
        "date_start": format_reconcile_utc_instant(start_utc),
        "date_end": format_reconcile_utc_instant(end_utc),
        "use_date_end": True,
    }
    count = (
        Conversation.objects.filter(
            project_id=project.uuid,
            channel_uuid=channel_uuid,
        )
        .filter(terminal_classification_q())
        .filter(date_window_q(cfg))
        .count()
    )

    return ChannelConversationCountResult(
        project_uuid=project.uuid,
        channel_uuid=channel_uuid,
        timezone=tz_name,
        start=start,
        end=end,
        start_utc=format_reconcile_utc_instant(start_utc),
        end_utc=format_reconcile_utc_instant(end_utc),
        count=count,
    )
