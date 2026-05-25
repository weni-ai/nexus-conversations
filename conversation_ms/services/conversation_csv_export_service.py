"""
Export conversations for one project on a calendar day (project timezone) to CSV bytes.

Merges Postgres message snapshots with DynamoDB for in-progress conversations,
matching ConversationSerializer message routing.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date

import pendulum
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q

from conversation_ms.adapters.dynamo import DynamoMessageRepository
from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation, Project
from conversation_ms.utils.date_helpers import ProjectDay, resolve_effective_project_timezone

logger = logging.getLogger(__name__)

IN_PROGRESS = ResolutionEntities.IN_PROGRESS

DEFAULT_EXPORT_ITERATOR_CHUNK_SIZE = 500

CSV_HEADERS = [
    "conversation_uuid",
    "contact_urn",
    "project_uuid",
    "project_name",
    "start",
    "end",
    "Has_Room",
    "reason",
    "channel_uuid",
    "msgs",
]


def _project_timezone(project: Project) -> str:
    return resolve_effective_project_timezone(project.timezone)


def resolve_target_date(project: Project, target_date: str | date | None) -> str:
    if target_date is not None:
        if isinstance(target_date, date):
            return target_date.isoformat()
        return str(target_date).strip()
    return pendulum.now(_project_timezone(project)).format("YYYY-MM-DD")


def _day_bounds_utc(project: Project, day: str) -> tuple[pendulum.DateTime, pendulum.DateTime]:
    tz_name = _project_timezone(project)
    return ProjectDay.for_date(day, tz_name).get_utc_range()


def _postgres_messages(conv: Conversation) -> list:
    try:
        return list(conv.messages_data.messages or [])
    except ObjectDoesNotExist:
        return []
    except AttributeError:
        return []
    except Exception:
        logger.exception(
            "[conversation_csv_export] Postgres messages fetch failed conversation_uuid=%s",
            conv.uuid,
        )
        return []


def _dynamo_messages(conv: Conversation) -> list:
    urn, ch = conv.contact_urn, conv.channel_uuid
    if not urn or not ch:
        return []
    try:
        repo = DynamoMessageRepository()
        items, cursor = [], None
        while True:
            page = repo.get_messages(
                project_uuid=str(conv.project_id),
                contact_urn=urn,
                channel_uuid=str(ch),
                limit=500,
                cursor=cursor,
            )
            batch = page.get("items") or []
            items.extend(batch)
            cursor = page.get("next_cursor")
            if not cursor or not batch:
                break
        return items
    except Exception:
        logger.exception(
            "[conversation_csv_export] Dynamo fetch failed conversation_uuid=%s",
            conv.uuid,
        )
        return []


def _merged_raw_messages(conv: Conversation) -> list:
    pg = _postgres_messages(conv)
    if str(conv.resolution) == IN_PROGRESS:
        dyn = _dynamo_messages(conv)
        return dyn if dyn else pg
    if pg:
        return pg
    return _dynamo_messages(conv)


def format_msgs_cell(conv: Conversation) -> str:
    raw = _merged_raw_messages(conv)
    parts = []
    for m in sorted(raw, key=lambda x: x.get("created_at") or ""):
        text = (m.get("text") or "").replace("\n", " ")
        src = (m.get("source") or "").lower()
        prefix = "o:" if src in ("outgoing", "agent", "assistant") else "i:"
        parts.append(prefix + text)
    return repr(parts)


def format_reason_cell(conv: Conversation) -> str:
    try:
        cc = conv.classification
    except ObjectDoesNotExist:
        return ""
    if cc.subtopic_id and cc.subtopic:
        return cc.subtopic.name or ""
    if cc.topic_id and cc.topic:
        return cc.topic.name or ""
    return ""


def conversations_queryset(project_uuid: str, start_utc: pendulum.DateTime, end_utc: pendulum.DateTime):
    return (
        Conversation.objects.filter(project__uuid=project_uuid)
        .filter(
            Q(start_date__gte=start_utc, start_date__lte=end_utc)
            | Q(created_at__gte=start_utc, created_at__lte=end_utc)
        )
        .select_related(
            "project",
            "classification",
            "classification__topic",
            "classification__subtopic",
            "messages_data",
        )
        .order_by("start_date", "created_at")
    )


def export_conversations_csv_bytes(
    project_uuid: str,
    target_date: str | date | None = None,
    iterator_chunk_size: int = DEFAULT_EXPORT_ITERATOR_CHUNK_SIZE,
) -> tuple[bytes, int, str]:
    """
    Build UTF-8 CSV for one project and local calendar day.

    Returns (csv_bytes, row_count, resolved_target_date YYYY-MM-DD).
    """
    project = Project.objects.get(uuid=project_uuid)
    day = resolve_target_date(project, target_date)
    start_utc, end_utc = _day_bounds_utc(project, day)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_HEADERS, extrasaction="ignore")
    writer.writeheader()

    row_count = 0
    qs = conversations_queryset(str(project_uuid), start_utc, end_utc)
    for conv in qs.iterator(chunk_size=iterator_chunk_size):
        writer.writerow(
            {
                "conversation_uuid": str(conv.uuid),
                "contact_urn": conv.contact_urn or "",
                "project_uuid": str(conv.project_id),
                "project_name": conv.project.name or "",
                "start": conv.start_date.isoformat() if conv.start_date else "",
                "end": conv.end_date.isoformat() if conv.end_date else "",
                "Has_Room": conv.has_chats_room,
                "reason": format_reason_cell(conv),
                "channel_uuid": str(conv.channel_uuid) if conv.channel_uuid else "",
                "msgs": format_msgs_cell(conv),
            }
        )
        row_count += 1

    return buffer.getvalue().encode("utf-8"), row_count, day
