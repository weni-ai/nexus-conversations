"""Export/import Conversation domain data between environments (JSON v1)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from datetime import timezone as stdlib_utc
from typing import Any
from uuid import UUID

import pendulum
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.db.models import Q
from django.utils import timezone as dj_tz

from conversation_ms.adapters.dynamo import DynamoMessageRepository
from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import (
    Conversation,
    ConversationClassification,
    ConversationMessages,
    Project,
    SubTopic,
    Topic,
)
from conversation_ms.utils.date_helpers import ProjectDay, resolve_effective_project_timezone

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION})
DEFAULT_ITERATOR_CHUNK_SIZE = 500
IN_PROGRESS = ResolutionEntities.IN_PROGRESS


@dataclass
class ExportDateRange:
    start_date: date | None
    end_date: date | None
    timezone: str
    start_utc: pendulum.DateTime | None
    end_utc: pendulum.DateTime | None


@dataclass
class ImportStats:
    created: dict[str, int] = field(default_factory=dict)
    updated: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, int] = field(default_factory=dict)

    def bump(self, entity: str, action: str, count: int = 1) -> None:
        bucket = getattr(self, action)
        bucket[entity] = bucket.get(entity, 0) + count


def resolve_export_date_range(
    project: Project,
    start_date: date | None,
    end_date: date | None,
) -> ExportDateRange:
    if start_date is None and end_date is None:
        tz = resolve_effective_project_timezone(project.timezone)
        return ExportDateRange(None, None, tz, None, None)
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date must both be provided or both omitted")
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")

    tz = resolve_effective_project_timezone(project.timezone)
    start_day, end_day = ProjectDay.for_date_range(
        start_date.isoformat(),
        end_date.isoformat(),
        tz,
    )
    return ExportDateRange(
        start_date=start_date,
        end_date=end_date,
        timezone=tz,
        start_utc=start_day.start_of_day_utc,
        end_utc=end_day.end_of_day_utc,
    )


def build_export_queryset(
    project_uuid: str | UUID,
    date_range: ExportDateRange,
):
    qs = Conversation.objects.filter(project__uuid=project_uuid).select_related(
        "project",
        "classification",
        "classification__topic",
        "classification__subtopic",
        "messages_data",
    )
    if date_range.start_utc is not None and date_range.end_utc is not None:
        qs = qs.filter(
            Q(start_date__gte=date_range.start_utc, start_date__lte=date_range.end_utc)
            | Q(created_at__gte=date_range.start_utc, created_at__lte=date_range.end_utc)
        )
    return qs.order_by("start_date", "created_at")


def _postgres_messages(conv: Conversation) -> list:
    try:
        return list(conv.messages_data.messages or [])
    except ObjectDoesNotExist:
        return []
    except AttributeError:
        return []
    except Exception:
        logger.exception(
            "[project_data_transfer] Postgres messages fetch failed conversation_uuid=%s",
            conv.uuid,
        )
        return []


def fetch_dynamo_messages(conv: Conversation) -> list:
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
            "[project_data_transfer] Dynamo fetch failed conversation_uuid=%s",
            conv.uuid,
        )
        return []


def get_conversation_messages(conv: Conversation, *, include_dynamo: bool = False) -> list:
    pg = _postgres_messages(conv)
    if not include_dynamo:
        return pg
    if str(conv.resolution) == IN_PROGRESS:
        dyn = fetch_dynamo_messages(conv)
        return dyn if dyn else pg
    if pg:
        return pg
    return fetch_dynamo_messages(conv)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if dj_tz.is_naive(value):
        value = dj_tz.make_aware(value, stdlib_utc.utc)
    return value.isoformat()


def _serialize_topic(topic: Topic) -> dict[str, Any]:
    return {
        "uuid": str(topic.uuid),
        "name": topic.name,
        "description": topic.description,
        "project_id": str(topic.project_id),
        "is_active": topic.is_active,
        "created_at": _serialize_datetime(topic.created_at),
        "updated_at": _serialize_datetime(topic.updated_at),
    }


def _serialize_subtopic(subtopic: SubTopic) -> dict[str, Any]:
    return {
        "uuid": str(subtopic.uuid),
        "name": subtopic.name,
        "description": subtopic.description,
        "topic_id": str(subtopic.topic_id),
        "is_active": subtopic.is_active,
        "created_at": _serialize_datetime(subtopic.created_at),
        "updated_at": _serialize_datetime(subtopic.updated_at),
    }


def _serialize_conversation(conv: Conversation) -> dict[str, Any]:
    return {
        "uuid": str(conv.uuid),
        "created_at": _serialize_datetime(conv.created_at),
        "contact_urn": conv.contact_urn,
        "ticket_uuid": str(conv.ticket_uuid) if conv.ticket_uuid else None,
        "project_id": str(conv.project_id),
        "external_id": conv.external_id,
        "start_date": _serialize_datetime(conv.start_date),
        "end_date": _serialize_datetime(conv.end_date),
        "has_chats_room": conv.has_chats_room,
        "contact_name": conv.contact_name,
        "channel_uuid": str(conv.channel_uuid) if conv.channel_uuid else None,
        "nps": conv.nps,
        "csat": conv.csat,
        "resolution": conv.resolution,
    }


def _serialize_classification(classification: ConversationClassification) -> dict[str, Any]:
    return {
        "uuid": str(classification.uuid),
        "conversation_id": str(classification.conversation_id),
        "topic_id": str(classification.topic_id) if classification.topic_id else None,
        "subtopic_id": str(classification.subtopic_id) if classification.subtopic_id else None,
        "confidence": classification.confidence,
        "created_at": _serialize_datetime(classification.created_at),
        "updated_at": _serialize_datetime(classification.updated_at),
    }


def collect_project_topics_and_subtopics(
    project_uuid: str | UUID,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Export all topics and subtopics belonging to the project (independent of date filter)."""
    topics_by_uuid: dict[str, dict[str, Any]] = {}
    subtopics_by_uuid: dict[str, dict[str, Any]] = {}

    for topic in Topic.objects.filter(project_id=project_uuid).order_by("uuid"):
        topics_by_uuid[str(topic.uuid)] = _serialize_topic(topic)

    for subtopic in SubTopic.objects.filter(topic__project_id=project_uuid).select_related("topic").order_by("uuid"):
        subtopics_by_uuid[str(subtopic.uuid)] = _serialize_subtopic(subtopic)

    return topics_by_uuid, subtopics_by_uuid


def _serialize_filters(date_range: ExportDateRange) -> dict[str, Any] | None:
    if date_range.start_date is None:
        return None
    return {
        "start_date": date_range.start_date.isoformat(),
        "end_date": date_range.end_date.isoformat() if date_range.end_date else None,
        "timezone": date_range.timezone,
        "start_utc": date_range.start_utc.isoformat() if date_range.start_utc else None,
        "end_utc": date_range.end_utc.isoformat() if date_range.end_utc else None,
    }


def export_project_data(
    project_uuid: str | UUID,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    include_dynamo: bool = False,
    iterator_chunk_size: int = DEFAULT_ITERATOR_CHUNK_SIZE,
) -> dict[str, Any]:
    project = Project.objects.get(uuid=project_uuid)
    date_range = resolve_export_date_range(project, start_date, end_date)

    topics_by_uuid, subtopics_by_uuid = collect_project_topics_and_subtopics(project_uuid)
    conversations: list[dict[str, Any]] = []
    classifications: list[dict[str, Any]] = []
    conversation_messages: list[dict[str, Any]] = []

    qs = build_export_queryset(project_uuid, date_range)
    for conv in qs.iterator(chunk_size=iterator_chunk_size):
        conversations.append(_serialize_conversation(conv))

        try:
            classification = conv.classification
        except ObjectDoesNotExist:
            classification = None

        if classification is not None:
            classifications.append(_serialize_classification(classification))

        messages = get_conversation_messages(conv, include_dynamo=include_dynamo)
        if messages:
            msg_created_at = None
            msg_updated_at = None
            try:
                msg_created_at = _serialize_datetime(conv.messages_data.created_at)
                msg_updated_at = _serialize_datetime(conv.messages_data.updated_at)
            except ObjectDoesNotExist:
                pass
            conversation_messages.append(
                {
                    "conversation_id": str(conv.uuid),
                    "messages": messages,
                    "created_at": msg_created_at,
                    "updated_at": msg_updated_at,
                }
            )

    return {
        "schema_version": SCHEMA_VERSION,
        "exported_at": pendulum.now("UTC").isoformat(),
        "source_project": {
            "uuid": str(project.uuid),
            "name": project.name,
            "timezone": project.timezone,
        },
        "filters": _serialize_filters(date_range),
        "topics": sorted(topics_by_uuid.values(), key=lambda row: row["uuid"]),
        "subtopics": sorted(subtopics_by_uuid.values(), key=lambda row: row["uuid"]),
        "conversations": conversations,
        "classifications": classifications,
        "conversation_messages": conversation_messages,
    }


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = pendulum.parse(value)
    dt = parsed.naive() if parsed.timezone_name == "UTC" else parsed.in_timezone("UTC").naive()
    return dj_tz.make_aware(dt, stdlib_utc.utc)


def _parse_uuid(value: str | None) -> UUID | None:
    if not value:
        return None
    return UUID(str(value))


def validate_export_document(data: dict[str, Any]) -> None:
    version = data.get("schema_version")
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(f"Unsupported schema_version: {version!r}")


def _upsert_model(
    model_cls,
    pk_field: str,
    pk_value,
    defaults: dict[str, Any],
    *,
    update_existing: bool,
    stats: ImportStats,
    entity_name: str,
) -> None:
    lookup = {pk_field: pk_value}
    if update_existing:
        _, created = model_cls.objects.update_or_create(defaults=defaults, **lookup)
        stats.bump(entity_name, "created" if created else "updated")
        return

    if model_cls.objects.filter(**lookup).exists():
        stats.bump(entity_name, "skipped")
        return
    model_cls.objects.create(**lookup, **defaults)
    stats.bump(entity_name, "created")


def import_project_data(
    data: dict[str, Any],
    target_project_uuid: str | UUID,
    *,
    update_existing: bool = False,
    sync_project_metadata: bool = False,
) -> ImportStats:
    validate_export_document(data)
    stats = ImportStats()
    target_uuid = UUID(str(target_project_uuid))
    source_project = data.get("source_project") or {}

    with transaction.atomic():
        project_defaults: dict[str, Any] = {}
        if sync_project_metadata:
            if source_project.get("name") is not None:
                project_defaults["name"] = source_project["name"]
            if source_project.get("timezone") is not None:
                project_defaults["timezone"] = source_project["timezone"]

        project, created = Project.objects.get_or_create(uuid=target_uuid, defaults=project_defaults)
        if created:
            stats.bump("projects", "created")
        elif update_existing and project_defaults:
            for key, value in project_defaults.items():
                setattr(project, key, value)
            project.save(update_fields=list(project_defaults.keys()))
            stats.bump("projects", "updated")
        elif not created:
            stats.bump("projects", "skipped")

        for row in data.get("topics") or []:
            _upsert_model(
                Topic,
                "uuid",
                _parse_uuid(row["uuid"]),
                {
                    "name": row["name"],
                    "description": row.get("description"),
                    "project_id": target_uuid,
                    "is_active": row.get("is_active", True),
                },
                update_existing=update_existing,
                stats=stats,
                entity_name="topics",
            )

        for row in data.get("subtopics") or []:
            topic_id = _parse_uuid(row["topic_id"])
            if topic_id and not Topic.objects.filter(uuid=topic_id).exists():
                logger.warning("[project_data_transfer] Skipping subtopic %s: topic %s missing", row["uuid"], topic_id)
                stats.bump("subtopics", "skipped")
                continue
            _upsert_model(
                SubTopic,
                "uuid",
                _parse_uuid(row["uuid"]),
                {
                    "name": row["name"],
                    "description": row.get("description"),
                    "topic_id": topic_id,
                    "is_active": row.get("is_active", True),
                },
                update_existing=update_existing,
                stats=stats,
                entity_name="subtopics",
            )

        for row in data.get("conversations") or []:
            _upsert_model(
                Conversation,
                "uuid",
                _parse_uuid(row["uuid"]),
                {
                    "contact_urn": row.get("contact_urn"),
                    "ticket_uuid": _parse_uuid(row.get("ticket_uuid")),
                    "project_id": target_uuid,
                    "external_id": row.get("external_id"),
                    "start_date": _parse_datetime(row.get("start_date")),
                    "end_date": _parse_datetime(row.get("end_date")),
                    "has_chats_room": row.get("has_chats_room", False),
                    "contact_name": row.get("contact_name"),
                    "channel_uuid": _parse_uuid(row.get("channel_uuid")),
                    "nps": row.get("nps"),
                    "csat": row.get("csat"),
                    "resolution": row.get("resolution", IN_PROGRESS),
                },
                update_existing=update_existing,
                stats=stats,
                entity_name="conversations",
            )

        for row in data.get("classifications") or []:
            conversation_id = _parse_uuid(row["conversation_id"])
            if conversation_id and not Conversation.objects.filter(uuid=conversation_id).exists():
                logger.warning(
                    "[project_data_transfer] Skipping classification %s: conversation %s missing",
                    row["uuid"],
                    conversation_id,
                )
                stats.bump("classifications", "skipped")
                continue
            _upsert_model(
                ConversationClassification,
                "uuid",
                _parse_uuid(row["uuid"]),
                {
                    "conversation_id": conversation_id,
                    "topic_id": _parse_uuid(row.get("topic_id")),
                    "subtopic_id": _parse_uuid(row.get("subtopic_id")),
                    "confidence": row.get("confidence", 0.0),
                },
                update_existing=update_existing,
                stats=stats,
                entity_name="classifications",
            )

        for row in data.get("conversation_messages") or []:
            conversation_id = _parse_uuid(row["conversation_id"])
            if conversation_id and not Conversation.objects.filter(uuid=conversation_id).exists():
                logger.warning(
                    "[project_data_transfer] Skipping messages for conversation %s: conversation missing",
                    conversation_id,
                )
                stats.bump("conversation_messages", "skipped")
                continue
            lookup = {"conversation_id": conversation_id}
            defaults = {"messages": row.get("messages") or []}
            if update_existing:
                _, created = ConversationMessages.objects.update_or_create(defaults=defaults, **lookup)
                stats.bump("conversation_messages", "created" if created else "updated")
            elif ConversationMessages.objects.filter(**lookup).exists():
                stats.bump("conversation_messages", "skipped")
            else:
                ConversationMessages.objects.create(**lookup, **defaults)
                stats.bump("conversation_messages", "created")

    return stats
