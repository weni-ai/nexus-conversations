"""Build canonical S3 archive payloads for closed conversations."""

from __future__ import annotations

import gzip
import hashlib
import json
from typing import Any
from uuid import UUID

import pendulum
from django.conf import settings
from django.utils import timezone

from conversation_ms.archive.eligibility import conversation_eligibility_timestamp, retention_days
from conversation_ms.models import Conversation

ARCHIVE_SCHEMA_VERSION = 1
SOURCE_SERVICE = "nexus-conversations"


def _isoformat_datetime(value) -> str | None:
    if value is None:
        return None
    if timezone.is_aware(value):
        return value.isoformat()
    return pendulum.instance(value, tz="UTC").isoformat()


def _serialize_classification(conversation: Conversation) -> dict[str, Any] | None:
    try:
        classification = conversation.classification
    except Exception:
        return None

    return {
        "topic": classification.topic.name if classification.topic_id else None,
        "subtopic": classification.subtopic.name if classification.subtopic_id else None,
        "confidence": classification.confidence,
        "topic_uuid": str(classification.topic_id) if classification.topic_id else None,
        "subtopic_uuid": str(classification.subtopic_id) if classification.subtopic_id else None,
    }


def _serialize_conversation(conversation: Conversation) -> dict[str, Any]:
    project_uuid = str(conversation.project_id)
    return {
        "uuid": str(conversation.uuid),
        "project_uuid": project_uuid,
        "contact_urn": conversation.contact_urn,
        "contact_name": conversation.contact_name,
        "ticket_uuid": str(conversation.ticket_uuid) if conversation.ticket_uuid else None,
        "external_id": conversation.external_id,
        "start_date": _isoformat_datetime(conversation.start_date),
        "end_date": _isoformat_datetime(conversation.end_date),
        "resolution": str(conversation.resolution),
        "channel_uuid": str(conversation.channel_uuid) if conversation.channel_uuid else None,
        "nps": conversation.nps,
        "csat": conversation.csat,
        "has_chats_room": conversation.has_chats_room,
        "created_at": _isoformat_datetime(conversation.created_at),
    }


def _serialize_messages(conversation: Conversation) -> list[dict[str, Any]]:
    try:
        messages_row = conversation.messages_data
    except Exception:
        return []
    return list(messages_row.messages or [])


def build_s3_key(
    *,
    prefix: str,
    project_uuid: UUID | str,
    conversation_uuid: UUID | str,
    eligibility_ts: pendulum.DateTime,
) -> str:
    utc_ts = eligibility_ts.in_timezone("UTC")
    return f"{prefix.strip('/')}/{project_uuid}/{utc_ts.year:04d}/{utc_ts.month:02d}/{conversation_uuid}.json.gz"


def canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_archive_artifact(conversation: Conversation) -> tuple[dict[str, Any], bytes, str, str]:
    """
    Build archive document, gzip body, content SHA-256, and deterministic S3 key.

    Returns:
        (payload dict, gzipped bytes, content_sha256 hex, s3_key)
    """
    archived_at = timezone.now()
    eligibility_ts = conversation_eligibility_timestamp(conversation)
    prefix = getattr(settings, "CONVERSATION_ARCHIVE_S3_PREFIX", "conversations-archive")

    payload_without_hash: dict[str, Any] = {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "archived_at": archived_at.isoformat(),
        "conversation": _serialize_conversation(conversation),
        "messages": _serialize_messages(conversation),
        "classification": _serialize_classification(conversation),
        "metadata": {
            "source_service": SOURCE_SERVICE,
            "retention_days": retention_days(),
            "content_sha256": "",
        },
    }
    content_sha256 = sha256_hex(canonical_json_bytes(payload_without_hash))
    payload = {
        **payload_without_hash,
        "metadata": {
            **payload_without_hash["metadata"],
            "content_sha256": content_sha256,
        },
    }
    uncompressed = canonical_json_bytes(payload)
    gzipped = gzip.compress(uncompressed)
    s3_key = build_s3_key(
        prefix=prefix,
        project_uuid=conversation.project_id,
        conversation_uuid=conversation.uuid,
        eligibility_ts=eligibility_ts,
    )
    return payload, gzipped, content_sha256, s3_key
