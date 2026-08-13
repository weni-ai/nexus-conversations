from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from django.conf import settings

from conversation_ms.models import Conversation
from improvements.services.agent_traces_service import fetch_agent_traces

logger = logging.getLogger(__name__)

_RESOLUTION_LABELS = {
    "0": "Resolved",
    "1": "Unresolved",
    "2": "In Progress",
    "3": "Unclassified",
    "4": "Has Chat Room",
}


def _to_iso_datetime(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        formatted = value.isoformat()
        if formatted.endswith("+00:00"):
            return f"{formatted[:-6]}Z"
        return formatted
    return str(value)


def _resolution_label(resolution: str | None) -> str:
    if resolution is None:
        return _RESOLUTION_LABELS["3"]
    return _RESOLUTION_LABELS.get(str(resolution), str(resolution))


def _resolution_as_int(resolution: str | None) -> int:
    try:
        return int(resolution or 0)
    except (TypeError, ValueError):
        return 0


def _get_classification_fields(conversation: Conversation) -> dict[str, str]:
    classification = getattr(conversation, "classification", None)
    if classification is None:
        return {"topic": "", "subtopic": ""}

    topic = getattr(classification, "topic", None)
    subtopic = getattr(classification, "subtopic", None)
    return {
        "topic": topic.name if topic else "",
        "subtopic": subtopic.name if subtopic else "",
    }


def build_conversation_detail(conversation: Conversation) -> dict[str, Any]:
    classification = _get_classification_fields(conversation)
    return {
        "conversation_uuid": str(conversation.uuid),
        "contact_urn": conversation.contact_urn or "",
        "status": _resolution_label(conversation.resolution),
        "topic": classification["topic"],
        "channel_uuid": str(conversation.channel_uuid) if conversation.channel_uuid else "",
        "created_at": _to_iso_datetime(conversation.start_date or conversation.created_at),
        "ended_at": _to_iso_datetime(conversation.end_date),
    }


def build_listing_item(conversation: Conversation) -> dict[str, Any]:
    classification = _get_classification_fields(conversation)
    return {
        "uuid": str(conversation.uuid),
        "contact_name": conversation.contact_name or conversation.contact_urn or "",
        "contact_urn": conversation.contact_urn or "",
        "status": _resolution_label(conversation.resolution),
        "resolution": _resolution_as_int(conversation.resolution),
        "start_date": _to_iso_datetime(conversation.start_date),
        "end_date": _to_iso_datetime(conversation.end_date),
        "classification": {
            "topic": classification["topic"],
            "subtopic": classification["subtopic"],
        },
    }


def _get_message_uuid(message: dict[str, Any]) -> str:
    return str(message.get("uuid") or message.get("message_id") or message.get("id") or "")


def _iter_outgoing_message_log_ids(conversation: Conversation) -> list[tuple[str, str]]:
    messages_data = getattr(conversation, "messages_data", None)
    raw_messages = messages_data.messages if messages_data is not None else []

    log_ids: list[tuple[str, str]] = []
    for message in raw_messages:
        if str(message.get("source") or "").strip().lower() != "outgoing":
            continue
        log_id = _get_message_uuid(message).strip()
        if not log_id:
            continue
        log_ids.append((log_id, log_id))
    return log_ids


def build_all_messages(conversation: Conversation) -> list[dict[str, Any]]:
    messages_data = getattr(conversation, "messages_data", None)
    raw_messages = messages_data.messages if messages_data is not None else []
    sorted_messages = sorted(raw_messages, key=lambda message: message.get("created_at") or "")

    formatted_messages: list[dict[str, Any]] = []
    for message in sorted_messages:
        message_uuid = _get_message_uuid(message)
        formatted_messages.append(
            {
                "uuid": message_uuid,
                "id": message_uuid,
                "created_at": str(message.get("created_at") or ""),
                "source": str(message.get("source") or ""),
                "text": str(message.get("text") or ""),
            }
        )
    return formatted_messages


def get_traces_by_message_id(conversation: Conversation) -> dict[str, list[dict[str, Any]]]:
    project_uuid = str(conversation.project.uuid)
    log_ids = _iter_outgoing_message_log_ids(conversation)
    if not log_ids:
        return {}

    max_workers = int(getattr(settings, "IMPROVEMENTS_TRACES_MAX_WORKERS", 8))
    worker_count = max(1, min(max_workers, len(log_ids)))
    traces_by_message_id: dict[str, list[dict[str, Any]]] = {}
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_key = {
            executor.submit(fetch_agent_traces, project_uuid, log_id): message_key
            for message_key, log_id in log_ids
        }
        for future in as_completed(future_to_key):
            message_key = future_to_key[future]
            traces = future.result()
            if traces:
                traces_by_message_id[message_key] = traces

    logger.info(
        "[get_traces_by_message_id] Fetched traces conversation_uuid=%s project_uuid=%s "
        "message_count=%s hit_count=%s workers=%s elapsed_seconds=%.2f",
        conversation.uuid,
        project_uuid,
        len(log_ids),
        len(traces_by_message_id),
        worker_count,
        time.monotonic() - started,
    )
    return traces_by_message_id


def build_raw_conversation(conversation: Conversation) -> dict[str, Any]:
    return {
        "detail": build_conversation_detail(conversation),
        "listing_item": build_listing_item(conversation),
        "all_messages": build_all_messages(conversation),
        "traces_by_message_id": get_traces_by_message_id(conversation),
    }


def iter_raw_conversations(conversations: Iterable[Conversation]) -> Iterator[dict[str, Any]]:
    for conversation in conversations:
        yield build_raw_conversation(conversation)


def build_raw_conversations(conversations: list[Conversation]) -> dict[str, list[dict[str, Any]]]:
    return {
        "raw_conversations": list(iter_raw_conversations(conversations)),
    }
