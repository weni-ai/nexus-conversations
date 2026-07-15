"""Map S3 archive documents (schema_version 1) to Supervisor Public V2 JSON."""

from __future__ import annotations

from typing import Any

# Keep labels aligned with Conversation.RESOLUTION_CHOICES without importing the model
# (adapter must stay usable when the Conversation row no longer exists).
RESOLUTION_STATUS_LABELS = {
    "0": "Resolved",
    "1": "Unresolved",
    "2": "In Progress",
    "3": "Unclassified",
    "4": "Has Chat Room",
}


def _normalize_message(raw: Any) -> dict[str, str] | None:
    if not isinstance(raw, dict):
        return None
    text = raw.get("text")
    source = raw.get("source")
    created_at = raw.get("created_at")
    if text is None and source is None and created_at is None:
        return None
    return {
        "text": "" if text is None else str(text),
        "source": "" if source is None else str(source),
        "created_at": "" if created_at is None else str(created_at),
    }


def archive_payload_to_supervisor_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Transform schema_version 1 archive JSON into Supervisor Public V2 item shape.

    Additive support fields: ``archived_at``, ``is_archived``.
    """
    if not isinstance(payload, dict):
        raise TypeError("archive payload must be a dict")

    conversation = payload.get("conversation") or {}
    if not isinstance(conversation, dict):
        conversation = {}

    classification = payload.get("classification")
    topic = None
    if isinstance(classification, dict):
        topic = classification.get("topic")

    resolution = conversation.get("resolution")
    resolution_key = "" if resolution is None else str(resolution)
    status = RESOLUTION_STATUS_LABELS.get(resolution_key, resolution_key)

    channel_uuid = conversation.get("channel_uuid")
    messages_raw = payload.get("messages") or []
    messages: list[dict[str, str]] = []
    if isinstance(messages_raw, list):
        for item in messages_raw:
            normalized = _normalize_message(item)
            if normalized is not None:
                messages.append(normalized)

    return {
        "conversation_uuid": str(conversation.get("uuid") or ""),
        "start_date": conversation.get("start_date"),
        "created_at": conversation.get("created_at"),
        "ended_at": conversation.get("end_date"),
        "status": status,
        "topic": topic or "",
        "channel_uuid": str(channel_uuid) if channel_uuid else None,
        "contact_urn": conversation.get("contact_urn") or "",
        "messages": messages,
        "archived_at": payload.get("archived_at"),
        "is_archived": True,
    }
