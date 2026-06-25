from __future__ import annotations

from typing import Any

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation, ConversationMessages
from conversation_ms.repositories.message_repository import MessageRepository
from improvements.models import ImprovementBacklogItem, ImprovementRunConversation


def _normalize_source(source: Any) -> str:
    if source == "user":
        return "incoming"
    if source in ("agent", "assistant"):
        return "outgoing"
    return str(source) if source is not None else "incoming"


def _normalize_postgres_message(msg: dict[str, Any]) -> dict[str, Any] | None:
    msg_uuid = msg.get("message_id") or msg.get("uuid")
    if not msg_uuid:
        return None
    msg_uuid = str(msg_uuid)
    return {
        "uuid": msg_uuid,
        "id": msg_uuid,
        "text": msg.get("text"),
        "source": _normalize_source(msg.get("source")),
        "created_at": msg.get("created_at"),
    }


def _normalize_dynamo_message(item: dict[str, Any]) -> dict[str, Any] | None:
    msg_uuid = item.get("message_id")
    if not msg_uuid:
        return None
    msg_uuid = str(msg_uuid)
    return {
        "uuid": msg_uuid,
        "id": item.get("id") or msg_uuid,
        "text": item.get("text"),
        "source": _normalize_source(item.get("source")),
        "created_at": item.get("created_at"),
    }


def _load_from_postgres(conversation: Conversation) -> list[dict[str, Any]] | None:
    try:
        raw_messages = conversation.messages_data.messages
    except ConversationMessages.DoesNotExist:
        return None

    normalized: list[dict[str, Any]] = []
    for msg in raw_messages or []:
        if not isinstance(msg, dict):
            continue
        entry = _normalize_postgres_message(msg)
        if entry is not None:
            normalized.append(entry)
    return normalized


def _load_from_dynamo(conversation: Conversation) -> list[dict[str, Any]] | None:
    try:
        repo = MessageRepository()
        items = repo.get_messages_from_dynamo(
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
        )
    except Exception:
        return None

    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        entry = _normalize_dynamo_message(item)
        if entry is not None:
            normalized.append(entry)
    return normalized


def load_conversation_messages(conversation: Conversation) -> list[dict[str, Any]]:
    if str(conversation.resolution) == str(ResolutionEntities.IN_PROGRESS):
        messages = _load_from_dynamo(conversation) or _load_from_postgres(conversation) or []
    else:
        messages = _load_from_postgres(conversation) or _load_from_dynamo(conversation) or []
    return messages


def filter_messages_by_uuids(
    all_messages: list[dict[str, Any]],
    uuids: list[str],
) -> list[dict[str, Any]]:
    if not uuids:
        return []

    index = {str(message["uuid"]): message for message in all_messages if message.get("uuid")}
    filtered: list[dict[str, Any]] = []
    for message_uuid in uuids:
        message = index.get(str(message_uuid))
        if message is not None:
            filtered.append(message)
    return filtered


def extract_message_uuids(evidence: list[Any]) -> list[str]:
    message_uuids: list[str] = []
    for entry in evidence:
        if isinstance(entry, str) and entry:
            message_uuids.append(entry)
            continue
        if isinstance(entry, dict):
            message_uuid = entry.get("message_uuid")
            if message_uuid:
                message_uuids.append(str(message_uuid))
    return message_uuids


def message_uuids_from_dimension_results(dimension_results: list[Any]) -> list[str]:
    for result in dimension_results:
        if not isinstance(result, dict):
            continue
        message_uuids = result.get("message_uuids_relevant")
        if isinstance(message_uuids, list):
            return [str(uuid) for uuid in message_uuids if uuid]
    return []


def resolve_conversation_message_uuids(
    item: ImprovementBacklogItem,
    link,
) -> list[str]:
    messages = extract_message_uuids(link.evidence or [])
    if messages:
        return messages

    run_conversation = (
        ImprovementRunConversation.objects.filter(
            run_id=item.run_id,
            conversation_id=link.conversation_id,
        )
        .only("dimension_results")
        .first()
    )
    if run_conversation is None:
        return []

    return message_uuids_from_dimension_results(run_conversation.dimension_results or [])


def map_affected_conversation(
    item: ImprovementBacklogItem,
    link,
) -> dict[str, Any]:
    conversation = link.conversation
    message_uuids = resolve_conversation_message_uuids(item, link)
    all_messages = load_conversation_messages(conversation)
    return {
        "uuid": str(conversation.uuid),
        "contact_urn": conversation.contact_urn or "",
        "contact_name": conversation.contact_name or "",
        "messages": filter_messages_by_uuids(all_messages, message_uuids),
    }
