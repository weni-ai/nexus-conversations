from __future__ import annotations

from typing import Any
from uuid import UUID

from conversation_ms.clients.nexus_client import NexusClient
from conversation_ms.models import Conversation
from improvements.models import ImprovementBacklogItemConversation
from improvements.services.improvements_detail_service import (
    get_backlog_item,
    get_improvement_detail,
)

MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET = 10

_IMPROVEMENT_ITEM_KEYS = (
    "uuid",
    "text",
    "type",
    "description",
    "suggested_change",
    "affected_instructions",
)


def _format_started_at(conversation: Conversation) -> str | None:
    if conversation.start_date is None:
        return None
    value = conversation.start_date.isoformat()
    if value.endswith("+00:00"):
        return value.replace("+00:00", "Z")
    return value


def _map_support_ticket_conversation(link: ImprovementBacklogItemConversation) -> dict[str, Any]:
    conversation = link.conversation
    return {
        "uuid": str(conversation.uuid),
        "contact_urn": conversation.contact_urn or "",
        "contact_name": conversation.contact_name or "",
        "started_at": _format_started_at(conversation),
    }


def build_open_support_ticket_payload(
    project_uuid: UUID | str,
    improvement_uuid: UUID | str,
    *,
    user_email: str,
) -> dict[str, Any]:
    detail = get_improvement_detail(project_uuid, improvement_uuid)
    item = get_backlog_item(project_uuid, improvement_uuid)
    improvement_item = {key: detail[key] for key in _IMPROVEMENT_ITEM_KEYS}

    links = item.affected_conversations.select_related("conversation").order_by(
        "created_at",
        "pk",
    )[:MAX_AFFECTED_CONVERSATIONS_FOR_SUPPORT_TICKET]

    return {
        "improvement_item": improvement_item,
        "affected_conversations": [_map_support_ticket_conversation(link) for link in links],
        "project_uuid": str(project_uuid),
        "user_email": user_email,
    }


def open_support_ticket_for_improvement(
    project_uuid: UUID | str,
    improvement_uuid: UUID | str,
    *,
    user_email: str,
    nexus_client: NexusClient | None = None,
) -> Any:
    payload = build_open_support_ticket_payload(
        project_uuid,
        improvement_uuid,
        user_email=user_email,
    )
    client = nexus_client or NexusClient()
    return client.open_support_ticket(str(project_uuid), payload)
