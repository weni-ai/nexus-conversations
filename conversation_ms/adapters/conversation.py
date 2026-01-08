"""
Conversation adapter for updating conversation data.
Adapted from nexus.usecases.inline_agents.update.
"""

import logging

from conversation_ms.models import Conversation

logger = logging.getLogger(__name__)


def update_conversation_data(to_update: dict, project_uuid: str, contact_urn: str, channel_uuid: str):
    """
    Update conversation data fields.
    Adapted from nexus.usecases.inline_agents.update.update_conversation_data.
    """
    conversation = (
        Conversation.objects.filter(project__uuid=project_uuid, contact_urn=contact_urn, channel_uuid=channel_uuid)
        .order_by("-created_at")
        .first()
    )
    if not conversation:
        logger.warning(
            "[update_conversation_data] Conversation not found",
            extra={"project_uuid": project_uuid, "contact_urn": contact_urn, "channel_uuid": channel_uuid},
        )
        return
    for field, value in to_update.items():
        setattr(conversation, field, value)
    conversation.save()
    logger.debug(
        "[update_conversation_data] Conversation updated",
        extra={"conversation_uuid": str(conversation.uuid), "updated_fields": list(to_update.keys())},
    )

