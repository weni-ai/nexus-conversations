"""
Conversation adapter for updating conversation data.
Adapted from nexus.usecases.inline_agents.update.
"""

import logging

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation
from conversation_ms.tasks import classify_conversation_task

logger = logging.getLogger(__name__)


def update_conversation_data(to_update: dict, project_uuid: str, contact_urn: str, channel_uuid: str):
    """
    Update conversation data fields.

    Automatically triggers message migration when resolution changes from IN_PROGRESS to another status.
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

    original_resolution = str(conversation.resolution)

    for field, value in to_update.items():
        setattr(conversation, field, value)
    conversation.save()

    current_resolution = str(conversation.resolution)
    if original_resolution == str(ResolutionEntities.IN_PROGRESS) and current_resolution != str(
        ResolutionEntities.IN_PROGRESS
    ):
        logger.info(
            "[update_conversation_data] Conversation closed, triggering message migration",
            extra={
                "conversation_uuid": str(conversation.uuid),
                "original_resolution": original_resolution,
                "current_resolution": current_resolution,
            },
        )

        try:
            from conversation_ms.services.message_migration_service import MessageMigrationService

            migration_service = MessageMigrationService()
            migration_service.migrate_conversation_messages_to_postgres(conversation)
            logger.info(
                "[update_conversation_data] Message migration completed",
                extra={"conversation_uuid": str(conversation.uuid)},
            )

            # Trigger classification
            classify_conversation_task.delay(str(conversation.uuid))
            logger.info(
                "[update_conversation_data] Classification task triggered",
                extra={"conversation_uuid": str(conversation.uuid)},
            )

        except Exception as e:
            logger.error(
                "[update_conversation_data] Error during message migration or classification trigger",
                extra={
                    "conversation_uuid": str(conversation.uuid),
                    "error": str(e),
                },
                exc_info=True,
            )

    logger.debug(
        "[update_conversation_data] Conversation updated",
        extra={"conversation_uuid": str(conversation.uuid), "updated_fields": list(to_update.keys())},
    )
