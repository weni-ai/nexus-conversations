import logging

import sentry_sdk

from conversation_ms.models import ConversationMessages
from conversation_ms.repositories.message_repository import MessageRepository

logger = logging.getLogger(__name__)


class MessageMigrationService:
    def __init__(self):
        self.message_repository = MessageRepository()

    def migrate_conversation_messages_to_postgres(self, conversation):
        """
        Migrate messages from DynamoDB to PostgreSQL ConversationMessages table.
        This should be called when a conversation is closed.
        """
        try:
            logger.info(
                f"[MessageMigrationService] Starting migration for conversation_uuid={conversation.uuid}",
            )

            messages = self.message_repository.get_messages_from_dynamo(
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
                channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
            )

            if not messages:
                logger.info(
                    f"[MessageMigrationService] No messages to migrate conversation_uuid={conversation.uuid}",
                )
                return

            formatted_messages = []
            for msg in messages:
                message_id = msg.get("message_id")
                formatted_message = {
                    "text": msg.get("text", ""),
                    "source": msg.get("source", ""),
                    "created_at": msg.get("created_at", ""),
                }
                if message_id:
                    formatted_message["message_id"] = str(message_id)
                    formatted_message["uuid"] = str(message_id)
                formatted_messages.append(formatted_message)

            conversation_messages, created = ConversationMessages.objects.update_or_create(
                conversation=conversation,
                defaults={"messages": formatted_messages},
            )

            # Delete messages from DynamoDB after successful migration
            deleted_count = self.message_repository.delete_messages_from_dynamo(
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
                channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
            )

            logger.info(
                f"[MessageMigrationService] Migration completed conversation_uuid={conversation.uuid} "
                f"messages_count={len(formatted_messages)} was_created={created} dynamo_deleted_count={deleted_count}",
            )

        except Exception as e:
            sentry_sdk.set_tag("conversation_uuid", str(conversation.uuid))
            sentry_sdk.set_context(
                "message_migration",
                {
                    "conversation_uuid": str(conversation.uuid),
                    "project_uuid": str(conversation.project.uuid),
                    "contact_urn": conversation.contact_urn,
                },
            )
            sentry_sdk.capture_exception(e)
            logger.error(
                f"[MessageMigrationService] Error migrating messages conversation_uuid={conversation.uuid} error={e!s}",
                exc_info=True,
            )
            raise
