import logging

import sentry_sdk

from conversation_ms.models import ConversationMessages
from conversation_ms.repositories.message_repository import MessageRepository

logger = logging.getLogger(__name__)


class MessageMigrationService:
    def __init__(self):
        self.message_repository = MessageRepository()

    @staticmethod
    def _sanitize_pg_text(value) -> str:
        """Strip NUL bytes; PostgreSQL rejects \\u0000 in text/JSON."""
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        return value.replace("\x00", "")

    def _format_messages_for_storage(self, messages):
        formatted_messages = []
        for msg in messages:
            message_id = msg.get("message_id")
            formatted_message = {
                "text": self._sanitize_pg_text(msg.get("text", "")),
                "source": self._sanitize_pg_text(msg.get("source", "")),
                "created_at": self._sanitize_pg_text(msg.get("created_at", "")),
            }
            if message_id:
                formatted_message["message_id"] = str(message_id)
                formatted_message["uuid"] = str(message_id)
            formatted_messages.append(formatted_message)
        return formatted_messages

    def persist_conversation_messages_to_postgres(self, conversation, delete_from_dynamo: bool = False):
        """
        Persist messages from DynamoDB into PostgreSQL ConversationMessages.
        Optionally delete from DynamoDB after successful persistence.
        """
        logger.info(
            f"[MessageMigrationService] Persisting messages for conversation_uuid={conversation.uuid} "
            f"delete_from_dynamo={delete_from_dynamo}",
        )

        messages = self.message_repository.get_messages_from_dynamo(
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
        )

        if not messages:
            logger.info(
                f"[MessageMigrationService] No Dynamo messages found for conversation_uuid={conversation.uuid}",
            )
            return {"persisted": False, "messages": []}

        formatted_messages = self._format_messages_for_storage(messages)
        ConversationMessages.objects.update_or_create(
            conversation=conversation,
            defaults={"messages": formatted_messages},
        )

        deleted_count = 0
        if delete_from_dynamo:
            deleted_count = self.message_repository.delete_messages_from_dynamo(
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
                channel_uuid=str(conversation.channel_uuid) if conversation.channel_uuid else None,
            )

        logger.info(
            f"[MessageMigrationService] Persisted messages conversation_uuid={conversation.uuid} "
            f"messages_count={len(formatted_messages)} dynamo_deleted_count={deleted_count}",
        )
        return {"persisted": True, "messages": formatted_messages}

    def migrate_conversation_messages_to_postgres(self, conversation):
        """
        Migrate messages from DynamoDB to PostgreSQL ConversationMessages table.
        This should be called when a conversation is closed.
        """
        try:
            logger.info(
                f"[MessageMigrationService] Starting migration for conversation_uuid={conversation.uuid}",
            )

            result = self.persist_conversation_messages_to_postgres(conversation, delete_from_dynamo=True)
            if not result.get("persisted"):
                logger.info(
                    f"[MessageMigrationService] No messages to migrate conversation_uuid={conversation.uuid}",
                )
                return

            logger.info(
                f"[MessageMigrationService] Migration completed conversation_uuid={conversation.uuid} "
                f"messages_count={len(result.get('messages', []))}",
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
