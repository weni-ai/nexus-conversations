"""
Service for processing conversation window events from Mailroom.

This service handles events that indicate conversation window updates,
including chat room opening (has_chats_room=True).
"""

import logging

import sentry_sdk

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.events import ConversationWindowEvent
from conversation_ms.models import Conversation, Project
from conversation_ms.services.message_migration_service import MessageMigrationService
from conversation_ms.tasks import classify_conversation_task

logger = logging.getLogger(__name__)


class ConversationWindowService:
    """Service for processing conversation window events."""

    def __init__(self):
        self.migration_service = MessageMigrationService()

    def process_conversation_window(self, event_data: dict):
        """
        Process conversation window event.

        This method:
        1. Parses the event data
        2. Gets or creates Project
        3. Updates or creates Conversation
        4. If has_chats_room=True, sets resolution to HAS_CHAT_ROOM (4)
        5. Migrates messages if conversation is being closed
        """
        try:
            event = ConversationWindowEvent.from_sqs_event(event_data)

            logger.info(
                "[ConversationWindowService] Processing conversation.window event",
                extra={
                    "correlation_id": event.correlation_id,
                    "project_uuid": event.project_uuid,
                    "contact_urn": event.contact_urn,
                    "has_chats_room": event.has_chats_room,
                },
            )

            if not event.channel_uuid:
                logger.warning(
                    "[ConversationWindowService] channel_uuid is missing, cannot process event",
                    extra={
                        "correlation_id": event.correlation_id,
                        "project_uuid": event.project_uuid,
                        "contact_urn": event.contact_urn,
                    },
                )
                return

            # Get or create Project
            project, _ = Project.objects.get_or_create(
                uuid=event.project_uuid,
                defaults={"name": None},
            )

            # Find existing conversation
            conversation = (
                Conversation.objects.filter(
                    project=project,
                    channel_uuid=event.channel_uuid,
                    contact_urn=event.contact_urn,
                )
                .order_by("-created_at")
                .first()
            )

            # Determine resolution based on has_chats_room
            if event.has_chats_room:
                resolution = str(ResolutionEntities.HAS_CHAT_ROOM)  # "4"
            else:
                # Keep existing resolution if conversation exists, otherwise IN_PROGRESS
                resolution = conversation.resolution if conversation else str(ResolutionEntities.IN_PROGRESS)

            # Check if conversation is being closed (resolution changed from IN_PROGRESS to something else)
            was_in_progress = conversation and str(conversation.resolution) == str(ResolutionEntities.IN_PROGRESS)
            will_be_closed = str(resolution) != str(ResolutionEntities.IN_PROGRESS)

            if conversation:
                # Update existing conversation
                conversation.external_id = event.external_id or conversation.external_id
                conversation.has_chats_room = event.has_chats_room
                conversation.start_date = event.start_date or conversation.start_date
                conversation.end_date = event.end_date or conversation.end_date
                conversation.contact_name = event.contact_name or conversation.contact_name
                conversation.resolution = resolution
                conversation.save()

                logger.info(
                    "[ConversationWindowService] Updated conversation",
                    extra={
                        "correlation_id": event.correlation_id,
                        "conversation_uuid": str(conversation.uuid),
                        "resolution": resolution,
                        "has_chats_room": event.has_chats_room,
                    },
                )
            else:
                # Create new conversation
                conversation = Conversation.objects.create(
                    project=project,
                    contact_urn=event.contact_urn,
                    contact_name=event.contact_name or "",
                    channel_uuid=event.channel_uuid,
                    external_id=event.external_id,
                    start_date=event.start_date,
                    end_date=event.end_date,
                    has_chats_room=event.has_chats_room,
                    resolution=resolution,
                )

                logger.info(
                    "[ConversationWindowService] Created new conversation",
                    extra={
                        "correlation_id": event.correlation_id,
                        "conversation_uuid": str(conversation.uuid),
                        "resolution": resolution,
                        "has_chats_room": event.has_chats_room,
                    },
                )

            # Migrate messages if conversation is being closed
            if was_in_progress and will_be_closed:
                try:
                    self.migration_service.migrate_conversation_messages_to_postgres(conversation)
                    logger.info(
                        "[ConversationWindowService] Message migration completed",
                        extra={
                            "correlation_id": event.correlation_id,
                            "conversation_uuid": str(conversation.uuid),
                        },
                    )

                    # Trigger classification
                    classify_conversation_task.delay(str(conversation.uuid))
                    logger.info(
                        "[ConversationWindowService] Classification task triggered",
                        extra={
                            "correlation_id": event.correlation_id,
                            "conversation_uuid": str(conversation.uuid),
                        },
                    )

                except Exception as e:
                    logger.error(
                        "[ConversationWindowService] Error during message migration or classification trigger",
                        extra={
                            "correlation_id": event.correlation_id,
                            "conversation_uuid": str(conversation.uuid),
                            "error": str(e),
                        },
                        exc_info=True,
                    )

            logger.info(
                "[ConversationWindowService] Conversation window event processed successfully",
                extra={
                    "correlation_id": event.correlation_id,
                    "conversation_uuid": str(conversation.uuid),
                },
            )

        except Exception as e:
            sentry_sdk.set_tag("project_uuid", event_data.get("data", {}).get("project_uuid", "unknown"))
            sentry_sdk.set_tag("contact_urn", event_data.get("data", {}).get("contact_urn", "unknown"))
            sentry_sdk.set_context(
                "conversation_window_processing",
                {
                    "event_type": "conversation.window",
                    "event_data": event_data,
                    "correlation_id": event_data.get("correlation_id"),
                },
            )
            sentry_sdk.capture_exception(e)
            logger.error(
                "[ConversationWindowService] Error processing conversation.window event",
                extra={"event_data": event_data, "error": str(e)},
                exc_info=True,
            )
            raise
