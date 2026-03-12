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

logger = logging.getLogger(__name__)


class ConversationWindowService:
    """Service for processing conversation window events."""

    def process_conversation_window(self, event_data: dict):
        """
        Process conversation window event.

        This method:
        1. Parses the event data
        2. Gets or creates Project
        3. Updates or creates Conversation with ticket_uuid, has_chats_room, dates, resolution, etc.

        Resolution is set from the event (e.g. HAS_CHAT_ROOM when ticket_uuid is present).
        The actual close (message migration, classification) is done only by
        close_daily_conversations_task, not by this handler.
        """
        try:
            event = ConversationWindowEvent.from_sqs_event(event_data)

            logger.info(
                f"[ConversationWindowService] Processing conversation.window event "
                f"correlation_id={event.correlation_id} project_uuid={event.project_uuid} "
                f"contact_urn={event.contact_urn} has_chats_room={event.has_chats_room} ticket_uuid={event.ticket_uuid}"
            )

            if not event.channel_uuid:
                logger.warning(
                    f"[ConversationWindowService] channel_uuid is missing, cannot process event "
                    f"correlation_id={event.correlation_id} project_uuid={event.project_uuid} "
                    f"contact_urn={event.contact_urn}"
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

            # Resolution can be set from ticket_uuid (e.g. HAS_CHAT_ROOM); actual close
            # (migration, classification) is done only by close_daily_conversations_task.
            if event.has_chats_room:
                resolution = ResolutionEntities.HAS_CHAT_ROOM  # "4"
            else:
                resolution = conversation.resolution if conversation else ResolutionEntities.IN_PROGRESS

            if conversation:
                # Update existing conversation
                conversation.external_id = event.external_id or conversation.external_id
                conversation.has_chats_room = event.has_chats_room
                conversation.start_date = event.start_date or conversation.start_date
                conversation.end_date = event.end_date or conversation.end_date
                conversation.contact_name = event.contact_name or conversation.contact_name
                conversation.ticket_uuid = event.ticket_uuid or conversation.ticket_uuid
                conversation.resolution = resolution
                conversation.save()

                logger.info(
                    f"[ConversationWindowService] Updated conversation "
                    f"correlation_id={event.correlation_id} conversation_uuid={conversation.uuid} "
                    f"resolution={resolution} has_chats_room={event.has_chats_room}"
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
                    ticket_uuid=event.ticket_uuid,
                )

                logger.info(
                    f"[ConversationWindowService] Created new conversation "
                    f"correlation_id={event.correlation_id} conversation_uuid={conversation.uuid} "
                    f"has_chats_room={event.has_chats_room}"
                )

            logger.info(
                f"[ConversationWindowService] Conversation window event processed successfully "
                f"correlation_id={event.correlation_id} conversation_uuid={conversation.uuid}"
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
                f"[ConversationWindowService] Error processing conversation.window event "
                f"event_data={event_data} error={e}",
                exc_info=True,
            )
            raise
