"""
Conversation service for creating and managing conversations locally.
This service is the source of truth for conversations in the microservice.

The microservice creates and manages conversations independently from nexus-ai,
following the architectural decision that nexus-conversations is the source of truth.
"""

import logging
from typing import Any, Dict, Optional

import pendulum
import sentry_sdk

from conversation_ms.models import Conversation, Project
from conversation_ms.sentry_reports import report_missing_required_sentry

logger = logging.getLogger(__name__)


class MainConversationService:
    """
    Service for managing conversations in the microservice.

    This service creates and manages conversations independently, making
    nexus-conversations the source of truth for conversation data.
    """

    def ensure_conversation_exists(
        self,
        project_uuid: str,
        contact_urn: str,
        contact_name: str,
        msg_created_at: str,
        channel_uuid: Optional[str] = None,
        event_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Conversation]:
        """
        Ensure conversation exists.

        This method:
        1. Gets or creates the Project
        2. Finds existing conversation in progress (resolution=2)
        3. Creates new conversation if none exists
        4. Handles multiple conversations by marking old ones as Unclassified

        Returns the conversation object or None if channel_uuid is missing.
        """
        if not channel_uuid:
            logger.warning(
                "[MainConversationService] channel_uuid is None, cannot create conversation "
                "project_uuid=%s contact_urn=%s contact_name=%s",
                project_uuid,
                contact_urn,
                contact_name,
            )
            return None

        try:
            # Get or create Project
            project, _ = Project.objects.get_or_create(
                uuid=project_uuid,
                defaults={"name": None},  # Project name can be updated later if needed
            )

            # Find existing conversation in progress
            conversation_queryset = Conversation.objects.filter(
                project=project,
                channel_uuid=channel_uuid,
                contact_urn=contact_urn,
                resolution=2,  # IN_PROGRESS
            )

            if not conversation_queryset.exists():
                # Sentry when creating new conversation but contact_name is not received (we still create)
                if not (contact_name or "").strip():
                    report_missing_required_sentry(
                        reason="contact_name is empty when creating new conversation",
                        missing_fields=["contact_name"],
                        project_uuid=project_uuid,
                        contact_urn=contact_urn,
                        contact_name=contact_name or "",
                        channel_uuid=channel_uuid,
                        msg_created_at=msg_created_at,
                        event_metadata=event_metadata,
                        level="warning",
                    )
                # Create new conversation
                conversation = self._create_conversation(
                    project=project,
                    contact_urn=contact_urn,
                    contact_name=contact_name or "",
                    channel_uuid=channel_uuid,
                    msg_created_at=msg_created_at,
                )
                logger.info(
                    "[MainConversationService] Created new conversation "
                    "conversation_uuid=%s project_uuid=%s contact_urn=%s",
                    conversation.uuid,
                    project_uuid,
                    contact_urn,
                )
                return conversation

            # Handle multiple conversations in progress
            in_progress_count = conversation_queryset.count()
            if in_progress_count > 1:
                conversation_queryset = conversation_queryset.order_by("-created_at")
                conversations_to_close = conversation_queryset.exclude(uuid=conversation_queryset.first().uuid)
                closed_count = conversations_to_close.count()

                for conversation in conversations_to_close:
                    original_resolution = str(conversation.resolution)
                    conversation.resolution = 3  # UNCLASSIFIED
                    conversation.save()

                    if original_resolution == "2":  # IN_PROGRESS
                        try:
                            from conversation_ms.services.message_migration_service import MessageMigrationService

                            migration_service = MessageMigrationService()
                            migration_service.migrate_conversation_messages_to_postgres(conversation)
                            logger.info(
                                "[MainConversationService] Message migration completed for closed "
                                "conversation conversation_uuid=%s",
                                conversation.uuid,
                            )
                        except Exception as e:
                            logger.error(
                                "[MainConversationService] Error during message migration "
                                "conversation_uuid=%s error=%s",
                                conversation.uuid,
                                str(e),
                                exc_info=True,
                            )

                logger.warning(
                    "[MainConversationService] Multiple conversations found, marked old ones as Unclassified "
                    "project_uuid=%s contact_urn=%s channel_uuid=%s count=%s closed_count=%s",
                    project_uuid,
                    contact_urn,
                    channel_uuid,
                    in_progress_count,
                    closed_count,
                )

            # Return the most recent conversation in progress
            conversation = conversation_queryset.first()

            # Backfill start_date/end_date when conversation was created by another path (e.g. Mailroom)
            if conversation.start_date is None and msg_created_at:
                try:
                    msg_date = pendulum.parse(msg_created_at)
                    conversation.start_date = msg_date
                    conversation.end_date = msg_date.add(days=1)
                    conversation.save(update_fields=["start_date", "end_date"])
                    logger.info(
                        "[MainConversationService] Backfilled start_date/end_date from message timestamp "
                        "conversation_uuid=%s project_uuid=%s contact_urn=%s start_date=%s",
                        conversation.uuid,
                        project_uuid,
                        contact_urn,
                        conversation.start_date,
                    )
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "[MainConversationService] Could not backfill start_date: invalid msg_created_at "
                        "conversation_uuid=%s msg_created_at=%s error=%s",
                        conversation.uuid,
                        msg_created_at,
                        str(e),
                    )

            logger.debug(
                "[MainConversationService] Found existing conversation "
                "conversation_uuid=%s project_uuid=%s contact_urn=%s",
                conversation.uuid,
                project_uuid,
                contact_urn,
            )
            return conversation

        except Exception as e:
            sentry_sdk.set_tag("project_uuid", project_uuid)
            sentry_sdk.set_tag("contact_urn", contact_urn)
            sentry_sdk.set_tag("channel_uuid", channel_uuid)
            sentry_sdk.set_context(
                "conversation_creation",
                {
                    "project_uuid": project_uuid,
                    "contact_urn": contact_urn,
                    "contact_name": contact_name,
                    "channel_uuid": channel_uuid,
                    "method": "ensure_conversation_exists",
                },
            )
            sentry_sdk.capture_exception(e)
            logger.error(
                "[MainConversationService] Error ensuring conversation exists "
                "project_uuid=%s contact_urn=%s channel_uuid=%s error=%s",
                project_uuid,
                contact_urn,
                channel_uuid,
                str(e),
                exc_info=True,
            )
            raise

    def _create_conversation(
        self,
        project: Project,
        contact_urn: str,
        contact_name: str,
        channel_uuid: str,
        msg_created_at: str,
    ) -> Conversation:
        """
        Create a new conversation with base structure.

        Sets start_date to msg_created_at and end_date to start_date + 1 day,
        following the pattern from nexus-ai.
        """
        msg_date = pendulum.parse(msg_created_at)
        start_date = msg_date
        end_date = start_date.add(days=1)

        conversation = Conversation.objects.create(
            project=project,
            contact_urn=contact_urn,
            contact_name=contact_name or "",
            channel_uuid=channel_uuid,
            start_date=start_date,
            end_date=end_date,
            resolution="2",  # IN_PROGRESS
        )

        return conversation
