"""
Conversation service for creating and managing conversations locally.
This service is the source of truth for conversations in the microservice.

The microservice creates and manages conversations independently from nexus-ai,
following the architectural decision that nexus-conversations is the source of truth.
"""

import logging
from typing import Optional

import pendulum
import sentry_sdk

from conversation_ms.models import Conversation, Project

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
        channel_uuid: Optional[str] = None,
    ) -> Optional[Conversation]:
        """
        Ensure conversation exists.

        This method:
        1. Gets or creates the Project
        2. Finds existing conversation in progress (resolution=2)
        3. Creates new conversation if none exists

        Returns the conversation object or None if channel_uuid is missing.
        """
        if not channel_uuid:
            logger.warning(
                "[MainConversationService] channel_uuid is None, cannot create conversation",
                extra={
                    "project_uuid": project_uuid,
                    "contact_urn": contact_urn,
                    "contact_name": contact_name,
                },
            )
            return None

        try:
            project = self._get_or_create_project(project_uuid)

            conversation = self._get_active_conversation(
                project=project, channel_uuid=channel_uuid, contact_urn=contact_urn
            )

            if conversation:
                return conversation

            # Create new conversation
            conversation = self._create_conversation(
                project=project,
                contact_urn=contact_urn,
                contact_name=contact_name,
                channel_uuid=channel_uuid,
            )

            logger.info(
                "[MainConversationService] Created new conversation",
                extra={
                    "conversation_uuid": str(conversation.uuid),
                    "project_uuid": project_uuid,
                    "contact_urn": contact_urn,
                },
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
                "[MainConversationService] Error ensuring conversation exists",
                extra={
                    "project_uuid": project_uuid,
                    "contact_urn": contact_urn,
                    "channel_uuid": channel_uuid,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

    def _get_or_create_project(self, project_uuid: str) -> Project:
        """Get or create Project instance."""
        project, _ = Project.objects.get_or_create(uuid=project_uuid, defaults={"name": None})
        return project

    def _get_active_conversation(self, project: Project, channel_uuid: str, contact_urn: str) -> Optional[Conversation]:
        """
        Get the most recent active conversation (resolution=2).

        If multiple active conversations exist, returns the most recent one
        and logs a warning.
        """
        conversations = Conversation.objects.filter(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn=contact_urn,
            resolution=2,  # IN_PROGRESS
        ).order_by("-created_at")

        if not conversations.exists():
            return None

        if conversations.count() > 1:
            logger.warning(
                "[MainConversationService] Multiple active conversations found, using the most recent one",
                extra={
                    "project_uuid": str(project.uuid),
                    "contact_urn": contact_urn,
                    "channel_uuid": channel_uuid,
                    "count": conversations.count(),
                },
            )

        conversation = conversations.first()
        logger.debug(
            "[MainConversationService] Found existing conversation",
            extra={
                "conversation_uuid": str(conversation.uuid),
                "project_uuid": str(project.uuid),
                "contact_urn": contact_urn,
            },
        )
        return conversation

    def _create_conversation(
        self,
        project: Project,
        contact_urn: str,
        contact_name: str,
        channel_uuid: str,
    ) -> Conversation:
        """
        Create a new conversation with base structure.

        Sets start_date to current time and end_date to start_date + 1 day,
        following the pattern from nexus-ai.
        """
        msg_created_at = pendulum.now()
        start_date = msg_created_at
        end_date = start_date.add(days=1)

        conversation = Conversation.objects.create(
            project=project,
            contact_urn=contact_urn,
            contact_name=contact_name or "",
            channel_uuid=channel_uuid,
            start_date=start_date,
            end_date=end_date,
            resolution=2,  # IN_PROGRESS
        )

        return conversation
