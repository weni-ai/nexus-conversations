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
from conversation_ms.utils.date_helpers import (
    conversation_effective_service_end_utc,
    end_of_project_local_calendar_day_utc,
    resolve_effective_project_timezone,
)

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
        2. Among IN_PROGRESS (resolution=2) for this project/channel/contact, finds conversations where
           the message instant is still on or before the effective service-day end (same rule as
           ``close_daily``: end of calendar day in project timezone, see ``date_helpers``).
        3. Creates a new IN_PROGRESS conversation if none match (older IN_PROGRESS stay unchanged)
        4. If several match, returns the most recent by ``created_at``

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

            tz_name = resolve_effective_project_timezone(project.timezone)
            try:
                msg_utc = pendulum.parse(msg_created_at).in_timezone("UTC")
            except Exception as parse_err:
                logger.error(
                    "[MainConversationService] Invalid msg_created_at for routing "
                    "project_uuid=%s msg_created_at=%s error=%s",
                    project_uuid,
                    msg_created_at,
                    parse_err,
                    exc_info=True,
                )
                raise ValueError(
                    f"Invalid msg_created_at for routing (project_uuid={project_uuid}): {msg_created_at!r}"
                ) from parse_err

            base_in_progress = Conversation.objects.filter(
                project=project,
                channel_uuid=channel_uuid,
                contact_urn=contact_urn,
                resolution=2,  # IN_PROGRESS
            ).order_by("-created_at")

            matched: Optional[Conversation] = None
            for conv in base_in_progress:
                try:
                    effective_end = conversation_effective_service_end_utc(conv, tz_name)
                    if msg_utc <= effective_end:
                        matched = conv
                        break
                except Exception as window_err:
                    logger.warning(
                        "[MainConversationService] Could not compute service window for conversation, not reusing "
                        "conversation_uuid=%s error=%s",
                        conv.uuid,
                        window_err,
                    )

            if matched is None:
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
                    tz_name=tz_name,
                )
                logger.info(
                    "[MainConversationService] Created new conversation "
                    "conversation_uuid=%s project_uuid=%s contact_urn=%s",
                    conversation.uuid,
                    project_uuid,
                    contact_urn,
                )
                return conversation

            conversation = matched

            # Backfill start_date/end_date when conversation was created by another path (e.g. Mailroom)
            if conversation.start_date is None and msg_created_at:
                try:
                    msg_date = pendulum.parse(msg_created_at)
                    conversation.start_date = msg_date
                    conversation.end_date = end_of_project_local_calendar_day_utc(msg_created_at, tz_name)
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
        tz_name: str,
    ) -> Conversation:
        """
        Create a new conversation with base structure.

        ``start_date`` is the message timestamp; ``end_date`` is end of that calendar day in
        ``tz_name`` (same instant as ``ProjectDay`` / ``close_daily_conversations_task``).
        """
        msg_date = pendulum.parse(msg_created_at)
        start_date = msg_date
        end_date = end_of_project_local_calendar_day_utc(msg_created_at, tz_name)

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
