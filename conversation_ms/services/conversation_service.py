import logging
from typing import Any, Dict, Optional

import sentry_sdk

from conversation_ms.adapters.router_service import MainConversationService
from conversation_ms.sentry_reports import report_missing_required_sentry

logger = logging.getLogger(__name__)


class ConversationService:
    def ensure_conversation_exists(
        self,
        project_uuid: str,
        contact_urn: str,
        contact_name: str,
        msg_created_at: str,
        channel_uuid: Optional[str] = None,
        event_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[object]:
        """
        Ensure a conversation row exists for routing, delegating persistence to ``MainConversationService``.
        """
        # Sentry when creating would need required fields but created_at is missing
        if not msg_created_at:
            report_missing_required_sentry(
                reason="msg_created_at is None or empty",
                missing_fields=["msg_created_at"],
                project_uuid=project_uuid,
                contact_urn=contact_urn,
                contact_name=contact_name,
                channel_uuid=channel_uuid,
                msg_created_at=msg_created_at,
                event_metadata=event_metadata,
                level="warning",
            )
            logger.warning(
                f"[ConversationService] Conversation not created: msg_created_at is None or empty "
                f"project_uuid={project_uuid} contact_urn={contact_urn} contact_name={contact_name} "
                f"channel_uuid={channel_uuid}",
            )
            return None

        if not channel_uuid:
            report_missing_required_sentry(
                reason="channel_uuid is None or missing",
                missing_fields=["channel_uuid"],
                project_uuid=project_uuid,
                contact_urn=contact_urn,
                contact_name=contact_name,
                channel_uuid=channel_uuid,
                msg_created_at=msg_created_at,
                event_metadata=event_metadata,
                level="warning",
            )
            logger.warning(
                f"[ConversationService] Conversation not created: channel_uuid is None "
                f"project_uuid={project_uuid} contact_urn={contact_urn} contact_name={contact_name}",
            )
            return None

        try:
            main_service = MainConversationService()

            conversation = main_service.ensure_conversation_exists(
                project_uuid=project_uuid,
                contact_urn=contact_urn,
                contact_name=contact_name,
                channel_uuid=channel_uuid,
                msg_created_at=msg_created_at,
            )

            if conversation:
                logger.debug(
                    f"[ConversationService] Conversation ensured conversation_uuid={conversation.uuid} "
                    f"project_uuid={project_uuid} contact_urn={contact_urn}",
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
                f"[ConversationService] Error ensuring conversation exists "
                f"project_uuid={project_uuid} contact_urn={contact_urn} channel_uuid={channel_uuid} error={e!s}",
                exc_info=True,
            )
            raise
