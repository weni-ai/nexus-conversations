"""
Router service adapter.
Adapted from router.services.conversation_service.

This adapter provides an interface to the main system's conversation service.
In production, this can be implemented to:
- Make HTTP API calls to the main system
- Use a shared library
- Access shared database directly
"""

import logging
from typing import Optional

import sentry_sdk

logger = logging.getLogger(__name__)


class MainConversationService:
    """
    Adapter for router.services.conversation_service.ConversationService.

    TODO: Implement this to call the main system.
    Options:
    1. HTTP API calls to main system
    2. Shared library import (if router is available)
    3. Direct database access (if using shared database)
    """

    def ensure_conversation_exists(
        self,
        project_uuid: str,
        contact_urn: str,
        contact_name: str,
        channel_uuid: Optional[str] = None,
    ) -> Optional[object]:
        """
        Ensure conversation exists.
        This is a stub that needs to be implemented.

        For now, it tries to import from router if available, otherwise returns None.
        """
        try:
            # Try to import from router if available (e.g., if router is installed as a package)
            try:
                from router.services.conversation_service import ConversationService as RouterConversationService

                router_service = RouterConversationService()
                return router_service.ensure_conversation_exists(
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                    contact_name=contact_name,
                    channel_uuid=channel_uuid,
                )
            except ImportError:
                # Router not available - this is expected in a standalone microservice
                logger.warning(
                    "[MainConversationService] Router not available. "
                    "Conversation creation needs to be implemented via API or shared library."
                )
                # TODO: Implement HTTP API call or other integration method
                return None

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

