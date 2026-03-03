import logging
from typing import Any, Optional, Tuple

import pendulum
import sentry_sdk

from conversation_ms.events import MessageReceivedEvent, MessageSentEvent
from conversation_ms.repositories.message_repository import MessageRepository
from conversation_ms.services.conversation_service import ConversationService
from conversation_ms.services.csat_nps_service import CSATNPSService

logger = logging.getLogger(__name__)

MESSAGE_RECEIVED = "message.received"
MESSAGE_SENT = "message.sent"


def _build_event_metadata(event_data: dict, event: Any, event_type: str) -> dict:
    """Build event metadata for Sentry and conversation creation."""
    data = event_data.get("data") or {}
    return {
        "event_type": event_type,
        "correlation_id": event_data.get("correlation_id", ""),
        "event_data_snapshot": {
            "project_uuid": data.get("project_uuid"),
            "contact_urn": data.get("contact_urn"),
            "channel_uuid": data.get("channel_uuid"),
            "message_has_created_at": "created_at" in event.message and bool(event.message.get("created_at")),
            "message_has_text": bool(event.message.get("text")),
            "message_keys": list(event.message.keys()) if event.message else [],
        },
    }


def _report_message_processing_exception(event_data: dict, event_type: str) -> None:
    """Report exception to Sentry with full message-processing context."""
    data = event_data.get("data") or {}
    sentry_sdk.set_tag("project_uuid", data.get("project_uuid", "unknown"))
    sentry_sdk.set_tag("contact_urn", data.get("contact_urn", "unknown"))
    sentry_sdk.set_tag("event_type", event_type)
    sentry_sdk.set_tag("correlation_id", event_data.get("correlation_id", "unknown"))
    sentry_sdk.set_context(
        "message_processing",
        {
            "event_type": event_type,
            "event_data": event_data,
            "correlation_id": event_data.get("correlation_id"),
        },
    )
    sentry_sdk.capture_exception()


class MessageService:
    def __init__(self):
        self.conversation_service = ConversationService()
        self.message_repository = MessageRepository()
        self.csat_nps_service = CSATNPSService()

    def _ensure_conversation_for_message(self, event_data: dict, event_type: str) -> Tuple[Optional[Any], Any]:
        """
        Parse event, ensure conversation exists, return (conversation, event).
        Returns (None, event) when conversation could not be created.
        """
        if event_type == MESSAGE_RECEIVED:
            event = MessageReceivedEvent.from_sqs_event(event_data)
        else:
            event = MessageSentEvent.from_sqs_event(event_data)

        logger.info(
            "[MessageService] Processing %s correlation_id=%s project_uuid=%s contact_urn=%s",
            event_type,
            event.correlation_id,
            event.project_uuid,
            event.contact_urn,
        )

        contact_name = event.message.get("contact_name", "")
        msg_created_at = event.message.get("created_at")

        # Handle missing created_at for dummy messages (CSAT, NPS, etc) – message.received only
        if event_type == MESSAGE_RECEIVED and not msg_created_at and not event.message.get("text"):
            msg_created_at = pendulum.now("UTC").to_iso8601_string()
            logger.info(
                "[MessageService] Generating timestamp for dummy message "
                "project_uuid=%s contact_urn=%s correlation_id=%s",
                event.project_uuid,
                event.contact_urn,
                event.correlation_id,
            )

        event_metadata = _build_event_metadata(event_data, event, event_type)
        conversation = self.conversation_service.ensure_conversation_exists(
            project_uuid=event.project_uuid,
            contact_urn=event.contact_urn,
            contact_name=contact_name,
            msg_created_at=msg_created_at,
            channel_uuid=event.channel_uuid,
            event_metadata=event_metadata,
        )
        return (conversation, event)

    def process_message_received(self, event_data: dict):
        try:
            conversation, event = self._ensure_conversation_for_message(event_data, MESSAGE_RECEIVED)

            if not conversation:
                logger.warning(
                    "[MessageService] Conversation not created, skipping message "
                    "correlation_id=%s project_uuid=%s contact_urn=%s",
                    event.correlation_id,
                    event.project_uuid,
                    event.contact_urn,
                )
                return

            message_text = event.message.get("text", "")
            if message_text:
                self.message_repository.save_received_message(conversation=conversation, event=event)
            else:
                logger.debug(
                    "[MessageService] Skipping dummy message save (empty text) correlation_id=%s",
                    event.correlation_id,
                )

            self._handle_special_events(event_data, conversation, event.project_uuid, event.contact_urn)
            logger.info(
                "[MessageService] Message.received processed successfully correlation_id=%s conversation_uuid=%s",
                event.correlation_id,
                str(conversation.uuid),
            )
        except Exception as e:
            _report_message_processing_exception(event_data, MESSAGE_RECEIVED)
            logger.error(
                "[MessageService] Error processing message.received event_data=%s error=%s",
                event_data,
                str(e),
                exc_info=True,
            )
            raise

    def process_message_sent(self, event_data: dict):
        try:
            conversation, event = self._ensure_conversation_for_message(event_data, MESSAGE_SENT)

            if not conversation:
                logger.warning(
                    "[MessageService] Conversation not created, skipping message "
                    "correlation_id=%s project_uuid=%s contact_urn=%s",
                    event.correlation_id,
                    event.project_uuid,
                    event.contact_urn,
                )
                return

            self.message_repository.save_sent_message(conversation=conversation, event=event)
            self._handle_special_events(event_data, conversation, event.project_uuid, event.contact_urn)
            logger.info(
                "[MessageService] Message.sent processed successfully correlation_id=%s conversation_uuid=%s",
                event.correlation_id,
                str(conversation.uuid),
            )
        except Exception as e:
            _report_message_processing_exception(event_data, MESSAGE_SENT)
            logger.error(
                "[MessageService] Error processing message.sent event_data=%s error=%s",
                event_data,
                str(e),
                exc_info=True,
            )
            raise

    def _handle_special_events(self, event_data: dict, conversation, project_uuid: str, contact_urn: str):
        try:
            event_key = event_data.get("key") or event_data.get("data", {}).get("key")
            event_value = event_data.get("value") or event_data.get("data", {}).get("value")

            if not event_key:
                return

            if event_key == "weni_csat":
                self.csat_nps_service.process_csat_event(
                    event_data={"value": event_value, **event_data},
                    conversation=conversation,
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                )
            elif event_key == "weni_nps":
                self.csat_nps_service.process_nps_event(
                    event_data={"value": event_value, **event_data},
                    conversation=conversation,
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                )
            else:
                self.csat_nps_service.process_custom_event(
                    event_data={"key": event_key, "value": event_value, **event_data},
                    conversation=conversation,
                    project_uuid=project_uuid,
                    contact_urn=contact_urn,
                )
        except Exception as e:
            logger.warning(
                "[MessageService] Error handling special events event_data=%s error=%s",
                event_data,
                str(e),
                exc_info=True,
            )
