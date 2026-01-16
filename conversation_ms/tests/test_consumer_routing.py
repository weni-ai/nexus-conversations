"""
Tests for SQS consumer event routing.
"""

import pytest
from unittest.mock import Mock, patch
from uuid import uuid4

from conversation_ms.consumers.sqs_consumer import ConversationSQSConsumer


class TestConsumerEventRouting:
    """Tests for event routing in ConversationSQSConsumer."""

    def test_route_event_message_received(self):
        """Test routing message.received event."""
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
            },
        }

        with patch.object(consumer, "_handle_message_received") as mock_handler:
            consumer._route_event("message.received", event_data)
            mock_handler.assert_called_once_with(event_data)

    def test_route_event_message_sent(self):
        """Test routing message.sent event."""
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
            },
        }

        with patch.object(consumer, "_handle_message_sent") as mock_handler:
            consumer._route_event("message.sent", event_data)
            mock_handler.assert_called_once_with(event_data)

    def test_route_event_conversation_window(self):
        """Test routing conversation.window event."""
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "has_chats_room": True,
            },
        }

        with patch.object(consumer, "_handle_conversation_window") as mock_handler:
            consumer._route_event("conversation.window", event_data)
            mock_handler.assert_called_once_with(event_data)

    def test_route_event_unknown_type(self):
        """Test routing unknown event type."""
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {},
        }

        with patch("conversation_ms.consumers.sqs_consumer.logger") as mock_logger:
            consumer._route_event("unknown.event.type", event_data)
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            assert "Unknown event type" in str(call_args)
            assert "unknown.event.type" in str(call_args)

    def test_handle_conversation_window_calls_service(self, sample_sqs_conversation_window_event):
        """Test that _handle_conversation_window calls ConversationWindowService."""
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")

        with patch(
            "conversation_ms.services.conversation_window_service.ConversationWindowService"
        ) as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            consumer._handle_conversation_window(sample_sqs_conversation_window_event)

            mock_service_class.assert_called_once()
            mock_service.process_conversation_window.assert_called_once_with(
                sample_sqs_conversation_window_event
            )

