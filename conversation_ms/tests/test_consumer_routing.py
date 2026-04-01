"""
Tests for SQS consumer event routing.
"""

import json
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

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
                "ticket_uuid": str(uuid4()),
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
            mock_service.process_conversation_window.assert_called_once_with(sample_sqs_conversation_window_event)

    def test_process_message_skips_routing_when_idempotent_duplicate(self, sample_sqs_received_event):
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test/queue")
        consumer._idempotency = Mock()
        consumer._idempotency.is_enabled = True
        consumer._idempotency.try_claim.return_value = False

        body = {**sample_sqs_received_event, "event_type": "message.received"}
        message = {
            "MessageId": "sqs-msg-duplicate",
            "ReceiptHandle": "rh-dup",
            "Body": json.dumps(body),
            "MessageAttributes": {},
        }

        with patch.object(consumer, "_route_event") as mock_route:
            receipt = consumer._process_message(message)

        assert receipt == "rh-dup"
        mock_route.assert_not_called()
        consumer._idempotency.try_claim.assert_called_once_with("sqs-msg-duplicate")
        consumer._idempotency.release_claim.assert_not_called()

    def test_process_message_releases_claim_when_handler_raises(self, sample_sqs_received_event):
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test/queue")
        consumer._idempotency = Mock()
        consumer._idempotency.is_enabled = True
        consumer._idempotency.try_claim.return_value = True

        body = {**sample_sqs_received_event, "event_type": "message.received"}
        message = {
            "MessageId": "sqs-msg-fail",
            "ReceiptHandle": "rh-fail",
            "Body": json.dumps(body),
            "MessageAttributes": {},
        }

        with patch.object(consumer, "_route_event", side_effect=RuntimeError("handler failed")):
            with pytest.raises(RuntimeError, match="handler failed"):
                consumer._process_message(message)

        consumer._idempotency.release_claim.assert_called_once_with("sqs-msg-fail")
