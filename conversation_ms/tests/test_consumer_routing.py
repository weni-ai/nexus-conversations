"""
Tests for SQS consumer event routing.
"""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from django.db.utils import InterfaceError

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

    def test_route_event_retries_once_on_stale_db_connection(self):
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {"correlation_id": "cid-1", "data": {}}

        with (
            patch.object(
                consumer, "_route_event", side_effect=[InterfaceError("connection already closed"), None]
            ) as mock_route,
            patch.object(consumer, "_close_all_db_connections") as mock_close_all,
        ):
            consumer._route_event_with_stale_db_retry("message.sent", event_data)

        mock_close_all.assert_called_once()
        assert mock_route.call_count == 2

    def test_route_event_logs_and_reraises_when_retry_fails(self):
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        event_data = {"correlation_id": "cid-1", "data": {}}
        stale = InterfaceError("connection already closed")

        with (
            patch.object(consumer, "_route_event", side_effect=[stale, stale]),
            patch.object(consumer, "_close_all_db_connections"),
            patch("conversation_ms.consumers.sqs_consumer.logger") as mock_logger,
        ):
            with pytest.raises(InterfaceError):
                consumer._route_event_with_stale_db_retry("message.sent", event_data)

        mock_logger.error.assert_called_once()
        assert "Retry failed" in mock_logger.error.call_args.args[0]

    def test_process_message_uses_stale_db_retry_wrapper(self):
        consumer = ConversationSQSConsumer(queue_url="https://sqs.test.queue")
        message = {
            "MessageId": "mid-1",
            "ReceiptHandle": "rh-1",
            "Body": '{"event_type": "message.sent", "correlation_id": "cid-1", "data": {}}',
            "MessageAttributes": {},
        }

        with patch.object(consumer, "_route_event_with_stale_db_retry") as mock_route:
            result = consumer._process_message(message)

        mock_route.assert_called_once()
        assert mock_route.call_args.args[0] == "message.sent"
        assert result == "rh-1"
