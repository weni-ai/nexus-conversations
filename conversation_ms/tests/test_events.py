"""
Tests for conversation_ms events (DTOs).
"""

import pytest
from datetime import datetime
from uuid import uuid4

from conversation_ms.events import MessageReceivedEvent, MessageSentEvent


class TestMessageReceivedEvent:
    """Tests for MessageReceivedEvent DTO."""

    def test_from_sqs_event_complete(self):
        """Test parsing complete SQS event."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "channel_uuid": str(uuid4()),
                "message": {
                    "id": str(uuid4()),
                    "text": "Hello",
                    "source": "incoming",
                    "contact_name": "Test Contact",
                    "created_at": "2024-01-01T12:00:00Z",
                },
            },
        }
        event = MessageReceivedEvent.from_sqs_event(event_data)
        assert event.correlation_id == event_data["correlation_id"]
        assert event.project_uuid == event_data["data"]["project_uuid"]
        assert event.contact_urn == event_data["data"]["contact_urn"]
        assert event.channel_uuid == event_data["data"]["channel_uuid"]
        assert event.message["text"] == "Hello"
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_minimal(self):
        """Test parsing minimal SQS event."""
        event_data = {
            "correlation_id": "",
            "data": {
                "project_uuid": "",
                "contact_urn": "",
                "message": {},
            },
        }
        event = MessageReceivedEvent.from_sqs_event(event_data)
        assert event.correlation_id == ""
        assert event.project_uuid == ""
        assert event.contact_urn == ""
        assert event.channel_uuid is None
        assert event.message == {}
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_without_channel_uuid(self):
        """Test parsing event without channel_uuid."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"text": "Hello"},
            },
        }
        event = MessageReceivedEvent.from_sqs_event(event_data)
        assert event.channel_uuid is None

    def test_from_sqs_event_timestamp_parsing(self):
        """Test timestamp parsing from different formats."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"created_at": "2024-01-01T12:00:00+00:00"},
            },
        }
        event = MessageReceivedEvent.from_sqs_event(event_data)
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_invalid_timestamp(self):
        """Test handling invalid timestamp."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"created_at": "invalid-timestamp"},
            },
        }
        event = MessageReceivedEvent.from_sqs_event(event_data)
        assert isinstance(event.timestamp, datetime)  # Should fallback to utcnow()


class TestMessageSentEvent:
    """Tests for MessageSentEvent DTO."""

    def test_from_sqs_event_complete(self):
        """Test parsing complete SQS event."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "channel_uuid": str(uuid4()),
                "message": {
                    "id": str(uuid4()),
                    "text": "Response",
                    "source": "outgoing",
                    "created_at": "2024-01-01T12:01:00Z",
                },
            },
        }
        event = MessageSentEvent.from_sqs_event(event_data)
        assert event.correlation_id == event_data["correlation_id"]
        assert event.project_uuid == event_data["data"]["project_uuid"]
        assert event.contact_urn == event_data["data"]["contact_urn"]
        assert event.channel_uuid == event_data["data"]["channel_uuid"]
        assert event.message["text"] == "Response"
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_minimal(self):
        """Test parsing minimal SQS event."""
        event_data = {
            "correlation_id": "",
            "data": {
                "project_uuid": "",
                "contact_urn": "",
                "message": {},
            },
        }
        event = MessageSentEvent.from_sqs_event(event_data)
        assert event.correlation_id == ""
        assert event.project_uuid == ""
        assert event.contact_urn == ""
        assert event.channel_uuid is None
        assert event.message == {}
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_without_channel_uuid(self):
        """Test parsing event without channel_uuid."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"text": "Response"},
            },
        }
        event = MessageSentEvent.from_sqs_event(event_data)
        assert event.channel_uuid is None

    def test_from_sqs_event_timestamp_parsing(self):
        """Test timestamp parsing from different formats."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"created_at": "2024-01-01T12:00:00+00:00"},
            },
        }
        event = MessageSentEvent.from_sqs_event(event_data)
        assert isinstance(event.timestamp, datetime)

    def test_from_sqs_event_invalid_timestamp(self):
        """Test handling invalid timestamp."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                "message": {"created_at": "invalid-timestamp"},
            },
        }
        event = MessageSentEvent.from_sqs_event(event_data)
        assert isinstance(event.timestamp, datetime)  # Should fallback to utcnow()

