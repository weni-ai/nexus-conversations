"""
Tests for conversation_ms repositories.
"""

import pytest
from unittest.mock import Mock, patch
from uuid import uuid4

from conversation_ms.repositories.message_repository import MessageRepository
from conversation_ms.repositories.conversation_repository import ConversationRepository
from conversation_ms.models import Project, Conversation
from conversation_ms.events import MessageReceivedEvent, MessageSentEvent
from datetime import datetime


@pytest.mark.django_db
class TestMessageRepository:
    """Tests for MessageRepository."""

    def test_save_received_message_in_progress(self, conversation, mock_dynamodb_repository):
        """Test saving received message when conversation is in progress."""
        conversation.resolution = 2  # IN_PROGRESS
        conversation.save()

        event = MessageReceivedEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Hello", "source": "incoming", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            repository.save_received_message(conversation=conversation, event=event)

            # Verify DynamoDB storage was called
            mock_storage.assert_called_once()
            call_args = mock_storage.call_args
            assert call_args.kwargs["project_uuid"] == str(conversation.project.uuid)
            assert call_args.kwargs["contact_urn"] == conversation.contact_urn
            assert call_args.kwargs["channel_uuid"] == str(conversation.channel_uuid)
            assert call_args.kwargs["resolution_status"] == 2

    def test_save_received_message_not_in_progress(self, conversation):
        """Test saving received message when conversation is not in progress."""
        conversation.resolution = 0  # RESOLVED
        conversation.save()

        event = MessageReceivedEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Hello", "source": "incoming", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            repository.save_received_message(conversation=conversation, event=event)

            # Verify DynamoDB storage was NOT called
            mock_storage.assert_not_called()

    def test_save_sent_message_in_progress(self, conversation, mock_dynamodb_repository):
        """Test saving sent message when conversation is in progress."""
        conversation.resolution = 2  # IN_PROGRESS
        conversation.save()

        event = MessageSentEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Response", "source": "outgoing", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            repository.save_sent_message(conversation=conversation, event=event)

            # Verify DynamoDB storage was called
            mock_storage.assert_called_once()
            call_args = mock_storage.call_args
            assert call_args.kwargs["project_uuid"] == str(conversation.project.uuid)
            assert call_args.kwargs["contact_urn"] == conversation.contact_urn
            assert call_args.kwargs["channel_uuid"] == str(conversation.channel_uuid)
            assert call_args.kwargs["resolution_status"] == 2

    def test_save_sent_message_not_in_progress(self, conversation):
        """Test saving sent message when conversation is not in progress."""
        conversation.resolution = 0  # RESOLVED
        conversation.save()

        event = MessageSentEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Response", "source": "outgoing", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            repository.save_sent_message(conversation=conversation, event=event)

            # Verify DynamoDB storage was NOT called
            mock_storage.assert_not_called()

    def test_get_messages_from_dynamo(self, mock_dynamodb_repository):
        """Test getting messages from DynamoDB."""
        mock_messages = [
            {"text": "Hello", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
            {"text": "Hi", "source": "outgoing", "created_at": "2024-01-01T12:01:00"},
        ]

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "get_messages") as mock_get:
            mock_get.return_value = {"items": mock_messages, "next_cursor": None, "total_count": 2}

            result = repository.get_messages_from_dynamo(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(uuid4()),
            )

            assert len(result) == 2
            assert result[0]["text"] == "Hello"
            assert result[1]["text"] == "Hi"

    def test_get_messages_from_dynamo_empty(self, mock_dynamodb_repository):
        """Test getting messages from DynamoDB when empty."""
        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "get_messages") as mock_get:
            mock_get.return_value = {"items": [], "next_cursor": None, "total_count": 0}

            result = repository.get_messages_from_dynamo(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(uuid4()),
            )

            assert result == []

    def test_save_received_message_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in save_received_message are properly handled."""
        event = MessageReceivedEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Hello", "source": "incoming", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            mock_storage.side_effect = Exception("DynamoDB error")

            with pytest.raises(Exception, match="DynamoDB error"):
                repository.save_received_message(conversation=conversation, event=event)

    def test_save_sent_message_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in save_sent_message are properly handled."""
        event = MessageSentEvent(
            correlation_id=str(uuid4()),
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
            channel_uuid=str(conversation.channel_uuid),
            message={"text": "Response", "source": "outgoing", "id": str(uuid4())},
            timestamp=datetime.utcnow(),
        )

        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "storage_message") as mock_storage:
            mock_storage.side_effect = Exception("DynamoDB error")

            with pytest.raises(Exception, match="DynamoDB error"):
                repository.save_sent_message(conversation=conversation, event=event)

    def test_get_messages_from_dynamo_handles_exception(self, mock_sentry):
        """Test that exceptions in get_messages_from_dynamo are properly handled."""
        repository = MessageRepository()
        with patch.object(repository.dynamo_repository, "get_messages") as mock_get:
            mock_get.side_effect = Exception("DynamoDB query error")

            with pytest.raises(Exception, match="DynamoDB query error"):
                repository.get_messages_from_dynamo(
                    project_uuid=str(uuid4()),
                    contact_urn="whatsapp:+5511999999999",
                    channel_uuid=str(uuid4()),
                )


@pytest.mark.django_db
class TestConversationRepository:
    """Tests for ConversationRepository."""

    def test_get_conversation_with_channel_uuid(self, project):
        """Test getting conversation with channel_uuid."""
        channel_uuid = uuid4()
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,
        )

        repository = ConversationRepository()
        result = repository.get_conversation(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=str(channel_uuid),
        )

        assert result is not None
        assert result.uuid == conversation.uuid

    def test_get_conversation_without_channel_uuid(self, project):
        """Test getting conversation without channel_uuid."""
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
            resolution=2,
        )

        repository = ConversationRepository()
        result = repository.get_conversation(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=None,
        )

        assert result is not None
        assert result.uuid == conversation.uuid

    def test_get_conversation_not_found(self, project):
        """Test getting conversation that doesn't exist."""
        repository = ConversationRepository()
        result = repository.get_conversation(
            project_uuid=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=str(uuid4()),
        )

        assert result is None

    def test_get_conversation_returns_most_recent(self, project):
        """Test getting the most recent conversation."""
        channel_uuid = uuid4()
        old_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,
        )
        new_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,
        )

        repository = ConversationRepository()
        result = repository.get_conversation(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=str(channel_uuid),
        )

        assert result is not None
        assert result.uuid == new_conversation.uuid

    def test_get_conversation_handles_exception(self, project, mock_sentry):
        """Test that exceptions are properly handled and re-raised."""
        repository = ConversationRepository()

        with patch("conversation_ms.models.Conversation.objects.filter") as mock_filter:
            mock_filter.side_effect = Exception("Database error")

            with pytest.raises(Exception, match="Database error"):
                repository.get_conversation(
                    project_uuid=str(project.uuid),
                    contact_urn="whatsapp:+5511999999999",
                    channel_uuid=str(uuid4()),
                )

