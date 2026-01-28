"""
Tests for ConversationWindowService.
"""

from unittest.mock import patch
from uuid import uuid4

import pytest

from conversation_ms.adapters.entities import ResolutionEntities
from conversation_ms.models import Conversation, Project
from conversation_ms.services.conversation_window_service import ConversationWindowService


@pytest.mark.django_db
class TestConversationWindowService:
    """Tests for ConversationWindowService."""

    def test_process_conversation_window_create_new(self, mock_sentry):
        """Test creating new conversation from window event."""
        project_uuid = uuid4()
        channel_uuid = uuid4()
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": "whatsapp:+5511999999999",
                "channel_uuid": str(channel_uuid),
                "external_id": "ext-123",
                "start": "2024-01-01T12:00:00Z",
                "end": "2024-01-01T13:00:00Z",
                "has_chats_room": False,
                "name": "Test Contact",
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        # Verify project was created
        project = Project.objects.get(uuid=project_uuid)
        assert project is not None

        # Verify conversation was created
        conversation = Conversation.objects.get(
            project=project,
            channel_uuid=channel_uuid,
            contact_urn="whatsapp:+5511999999999",
        )
        assert conversation.external_id == "ext-123"
        assert conversation.has_chats_room is False
        assert conversation.contact_name == "Test Contact"
        assert conversation.resolution == str(ResolutionEntities.IN_PROGRESS)

    def test_process_conversation_window_update_existing(self, conversation, mock_sentry):
        """Test updating existing conversation from window event."""
        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "external_id": "ext-updated",
                "start": "2024-01-01T14:00:00Z",
                "end": "2024-01-01T15:00:00Z",
                "has_chats_room": True,
                "name": "Updated Contact",
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        # Verify conversation was updated
        conversation.refresh_from_db()
        assert conversation.external_id == "ext-updated"
        assert conversation.has_chats_room is True
        assert conversation.contact_name == "Updated Contact"
        assert conversation.resolution == str(ResolutionEntities.HAS_CHAT_ROOM)

    def test_process_conversation_window_has_chats_room_sets_resolution(self, conversation, mock_sentry):
        """Test that has_chats_room=True sets resolution to HAS_CHAT_ROOM."""
        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "has_chats_room": True,
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        conversation.refresh_from_db()
        assert conversation.has_chats_room is True
        assert conversation.resolution == str(ResolutionEntities.HAS_CHAT_ROOM)

    def test_process_conversation_window_migrates_messages_on_close(self, conversation, mock_sentry):
        """Test that messages are migrated when conversation is closed."""
        # Set conversation to IN_PROGRESS
        conversation.resolution = str(ResolutionEntities.IN_PROGRESS)
        conversation.save()

        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "has_chats_room": True,  # This will close the conversation
            },
        }

        service = ConversationWindowService()
        with patch.object(service.migration_service, "migrate_conversation_messages_to_postgres") as mock_migrate:
            service.process_conversation_window(event_data)

            # Verify migration was called
            mock_migrate.assert_called_once_with(conversation)

    def test_process_conversation_window_no_migration_if_not_closing(self, conversation, mock_sentry):
        """Test that messages are not migrated if conversation is not being closed."""
        # Set conversation to IN_PROGRESS
        conversation.resolution = ResolutionEntities.IN_PROGRESS
        conversation.save()

        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "has_chats_room": False,  # This keeps it IN_PROGRESS
            },
        }

        service = ConversationWindowService()
        with patch.object(service.migration_service, "migrate_conversation_messages_to_postgres") as mock_migrate:
            service.process_conversation_window(event_data)

            # Verify migration was NOT called
            mock_migrate.assert_not_called()

    def test_process_conversation_window_missing_channel_uuid(self, mock_sentry):
        """Test handling event with missing channel_uuid."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(uuid4()),
                "contact_urn": "whatsapp:+5511999999999",
                # channel_uuid missing
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        # Verify no conversation was created
        assert Conversation.objects.count() == 0

    def test_process_conversation_window_preserves_existing_resolution(self, conversation, mock_sentry):
        """Test that existing resolution is preserved if has_chats_room=False."""
        # Set conversation to RESOLVED
        conversation.resolution = ResolutionEntities.RESOLVED
        conversation.save()

        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "has_chats_room": False,
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        conversation.refresh_from_db()
        assert conversation.resolution == str(ResolutionEntities.RESOLVED)

    def test_process_conversation_window_error_handling(self, mock_sentry):
        """Test error handling in process_conversation_window."""
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": "invalid-uuid",  # Invalid UUID will cause error
                "contact_urn": "whatsapp:+5511999999999",
                "channel_uuid": str(uuid4()),
            },
        }

        service = ConversationWindowService()
        with patch("sentry_sdk.capture_exception") as mock_capture:
            with pytest.raises(Exception):  # noqa: B017
                service.process_conversation_window(event_data)
            mock_capture.assert_called_once()
