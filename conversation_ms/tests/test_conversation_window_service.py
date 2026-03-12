"""
Tests for ConversationWindowService.
"""

from unittest.mock import patch
from uuid import uuid4

import pytest
from django.core.exceptions import ValidationError

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
                "ticket_uuid": str(uuid4()),
                "name": "Updated Contact",
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        # Verify conversation was updated; resolution set from ticket_uuid, close is from task only
        conversation.refresh_from_db()
        assert conversation.external_id == "ext-updated"
        assert conversation.has_chats_room is True
        assert conversation.contact_name == "Updated Contact"
        assert conversation.resolution == str(ResolutionEntities.HAS_CHAT_ROOM)

    def test_process_conversation_window_ticket_uuid_sets_resolution_but_no_close_actions(
        self, conversation, mock_sentry
    ):
        """Test that ticket_uuid sets resolution to HAS_CHAT_ROOM"""
        conversation.resolution = str(ResolutionEntities.IN_PROGRESS)
        conversation.save()
        ticket_uuid = uuid4()
        project_uuid = conversation.project.uuid
        channel_uuid = conversation.channel_uuid
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": conversation.contact_urn,
                "channel_uuid": str(channel_uuid),
                "ticket_uuid": str(ticket_uuid),
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        conversation.refresh_from_db()
        assert conversation.has_chats_room is True
        assert str(conversation.ticket_uuid) == str(ticket_uuid)
        assert conversation.resolution == str(ResolutionEntities.HAS_CHAT_ROOM)

    def test_process_conversation_window_ticket_uuid_does_not_trigger_migration(self, conversation, mock_sentry):
        """Test that receiving ticket_uuid sets resolution but does not trigger migration; close is done by the task."""
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
                "ticket_uuid": str(uuid4()),
            },
        }

        service = ConversationWindowService()
        service.process_conversation_window(event_data)

        conversation.refresh_from_db()
        # Resolution is set to HAS_CHAT_ROOM; migration/classification are only done by close_daily_conversations_task
        assert conversation.resolution == str(ResolutionEntities.HAS_CHAT_ROOM)

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
        """Test that existing resolution is preserved when no ticket_uuid (has_chats_room=False)."""
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
            with pytest.raises(ValidationError):
                service.process_conversation_window(event_data)
            mock_capture.assert_called_once()
