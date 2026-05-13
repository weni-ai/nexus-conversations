"""
Tests for ConversationWindowService.
"""

from unittest.mock import patch
from uuid import uuid4

import pendulum
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

        # Ticket / chat room flags only; resolution stays IN_PROGRESS for close_daily
        conversation.refresh_from_db()
        assert conversation.external_id == "ext-updated"
        assert conversation.has_chats_room is True
        assert conversation.contact_name == "Updated Contact"
        assert conversation.resolution == str(ResolutionEntities.IN_PROGRESS)

    def test_process_conversation_window_ticket_uuid_keeps_in_progress_no_close_actions(
        self, conversation, mock_sentry
    ):
        """ticket_uuid sets has_chats_room but leaves resolution IN_PROGRESS (close_daily closes later)."""
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
        assert conversation.resolution == str(ResolutionEntities.IN_PROGRESS)

    def test_process_conversation_window_ticket_uuid_does_not_trigger_migration(self, conversation, mock_sentry):
        """ticket_uuid does not change resolution to HAS_CHAT_ROOM; migration/close remain with close_daily."""
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
        assert conversation.resolution == str(ResolutionEntities.IN_PROGRESS)

    def test_process_conversation_window_preserves_start_date_when_already_set(self, project, mock_sentry):
        """Later window events must not overwrite ``start_date`` (e.g. ticket time vs first message)."""
        channel_uuid = uuid4()
        original_start = pendulum.parse("2026-05-13T09:51:46Z")
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Mari",
            channel_uuid=channel_uuid,
            resolution=str(ResolutionEntities.IN_PROGRESS),
            start_date=original_start,
            end_date=pendulum.parse("2026-05-14T02:59:59Z"),
        )
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project.uuid),
                "contact_urn": conv.contact_urn,
                "channel_uuid": str(channel_uuid),
                "start": "2026-05-13T10:07:00Z",
                "end": "2026-05-13T23:59:59Z",
                "ticket_uuid": str(uuid4()),
            },
        }
        ConversationWindowService().process_conversation_window(event_data)
        conv.refresh_from_db()
        assert pendulum.instance(conv.start_date).in_timezone("UTC") == original_start.in_timezone("UTC")

    def test_process_conversation_window_create_with_ticket_stays_in_progress(self, mock_sentry):
        """New conversation with ticket_uuid is still IN_PROGRESS for batch close."""
        project_uuid = uuid4()
        channel_uuid = uuid4()
        ticket = str(uuid4())
        event_data = {
            "correlation_id": str(uuid4()),
            "data": {
                "project_uuid": str(project_uuid),
                "contact_urn": "whatsapp:+5511999999999",
                "channel_uuid": str(channel_uuid),
                "ticket_uuid": ticket,
                "name": "With Ticket",
            },
        }
        ConversationWindowService().process_conversation_window(event_data)
        conv = Conversation.objects.get(project__uuid=project_uuid, channel_uuid=channel_uuid)
        assert conv.has_chats_room is True
        assert str(conv.ticket_uuid) == ticket
        assert conv.resolution == str(ResolutionEntities.IN_PROGRESS)

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
