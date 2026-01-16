"""
Tests for conversation_ms services.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from uuid import uuid4

from conversation_ms.services.message_service import MessageService
from conversation_ms.services.conversation_service import ConversationService
from conversation_ms.services.csat_nps_service import CSATNPSService
from conversation_ms.services.message_migration_service import MessageMigrationService
from conversation_ms.models import Conversation, Project, ConversationMessages


@pytest.mark.django_db
class TestMessageService:
    """Tests for MessageService."""

    def test_process_message_received_success(self, sample_sqs_received_event, mock_dynamodb_repository, mock_sentry):
        """Test successful processing of message.received event."""
        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo, patch("conversation_ms.services.message_service.CSATNPSService") as mock_csat_service:
            # Setup mocks
            mock_conversation = Mock(spec=Conversation)
            mock_conversation.uuid = uuid4()
            mock_conv_service.return_value.ensure_conversation_exists.return_value = mock_conversation
            mock_msg_repo.return_value.save_received_message = Mock()
            mock_csat_service.return_value.process_csat_event = Mock()
            mock_csat_service.return_value.process_nps_event = Mock()

            service = MessageService()
            service.process_message_received(sample_sqs_received_event)

            # Verify conversation service was called
            mock_conv_service.return_value.ensure_conversation_exists.assert_called_once()
            # Verify message repository was called
            mock_msg_repo.return_value.save_received_message.assert_called_once()

    def test_process_message_received_no_conversation(self, sample_sqs_received_event, mock_sentry):
        """Test processing message.received when conversation is not created."""
        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo:
            # Setup mocks - conversation service returns None
            mock_conv_service.return_value.ensure_conversation_exists.return_value = None
            mock_msg_repo.return_value.save_received_message = Mock()

            service = MessageService()
            service.process_message_received(sample_sqs_received_event)

            # Verify message repository was NOT called
            mock_msg_repo.return_value.save_received_message.assert_not_called()

    def test_process_message_sent_success(self, sample_sqs_sent_event, mock_dynamodb_repository, mock_sentry):
        """Test successful processing of message.sent event."""
        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo, patch("conversation_ms.services.message_service.CSATNPSService") as mock_csat_service:
            # Setup mocks
            mock_conversation = Mock(spec=Conversation)
            mock_conversation.uuid = uuid4()
            mock_conv_service.return_value.ensure_conversation_exists.return_value = mock_conversation
            mock_msg_repo.return_value.save_sent_message = Mock()
            mock_csat_service.return_value.process_csat_event = Mock()
            mock_csat_service.return_value.process_nps_event = Mock()

            service = MessageService()
            service.process_message_sent(sample_sqs_sent_event)

            # Verify conversation service was called
            mock_conv_service.return_value.ensure_conversation_exists.assert_called_once()
            # Verify message repository was called
            mock_msg_repo.return_value.save_sent_message.assert_called_once()

    def test_process_message_sent_no_conversation(self, sample_sqs_sent_event, mock_sentry):
        """Test processing message.sent when conversation is not created."""
        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo:
            # Setup mocks - conversation service returns None
            mock_conv_service.return_value.ensure_conversation_exists.return_value = None
            mock_msg_repo.return_value.save_sent_message = Mock()

            service = MessageService()
            service.process_message_sent(sample_sqs_sent_event)

            # Verify message repository was NOT called
            mock_msg_repo.return_value.save_sent_message.assert_not_called()

    def test_process_message_received_with_csat_event(
        self, sample_sqs_received_event, mock_dynamodb_repository, mock_data_lake_task, mock_sentry
    ):
        """Test processing message.received with CSAT event."""
        sample_sqs_received_event["key"] = "weni_csat"
        sample_sqs_received_event["value"] = "5"

        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo, patch("conversation_ms.services.message_service.CSATNPSService") as mock_csat_service:
            mock_conversation = Mock(spec=Conversation)
            mock_conversation.uuid = uuid4()
            mock_conv_service.return_value.ensure_conversation_exists.return_value = mock_conversation
            mock_msg_repo.return_value.save_received_message = Mock()
            mock_csat_service.return_value.process_csat_event = Mock()

            service = MessageService()
            service.process_message_received(sample_sqs_received_event)

            # Verify CSAT service was called
            mock_csat_service.return_value.process_csat_event.assert_called_once()

    def test_process_message_received_with_nps_event(
        self, sample_sqs_received_event, mock_dynamodb_repository, mock_data_lake_task, mock_sentry
    ):
        """Test processing message.received with NPS event."""
        sample_sqs_received_event["key"] = "weni_nps"
        sample_sqs_received_event["value"] = "9"

        with patch("conversation_ms.services.message_service.ConversationService") as mock_conv_service, patch(
            "conversation_ms.services.message_service.MessageRepository"
        ) as mock_msg_repo, patch("conversation_ms.services.message_service.CSATNPSService") as mock_csat_service:
            mock_conversation = Mock(spec=Conversation)
            mock_conversation.uuid = uuid4()
            mock_conv_service.return_value.ensure_conversation_exists.return_value = mock_conversation
            mock_msg_repo.return_value.save_received_message = Mock()
            mock_csat_service.return_value.process_nps_event = Mock()

            service = MessageService()
            service.process_message_received(sample_sqs_received_event)

            # Verify NPS service was called
            mock_csat_service.return_value.process_nps_event.assert_called_once()

    def test_process_message_received_handles_exception(self, sample_sqs_received_event, mock_sentry):
        """Test that exceptions in process_message_received are properly handled."""
        with patch("conversation_ms.services.message_service.MessageReceivedEvent") as mock_event:
            mock_event.from_sqs_event.side_effect = Exception("Event parsing error")

            service = MessageService()
            with pytest.raises(Exception, match="Event parsing error"):
                service.process_message_received(sample_sqs_received_event)

    def test_process_message_sent_handles_exception(self, sample_sqs_sent_event, mock_sentry):
        """Test that exceptions in process_message_sent are properly handled."""
        with patch("conversation_ms.services.message_service.MessageSentEvent") as mock_event:
            mock_event.from_sqs_event.side_effect = Exception("Event parsing error")

            service = MessageService()
            with pytest.raises(Exception, match="Event parsing error"):
                service.process_message_sent(sample_sqs_sent_event)

    def test_handle_special_events_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in _handle_special_events are handled gracefully."""
        with patch("conversation_ms.services.message_service.CSATNPSService") as mock_csat_service:
            mock_csat_service.return_value.process_csat_event.side_effect = Exception("CSAT processing error")

            service = MessageService()
            event_data = {"key": "weni_csat", "value": "5"}

            # Should not raise exception, just log warning
            service._handle_special_events(
                event_data=event_data,
                conversation=conversation,
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
            )


@pytest.mark.django_db
class TestConversationService:
    """Tests for ConversationService."""

    def test_ensure_conversation_exists_with_channel_uuid(self, project, mock_sentry):
        """Test ensuring conversation exists with channel_uuid."""
        channel_uuid = uuid4()
        with patch("conversation_ms.adapters.router_service.MainConversationService") as mock_main_service:
            mock_conversation = Mock(spec=Conversation)
            mock_main_service.return_value.ensure_conversation_exists.return_value = mock_conversation

            service = ConversationService()
            result = service.ensure_conversation_exists(
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                contact_name="Test Contact",
                channel_uuid=str(channel_uuid),
            )

            assert result == mock_conversation
            mock_main_service.return_value.ensure_conversation_exists.assert_called_once()

    def test_ensure_conversation_exists_without_channel_uuid(self, project, mock_sentry):
        """Test ensuring conversation exists without channel_uuid."""
        service = ConversationService()
        result = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=None,
        )

        assert result is None

    def test_ensure_conversation_exists_handles_exception(self, project, mock_sentry):
        """Test that exceptions in ensure_conversation_exists are properly handled."""
        with patch("conversation_ms.adapters.router_service.MainConversationService") as mock_main_service:
            mock_main_service.return_value.ensure_conversation_exists.side_effect = Exception("Service error")

            service = ConversationService()
            with pytest.raises(Exception, match="Service error"):
                service.ensure_conversation_exists(
                    project_uuid=str(project.uuid),
                    contact_urn="whatsapp:+5511999999999",
                    contact_name="Test Contact",
                    channel_uuid=str(uuid4()),
                )


@pytest.mark.django_db
class TestCSATNPSService:
    """Tests for CSATNPSService."""

    def test_process_csat_event_success(self, conversation, mock_sentry):
        """Test successful processing of CSAT event."""
        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "django.conf.settings"
        ) as mock_settings, patch("conversation_ms.services.csat_nps_service.send_data_lake_event") as mock_task:
            mock_settings.AGENT_UUID_CSAT = str(uuid4())
            mock_update.return_value = None
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "5", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            service.process_csat_event(
                event_data=event_data,
                conversation=conversation,
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
            )

            # Verify update_conversation_data was called
            mock_update.assert_called_once()
            # Verify Celery task was called
            mock_task.delay.assert_called_once()

    def test_process_csat_event_with_dates(self, conversation, mock_sentry):
        """Test CSAT event processing with conversation start_date and end_date."""
        import pendulum
        from datetime import timedelta

        conversation.start_date = pendulum.now()
        conversation.end_date = pendulum.now() + timedelta(days=1)
        conversation.save()

        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "django.conf.settings"
        ) as mock_settings, patch("conversation_ms.services.csat_nps_service.send_data_lake_event") as mock_task:
            mock_settings.AGENT_UUID_CSAT = str(uuid4())
            mock_update.return_value = None
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "5", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            service.process_csat_event(
                event_data=event_data,
                conversation=conversation,
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
            )

            # Verify metadata includes dates
            call_args = mock_task.delay.call_args[0][0]
            assert "conversation_start_date" in call_args["metadata"]
            assert "conversation_end_date" in call_args["metadata"]

    def test_process_nps_event_with_dates(self, conversation, mock_sentry):
        """Test NPS event processing with conversation start_date and end_date."""
        import pendulum
        from datetime import timedelta

        conversation.start_date = pendulum.now()
        conversation.end_date = pendulum.now() + timedelta(days=1)
        conversation.save()

        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "django.conf.settings"
        ) as mock_settings, patch("conversation_ms.services.csat_nps_service.send_data_lake_event") as mock_task:
            mock_settings.AGENT_UUID_NPS = str(uuid4())
            mock_update.return_value = None
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "9", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            service.process_nps_event(
                event_data=event_data,
                conversation=conversation,
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
            )

            # Verify metadata includes dates
            call_args = mock_task.delay.call_args[0][0]
            assert "conversation_start_date" in call_args["metadata"]
            assert "conversation_end_date" in call_args["metadata"]

    def test_process_csat_event_missing_value(self, conversation, mock_sentry):
        """Test processing CSAT event with missing value."""
        service = CSATNPSService()
        event_data = {"project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

        service.process_csat_event(
            event_data=event_data,
            conversation=conversation,
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
        )

        # Should not raise exception, just log warning

    def test_process_nps_event_success(self, conversation, mock_sentry):
        """Test successful processing of NPS event."""
        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "django.conf.settings"
        ) as mock_settings, patch("conversation_ms.services.csat_nps_service.send_data_lake_event") as mock_task:
            mock_settings.AGENT_UUID_NPS = str(uuid4())
            mock_update.return_value = None
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "9", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            service.process_nps_event(
                event_data=event_data,
                conversation=conversation,
                project_uuid=str(conversation.project.uuid),
                contact_urn=conversation.contact_urn,
            )

            # Verify update_conversation_data was called
            mock_update.assert_called_once()
            # Verify Celery task was called
            mock_task.delay.assert_called_once()

    def test_process_nps_event_missing_value(self, conversation, mock_sentry):
        """Test processing NPS event with missing value."""
        service = CSATNPSService()
        event_data = {"project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

        service.process_nps_event(
            event_data=event_data,
            conversation=conversation,
            project_uuid=str(conversation.project.uuid),
            contact_urn=conversation.contact_urn,
        )

        # Should not raise exception, just log warning

    def test_process_csat_event_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in process_csat_event are properly handled."""
        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "conversation_ms.services.csat_nps_service.send_data_lake_event"
        ) as mock_task:
            mock_update.side_effect = Exception("Update error")
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "5", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            with pytest.raises(Exception, match="Update error"):
                service.process_csat_event(
                    event_data=event_data,
                    conversation=conversation,
                    project_uuid=str(conversation.project.uuid),
                    contact_urn=conversation.contact_urn,
                )

    def test_process_nps_event_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in process_nps_event are properly handled."""
        with patch("conversation_ms.adapters.conversation.update_conversation_data") as mock_update, patch(
            "conversation_ms.services.csat_nps_service.send_data_lake_event"
        ) as mock_task:
            mock_update.side_effect = Exception("Update error")
            mock_task.delay = Mock(return_value=Mock())

            service = CSATNPSService()
            event_data = {"value": "9", "project_uuid": str(conversation.project.uuid), "contact_urn": conversation.contact_urn}

            with pytest.raises(Exception, match="Update error"):
                service.process_nps_event(
                    event_data=event_data,
                    conversation=conversation,
                    project_uuid=str(conversation.project.uuid),
                    contact_urn=conversation.contact_urn,
                )


@pytest.mark.django_db
class TestMessageMigrationService:
    """Tests for MessageMigrationService."""

    def test_migrate_conversation_messages_to_postgres_success(
        self, conversation, mock_dynamodb_repository, mock_sentry
    ):
        """Test successful migration of messages from DynamoDB to PostgreSQL."""
        # Mock DynamoDB response
        mock_messages = [
            {"text": "Hello", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
            {"text": "Hi there", "source": "outgoing", "created_at": "2024-01-01T12:01:00"},
        ]

        with patch("conversation_ms.services.message_migration_service.MessageRepository") as mock_repo:
            mock_repo.return_value.get_messages_from_dynamo.return_value = mock_messages

            service = MessageMigrationService()
            service.migrate_conversation_messages_to_postgres(conversation)

            # Verify ConversationMessages was created/updated
            conversation_messages = ConversationMessages.objects.filter(conversation=conversation).first()
            assert conversation_messages is not None
            assert len(conversation_messages.messages) == 2
            assert conversation_messages.messages[0]["text"] == "Hello"
            assert conversation_messages.messages[1]["text"] == "Hi there"

    def test_migrate_conversation_messages_no_messages(self, conversation, mock_sentry):
        """Test migration when there are no messages in DynamoDB."""
        with patch("conversation_ms.services.message_migration_service.MessageRepository") as mock_repo:
            mock_repo.return_value.get_messages_from_dynamo.return_value = []

            service = MessageMigrationService()
            service.migrate_conversation_messages_to_postgres(conversation)

            # Verify ConversationMessages was not created
            conversation_messages = ConversationMessages.objects.filter(conversation=conversation).first()
            assert conversation_messages is None

    def test_migrate_conversation_messages_update_existing(self, conversation, mock_dynamodb_repository, mock_sentry):
        """Test migration updates existing ConversationMessages."""
        # Create existing ConversationMessages
        ConversationMessages.objects.create(conversation=conversation, messages=[{"text": "Old message"}])

        # Mock DynamoDB response
        mock_messages = [
            {"text": "New message", "source": "incoming", "created_at": "2024-01-01T12:00:00"},
        ]

        with patch("conversation_ms.services.message_migration_service.MessageRepository") as mock_repo:
            mock_repo.return_value.get_messages_from_dynamo.return_value = mock_messages

            service = MessageMigrationService()
            service.migrate_conversation_messages_to_postgres(conversation)

            # Verify ConversationMessages was updated
            conversation_messages = ConversationMessages.objects.get(conversation=conversation)
            assert len(conversation_messages.messages) == 1
            assert conversation_messages.messages[0]["text"] == "New message"

    def test_migrate_conversation_messages_handles_exception(self, conversation, mock_sentry):
        """Test that exceptions in migrate_conversation_messages_to_postgres are properly handled."""
        with patch("conversation_ms.services.message_migration_service.MessageRepository") as mock_repo:
            mock_repo.return_value.get_messages_from_dynamo.side_effect = Exception("DynamoDB error")

            service = MessageMigrationService()
            with pytest.raises(Exception, match="DynamoDB error"):
                service.migrate_conversation_messages_to_postgres(conversation)

