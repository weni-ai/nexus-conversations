"""
Tests for conversation_ms adapters.
"""

from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from conversation_ms.adapters.conversation import update_conversation_data
from conversation_ms.adapters.data_lake import DataLakeEventDTO
from conversation_ms.adapters.dynamo import DynamoMessageRepository
from conversation_ms.adapters.router_service import MainConversationService
from conversation_ms.models import Conversation, Project


@pytest.mark.django_db
class TestMainConversationService:
    """Tests for MainConversationService."""

    def test_ensure_conversation_exists_creates_new(self, project):
        """Test creating a new conversation when none exists."""
        channel_uuid = uuid4()
        service = MainConversationService()

        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=str(channel_uuid),
        )

        assert conversation is not None
        assert conversation.project == project
        assert conversation.contact_urn == "whatsapp:+5511999999999"
        assert conversation.contact_name == "Test Contact"
        assert str(conversation.channel_uuid) == str(channel_uuid)
        assert conversation.resolution == 2  # IN_PROGRESS
        assert conversation.start_date is not None
        assert conversation.end_date is not None

    def test_ensure_conversation_exists_returns_existing(self, project):
        """Test returning existing conversation in progress."""
        channel_uuid = uuid4()
        existing_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        service = MainConversationService()
        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=str(channel_uuid),
        )

        assert conversation.uuid == existing_conversation.uuid

    def test_ensure_conversation_exists_creates_project(self):
        """Test creating project if it doesn't exist."""
        project_uuid = uuid4()
        channel_uuid = uuid4()
        service = MainConversationService()

        conversation = service.ensure_conversation_exists(
            project_uuid=str(project_uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=str(channel_uuid),
        )

        assert conversation is not None
        project = Project.objects.get(uuid=project_uuid)
        assert project is not None

    def test_ensure_conversation_exists_handles_multiple_conversations(self, project):
        """Test handling multiple conversations in progress."""
        channel_uuid = uuid4()
        # Create multiple conversations in progress
        old_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )
        new_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            mock_migration.return_value.migrate_conversation_messages_to_postgres = Mock()
            service = MainConversationService()
            conversation = service.ensure_conversation_exists(
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                contact_name="Test Contact",
                channel_uuid=str(channel_uuid),
            )

            # Should return the most recent conversation
            assert conversation.uuid == new_conversation.uuid

            # Old conversation should be marked as UNCLASSIFIED
            old_conversation.refresh_from_db()
            assert str(old_conversation.resolution) == "3"  # UNCLASSIFIED

    def test_ensure_conversation_exists_handles_migration_error(self, project):
        """Test that migration errors are handled gracefully when closing multiple conversations."""
        channel_uuid = uuid4()
        # Create multiple conversations in progress
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )
        new_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            mock_migration.return_value.migrate_conversation_messages_to_postgres.side_effect = Exception(
                "Migration error"
            )

            service = MainConversationService()
            # Should not raise exception, just log error
            conversation = service.ensure_conversation_exists(
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                contact_name="Test Contact",
                channel_uuid=str(channel_uuid),
            )

            # Should still return the most recent conversation
            assert conversation.uuid == new_conversation.uuid

    def test_ensure_conversation_exists_returns_none_without_channel_uuid(self, project):
        """Test returning None when channel_uuid is missing."""
        service = MainConversationService()
        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=None,
        )

        assert conversation is None


@pytest.mark.django_db
class TestDynamoMessageRepository:
    """Tests for DynamoMessageRepository."""

    def test_storage_message(self, mock_dynamodb_table):
        """Test storing a message in DynamoDB."""
        repository = DynamoMessageRepository()
        message_data = {
            "text": "Hello",
            "source": "incoming",
            "created_at": "2024-01-01T12:00:00Z",
        }

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None

            repository.storage_message(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                message_data=message_data,
                channel_uuid=str(uuid4()),
                resolution_status=2,
                ttl_hours=48,
            )

            # Verify put_item was called
            mock_dynamodb_table.put_item.assert_called_once()
            call_args = mock_dynamodb_table.put_item.call_args
            assert "Item" in call_args.kwargs
            item = call_args.kwargs["Item"]
            assert item["message_text"] == "Hello"
            assert item["source_type"] == "incoming"
            assert "ExpiresOn" in item

    def test_get_messages(self, mock_dynamodb_table):
        """Test getting messages from DynamoDB."""
        mock_items = [
            {
                "conversation_key": "project#contact#channel",
                "message_timestamp": "2024-01-01T12:00:00#uuid",
                "message_text": "Hello",
                "source_type": "incoming",
                "created_at": "2024-01-01T12:00:00",
            }
        ]
        mock_dynamodb_table.query.return_value = {"Items": mock_items, "LastEvaluatedKey": None}

        repository = DynamoMessageRepository()

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None

            result = repository.get_messages(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(uuid4()),
                limit=50,
            )

            assert "items" in result
            assert len(result["items"]) == 1
            assert result["items"][0]["text"] == "Hello"
            assert result["items"][0]["source"] == "incoming"

    def test_convert_to_dynamo_sortable_timestamp(self):
        """Test timestamp conversion for DynamoDB."""
        repository = DynamoMessageRepository()
        timestamp = "2024-01-01T12:00:00Z"
        result = repository._convert_to_dynamo_sortable_timestamp(timestamp)
        assert result == "2024-01-01T12:00:00"


class TestDataLakeEventDTO:
    """Tests for DataLakeEventDTO."""

    def test_validate_success(self):
        """Test successful validation."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value="5",
        )
        # Should not raise exception
        dto.validate()

    def test_validate_empty_project(self):
        """Test validation fails with empty project."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project="",
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value="5",
        )
        with pytest.raises(ValueError, match="project cannot be empty"):
            dto.validate()

    def test_validate_none_value(self):
        """Test validation fails with None value."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value=None,
        )
        with pytest.raises(ValueError, match="value cannot be None"):
            dto.validate()

    def test_validate_wrong_event_name(self):
        """Test validation fails with wrong event name."""
        dto = DataLakeEventDTO(
            event_name="wrong_event",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value="5",
        )
        with pytest.raises(ValueError, match='event_name must be "weni_nexus_data"'):
            dto.validate()

    def test_dict(self):
        """Test converting DTO to dictionary."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value="5",
            metadata={"conversation_uuid": str(uuid4())},
        )
        result = dto.dict()
        assert result["event_name"] == "weni_nexus_data"
        assert result["project"] == dto.project
        assert result["contact_urn"] == dto.contact_urn
        assert result["key"] == dto.key
        assert result["value"] == "5"
        assert result["metadata"]["conversation_uuid"] == dto.metadata["conversation_uuid"]

    def test_validate_empty_contact_urn(self):
        """Test validation fails with empty contact_urn."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="",
            key="weni_csat",
            value_type="string",
            value="5",
        )
        with pytest.raises(ValueError, match="contact_urn cannot be empty"):
            dto.validate()

    def test_validate_empty_key(self):
        """Test validation fails with empty key."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            key="",
            value_type="string",
            value="5",
        )
        with pytest.raises(ValueError, match="key cannot be empty"):
            dto.validate()

    def test_validate_whitespace_only_project(self):
        """Test validation fails with whitespace-only project."""
        dto = DataLakeEventDTO(
            event_name="weni_nexus_data",
            date="2024-01-01T12:00:00",
            project="   ",
            contact_urn="whatsapp:+5511999999999",
            key="weni_csat",
            value_type="string",
            value="5",
        )
        with pytest.raises(ValueError, match="project cannot be empty"):
            dto.validate()


@pytest.mark.django_db
class TestUpdateConversationData:
    """Tests for update_conversation_data function."""

    def test_update_conversation_data_success(self, project):
        """Test successful update of conversation data."""
        channel_uuid = uuid4()
        conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        update_conversation_data(
            to_update={"csat": "5"},
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=str(channel_uuid),
        )

        conversation.refresh_from_db()
        assert conversation.csat == "5"

    def test_update_conversation_data_triggers_migration(self, project):
        """Test that updating resolution triggers message migration."""
        channel_uuid = uuid4()
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            mock_migration.return_value.migrate_conversation_messages_to_postgres = Mock()

            update_conversation_data(
                to_update={"resolution": 0},  # RESOLVED
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(channel_uuid),
            )

            # Verify migration service was called
            mock_migration.return_value.migrate_conversation_messages_to_postgres.assert_called_once()

    def test_update_conversation_data_no_migration_when_still_in_progress(self, project):
        """Test that migration is not triggered when conversation is still in progress."""
        channel_uuid = uuid4()
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            update_conversation_data(
                to_update={"csat": "5"},  # Not changing resolution
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(channel_uuid),
            )

            # Verify migration service was NOT called
            mock_migration.return_value.migrate_conversation_messages_to_postgres.assert_not_called()

    def test_update_conversation_data_not_found(self, project):
        """Test updating conversation that doesn't exist."""
        update_conversation_data(
            to_update={"csat": "5"},
            project_uuid=str(uuid4()),
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=str(uuid4()),
        )

        # Should not raise exception, just log warning

    def test_ensure_conversation_exists_handles_exception(self, project, mock_sentry):
        """Test that exceptions in ensure_conversation_exists are properly handled."""
        with patch("conversation_ms.models.Project.objects.get_or_create") as mock_project:
            mock_project.side_effect = Exception("Database error")

            service = MainConversationService()
            with pytest.raises(Exception, match="Database error"):
                service.ensure_conversation_exists(
                    project_uuid=str(project.uuid),
                    contact_urn="whatsapp:+5511999999999",
                    contact_name="Test Contact",
                    channel_uuid=str(uuid4()),
                )

    def test_get_dynamodb_table_handles_exception(self, mock_sentry):
        """Test that exceptions in get_dynamodb_table are properly handled."""
        from conversation_ms.adapters.dynamo import get_dynamodb_table

        with patch("boto3.resource") as mock_boto3:
            mock_boto3.side_effect = Exception("AWS connection error")

            with pytest.raises(Exception, match="AWS connection error"):
                with get_dynamodb_table("test_table"):
                    pass

    def test_get_dynamodb_table_handles_table_access_exception(self, mock_sentry):
        """Test that exceptions when accessing table are properly handled."""
        from conversation_ms.adapters.dynamo import get_dynamodb_table

        with patch("boto3.resource") as mock_boto3:
            mock_dynamodb = Mock()
            mock_dynamodb.Table.side_effect = Exception("Table access error")
            mock_boto3.return_value = mock_dynamodb

            with pytest.raises(Exception, match="Table access error"):
                with get_dynamodb_table("test_table"):
                    pass

    def test_convert_to_dynamo_sortable_timestamp_invalid_format(self):
        """Test timestamp conversion with invalid format."""
        repository = DynamoMessageRepository()
        invalid_timestamp = "invalid-timestamp-format"
        result = repository._convert_to_dynamo_sortable_timestamp(invalid_timestamp)

        # Should return fallback value
        assert result == "invalid-timestamp-format"

    def test_get_messages_handles_exception(self, mock_dynamodb_table):
        """Test that exceptions in get_messages are properly handled."""
        mock_dynamodb_table.query.side_effect = Exception("DynamoDB query error")

        repository = DynamoMessageRepository()

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None

            with pytest.raises(Exception, match="DynamoDB query error"):
                repository.get_messages(
                    project_uuid=str(uuid4()),
                    contact_urn="whatsapp:+5511999999999",
                    channel_uuid=str(uuid4()),
                    limit=50,
                )

    def test_get_messages_handles_invalid_cursor(self, mock_dynamodb_table):
        """Test that invalid cursor in get_messages is handled gracefully."""
        mock_dynamodb_table.query.return_value = {"Items": [], "LastEvaluatedKey": None}

        repository = DynamoMessageRepository()

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None

            # Should not raise exception, just log warning
            result = repository.get_messages(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(uuid4()),
                limit=50,
                cursor="invalid-cursor",
            )

            assert "items" in result

    def test_get_messages_with_valid_cursor(self, mock_dynamodb_table):
        """Test get_messages with valid cursor."""
        import base64
        import json

        cursor_data = {"conversation_key": "test", "message_timestamp": "2024-01-01T12:00:00#uuid"}
        valid_cursor = base64.b64encode(json.dumps(cursor_data).encode("utf-8")).decode("utf-8")

        mock_dynamodb_table.query.return_value = {"Items": [], "LastEvaluatedKey": None}

        repository = DynamoMessageRepository()

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None

            result = repository.get_messages(
                project_uuid=str(uuid4()),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(uuid4()),
                limit=50,
                cursor=valid_cursor,
            )

            assert "items" in result
            # Verify query was called with ExclusiveStartKey
            call_kwargs = mock_dynamodb_table.query.call_args[1]
            assert "ExclusiveStartKey" in call_kwargs

    def test_update_conversation_data_handles_migration_exception(self, project):
        """Test that exceptions during migration are handled gracefully."""
        channel_uuid = uuid4()
        Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=channel_uuid,
            resolution=2,  # IN_PROGRESS
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            mock_migration.return_value.migrate_conversation_messages_to_postgres.side_effect = Exception(
                "Migration error"
            )

            # Should not raise exception, just log error
            update_conversation_data(
                to_update={"resolution": 0},  # RESOLVED
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                channel_uuid=str(channel_uuid),
            )
