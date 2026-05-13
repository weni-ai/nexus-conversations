"""
Tests for conversation_ms adapters.
"""

from unittest.mock import Mock, patch
from uuid import uuid4

import pendulum
import pytest

from conversation_ms.adapters.conversation import update_conversation_data
from conversation_ms.adapters.data_lake import DataLakeEventDTO, build_topics_event
from conversation_ms.adapters.dynamo import DynamoMessageRepository
from conversation_ms.adapters.router_service import MainConversationService
from conversation_ms.models import Conversation, Project
from conversation_ms.utils.date_helpers import (
    end_of_project_local_calendar_day_utc,
    resolve_effective_project_timezone,
)


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
            msg_created_at="2026-02-20T12:00:00Z",
        )

        assert conversation is not None
        assert conversation.project == project
        assert conversation.contact_urn == "whatsapp:+5511999999999"
        assert conversation.contact_name == "Test Contact"
        assert str(conversation.channel_uuid) == str(channel_uuid)
        assert conversation.resolution == "2"  # IN_PROGRESS
        assert conversation.start_date is not None
        assert conversation.end_date is not None
        tz = resolve_effective_project_timezone(project.timezone)
        expected_end = end_of_project_local_calendar_day_utc("2026-02-20T12:00:00Z", tz)
        assert pendulum.instance(conversation.end_date).in_timezone("UTC") == expected_end

    def test_ensure_conversation_exists_returns_existing(self, project):
        """Test returning existing conversation in progress."""
        channel_uuid = uuid4()
        msg_time = pendulum.parse("2026-02-20T12:00:00Z")
        tz = resolve_effective_project_timezone(project.timezone)
        existing_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution="2",  # IN_PROGRESS
            start_date=msg_time,
            end_date=end_of_project_local_calendar_day_utc(msg_time, tz),
        )

        service = MainConversationService()
        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=str(channel_uuid),
            msg_created_at="2026-02-20T12:00:00Z",
        )

        assert conversation.uuid == existing_conversation.uuid

    def test_ensure_conversation_exists_backfills_start_date_when_null(self, project):
        """Test that existing conversation with start_date=None gets backfilled from message timestamp."""
        channel_uuid = uuid4()
        existing = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution="2",  # IN_PROGRESS
            start_date=None,
            end_date=None,
        )
        # Anchor day match uses created_at when start_date is null — align with message day
        Conversation.objects.filter(pk=existing.pk).update(
            created_at=pendulum.parse("2026-02-20T08:00:00Z"),
        )

        service = MainConversationService()
        msg_created_at = "2026-02-20T14:30:00Z"
        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=str(channel_uuid),
            msg_created_at=msg_created_at,
        )

        assert conversation.uuid == existing.uuid
        existing.refresh_from_db()
        assert existing.start_date is not None
        assert existing.end_date is not None
        expected_start = pendulum.parse(msg_created_at)
        assert pendulum.instance(existing.start_date) == expected_start
        tz = resolve_effective_project_timezone(project.timezone)
        assert pendulum.instance(existing.end_date).in_timezone("UTC") == end_of_project_local_calendar_day_utc(
            msg_created_at, tz
        )

    def test_ensure_conversation_exists_moves_start_date_earlier_when_message_precedes_stored_start(self, project):
        """If ``start_date`` was set too late (e.g. room event), a customer message can pull it back."""
        project.timezone = "America/Sao_Paulo"
        project.save(update_fields=["timezone"])
        channel_uuid = uuid4()
        late_start = pendulum.parse("2026-05-13T10:07:00Z")
        tz = resolve_effective_project_timezone(project.timezone)
        existing = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Mari",
            channel_uuid=channel_uuid,
            resolution="2",
            start_date=late_start,
            end_date=end_of_project_local_calendar_day_utc(late_start, tz),
        )
        service = MainConversationService()
        earlier = "2026-05-13T09:51:46Z"
        conv = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Mari",
            channel_uuid=str(channel_uuid),
            msg_created_at=earlier,
        )
        assert conv.uuid == existing.uuid
        existing.refresh_from_db()
        assert pendulum.instance(existing.start_date).in_timezone("UTC") == pendulum.parse(earlier).in_timezone("UTC")
        assert pendulum.instance(existing.end_date).in_timezone("UTC") == end_of_project_local_calendar_day_utc(
            late_start, tz
        )

    def test_ensure_conversation_exists_does_not_move_start_across_prior_project_local_day(self, project):
        """Late message from previous project-local day must not move start_date (service-day anchor)."""
        project.timezone = "America/Sao_Paulo"
        project.save(update_fields=["timezone"])
        channel_uuid = uuid4()
        late_start = pendulum.parse("2026-05-13T10:07:00Z")
        tz = resolve_effective_project_timezone(project.timezone)
        existing = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Mari",
            channel_uuid=channel_uuid,
            resolution="2",
            start_date=late_start,
            end_date=end_of_project_local_calendar_day_utc(late_start, tz),
        )
        service = MainConversationService()
        # May 12 22:00 -03 == May 13 01:00Z — strictly before stored UTC start but different local calendar day
        prior_local_day_msg = "2026-05-13T01:00:00Z"
        conv = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Mari",
            channel_uuid=str(channel_uuid),
            msg_created_at=prior_local_day_msg,
        )
        assert conv.uuid == existing.uuid
        existing.refresh_from_db()
        assert pendulum.instance(existing.start_date).in_timezone("UTC") == late_start.in_timezone("UTC")

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
            msg_created_at="2026-02-20T12:00:00Z",
        )

        assert conversation is not None
        project = Project.objects.get(uuid=project_uuid)
        assert project is not None

    def test_ensure_conversation_exists_handles_multiple_conversations(self, project):
        """Multiple same-day IN_PROGRESS: reuse most recent; do not reclassify others."""
        channel_uuid = uuid4()
        day = pendulum.parse("2026-02-20T10:00:00Z")
        tz = resolve_effective_project_timezone(project.timezone)
        old_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution="2",  # IN_PROGRESS
            start_date=day,
            end_date=end_of_project_local_calendar_day_utc(day, tz),
        )
        later = day.add(hours=4)
        new_conversation = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=channel_uuid,
            resolution="2",  # IN_PROGRESS
            start_date=later,
            end_date=end_of_project_local_calendar_day_utc(later, tz),
        )

        with patch("conversation_ms.services.message_migration_service.MessageMigrationService") as mock_migration:
            mock_migration.return_value.migrate_conversation_messages_to_postgres = Mock()
            service = MainConversationService()
            conversation = service.ensure_conversation_exists(
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                contact_name="Test Contact",
                channel_uuid=str(channel_uuid),
                msg_created_at="2026-02-20T12:00:00Z",
            )

            assert conversation.uuid == new_conversation.uuid
            old_conversation.refresh_from_db()
            assert str(old_conversation.resolution) == "2"  # IN_PROGRESS unchanged
            mock_migration.return_value.migrate_conversation_messages_to_postgres.assert_not_called()

    def test_creates_new_in_progress_when_message_on_next_project_day(self, project):
        """Previous-day IN_PROGRESS stays open; message on a new calendar day opens a new conversation."""
        project.timezone = "America/Sao_Paulo"
        project.save(update_fields=["timezone"])
        channel_uuid = uuid4()
        june1_evening_sp = pendulum.parse("2026-06-02T02:00:00Z")  # June 1 23:00 -03
        old = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="Old",
            channel_uuid=channel_uuid,
            resolution="2",
            start_date=june1_evening_sp,
            end_date=end_of_project_local_calendar_day_utc(june1_evening_sp, "America/Sao_Paulo"),
        )
        service = MainConversationService()
        conv = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Next day",
            channel_uuid=str(channel_uuid),
            msg_created_at="2026-06-02T15:00:00Z",  # June 2 in São Paulo
        )
        assert conv.pk != old.pk
        old.refresh_from_db()
        assert str(old.resolution) == "2"
        assert str(conv.resolution) == "2"

    def test_same_project_day_reuses_despite_utc_date_roll(self, project):
        """Same calendar day in project TZ reuses even when UTC date differs within that local day."""
        project.timezone = "America/Sao_Paulo"
        project.save(update_fields=["timezone"])
        channel_uuid = uuid4()
        start = pendulum.parse("2026-06-02T02:00:00Z")  # June 1 late evening SP
        existing = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            contact_name="X",
            channel_uuid=channel_uuid,
            resolution="2",
            start_date=start,
            end_date=end_of_project_local_calendar_day_utc(start, "America/Sao_Paulo"),
        )
        service = MainConversationService()
        conv = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="X",
            channel_uuid=str(channel_uuid),
            msg_created_at="2026-06-02T01:30:00Z",  # still June 1 in SP
        )
        assert conv.uuid == existing.uuid

    def test_ensure_conversation_exists_returns_none_without_channel_uuid(self, project):
        """Test returning None when channel_uuid is missing."""
        service = MainConversationService()
        conversation = service.ensure_conversation_exists(
            project_uuid=str(project.uuid),
            contact_urn="whatsapp:+5511999999999",
            contact_name="Test Contact",
            channel_uuid=None,
            msg_created_at="2026-02-20T12:00:00Z",
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
                resolution_status="2",
                ttl_hours=168,
            )

            # Verify put_item was called
            mock_dynamodb_table.put_item.assert_called_once()
            call_args = mock_dynamodb_table.put_item.call_args
            assert "Item" in call_args.kwargs
            item = call_args.kwargs["Item"]
            assert item["message_text"] == "Hello"
            assert item["source_type"] == "incoming"
            assert "ExpiresOn" in item

    def test_storage_message_uses_message_id_from_event(self, mock_dynamodb_table):
        """Test that message_id from event (message_data) is used when provided."""
        repository = DynamoMessageRepository()
        expected_message_id = "evt-abc123-from-nexus-ai"
        message_data = {
            "message_id": expected_message_id,
            "text": "Hello",
            "source": "incoming",
            "created_at": "2024-01-01T12:00:00Z",
        }

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None
            with patch("conversation_ms.adapters.dynamo.sentry_sdk.capture_message") as mock_capture_message:
                repository.storage_message(
                    project_uuid=str(uuid4()),
                    contact_urn="whatsapp:+5511999999999",
                    message_data=message_data,
                    channel_uuid=str(uuid4()),
                    resolution_status="2",
                    ttl_hours=48,
                )

                # Provided message_id is used in stored item
                mock_dynamodb_table.put_item.assert_called_once()
                item = mock_dynamodb_table.put_item.call_args.kwargs["Item"]
                assert item["message_id"] == expected_message_id
                assert item["message_timestamp"].endswith(f"#{expected_message_id}")

                # Sentry warning path is not triggered
                mock_capture_message.assert_not_called()

    def test_storage_message_uses_id_fallback_from_event(self, mock_dynamodb_table):
        """Test that message_data['id'] is used when message_id is not present."""
        repository = DynamoMessageRepository()
        expected_id = "evt-fallback-id-456"
        message_data = {
            "id": expected_id,
            "text": "Hello",
            "source": "incoming",
            "created_at": "2024-01-01T12:00:00Z",
        }

        with patch("conversation_ms.adapters.dynamo.get_message_table") as mock_get_table:
            mock_get_table.return_value.__enter__.return_value = mock_dynamodb_table
            mock_get_table.return_value.__exit__.return_value = None
            with patch("conversation_ms.adapters.dynamo.sentry_sdk.capture_message") as mock_capture_message:
                repository.storage_message(
                    project_uuid=str(uuid4()),
                    contact_urn="whatsapp:+5511999999999",
                    message_data=message_data,
                    channel_uuid=str(uuid4()),
                )

                item = mock_dynamodb_table.put_item.call_args.kwargs["Item"]
                assert item["message_id"] == expected_id
                mock_capture_message.assert_not_called()

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
class TestBuildTopicsEvent:
    """Tests for build_topics_event (nexus-ai topics event shape)."""

    def test_bias_when_no_active_topics(self, project):
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
        )
        dto = build_topics_event(conv, str(project.uuid), None, has_active_topics=False)
        assert dto.key == "topics"
        assert dto.value == "bias"
        assert dto.metadata["topic_uuid"] == ""
        assert dto.metadata["subtopic_uuid"] == ""

    def test_derives_topic_from_subtopic_when_topic_fk_null(self, project):
        from conversation_ms.models import ConversationClassification, SubTopic, Topic

        topic = Topic.objects.create(project=project, name="Parent")
        sub = SubTopic.objects.create(topic=topic, name="Child")
        conv = Conversation.objects.create(
            project=project,
            contact_urn="whatsapp:+5511999999999",
            channel_uuid=uuid4(),
        )
        cc = ConversationClassification.objects.create(conversation=conv, topic=None, subtopic=sub)

        dto = build_topics_event(conv, str(project.uuid), cc, has_active_topics=True)
        assert dto.value == "Parent"
        assert dto.metadata["topic_uuid"] == str(topic.uuid)
        assert dto.metadata["subtopic_uuid"] == str(sub.uuid)
        assert dto.metadata["subtopic"] == "Child"


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
                    msg_created_at="2026-02-20T12:00:00Z",
                )

    def test_ensure_conversation_exists_invalid_msg_timestamp_raises(self, project):
        """Unparseable msg_created_at fails fast instead of repeating a doomed parse."""
        service = MainConversationService()
        with pytest.raises(ValueError, match="Invalid msg_created_at"):
            service.ensure_conversation_exists(
                project_uuid=str(project.uuid),
                contact_urn="whatsapp:+5511999999999",
                contact_name="Test Contact",
                channel_uuid=str(uuid4()),
                msg_created_at="totally-not-a-date",
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
